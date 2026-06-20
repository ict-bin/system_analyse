"""task_workspace.py — V3.0 任务工作区 NFS↔本地 桥接（控制器使用）。

目标：任务执行的重 I/O 落到容器本地盘，避免 NFS 阻塞（之前 run_task 子进程在
NFS O_EXCL 锁 / 大量读写上 `nfs_wait_bit_killable` D 态卡死）。

机制（全部由 WorkerControl 控制进程调用，任务进程路径不变）：
  setup_local_workspace:  spawn 前
      - 在 NFS 上 mkdir 任务根 + 写 .task_version（防 ensure_task_format_version 误删）
      - 创建本地 run 目录
      - 把 NFS 上的 {task}/run 建成软链接 → 本地 run（任务写 run/* 即落本地）
      - output/ 保持真实 NFS 目录（最终产物 + 前端读）
  sync_for_frontend:      执行中周期调用
      - events.jsonl(本地) → {task}/events.jsonl(NFS)        前端 timeline
      - workspace/modules/*/{module_report.md,files.list} → {task}/output/modules/  前端模块进度
      - workspace/final_report.md, workspace/modules.list → {task}/output/           前端报告
  finalize_workspace:     cancel/正常/异常结束
      - 确保 output/ 有产物（被杀场景代归档：workspace→output）
      - 把本地 run 拷回 NFS 真实 run/（保 resume/.checkpoint/归档），替换软链接
      - 删除本地 run

跨 pod 关键点：软链接 target 是 runner 本地路径，API pod 跨 pod 读软链会失效；
因此前端需要的文件由 sync_for_frontend 复制到 **真实 NFS 路径** 供前端读。
"""
from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

logger = logging.getLogger("sa.task_workspace")

LOCAL_BASE = os.environ.get("SECFLOW_SYSTEM_ANALYSE_LOCAL_WORKSPACE_BASE", "/local-workspace")
# 与 app/task_version.py 保持一致；写在这里避免控制器依赖 pipeline
TASK_FORMAT_VERSION = "2.0"


def _nfs_root(output_path: str, task_id: str) -> Path:
    return Path(output_path) / task_id


def _local_run(task_id: str) -> Path:
    return Path(LOCAL_BASE) / task_id / "run"


# ── setup：建本地 run + NFS 软链接 ────────────────────────────────────────────

def setup_local_workspace(output_path: str, task_id: str) -> dict:
    """spawn 前调用。返回 {ok, nfs_run, local_run, reason}。失败时降级（不软链，直接用 NFS）。"""
    try:
        nfs_root = _nfs_root(output_path, task_id)
        nfs_run = nfs_root / "run"
        local_run = _local_run(task_id)

        # 1. 本地 run（含子目录）
        (local_run / "workspace").mkdir(parents=True, exist_ok=True)
        (local_run / "sessions").mkdir(parents=True, exist_ok=True)

        # 2. NFS 任务根 + .task_version（防 orchestrator.ensure_task_format_version 整目录 rmtree）
        nfs_root.mkdir(parents=True, exist_ok=True)
        try:
            (nfs_root / ".task_version").write_text(TASK_FORMAT_VERSION, encoding="utf-8")
        except OSError:
            pass
        (nfs_root / "output").mkdir(parents=True, exist_ok=True)

        # 3. 把 NFS 上的 run 替换为指向本地的软链接
        if nfs_run.is_symlink():
            try: nfs_run.unlink()
            except OSError: pass
        elif nfs_run.exists():
            # 旧的真实 run 目录（restart/resume 残留）→ 移除（restart 语义已清；resume 另行处理）
            shutil.rmtree(str(nfs_run), ignore_errors=True)
        os.symlink(str(local_run), str(nfs_run))
        logger.info("task %s: NFS run -> local %s (symlink)", task_id, local_run)
        return {"ok": True, "nfs_run": str(nfs_run), "local_run": str(local_run)}
    except Exception as exc:
        logger.exception("setup_local_workspace failed for %s, fallback to NFS-direct", task_id)
        return {"ok": False, "reason": str(exc)}


# ── sync：把前端需要的文件从本地复制到真实 NFS ───────────────────────────────

def sync_for_frontend(output_path: str, task_id: str) -> None:
    """执行中周期调用：events.jsonl + 模块产物 本地 → NFS（供前端读）。"""
    try:
        nfs_root = _nfs_root(output_path, task_id)
        local_run = _local_run(task_id)
        if not local_run.exists():
            return
        # events.jsonl → {task}/events.jsonl (NFS, 前端 timeline 读这里)
        ev_src = local_run / "events.jsonl"
        if ev_src.exists():
            _safe_copy(ev_src, nfs_root / "events.jsonl")
        # 模块进度：workspace/modules/*/{module_report.md, files.list} → output/modules/
        ws_mods = local_run / "workspace" / "modules"
        if ws_mods.is_dir():
            out_mods = nfs_root / "output" / "modules"
            out_mods.mkdir(parents=True, exist_ok=True)
            for mod in ws_mods.iterdir():
                if not mod.is_dir():
                    continue
                dst = out_mods / mod.name
                dst.mkdir(parents=True, exist_ok=True)
                for fname in ("module_report.md", "files.list"):
                    f = mod / fname
                    if f.exists():
                        _safe_copy(f, dst / fname)
        # workspace/final_report.md, modules.list → output/
        for fname in ("final_report.md", "modules.list"):
            f = local_run / "workspace" / fname
            if f.exists():
                _safe_copy(f, nfs_root / "output" / fname)
    except Exception:
        logger.exception("sync_for_frontend failed for %s", task_id)


# ── finalize：产物回 NFS + 清本地 ─────────────────────────────────────────────

def finalize_workspace(output_path: str, task_id: str, normal: bool) -> None:
    """cancel/正常/异常结束调用：产物移回 NFS 真实路径 + 删本地。"""
    try:
        nfs_root = _nfs_root(output_path, task_id)
        nfs_run = nfs_root / "run"
        local_run = _local_run(task_id)
        if not local_run.exists():
            return

        # 1. 被杀场景代归档：output/ 若缺产物，从本地 workspace 补
        _surrogate_archive_if_needed(nfs_root, local_run, normal)

        # 2. 最后同步一次 events + 模块产物
        sync_for_frontend(output_path, task_id)

        # 3. 把本地 run 拷回 NFS 真实 run/（保 .checkpoint/resume/archive），替换软链接
        try:
            if nfs_run.is_symlink():
                nfs_run.unlink()
            elif nfs_run.exists():
                shutil.rmtree(str(nfs_run), ignore_errors=True)
            shutil.copytree(str(local_run), str(nfs_run))
            logger.info("task %s: local run -> NFS run (copied back, %d items)",
                        task_id, sum(1 for _ in nfs_run.rglob("*")))
        except Exception:
            logger.exception("copy local run back to NFS failed for %s", task_id)
            # 拷回失败也要保证软链接被清掉，避免悬空
            try:
                if nfs_run.is_symlink():
                    nfs_run.unlink()
            except OSError:
                pass

        # 4. 删除本地
        try:
            shutil.rmtree(str(Path(LOCAL_BASE) / task_id), ignore_errors=True)
        except Exception:
            logger.exception("cleanup local workspace failed for %s", task_id)
    except Exception:
        logger.exception("finalize_workspace failed for %s", task_id)


def _surrogate_archive_if_needed(nfs_root: Path, local_run: Path, normal: bool) -> None:
    """被杀/异常结束且 output/ 缺产物时，从本地 workspace 补齐 modules/final_report/modules.list。"""
    try:
        out = nfs_root / "output"
        out.mkdir(parents=True, exist_ok=True)
        ws = local_run / "workspace"
        if not ws.is_dir():
            return
        # modules
        ws_mods = ws / "modules"
        out_mods = out / "modules"
        if ws_mods.is_dir() and not out_mods.exists():
            out_mods.mkdir(parents=True, exist_ok=True)
            for mod in ws_mods.iterdir():
                if mod.is_dir():
                    dst = out_mods / mod.name
                    if not dst.exists():
                        shutil.copytree(str(mod), str(dst))
        # final_report.md
        rep = ws / "final_report.md"
        if rep.exists() and not (out / "final_report.md").exists():
            _safe_copy(rep, out / "final_report.md")
        # modules.list（若 workspace 有则用；否则由调用方/上层生成）
        ml = ws / "modules.list"
        if ml.exists() and not (out / "modules.list").exists():
            _safe_copy(ml, out / "modules.list")
    except Exception:
        logger.exception("surrogate archive failed")


def _safe_copy(src: Path, dst: Path) -> None:
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst.with_suffix(dst.suffix + ".tmp")
        shutil.copy2(str(src), str(tmp))
        os.replace(str(tmp), str(dst))
    except Exception:
        # 单文件复制失败不致命
        pass
