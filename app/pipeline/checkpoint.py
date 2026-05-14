"""
pipeline/checkpoint.py — 断点续跑管理器

每个阶段/模块完成后，将原子写入一个 .done 文件到 workspace/.checkpoint/。
续跑时通过检测这些文件决定哪些阶段/模块可以跳过，无需修改 DB 状态。

目录结构:
  workspace/
    .checkpoint/
      s0_filter.done
      s0_explore.done
      s0_prescan.done
      s0_pathgroup.done
      s1_classify.done
      s1_security_filter.done
      s2_refine.done
      s2_global_check.done
      s2_modules/
        auth.done
        network.done
        ...
      s3_analyse.done
      s3_modules/
        auth.done
        network.done
        ...
      s4_completeness.done
      s4_report.done

每个 .done 文件为 JSON:
  {
    "completed_at": "2025-05-14T10:23:45+08:00",
    "duration_ms": 12345.6,
    "extra": {}
  }
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

_CHECKPOINT_DIR = ".checkpoint"
_S2_MODULES_DIR = "s2_modules"
_S3_MODULES_DIR = "s3_modules"

# 所有阶段的有序标记名称（用于 load_summary 返回）
STAGE_KEYS = [
    "s0_filter",
    "s0_type_classify",
    "s0_unknown_checker",
    "s0_explore",
    "s0_prescan",
    "s0_pathgroup",
    "s0_sub_reader",
    "s0_validate_details",
    "s1_classify",
    "s1_security_filter",
    "s2_refine",
    "s2_global_check",
    "s3_analyse",
    "s4_completeness",
    "s4_report",
]


def _utc_now_iso() -> str:
    """返回带时区的 ISO8601 时间字符串（使用本地时区）。"""
    tz_offset = datetime.now(timezone.utc).astimezone().utcoffset()
    tz = timezone(tz_offset) if tz_offset is not None else timezone.utc
    return datetime.now(tz).isoformat(timespec="seconds")


class CheckpointManager:
    """
    断点续跑管理器。

    所有读写操作均通过此类进行，保证：
    1. 原子写入（tmp → rename）防止脏标记
    2. 统一的路径解析逻辑
    3. 线程/协程安全（asyncio 单线程模型下，rename 原子操作已足够）
    """

    def __init__(self, workspace: Path) -> None:
        self._workspace = workspace
        self._dir = workspace / _CHECKPOINT_DIR
        self._dir.mkdir(exist_ok=True)
        (self._dir / _S2_MODULES_DIR).mkdir(exist_ok=True)
        (self._dir / _S3_MODULES_DIR).mkdir(exist_ok=True)

    # ── 核心读写 ──────────────────────────────────────────────────────────

    def mark_done(self, name: str, duration_ms: float = 0.0, **extra: Any) -> None:
        """原子写入 checkpoint 标记。

        name 格式:
          "s0_filter"           → .checkpoint/s0_filter.done
          "s2_modules/auth"     → .checkpoint/s2_modules/auth.done
          "s3_modules/network"  → .checkpoint/s3_modules/network.done
        """
        payload = {
            "completed_at": _utc_now_iso(),
            "duration_ms": round(duration_ms, 1),
            "extra": extra,
        }
        target = self._resolve(name)
        tmp = target.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            tmp.rename(target)
        except Exception:
            # 写入失败不影响主流程，只是续跑时需要重新执行该阶段
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass

    def is_done(self, name: str) -> bool:
        """检查 checkpoint 标记是否存在。"""
        return self._resolve(name).exists()

    def clear(self, name: str) -> None:
        """清除 checkpoint 标记（redo 场景）。"""
        try:
            self._resolve(name).unlink(missing_ok=True)
        except Exception:
            pass

    def clear_all(self) -> None:
        """清除所有 checkpoint（restart 场景）。"""
        import shutil
        try:
            shutil.rmtree(str(self._dir), ignore_errors=True)
            # 重建空目录
            self._dir.mkdir(exist_ok=True)
            (self._dir / _S2_MODULES_DIR).mkdir(exist_ok=True)
            (self._dir / _S3_MODULES_DIR).mkdir(exist_ok=True)
        except Exception:
            pass

    # ── 模块级批量操作 ────────────────────────────────────────────────────

    def list_done_modules(self, stage: str) -> set[str]:
        """
        列出某 stage 下所有已完成的模块名。

        stage: "s2" 或 "s3"
        返回: {"auth", "network", "crypto", ...}
        """
        stage_dir = self._dir / f"{stage}_modules"
        if not stage_dir.exists():
            return set()
        return {p.stem for p in stage_dir.glob("*.done")}

    def clear_stage_modules(self, stage: str, module_names: list[str]) -> None:
        """清除指定 stage 下指定模块的 checkpoint（redo 场景）。"""
        for name in module_names:
            self.clear(f"{stage}_modules/{name}")

    # ── 状态汇总（供 API 查询） ────────────────────────────────────────────

    def load_summary(self) -> dict:
        """
        加载所有 checkpoint 状态，返回结构化摘要。

        返回格式:
        {
            "stages": {
                "s0_filter": {"done": true, "completed_at": "...", "extra": {...}},
                ...
            },
            "s2_modules": {
                "auth": {"done": true, "completed_at": "..."},
                ...
            },
            "s3_modules": {
                "auth": {"done": true, "completed_at": "..."},
                ...
            },
            "overall_done": false,
            "last_completed_stage": "s2_refine",
        }
        """
        stages: dict[str, dict] = {}
        for key in STAGE_KEYS:
            p = self._resolve(key)
            if p.exists():
                try:
                    data = json.loads(p.read_text(encoding="utf-8"))
                    stages[key] = {"done": True, **data}
                except Exception:
                    stages[key] = {"done": True, "completed_at": None, "extra": {}}
            else:
                stages[key] = {"done": False}

        def _load_module_dir(subdir: str) -> dict[str, dict]:
            d = self._dir / subdir
            result: dict[str, dict] = {}
            if not d.exists():
                return result
            for p in sorted(d.glob("*.done")):
                try:
                    data = json.loads(p.read_text(encoding="utf-8"))
                    result[p.stem] = {"done": True, **data}
                except Exception:
                    result[p.stem] = {"done": True}
            return result

        s2_mods = _load_module_dir(_S2_MODULES_DIR)
        s3_mods = _load_module_dir(_S3_MODULES_DIR)

        last_completed = None
        for key in reversed(STAGE_KEYS):
            if stages.get(key, {}).get("done"):
                last_completed = key
                break

        overall_done = stages.get("s4_report", {}).get("done", False)

        return {
            "stages": stages,
            "s2_modules": s2_mods,
            "s3_modules": s3_mods,
            "overall_done": overall_done,
            "last_completed_stage": last_completed,
            "s2_done_count": len(s2_mods),
            "s3_done_count": len(s3_mods),
        }

    def has_any_checkpoint(self) -> bool:
        """是否存在任何 checkpoint（用于判断 workspace 是否有续跑价值）。"""
        return any(self._resolve(k).exists() for k in STAGE_KEYS)

    # ── 内部工具 ──────────────────────────────────────────────────────────

    def _resolve(self, name: str) -> Path:
        """
        将 name 解析为 .done 文件的绝对路径。

        "s0_filter"          → .checkpoint/s0_filter.done
        "s2_modules/auth"    → .checkpoint/s2_modules/auth.done
        """
        # 防止路径穿越
        safe = name.replace("..", "").strip("/")
        return self._dir / (safe + ".done")
