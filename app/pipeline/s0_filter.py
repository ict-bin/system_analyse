"""
pipeline/s0_filter.py — Stage 0: 文件过滤 + 目录探索 + 预扫描

入: cfg.target_dir, cfg.analyse_targets, cfg.binary_arch
出: ctx.filtered_files, ctx.filter_count
    workspace/filtered_files.txt
    workspace/keywords.txt
    workspace/keyword_summary.txt
    ctx.prescan_summary
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from .base import BaseStage
from .context import PipelineContext
from .filter_engine import (
    load_script_filter_outputs,
    normalize_filter_engine,
    run_agent_filter_engine,
)
from .helpers import run_agent_with_stage_guard, load_prompt


def _restore_filtered_files(ctx: PipelineContext) -> None:
    """从磁盘恢复 ctx.filtered_files / ctx.filter_count（checkpoint跳过时使用）。"""
    workspace = ctx.workspace
    for fname in ("filtered_files.txt", ".filtered_backup.txt"):
        p = workspace / fname
        if p.exists():
            lines = [l.strip() for l in p.read_text("utf-8", errors="replace").splitlines() if l.strip()]
            if lines:
                ctx.filtered_files = lines
                ctx.filter_count = len(lines)
                return


def _restore_prescan_summary(ctx: PipelineContext) -> None:
    """从磁盘恢复 ctx.prescan_summary（checkpoint跳过时使用）。"""
    workspace = ctx.workspace
    summary_file = workspace / "keyword_summary.txt"
    if summary_file.exists():
        ctx.prescan_summary = summary_file.read_text("utf-8", errors="replace")


def _count_input_files(target_dir: str) -> int:
    path = Path(target_dir)
    if not path.exists():
        return 0
    if path.is_file():
        return 1
    return sum(1 for item in path.rglob("*") if item.is_file())


class FilterStage(BaseStage):
    """Stage 0: 文件类型过滤 → filtered_files.txt"""

    stage_num = 0
    stage_name = "文件过滤"

    async def execute(self, ctx: PipelineContext) -> None:
        cp = ctx.checkpoint
        cfg = ctx.cfg
        workspace = ctx.workspace
        ctx.selected_filter_engine = normalize_filter_engine(getattr(cfg, "filter_engine", "script"))
        ctx.effective_filter_engine = "script"

        # ── checkpoint 跳过 ───────────────────────────────────────────────
        if cp and cp.is_done("s0_filter"):
            _restore_filtered_files(ctx)
            ctx.emit_event("log", level="info",
                           msg=f"[S0-Filter] checkpoint已完成，跳过（{ctx.filter_count}个文件）")
            return

        filter_script = "/app/scripts/filter_files.sh"
        if not os.path.isfile(filter_script):
            if cp:
                cp.mark_done("s0_filter", file_count=0, skipped="no_script")
            return

        types_str = " ".join(cfg.analyse_targets)
        arch_str = " ".join(cfg.binary_arch)
        ctx.emit_event(
            "stage",
            stage="filter",
            types=types_str,
            arch=arch_str,
            security_focus_categories=list(cfg.security_focus_categories),
            module_granularity=cfg.module_granularity,
            selected_filter_engine=ctx.selected_filter_engine,
        )
        ctx.emit_event(
            "log",
            level="info",
            msg=(
                "安全过滤配置："
                f"analyse_targets={cfg.analyse_targets}, "
                f"binary_arch={cfg.binary_arch}, "
                f"security_focus_categories={cfg.security_focus_categories}, "
                f"module_granularity={cfg.module_granularity}, "
                f"filter_engine={ctx.selected_filter_engine}"
            ),
        )
        ctx.total_input_file_count = await asyncio.to_thread(_count_input_files, cfg.target_dir)

        if ctx.selected_filter_engine == "agent":
            try:
                stats = await run_agent_filter_engine(ctx)
                (workspace / "filter_summary.json").write_text(
                    json.dumps({
                        "total_input_file_count": ctx.total_input_file_count,
                        "accepted_input_file_count": stats.file_count,
                        "selected_filter_engine": ctx.selected_filter_engine,
                        "effective_filter_engine": ctx.effective_filter_engine,
                        "fallback_reason": ctx.filter_fallback_reason or None,
                    }, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                ctx.emit_event(
                    "stage_result",
                    stage="filter-engine",
                    status="passed",
                    total_input_file_count=ctx.total_input_file_count,
                    accepted_input_file_count=stats.file_count,
                    selected_engine=ctx.selected_filter_engine,
                    effective_engine=ctx.effective_filter_engine,
                    file_count=stats.file_count,
                    module_count=stats.module_count,
                    batch_count=stats.batch_count,
                )
                return
            except Exception as exc:
                ctx.filter_fallback_reason = str(exc)
                ctx.effective_filter_engine = "script"
                ctx.emit_event(
                    "stage",
                    stage="filter-fallback",
                    selected_engine=ctx.selected_filter_engine,
                    effective_engine="script",
                    reason=str(exc),
                )
                ctx.emit_event(
                    "log",
                    level="warn",
                    msg=f"智能体过滤引擎失败，已自动回退脚本引擎: {exc}",
                )

        task_tmp = ctx.task_tmp
        extra_env = {**os.environ, "TMPDIR": str(task_tmp)}
        if cfg.skip_path_patterns:
            extra_env["SECFLOW_SA_SKIP_PATH_PATTERNS"] = " ".join(cfg.skip_path_patterns)
        proc = await asyncio.create_subprocess_exec(
            "bash", filter_script, cfg.target_dir,
            str(workspace / "filtered_files.txt"),
            "--arch", arch_str,
            *cfg.analyse_targets,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=extra_env,
        )
        stdout, stderr_bytes = await proc.communicate()
        _out = (stdout or b"").decode("utf-8", errors="replace").strip()
        _err = (stderr_bytes or b"").decode("utf-8", errors="replace").strip()
        _cli = (_out + ("\n" + _err if _err else "")).strip()
        if _cli:
            ctx.emit_event("cli_output", stage="filter", text=_cli[:3000])

        load_script_filter_outputs(workspace, ctx)
        (workspace / "filter_summary.json").write_text(
            json.dumps({
                "total_input_file_count": ctx.total_input_file_count,
                "accepted_input_file_count": ctx.filter_count,
                "selected_filter_engine": ctx.selected_filter_engine,
                "effective_filter_engine": ctx.effective_filter_engine,
                "fallback_reason": ctx.filter_fallback_reason or None,
            }, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        ctx.emit_event("stage_result", stage="filter",
                       types=cfg.analyse_targets, file_count=ctx.filter_count,
                       total_input_file_count=ctx.total_input_file_count,
                       accepted_input_file_count=ctx.filter_count,
                       arch=arch_str,
                       security_focus_categories=list(cfg.security_focus_categories),
                       module_granularity=cfg.module_granularity,
                       selected_engine=ctx.selected_filter_engine,
                       effective_engine=ctx.effective_filter_engine,
                       fallback_reason=ctx.filter_fallback_reason)

        # ── 写 checkpoint ────────────────────────────────────────────────────
        if cp:
            cp.mark_done("s0_filter", file_count=ctx.filter_count)


class ExploreStage(BaseStage):
    """Stage 0.1: 目录探索 → keywords.txt（聚焦关键词生成，禁止安全分析）"""

    stage_num = 0
    stage_name = "探索目录"

    async def execute(self, ctx: PipelineContext) -> None:
        cp = ctx.checkpoint
        cfg = ctx.cfg
        workspace = ctx.workspace

        # ── checkpoint 跳过 ───────────────────────────────────────────────
        if cp and cp.is_done("s0_explore"):
            ctx.emit_event("log", level="info", msg="[S0-Explore] checkpoint已完成，跳过")
            return

        explore_prompt = load_prompt(cfg, "step1_explore", "workers")
        if not explore_prompt:
            if cp:
                cp.mark_done("s0_explore", skipped="no_prompt")
            return

        explore_model = ctx.wm("explore")
        ctx.emit_event("stage", stage="explore")
        ctx.emit_event("model", stage="explore", model=explore_model.split("/")[-1])

        filtered_path = workspace / "filtered_files.txt"
        if filtered_path.exists():
            efc = sum(1 for l in filtered_path.read_text("utf-8").splitlines() if l.strip())
            explore_scope = (
                f"\n\n⚠️ **文件范围（不得修改）**: `{workspace}/filtered_files.txt` "
                f"已含 {efc} 个待分析文件。"
                f"请从该文件采样文件路径来了解目录结构，"
                f"**不要重新扫描目标目录，不要修改该文件**。"
            )
        else:
            explore_scope = (
                f"\n\n目标目录：`{cfg.target_dir}`（通过工作目录下 `target/` 符号链接访问）"
            )

        explore_user_prompt = (
            f"探索目标软件包目录结构，生成功能分类关键词并写入 keywords.txt。\n"
            f"工作目录: `{workspace}`\n"
            f"**⚠️ 任务：仅通过文件名/路径了解功能组成，输出分类关键词列表。**\n"
            f"**⚠️ 禁止做安全分析或漏洞挖掘。禁止读取文件内容（二进制或文本）。**"
            + explore_scope
        )

        explore_session = str(ctx.sess_dir / "explore.jsonl")
        ar = await run_agent_with_stage_guard(
            ctx=ctx,
            stage="explore",
            context="explore",
            heartbeat_payload_factory=lambda beat: {
                "heartbeat": beat,
                "session_file": explore_session,
            },
            prompt=explore_user_prompt,
            model=explore_model,
            system_prompt=explore_prompt,
            session_file=explore_session,
            cwd=str(workspace),
            tools=cfg.workers.default_tools,
            env={**os.environ, "TMPDIR": str(ctx.task_tmp), "HOME": str(workspace)},
            thinking_level="off",
            cancel_event=ctx.cancel_event,
            max_retries=cfg.agent_max_retries,
            retry_delay=cfg.agent_retry_delay,
            pi_max_retries=cfg.pi_max_retries,
            pi_retry_delay=cfg.pi_retry_delay,
        )
        ctx.tokens += ar.token_usage
        if ar.output:
            ctx.emit_event("agent_output", stage="explore", output=ar.output[-1200:])

        # 还原 filtered_files.txt（防止 explore agent 意外覆盖）
        filter_backup = workspace / ".filtered_backup.txt"
        if filter_backup.exists():
            (workspace / "filtered_files.txt").write_text(
                filter_backup.read_text("utf-8"), encoding="utf-8")

        ctx.emit_event("stage_result", stage="explore")

        # ── 写 checkpoint ────────────────────────────────────────────────────
        if cp:
            cp.mark_done("s0_explore")


class PrescanStage(BaseStage):
    """Stage 0.2: 预扫描（bash/python） → keyword_summary.txt → ctx.prescan_summary"""

    stage_num = 0
    stage_name = "预扫描"

    async def execute(self, ctx: PipelineContext) -> None:
        cp = ctx.checkpoint
        cfg = ctx.cfg
        workspace = ctx.workspace

        # ── checkpoint 跳过 ───────────────────────────────────────────────
        if cp and cp.is_done("s0_prescan"):
            _restore_prescan_summary(ctx)
            ctx.emit_event("log", level="info", msg="[S0-Prescan] checkpoint已完成，跳过")
            return

        keywords_file = workspace / "keywords.txt"
        if not keywords_file.exists():
            if cp:
                cp.mark_done("s0_prescan", skipped="no_keywords")
            return

        prescan_script = "/app/scripts/prescan_files.py"
        if not os.path.isfile(prescan_script):
            prescan_script = "/app/scripts/prescan_files.sh"
        if not os.path.isfile(prescan_script):
            if cp:
                cp.mark_done("s0_prescan", skipped="no_script")
            return

        ctx.emit_event("stage", stage="prescan")
        cmd = (["python3", prescan_script] if prescan_script.endswith(".py")
               else ["bash", prescan_script])
        proc = await asyncio.create_subprocess_exec(
            *cmd, cfg.target_dir, str(workspace),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "TMPDIR": str(ctx.task_tmp)},
        )
        stdout, stderr_bytes = await proc.communicate()
        _pout = (stdout or b"").decode("utf-8", errors="replace").strip()
        _perr = (stderr_bytes or b"").decode("utf-8", errors="replace").strip()
        _pcli = (_pout + ("\n" + _perr if _perr else "")).strip()
        if _pcli:
            ctx.emit_event("cli_output", stage="prescan", text=_pcli[:3000])

        summary_file = workspace / "keyword_summary.txt"
        if summary_file.exists():
            ctx.prescan_summary = summary_file.read_text("utf-8")

        ctx.emit_event("stage_result", stage="prescan",
                       summary_lines=ctx.prescan_summary.count("\n"))
        # ── 写 checkpoint ────────────────────────────────────────────────────
        if cp:
            cp.mark_done("s0_prescan")
