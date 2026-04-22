"""
pipeline/s0_filter.py — Stage 0: 文件过滤 + 目录探索 + 预扫描

入: cfg.target_dir, cfg.analyse_targets, cfg.binary_arch
出: ctx.filtered_files, ctx.filter_count
    workspace/filtered_files.txt
    workspace/keywords.txt
    workspace/keyword_summary.txt
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path

from .base import BaseStage
from .context import PipelineContext
from .helpers import run_agent_checked, load_prompt


class FilterStage(BaseStage):
    """Stage 0: 文件类型过滤 → filtered_files.txt"""

    stage_num = 0
    stage_name = "文件过滤"

    async def execute(self, ctx: PipelineContext) -> None:
        cfg = ctx.cfg
        workspace = ctx.workspace
        task_id = ctx.task_id

        filter_script = "/opt/system_analyse/scripts/filter_files.sh"
        if not os.path.isfile(filter_script):
            return

        types_str = " ".join(cfg.analyse_targets)
        arch_str = " ".join(cfg.binary_arch)
        ctx.emit_event("stage", stage="filter", types=types_str, arch=arch_str)

        proc = await asyncio.create_subprocess_exec(
            "bash", filter_script, cfg.target_dir,
            str(workspace / "filtered_files.txt"),
            "--arch", arch_str,
            *cfg.analyse_targets,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate()

        filtered_path = workspace / "filtered_files.txt"
        if filtered_path.exists():
            lines = [l.strip() for l in filtered_path.read_text("utf-8").splitlines() if l.strip()]
            ctx.filtered_files = lines
            ctx.filter_count = len(lines)

        ctx.emit_event("stage_result", stage="filter",
                       types=cfg.analyse_targets, file_count=ctx.filter_count,
                       arch=arch_str)


class ExploreStage(BaseStage):
    """Stage 0.1: 目录探索（MiniMax） → keywords.txt"""

    stage_num = 0
    stage_name = "探索目录"

    async def execute(self, ctx: PipelineContext) -> None:
        cfg = ctx.cfg
        workspace = ctx.workspace

        w_prompt_dir = cfg.workers.system_prompt_dir
        explore_prompt = load_prompt(w_prompt_dir, "step1_explore")
        if not explore_prompt:
            return

        explore_model = cfg.workers.model_for("explore")
        ctx.emit_event("stage", stage="explore")
        ctx.emit_event("model", stage="explore", model=explore_model.split("/")[-1])

        ar = await run_agent_checked(
            context="explore",
            prompt=f"探索目标目录并生成关键词文件。目标：{ctx.task}",
            model=explore_model,
            tools=cfg.workers.default_tools,
            system_prompt=explore_prompt,
            cwd=str(workspace),
            thinking_level="off",
            session_file=None,
            cancel_event=ctx.cancel_event,
            max_retries=cfg.agent_max_retries,
            retry_delay=cfg.agent_retry_delay,
            pi_max_retries=cfg.pi_max_retries,
            pi_retry_delay=cfg.pi_retry_delay,
        )
        ctx.tokens += ar.token_usage
        ctx.emit_event("stage_result", stage="explore")


class PrescanStage(BaseStage):
    """Stage 0.2: 预扫描（bash/python） → keyword_summary.txt"""

    stage_num = 0
    stage_name = "预扫描"

    async def execute(self, ctx: PipelineContext) -> None:
        cfg = ctx.cfg
        workspace = ctx.workspace

        keywords_file = workspace / "keywords.txt"
        if not keywords_file.exists():
            return

        prescan_script = "/opt/system_analyse/scripts/prescan_files.py"
        if not os.path.isfile(prescan_script):
            prescan_script = "/opt/system_analyse/scripts/prescan_files.sh"
        if not os.path.isfile(prescan_script):
            return

        ctx.emit_event("stage", stage="prescan")
        cmd = (["python3", prescan_script] if prescan_script.endswith(".py")
               else ["bash", prescan_script])
        proc = await asyncio.create_subprocess_exec(
            *cmd, cfg.target_dir, str(workspace),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()

        summary_file = workspace / "keyword_summary.txt"
        summary_lines = 0
        prescan_summary = ""
        if summary_file.exists():
            prescan_summary = summary_file.read_text("utf-8")
            summary_lines = prescan_summary.count("\n")

        ctx.emit_event("stage_result", stage="prescan", summary_lines=summary_lines)
        # 把 prescan_summary 写入 context 供 Stage 1 使用
        ctx._prescan_summary = prescan_summary  # type: ignore[attr-defined]
