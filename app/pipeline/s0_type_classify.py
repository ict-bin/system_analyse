"""
pipeline/s0_type_classify.py — Stage 0: 文件类型分类

入: ctx.filtered_files / workspace/filtered_files.txt
出: workspace/file_catalog.json
    workspace/unknown_files.txt（若有 UNKNOWN 类型）
    ctx.file_catalog
    ctx.unknown_files
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from .base import BaseStage
from .context import PipelineContext


class TypeClassifyStage(BaseStage):
    """Stage 0: 文件类型分类 → file_catalog.json（纯 Python，无 LLM）"""

    stage_num = 0
    stage_name = "文件类型分类"

    async def execute(self, ctx: PipelineContext) -> None:
        cp = ctx.checkpoint
        workspace = ctx.workspace

        # ── checkpoint 跳过 ───────────────────────────────────────────────
        catalog_path = workspace / "file_catalog.json"
        if cp and cp.is_done("s0_type_classify") and catalog_path.exists():
            try:
                ctx.file_catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
                unknown_path = workspace / "unknown_files.txt"
                if unknown_path.exists():
                    ctx.unknown_files = [
                        l.strip() for l in unknown_path.read_text(encoding="utf-8").splitlines()
                        if l.strip()
                    ]
                ctx.emit_event("log", level="info",
                               msg=f"[S0-TypeClassify] checkpoint 已完成，跳过"
                                   f"（{ctx.file_catalog.get('filtered_count', 0)} 个文件，"
                                   f"{len(ctx.unknown_files)} 个 UNKNOWN）")
            except Exception:
                cp.clear("s0_type_classify")
            else:
                return

        # ── filtered_files.txt 必须存在 ──────────────────────────────────
        ff = workspace / "filtered_files.txt"
        if not ff.exists():
            ctx.emit_event("log", level="warn",
                           msg="[S0-TypeClassify] filtered_files.txt 不存在，跳过")
            if cp:
                cp.mark_done("s0_type_classify", skipped="no_filtered_files")
            return

        classify_script = "/app/scripts/classify_files.py"
        if not os.path.isfile(classify_script):
            # 开发环境兼容
            classify_script = str(
                Path(__file__).parent.parent.parent / "scripts" / "classify_files.py"
            )

        if not os.path.isfile(classify_script):
            ctx.emit_event("log", level="warn",
                           msg="[S0-TypeClassify] classify_files.py 未找到，跳过")
            if cp:
                cp.mark_done("s0_type_classify", skipped="no_script")
            return

        ctx.emit_event("stage", stage="type_classify")
        proc = await asyncio.create_subprocess_exec(
            "python3", classify_script,
            ctx.cfg.target_dir, str(workspace),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "TMPDIR": str(ctx.task_tmp)},
        )
        stdout, stderr = await proc.communicate()
        out = (stdout or b"").decode("utf-8", errors="replace").strip()
        err = (stderr or b"").decode("utf-8", errors="replace").strip()
        combined = (out + ("\n" + err if err else "")).strip()
        if combined:
            ctx.emit_event("cli_output", stage="type_classify", text=combined[:2000])

        # ── 加载结果 ──────────────────────────────────────────────────────
        if catalog_path.exists():
            try:
                ctx.file_catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
            except Exception as e:
                ctx.emit_event("log", level="warn",
                               msg=f"[S0-TypeClassify] file_catalog.json 解析失败: {e}")

        unknown_path = workspace / "unknown_files.txt"
        if unknown_path.exists():
            ctx.unknown_files = [
                l.strip() for l in unknown_path.read_text(encoding="utf-8").splitlines()
                if l.strip()
            ]

        ctx.emit_event("stage_result", stage="type_classify",
                       total=ctx.file_catalog.get("filtered_count", 0),
                       unknown=len(ctx.unknown_files),
                       type_summary=ctx.file_catalog.get("type_summary", {}))

        if cp:
            cp.mark_done("s0_type_classify",
                         file_count=ctx.file_catalog.get("filtered_count", 0),
                         unknown_count=len(ctx.unknown_files))
