"""
pipeline/s0_unknown_checker.py — Stage 0.1: UNKNOWN 文件类型识别

入: ctx.unknown_files（TypeClassifyStage 产出）
    workspace/unknown_files.txt
出: workspace/file_catalog.json（更新 UNKNOWN 条目的 type 字段）
    ctx.file_catalog（已更新）

当 unknown_files 为空或 enable_unknown_checker=False 时直接跳过。
无 Judge（辅助信息，识别错误影响可控）。
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

from .base import BaseStage
from .context import PipelineContext
from .helpers import run_agent_checked, load_prompt


class UnknownCheckerStage(BaseStage):
    """Stage 0.1: UNKNOWN 文件类型识别（LLM agent，单轮，无 Judge）"""

    stage_num = 0
    stage_name = "未知文件类型识别"

    async def execute(self, ctx: PipelineContext) -> None:
        cp = ctx.checkpoint
        cfg = ctx.cfg
        workspace = ctx.workspace

        # ── 跳过条件 ─────────────────────────────────────────────────────
        enable = getattr(cfg, "enable_unknown_checker", True)
        if not enable:
            ctx.emit_event("log", level="info",
                           msg="[S0-UnknownChecker] enable_unknown_checker=False，跳过")
            if cp:
                cp.mark_done("s0_unknown_checker", skipped="disabled")
            return

        # ── 从 ctx 或磁盘获取 unknown 文件列表 ───────────────────────────
        unknown_files = list(ctx.unknown_files)
        if not unknown_files:
            unknown_path = workspace / "unknown_files.txt"
            if unknown_path.exists():
                unknown_files = [
                    l.strip() for l in unknown_path.read_text(encoding="utf-8").splitlines()
                    if l.strip()
                ]

        if not unknown_files:
            ctx.emit_event("log", level="info",
                           msg="[S0-UnknownChecker] 无 UNKNOWN 文件，跳过")
            if cp:
                cp.mark_done("s0_unknown_checker", skipped="no_unknown_files")
            return

        # ── checkpoint 跳过 ───────────────────────────────────────────────
        if cp and cp.is_done("s0_unknown_checker"):
            ctx.emit_event("log", level="info",
                           msg=f"[S0-UnknownChecker] checkpoint 已完成，跳过"
                               f"（{len(unknown_files)} 个 UNKNOWN 文件）")
            return

        checker_prompt = load_prompt(cfg, "step0_unknown_checker", "workers")
        if not checker_prompt:
            ctx.emit_event("log", level="warn",
                           msg="[S0-UnknownChecker] prompt 未找到，跳过")
            if cp:
                cp.mark_done("s0_unknown_checker", skipped="no_prompt")
            return

        ctx.emit_event("stage", stage="unknown_checker",
                       unknown_count=len(unknown_files))
        ctx.emit_event("log", level="info",
                       msg=f"[S0-UnknownChecker] 分析 {len(unknown_files)} 个 UNKNOWN 文件")

        # ── 分批处理（每批最多 50 个）─────────────────────────────────────
        BATCH = 50
        resolved: list[dict] = []

        for batch_start in range(0, len(unknown_files), BATCH):
            batch = unknown_files[batch_start: batch_start + BATCH]
            file_list_md = "\n".join(f"- `target/{f}`" for f in batch)
            prompt = (
                f"请识别以下 {len(batch)} 个文件的类型（第 {batch_start // BATCH + 1} 批）：\n\n"
                f"工作目录：`{workspace}`\n\n"
                f"{file_list_md}\n\n"
                f"对每个文件运行 `file` 命令并分析结果，然后输出 JSON 数组。"
            )
            session = str(ctx.sess_dir / f"unknown-checker-batch{batch_start // BATCH + 1}.jsonl")
            ar = await run_agent_checked(
                context=f"s0-unknown-checker-batch{batch_start // BATCH + 1}",
                prompt=prompt,
                model=cfg.workers.model_for("explore"),
                system_prompt=checker_prompt,
                tools=["bash", "read"],
                cwd=str(workspace),
                thinking_level="off",
                session_file=session,
                cancel_event=ctx.cancel_event,
                max_retries=cfg.agent_max_retries,
                retry_delay=cfg.agent_retry_delay,
                pi_max_retries=cfg.pi_max_retries,
                pi_retry_delay=cfg.pi_retry_delay,
            )
            ctx.tokens += ar.token_usage

            # ── 解析 JSON 输出 ─────────────────────────────────────────
            if ar.output:
                raw = re.sub(r"<result>.*?</result>", "", ar.output, flags=re.DOTALL)
                # 提取 JSON 数组
                m = re.search(r"\[.*?\]", raw, flags=re.DOTALL)
                if m:
                    try:
                        batch_results = json.loads(m.group())
                        if isinstance(batch_results, list):
                            resolved.extend(batch_results)
                    except json.JSONDecodeError:
                        ctx.emit_event("log", level="warn",
                                       msg=f"[S0-UnknownChecker] batch {batch_start // BATCH + 1} JSON 解析失败")

        # ── 将识别结果合并回 file_catalog.json ───────────────────────────
        if resolved:
            catalog_path = workspace / "file_catalog.json"
            catalog: dict = ctx.file_catalog
            if not catalog and catalog_path.exists():
                try:
                    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
                except Exception:
                    catalog = {"files": []}

            # 建立 path → index 映射
            path_map = {f["path"]: i for i, f in enumerate(catalog.get("files", []))}
            updated = 0
            for r in resolved:
                path = r.get("path", "")
                if path in path_map:
                    idx = path_map[path]
                    old_type = catalog["files"][idx].get("type", "UNKNOWN")
                    new_type = r.get("type", "UNKNOWN")
                    if new_type and new_type != "UNKNOWN":
                        catalog["files"][idx]["type"] = new_type
                        if "arch" in r:
                            catalog["files"][idx]["arch"] = r["arch"]
                        catalog["files"][idx]["unknown_checker_confidence"] = r.get("confidence", "")
                        updated += 1
                        ctx.emit_event("log", level="info",
                                       msg=f"[S0-UnknownChecker] {path}: {old_type} → {new_type}")

            # 重新统计 unknown_count 和 type_summary
            from collections import Counter
            type_counter = Counter(f["type"] for f in catalog.get("files", []))
            catalog["unknown_count"] = type_counter.get("UNKNOWN", 0) + type_counter.get("TEXT_UNKNOWN", 0)
            catalog["type_summary"] = dict(type_counter.most_common())

            catalog_path.write_text(
                json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            ctx.file_catalog = catalog
            ctx.emit_event("log", level="info",
                           msg=f"[S0-UnknownChecker] 更新 {updated}/{len(resolved)} 个文件类型")

        ctx.emit_event("stage_result", stage="unknown_checker",
                       resolved=len(resolved),
                       remaining_unknown=ctx.file_catalog.get("unknown_count", 0))
        if cp:
            cp.mark_done("s0_unknown_checker",
                         resolved=len(resolved),
                         remaining_unknown=ctx.file_catalog.get("unknown_count", 0))
