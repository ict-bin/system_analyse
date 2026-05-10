"""
pipeline/s4_report.py — Stage 4: 最终报告

入: ctx.analysed_modules
    workspace/modules/*/module_report.md
出: ctx.final_report_path
    final_out_dir/final_report.md
    final_out_dir/modules/
    final_out_dir/modules.list
    final_out_dir/archive.zip（在 run_dir）

核心流程:
  Stage 4a: Judge 完整性检查
    → 缺失模块回 Stage 2+3 补做（_redo_missing）
  Stage 4b: Worker(step4_final_report.md) 生成总报告
    → Judge(step4_check_report.md) 评审
  后处理: 生成 modules.list, 归档 zip, 写 flag=1
"""
from __future__ import annotations

import re
import shutil
import time
from pathlib import Path

from .base import BaseStage
from .context import PipelineContext
from .evaluation import utc_now_iso
from .helpers import (
    run_agent_checked, parse_eval_md, check_voting,
    discover_modules, get_modules_root, load_prompt,
    archive_file, max_iter, pre_read_module,
    generate_modules_list, strip_target_prefix,
    StageError, PiFatalError,
)


FINAL_REPORT_CONTEXT_TOTAL_LIMIT = 70_000
FINAL_REPORT_CONTEXT_PER_MODULE_LIMIT = 900
FINAL_REPORT_CONTEXT_HIGH_RISK_LIMIT = 1_800


def _extract_report_meta(text: str) -> dict:
    """Extract lightweight metadata from a module_report.md for final aggregation."""
    head = text[:4_000]
    risk_level = "未知"
    risk_score = 0
    threat_count = 0

    m = re.search(r"RISK_LEVEL:\s*(.+?)\s*-->", head)
    if m:
        risk_level = m.group(1).strip()
    else:
        m = re.search(r"风险等级\s*[：:]\s*([^\n|]+)", head)
        if m:
            risk_level = m.group(1).strip()

    m = re.search(r"RISK_SCORE:\s*(\d+)", head)
    if m:
        risk_score = min(int(m.group(1)), 100)
    else:
        m = re.search(r"风险评分\s*[：:]\s*(\d+)", head)
        if m:
            risk_score = min(int(m.group(1)), 100)

    threat_count = len(re.findall(r"(?m)^#{2,4}\s+.*(?:威胁|漏洞|风险)", text))
    if threat_count == 0:
        threat_count = len(re.findall(r"(?:STRIDE 分类|风险等级|修复建议)", text))

    summary_lines = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#") or "风险" in line or "威胁" in line or "修复" in line:
            summary_lines.append(line)
        if len(summary_lines) >= 40:
            break

    return {
        "risk_level": risk_level,
        "risk_score": risk_score,
        "threat_count": threat_count,
        "summary": "\n".join(summary_lines),
    }


def _build_final_report_context(workspace: Path, limit: int = FINAL_REPORT_CONTEXT_TOTAL_LIMIT) -> str:
    """Build bounded final-report input so Stage 4b cannot read all module reports."""
    modules_root = get_modules_root(str(workspace))
    records = []
    total_files = 0

    for mod_name in discover_modules(str(workspace)):
        mod_dir = modules_root / mod_name
        files = []
        try:
            files = [l.strip() for l in (mod_dir / "files.list").read_text("utf-8").splitlines() if l.strip()]
        except OSError:
            files = []
        report_path = mod_dir / "module_report.md"
        try:
            report_text = report_path.read_text("utf-8", errors="replace")
        except OSError:
            report_text = ""
        meta = _extract_report_meta(report_text)
        total_files += len(files)
        records.append({
            "module": mod_name,
            "file_count": len(files),
            "report_path": str(report_path.relative_to(workspace)) if report_path.exists() else "",
            "report_text": report_text,
            **meta,
        })

    risk_order = {"严重": 0, "高": 1, "中": 2, "低": 3, "信息": 4, "未知": 5}
    records.sort(key=lambda r: (risk_order.get(str(r["risk_level"]).replace("🔴", "").replace("🟡", "").replace("🟢", "").strip(), 5), -int(r["risk_score"] or 0), r["module"]))

    counts = {"高": 0, "中": 0, "低": 0, "未知": 0}
    for r in records:
        level = str(r["risk_level"])
        if "高" in level or "严重" in level:
            counts["高"] += 1
        elif "中" in level:
            counts["中"] += 1
        elif "低" in level:
            counts["低"] += 1
        else:
            counts["未知"] += 1

    parts = [
        "# final_report.md 生成上下文（已截断汇总）",
        "",
        "注意：只能基于以下汇总生成最终报告，不要读取 modules/*/module_report.md 全文。",
        "必须写入 workspace 根目录下的 final_report.md。",
        "",
        "## 任务级统计",
        f"- 分析模块数: {len(records)}",
        f"- 总文件数: {total_files}",
        f"- 高风险模块数: {counts['高']}",
        f"- 中风险模块数: {counts['中']}",
        f"- 低风险模块数: {counts['低']}",
        f"- 未知/信息模块数: {counts['未知']}",
        "",
        "## 模块清单",
        "| 模块名 | 文件数 | 风险等级 | 风险评分 | 关键威胁数 |",
        "|--------|--------|----------|----------|------------|",
    ]
    for r in records:
        parts.append(
            f"| {r['module']} | {r['file_count']} | {r['risk_level']} | "
            f"{r['risk_score']} | {r['threat_count']} |"
        )
    parts.append("")
    parts.append("## 模块报告摘要")

    used = len("\n".join(parts))
    for r in records:
        if used >= limit:
            break
        risk_text = str(r["risk_level"])
        per_limit = FINAL_REPORT_CONTEXT_HIGH_RISK_LIMIT if ("高" in risk_text or "严重" in risk_text) else FINAL_REPORT_CONTEXT_PER_MODULE_LIMIT
        snippet_source = r["summary"] or r["report_text"]
        snippet = snippet_source[:per_limit]
        section = [
            "",
            f"### {r['module']}",
            f"- 文件数: {r['file_count']}",
            f"- 风险等级: {r['risk_level']}",
            f"- 风险评分: {r['risk_score']}",
            f"- 关键威胁数: {r['threat_count']}",
            "- 摘要片段:",
            "```markdown",
            snippet,
            "```",
        ]
        section_text = "\n".join(section)
        if used + len(section_text) > limit:
            remain = max(0, limit - used - 200)
            if remain <= 0:
                break
            section[-2] = snippet[:remain]
            section_text = "\n".join(section)
        parts.append(section_text)
        used += len(section_text)

    context = "\n".join(parts)
    try:
        (workspace / "final_report_context.md").write_text(context, encoding="utf-8")
    except OSError:
        pass
    return context


class CompletenessCheckStage(BaseStage):
    """Stage 4a: 完整性检查（缺失模块回 Stage 2+3 补做）"""

    stage_num = 4
    stage_name = "完整性检查"

    async def execute(self, ctx: PipelineContext) -> None:
        cfg = ctx.cfg
        workspace = ctx.workspace

        j_completeness_prompt = load_prompt(
            cfg.judges.system_prompt_dir, "step4_check_completeness")
        j_base = ctx.make_j_base()

        ctx.emit_event("stage", stage="4a")
        judge_results = []
        missing_modules: list[str] = []

        for j_idx, j_item in enumerate(ctx.j_cfgs):
            j_ar = await run_agent_checked(
                context=f"s4a-judge-j{j_idx}",
                prompt="运行 check_outputs.sh 检查所有模块是否都有 module_report.md。",
                model=ctx.jm("completeness", j_item),
                system_prompt=j_completeness_prompt,
                tools=cfg.judges.default_tools,
                cwd=str(workspace),
                **j_base,
            )
            ctx.tokens += j_ar.token_usage
            parsed = parse_eval_md(j_ar.output or "")
            judge_results.append(parsed)
            ctx.emit_event("judge_eval", stage="4a", judge_id=f"judge-{j_idx}",
                           passed=parsed["pass"], score=parsed["score"])
            archive_file(
                ctx.output_dir,
                f"s4a-j{j_idx}.md",
                f"Score: {parsed['score']}\nPass: {parsed['pass']}\n\n"
                f"{parsed['feedback']}\n\n---\n## Raw Output\n\n{j_ar.output[:3000]}",
            )
            if not parsed["pass"]:
                for m in re.findall(r'\u274c\s+(\S+)', j_ar.output or ""):
                    if m not in missing_modules:
                        missing_modules.append(m)

        s4a_pass = check_voting(judge_results, "all", ctx.j_count)

        if not s4a_pass and missing_modules:
            await self._redo_missing(ctx, missing_modules)

    # ── 补做缺失模块的 Stage 2+3 ──────────────────────────────────────────
    async def _redo_missing(self, ctx: PipelineContext, missing_modules: list[str]) -> None:
        cfg = ctx.cfg
        workspace = ctx.workspace
        mods_root = get_modules_root(str(workspace))

        ctx.emit_event("stage", stage="2-redo-s4", modules=missing_modules)

        s_cfg_refine = cfg.stages.refine
        s_cfg_analyse = cfg.stages.analyse
        w_sys_refine = load_prompt(cfg.workers.system_prompt_dir, "step2_refine")
        w_sys_analyse = load_prompt(cfg.workers.system_prompt_dir, "step3_analyse")
        j_sys_analyse = load_prompt(cfg.judges.system_prompt_dir, "step3_check_analyse")
        w_base = ctx.make_w_base()
        j_base = ctx.make_j_base()

        for mod_name in missing_modules:
            mod_dir = mods_root / mod_name
            if not mod_dir.exists() or not (mod_dir / "files.list").exists():
                continue

            # Stage 2 补做
            refine_session = str(ctx.sess_dir / f"refine-s4-{mod_name}.jsonl")
            ar = await run_agent_checked(
                context=f"s4-s2-redo-{mod_name}",
                model=ctx.wm("refine"),
                prompt=f"检查模块 `{mod_name}` 是否需要细分。",
                system_prompt=w_sys_refine,
                session_file=refine_session,
                **w_base,
            )
            ctx.tokens += ar.token_usage

            # Stage 3 补做（预读内容）
            loop = __import__("asyncio").get_event_loop()
            pre_content = await loop.run_in_executor(
                None, pre_read_module, cfg.target_dir, mod_dir
            )
            w_sys_s4 = w_sys_analyse.replace("{{PRE_READ_CONTENT}}", pre_content) \
                                     .replace("{{MODULE_NAME}}", mod_name)

            analyse_session = str(ctx.sess_dir / f"analyse-s4-{mod_name}.jsonl")
            feedback = ""
            for attempt in range(max_iter(s_cfg_analyse)):
                prompt_parts = [
                    f"将模块 `{mod_name}` 的分析报告写入 `modules/{mod_name}/module_report.md`。",
                    "文件内容已在 system prompt 中提供。",
                ]
                if feedback:
                    prompt_parts.append(f"\n\n{feedback}")
                ar = await run_agent_checked(
                    context=f"s4-s3-redo-{mod_name}-a{attempt+1}",
                    model=ctx.wm("analyse"),
                    prompt="\n".join(prompt_parts),
                    system_prompt=w_sys_s4,
                    tools=["write"],
                    session_file=analyse_session,
                    cwd=str(workspace),
                    cancel_event=ctx.cancel_event,
                    max_retries=1,
                    retry_delay=0,
                    pi_max_retries=cfg.pi_max_retries,
                    pi_retry_delay=cfg.pi_retry_delay,
                )
                ctx.tokens += ar.token_usage

                judge_results = []
                for j_idx, j_item in enumerate(ctx.j_cfgs):
                    j_ar = await run_agent_checked(
                        context=f"s4-s3-judge-{mod_name}-j{j_idx}-a{attempt+1}",
                        prompt=f"评审模块 `{mod_name}` 的分析报告。",
                        model=ctx.jm("analyse", j_item),
                        system_prompt=j_sys_analyse,
                        tools=cfg.judges.default_tools,
                        cwd=str(mod_dir) if mod_dir.exists() else str(workspace),
                        **j_base,
                    )
                    ctx.tokens += j_ar.token_usage
                    parsed = parse_eval_md(j_ar.output or "")
                    judge_results.append(parsed)
                    ctx.emit_event("judge_eval", stage="3-redo-s4", judge_id=f"judge-{j_idx}",
                                   module=mod_name, passed=parsed["pass"], score=parsed["score"])

                if check_voting(judge_results, s_cfg_analyse.pass_mode, ctx.j_count):
                    break
                fail_fb = "\n".join(
                    f"judge-{i}: {r['feedback'][:500]}"
                    for i, r in enumerate(judge_results) if not r["pass"])
                feedback = f"# 评审意见\n\n{fail_fb}"
            else:
                raise StageError(f"Stage 4a 补做模块 {mod_name} 分析未通过")


class FinalReportStage(BaseStage):
    """Stage 4b: 生成最终安全分析报告 + 输出归档"""

    stage_num = 5
    stage_name = "生成报告"

    async def execute(self, ctx: PipelineContext) -> None:
        cfg = ctx.cfg
        workspace = ctx.workspace
        final_out_dir = ctx.final_out_dir

        s_cfg = cfg.stages.final_check
        report_sys_prompt = load_prompt(cfg.workers.system_prompt_dir, "step4_final_report")
        report_sys_prompt = (
            report_sys_prompt
            + "\n\n# 强制约束\n"
            + "本次运行的用户提示会直接提供已截断的模块汇总上下文。"
            + "不要执行 ls/read，也不要逐个读取 modules/*/module_report.md；"
            + "只基于用户提示中的上下文生成 final_report.md。"
        )
        j_report_prompt = load_prompt(cfg.judges.system_prompt_dir, "step4_check_report")
        reflect_report = load_prompt(cfg.workers.system_prompt_dir, "reflect_report")
        report_session = str(ctx.sess_dir / "final_report.jsonl")
        w_base = ctx.make_w_base()
        report_w_base = {**w_base, "tools": ["write"]}
        j_base = ctx.make_j_base()

        feedback = ""
        for attempt in range(max_iter(s_cfg)):
            round_started = utc_now_iso()
            round_start_ts = time.time()
            ctx.emit_event("stage", stage="4b", attempt=attempt + 1)

            prompt_parts = [
                "基于以下已截断的模块汇总上下文，生成最终分析总报告 final_report.md。",
                "不要读取 module_report.md 全文；上下文已经包含生成总报告所需的模块清单、风险等级和关键摘要。",
                "",
                _build_final_report_context(workspace),
            ]
            if feedback:
                prompt_parts.append(f"\n\n{feedback}")

            ar = await run_agent_checked(
                context=f"s4b-report-a{attempt+1}",
                model=ctx.wm("report"),
                prompt="\n".join(prompt_parts),
                system_prompt=report_sys_prompt,
                session_file=report_session,
                **report_w_base,
            )
            ctx.tokens += ar.token_usage

            has_report = (workspace / "final_report.md").exists()
            ctx.emit_event("stage_result", stage="4b", has_report=has_report)

            judge_results = []
            judge_records = []
            for j_idx, j_item in enumerate(ctx.j_cfgs):
                j_model = ctx.jm("report", j_item)
                j_ar = await run_agent_checked(
                    context=f"s4b-judge-j{j_idx}-a{attempt+1}",
                    prompt="评审 final_report.md 的质量和完整性。",
                    model=j_model,
                    system_prompt=j_report_prompt,
                    tools=cfg.judges.default_tools,
                    cwd=str(workspace),
                    **j_base,
                )
                ctx.tokens += j_ar.token_usage
                parsed = parse_eval_md(j_ar.output or "")
                judge_results.append(parsed)
                judge_records.append({
                    "judge_id": f"judge-{j_idx}",
                    "model": j_model,
                    "score": parsed["score"],
                    "passed": parsed["pass"],
                    "feedback": parsed["feedback"],
                    "token_usage": j_ar.token_usage,
                })
                ctx.emit_event("judge_eval", stage="4b", judge_id=f"judge-{j_idx}",
                               passed=parsed["pass"], score=parsed["score"])
                archive_file(
                    ctx.output_dir,
                    f"s4b-a{attempt+1}-j{j_idx}.md",
                    f"Score: {parsed['score']}\nPass: {parsed['pass']}\n\n"
                    f"{parsed['feedback']}\n\n---\n## Raw Output\n\n{j_ar.output[:3000]}",
                )

            voted_pass = check_voting(judge_results, s_cfg.pass_mode, ctx.j_count)
            final_pass = voted_pass and attempt + 1 >= s_cfg.min_rounds
            max_reached = attempt + 1 >= max_iter(s_cfg)
            ctx.record_evaluation_round(
                module_name="__task__",
                stage="final_report",
                stage_round=attempt + 1,
                status="passed" if final_pass else "failed" if max_reached else "running",
                started_at=round_started,
                ended_at=utc_now_iso(),
                duration_ms=(time.time() - round_start_ts) * 1000,
                worker={
                    "model": ctx.wm("report"),
                    "session_file": report_session,
                    "token_usage": ar.token_usage,
                    "error": ar.error,
                },
                judges=judge_records,
                passed_by_vote=voted_pass,
                module_completed=final_pass and has_report,
                completion_reason="passed" if final_pass and has_report else "max_rounds_exceeded" if max_reached else "",
                needed_reflection=not final_pass,
                artifact_paths=[str(workspace / "final_report.md")],
                extra={"has_report": has_report},
            )
            if voted_pass:
                if attempt + 1 >= s_cfg.min_rounds:
                    break
                else:
                    ctx.emit_event("reflect", stage="4b", round=attempt + 1,
                                   min_rounds=s_cfg.min_rounds)
                    feedback = (
                        f"# 自查要求（第 {attempt+1} 轮，需至少 {s_cfg.min_rounds} 轮）\n\n"
                        + reflect_report
                    )
            else:
                fail_fb = "\n".join(
                    f"judge-{i}: {r['feedback'][:500]}"
                    for i, r in enumerate(judge_results) if not r["pass"])
                feedback = (f"# 评审意见（未通过）\n\n{fail_fb}"
                            "\n\n请根据意见修正 final_report.md。")
        else:
            raise StageError(f"Stage 4b 最终报告未通过，已达最大轮数 {s_cfg.max_rounds}")

        # ── 组装输出目录 ──────────────────────────────────────────────────────
        final_mods = discover_modules(str(workspace))

        # modules/ — 分类后的模块文件夹（files.list + module_report.md）
        modules_out = final_out_dir / "modules"
        if modules_out.exists():
            shutil.rmtree(str(modules_out))
        modules_out.mkdir(parents=True, exist_ok=True)
        for mod in final_mods:
            src = get_modules_root(str(workspace)) / mod
            dst = modules_out / mod
            if src.is_dir():
                shutil.copytree(str(src), str(dst))

        # final_report.md
        report_src = workspace / "final_report.md"
        report_dst = final_out_dir / "final_report.md"
        if report_src.exists():
            shutil.copy2(str(report_src), str(report_dst))
        ctx.final_report_path = str(report_dst)

        # modules.list — 按风险等级排序
        generate_modules_list(modules_out, final_out_dir / "modules.list")

        # 路径清洗 — 去除容器内绝对路径前缀
        strip_target_prefix(modules_out, cfg.target_dir)
        if report_dst.exists():
            strip_target_prefix(report_dst.parent, cfg.target_dir)
