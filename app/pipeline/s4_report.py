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

import asyncio
import re
import shutil
import time
from pathlib import Path

from .base import BaseStage
from .context import PipelineContext
from .evaluation import utc_now_iso
from .helpers import (
    run_agent_with_stage_guard, parse_eval_md, check_voting,
    discover_modules, get_modules_root, load_prompt, load_granularity_prompt,
    archive_file, max_iter, pre_read_module, pre_read_module_with_details,
    generate_modules_list, strip_target_prefix, write_judge_feedback,
    StageError, PiFatalError, max_rounds_exceeded_treated_as_passed,
    enforce_filter_constraint,
)


class CompletenessCheckStage(BaseStage):
    """Stage 4a: 完整性检查（缺失模块回 Stage 2+3 补做）"""

    stage_num = 4
    stage_name = "完整性检查"

    async def execute(self, ctx: PipelineContext) -> None:
        cp = ctx.checkpoint
        cfg = ctx.cfg
        workspace = ctx.workspace

        # ── checkpoint 跳过 ──────────────────────────────────────────────────
        if cp and cp.is_done("s4_completeness"):
            ctx.emit_event("log", level="info",
                           msg="[S4a-Completeness] checkpoint已完成，跳过")
            return
        if not getattr(cfg, "enable_final_check", False):
            ctx.emit_event("stage", stage="4a", skipped=True, reason="disabled")
            return

        j_completeness_prompt = load_prompt(cfg, "step4_check_completeness", "judges")
        j_base = ctx.make_j_base()

        ctx.emit_event("stage", stage="4a")
        judge_results = []
        missing_modules: list[str] = []

        for j_idx, j_item in enumerate(ctx.j_cfgs):
            judge_session = ctx.session_path(
                "judges",
                "report-completeness",
                f"s4a-j{j_idx}.jsonl",
            )
            j_ar = await run_agent_with_stage_guard(
                ctx=ctx,
                stage="4a",
                context=f"s4a-judge-j{j_idx}",
                heartbeat_payload_factory=lambda beat, judge_id=j_idx, session=judge_session: {
                    "heartbeat": beat,
                    "judge_id": f"judge-{judge_id}",
                    "session_file": session,
                },
                prompt="运行 check_outputs.sh 检查所有模块是否都有 module_report.md。",
                model=ctx.jm("completeness", j_item),
                system_prompt=j_completeness_prompt,
                tools=cfg.judges.default_tools,
                cwd=str(workspace),
                session_file=judge_session,
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

        # ── 写 checkpoint ────────────────────────────────────────────────────────
        if cp := ctx.checkpoint:
            cp.mark_done("s4_completeness")

    # ── 补做缺失模块的 Stage 2+3 ──────────────────────────────────────────
    async def _redo_missing(self, ctx: PipelineContext, missing_modules: list[str]) -> None:
        cfg = ctx.cfg
        workspace = ctx.workspace
        mods_root = get_modules_root(str(workspace))

        ctx.emit_event("stage", stage="2-redo-s4", modules=missing_modules)

        s_cfg_refine = cfg.stages.refine
        s_cfg_analyse = cfg.stages.analyse
        granularity = getattr(cfg, "module_granularity", "fine") or "fine"
        w_sys_refine = load_granularity_prompt(cfg, "step2_refine", granularity, "workers")
        w_sys_analyse = load_granularity_prompt(cfg, "step3_analyse", granularity, "workers")
        j_sys_analyse = load_granularity_prompt(cfg, "step3_check_analyse", granularity, "judges")
        w_base = ctx.make_w_base()
        j_base = ctx.make_j_base()

        for mod_name in missing_modules:
            mod_dir = mods_root / mod_name
            if not mod_dir.exists() or not (mod_dir / "files.list").exists():
                continue
            try:
                # Stage 2 补做
                refine_session = ctx.session_path("refine-s4", f"{mod_name}.jsonl")
                ar = await run_agent_with_stage_guard(
                    ctx=ctx,
                    stage="2-redo-s4",
                    context=f"s4-s2-redo-{mod_name}",
                    heartbeat_payload_factory=lambda beat, module=mod_name, session=refine_session: {
                        "module": module,
                        "heartbeat": beat,
                        "session_file": session,
                    },
                    model=ctx.wm("refine"),
                    prompt=f"检查模块 `{mod_name}` 是否需要细分。",
                    system_prompt=w_sys_refine,
                    session_file=refine_session,
                    **w_base,
                )
                ctx.tokens += ar.token_usage

                # Stage 3 补做（预读内容，优先复用 details/ JSON）
                loop = __import__("asyncio").get_event_loop()
                _details_dir_opt = ctx.details_dir if ctx.details_dir.exists() else None
                pre_content = await loop.run_in_executor(
                    None, pre_read_module_with_details,
                    cfg.target_dir, mod_dir, _details_dir_opt
                )
                w_sys_s4 = w_sys_analyse.replace("{{PRE_READ_CONTENT}}", pre_content) \
                                         .replace("{{MODULE_NAME}}", mod_name)

                analyse_session = ctx.session_path("analyse-s4", f"{mod_name}.jsonl")
                feedback = ""
                for attempt in range(max_iter(s_cfg_analyse)):
                    prompt_parts = [
                        f"将模块 `{mod_name}` 的分析报告写入 `modules/{mod_name}/module_report.md`。",
                        "文件内容已在 system prompt 中提供。",
                    ]
                    if feedback:
                        prompt_parts.append(f"\n\n{feedback}")
                    ar = await run_agent_with_stage_guard(
                        ctx=ctx,
                        stage="3-redo-s4",
                        context=f"s4-s3-redo-{mod_name}-a{attempt+1}",
                        heartbeat_payload_factory=lambda beat, module=mod_name, attempt_no=attempt + 1, session=analyse_session: {
                            "module": module,
                            "attempt": attempt_no,
                            "heartbeat": beat,
                            "session_file": session,
                        },
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

                    if ctx.filtered_files:
                        _rm = enforce_filter_constraint(workspace, set(ctx.filtered_files))
                        if _rm:
                            ctx.emit_event("log", level="warn",
                                           msg=f"[S4-redo过滤] 补先移除 {_rm} 个越界条目")

                    judge_results = []
                    for j_idx, j_item in enumerate(ctx.j_cfgs):
                        judge_session = ctx.session_path(
                            "judges",
                            "analyse-s4",
                            mod_name,
                            f"analyse-s4-a{attempt + 1}-j{j_idx}.jsonl",
                        )
                        j_ar = await run_agent_with_stage_guard(
                            ctx=ctx,
                            stage="3-redo-s4",
                            context=f"s4-s3-judge-{mod_name}-j{j_idx}-a{attempt+1}",
                            heartbeat_payload_factory=lambda beat, module=mod_name, attempt_no=attempt + 1, judge_id=j_idx, session=judge_session: {
                                "module": module,
                                "attempt": attempt_no,
                                "heartbeat": beat,
                                "judge_id": f"judge-{judge_id}",
                                "session_file": session,
                            },
                            prompt=f"评审模块 `{mod_name}` 的分析报告。",
                            model=ctx.jm("analyse", j_item),
                            system_prompt=j_sys_analyse,
                            tools=cfg.judges.default_tools,
                            cwd=str(workspace),  # workspace根, 避免双重modules/路径
                            session_file=judge_session,
                            **j_base,
                        )
                        ctx.tokens += j_ar.token_usage
                        parsed = parse_eval_md(j_ar.output or "")
                        judge_results.append(parsed)
                        ctx.emit_event("judge_eval", stage="3-redo-s4", judge_id=f"judge-{j_idx}",
                                       module=mod_name, passed=parsed["pass"], score=parsed["score"])

                    if check_voting(judge_results, s_cfg_analyse.pass_mode, ctx.j_count):
                        break
                    fb_redo = write_judge_feedback(
                        workspace, "s3_analyse", mod_name, attempt + 1, judge_results)
                    feedback = f"评审未通过，完整意见请 read {fb_redo}"
                else:
                    raise StageError(f"Stage 4a 补做模块 {mod_name} 分析未通过")
            except PiFatalError:
                raise
            except StageError as exc:
                if ctx.continue_on_module_failure:
                    ctx.record_soft_module_failure(
                        stage="3-redo-s4",
                        module_name=mod_name,
                        error=str(exc),
                        session_file=ctx.session_path("analyse-s4", f"{mod_name}.jsonl"),
                        artifact_paths=[str(mod_dir / "module_report.md")],
                        extra={"soft_failed": True, "from_stage": "s4_completeness_redo"},
                    )
                    continue
                raise


class FinalReportStage(BaseStage):
    """Stage 4b: 生成最终安全分析报告 + 输出归档"""

    stage_num = 5
    stage_name = "生成报告"

    async def execute(self, ctx: PipelineContext) -> None:
        cp = ctx.checkpoint
        cfg = ctx.cfg
        workspace = ctx.workspace
        final_out_dir = ctx.final_out_dir

        # ── checkpoint 跳过（checkpoint + final_report.md 双重确认） ────────────
        if cp and cp.is_done("s4_report"):
            report_dst = final_out_dir / "final_report.md"
            if report_dst.exists():
                ctx.final_report_path = str(report_dst)
                ctx.emit_event("log", level="info",
                               msg="[S4b-Report] checkpoint已完成，跳过")
                # 直接进入后处理（并不跳过。级处理将在后面执行）
                # 不 return，需要继续进行论文归档等后处理
                return

        s_cfg = cfg.stages.final_check
        granularity = getattr(cfg, "module_granularity", "fine") or "fine"
        report_sys_prompt = load_prompt(cfg, "step4_final_report", "workers")
        j_report_prompt = load_prompt(cfg, "step4_check_report", "judges")
        reflect_report = load_prompt(cfg, "reflect_report", "workers")
        report_session = ctx.session_path("final_report.jsonl")
        w_base = ctx.make_w_base()
        j_base = ctx.make_j_base()

        feedback = ""
        for attempt in range(max_iter(s_cfg)):
            round_started = utc_now_iso()
            round_start_ts = time.time()
            ctx.emit_event("stage", stage="4b", attempt=attempt + 1)

            prompt_parts = [
                "读取所有模块的 module_report.md，生成最终分析总报告 final_report.md。"
            ]
            if feedback:
                prompt_parts.append(f"\n\n{feedback}")

            ar = await run_agent_with_stage_guard(
                ctx=ctx,
                stage="4b",
                context=f"s4b-report-a{attempt+1}",
                heartbeat_payload_factory=lambda beat, attempt_no=attempt + 1, session=report_session: {
                    "attempt": attempt_no,
                    "heartbeat": beat,
                    "session_file": session,
                },
                model=ctx.wm("report"),
                prompt="\n".join(prompt_parts),
                system_prompt=report_sys_prompt,
                session_file=report_session,
                **w_base,
            )
            ctx.tokens += ar.token_usage

            has_report = (workspace / "final_report.md").exists()
            ctx.emit_event("stage_result", stage="4b", has_report=has_report)

            # ── 并行 per-module 验收 judge ─────────────────────────────────────
            if has_report and ctx.j_cfgs:
                _final_mods = discover_modules(str(workspace))
                _j_sys = load_granularity_prompt(cfg, "step3_check_analyse", granularity, "judges")
                _sem_pm = asyncio.Semaphore(cfg.parallel_modules)
                _pm_failed: list[str] = []
                _pm_lock = asyncio.Lock()

                async def _check_one_module_pm(mod_name_pm: str) -> None:
                    async with _sem_pm:
                        jpm_sess = ctx.session_path(
                            "judges", "final_check", mod_name_pm,
                            f"final-check-a{attempt + 1}-j0.jsonl",
                        )
                        try:
                            jpm_ar = await run_agent_with_stage_guard(
                                ctx=ctx, stage="4b-check",
                                context=f"s4b-check-{mod_name_pm}",
                                heartbeat_payload_factory=lambda beat, m=mod_name_pm: {
                                    "module": m, "heartbeat": beat},
                                prompt=f"最终验收：评审模块 `{mod_name_pm}` 的分析报告完整性。",
                                model=ctx.jm("analyse", ctx.j_cfgs[0]),
                                system_prompt=_j_sys,
                                tools=cfg.judges.default_tools,
                                cwd=str(workspace),
                                session_file=jpm_sess,
                                cancel_event=ctx.cancel_event,
                                max_retries=cfg.agent_max_retries,
                                retry_delay=cfg.agent_retry_delay,
                                pi_max_retries=cfg.pi_max_retries,
                                pi_retry_delay=cfg.pi_retry_delay,
                            )
                            ctx.tokens += jpm_ar.token_usage
                            _pm_parsed = parse_eval_md(jpm_ar.output or "")
                            if not _pm_parsed["pass"]:
                                async with _pm_lock:
                                    _pm_failed.append(mod_name_pm)
                                write_judge_feedback(
                                    workspace, "s4_completeness", mod_name_pm,
                                    attempt + 1, [_pm_parsed])
                        except Exception as _exc_pm:
                            ctx.emit_event("log", level="warn",
                                           msg=f"[S4b-check] {mod_name_pm} 异常: {_exc_pm}")

                await asyncio.gather(*[_check_one_module_pm(m) for m in _final_mods])

                if _pm_failed:
                    _sum = workspace / "judge_output" / "s4_completeness" / "module_check_summary.md"
                    _sum.parent.mkdir(parents=True, exist_ok=True)
                    _sum.write_text(
                        f"# 最终验收失败模块（第 {attempt + 1} 轮）\n\n"
                        + "\n".join(f"- {m}" for m in _pm_failed),
                        encoding="utf-8",
                    )
                    ctx.emit_event("log", level="warn",
                                   msg=f"[S4b-check] {len(_pm_failed)} 个模块未通过，详见 judge_output/s4_completeness/")

            # ── 全局 Judge ───────────────────────────────────────────────────────
            judge_results = []
            judge_records = []
            for j_idx, j_item in enumerate(ctx.j_cfgs):
                j_model = ctx.jm("report", j_item)
                judge_session = ctx.session_path(
                    "judges",
                    "final_report",
                    f"final-report-a{attempt + 1}-j{j_idx}.jsonl",
                )
                j_ar = await run_agent_with_stage_guard(
                    ctx=ctx,
                    stage="4b",
                    context=f"s4b-judge-j{j_idx}-a{attempt+1}",
                    heartbeat_payload_factory=lambda beat, attempt_no=attempt + 1, judge_id=j_idx, session=judge_session: {
                        "attempt": attempt_no,
                        "heartbeat": beat,
                        "judge_id": f"judge-{judge_id}",
                        "session_file": session,
                    },
                    prompt="评审 final_report.md 的质量和完整性。",
                    model=j_model,
                    system_prompt=j_report_prompt,
                    tools=cfg.judges.default_tools,
                    cwd=str(workspace),
                    session_file=judge_session,
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
                    "session_file": judge_session,
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
            forced_pass = max_reached and has_report and max_rounds_exceeded_treated_as_passed(cfg)
            ctx.record_evaluation_round(
                module_name="__task__",
                stage="final_report",
                stage_round=attempt + 1,
                status="passed" if (final_pass or forced_pass) else "failed" if max_reached else "running",
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
                module_completed=(final_pass or forced_pass) and has_report,
                completion_reason=(
                    "passed"
                    if final_pass and has_report
                    else "max_rounds_exceeded_treated_as_passed"
                    if forced_pass
                    else "max_rounds_exceeded"
                    if max_reached
                    else ""
                ),
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
                fb4 = write_judge_feedback(
                    workspace, "s4_report", None, attempt + 1, judge_results)
                ctx.emit_event("log", level="info",
                               msg=f"[S4b] judge意见已写入 {fb4}")
                feedback = f"评审未通过，完整意见请 read {fb4} ，阅后修正 final_report.md"
            if forced_pass:
                break
        else:
            raise StageError(f"Stage 4b 最终报告未通过，已达最大轮数 {s_cfg.max_rounds}")

        # ── 写 checkpoint（在归档前确认报告已生成） ─────────────────────────────
        if cp and (workspace / "final_report.md").exists():
            cp.mark_done("s4_report")

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
