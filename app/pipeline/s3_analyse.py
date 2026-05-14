"""
pipeline/s3_analyse.py — Stage 3: 模块分析 (STRIDE)

入: ctx.refined_modules
    workspace/modules/*/files.list
出: ctx.analysed_modules
    workspace/modules/*/module_report.md
    ctx.modules_needing_reclassify → 触发 Stage 2 重做

核心流程:
  对每个模块并行运行（asyncio.Semaphore）:
    Python预读模块所有文件（pre_read_module）
    → Worker(step3_analyse.md, 占位符替换) → Judge(step3_check_analyse.md)
  重分类检测: judge 输出含 [需要重新分类] → 回 Stage 2 重做
  Stage 2/3 重做循环: 修正模块分类后重新分析

并发控制: asyncio.Semaphore(parallel_modules)
"""
from __future__ import annotations

import asyncio
import time
from pathlib import Path

from .base import BaseStage
from .context import PipelineContext
from .evaluation import utc_now_iso
from .helpers import (
    run_agent_with_stage_guard, parse_eval_md, check_voting,
    discover_modules, get_modules_root, load_prompt, build_granularity_hint,
    archive_file, max_iter, pre_read_module, pre_read_module_with_details,
    module_has_nonempty_files,
    load_details_for_module,
    StageError, PiFatalError, max_rounds_exceeded_treated_as_passed,
    enforce_filter_constraint,
)


class AnalyseStage(BaseStage):
    """Stage 3: 模块 STRIDE 分析（含 Stage 2/3 重分类回溯）"""

    stage_num = 3
    stage_name = "分析"

    async def execute(self, ctx: PipelineContext) -> None:
        cp = ctx.checkpoint
        cfg = ctx.cfg
        workspace = ctx.workspace

        # ── checkpoint: 整体已完成 ────────────────────────────────────────────
        if cp and cp.is_done("s3_analyse"):
            ctx.analysed_modules = [
                d.name for d in ctx.modules_root().iterdir()
                if d.is_dir() and (d / "module_report.md").exists() and module_has_nonempty_files(d)
            ]
            ctx.emit_event("log", level="info",
                           msg=f"[S3] 整体 checkpoint 已完成，跳过({len(ctx.analysed_modules)}个模块)")
            return

        w_sys_prompt = load_prompt(cfg, "step3_analyse", "workers")
        j_sys_prompt = load_prompt(cfg, "step3_check_analyse", "judges")
        reflect_prompt = load_prompt(cfg, "reflect_analyse", "workers")

        # ── 粒度约束注入（S3 Worker 和 Judge）───────────────────────────────────────────
        _gran_hint = build_granularity_hint(getattr(cfg, "module_granularity", "fine") or "fine")
        if _gran_hint:
            w_sys_prompt += _gran_hint
            j_sys_prompt += _gran_hint

        final_modules = discover_modules(str(workspace))
        ctx.modules_needing_reclassify = []
        s3_errors: list[BaseException] = []
        s3_sem = asyncio.Semaphore(max(1, cfg.parallel_modules))

        async def _analyse_one(mod_name: str) -> None:
            async with s3_sem:
                try:
                    await self._analyse_module(
                        ctx, mod_name,
                        w_sys_prompt, j_sys_prompt, reflect_prompt,
                    )
                except PiFatalError as e:
                    s3_errors.append(e)
                except StageError as e:
                    if ctx.continue_on_module_failure:
                        ctx.record_soft_module_failure(
                            stage="analyse",
                            module_name=mod_name,
                            error=str(e),
                            session_file=ctx.session_path("analyse", f"{mod_name}.jsonl"),
                            artifact_paths=[str(ctx.module_dir(mod_name) / "module_report.md")],
                            extra={"soft_failed": True},
                            record_round="已达最大轮数" not in str(e),
                        )
                    else:
                        s3_errors.append(e)

        results = await asyncio.gather(
            *[_analyse_one(m) for m in final_modules],
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, PiFatalError):
                raise r
        for r in results:
            if isinstance(r, StageError):
                raise r
            if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError):
                raise r
        if s3_errors:
            for e in s3_errors:
                if isinstance(e, PiFatalError):
                    raise e
            raise s3_errors[0]

        # ── Stage 2 → Stage 3 重分类回溯 ──────────────────────────────────────
        if ctx.modules_needing_reclassify:
            # redo 前清除相关模块的 S3 checkpoint（分类已改变，旧报告无效）
            if cp:
                cp.clear_stage_modules("s3", ctx.modules_needing_reclassify)
            await self._redo_s2_s3(
                ctx, final_modules,
                w_sys_prompt, j_sys_prompt, reflect_prompt,
            )

        ctx.analysed_modules = [
            d.name for d in ctx.modules_root().iterdir()
            if d.is_dir() and (d / "module_report.md").exists() and module_has_nonempty_files(d)
        ]

        # ── 写整体 checkpoint ────────────────────────────────────────────────
        if cp:
            cp.mark_done("s3_analyse", module_count=len(ctx.analysed_modules))

    # ── 单模块分析（W+J 多轮）────────────────────────────────────────────────
    async def _analyse_module(
        self,
        ctx: PipelineContext,
        mod_name: str,
        w_sys_prompt: str,
        j_sys_prompt: str,
        reflect_prompt: str,
        session_suffix: str = "",
    ) -> None:
        cfg = ctx.cfg
        workspace = ctx.workspace
        s_cfg = cfg.stages.analyse
        j_base = ctx.make_j_base()

        cp = ctx.checkpoint
        mod_dir = get_modules_root(str(workspace)) / mod_name
        analyse_session = ctx.session_path(
            "analyse" if not session_suffix else f"analyse{session_suffix}",
            f"{mod_name}.jsonl",
        )
        report_path = mod_dir / "module_report.md"
        has_files = module_has_nonempty_files(mod_dir)

        if report_path.exists() and not has_files:
            ctx.emit_event(
                "log",
                level="warn",
                msg=f"[清理 S3 脏模块] {mod_name} 存在旧 module_report.md，但 files.list 为空，移除陈旧报告并跳过",
            )
            try:
                report_path.unlink()
            except OSError:
                pass
            return

        if not has_files:
            ctx.emit_event(
                "log",
                level="warn",
                msg=f"[跳过 S3] {mod_name} files.list 为空，视为无效空壳模块",
            )
            return

        # ── 双重保护 checkpoint 跳过（checkpoint + 报告文件双重确认）──────────
        # checkpoint 存在 + 报告文件存在 → 安全跳过
        if cp and cp.is_done(f"s3_modules/{mod_name}") and report_path.exists():
            ctx.emit_event("log", level="info",
                           msg=f"[S3] {mod_name} checkpoint 已完成，跳过")
            return
        # checkpoint 存在但报告丢失 → 清除脏 checkpoint 重做
        if cp and cp.is_done(f"s3_modules/{mod_name}") and not report_path.exists():
            cp.clear(f"s3_modules/{mod_name}")
        # 报告存在但无 checkpoint → 旧版本遗留或写到一半 → 删除脏报告重做
        if report_path.exists() and not (cp and cp.is_done(f"s3_modules/{mod_name}")):
            try:
                report_path.unlink()
            except OSError:
                pass

        # 如果 module_report.md 已存在且 files.list 非空（旧逻辑保留作兜底）
        if report_path.exists():
            ctx.emit_event("log", level="info",
                           msg=f"[跳过 S3] {mod_name} module_report.md 已存在，跳过本轮分析")
            now = utc_now_iso()
            ctx.record_evaluation_round(
                module_name=mod_name,
                stage="analyse",
                stage_round=0,
                status="skipped",
                started_at=now,
                ended_at=now,
                duration_ms=0.0,
                worker={
                    "model": ctx.wm("analyse"),
                    "session_file": analyse_session,
                    "token_usage": None,
                    "error": None,
                },
                judges=[],
                passed_by_vote=True,
                module_completed=True,
                completion_reason="skipped_existing_report",
                artifact_paths=[str(mod_dir / "module_report.md")],
            )
            return

        # 预读所有文件（Python侧，无需 LLM tool call）
        # 优先复用 details/ JSON 避免重复 nm/readelf 系统调用
        # ctx.details_dir 由 orchestrator 初始化，永不为 None
        details_dir_opt = ctx.details_dir if ctx.details_dir.exists() else None
        loop = asyncio.get_event_loop()
        pre_read_content = await loop.run_in_executor(
            None, pre_read_module_with_details,
            cfg.target_dir, mod_dir, details_dir_opt
        )
        has_text = pre_read_content.startswith('__HAS_TEXT__\n')
        if has_text:
            pre_read_content = pre_read_content[len('__HAS_TEXT__\n'):]

        # 如果 mod_dir 中存在 analysis.md / SPLITTING_EVAL.md 等阶段产出文件，
        # 将其内容追加到 system prompt，并开启 read 工具让 worker 可继续查阅
        for extra_md in ["analysis.md", "SPLITTING_EVAL.md"]:
            extra_path = mod_dir / extra_md
            if extra_path.exists():
                try:
                    extra_content = extra_path.read_text(encoding="utf-8", errors="ignore")
                    if extra_content.strip():
                        pre_read_content += (
                            f"\n\n## 模块目录已有分析文件：{extra_md}\n\n{extra_content}"
                        )
                        has_text = True  # 开启 read 工具，以便 worker 进一步查阅
                except OSError:
                    pass

        w_sys = w_sys_prompt.replace("{{PRE_READ_CONTENT}}", pre_read_content) \
                            .replace("{{MODULE_NAME}}", mod_name)
        w_tools_s3 = ["read", "write"] if has_text else ["write"]

        feedback = ""
        for attempt in range(max_iter(s_cfg)):
            round_started = utc_now_iso()
            round_start_ts = time.time()
            ctx.emit_event("stage", stage=3, module=mod_name, attempt=attempt + 1)

            prompt_parts = [
                f"现在将模块 `{mod_name}` 的分析报告写入 `modules/{mod_name}/module_report.md`。",
                "文件内容已在 system prompt 中提供，直接写报告即可。",
            ]
            if feedback:
                prompt_parts.append("\n\n" + feedback)

            ar = await run_agent_with_stage_guard(
                ctx=ctx,
                stage="analyse",
                context=f"s3-analyse-{mod_name}-a{attempt+1}",
                heartbeat_payload_factory=lambda beat, module=mod_name, attempt_no=attempt + 1, session=analyse_session: {
                    "module": module,
                    "attempt": attempt_no,
                    "heartbeat": beat,
                    "session_file": session,
                },
                prompt="\n".join(prompt_parts),
                model=ctx.wm("analyse"),
                system_prompt=w_sys,
                tools=w_tools_s3,
                session_file=analyse_session,
                cwd=str(workspace),
                cancel_event=ctx.cancel_event,
                max_retries=1,
                retry_delay=0,
                pi_max_retries=cfg.pi_max_retries,
                pi_retry_delay=cfg.pi_retry_delay,
            )
            ctx.tokens += ar.token_usage
            ctx.emit_event("stage_result", stage=3, module=mod_name)

            judge_results = []
            judge_records = []
            for j_idx, j_item in enumerate(ctx.j_cfgs):
                j_model = ctx.jm("analyse", j_item)
                judge_session = ctx.session_path(
                    "judges",
                    "analyse" if not session_suffix else f"analyse{session_suffix}",
                    mod_name,
                    f"analyse-a{attempt + 1}-j{j_idx}.jsonl",
                )
                j_ar = await run_agent_with_stage_guard(
                    ctx=ctx,
                    stage="analyse",
                    context=f"s3-judge-{mod_name}-j{j_idx}-a{attempt+1}",
                    heartbeat_payload_factory=lambda beat, module=mod_name, attempt_no=attempt + 1, judge_id=j_idx, session=judge_session: {
                        "module": module,
                        "attempt": attempt_no,
                        "heartbeat": beat,
                        "judge_id": f"judge-{judge_id}",
                        "session_file": session,
                    },
                    prompt=f"评审模块 `{mod_name}` 的分析报告。",
                    model=j_model,
                    system_prompt=j_sys_prompt,
                    tools=cfg.judges.default_tools,
                    cwd=str(mod_dir) if mod_dir.exists() else str(workspace),
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
                ctx.emit_event("judge_eval", stage=3, judge_id=f"judge-{j_idx}",
                               module=mod_name, passed=parsed["pass"], score=parsed["score"])
                archive_file(
                    ctx.output_dir,
                    f"s3-{mod_name}-a{attempt+1}-j{j_idx}.md",
                    f"Score: {parsed['score']}\nPass: {parsed['pass']}\n\n"
                    f"{parsed['feedback']}\n\n---\n## Raw Output\n\n{j_ar.output[:3000]}",
                )

            # 重分类检测
            reclass_votes = sum(
                1 for r in judge_results if "[需要重新分类]" in r.get("feedback", "")
            )
            if reclass_votes and check_voting(
                [{"pass": True}] * reclass_votes + [{"pass": False}] * (ctx.j_count - reclass_votes),
                s_cfg.pass_mode, ctx.j_count,
            ):
                ctx.record_evaluation_round(
                    module_name=mod_name,
                    stage="analyse",
                    stage_round=attempt + 1,
                    status="failed",
                    started_at=round_started,
                    ended_at=utc_now_iso(),
                    duration_ms=(time.time() - round_start_ts) * 1000,
                    worker={
                        "model": ctx.wm("analyse"),
                        "session_file": analyse_session,
                        "token_usage": ar.token_usage,
                        "error": ar.error,
                    },
                    judges=judge_records,
                    passed_by_vote=False,
                    module_completed=False,
                    completion_reason="reclassify_required",
                    needed_reflection=True,
                    triggered_reclassify=True,
                    artifact_paths=[str(mod_dir / "module_report.md")],
                )
                ctx.emit_event("reclassify", module=mod_name)
                ctx.modules_needing_reclassify.append(mod_name)
                return  # 交给 _redo_s2_s3 处理

            voted_pass = check_voting(judge_results, s_cfg.pass_mode, ctx.j_count)
            final_pass = voted_pass and attempt + 1 >= s_cfg.min_rounds
            max_reached = attempt + 1 >= max_iter(s_cfg)
            report_exists = (mod_dir / "module_report.md").exists()
            forced_pass = max_reached and report_exists and max_rounds_exceeded_treated_as_passed(cfg)
            ctx.record_evaluation_round(
                module_name=mod_name,
                stage="analyse",
                stage_round=attempt + 1,
                status=(
                    "passed"
                    if (final_pass or forced_pass)
                    else "failed"
                    if max_reached
                    else "needs_reflection"
                    if voted_pass
                    else "needs_retry"
                ),
                started_at=round_started,
                ended_at=utc_now_iso(),
                duration_ms=(time.time() - round_start_ts) * 1000,
                worker={
                    "model": ctx.wm("analyse"),
                    "session_file": analyse_session,
                    "token_usage": ar.token_usage,
                    "error": ar.error,
                },
                judges=judge_records,
                passed_by_vote=voted_pass,
                module_completed=(final_pass or forced_pass) and report_exists,
                completion_reason=(
                    "passed"
                    if final_pass and report_exists
                    else "max_rounds_exceeded_treated_as_passed"
                    if forced_pass
                    else "max_rounds_exceeded"
                    if max_reached
                    else ""
                ),
                needed_reflection=not final_pass,
                artifact_paths=[str(mod_dir / "module_report.md")],
                extra={
                    "report_exists": report_exists,
                    "has_text_pre_read": has_text,
                },
            )
            if voted_pass:
                if attempt + 1 >= s_cfg.min_rounds:
                    # ── 写模块级 checkpoint ─────────────────────────────
                    if cp:
                        cp.mark_done(f"s3_modules/{mod_name}")
                    return
                else:
                    ctx.emit_event("reflect", stage=3, module=mod_name, round=attempt + 1)
                    feedback = (
                        f"# 自查要求（第 {attempt+1} 轮，需至少 {s_cfg.min_rounds} 轮）\n\n"
                        + reflect_prompt
                    )
                    jfb = "\n".join(
                        f"judge-{i}: {r['feedback'][:500]}"
                        for i, r in enumerate(judge_results))
                    feedback += "\n\n## Judge 上轮意见\n\n" + jfb
            else:
                fail_fb = "\n".join(
                    f"judge-{i}: {r['feedback'][:500]}"
                    for i, r in enumerate(judge_results) if not r["pass"])
                feedback = "# 评审意见（未通过）\n\n" + fail_fb + "\n\n请根据意见修正分析。"
            if forced_pass:
                if cp:
                    cp.mark_done(f"s3_modules/{mod_name}", forced=True)
                return

        if mod_name not in ctx.modules_needing_reclassify:
            raise StageError(f"Stage 3 模块 {mod_name} 分析未通过，已达最大轮数 {s_cfg.max_rounds}")

    # ── Stage 2/3 重分类回溯 ─────────────────────────────────────────────────
    async def _redo_s2_s3(
        self,
        ctx: PipelineContext,
        original_modules: list[str],
        w_sys_analyse: str,
        j_sys_analyse: str,
        reflect_analyse: str,
    ) -> None:
        cfg = ctx.cfg
        workspace = ctx.workspace
        mods_root = get_modules_root(str(workspace))

        to_reclassify = ctx.modules_needing_reclassify[:]
        ctx.emit_event("stage", stage="2-redo", modules=to_reclassify)

        s_cfg_refine = cfg.stages.refine
        w_sys_refine = load_prompt(cfg, "step2_refine", "workers")
        j_sys_refine = load_prompt(cfg, "step2_check_refine", "judges")
        reflect_refine = load_prompt(cfg, "reflect_refine", "workers")
        # 2-redo 中注入安全维度约束，防止重分类超出过滤范围
        from .s1_classify import _build_security_focus_section  # noqa: PLC0415
        _sec_cats = getattr(cfg, "security_focus_categories", ["all"])
        _sec_focus_hint = _build_security_focus_section(_sec_cats)
        # 2-redo 中同样注入粒度约束，与 S2 保持一致
        _gran_hint = build_granularity_hint(getattr(cfg, "module_granularity", "fine") or "fine")
        if _gran_hint:
            w_sys_refine += _gran_hint
            j_sys_refine += _gran_hint
        w_base = ctx.make_w_base()
        j_base = ctx.make_j_base()

        for mod_name in to_reclassify:
            mod_dir = mods_root / mod_name
            if not mod_dir.exists():
                continue

            refine_session = ctx.session_path("refine-redo", f"{mod_name}.jsonl")
            feedback = "# 重分类要求\n\nStage 3 分析发现该模块分类不合理，需要重新细分。"
            if _sec_focus_hint:
                feedback += _sec_focus_hint  # 注入安全维度约束

            # ── 为 redo 的 refine Worker 预加载 details/ 摘要（节省 token）─────────────
            _details_dir = ctx.details_dir  # 由 orchestrator 初始化，永不为 None
            if _details_dir.exists() and mod_dir.exists():
                _flist = [l.strip() for l in (mod_dir / "files.list").read_text("utf-8",errors="replace").splitlines() if l.strip()] if (mod_dir / "files.list").exists() else []
                if _flist:
                    _det_summary, _unclear = load_details_for_module(_details_dir, _flist, cfg.target_dir)
                    if _det_summary:
                        feedback += (
                            "\n\n## 模块文件摘要（来自 details/ 预处理）\n\n"
                            + _det_summary[:3000]
                            + ("\n（…摘要已截断）" if len(_det_summary) > 3000 else "")
                        )
                        ctx.emit_event("log", level="info",
                                       msg=(f"[S3-2redo] {mod_name}: 注入 details 摘要"
                                            f"（{len(_flist)}个文件，{len(_unclear)}个需补充）"))

            for attempt in range(max_iter(s_cfg_refine)):
                ctx.emit_event("stage", stage="2-redo", module=mod_name, attempt=attempt + 1)

                ar = await run_agent_with_stage_guard(
                    ctx=ctx,
                    stage="2-redo",
                    context=f"s2-redo-{mod_name}-a{attempt+1}",
                    heartbeat_payload_factory=lambda beat, module=mod_name, attempt_no=attempt + 1, session=refine_session: {
                        "module": module,
                        "attempt": attempt_no,
                        "heartbeat": beat,
                        "session_file": session,
                    },
                    model=ctx.wm("refine"),
                    prompt=f"重新检查模块 `{mod_name}` 并细分。\n\n{feedback}",
                    system_prompt=w_sys_refine,
                    session_file=refine_session,
                    **w_base,
                )
                ctx.tokens += ar.token_usage

                # enforce 在 Judge 前运行，Judge 看到清洁数据
                if ctx.filtered_files:
                    _rm = enforce_filter_constraint(workspace, set(ctx.filtered_files))
                    if _rm:
                        ctx.emit_event("log", level="warn",
                                       msg=f"[S3-2redo过滤] 补先移除 {_rm} 个越界条目")

                judge_results = []
                eval_cwd = str(mod_dir) if mod_dir.exists() else str(workspace)
                for j_idx, j_item in enumerate(ctx.j_cfgs):
                    judge_session = ctx.session_path(
                        "judges",
                        "refine-redo",
                        mod_name,
                        f"refine-redo-a{attempt + 1}-j{j_idx}.jsonl",
                    )
                    j_ar = await run_agent_with_stage_guard(
                        ctx=ctx,
                        stage="2-redo",
                        context=f"s2-redo-judge-{mod_name}-j{j_idx}-a{attempt+1}",
                        heartbeat_payload_factory=lambda beat, module=mod_name, attempt_no=attempt + 1, judge_id=j_idx, session=judge_session: {
                            "module": module,
                            "attempt": attempt_no,
                            "heartbeat": beat,
                            "judge_id": f"judge-{judge_id}",
                            "session_file": session,
                        },
                        prompt=f"评审模块 `{mod_name}` 的重新细分。",
                        model=ctx.jm("refine", j_item),
                        system_prompt=j_sys_refine,
                        tools=cfg.judges.default_tools,
                        cwd=eval_cwd,
                        session_file=judge_session,
                        **j_base,
                    )
                    ctx.tokens += j_ar.token_usage
                    parsed = parse_eval_md(j_ar.output or "")
                    judge_results.append(parsed)
                    ctx.emit_event("judge_eval", stage="2-redo", judge_id=f"judge-{j_idx}",
                                   module=mod_name, passed=parsed["pass"], score=parsed["score"])

                voted_pass = check_voting(judge_results, s_cfg_refine.pass_mode, ctx.j_count)
                if voted_pass:
                    if attempt + 1 >= s_cfg_refine.min_rounds:
                        break
                    feedback = "# 自查要求\n\n" + reflect_refine
                    jfb = "\n".join(
                        f"judge-{i}: {r['feedback'][:500]}"
                        for i, r in enumerate(judge_results))
                    feedback += "\n\n## Judge 上轮意见\n\n" + jfb
                else:
                    fail_fb = "\n".join(
                        f"judge-{i}: {r['feedback'][:500]}"
                        for i, r in enumerate(judge_results) if not r["pass"])
                    feedback = f"# 评审意见\n\n{fail_fb}"
            else:
                raise StageError(f"Stage 2-redo 模块 {mod_name} 重分类未通过")

        # Stage 3-redo: 只处理新子模块 + 原始模块（files.list 非空）
        new_mods = discover_modules(str(workspace))
        redo_analyse = []
        for m in new_mods:
            if m not in original_modules:
                redo_analyse.append(m)
            elif m in to_reclassify:
                flist = mods_root / m / "files.list"
                if flist.exists() and flist.stat().st_size > 0:
                    redo_analyse.append(m)

        if redo_analyse:
            ctx.emit_event("stage", stage="3-redo", modules=redo_analyse)
            for mod_name in redo_analyse:
                await self._analyse_module(
                    ctx, mod_name,
                    w_sys_analyse, j_sys_analyse, reflect_analyse,
                    session_suffix="-redo",
                )
