"""
pipeline/s3_analyse.py — Stage 3: 模块分析 (STRIDE)

入: ctx.refined_modules
    workspace/modules/*/files.list
出: ctx.analysed_modules
    workspace/modules/*/module_report.md
    ctx.modules_needing_reclassify → 触发 Stage 2 重做

核心流程:
  对每个模块并行运行（asyncio.Semaphore）:
    从 details/ 加载文件摘要行（load_details_for_module，按需 read）
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
    discover_modules, get_modules_root, load_prompt, load_granularity_prompt, build_granularity_hint,
    archive_file, max_iter, write_judge_feedback,
    module_has_nonempty_files,
    load_details_for_module,
    StageError, PiFatalError, max_rounds_exceeded_treated_as_passed,
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

        granularity = getattr(cfg, "module_granularity", "fine") or "fine"
        w_sys_prompt = load_granularity_prompt(cfg, "step3_analyse", granularity, "workers")
        j_sys_prompt = load_granularity_prompt(cfg, "step3_check_analyse", granularity, "judges")
        reflect_prompt = load_granularity_prompt(cfg, "reflect_analyse", granularity, "workers")

        # 兼容旧 prompt：若粒度专用提示词未完全内嵌，再追加统一提示
        _gran_hint = build_granularity_hint(granularity)
        if _gran_hint and _gran_hint not in w_sys_prompt:
            w_sys_prompt += _gran_hint
        if _gran_hint and _gran_hint not in j_sys_prompt:
            j_sys_prompt += _gran_hint

        final_modules = discover_modules(str(workspace))

        # ── 构建模块依赖图（S3 风险排序用）──
        if ctx.module_dependency_graph is None:
            ctx.module_dependency_graph = _build_module_dep_graph(workspace)

        ctx.redo_modules = []
        ctx.redo_feedback = {}
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

        ctx.analysed_modules = [
            d.name for d in ctx.modules_root().iterdir()
            if d.is_dir() and (d / "module_report.md").exists() and module_has_nonempty_files(d)
        ]

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
            try:
                import json as _json
                _cp_data = _json.loads(cp._resolve(f"s3_modules/{mod_name}").read_text(encoding="utf-8"))
                _score = _cp_data.get("extra", {}).get("score")
                _score_str = f" (score={_score})" if _score is not None else ""
            except Exception:
                _score_str = ""
            ctx.emit_event("log", level="info",
                           msg=f"[S3] {mod_name} checkpoint 已完成，跳过{_score_str}")
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

        # 使用 details/ 摘要行（按需 read）替代全量符号预展开
        # 每文件一行：路径 | 类型 | 功能摘要 | 关键符号(前5个) | 建议子模块
        # Worker 通过 read details/<path>.json 按需获取完整符号表
        flist: list[str] = []
        flist_path = mod_dir / "files.list"
        if flist_path.exists():
            flist = [
                l.strip()
                for l in flist_path.read_text("utf-8", errors="replace").splitlines()
                if l.strip()
            ]
        pre_read_content, _unclear_files = load_details_for_module(
            ctx.details_dir, flist, cfg.target_dir
        )
        if _unclear_files:
            ctx.emit_event("log", level="info",
                           msg=f"[S3] {mod_name}: {len(_unclear_files)}/{len(flist)} 个文件 details 不足，"
                               f"Worker 可用 read target/<path> 补充")

        # 如果 mod_dir 中存在 analysis.md / SPLITTING_EVAL.md 等阶段产出文件，追加到摘要
        for extra_md in ["analysis.md", "SPLITTING_EVAL.md"]:
            extra_path = mod_dir / extra_md
            if extra_path.exists():
                try:
                    extra_content = extra_path.read_text(encoding="utf-8", errors="ignore")
                    if extra_content.strip():
                        pre_read_content += (
                            f"\n\n## 模块目录已有分析文件：{extra_md}\n\n{extra_content}"
                        )
                except OSError:
                    pass

        w_sys = w_sys_prompt.replace("{{PRE_READ_CONTENT}}", pre_read_content) \
                            .replace("{{MODULE_NAME}}", mod_name)
        # read 工具始终开放：Worker 需要通过 read details/<path>.json 获取完整符号表
        w_tools_s3 = ["read", "write", "bash"]

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
                    cwd=str(workspace),  # workspace根, 避免双重modules/路径
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

            # 重分类检测 — 保存 S3 Judge 反馈，交由 Orchestrator 循环 S2
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
                # ★ 不做 S2 redo — 模块边界由 S2 Judge 最终裁定
                # 重分类意见记录到 timeline，但不触发重分类循环
                ctx.emit_event("log", level="warn",
                               msg=f"[S3] {mod_name} Judge 建议重分类，但不触发 S2 redo")
                return

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
                    "has_text_pre_read": False,
                },
            )
            if voted_pass:
                if attempt + 1 >= s_cfg.min_rounds:
                    # ── 写模块级 checkpoint ─────────────────────────────
                    if cp:
                        _avg = int(sum(r["score"] for r in judge_results) / max(len(judge_results), 1)) if judge_results else 0
                        cp.mark_done(f"s3_modules/{mod_name}", score=_avg, attempts=attempt + 1)
                    return
                else:
                    ctx.emit_event("reflect", stage=3, module=mod_name, round=attempt + 1)
                    feedback = (
                        f"# 自查要求（第 {attempt+1} 轮，需至少 {s_cfg.min_rounds} 轮）\n\n"
                        + reflect_prompt
                    )
                    jfb = "\n".join(
                        f"judge-{i}: {r['feedback']}"
                        for i, r in enumerate(judge_results))
                    feedback += "\n\n## Judge 上轮意见\n\n" + jfb
            else:
                fb_rel = write_judge_feedback(
                    workspace, "s3_analyse", mod_name, attempt + 1, judge_results)
                ctx.emit_event("log", level="info",
                               msg=f"[S3] judge意见已写入 {fb_rel}")
                feedback = f"评审未通过，完整意见请 read {fb_rel} ，阅后修正 modules/{mod_name}/module_report.md"
                # ★ 不删除报告 — Worker 基于 Judge 反馈直接修改
            if forced_pass:
                if cp:
                    cp.mark_done(f"s3_modules/{mod_name}", forced=True)
                return

        raise StageError(f"Stage 3 模块 {mod_name} 分析未通过，已达最大轮数 {s_cfg.max_rounds}")


# ── 模块依赖图构建 ──────────────────────────────────────────────────────
