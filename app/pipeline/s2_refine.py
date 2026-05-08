"""
pipeline/s2_refine.py — Stage 2: 细分类

入: ctx.classified_modules
    workspace/modules/*/files.list
出: ctx.refined_modules
    workspace/modules/*/files.list（可能重组）
    workspace/.s2_snapshots/*.snapshot

核心流程:
  对每个模块并行运行（asyncio.Queue + parallel_modules 个 worker）:
    (文件数>阈值时) 子Worker批量预读文件摘要
    → Worker(step2_refine.md) → Judge(step2_check_refine.md) 多轮
  Stage 2 后全局检查: filtered_files.txt vs 所有 files.list
  遗漏文件用补分类(step2_reclassify.md)

并发控制: asyncio.Queue + parallel_modules 个 worker
"""
from __future__ import annotations

import asyncio
import shutil
import time
from pathlib import Path

from .base import BaseStage
from .context import PipelineContext
from .evaluation import utc_now_iso
from .helpers import (
    run_agent_checked, parse_eval_md, check_voting,
    discover_modules, get_modules_root, load_prompt,
    archive_file, max_iter,
    SUB_WORKER_THRESHOLD, collect_file_summaries,
    StageError, PiFatalError,
)


class RefineStage(BaseStage):
    """Stage 2: 细分类（含子Worker摘要生成 + 全局补分类）"""

    stage_num = 2
    stage_name = "细分"

    def _reset(self) -> None:
        """每次 execute 前重置并发状态。"""
        self._refined: set[str] = set()
        self._in_progress: set[str] = set()
        self._errors: list[BaseException] = []
        self._queue: asyncio.Queue = asyncio.Queue()
        self._ctx: PipelineContext | None = None

    # ── 主入口 ─────────────────────────────────────────────────────────────
    async def execute(self, ctx: PipelineContext) -> None:
        self._reset()
        self._ctx = ctx
        cfg = ctx.cfg
        workspace = ctx.workspace

        for mod in discover_modules(str(workspace)):
            await self._queue.put(mod)

        parallel = max(1, cfg.parallel_modules)
        workers = [asyncio.create_task(self._worker()) for _ in range(parallel)]
        await self._queue.join()
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

        if self._errors:
            for e in self._errors:
                if isinstance(e, PiFatalError):
                    raise e
            raise self._errors[0]

        await self._global_completeness_check()
        ctx.refined_modules = discover_modules(str(workspace))

    # ── Queue worker ───────────────────────────────────────────────────────
    async def _worker(self) -> None:
        while True:
            mod_name = await self._queue.get()
            try:
                if mod_name not in self._refined:
                    await self._refine_one(mod_name)
            except (StageError, PiFatalError) as e:
                self._errors.append(e)
            finally:
                self._queue.task_done()

    # ── 单模块细分 ─────────────────────────────────────────────────────────
    async def _refine_one(self, mod_name: str) -> None:
        ctx = self._ctx
        cfg = ctx.cfg
        workspace = ctx.workspace
        s_cfg = cfg.stages.refine
        w_base = ctx.make_w_base()
        j_base = ctx.make_j_base()

        mod_dir = get_modules_root(str(workspace)) / mod_name
        if not (mod_dir / "files.list").exists():
            return

        fc = sum(1 for l in (mod_dir / "files.list").read_text("utf-8").splitlines() if l.strip())
        if fc == 0:
            ctx.emit_event("log", level="warn",
                           msg=f"[跳过] {mod_name} 过滤后 0 个文件，自动移除空模块")
            shutil.rmtree(str(mod_dir), ignore_errors=True)
            return

        refine_session = str(ctx.sess_dir / f"refine-{mod_name}.jsonl")

        # 快照（拆分前保存，重试时不覆盖）
        snapshots_dir = workspace / ".s2_snapshots"
        snapshots_dir.mkdir(exist_ok=True)
        snapshot_path = snapshots_dir / f"{mod_name}.snapshot"
        if not snapshot_path.exists():
            shutil.copy2(str(mod_dir / "files.list"), str(snapshot_path))

        # 文件数超过阈值时，先用子 Worker 收集文件摘要
        sub_prompt = load_prompt(cfg.workers.system_prompt_dir, "step2_sub_read")
        file_summary = ""
        if sub_prompt and fc > SUB_WORKER_THRESHOLD:
            file_summary = await collect_file_summaries(
                ctx=ctx,
                mod_name=mod_name,
                mod_dir=mod_dir,
                sub_prompt_template=sub_prompt,
                parallel=cfg.parallel_sub_workers,
                sub_model=cfg.workers.model_for("sub_read"),
                target_dir=cfg.target_dir,
            )

        w_sys_prompt = load_prompt(cfg.workers.system_prompt_dir, "step2_refine")
        j_sys_prompt = load_prompt(cfg.judges.system_prompt_dir, "step2_check_refine")
        reflect_prompt = load_prompt(cfg.workers.system_prompt_dir, "reflect_refine")

        feedback = ""
        for attempt in range(max_iter(s_cfg)):
            round_started = utc_now_iso()
            round_start_ts = time.time()
            ctx.emit_event("stage", stage=2, module=mod_name, attempt=attempt + 1)

            mods_before = set(discover_modules(str(workspace)))
            prompt_parts = [f"检查模块 `{mod_name}` 是否需要细分。"]
            if file_summary:
                prompt_parts.append("\n\n## 文件摘要（子 Worker 已分析）\n\n" + file_summary)
            if feedback:
                prompt_parts.append("\n\n" + feedback)

            ar = await run_agent_checked(
                context=f"s2-refine-{mod_name}-a{attempt+1}",
                prompt="\n".join(prompt_parts),
                model=ctx.wm("refine"),
                system_prompt=w_sys_prompt,
                session_file=refine_session,
                **w_base,
            )
            ctx.tokens += ar.token_usage

            mods_after = set(discover_modules(str(workspace)))
            new_ones = sorted(
                (mods_after - mods_before) - self._refined - self._in_progress
            )
            was_split = (mod_name not in mods_after
                         and bool(mods_after - mods_before - self._refined - self._in_progress))
            ctx.emit_event("stage_result", stage=2, module=mod_name,
                           split=was_split, new_modules=new_ones)

            # ── Judge ──────────────────────────────────────────────────
            judge_results = []
            judge_records = []
            for j_idx, j_item in enumerate(ctx.j_cfgs):
                j_model = ctx.jm("refine", j_item)
                j_ar = await run_agent_checked(
                    context=f"s2-judge-{mod_name}-j{j_idx}-a{attempt+1}",
                    prompt=f"评审 Worker 对模块 `{mod_name}` 的细分判断。",
                    model=j_model,
                    system_prompt=j_sys_prompt,
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
                ctx.emit_event("judge_eval", stage=2, judge_id=f"judge-{j_idx}",
                               module=mod_name, passed=parsed["pass"], score=parsed["score"])
                archive_file(
                    ctx.output_dir,
                    f"s2-{mod_name}-a{attempt+1}-j{j_idx}.md",
                    f"Score: {parsed['score']}\nPass: {parsed['pass']}\n\n"
                    f"{parsed['feedback']}\n\n---\n## Raw Output\n\n{j_ar.output[:3000]}",
                )

            voted_pass = check_voting(judge_results, s_cfg.pass_mode, ctx.j_count)
            final_pass = voted_pass and attempt + 1 >= s_cfg.min_rounds
            max_reached = attempt + 1 >= max_iter(s_cfg)
            ctx.record_evaluation_round(
                module_name=mod_name,
                stage="refine",
                stage_round=attempt + 1,
                status="passed" if final_pass else "failed" if max_reached else "running",
                started_at=round_started,
                ended_at=utc_now_iso(),
                duration_ms=(time.time() - round_start_ts) * 1000,
                worker={
                    "model": ctx.wm("refine"),
                    "session_file": refine_session,
                    "token_usage": ar.token_usage,
                    "error": ar.error,
                },
                judges=judge_records,
                passed_by_vote=voted_pass,
                module_completed=False,
                completion_reason="passed" if final_pass else "max_rounds_exceeded" if max_reached else "",
                needed_reflection=not final_pass,
                artifact_paths=[str(mod_dir / "files.list")],
                extra={
                    "file_count": fc,
                    "split": was_split,
                    "new_modules": new_ones,
                },
            )

            if voted_pass:
                if attempt + 1 >= s_cfg.min_rounds:
                    if was_split and new_ones:
                        for nm in new_ones:
                            if nm not in self._refined and nm not in self._in_progress:
                                self._in_progress.add(nm)
                                await self._queue.put(nm)
                    self._refined.add(mod_name)
                    return
                else:
                    ctx.emit_event("reflect", stage=2, module=mod_name, round=attempt + 1)
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
                if "missing" in fail_fb.lower() or "丢失" in fail_fb or "遗漏" in fail_fb:
                    guidance = (
                        "\n\n⚠️ **文件丢失！** 请修复文件覆盖问题，不要改变拆分策略。\n"
                        "运行 check_classification.sh 查看遗漏文件，将它们归入合适的模块。"
                    )
                else:
                    guidance = "\n\n请根据评审意见调整拆分策略。"
                feedback = "# 评审意见（未通过）\n\n" + fail_fb + guidance

        raise StageError(f"Stage 2 模块 {mod_name} 细分未通过，已达最大轮数 {s_cfg.max_rounds}")

    # ── Stage 2 后：全局完整性检查 + 遗漏文件补分类 ───────────────────────
    async def _global_completeness_check(self) -> None:
        ctx = self._ctx
        cfg = ctx.cfg
        workspace = ctx.workspace
        w_base = ctx.make_w_base()

        filtered_txt = workspace / "filtered_files.txt"
        if not filtered_txt.exists():
            return

        all_target = set(
            l.strip() for l in filtered_txt.read_text("utf-8").splitlines() if l.strip()
        )
        mods_root = get_modules_root(str(workspace))
        all_classified: set[str] = set()
        for flist in mods_root.glob("*/files.list"):
            if flist.name == "files.list.snapshot":
                continue
            for l in flist.read_text("utf-8").splitlines():
                l = l.strip()
                if l:
                    all_classified.add(l)
        missing_files = sorted(all_target - all_classified)

        if not missing_files:
            ctx.emit_event("log", level="info",
                           msg=f"Stage2 全局检查: 全部 {len(all_target)} 个文件已归类 ✅")
            return

        ctx.emit_event("log", level="warn",
                       msg=f"Stage2 全局检查: {len(missing_files)} 个文件未归类，启动补分类")

        mod_summary_lines = ["## 已有模块（名称 | 示例文件）"]
        for flist in sorted(mods_root.glob("*/files.list")):
            mod_name = flist.parent.name
            sample = next(
                (l.strip() for l in flist.read_text("utf-8").splitlines() if l.strip()),
                "(空)"
            )
            mod_summary_lines.append(f"- {mod_name} | {Path(sample).name}")
        mod_summary = "\n".join(mod_summary_lines)

        reclass_prompt_tmpl = load_prompt(cfg.workers.system_prompt_dir, "step2_reclassify")
        max_rc = min(3, max_iter(cfg.stages.refine))

        reclass_prompt = (
            f"## 待归类文件（{len(missing_files)} 个）\n\n"
            + "\n".join(missing_files)
            + f"\n\n{mod_summary}"
        )

        for rc_attempt in range(max_rc):
            rc_ar = await run_agent_checked(
                context=f"s2-reclassify-a{rc_attempt+1}",
                prompt=reclass_prompt,
                model=ctx.wm("classify"),
                tools=w_base["tools"],
                system_prompt=reclass_prompt_tmpl,
                cwd=str(workspace),
                thinking_level=w_base.get("thinking_level", "off"),
                session_file=None,
                cancel_event=w_base.get("cancel_event"),
                max_retries=w_base.get("max_retries", 3),
                retry_delay=w_base.get("retry_delay", 10),
                pi_max_retries=w_base.get("pi_max_retries", -1),
                pi_retry_delay=w_base.get("pi_retry_delay", 10),
            )
            ctx.tokens += rc_ar.token_usage

            all_classified2: set[str] = set()
            for flist in mods_root.glob("*/files.list"):
                for l in flist.read_text("utf-8").splitlines():
                    l = l.strip()
                    if l:
                        all_classified2.add(l)
            still_missing = sorted(all_target - all_classified2)
            ctx.emit_event("log", level="info",
                           msg=f"补分类第{rc_attempt+1}轮: 剩余 {len(still_missing)} 个未归类")
            if not still_missing:
                break
            missing_files = still_missing
            reclass_prompt = (
                f"## 仍未归类文件（{len(missing_files)} 个）\n\n"
                + "\n".join(missing_files)
                + f"\n\n{mod_summary}"
            )
