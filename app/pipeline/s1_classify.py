"""
pipeline/s1_classify.py — Stage 1: 粗分类

入: ctx.workspace (含 filtered_files.txt, keyword_summary.txt)
    ctx.cfg.stages.classify, ctx.cfg.workers/judges
出: ctx.classified_modules
    workspace/modules/*/files.list
    workspace/modules.list

核心流程:
  Worker (step1_classify.md) + Judge (step1_check_classify.md)
  多轮 W+J，直到 judge 通过且满足 min_rounds
"""
from __future__ import annotations

import asyncio
import time
from pathlib import Path

from .base import BaseStage
from .context import PipelineContext
from .evaluation import utc_now_iso
from .helpers import (
    run_agent_checked, parse_eval_md, check_voting,
    discover_modules, get_modules_root, load_prompt, StageError,
)


class ClassifyStage(BaseStage):
    """Stage 1: 粗分类"""

    stage_num = 1
    stage_name = "分类"

    async def execute(self, ctx: PipelineContext) -> None:
        cfg = ctx.cfg
        workspace = ctx.workspace
        task_id = ctx.task_id
        s_cfg = cfg.stages.classify

        w_prompt_dir = cfg.workers.system_prompt_dir
        j_prompt_dir = cfg.judges.system_prompt_dir
        classify_prompt = load_prompt(w_prompt_dir, "step1_classify")
        check_prompt = load_prompt(j_prompt_dir, "step1_check_classify")
        reflect_prompt = load_prompt(w_prompt_dir, "reflect_classify")

        classify_session = str(ctx.sess_dir / "classify.jsonl")
        classify_model = cfg.workers.model_for("classify")
        judge_model = (
            ctx.jm("classify", ctx.j_cfgs[0])
            if ctx.j_cfgs else
            classify_model
        )

        ctx.emit_event("stage", stage=1)
        ctx.emit_event("model", stage="classify",
                       worker=classify_model.split("/")[-1],
                       judge=judge_model.split("/")[-1])

        # ── 构建 prescan 摘要注入 ──
        prescan_summary = ctx.prescan_summary

        # ── 基础 Worker 参数 ──
        w_tools = cfg.workers.default_tools
        w_base = dict(
            tools=w_tools,
            cwd=str(workspace),
            thinking_level="off",
            session_file=classify_session,
            cancel_event=ctx.cancel_event,
            max_retries=cfg.agent_max_retries,
            retry_delay=cfg.agent_retry_delay,
            run_timeout_seconds=cfg.agent_run_timeout_seconds,
            timeout_retry_enabled=cfg.agent_timeout_retry_enabled,
            timeout_max_retries=cfg.agent_timeout_max_retries,
            pi_max_retries=cfg.pi_max_retries,
            pi_retry_delay=cfg.pi_retry_delay,
        )

        feedback = ""
        max_iter = 999 if s_cfg.max_rounds < 0 else s_cfg.max_rounds

        for attempt in range(max_iter):
            round_started = utc_now_iso()
            round_start_ts = time.time()
            # 构建 prompt
            prompt_parts = [ctx.task]
            # ⚠️ 注入工作目录绝对路径，防止 agent cd 到错误目录
            prompt_parts.append(
                f"\n\n# ⚠️ 工作目录（绝对禁止离开）\n\n"
                f"你的工作目录已固定为：`{workspace}`\n\n"
                f"- `filtered_files.txt` 完整路径：`{workspace}/filtered_files.txt`\n"
                f"- 预扫描数据目录：`{workspace}/prescan/`\n"
                f"- 模块输出路径：`{workspace}/modules/<模块名>/files.list`\n\n"
                f"**严禁执行任何 `cd` 命令**。所有脚本必须使用绝对路径或相对当前工作目录的路径。"
            )
            if attempt == 0 and prescan_summary:
                prompt_parts.append(
                    "\n\n# 预扫描摘要（已自动生成，请基于此分类）\n\n"
                    + prescan_summary
                    + "\n\n预扫描已将文件按关键词分组到 `prescan/` 目录下，"
                    "每个 `prescan/<keyword>.list` 包含对应文件列表。\n"
                    "你可以直接用脚本将 prescan/*.list 移入模块目录。"
                    "\n\n每个模块的 files.list 必须写的是 filtered_files.txt 里的相对路径。"
                )
            if feedback:
                prompt_parts.append("\n\n" + feedback)

            ar = await run_agent_checked(
                context=f"s1-classify-a{attempt+1}",
                prompt="\n".join(prompt_parts),
                model=classify_model,
                system_prompt=classify_prompt,
                **w_base,
            )
            ctx.tokens += ar.token_usage

            # 发现模块
            modules = discover_modules(str(workspace))
            ctx.emit_event("stage_result", stage=1, modules=modules)

            # ── Judge ──
            judge_results = []
            judge_records = []
            for j_idx, j_agent in enumerate(cfg.judges.agents):
                j_model = cfg.judges.model_for("classify") or j_agent.model
                j_ar = await run_agent_checked(
                    context=f"s1-classify-judge{j_idx}",
                    prompt=f"审核分类结果。模块数：{len(modules)}。",
                    model=j_model,
                    tools=cfg.judges.default_tools,
                    system_prompt=check_prompt,
                    cwd=str(workspace),
                    thinking_level="off",
                    session_file=None,
                    cancel_event=ctx.cancel_event,
                    max_retries=cfg.agent_max_retries,
                    retry_delay=cfg.agent_retry_delay,
                    run_timeout_seconds=cfg.agent_run_timeout_seconds,
                    timeout_retry_enabled=cfg.agent_timeout_retry_enabled,
                    timeout_max_retries=cfg.agent_timeout_max_retries,
                    pi_max_retries=cfg.pi_max_retries,
                    pi_retry_delay=cfg.pi_retry_delay,
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

                # 保存 judge 文件
                j_path = ctx.output_dir / f"s1-a{attempt+1}-j{j_idx}.md"
                j_path.write_text(
                    f"Score: {parsed['score']}\nPass: {parsed['pass']}\n\n"
                    f"{parsed['feedback']}\n\n---\n## Raw Output\n\n{j_ar.output[:3000]}",
                    encoding="utf-8"
                )
                ctx.emit_event("judge_eval", stage=1, judge_id=f"judge-{j_idx}",
                               score=parsed["score"], passed=parsed["pass"],
                               attempt=attempt + 1, module="")

            j_count = len(judge_results)
            voted_pass = check_voting(judge_results, s_cfg.pass_mode, j_count)
            final_pass = voted_pass and attempt + 1 >= s_cfg.min_rounds
            max_reached = attempt + 1 >= max_iter
            ctx.record_evaluation_round(
                module_name="__task__",
                stage="classify",
                stage_round=attempt + 1,
                status="passed" if final_pass else "failed" if max_reached else "running",
                started_at=round_started,
                ended_at=utc_now_iso(),
                duration_ms=(time.time() - round_start_ts) * 1000,
                worker={
                    "model": classify_model,
                    "session_file": classify_session,
                    "token_usage": ar.token_usage,
                    "error": ar.error,
                },
                judges=judge_records,
                passed_by_vote=voted_pass,
                module_completed=False,
                completion_reason="passed" if final_pass else "max_rounds_exceeded" if max_reached else "",
                needed_reflection=not final_pass,
                artifact_paths=[str(workspace / "modules"), str(workspace / "modules.list")],
                extra={
                    "module_count": len(modules),
                    "modules": modules,
                },
            )

            if voted_pass and attempt + 1 >= s_cfg.min_rounds:
                ctx.classified_modules = modules
                return

            if voted_pass:
                # 需要再跑一轮（min_rounds 要求）
                ctx.emit_event("reflect", stage=1, round=attempt + 1)
                feedback = (
                    f"# 自查要求（第 {attempt+1} 轮，需至少 {s_cfg.min_rounds} 轮）\n\n"
                    + reflect_prompt
                )
            else:
                # judge 失败
                fail_fb = "\n".join(
                    f"judge-{i}: {r['feedback'][:500]}"
                    for i, r in enumerate(judge_results)
                )
                feedback = (
                    f"# 上轮评审不通过（第 {attempt+1} 轮）\n\n"
                    f"## Judge 上轮意见\n\n{fail_fb}\n\n"
                    + reflect_prompt
                )

        raise StageError(f"Stage 1 分类未通过，已达最大轮数 {s_cfg.max_rounds}")
