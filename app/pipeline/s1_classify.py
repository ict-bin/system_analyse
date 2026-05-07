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
from pathlib import Path

from .base import BaseStage
from .context import PipelineContext
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
        judge_model = cfg.judges.agents[0].model if cfg.judges.agents else classify_model

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
            pi_max_retries=cfg.pi_max_retries,
            pi_retry_delay=cfg.pi_retry_delay,
        )

        feedback = ""
        max_iter = 999 if s_cfg.max_rounds < 0 else s_cfg.max_rounds

        for attempt in range(max_iter):
            # 构建 prompt
            prompt_parts = [ctx.task]
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
                    pi_max_retries=cfg.pi_max_retries,
                    pi_retry_delay=cfg.pi_retry_delay,
                )
                ctx.tokens += j_ar.token_usage
                parsed = parse_eval_md(j_ar.output or "")
                judge_results.append(parsed)

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
