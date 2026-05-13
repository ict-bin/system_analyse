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
    max_rounds_exceeded_treated_as_passed,
)


def _build_security_focus_section(sec_cats: list[str]) -> str:
    """生成安全维度约束段落（Worker+Judge 共用）。

    从 SECURITY_CATEGORIES 的 includes/boundary_note 动态生成，
    不使用任何场景化硬编码规则。
    """
    if not sec_cats or "all" in sec_cats:
        return ""

    from app.models import SECURITY_CATEGORIES  # noqa: PLC0415

    cat_lines = []
    includes_lines = []
    boundary_lines = []
    for cat_key in sec_cats:
        cat_info = SECURITY_CATEGORIES.get(cat_key, {})
        cat_lines.append(
            f"- **{cat_key}**（{cat_info.get('name', '')}）：{cat_info.get('desc', '')}"
        )
        inc = cat_info.get("includes", "")
        if inc:
            includes_lines.append(f"  - `{cat_key}` 包含：{inc}")
        bn = cat_info.get("boundary_note", "")
        if bn:
            boundary_lines.append(f"  - `{cat_key}` 判断规则：{bn}")

    includes_section = ""
    if includes_lines:
        includes_section = (
            "\n\n**目标范围示例（包含所有直接实现该维度功能的代码）：**\n"
            + "\n".join(includes_lines)
        )

    boundary_section = ""
    if boundary_lines:
        boundary_section = (
            "\n\n**边界判断规则（通用，适用于任何目标系统）：**\n"
            + "\n".join(boundary_lines)
        )

    return (
        "\n\n# ⚠️ 安全分析范围约束（必须严格执行）\n\n"
        "**只将与以下安全维度直接相关的文件归入模块**，"
        "无关文件（测试代码、国际化字符串、构建脚本、样例数据、文档等）"
        "**绝对不得**创建任何模块——直接丢弃。\n\n"
        "**指定安全维度：**\n" + "\n".join(cat_lines)
        + includes_section
        + boundary_section
        + "\n\n**裁判标准**：分类结果中，凡"
        "直接实现或调用上述安全维度功能的模块均视为符合范围，"
        "不限于底层实现细节；"
        "只有与指定维度完全无关的代码才应当被排除。"
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

        classify_prompt = load_prompt(cfg, "step1_classify", "workers")
        check_prompt = load_prompt(cfg, "step1_check_classify", "judges")
        reflect_prompt = load_prompt(cfg, "reflect_classify", "workers")

        classify_session = str(ctx.sess_dir / "classify.jsonl")
        classify_model = cfg.workers.model_for("classify")
        judge_model = cfg.judges.agents[0].model if cfg.judges.agents else classify_model

        ctx.emit_event("stage", stage=1)
        ctx.emit_event("model", stage="classify",
                       worker=classify_model.split("/")[-1],
                       judge=judge_model.split("/")[-1])
        ctx.emit_event(
            "log",
            level="info",
            msg=(
                "分类阶段安全过滤配置："
                f"security_focus_categories={list(cfg.security_focus_categories)}, "
                f"module_granularity={cfg.module_granularity}"
            ),
        )

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

            # ── 注意：S1 不进行安全维度过滤──
            # 安全维度过滤是 S1.5 (SecurityFocusFilterStage) 的职责。
            # S1 应对 filtered_files.txt 中的全量文件按功能分组，不限安全维度。
            # 如果在 S1 注入 security_focus_section，Judge 会错误拒绝非安全相关的模块，
            # 导致任务无限卡死在 S1 评审轮。

            # ── 模块粒度约束 ──
            granularity = getattr(cfg, "module_granularity", "fine")
            if attempt == 0 and granularity == "coarse":
                prompt_parts.append(
                    "\n\n# ⚠️ 模块划分粒度：粗粒度（协议/服务/功能级）\n\n"
                    "模块划分必须以**完整协议 / 完整服务 / 完整安全功能**为边界：\n"
                    "- 某个网络协议的所有实现代码（解析器、编码器、状态机、会话管理等）"
                    "统一归入**同一个模块**。\n"
                    "- 例如：`HTTP协议` 是一个模块，**不要**拆分为"
                    " `HTTP请求解析`、`HTTP响应构造`、`HTTP分块传输`。\n"
                    "- 固件中有多少个协议 / 功能就创建多少个模块，**不限制总模块数量**。"
                )

            # ── 路径先验分组指引（path_groups.md 由 PathGroupStage 生成）──
            if attempt == 0 and (workspace / "prescan" / "path_groups.md").exists():
                prompt_parts.append(
                    "\n\n# 路径先验分组（已由工具自动生成，请优先采用）\n\n"
                    "prescan_summary 中包含路径先验分组结果（path_groups.md）。\n"
                    "「直接路径组」的文件归属已按目录路径推断完毕，"
                    "**请优先直接写入对应模块的 files.list**，无需重新分析路径。\n"
                    "「特殊路径文件」已按 lib 名前缀二次分组，"
                    "请根据功能语义判断归入哪个模块。\n"
                    "`__unmatched_shared__` 文件若无法判断归属，"
                    "统一归入 `misc_shared_libs` 模块。"
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
                judge_prompt = [f"审核分类结果。模块数：{len(modules)}。"]
                # 注意：Judge 不应检查安全维度相关性，那是 S1.5 的职责。
                # S1 Judge 只检查：所有 filtered_files.txt 中的文件是否已全部分类。
                if granularity == "coarse":
                    judge_prompt.append(
                        "\n\n# 模块粒度要求\n\n"
                        "当前要求是粗粒度（协议/服务/功能级）。"
                        "不要把同一协议拆成多个碎片模块。"
                    )
                j_ar = await run_agent_checked(
                    context=f"s1-classify-judge{j_idx}",
                    prompt="\n\n".join(judge_prompt),
                    model=j_model,
                    tools=cfg.judges.default_tools,
                    system_prompt=check_prompt,
                    cwd=str(workspace),
                    thinking_level="off",
                    session_file=str(ctx.sess_dir / f"classify-judge{j_idx}-a{attempt+1}.jsonl"),
                    cancel_event=ctx.cancel_event,
                    max_retries=cfg.agent_max_retries,
                    retry_delay=cfg.agent_retry_delay,
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
            forced_pass = max_reached and max_rounds_exceeded_treated_as_passed(cfg)
            ctx.record_evaluation_round(
                module_name="__task__",
                stage="classify",
                stage_round=attempt + 1,
                status="passed" if (final_pass or forced_pass) else "failed" if max_reached else "running",
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
                completion_reason=(
                    "passed"
                    if final_pass
                    else "max_rounds_exceeded_treated_as_passed"
                    if forced_pass
                    else "max_rounds_exceeded"
                    if max_reached
                    else ""
                ),
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
            if forced_pass:
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
