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
import shutil

from .helpers import (
    run_agent_checked, parse_eval_md, check_voting,
    discover_modules, get_modules_root, load_prompt, StageError,
)


def _enforce_filter_constraint(ctx: PipelineContext, workspace: Path) -> None:
    """程序化校验：删除 modules/*/files.list 中不属于 filtered_files.txt 的在内条目。

    LLM 可能将目标目录中的其他文件写入 files.list，本函数确保
    所有 files.list 中的条目都属于过滤后的文件集合。
    """
    if not ctx.filtered_files:
        return
    allowed: set[str] = set(ctx.filtered_files)
    mods_root = get_modules_root(str(workspace))
    removed_total = 0
    for flist in sorted(mods_root.glob("*/files.list")):
        lines = [l.strip() for l in flist.read_text("utf-8").splitlines()]
        valid = [l for l in lines if not l or l.startswith("#") or l in allowed]
        extra = [l for l in lines if l and not l.startswith("#") and l not in allowed]
        if extra:
            removed_total += len(extra)
            ctx.emit_event("log", level="warn",
                           msg=(f"[filter-constraint] {flist.parent.name}: 删除 {len(extra)} 个"
                                f"非过滤文件，示例：{extra[:3]}"))
            flist.write_text("\n".join(valid).strip() + "\n", encoding="utf-8")
            # 如果删除后 files.list 为空，移除整个模块目录
            remaining = [l for l in valid if l and not l.startswith("#")]
            if not remaining:
                ctx.emit_event("log", level="warn",
                               msg=f"[filter-constraint] {flist.parent.name}: 移除空模块")
                shutil.rmtree(str(flist.parent), ignore_errors=True)
    if removed_total:
        ctx.emit_event("log", level="warn",
                       msg=f"[filter-constraint] 共删除 {removed_total} 个非过滤条目")
    else:
        ctx.emit_event("log", level="info",
                       msg="[filter-constraint] 校验通过：所有 files.list 条目均在过滤集合内 ✅")


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
            # ⚠️ 过滤约束：始终注入（不依赖 prescan_summary，不依赖轮次）
            prompt_parts.append(
                f"\n\n# ⚠️ 文件来源约束（必须严格遵守）\n\n"
                f"每个模块的 `files.list` **只能包含** `{workspace}/filtered_files.txt` "
                f"中的相对路径，**禁止写入任何该文件之外的路径**。"
                f"如有疑问请先读取 `filtered_files.txt` 确认范围。"
            )
            if attempt == 0 and prescan_summary:
                prompt_parts.append(
                    "\n\n# 预扫描摘要（已自动生成，请基于此分类）\n\n"
                    + prescan_summary
                    + "\n\n预扫描已将文件按关键词分组到 `prescan/` 目录下，"
                    "每个 `prescan/<keyword>.list` 包含对应文件列表。\n"
                    "你可以直接用脚本将 prescan/*.list 移入模块目录。"
                )

            # ── 安全维度过滤约束 ──
            sec_cats = getattr(cfg, "security_focus_categories", ["all"])
            if attempt == 0 and sec_cats and "all" not in sec_cats:
                from app.models import SECURITY_CATEGORIES  # noqa: PLC0415
                cat_lines = []
                for cat_key in sec_cats:
                    cat_info = SECURITY_CATEGORIES.get(cat_key, {})
                    cat_lines.append(
                        f"- **{cat_key}**（{cat_info.get('name', '')}）：{cat_info.get('desc', '')}"
                    )
                prompt_parts.append(
                    "\n\n# ⚠️ 安全分析范围约束（必须严格执行）\n\n"
                    "**只将与以下安全维度直接相关的文件归入模块**，"
                    "无关文件（测试代码、国际化字符串、构建脚本、样例数据、文档等）"
                    "**绝对不得**创建任何模块——直接丢弃。\n\n"
                    "**指定安全维度：**\n" + "\n".join(cat_lines)
                )

            # ── 模块粒度约束（每轮注入，不仅首轮）──
            granularity = getattr(cfg, "module_granularity", "fine")
            if granularity == "coarse":
                prompt_parts.append(
                    "\n\n# ⚠️ 模块划分粒度：粗粒度（协议/服务/功能级）\n\n"
                    "模块划分必须以**完整协议 / 完整服务 / 完整安全功能**为边界：\n"
                    "- 某个网络协议的所有实现代码（解析器、编码器、状态机、会话管理等）"
                    "统一归入**同一个模块**。\n"
                    "- 例如：`HTTP协议` 是一个模块，**不要**拆分为"
                    " `HTTP请求解析`、`HTTP响应构造`、`HTTP分块传输`。\n"
                    "- `SSH` 的所有相关实现（协商、认证、通道、密钥交换等）属于**同一个模块**。\n"
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
            # ── 构建 judge prompt（含粒度约束） ──
            granularity_note = ""
            if granularity == "coarse":
                granularity_note = (
                    "\n\n⚠️ 当前为粗粒度模式：请检查是否有同一协议/服务被拆分为多个子模块。"
                    "例如 ssh_client、ssh_channel、ssh_transport 均属于 SSH 协议，应合并为一个模块。"
                    "若发现此类拆分，请在评审意见中明确指出并要求合并。"
                )
            for j_idx, j_agent in enumerate(cfg.judges.agents):
                j_model = cfg.judges.model_for("classify") or j_agent.model
                j_ar = await run_agent_checked(
                    context=f"s1-classify-judge{j_idx}",
                    prompt=f"审核分类结果。模块数：{len(modules)}。{granularity_note}",
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
                _enforce_filter_constraint(ctx, workspace)
                ctx.classified_modules = discover_modules(str(workspace))
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
