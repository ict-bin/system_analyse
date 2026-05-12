"""
pipeline/s1_security_filter.py — Stage 1.5: 安全维度过滤

位置：ClassifyStage（S1）之后，RefineStage（S2）之前。
stage_num=1（与 ClassifyStage 相同），Pipeline stable-sort 保证顺序。

入:  ctx.classified_modules（S1 产出的粗分类模块集合）
     ctx.cfg.security_focus_categories
出:  workspace/modules/（仅保留与指定安全维度相关的模块）
     ctx.classified_modules（已更新）

核心流程:
  1. 备份 modules/ → modules_pre_filter_backup/
  2. W+J 循环：
       每轮开始从备份还原 modules/，确保幂等
       Worker 直接删除 modules/ 中无关模块目录
       Judge 对比 modules_pre_filter_backup/ 与 modules/ 验证过滤质量
  3. 通过后删除备份目录

跳过条件:
  security_focus_categories == ["all"]（不过滤时直接跳过）
"""
from __future__ import annotations

import shutil
import time
from pathlib import Path

from .base import BaseStage
from .context import PipelineContext
from .evaluation import utc_now_iso
from .helpers import (
    run_agent_checked, parse_eval_md, check_voting,
    discover_modules, get_modules_root, load_prompt, StageError,
)

_BACKUP_DIR_NAME = "modules_pre_filter_backup"


def _backup_modules(modules_root: Path, workspace: Path) -> Path:
    """将 modules/ 完整备份到 modules_pre_filter_backup/，返回备份路径。"""
    backup = workspace / _BACKUP_DIR_NAME
    if backup.exists():
        shutil.rmtree(str(backup))
    shutil.copytree(str(modules_root), str(backup))
    return backup


def _restore_from_backup(modules_root: Path, backup: Path) -> None:
    """将 modules_pre_filter_backup/ 还原到 modules/（每轮重试前调用）。"""
    if modules_root.exists():
        shutil.rmtree(str(modules_root))
    shutil.copytree(str(backup), str(modules_root))


class SecurityFocusFilterStage(BaseStage):
    """Stage 1.5: 安全维度过滤 — 丢弃与指定安全维度无关的模块。

    Worker 直接删除 modules/ 下的无关模块目录；Judge 对比备份核查。
    每轮重试前从备份还原，保证幂等。通过后删除备份。
    """

    stage_num = 1       # 与 ClassifyStage 相同；Pipeline stable-sort 保证本阶段在其之后
    stage_name = "安全维度过滤"

    async def execute(self, ctx: PipelineContext) -> None:
        cfg = ctx.cfg
        sec_cats: list[str] = getattr(cfg, "security_focus_categories", ["all"])

        # ── 快速跳过 ─────────────────────────────────────────────────────────
        if not sec_cats or "all" in sec_cats:
            ctx.emit_event("log", level="info",
                           msg="[安全维度过滤] security_focus_categories=all，跳过")
            return

        workspace = ctx.workspace
        modules_root = get_modules_root(str(workspace))

        # ── Stage 配置（向后兼容旧配置无此字段时使用默认值） ────────────────
        s_cfg = getattr(cfg.stages, "security_filter", None)
        if s_cfg is None:
            from ..models import StageLoopConfig
            s_cfg = StageLoopConfig(min_rounds=1, max_rounds=3, pass_mode="all")

        # ── 构建安全维度描述 ──────────────────────────────────────────────────
        from app.models import SECURITY_CATEGORIES  # noqa: PLC0415
        cat_lines: list[str] = []
        for cat_key in sec_cats:
            info = SECURITY_CATEGORIES.get(cat_key, {})
            cat_lines.append(
                f"- **{cat_key}**（{info.get('name', cat_key)}）：{info.get('desc', '')}"
            )
        cat_desc = "\n".join(cat_lines)

        # ── 读取 Prompt ───────────────────────────────────────────────────────
        worker_system_prompt = load_prompt(cfg.workers.system_prompt_dir, "step1_security_filter")
        judge_system_prompt  = load_prompt(cfg.judges.system_prompt_dir, "step1_check_security_filter")

        filter_model = cfg.workers.model_for("classify")
        judge_model  = (
            cfg.judges.model_for("classify")
            or (cfg.judges.agents[0].model if cfg.judges.agents else filter_model)
        )

        ctx.emit_event("stage", stage="1.5-security-filter")
        ctx.emit_event("model", stage="security_filter",
                       worker=filter_model.split("/")[-1],
                       judge=judge_model.split("/")[-1])

        all_modules = discover_modules(str(workspace))
        ctx.emit_event("log", level="info",
                       msg=(f"[安全维度过滤] 启动：{len(all_modules)} 个模块，"
                            f"维度：{sec_cats}"))

        # ── 第一步：备份 ──────────────────────────────────────────────────────
        backup = _backup_modules(modules_root, workspace)
        ctx.emit_event("log", level="info",
                       msg=f"[安全维度过滤] 备份已创建：{backup}")

        # ── W+J 循环 ──────────────────────────────────────────────────────────
        filter_session = str(ctx.sess_dir / "security_filter.jsonl")
        w_base = dict(
            tools=cfg.workers.default_tools,
            cwd=str(workspace),
            thinking_level="off",
            session_file=filter_session,
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

            # 每轮从备份还原，保证 Worker 每次都面对完整的原始集合
            _restore_from_backup(modules_root, backup)
            current_modules = discover_modules(str(workspace))

            ctx.emit_event("log", level="info",
                           msg=(f"[安全维度过滤] 第{attempt+1}轮开始，"
                                f"还原后模块数：{len(current_modules)}"))

            # ── Worker Prompt ─────────────────────────────────────────────────
            prompt_parts = [
                f"# 安全维度过滤任务（第 {attempt+1} 轮）\n\n"
                f"## 指定安全维度（只保留与此相关的模块）\n\n{cat_desc}\n\n"
                f"## 当前模块（共 {len(current_modules)} 个，已从备份还原）\n\n"
                + "\n".join(f"- `{m}`" for m in sorted(current_modules))
                + f"\n\n## 目录路径\n\n"
                f"- 工作目录：`{workspace}`\n"
                f"- 模块目录：`{modules_root}`\n"
                f"- 备份目录：`{backup}`（**只读，不要修改**）\n\n"
                f"## 操作要求\n\n"
                f"逐模块判断相关性，对每个**无关**模块执行删除：\n"
                f"```bash\n"
                f"rm -rf {modules_root}/<模块名>\n"
                f"```\n"
                f"完成后输出 `<result>` 摘要（保留/删除了哪些模块及原因）。"
            ]
            if feedback:
                prompt_parts.append(f"\n\n---\n\n# 上轮评审意见（请据此修正）\n\n{feedback}")

            ar = await run_agent_checked(
                context=f"s1-security-filter-a{attempt+1}",
                prompt="\n".join(prompt_parts),
                model=filter_model,
                system_prompt=worker_system_prompt,
                **w_base,
            )
            ctx.tokens += ar.token_usage

            # Worker 执行后发现剩余模块
            kept_modules  = discover_modules(str(workspace))
            removed_count = len(current_modules) - len(kept_modules)
            ctx.emit_event("log", level="info",
                           msg=(f"[安全维度过滤] 第{attempt+1}轮 Worker 完成："
                                f"保留 {len(kept_modules)}，删除 {removed_count}"))

            # ── Judge ─────────────────────────────────────────────────────────
            judge_results = []
            judge_records = []
            for j_idx, j_agent in enumerate(cfg.judges.agents):
                j_model = cfg.judges.model_for("classify") or j_agent.model

                # 列出被删除的模块（备份有但 modules/ 没有的）
                backup_mods = {
                    p.name for p in backup.iterdir() if p.is_dir()
                }
                removed_mods = sorted(backup_mods - set(kept_modules))

                j_prompt = (
                    f"# 安全维度过滤评审\n\n"
                    f"## 指定安全维度\n\n{cat_desc}\n\n"
                    f"## 过滤前（备份）：{len(backup_mods)} 个模块\n\n"
                    + "\n".join(f"- `{m}`" for m in sorted(backup_mods))
                    + f"\n\n## 过滤后（当前）：{len(kept_modules)} 个模块\n\n"
                    + "\n".join(f"- `{m}`" for m in sorted(kept_modules))
                    + f"\n\n## 被删除：{len(removed_mods)} 个\n\n"
                    + "\n".join(f"- `{m}`" for m in removed_mods)
                    + f"\n\n备份目录 `{backup}` 可供读取详细 files.list 核查。\n\n"
                    f"请验证：删除的模块是否确实无关？保留的是否都相关？"
                )
                j_ar = await run_agent_checked(
                    context=f"s1-security-filter-judge{j_idx}-a{attempt+1}",
                    prompt=j_prompt,
                    model=j_model,
                    tools=cfg.judges.default_tools,
                    system_prompt=judge_system_prompt,
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
                })
                j_path = ctx.output_dir / f"s1-sec-filter-a{attempt+1}-j{j_idx}.md"
                j_path.write_text(
                    f"Score: {parsed['score']}\nPass: {parsed['pass']}\n\n"
                    f"{parsed['feedback']}\n\n---\n## Raw Output\n\n{j_ar.output[:3000]}",
                    encoding="utf-8",
                )
                ctx.emit_event("judge_eval", stage="1.5", judge_id=f"judge-{j_idx}",
                               score=parsed["score"], passed=parsed["pass"],
                               attempt=attempt + 1, module="")

            j_count = len(judge_results)
            voted_pass = check_voting(judge_results, s_cfg.pass_mode, j_count)
            final_pass = voted_pass and attempt + 1 >= s_cfg.min_rounds
            max_reached = attempt + 1 >= max_iter

            ctx.record_evaluation_round(
                module_name="__task__",
                stage="security_filter",
                stage_round=attempt + 1,
                status="passed" if final_pass else "failed" if max_reached else "running",
                started_at=round_started,
                ended_at=utc_now_iso(),
                duration_ms=(time.time() - round_start_ts) * 1000,
                worker={
                    "model": filter_model,
                    "session_file": filter_session,
                    "token_usage": ar.token_usage,
                },
                judges=judge_records,
                passed_by_vote=voted_pass,
                module_completed=False,
                completion_reason=(
                    "passed" if final_pass
                    else "max_rounds_exceeded" if max_reached
                    else ""
                ),
                extra={
                    "before_count": len(current_modules),
                    "after_count": len(kept_modules),
                    "removed_count": removed_count,
                    "kept_modules": sorted(kept_modules),
                    "removed_modules": sorted(
                        set(current_modules) - set(kept_modules)
                    ),
                },
            )

            if final_pass:
                # ── 删除备份目录 ──────────────────────────────────────────────
                shutil.rmtree(str(backup), ignore_errors=True)
                ctx.emit_event("log", level="info",
                               msg=(f"[安全维度过滤] 完成：保留 {len(kept_modules)} 个，"
                                    f"删除 {removed_count} 个，备份已清理"))
                ctx.classified_modules = discover_modules(str(workspace))
                return

            # ── 未通过：收集 Judge 意见用于下轮 Worker ────────────────────────
            if not max_reached:
                fail_fb = "\n\n".join(
                    f"**Judge-{i}（分数 {r['score']}）：**\n{r['feedback'][:500]}"
                    for i, r in enumerate(judge_results)
                )
                feedback = (
                    f"评审意见（共 {j_count} 个 Judge）：\n\n{fail_fb}\n\n"
                    f"请根据上述意见重新判断每个模块的相关性，"
                    f"下轮将从备份重新还原所有模块后再执行。"
                )

        # 达到最大轮数仍未通过——保留备份供排查，抛出异常
        raise StageError(
            f"安全维度过滤阶段未通过，已达最大轮数 {s_cfg.max_rounds}。"
            f"备份保留于 {backup} 供人工排查。"
        )
