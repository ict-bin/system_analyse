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
       第 1 轮：Worker 全量审查并直接删除无关模块目录
       第 2+ 轮：Worker 按 Judge 意见增量修正：
                  • 恢复误删的模块（cp -r backup/<name> modules/<name>）
                  • 删除漏删的模块（rm -rf modules/<name>）
       Judge 对比 modules_pre_filter_backup/ 与 modules/ 验证过滤质量，
       输出结构化修正列表（恢复列表 / 删除列表）供下轮 Worker 使用
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
    run_agent_with_stage_guard, parse_eval_md, check_voting,
    discover_modules, get_modules_root, load_prompt, StageError,
    write_judge_feedback,
)

_BACKUP_DIR_NAME = "modules_pre_filter_backup"


def _backup_modules(modules_root: Path, workspace: Path) -> Path:
    """将 modules/ 完整备份到 modules_pre_filter_backup/，返回备份路径。"""
    backup = workspace / _BACKUP_DIR_NAME
    if backup.exists():
        shutil.rmtree(str(backup))
    shutil.copytree(str(modules_root), str(backup))
    return backup


class SecurityFocusFilterStage(BaseStage):
    """Stage 1.5: 安全维度过滤 — 丢弃与指定安全维度无关的模块。

    第 1 轮：Worker 全量审查，直接 rm -rf 无关模块目录。
    重试轮：Worker 按 Judge 结构化意见增量修正
            （恢复误删：cp -r backup/<name> modules/<name>；
              删除漏删：rm -rf modules/<name>）。
    通过后删除备份。
    """

    stage_num = 1       # 与 ClassifyStage 相同；Pipeline stable-sort 保证本阶段在其之后
    stage_name = "安全维度过滤"

    async def execute(self, ctx: PipelineContext) -> None:
        cp = ctx.checkpoint
        cfg = ctx.cfg
        sec_cats: list[str] = getattr(cfg, "security_focus_categories", ["all"])

        # ── 确定运行模式 ──────────────────────────────────────────────────────
        do_security_filter = bool(sec_cats) and "all" not in sec_cats
        do_useless_filter = getattr(cfg, "filter_useless_modules", True)

        # 两种过滤都不需要时跳过
        if not do_security_filter and not do_useless_filter:
            ctx.emit_event("log", level="info",
                           msg="[S1.5] 安全过滤=all 且 无用模块过滤=False，跳过")
            return

        # ── checkpoint 跳过 ───────────────────────────────────────────────────
        if cp and cp.is_done("s1_security_filter"):
            ctx.classified_modules = discover_modules(str(ctx.workspace))
            ctx.emit_event("log", level="info",
                           msg=f"[S1.5] checkpoint已完成，跳过"
                               f"（{len(ctx.classified_modules)}个模块保留）")
            return

        workspace = ctx.workspace
        modules_root = get_modules_root(str(workspace))

        # ── 记录本次运行模式 ──────────────────────────────────────────────────
        ctx.emit_event("log", level="info",
                       msg=(f"[S1.5] 过滤模式: "
                            f"安全维度={'开启('+','.join(sec_cats)+')' if do_security_filter else '跳过(all)'}，"
                            f"无用模块过滤={'开启' if do_useless_filter else '关闭'}"))

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

        # ── 备份（仅一次，不在重试时覆盖） ───────────────────────────────────
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

        # judge_corrections: 由 Judge 输出的结构化修正指令，注入下轮 Worker prompt
        judge_corrections: str = ""
        max_iter = 999 if s_cfg.max_rounds < 0 else s_cfg.max_rounds

        for attempt in range(max_iter):
            round_started = utc_now_iso()
            round_start_ts = time.time()

            current_modules = discover_modules(str(workspace))
            ctx.emit_event("log", level="info",
                           msg=(f"[安全维度过滤] 第{attempt+1}轮开始，"
                                f"当前模块数：{len(current_modules)}"))

            # ── Worker Prompt ─────────────────────────────────────────────────
            if attempt == 0:
                # 第 1 轮：全量审查
                prompt_parts = [
                    f"# 安全维度过滤任务（第 1 轮 — 全量审查）\n\n"
                    f"## 指定安全维度（只保留与此相关的模块）\n\n{cat_desc}\n\n"
                    f"## 当前全部模块（共 {len(current_modules)} 个）\n\n"
                    + "\n".join(f"- `{m}`" for m in sorted(current_modules))
                    + f"\n\n## 目录路径\n\n"
                    f"- 模块目录：`{modules_root}`\n"
                    f"- 备份目录：`{backup}`（**只读，不要修改**）\n\n"
                    f"**必须结合模块内文件信息判断，不能只看模块名。**\n"
                    f"请先查看 `modules/<模块名>/files.list`，必要时再结合 `details/<path>.json` 判断。\n"
                    f"逐模块判断相关性，对每个**无关**模块执行删除：\n"
                    f"```bash\n"
                    f"rm -rf {modules_root}/<模块名>\n"
                    f"```\n"
                    f"完成后输出 `<result>` 摘要。"
                ]
            else:
                # 第 2+ 轮：按 Judge 意见增量修正
                prompt_parts = [
                    f"# 安全维度过滤修正（第 {attempt+1} 轮 — 增量修正）\n\n"
                    f"## 指定安全维度\n\n{cat_desc}\n\n"
                    f"## 当前模块状态（共 {len(current_modules)} 个）\n\n"
                    + "\n".join(f"- `{m}`" for m in sorted(current_modules))
                    + f"\n\n## 目录路径\n\n"
                    f"- 模块目录：`{modules_root}`\n"
                    f"- 备份目录：`{backup}`（可从此处恢复误删模块）\n\n"
                    f"**必须结合模块内文件信息判断，不能只看模块名。**\n"
                    f"请先查看 `modules/<模块名>/files.list`，必要时再结合 `details/<path>.json` 判断。\n\n"
                    f"## Judge 要求的修正操作\n\n"
                    f"{judge_corrections}\n\n"
                    f"**操作指令：**\n\n"
                    f"恢复误删的模块（从备份复制回来）：\n"
                    f"```bash\n"
                    f"cp -r {backup}/<模块名> {modules_root}/<模块名>\n"
                    f"```\n\n"
                    f"删除漏删的模块：\n"
                    f"```bash\n"
                    f"rm -rf {modules_root}/<模块名>\n"
                    f"```\n\n"
                    f"完成后输出 `<result>` 摘要（执行了哪些恢复/删除操作）。"
                ]

            ar = await run_agent_with_stage_guard(
                ctx=ctx,
                stage="security_filter",
                context=f"s1-security-filter-a{attempt+1}",
                heartbeat_payload_factory=lambda beat, attempt_no=attempt + 1: {
                    "heartbeat": beat,
                    "attempt": attempt_no,
                    "role": "worker",
                },
                prompt="\n".join(prompt_parts),
                model=filter_model,
                system_prompt=worker_system_prompt,
                **w_base,
            )
            ctx.tokens += ar.token_usage

            # Worker 执行后发现剩余模块
            kept_modules  = discover_modules(str(workspace))
            removed_count = len(all_modules) - len(kept_modules)
            ctx.emit_event("log", level="info",
                           msg=(f"[安全维度过滤] 第{attempt+1}轮 Worker 完成："
                                f"当前保留 {len(kept_modules)}，"
                                f"累计删除 {removed_count}"))

            # ── Judge ─────────────────────────────────────────────────────────
            judge_results = []
            judge_records = []
            for j_idx, j_agent in enumerate(cfg.judges.agents):
                j_model = cfg.judges.model_for("classify") or j_agent.model

                backup_mods = {p.name for p in backup.iterdir() if p.is_dir()}
                removed_mods = sorted(backup_mods - set(kept_modules))

                j_prompt = (
                    f"# 安全维度过滤评审（第 {attempt+1} 轮）\n\n"
                    f"## 指定安全维度\n\n{cat_desc}\n\n"
                    f"## 过滤前（备份）：{len(backup_mods)} 个模块\n\n"
                    + "\n".join(f"- `{m}`" for m in sorted(backup_mods))
                    + f"\n\n## 过滤后（当前）：{len(kept_modules)} 个模块\n\n"
                    + "\n".join(f"- `{m}`" for m in sorted(kept_modules))
                    + f"\n\n## 被删除：{len(removed_mods)} 个\n\n"
                    + "\n".join(f"- `{m}`" for m in removed_mods)
                    + f"\n\n备份目录 `{backup}` 可供读取 files.list 核查。\n"
                    + f"请先查看被删模块与保留模块的 `files.list`，必要时抽查 `details/<path>.json`，再判断误删/漏删。\n\n"
                    f"请验证过滤质量，并在意见末尾输出结构化修正列表（即使全部正确也要输出空列表）：\n\n"
                    f"```\n"
                    f"## 需恢复（误删）:\n"
                    f"- <模块名>  # 原因说明\n\n"
                    f"## 需删除（漏删）:\n"
                    f"- <模块名>  # 原因说明\n"
                    f"```"
                )
                j_ar = await run_agent_with_stage_guard(
                    ctx=ctx,
                    stage="security_filter_judge",
                    context=f"s1-security-filter-judge{j_idx}-a{attempt+1}",
                    heartbeat_payload_factory=lambda beat, attempt_no=attempt + 1, judge_id=j_idx: {
                        "heartbeat": beat,
                        "attempt": attempt_no,
                        "role": "judge",
                        "judge_id": f"judge-{judge_id}",
                    },
                    prompt=j_prompt,
                    model=j_model,
                    tools=cfg.judges.default_tools,
                    system_prompt=judge_system_prompt,
                    cwd=str(workspace),
                    thinking_level="off",
                    session_file=str(ctx.sess_dir / f"sec-filter-j{j_idx}-a{attempt+1}.jsonl"),
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
                    "raw_output": (j_ar.output or "")[:2000],
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

            # ── 解析 Judge 结构化修正列表：只要存在需恢复/需删除项，本轮禁止通过 ─────────
            raw_feedback = "\n\n".join(str(r.get("feedback", "")) for r in judge_records)
            need_restore = []
            need_remove = []
            current_section = None
            for line in raw_feedback.splitlines():
                s = line.strip()
                low = s.lower()
                if "需恢复" in s:
                    current_section = "restore"
                    continue
                if "需删除" in s:
                    current_section = "remove"
                    continue
                if not s.startswith("-"):
                    continue
                if "（无）" in s or "(无)" in s:
                    continue
                item = s.lstrip("- ").split("#", 1)[0].strip().strip('`')
                if not item:
                    continue
                if current_section == "restore":
                    need_restore.append(item)
                elif current_section == "remove":
                    need_remove.append(item)

            j_count = len(judge_results)
            voted_pass = check_voting(judge_results, s_cfg.pass_mode, j_count)
            has_corrections = bool(need_restore or need_remove)
            final_pass = voted_pass and not has_corrections and attempt + 1 >= s_cfg.min_rounds
            max_reached = attempt + 1 >= max_iter

            ctx.record_evaluation_round(
                module_name="__task__",
                stage="security_filter",
                stage_round=attempt + 1,
                status="passed" if final_pass else "failed" if max_reached else "running",
                started_at=round_started,
                ended_at=utc_now_iso(),
                duration_ms=(time.time() - round_start_ts) * 1000,
                worker={"model": filter_model, "session_file": filter_session,
                        "token_usage": ar.token_usage},
                judges=judge_records,
                passed_by_vote=voted_pass,
                module_completed=False,
                completion_reason=(
                    "passed" if final_pass
                    else "max_rounds_exceeded" if max_reached else ""
                ),
                extra={
                    "original_count": len(all_modules),
                    "after_count": len(kept_modules),
                    "removed_count": removed_count,
                    "kept_modules": sorted(kept_modules),
                    "removed_modules": sorted(backup_mods - set(kept_modules)),
                },
            )

            if final_pass:
                # 归档被删模块的文件到 workspace/deleted.list
                kept_modules = discover_modules(str(workspace))
                removed_mod_names = sorted(backup_mods - set(kept_modules))
                all_deleted_files: list[str] = []
                for rm_mod in removed_mod_names:
                    mod_flist = backup / rm_mod / "files.list"
                    if mod_flist.exists():
                        lines = [ln.strip() for ln in
                                 mod_flist.read_text("utf-8", errors="replace").splitlines()
                                 if ln.strip()]
                        all_deleted_files.extend(lines)
                if all_deleted_files:
                    with open(str(ctx.deleted_list_path), "a", encoding="utf-8") as _f:
                        for fp in all_deleted_files:
                            _f.write(fp + "\n")
                    ctx.emit_event("log", level="info",
                                   msg=(f"[S1.5] 归档 {len(all_deleted_files)} 个排除文件 "
                                        f"(来自 {len(removed_mod_names)} 个删除模块)"))
                shutil.rmtree(str(backup), ignore_errors=True)
                ctx.emit_event("log", level="info",
                               msg=(f"[安全维度过滤] 完成：保留 {len(kept_modules)} 个，"
                                    f"删除 {removed_count} 个，备份已清理"))
                ctx.classified_modules = discover_modules(str(workspace))
                if cp:
                    cp.mark_done("s1_security_filter",
                                 kept=len(kept_modules),
                                 removed=removed_count)
                return

            # ── 未通过：写完整 judge 意见到文件 + 提取结构化修正列表 ────────────
            if not max_reached:
                fb_rel = write_judge_feedback(
                    workspace, "s1_security", None, attempt + 1, judge_results)
                ctx.emit_event("log", level="info",
                               msg=f"[S1.5] judge 意见已写入 {fb_rel}")
                corrections_parts: list[str] = []
                for i, rec in enumerate(judge_records):
                    raw = rec.get("raw_output", "")
                    corrections_parts.append(
                        f"### Judge-{i}（分数 {rec['score']}）修正列表\n\n{raw}"
                    )
                judge_corrections = (
                    f"请先阅读 judge 完整意见：\n"
                    f"```\nread {fb_rel}\n```\n\n"
                    + "\n\n".join(corrections_parts)
                )

        raise StageError(
            f"安全维度过滤阶段未通过，已达最大轮数 {s_cfg.max_rounds}。"
            f"备份保留于 {backup} 供人工排查。"
        )
