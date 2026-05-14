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
from .filter_engine import normalize_filter_engine
from .helpers import (
    run_agent_with_stage_guard, parse_eval_md, check_voting,
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


# ── S1 deleted/recover/ lifecycle helpers ────────────────────────────────────

def _archive_s1_deleted(workspace):
    """Archive workspace/deleted/files.list -> workspace/deleted.list."""
    import shutil as _sh
    src_p = workspace / 'deleted' / 'files.list'
    if not src_p.exists():
        return 0
    lines = [ln.strip() for ln in src_p.read_text('utf-8', errors='replace').splitlines() if ln.strip()]
    if lines:
        with open(str(workspace / 'deleted.list'), 'a', encoding='utf-8') as _f:
            _f.write(chr(10).join(lines) + chr(10))
    _sh.rmtree(str(workspace / 'deleted'), ignore_errors=True)
    return len(lines)


def _pop_s1_recover(workspace):
    """Read & clear workspace/recover/files.list; return list for Worker feedback."""
    import shutil as _sh
    src_p = workspace / 'recover' / 'files.list'
    if not src_p.exists():
        return []
    lines = [ln.strip() for ln in src_p.read_text('utf-8', errors='replace').splitlines() if ln.strip()]
    _sh.rmtree(str(workspace / 'recover'), ignore_errors=True)
    return lines


def _clear_s1_deleted(workspace):
    """Clear workspace/deleted/ before retry."""
    import shutil as _sh
    _sh.rmtree(str(workspace / 'deleted'), ignore_errors=True)


def _build_recover_prompt(recover_files):
    """Build feedback section for files recovered from deleted/."""
    nl = chr(10)
    items = nl.join(f"  - {f}" for f in recover_files[:50])
    extra = f"（共 {len(recover_files)} 个）" if len(recover_files) > 50 else ""
    return (
        nl + nl
        + "# ⚠️ 必须重新分类的文件（上轮误放入 deleted/）" + nl + nl
        + "以下文件经 Judge 审核确认不应删除，已从 deleted/ 恢复，"
        + "**必须归入合适的安全维度模块，禁止再次写入 deleted/files.list**：" + nl
        + items + extra
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
        cp = ctx.checkpoint

        # ── Checkpoint 检查：已完成则跳过 ──────────────────────────────────────
        # 防御性规则：若 s1_classify 已标记完成，或后续阶段（s1_security_filter /
        # s2_refine）已完成（意味着 S1 必然成功过），则直接跳过本阶段。
        # 这避免了 resume 时因 s1_classify.done 缺失而重跑分类、覆盖 S2 成果。
        _downstream_done = cp and (
            cp.is_done("s1_security_filter")
            or cp.is_done("s2_refine")
            or cp.is_done("s3_analyse")
        )
        if (cp and cp.is_done("s1_classify")) or _downstream_done:
            reason = "s1_classify.done" if (cp and cp.is_done("s1_classify")) else "downstream_stage_done"
            ctx.emit_event("log", level="info",
                           msg=f"[S1-classify] checkpoint 已存在（{reason}），跳过分类重新执行，直接恢复模块列表")
            ctx.classified_modules = discover_modules(str(workspace))
            # 若 downstream 完成但 s1_classify.done 缺失，补写以防下次 resume 再次跳过
            if cp and not cp.is_done("s1_classify"):
                cp.mark_done("s1_classify", extra={"note": f"backfilled_by_resume_{reason}",
                                                    "module_count": len(ctx.classified_modules)})
                ctx.emit_event("log", level="info",
                               msg=f"[S1-classify] 已补写 s1_classify.done（{len(ctx.classified_modules)} 个模块）")
            return

        if normalize_filter_engine(getattr(cfg, "filter_engine", "script")) == "agent":
            modules = discover_modules(str(workspace))
            if modules:
                ctx.classified_modules = modules
                ctx.emit_event(
                    "stage_result",
                    stage=1,
                    status="skipped",
                    reason="agent_filter_engine_already_produced_modules",
                    modules=modules,
                    effective_engine=getattr(ctx, "effective_filter_engine", "agent"),
                )
                return

        classify_prompt = load_prompt(cfg, "step1_classify", "workers")
        check_prompt = load_prompt(cfg, "step1_check_classify", "judges")
        reflect_prompt = load_prompt(cfg, "reflect_classify", "workers")

        classify_session = ctx.session_path("classify.jsonl")
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
        ctx.emit_event(
            "log",
            level="info",
            msg=(
                "分类阶段安全过滤配置："
                f"security_focus_categories={list(cfg.security_focus_categories)}, "
                f"module_granularity={cfg.module_granularity}"
            ),
        )

        # ── 构建分类上下文注入（details/ 优先，fallback prescan 摘要）──
        prescan_summary = ctx.prescan_summary
        # ctx.classify_context_path 已由 orchestrator 初始化为 workspace/classify_context.md，永不为 None
        classify_context_path = ctx.classify_context_path
        has_classify_context = classify_context_path.exists()

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

            # Pre-retry: clear old deleted/, pop recover/ for feedback
            recover_files = []
            if attempt > 0:
                _clear_s1_deleted(workspace)
                recover_files = _pop_s1_recover(workspace)
                if recover_files:
                    ctx.emit_event("log", level="info",
                                   msg=f"[S1] pop recover/ {len(recover_files)} files for re-classification")
            # 构建 prompt
            prompt_parts = [ctx.task]
            # ⚠️ 注入工作目录绝对路径，防止 agent cd 到错误目录
            prompt_parts.append(
                f"\n\n# ⚠️ 工作目录（绝对禁止离开）\n\n"
                f"你的工作目录已固定为：`{workspace}`\n\n"
                f"- `filtered_files.txt` 完整路径：`{workspace}/filtered_files.txt`\n"
                f"- 预扫描数据目录：`{workspace}/prescan/`\n"
                f"- 文件详情目录：`{workspace}/details/`（每个文件的类型/摘要/符号表 JSON）\n"
                f"- 分类上下文：`{workspace}/classify_context.md`（按类型分组的文件汇总）\n"
                f"- 模块输出路径：`{workspace}/modules/<模块名>/files.list`\n\n"
                f"**严禁执行任何 `cd` 命令**。所有脚本必须使用绝对路径或相对当前工作目录的路径。"
            )
            # ── details/ 优先注入（比 prescan 更丰富的结构化信息）──────────────
            if attempt == 0 and has_classify_context:
                prompt_parts.append(
                    f"\n\n# 文件预处理信息（优先使用）\n\n"
                    f"`classify_context.md` 已包含按类型/建议模块分组的文件汇总，"
                    "请用 `read classify_context.md` 查看。\n\n"
                    "每个文件的详细信息（类型/摘要/符号表/函数名）在 `details/<path>.json`，"
                    "可用 `read details/<path>.json` 按需查阅。\n\n"
                    "**分类策略**：优先根据 details/ 中的功能摘要和符号信息归类，"
                    "无需再读原始文件（除非 details 中显示 [需补充]）。"
                )
            elif attempt == 0 and prescan_summary:
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
                    "**每个完整协议/服务/独立守护进程 → 一个模块**\n\n"
                    "判断口诀：问自己「这两组文件是否实现同一个 RFC 标准\n"
                    "或同一个守护进程？」"
                    " - 是 → 必须合并入同一模块\n"
                    " - 否（完全不同的协议/服务）→ 分为不同模块\n\n"
                    "**必须合并的场景：**\n"
                    "- 同协议的 client/server/config/parser/utils"
                    " → 必须合并，**不得**拆成多个模块\n"
                    "- 同协议族子协议变体（OSPFv2+OSPFv3→`ospf`；"
                    "ICMPv4+ICMPv6→`icmp`）\n\n"
                    "**正确示范 ✅**\n"
                    "- `ssh`（ssh_server+ssh_client+ssh_config）\n"
                    "- `ospf`（OSPFv2+OSPFv3+OSPF工具）\n"
                    "- `tls`（libssl+libcrypto+TLS握手实现）\n\n"
                    "**错误示范 ❌**\n"
                    "- `ssh_server`+`ssh_client`（同协议拆碎）\n"
                    "- `ospfv2`+`ospfv3`（同协议版本拆碎）\n"
                    "- 固件中有多少个协议"
                    "/功能就创建多少个模块，**不限制总模块数量**。"
                )

            # ── 路径先验分组指引（path_groups.md 由 PathGroupStage 生成）──
            if attempt == 0 and (workspace / "prescan" / "path_groups.md").exists():
                prompt_parts.append(
                    "\n\n# 路径先验分组（已由工具自动生成，请优先采用）\n\n"
                    "上方摘要显示了各路径组的文件数量。"
                    "完整文件列表已写入 `prescan/path_groups.md`，"
                    "直接通过 `read prescan/path_groups.md` 读取。\n"
                    "请根据模块名直接将对应路径组写入模块的 files.list，无需重新分析路径。"
                )

            if recover_files:
                prompt_parts.append(_build_recover_prompt(recover_files))
            if feedback:
                prompt_parts.append("\n\n" + feedback)

            ar = await run_agent_with_stage_guard(
                ctx=ctx,
                stage="classify",
                context=f"s1-classify-a{attempt+1}",
                heartbeat_payload_factory=lambda beat, attempt_no=attempt + 1: {
                    "attempt": attempt_no,
                    "heartbeat": beat,
                    "session_file": classify_session,
                },
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
                judge_session = ctx.session_path(
                    "judges",
                    "classify",
                    f"classify-a{attempt + 1}-j{j_idx}.jsonl",
                )
                judge_prompt = [f"审核分类结果。模块数：{len(modules)}。"]
                # 注意：Judge 不应检查安全维度相关性，那是 S1.5 的职责。
                # S1 Judge 只检查：所有 filtered_files.txt 中的文件是否已全部分类。
                if granularity == "coarse":
                    judge_prompt.append(
                        "\n\n# 模块粒度要求（粗粒度审核）\n\n"
                        "当前要求是粗粒度（协议/服务/功能级）。\n"
                        "验收重点：\n"
                        "1. 是否存在同一协议的碎片模块"
                        "（如 ssh_server 和 ssh_client 分开存在）"
                        "——若有则不通过\n"
                        "2. 是否存在同协议族子协议碎片"
                        "（如 ospfv2 和 ospfv3 分开存在）"
                        "——若有则不通过\n"
                        "3. 所有 filtered_files.txt 中的文件"
                        "是否已全部分类（覆盖率 100%）"
                    )
                j_ar = await run_agent_with_stage_guard(
                    ctx=ctx,
                    stage="classify",
                    context=f"s1-classify-judge{j_idx}",
                    heartbeat_payload_factory=lambda beat, attempt_no=attempt + 1, judge_id=j_idx, session=judge_session: {
                        "attempt": attempt_no,
                        "heartbeat": beat,
                        "judge_id": f"judge-{judge_id}",
                        "session_file": session,
                    },
                    prompt="\n\n".join(judge_prompt),
                    model=j_model,
                    tools=cfg.judges.default_tools,
                    system_prompt=check_prompt,
                    cwd=str(workspace),
                    thinking_level="off",
                    session_file=judge_session,
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
                    "session_file": judge_session,
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
                archived = _archive_s1_deleted(workspace)
                if archived:
                    ctx.emit_event("log", level="info",
                                   msg=f"[S1] archived {archived} proposed-deleted files")
                ctx.classified_modules = modules
                if cp:
                    cp.mark_done("s1_classify", extra={"module_count": len(modules), "modules": modules[:50]})
                return
            if forced_pass:
                _archive_s1_deleted(workspace)
                ctx.classified_modules = modules
                if cp:
                    cp.mark_done("s1_classify", extra={"module_count": len(modules), "modules": modules[:50],
                                                        "forced": True})
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
                # 问题6：注入增量修复指导，防止 Worker 全量重写分类脚本
                current_mods = discover_modules(str(workspace))
                total_classified = 0
                filtered_total = 0
                try:
                    fl = workspace / "filtered_files.txt"
                    if fl.exists():
                        filtered_total = sum(1 for l in fl.read_text("utf-8").splitlines() if l.strip())
                    for m in current_mods:
                        flist = get_modules_root(str(workspace)) / m / "files.list"
                        if flist.exists():
                            total_classified += sum(1 for l in flist.read_text("utf-8").splitlines() if l.strip())
                except Exception:
                    pass
                coverage = f"{total_classified}/{filtered_total}" if filtered_total > 0 else str(total_classified)
                incremental_guidance = (
                    f"\n\n## ⚠️ 增量修复要求（必读）\n\n"
                    f"当前状态：{len(current_mods)} 个模块，已分类 {coverage} 个文件。"
                    f"现有分类基本正确，**请不要重写整个分类脚本**。\n\n"
                    f"正确做法：只针对 judge 指出的遗漏文件做增量补充：\n"
                    f"```bash\n"
                    f"# 示例：只将遗漏文件追加到已有模块\n"
                    f"echo 'path/to/missing_file.c' >> modules/最合适的模块名/files.list\n"
                    f"# 然后重新校验覆盖率\n"
                    f"bash /app/scripts/check_classification.sh {cfg.target_dir} .\n"
                    f"```\n"
                )
                feedback = (
                    f"# 上轮评审不通过（第 {attempt+1} 轮）\n\n"
                    f"## Judge 上轮意见\n\n{fail_fb}\n\n"
                    + incremental_guidance
                    + reflect_prompt
                )

        raise StageError(f"Stage 1 分类未通过，已达最大轮数 {s_cfg.max_rounds}")
