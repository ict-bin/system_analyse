"""
pipeline/s4_report.py — Stage 4: 最终报告

入: ctx.analysed_modules
    workspace/modules/*/module_report.md
出: ctx.final_report_path
    final_out_dir/final_report.md
    final_out_dir/modules/
    final_out_dir/modules.list
    final_out_dir/archive.zip（在 run_dir）

核心流程:
  Stage 4a: Judge 完整性检查
    → 缺失模块回 Stage 2+3 补做（_redo_missing）
  Stage 4b: Worker(step4_final_report.md) 生成总报告
    → Judge(step4_check_report.md) 评审
  后处理: 生成 modules.list, 归档 zip, 写 flag=1
"""
from __future__ import annotations

import subprocess
import re
import shutil
import time
from pathlib import Path

from app.copy_utils import safe_copy2
from .base import BaseStage
from .context import PipelineContext
from .evaluation import utc_now_iso
from .helpers import (
    run_agent_with_stage_guard, parse_eval_md, check_voting,
    discover_modules, get_modules_root, load_prompt, load_granularity_prompt,
    archive_file, max_iter, pre_read_module, pre_read_module_with_details,
    generate_modules_list, strip_target_prefix, write_judge_feedback,
    StageError, PiFatalError, max_rounds_exceeded_treated_as_passed,
    enforce_filter_constraint,
)
from .module_dependency import build_module_dependency_graph


def _extract_first(pattern: str, text: str, default: str = "") -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else default


def _ensure_report_generation_marker(report_path: Path, generation_type: str) -> None:
    if not report_path.exists():
        return
    text = report_path.read_text("utf-8", errors="replace")
    if "REPORT_GENERATION_TYPE:" in text[:1000]:
        return
    normalized = "program" if generation_type == "program" else "ai"
    label = "程序汇总报告" if normalized == "program" else "AI 汇总报告"
    description = (
        "该报告由系统分析服务根据各模块 module_report.md 自动汇总生成。"
        if normalized == "program"
        else "该报告由最终报告智能体读取模块级分析结果后汇总生成。"
    )
    prefix = (
        f"<!-- REPORT_GENERATION_TYPE: {normalized} -->\n"
        f"<!-- REPORT_GENERATION_LABEL: {label} -->\n\n"
        f"> **报告生成方式：{label}**。{description}\n\n"
    )
    report_path.write_text(prefix + text, encoding="utf-8")


def _write_fallback_final_report(workspace: Path, modules: list[str]) -> bool:
    """Write a deterministic final report if the LLM stopped before creating it."""
    if not modules:
        return False

    rows: list[dict[str, object]] = []
    for module_name in modules:
        report_path = get_modules_root(str(workspace)) / module_name / "module_report.md"
        if not report_path.exists():
            continue
        text = report_path.read_text("utf-8", errors="replace")
        risk_level = _extract_first(r"RISK_LEVEL:\s*([^>\n]+)", text, "未知")
        risk_score_text = _extract_first(r"RISK_SCORE:\s*(\d+)", text, "0")
        try:
            risk_score = int(risk_score_text)
        except ValueError:
            risk_score = 0
        rows.append({
            "module": module_name,
            "risk_level": risk_level,
            "risk_score": risk_score,
            "high": text.count("🔴"),
            "medium": text.count("🟡"),
            "low": text.count("🟢"),
        })

    if not rows:
        return False

    rows.sort(key=lambda item: (-int(item["risk_score"]), str(item["module"])))
    high_modules = [item for item in rows if str(item["risk_level"]).startswith("高")]
    medium_modules = [item for item in rows if str(item["risk_level"]).startswith("中")]
    total_high = sum(int(item["high"]) for item in rows)
    total_medium = sum(int(item["medium"]) for item in rows)
    total_low = sum(int(item["low"]) for item in rows)

    lines = [
        "<!-- REPORT_GENERATION_TYPE: program -->",
        "<!-- REPORT_GENERATION_LABEL: 程序汇总报告 -->",
        "",
        "# 系统安全分析最终报告",
        "",
        "> **报告生成方式：程序汇总报告**。该报告由系统分析服务根据各模块 `module_report.md` 自动汇总生成。原因：最终报告智能体未在本轮执行中写出 `final_report.md`，系统使用已完成的模块级分析结果生成兜底总报告。",
        "",
        "## 1. 总览",
        "",
        f"- 已发现 {len(rows)} 个模块",
        f"- 高风险模块：{len(high_modules)} 个",
        f"- 中风险模块：{len(medium_modules)} 个",
        f"- 高风险威胁标记：{total_high} 个",
        f"- 中风险威胁标记：{total_medium} 个",
        f"- 低风险威胁标记：{total_low} 个",
        "",
        "## 2. 风险最高模块",
        "",
        "| 排名 | 模块 | 风险等级 | 风险评分 | 高/中/低风险标记 |",
        "|---:|---|---|---:|---|",
    ]
    for rank, item in enumerate(rows[:20], start=1):
        lines.append(
            f"| {rank} | `{item['module']}` | {item['risk_level']} | "
            f"{item['risk_score']} | {item['high']} / {item['medium']} / {item['low']} |"
        )

    lines.extend([
        "",
        "## 3. 全量模块清单",
        "",
        "| 模块 | 风险等级 | 风险评分 | 高/中/低风险标记 |",
        "|---|---|---:|---|",
    ])
    for item in rows:
        lines.append(
            f"| `{item['module']}` | {item['risk_level']} | "
            f"{item['risk_score']} | {item['high']} / {item['medium']} / {item['low']} |"
        )

    lines.extend([
        "",
        "## 4. 后续建议",
        "",
        "1. 优先复核风险评分最高的模块及其高风险 STRIDE 条目。",
        "2. 对网络暴露面、认证授权、明文协议、输入解析和资源耗尽类风险进行专项验证。",
        "3. 如需更完整的自然语言总结，可在修复最终报告智能体输出后重试最终报告阶段。",
    ])

    (workspace / "final_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return True


class CompletenessCheckStage(BaseStage):
    """Stage 4a: 完整性检查（缺失模块回 Stage 2+3 补做）"""

    stage_num = 4
    stage_name = "完整性检查"

    def execute(self, ctx: PipelineContext) -> None:
        cfg = ctx.cfg
        workspace = ctx.workspace

        if not getattr(cfg, "enable_final_check", False):
            ctx.emit_event("stage", stage="4a", skipped=True, reason="disabled")
            return

        j_completeness_prompt = load_prompt(cfg, "step4_check_completeness", "judges")
        j_base = ctx.make_j_base()

        ctx.emit_event("stage", stage="4a")
        judge_results = []
        missing_modules: list[str] = []

        for j_idx, j_item in enumerate(ctx.j_cfgs):
            judge_session = ctx.session_path(
                "judges",
                "report-completeness",
                f"s4a-j{j_idx}.jsonl",
            )
            j_ar = run_agent_with_stage_guard(
                ctx=ctx,
                stage="4a",
                context=f"s4a-judge-j{j_idx}",
                heartbeat_payload_factory=lambda beat, judge_id=j_idx, session=judge_session: {
                    "heartbeat": beat,
                    "judge_id": f"judge-{judge_id}",
                    "session_file": session,
                },
                prompt="运行 check_outputs.sh 检查所有模块是否都有 module_report.md。",
                model=ctx.jm("completeness", j_item),
                system_prompt=j_completeness_prompt,
                tools=cfg.judges.default_tools,
                cwd=str(workspace),
                session_file=judge_session,
                **j_base,
            )
            ctx.tokens += j_ar.token_usage
            parsed = parse_eval_md(j_ar.output or "")
            judge_results.append(parsed)
            ctx.emit_event("judge_eval", stage="4a", judge_id=f"judge-{j_idx}",
                           passed=parsed["pass"], score=parsed["score"])
            archive_file(
                ctx.output_dir,
                f"s4a-j{j_idx}.md",
                f"Score: {parsed['score']}\nPass: {parsed['pass']}\n\n"
                f"{parsed['feedback']}\n\n---\n## Raw Output\n\n{j_ar.output[:3000]}",
            )
            if not parsed["pass"]:
                for m in re.findall(r'\u274c\s+(\S+)', j_ar.output or ""):
                    if m not in missing_modules:
                        missing_modules.append(m)

        s4a_pass = check_voting(judge_results, "all", ctx.j_count)

        if not s4a_pass and missing_modules:
            self._redo_missing(ctx, missing_modules)

    # ── 补做缺失模块的 Stage 2+3 ──────────────────────────────────────────
    def _redo_missing(self, ctx: PipelineContext, missing_modules: list[str]) -> None:
        cfg = ctx.cfg
        workspace = ctx.workspace
        mods_root = get_modules_root(str(workspace))

        ctx.emit_event("stage", stage="2-redo-s4", modules=missing_modules)

        s_cfg_refine = cfg.stages.refine
        s_cfg_analyse = cfg.stages.analyse
        granularity = getattr(cfg, "module_granularity", "fine") or "fine"
        w_sys_refine = load_granularity_prompt(cfg, "step2_refine", granularity, "workers")
        w_sys_analyse = load_granularity_prompt(cfg, "step3_analyse", granularity, "workers")
        j_sys_analyse = load_granularity_prompt(cfg, "step3_check_analyse", granularity, "judges")
        w_base = ctx.make_w_base()
        j_base = ctx.make_j_base()

        for mod_name in missing_modules:
            mod_dir = mods_root / mod_name
            if not mod_dir.exists() or not (mod_dir / "files.list").exists():
                continue
            try:
                # Stage 2 补做
                refine_session = ctx.session_path("refine-s4", f"{mod_name}.jsonl")
                ar = run_agent_with_stage_guard(
                    ctx=ctx,
                    stage="2-redo-s4",
                    context=f"s4-s2-redo-{mod_name}",
                    heartbeat_payload_factory=lambda beat, module=mod_name, session=refine_session: {
                        "module": module,
                        "heartbeat": beat,
                        "session_file": session,
                    },
                    model=ctx.wm("refine"),
                    prompt=f"检查模块 `{mod_name}` 是否需要细分。",
                    system_prompt=w_sys_refine,
                    session_file=refine_session,
                    **w_base,
                )
                ctx.tokens += ar.token_usage

                # Stage 3 补做（预读内容，优先复用 details/ JSON）
                loop = __import__("asyncio").get_event_loop()
                _details_dir_opt = ctx.details_dir if ctx.details_dir.exists() else None
                pre_content = loop.run_in_executor(
                    None, pre_read_module_with_details,
                    cfg.target_dir, mod_dir, _details_dir_opt
                )
                w_sys_s4 = w_sys_analyse.replace("{{PRE_READ_CONTENT}}", pre_content) \
                                         .replace("{{MODULE_NAME}}", mod_name)

                analyse_session = ctx.session_path("analyse-s4", f"{mod_name}.jsonl")
                feedback = ""
                for attempt in range(max_iter(s_cfg_analyse)):
                    prompt_parts = [
                        f"将模块 `{mod_name}` 的分析报告写入 `modules/{mod_name}/module_report.md`。",
                        "文件内容已在 system prompt 中提供。",
                    ]
                    if feedback:
                        prompt_parts.append(f"\n\n{feedback}")
                    ar = run_agent_with_stage_guard(
                        ctx=ctx,
                        stage="3-redo-s4",
                        context=f"s4-s3-redo-{mod_name}-a{attempt+1}",
                        heartbeat_payload_factory=lambda beat, module=mod_name, attempt_no=attempt + 1, session=analyse_session: {
                            "module": module,
                            "attempt": attempt_no,
                            "heartbeat": beat,
                            "session_file": session,
                        },
                        model=ctx.wm("analyse"),
                        prompt="\n".join(prompt_parts),
                        system_prompt=w_sys_s4,
                        tools=["write"],
                        session_file=analyse_session,
                        cwd=str(workspace),
                        cancel_event=ctx.cancel_event,
                        max_retries=1,
                        retry_delay=0,
                        pi_max_retries=cfg.pi_max_retries,
                        pi_retry_delay=cfg.pi_retry_delay,
                    )
                    ctx.tokens += ar.token_usage

                    if ctx.filtered_files:
                        _rm = enforce_filter_constraint(workspace, set(ctx.filtered_files))
                        if _rm:
                            ctx.emit_event("log", level="warn",
                                           msg=f"[S4-redo过滤] 补先移除 {_rm} 个越界条目")

                    judge_results = []
                    for j_idx, j_item in enumerate(ctx.j_cfgs):
                        judge_session = ctx.session_path(
                            "judges",
                            "analyse-s4",
                            mod_name,
                            f"analyse-s4-a{attempt + 1}-j{j_idx}.jsonl",
                        )
                        j_ar = run_agent_with_stage_guard(
                            ctx=ctx,
                            stage="3-redo-s4",
                            context=f"s4-s3-judge-{mod_name}-j{j_idx}-a{attempt+1}",
                            heartbeat_payload_factory=lambda beat, module=mod_name, attempt_no=attempt + 1, judge_id=j_idx, session=judge_session: {
                                "module": module,
                                "attempt": attempt_no,
                                "heartbeat": beat,
                                "judge_id": f"judge-{judge_id}",
                                "session_file": session,
                            },
                            prompt=f"评审模块 `{mod_name}` 的分析报告。",
                            model=ctx.jm("analyse", j_item),
                            system_prompt=j_sys_analyse,
                            tools=cfg.judges.default_tools,
                            cwd=str(workspace),  # workspace根, 避免双重modules/路径
                            session_file=judge_session,
                            **j_base,
                        )
                        ctx.tokens += j_ar.token_usage
                        parsed = parse_eval_md(j_ar.output or "")
                        judge_results.append(parsed)
                        ctx.emit_event("judge_eval", stage="3-redo-s4", judge_id=f"judge-{j_idx}",
                                       module=mod_name, passed=parsed["pass"], score=parsed["score"])

                    if check_voting(judge_results, s_cfg_analyse.pass_mode, ctx.j_count):
                        break
                    fb_redo = write_judge_feedback(
                        workspace, "s3_analyse", mod_name, attempt + 1, judge_results)
                    feedback = f"评审未通过，完整意见请 read {fb_redo}"
                else:
                    raise StageError(f"Stage 4a 补做模块 {mod_name} 分析未通过")
            except PiFatalError:
                raise
            except StageError as exc:
                if ctx.continue_on_module_failure:
                    ctx.record_soft_module_failure(
                        stage="3-redo-s4",
                        module_name=mod_name,
                        error=str(exc),
                        session_file=ctx.session_path("analyse-s4", f"{mod_name}.jsonl"),
                        artifact_paths=[str(mod_dir / "module_report.md")],
                        extra={"soft_failed": True, "from_stage": "s4_completeness_redo"},
                    )
                    continue
                raise


class FinalReportStage(BaseStage):
    """Stage 4b: 生成最终安全分析报告 + 输出归档"""

    stage_num = 5
    stage_name = "生成报告"

    def execute(self, ctx: PipelineContext) -> None:
        cfg = ctx.cfg
        workspace = ctx.workspace
        final_out_dir = ctx.final_out_dir

        # ── 0 模块快速路径：安全过滤后无相关模块，跳过 LLM，直接进行输出组装 ────────
        _sec_cats: list = getattr(cfg, "security_focus_categories", ["all"])
        _all_mods = discover_modules(str(workspace))
        _zero_modules_mode = bool(not _all_mods and "all" not in _sec_cats)
        if _zero_modules_mode:
            _no_mod_report = (
                f"# 分析任务已完成（过滤后没有符合要求的模块）\n\n"
                f"经 Stage 1.5 安全维度过滤，目标中所有模块均与指定安全维度无关，无需进行后续分析。\n\n"
                f"## 指定安全维度\n\n"
                + "\n".join(f"- `{c}`" for c in _sec_cats)
                + "\n\n目标中不包含与指定安全维度相关的组件。"
                f"若需分析全量内容，可将 `security_focus_categories` 设置为 `[\"all\"]` 重新运行任务。\n"
            )
            (workspace / "final_report.md").write_text(_no_mod_report, encoding="utf-8")
            ctx.emit_event("log", level="info",
                           msg="[S4b] 0 个安全相关模块，已写入说明报告，跳过 LLM，继续组装输出目录")
        else:
            # ── 0 模块快速路径：安全过滤后无相关模块，无需运行任何 LLM——直接写说明文件 ────────
            _sec_cats: list = getattr(cfg, "security_focus_categories", ["all"])
            _all_mods = discover_modules(str(workspace))
            if not _all_mods and "all" not in _sec_cats:
                _no_mod_report = (
                    f"# 分析任务已完成（安全过滤后无相关模块）\n\n"
                    f"经 Stage 1.5 安全维度过滤，目标中所有模块均与指定安全维度无关，"
                    f"无需进行后续分析。\n\n"
                    f"## 指定安全维度\n\n"
                    + "\n".join(f"- `{c}`" for c in _sec_cats)
                    + f"\n\n## 结论\n\n"
                    f"目标中不包含与指定安全维度相关的组件。"
                    f"若需分析全量内容，可将 `security_focus_categories` 设置为 `[\"all\"]` 重新运行任务。\n"
                )
                (workspace / "final_report.md").write_text(_no_mod_report, encoding="utf-8")
                ctx.emit_event("log", level="info",
                               msg="[S4b] 0 个安全相关模块，已写入说明报告，跳过 LLM")
                # 后续干跡存模块目录和模块列表
                from .helpers import discover_modules as _dm  # noqa
                return

            # ── 程序化最终报告合并 ─────────────────────────────────────────────
            # 最终报告不再由 LLM 生成/评审驱动，避免全局 Judge 失败导致所有模块集体重做。
            # 模块级质量问题应在 S3 的 per-module Judge 中解决；这里仅确定性合并
            # 已存在的 module_report.md。
            report_modules = discover_modules(str(workspace))
            if not report_modules:
                raise StageError("Stage 4b: modules/ 为空，无法生成报告（不应到达此处）")
            if _write_fallback_final_report(workspace, report_modules):
                ctx.emit_event(
                    "log",
                    level="info",
                    msg="[S4b] 已由程序根据各模块 module_report.md 合并生成最终报告，跳过最终报告 LLM/Judge",
                )
            if not (workspace / "final_report.md").exists():
                raise StageError("Stage 4b 程序化最终报告合并失败，final_report.md 未生成")
            _ensure_report_generation_marker(workspace / "final_report.md", "program")

        # ── 组装输出目录 ──────────────────────────────────────────────────────
        final_mods = discover_modules(str(workspace))

        # modules/ — 分类后的模块文件夹（files.list + module_report.md）
        modules_out = final_out_dir / "modules"
        if modules_out.exists():
            shutil.rmtree(str(modules_out))
        modules_out.mkdir(parents=True, exist_ok=True)
        for mod in final_mods:
            src = get_modules_root(str(workspace)) / mod
            dst = modules_out / mod
            if src.is_dir():
                shutil.copytree(str(src), str(dst))

        # module dependency graph — 依赖图持久化为 SQLite + JSON，供后端查询和前端渲染
        try:
            graph = build_module_dependency_graph(
                workspace=workspace,
                details_dir=ctx.details_dir,
                sqlite_path=final_out_dir / "module_dependency_graph.sqlite",
                json_path=final_out_dir / "module_dependency_graph.json",
            )
            ctx.emit_event(
                "stage_result",
                stage="module_dependency_graph",
                module_count=graph.get("summary", {}).get("module_count", 0),
                edge_count=graph.get("summary", {}).get("edge_count", 0),
            )
        except Exception as exc:
            ctx.emit_event("log", level="warn", msg=f"[依赖图] 生成失败: {exc}")

        # final_report.md
        report_src = workspace / "final_report.md"
        report_dst = final_out_dir / "final_report.md"
        if report_src.exists():
            safe_copy2(str(report_src), str(report_dst))
        ctx.final_report_path = str(report_dst)

        # modules.list — 按风险等级排序
        generate_modules_list(modules_out, final_out_dir / "modules.list")

        # 路径清洗 — 去除容器内绝对路径前缀
        strip_target_prefix(modules_out, cfg.target_dir)
        if report_dst.exists():
            strip_target_prefix(report_dst.parent, cfg.target_dir)
