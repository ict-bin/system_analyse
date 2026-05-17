"""
pipeline/self_reflection.py — 任务完成后自动触发的自省分析服务

流程：
  1. 任务结束（passed / failed / error）后由 task_service._execute_task() 调用 trigger_async()
  2. 在独立 asyncio.Task 中后台运行，不阻塞任务完成
  3. 从 run_dir（evaluation JSON）和 output_dir（最终报告）收集数据摘要
  4. 调用 LLM（无工具调用，纯推理）生成 Markdown 分析报告
  5. 写入 cfg.self_reflection.output_dir/{task_id}_{timestamp}.md
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.models import TaskConfig

logger = logging.getLogger("sa.self_reflection")

# 每个 session jsonl 中最多保留的行
_DEFAULT_MAX_SESSION_LINES = 1000
# 最终报告摘取字符数上限
_FINAL_REPORT_EXCERPT = 3000
# 每个模块报告摘取字符数上限
_MODULE_REPORT_EXCERPT = 500
# judge feedback 汇总字符上限
_FEEDBACK_TOTAL_LIMIT = 4000
# Top-N high-token rounds to include
_TOP_TOKEN_ROUNDS = 10


# ─── 数据收集 ──────────────────────────────────────────────────────────────────

def _collect_task_data(
    run_dir: Path,
    output_dir: Path,
    max_session_lines: int,
) -> dict:
    """从磁盘收集任务执行数据并压缩为可注入 LLM 的结构。"""
    data: dict = {
        "evaluation_summary": None,
        "stage_stats": [],          # 按 stage 聚合的统计
        "top_token_rounds": [],     # token 消耗最高的轮次
        "failed_feedback": [],      # 失败轮次的 judge feedback 摘要
        "session_tool_stats": {},   # session 中 tool call 统计
        "final_report_excerpt": "",
        "failed_module_reports": [],
    }

    # 1. evaluation_summary.json
    summary_path = run_dir / "evaluation_summary.json"
    if summary_path.exists():
        try:
            data["evaluation_summary"] = json.loads(summary_path.read_text("utf-8", errors="replace"))
        except Exception:
            pass

    # 2. round_*.json 文件聚合
    all_rounds: list[dict] = []
    for round_dir in sorted(run_dir.glob("round_*")):
        if not round_dir.is_dir():
            continue
        for path in sorted(round_dir.glob("*.json")):
            if path.name.endswith(".tmp"):
                continue
            try:
                payload = json.loads(path.read_text("utf-8", errors="replace"))
                if isinstance(payload, dict):
                    all_rounds.append(payload)
            except Exception:
                pass

    # 按 stage 聚合统计
    stage_map: dict[str, dict] = {}
    for r in all_rounds:
        stage = str(r.get("stage", "unknown"))
        if stage not in stage_map:
            stage_map[stage] = {
                "stage": stage,
                "round_count": 0,
                "total_tokens": 0,
                "total_duration_ms": 0.0,
                "passed_count": 0,
                "avg_score_sum": 0.0,
                "judge_feedback_failed": [],
            }
        s = stage_map[stage]
        s["round_count"] += 1
        metrics = r.get("metrics", {})
        s["total_tokens"] += int(metrics.get("token_total") or 0)
        s["total_duration_ms"] += float(r.get("duration_ms") or 0.0)
        if metrics.get("passed_by_vote"):
            s["passed_count"] += 1
        s["avg_score_sum"] += float(metrics.get("avg_judge_score") or 0.0)
        # collect failed judge feedback
        if not metrics.get("passed_by_vote"):
            for j in r.get("judges", []):
                fb = str(j.get("feedback_excerpt") or "")[:300]
                if fb:
                    s["judge_feedback_failed"].append(
                        f"[{stage}/{r.get('module_name','?')}] {fb}"
                    )

    # Build stage_stats list
    for s in stage_map.values():
        cnt = s["round_count"]
        s["pass_rate"] = s["passed_count"] / cnt if cnt > 0 else 0.0
        s["avg_score"] = s["avg_score_sum"] / cnt if cnt > 0 else 0.0
        del s["avg_score_sum"]
        data["stage_stats"].append(s)

    data["stage_stats"].sort(key=lambda x: -x["total_tokens"])

    # Top-N rounds by token
    rounds_sorted = sorted(
        all_rounds,
        key=lambda r: int((r.get("metrics") or {}).get("token_total") or 0),
        reverse=True,
    )
    for r in rounds_sorted[:_TOP_TOKEN_ROUNDS]:
        metrics = r.get("metrics", {})
        data["top_token_rounds"].append({
            "stage": r.get("stage"),
            "module_name": r.get("module_name"),
            "stage_round": r.get("stage_round"),
            "tokens": int(metrics.get("token_total") or 0),
            "duration_ms": float(r.get("duration_ms") or 0.0),
            "passed": bool(metrics.get("passed_by_vote")),
            "avg_score": float(metrics.get("avg_judge_score") or 0.0),
        })

    # Failed feedback summary (truncated total)
    total_fb_chars = 0
    for s in data["stage_stats"]:
        for fb in s.get("judge_feedback_failed", []):
            if total_fb_chars < _FEEDBACK_TOTAL_LIMIT:
                data["failed_feedback"].append(fb)
                total_fb_chars += len(fb)
        s.pop("judge_feedback_failed", None)  # remove from stage_stats to keep clean

    # 3. Session files — tool call statistics
    sessions_dir = run_dir / "sessions"
    if sessions_dir.is_dir():
        tool_call_counts: dict[str, int] = {}
        for sf in sorted(sessions_dir.rglob("*.jsonl")):
            lines_read = 0
            try:
                with open(sf, encoding="utf-8", errors="replace") as fh:
                    for raw_line in fh:
                        if lines_read >= max_session_lines:
                            break
                        lines_read += 1
                        line = raw_line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                        except Exception:
                            continue
                        # Only look at messages with toolCall content
                        if not isinstance(obj, dict):
                            continue
                        role = obj.get("role", "")
                        if role == "assistant":
                            content = obj.get("content", [])
                            if isinstance(content, list):
                                for item in content:
                                    if isinstance(item, dict) and item.get("type") == "toolCall":
                                        name = item.get("name", "unknown")
                                        tool_call_counts[name] = tool_call_counts.get(name, 0) + 1
            except Exception:
                pass
        data["session_tool_stats"] = tool_call_counts

    # 4. Final report excerpt
    final_report = output_dir / "final_report.md"
    if final_report.exists():
        try:
            data["final_report_excerpt"] = final_report.read_text(
                "utf-8", errors="replace"
            )[:_FINAL_REPORT_EXCERPT]
        except Exception:
            pass

    # 5. Low-score module reports (score < 70 or no report)
    modules_dir = output_dir / "modules"
    if modules_dir.is_dir():
        for mod_dir in sorted(modules_dir.iterdir()):
            if not mod_dir.is_dir():
                continue
            report = mod_dir / "module_report.md"
            if not report.exists():
                data["failed_module_reports"].append(
                    {"module": mod_dir.name, "content": "(无报告)"}
                )
            else:
                try:
                    text = report.read_text("utf-8", errors="replace")
                    # Only include low-score modules
                    import re as _re
                    m = _re.search(r"RISK_SCORE:\s*(\d+)", text)
                    score = int(m.group(1)) if m else 50
                    if score < 60:
                        data["failed_module_reports"].append({
                            "module": mod_dir.name,
                            "score": score,
                            "content": text[:_MODULE_REPORT_EXCERPT],
                        })
                except Exception:
                    pass

    return data


def _build_analysis_prompt(task_id: str, task_status: str, data: dict) -> str:
    """将收集的数据格式化为 LLM 分析 prompt（用户消息部分）。"""
    lines: list[str] = []
    lines.append(f"# 待分析任务\n\n**task_id**: `{task_id}`  **status**: `{task_status}`\n")

    # 1. evaluation_summary
    summary = data.get("evaluation_summary") or {}
    if summary:
        eff = summary.get("effectiveness", {})
        lines.append("## 任务总览\n")
        lines.append("| 指标 | 值 |")
        lines.append("|------|-----|")

        def _fmt_ms(ms: float) -> str:
            if ms >= 3600000:
                return f"{ms/3600000:.1f}h"
            if ms >= 60000:
                return f"{ms/60000:.1f}min"
            return f"{ms/1000:.1f}s"

        lines.append(f"| 总 Token | {summary.get('total_tokens', 0):,} |")
        lines.append(f"| 总耗时 | {_fmt_ms(float(summary.get('total_duration_ms') or 0))} |")
        lines.append(f"| 总轮数 | {summary.get('round_count', 0)} |")
        lines.append(f"| 模块总数 | {summary.get('module_count', 0)} |")
        lines.append(f"| 完成模块数 | {summary.get('completed_module_count', 0)} |")
        lines.append(f"| 失败模块数 | {summary.get('failed_module_count', 0)} |")
        lines.append(f"| 首轮通过率 | {eff.get('first_round_pass_rate', 0):.1%} |")
        lines.append(f"| 最终通过率 | {eff.get('final_module_pass_rate', 0):.1%} |")
        lines.append(f"| Reclassify 次数 | {eff.get('reclassify_count', 0)} |")
        lines.append(f"| 反思轮数 | {eff.get('reflection_round_count', 0)} |")
        lines.append("")

    # 2. stage stats table
    stage_stats = data.get("stage_stats", [])
    if stage_stats:
        lines.append("## 各阶段统计\n")
        lines.append("| 阶段 | 轮数 | 通过率 | 平均分 | Token 合计 | 平均耗时 |")
        lines.append("|------|------|--------|--------|-----------|---------|")
        for s in stage_stats:
            cnt = s["round_count"]
            avg_dur = s["total_duration_ms"] / cnt / 1000 if cnt > 0 else 0
            lines.append(
                f"| {s['stage']} | {cnt} | {s['pass_rate']:.1%} | "
                f"{s['avg_score']:.1f} | {s['total_tokens']:,} | {avg_dur:.1f}s |"
            )
        lines.append("")

    # 3. Top token rounds
    top_rounds = data.get("top_token_rounds", [])
    if top_rounds:
        lines.append("## Token 消耗 Top Rounds\n")
        lines.append("| 阶段 | 模块 | 轮次 | Token | 耗时(s) | 通过 | 评分 |")
        lines.append("|------|------|------|-------|---------|------|------|")
        for r in top_rounds:
            lines.append(
                f"| {r['stage']} | {r['module_name'] or '-'} | {r['stage_round']} | "
                f"{r['tokens']:,} | {r['duration_ms']/1000:.1f} | "
                f"{'✅' if r['passed'] else '❌'} | {r['avg_score']:.1f} |"
            )
        lines.append("")

    # 4. Failed feedback
    failed_fb = data.get("failed_feedback", [])
    if failed_fb:
        lines.append("## Judge 失败 Feedback 摘要\n")
        for fb in failed_fb[:20]:
            lines.append(f"- {fb}")
        lines.append("")

    # 5. Session tool stats
    tool_stats = data.get("session_tool_stats", {})
    if tool_stats:
        lines.append("## 工具调用统计（来自 session.jsonl）\n")
        for tool, cnt in sorted(tool_stats.items(), key=lambda x: -x[1]):
            lines.append(f"- `{tool}`: {cnt} 次")
        lines.append("")

    # 6. Final report excerpt
    excerpt = data.get("final_report_excerpt", "")
    if excerpt:
        lines.append("## 最终报告片段（前 3000 字）\n")
        lines.append("```markdown")
        lines.append(excerpt)
        lines.append("```\n")

    # 7. Low-score module reports
    failed_mods = data.get("failed_module_reports", [])
    if failed_mods:
        lines.append(f"## 低分/缺失报告模块（共 {len(failed_mods)} 个）\n")
        for m in failed_mods[:5]:
            lines.append(f"### {m['module']} (score={m.get('score', 'N/A')})\n")
            lines.append("```")
            lines.append(m.get("content", ""))
            lines.append("```\n")

    return "\n".join(lines)


# ─── 服务类 ───────────────────────────────────────────────────────────────────

class SelfReflectionService:
    """任务完成后异步触发的自省分析服务。"""

    async def trigger_async(
        self,
        task_id: str,
        run_dir: Path,
        output_dir: Path,
        cfg: "TaskConfig",
        task_status: str,
    ) -> None:
        """非阻塞入口 — 在独立 asyncio.Task 中运行，不影响主流水线。"""
        if not cfg.self_reflection.enabled:
            return
        if task_status == "cancelled":
            return  # 取消任务不做自省
        asyncio.create_task(
            self._run_reflection(task_id, run_dir, output_dir, cfg, task_status),
            name=f"self-reflection-{task_id}",
        )

    async def _run_reflection(
        self,
        task_id: str,
        run_dir: Path,
        output_dir: Path,
        cfg: "TaskConfig",
        task_status: str,
    ) -> None:
        """实际的自省分析逻辑。"""
        from app.runner import run_agent  # 延迟导入避免循环

        logger.info("[self-reflection] 开始分析任务 %s (status=%s)", task_id, task_status)
        try:
            # 确定使用的模型
            sr_cfg = cfg.self_reflection
            model = sr_cfg.model.strip()
            if not model:
                model = cfg.workers.agents[0].model if cfg.workers.agents else ""
            if not model:
                logger.warning("[self-reflection] 无可用模型，跳过 %s", task_id)
                return

            # 收集数据
            max_lines = sr_cfg.max_session_lines
            data = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: _collect_task_data(run_dir, output_dir, max_lines),
            )

            # 准备输出路径（由 config_service.get_config 已填充为项目级路径）
            out_dir = Path(sr_cfg.output_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = out_dir / f"{task_id}_{ts}.md"

            # 加载 prompt
            prompt_path = Path("/app/prompts/self_reflection_analysis.md")
            system_prompt = (
                prompt_path.read_text("utf-8", errors="replace")
                if prompt_path.exists()
                else "分析以下任务执行数据，输出 Markdown 格式的改进建议报告。"
            )

            # 构建用户消息
            user_prompt = _build_analysis_prompt(task_id, task_status, data)

            # ★ 使用独立 cancel_event（不共用主任务的取消信号）
            cancel_ev = asyncio.Event()

            # 调用 LLM（无工具调用，纯推理）
            agent_task = asyncio.create_task(
                run_agent(
                    prompt=user_prompt,
                    model=model,
                    system_prompt=system_prompt,
                    tools=[],           # 纯推理，无工具
                    session_file=None,  # 无 session，每次全新 context
                    thinking_level="off",
                    max_retries=3,
                    retry_delay=10,
                    pi_max_retries=1,
                    pi_retry_delay=5,
                    cancel_event=cancel_ev,
                )
            )
            timeout_seconds = max(60.0, float(getattr(cfg, "agent_timeout_seconds", 1800.0) or 1800.0))
            try:
                ar = await asyncio.wait_for(agent_task, timeout=timeout_seconds)
            except asyncio.TimeoutError:
                cancel_ev.set()
                agent_task.cancel()
                await asyncio.gather(agent_task, return_exceptions=True)
                logger.warning(
                    "[self-reflection] 任务 %s 自省分析超时（%.1fs），已终止",
                    task_id,
                    timeout_seconds,
                )
                return

            # 写入报告
            content = ar.output or "(LLM 未返回输出)"
            report_path.write_text(content, encoding="utf-8")
            logger.info(
                "[self-reflection] 报告已生成: %s (%.1f KB, tokens=%s)",
                report_path,
                len(content) / 1024,
                ar.token_usage.input + ar.token_usage.output,
            )

        except Exception as exc:
            logger.warning(
                "[self-reflection] 任务 %s 自省分析失败（不影响任务结果）: %s",
                task_id, exc,
            )


# 模块级单例
_service: SelfReflectionService | None = None


def get_self_reflection_service() -> SelfReflectionService:
    global _service
    if _service is None:
        _service = SelfReflectionService()
    return _service
