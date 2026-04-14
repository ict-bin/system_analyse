"""
system_analyse — 多 Agent 编排核心

Worker 两阶段:
  Phase A: 文件分类 → 模块子目录
  Phase B: 逐模块分析（每次 fork Phase A 上下文，模块间互不影响）

Judge 三步:
  Step 1: 文件分类完整性检查
  Step 2: 逐模块评审（每模块新上下文）
  Step 3: 综合评分（所有模块必须通过）
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import re
import shutil
import time
from collections import Counter
from pathlib import Path
from typing import Callable

from .config import load_system_prompts, resolve_system_prompt
from .models import (
    AgentInstanceConfig,
    JudgeRoundResult,
    JudgeSummary,
    ModuleEvaluation,
    RoundResult,
    SwarmEvent,
    TaskConfig,
    TaskResult,
    TaskStatus,
    TokenUsage,
    WorkerEvaluation,
    WorkerResult,
    make_id,
)
from .runner import run_agent, run_agents_parallel

WORKER_CONCURRENCY = 8
JUDGE_CONCURRENCY = 8


# ─── 解析工具 ─────────────────────────────────────────────────────────────────

def _extract_result(output: str) -> str:
    m = re.search(r"<result>(.*?)</result>", output, re.DOTALL)
    return m.group(1).strip() if m else output


def _discover_modules(worker_output_dir: str) -> list[str]:
    """发现 Worker 输出目录中的模块子文件夹。"""
    modules = []
    out = Path(worker_output_dir)
    if out.is_dir():
        for d in sorted(out.iterdir()):
            if d.is_dir() and not d.name.startswith("."):
                modules.append(d.name)
    return modules


def _extract_json_object(text: str, required_key: str) -> dict | None:
    """从文本中提取包含指定 key 的 JSON 对象。"""
    code_match = re.search(r"```(?:json)?\s*\n(.*?)\n\s*```", text, re.DOTALL)
    if code_match:
        try:
            obj = json.loads(code_match.group(1))
            if isinstance(obj, dict) and required_key in obj:
                return obj
        except json.JSONDecodeError:
            pass
    for i, ch in enumerate(text):
        if ch != '{':
            continue
        ahead = text[i:i+100]
        if required_key not in ahead and '"' not in ahead[:30]:
            continue
        depth = 0
        in_str = False
        escape = False
        for j in range(i, len(text)):
            c = text[j]
            if escape:
                escape = False
                continue
            if c == '\\' and in_str:
                escape = True
                continue
            if c == '"' and not escape:
                in_str = not in_str
                continue
            if in_str:
                continue
            if c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    try:
                        obj = json.loads(text[i:j+1])
                        if isinstance(obj, dict) and required_key in obj:
                            return obj
                    except json.JSONDecodeError:
                        pass
                    break
    return None


def _parse_eval_md(output: str) -> dict:
    """从 Judge markdown 输出中解析评审结果。"""
    score = 0
    passed = False
    feedback = ""

    m = re.search(r'##\s*评分[::=：]\s*(\d+)', output)
    if not m:
        m = re.search(r'##\s*[Ss]core[::=：]\s*(\d+)', output)
    if not m:
        # 匹配 "**评分**: 85" 或 "评分：85" 等变体
        m = re.search(r'\*{0,2}评分\*{0,2}[::=：]\s*(\d+)', output)
    if not m:
        # 匹配 "Score: 85" 无 ## 前缀
        m = re.search(r'[Ss]core[::=：]\s*(\d+)', output)
    if m:
        score = min(int(m.group(1)), 100)

    m = re.search(r'##\s*通过[::=：]\s*(是|否|true|false|yes|no|pass|fail)', output, re.IGNORECASE)
    if not m:
        m = re.search(r'##\s*[Pp]ass[::=：]\s*(是|否|true|false|yes|no)', output, re.IGNORECASE)
    if not m:
        m = re.search(r'\*{0,2}通过\*{0,2}[::=：]\s*(是|否|true|false)', output, re.IGNORECASE)
    if not m:
        m = re.search(r'[Pp]ass[::=：]\s*(是|否|true|false|yes|no)', output, re.IGNORECASE)
    if m:
        passed = m.group(1).lower() in ('是', 'true', 'yes', 'pass')
    elif score >= 70:
        passed = True

    m = re.search(r'##\s*(?:评审意见|评审|反馈|[Ff]eedback)\s*\n(.*?)(?=\n##|$)', output, re.DOTALL)
    if m:
        feedback = m.group(1).strip()

    if score > 0:
        return {"pass": passed, "score": score, "feedback": feedback or output[:500]}

    # 回退 JSON
    obj = _extract_json_object(output, "pass")
    if obj:
        return {"pass": bool(obj.get("pass", False)), "score": int(obj.get("score", 0)),
                "feedback": str(obj.get("feedback", ""))}

    # 回退文本分数
    sm = re.search(r'(\d{1,3})\s*/\s*100|\b(\d{2,3})分', output)
    if sm:
        score = int(sm.group(1) or sm.group(2))
        return {"pass": score >= 70, "score": score, "feedback": output[:500]}

    return {"pass": False, "score": 0, "feedback": output[:500]}


def _parse_summary_md(output: str) -> dict:
    """从 Judge 总结中解析综合评分。"""
    best_worker = ""
    overall_passed = False
    reasoning = ""

    m = re.search(r'##\s*最佳\s*[Ww]orker[::=：]\s*(worker-\d+)', output, re.IGNORECASE)
    if not m:
        m = re.search(r'##\s*[Bb]est\s*[Ww]orker[::=：]\s*(worker-\d+)', output, re.IGNORECASE)
    if m:
        best_worker = m.group(1)

    m = re.search(r'##\s*整体通过[::=：]\s*(是|否|true|false|yes|no)', output, re.IGNORECASE)
    if m:
        overall_passed = m.group(1).lower() in ('是', 'true', 'yes')

    m = re.search(r'##\s*(?:对比理由|理由|[Rr]easoning)\s*\n(.*?)(?=\n##|$)', output, re.DOTALL)
    if m:
        reasoning = m.group(1).strip()

    if best_worker:
        return {"best_worker": best_worker, "reasoning": reasoning or output[:500],
                "overall_passed": overall_passed}

    obj = _extract_json_object(output, "best_worker")
    if obj:
        return {"best_worker": str(obj.get("best_worker", "")),
                "reasoning": str(obj.get("reasoning", "")),
                "overall_passed": bool(obj.get("overall_passed", False))}

    m = re.search(r'(worker-\d+)', output)
    return {"best_worker": m.group(1) if m else "",
            "reasoning": output[:500], "overall_passed": overall_passed}


# ─── 编排器 ───────────────────────────────────────────────────────────────────

class Orchestrator:

    def __init__(self, config: TaskConfig, on_event: Callable[[SwarmEvent], None] | None = None):
        self.cfg = config
        self._on_event = on_event
        self._cancel_event: asyncio.Event | None = None

    def _emit(self, event_type: str, task_id: str, **data):
        if self._on_event:
            try:
                self._on_event(SwarmEvent(type=event_type, task_id=task_id, data=data))
            except Exception:
                pass

    def abort(self):
        if self._cancel_event:
            self._cancel_event.set()

    # ═══════════════════════════════════════════════════════════════════════
    # 主执行
    # ═══════════════════════════════════════════════════════════════════════

    async def execute(self, task_id: str | None = None) -> TaskResult:
        cfg = self.cfg
        task_id = task_id or make_id()
        start = time.time()
        target_dir = os.path.abspath(cfg.target_dir)
        threshold = cfg.pass_threshold or math.ceil(cfg.judge_count / 2)
        self._cancel_event = asyncio.Event()

        out_dir = Path(os.path.abspath(cfg.output_dir)) / task_id
        out_dir.mkdir(parents=True, exist_ok=True)
        sess_dir = out_dir / "sessions"
        sess_dir.mkdir(exist_ok=True)

        worker_dir_prompts = load_system_prompts(cfg.workers.system_prompt_dir, cfg.worker_count)
        judge_dir_prompts = load_system_prompts(cfg.judges.system_prompt_dir, cfg.judge_count)

        # Worker session 文件（Phase A 跨轮保持）
        worker_sessions_a = [str(sess_dir / f"worker-{i}-phaseA.jsonl") for i in range(cfg.worker_count)]

        # Worker 输出目录
        worker_out_dirs = []
        for i in range(cfg.worker_count):
            wdir = out_dir / f"workspace-worker-{i}"
            wdir.mkdir(exist_ok=True)
            worker_out_dirs.append(str(wdir))

        result = TaskResult(task_id=task_id, status=TaskStatus.RUNNING,
                            task=cfg.task, config_snapshot=cfg.model_dump())

        agents_desc = ([f"worker-{i}={a.model}" for i, a in enumerate(cfg.workers.agents)]
                       + [f"judge-{i}={a.model}" for i, a in enumerate(cfg.judges.agents)])
        self._emit("task_start", task_id, task=cfg.task, agents=agents_desc)

        try:
            feedback_for_workers = ""

            for rnd_num in range(1, cfg.max_rounds + 1):
                if self._cancel_event.is_set():
                    break

                self._emit("round_start", task_id, round=rnd_num)
                rnd_dir = out_dir / f"round-{rnd_num}"
                rnd_workers_dir = rnd_dir / "workers"
                rnd_judges_dir = rnd_dir / "judges"
                rnd_workers_dir.mkdir(parents=True, exist_ok=True)
                rnd_judges_dir.mkdir(parents=True, exist_ok=True)

                # ═══════════════════════════════════════════════════════
                # 1. Workers 并行（两阶段）
                # ═══════════════════════════════════════════════════════

                # 准备跨 Worker 分类参考（上一轮的结果）
                prev_classifications = ""
                prev_judges = None
                if rnd_num > 1 and result.rounds:
                    prev_rnd = result.rounds[-1]
                    cls_parts = []
                    for pw in prev_rnd.worker_results:
                        cls_parts.append(
                            f"**{pw.worker_id}** ({len(pw.modules)} 模块): "
                            f"{', '.join(pw.modules)}")
                    prev_classifications = "\n".join(cls_parts)
                    prev_judges = prev_rnd.judge_results

                w_tasks = []
                for i, acfg in enumerate(cfg.workers.agents):
                    wid = f"worker-{i}"
                    self._emit("worker_start", task_id, worker_id=wid,
                               model=acfg.model, round=rnd_num)
                    w_tasks.append(self._run_worker_phases(
                        worker_idx=i,
                        worker_cfg=acfg,
                        worker_sys_prompt=resolve_system_prompt(i, acfg, worker_dir_prompts),
                        task_id=task_id,
                        rnd_num=rnd_num,
                        target_dir=target_dir,
                        worker_out_dir=worker_out_dirs[i],
                        session_a_file=worker_sessions_a[i],
                        sess_dir=sess_dir,
                        feedback=feedback_for_workers,
                        other_classifications=prev_classifications,
                        prev_round_judges=prev_judges,
                    ))

                worker_results_raw = await asyncio.gather(*w_tasks)
                round_workers: list[WorkerResult] = []

                for i, wr in enumerate(worker_results_raw):
                    result.total_tokens += wr.token_usage
                    self._emit("worker_done", task_id, worker_id=wr.worker_id,
                               modules=wr.modules, module_count=len(wr.modules))
                    round_workers.append(wr)
                    # 归档
                    (rnd_workers_dir / f"{wr.worker_id}-summary.md").write_text(
                        f"# {wr.worker_id} Round {rnd_num}\n\n"
                        f"Modules: {', '.join(wr.modules)}\n\n{wr.output}",
                        encoding="utf-8")

                # ═══════════════════════════════════════════════════════
                # 2. Judges 并行（三步评审）
                # ═══════════════════════════════════════════════════════

                for j_idx, j_acfg in enumerate(cfg.judges.agents):
                    self._emit("judge_start", task_id, judge_id=f"judge-{j_idx}",
                               model=j_acfg.model, round=rnd_num)

                async def _run_one_judge(j_idx: int, j_acfg: AgentInstanceConfig):
                    return await self._run_judge_three_steps(
                        judge_idx=j_idx,
                        judge_cfg=j_acfg,
                        judge_sys_prompt=resolve_system_prompt(j_idx, j_acfg, judge_dir_prompts),
                        round_workers=round_workers,
                        task_id=task_id,
                        rnd_num=rnd_num,
                        target_dir=target_dir,
                        rnd_judges_dir=rnd_judges_dir,
                    )

                judge_tasks = [_run_one_judge(j, a) for j, a in enumerate(cfg.judges.agents)]
                round_judges = await asyncio.gather(*judge_tasks)

                # ═══════════════════════════════════════════════════════
                # 3. 投票——基于实际 eval 结果，不依赖 summary 声明
                # ═══════════════════════════════════════════════════════

                # 每个 Judge 对每个 Worker 的实际结果：找到每个 Judge 认为通过的 best worker
                pass_count = 0
                best_votes: Counter[str] = Counter()
                best_scores: Counter[str] = Counter()

                for j in round_judges:
                    # 找该 Judge 下通过的 worker 中得分最高的
                    passed_evals = [e for e in j.evaluations if e.overall_passed]
                    if passed_evals:
                        pass_count += 1
                        best_ev = max(passed_evals, key=lambda e: e.overall_score)
                        best_votes[best_ev.worker_id] += 1
                        best_scores[best_ev.worker_id] += best_ev.overall_score

                is_passed = pass_count >= threshold
                # best worker = 最多票，同票取总分高
                if best_votes:
                    max_vote = best_votes.most_common(1)[0][1]
                    top = [w for w, v in best_votes.items() if v == max_vote]
                    best_wid = max(top, key=lambda w: best_scores[w])
                else:
                    # 无人通过，按总分选最佳
                    all_scores: Counter[str] = Counter()
                    for j in round_judges:
                        for e in j.evaluations:
                            all_scores[e.worker_id] += e.overall_score
                    best_wid = all_scores.most_common(1)[0][0] if all_scores else "worker-0"

                feedback_md = self._build_feedback_md(round_workers, round_judges, best_wid, rnd_num)
                (rnd_dir / "feedback.md").write_text(feedback_md, encoding="utf-8")

                rnd = RoundResult(
                    round=rnd_num,
                    worker_results=round_workers,
                    judge_results=round_judges,
                    pass_count=pass_count,
                    total_judges=cfg.judge_count,
                    passed=is_passed,
                    best_worker_id=best_wid,
                    feedback_to_workers=feedback_md,
                )
                result.rounds.append(rnd)

                self._emit("round_end", task_id, round=rnd_num,
                           passed=is_passed, pass_count=pass_count,
                           total_judges=cfg.judge_count, best_worker=best_wid)

                if is_passed and rnd_num >= cfg.min_rounds:
                    result.status = TaskStatus.PASSED
                    best_w = next((w for w in round_workers if w.worker_id == best_wid), round_workers[0])
                    result.final_output = best_w.output
                    break

                if is_passed and rnd_num < cfg.min_rounds:
                    self._emit("round_reflection", task_id, round=rnd_num,
                               message=f"Round {rnd_num} passed but min_rounds={cfg.min_rounds}, forcing reflection")

                feedback_for_workers = feedback_md
                if rnd_num == cfg.max_rounds:
                    result.status = TaskStatus.FAILED
                    best_w = next((w for w in round_workers if w.worker_id == best_wid), round_workers[0])
                    result.final_output = best_w.output

        except Exception as e:
            result.status = TaskStatus.ERROR
            result.error = str(e)
            self._emit("error", task_id, error=str(e))

        result.total_duration_ms = (time.time() - start) * 1000

        # 归档
        (out_dir / "report.md").write_text(self._report(result), encoding="utf-8")
        (out_dir / "result.json").write_text(result.model_dump_json(indent=2), encoding="utf-8")

        result_dir = Path(os.path.abspath(cfg.result_dir))
        result_dir.mkdir(parents=True, exist_ok=True)
        fname = f"{cfg.source_file}_{cfg.function_name}".replace("/", "_").replace(" ", "_")
        if not fname or fname == "_":
            fname = task_id

        # 最终输出 = 最佳 worker 的模块文件夹
        best_w = None
        for rnd in reversed(result.rounds):
            for w in rnd.worker_results:
                if w.worker_id == (rnd.best_worker_id or "worker-0"):
                    best_w = w
                    break
            if best_w:
                break

        result_md = result_dir / f"{fname}.md"
        result_md.write_text(
            f"---\ntask_id: {task_id}\nstatus: {result.status.value}\n"
            f"best_worker: {best_w.worker_id if best_w else ''}\n"
            f"modules: {best_w.modules if best_w else []}\n"
            f"rounds: {len(result.rounds)}\n"
            f"duration: {result.total_duration_ms / 1000:.1f}s\n---\n\n"
            f"{result.final_output}",
            encoding="utf-8",
        )

        archive_dir = Path(os.path.abspath(cfg.archive_dir))
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_path = str(archive_dir / f"{fname}_log")
        shutil.make_archive(archive_path, "zip", str(out_dir.parent), out_dir.name)

        self._emit("task_end", task_id, status=result.status.value,
                    archive=f"{archive_path}.zip",
                    result_file=str(result_md))

        # 清理工作目录（保留结果和归档）
        try:
            shutil.rmtree(str(out_dir))
        except OSError:
            pass

        return result

    # ═══════════════════════════════════════════════════════════════════════
    # Worker 两阶段执行
    # ═══════════════════════════════════════════════════════════════════════

    async def _run_worker_phases(
        self,
        worker_idx: int,
        worker_cfg: AgentInstanceConfig,
        worker_sys_prompt: str,
        task_id: str,
        rnd_num: int,
        target_dir: str,
        worker_out_dir: str,
        session_a_file: str,
        sess_dir: Path,
        feedback: str,
        other_classifications: str = "",
        prev_round_judges: list | None = None,
    ) -> WorkerResult:
        cfg = self.cfg
        wid = f"worker-{worker_idx}"
        wr = WorkerResult(worker_id=wid, model=worker_cfg.model, output_dir=worker_out_dir)

        base_kwargs = {
            "model": worker_cfg.model,
            "tools": worker_cfg.tools or cfg.workers.default_tools,
            "system_prompt": worker_sys_prompt,
            "cwd": worker_out_dir,
            "thinking_level": worker_cfg.thinking_level or cfg.workers.default_thinking_level,
            "cancel_event": self._cancel_event,
            "max_retries": cfg.agent_max_retries,
            "retry_delay": cfg.agent_retry_delay,
        }

        # ── Phase A: 文件分类 ─────────────────────────────────
        phase_a_prompt = self._build_phase_a_prompt(
            cfg.task, target_dir, rnd_num, feedback,
            worker_id=wid, other_classifications=other_classifications)
        ar = await run_agent(
            prompt=phase_a_prompt, **base_kwargs,
            session_file=session_a_file,
        )
        wr.token_usage += ar.token_usage
        wr.output = _extract_result(ar.output)

        if ar.error:
            wr.error = ar.error
            return wr

        # 发现分类后的模块
        modules = _discover_modules(worker_out_dir)
        wr.modules = modules

        self._emit("worker_phase", task_id, worker_id=wid,
                    phase="A", modules=modules, module_count=len(modules))

        # ── Phase B: 逐模块分析（每次 fork Phase A 上下文）──────
        for mod_name in modules:
            # 复制 Phase A session 作为本模块的起点
            mod_session = str(sess_dir / f"{wid}-phaseB-{mod_name}-r{rnd_num}.jsonl")
            try:
                shutil.copy2(session_a_file, mod_session)
            except OSError:
                pass

            # 获取上轮该模块的报告和 Judge 反馈
            prev_report = ""
            judge_feedback_for_mod = ""
            if rnd_num > 1:
                report_path = Path(worker_out_dir) / mod_name / "module_report.md"
                if report_path.exists():
                    try:
                        prev_report = report_path.read_text(encoding="utf-8")[:3000]
                    except OSError:
                        pass
                # 从上轮 Judge 结果中提取该模块的反馈
                if prev_round_judges:
                    fb_parts = []
                    for j in prev_round_judges:
                        for ev in j.evaluations:
                            if ev.worker_id == wid:
                                for me in ev.module_evals:
                                    if me.module_name == mod_name:
                                        fb_parts.append(
                                            f"{j.judge_id}: {'PASS' if me.passed else 'FAIL'} "
                                            f"({me.score}/100) {me.feedback[:500]}")
                    judge_feedback_for_mod = "\n\n".join(fb_parts)

            mod_prompt = self._build_phase_b_prompt(
                mod_name, worker_out_dir,
                worker_id=wid,
                prev_report=prev_report,
                judge_feedback=judge_feedback_for_mod,
            )
            ar = await run_agent(
                prompt=mod_prompt, **base_kwargs,
                session_file=mod_session,
            )
            wr.token_usage += ar.token_usage

            self._emit("worker_phase", task_id, worker_id=wid,
                        phase="B", module=mod_name)

        return wr

    # ═══════════════════════════════════════════════════════════════════════
    # Judge 三步评审
    # ═══════════════════════════════════════════════════════════════════════

    async def _run_judge_three_steps(
        self,
        judge_idx: int,
        judge_cfg: AgentInstanceConfig,
        judge_sys_prompt: str,
        round_workers: list[WorkerResult],
        task_id: str,
        rnd_num: int,
        target_dir: str,
        rnd_judges_dir: Path,
    ) -> JudgeRoundResult:
        cfg = self.cfg
        jid = f"judge-{judge_idx}"

        j_dir = rnd_judges_dir / jid
        j_dir.mkdir(parents=True, exist_ok=True)

        j_result = JudgeRoundResult(judge_id=jid, model=judge_cfg.model)

        base_kwargs = {
            "model": judge_cfg.model,
            "tools": judge_cfg.tools or cfg.judges.default_tools,
            "system_prompt": judge_sys_prompt,
            "thinking_level": judge_cfg.thinking_level or cfg.judges.default_thinking_level,
            "cancel_event": self._cancel_event,
            "max_retries": cfg.agent_max_retries,
            "retry_delay": cfg.agent_retry_delay,
            "session_file": None,  # 每步独立上下文
        }

        for w in round_workers:
            weval = WorkerEvaluation(worker_id=w.worker_id)
            w_j_dir = j_dir / w.worker_id
            w_j_dir.mkdir(exist_ok=True)

            # ── Step 1: 文件分类完整性 ────────────────────────
            step1_prompt = self._build_judge_step1_prompt(
                target_dir, w.output_dir, w.modules)

            ar = await run_agent(prompt=step1_prompt, cwd=w.output_dir, **base_kwargs)
            j_result.token_usage += ar.token_usage

            parsed = _parse_eval_md(ar.output)
            weval.classification_ok = parsed["pass"]
            weval.classification_feedback = parsed["feedback"]
            (w_j_dir / "step1-classification.md").write_text(
                f"# {jid} → {w.worker_id} Step 1: Classification\n\n"
                f"Pass: {parsed['pass']}\nScore: {parsed['score']}\n\n{parsed['feedback']}\n\n"
                f"---\n## Raw Output\n\n{ar.output[:2000]}",
                encoding="utf-8")

            self._emit("judge_step", task_id, judge_id=jid,
                        worker_id=w.worker_id, step=1,
                        passed=parsed["pass"], score=parsed["score"])

            # ── Step 2: 逐模块评审 ────────────────────────────
            all_modules_pass = True
            for mod_name in w.modules:
                mod_dir = os.path.join(w.output_dir, mod_name)
                step2_prompt = self._build_judge_step2_prompt(mod_name, mod_dir)

                ar = await run_agent(prompt=step2_prompt, cwd=mod_dir, **base_kwargs)
                j_result.token_usage += ar.token_usage

                parsed = _parse_eval_md(ar.output)
                mod_eval = ModuleEvaluation(
                    module_name=mod_name,
                    passed=parsed["pass"],
                    score=parsed["score"],
                    feedback=parsed["feedback"],
                )
                weval.module_evals.append(mod_eval)
                if not parsed["pass"]:
                    all_modules_pass = False

                (w_j_dir / f"step2-module-{mod_name}.md").write_text(
                    f"# {jid} → {w.worker_id} → {mod_name}\n\n"
                    f"Pass: {parsed['pass']}\nScore: {parsed['score']}\n\n{parsed['feedback']}\n\n"
                    f"---\n## Raw Output\n\n{ar.output[:3000]}",
                    encoding="utf-8")

                self._emit("judge_step", task_id, judge_id=jid,
                            worker_id=w.worker_id, step=2,
                            module=mod_name, passed=parsed["pass"],
                            score=parsed["score"])

            # ── Step 3: 综合评分 ──────────────────────────────
            eval_files = [f.name for f in w_j_dir.glob("step2-module-*.md")]
            step3_prompt = self._build_judge_step3_prompt(
                w.worker_id, weval.classification_ok, weval.module_evals, eval_files)

            ar = await run_agent(prompt=step3_prompt, cwd=str(w_j_dir), **base_kwargs)
            j_result.token_usage += ar.token_usage

            parsed = _parse_eval_md(ar.output)
            # overall_passed 必须同时满足：分类完整 + 所有模块通过 + Step3 综合评分通过
            # 如果 Step3 解析失败(score=0)，用实际模块评分均值作为兑底
            step3_score = parsed["score"]
            if step3_score == 0 and weval.module_evals:
                step3_score = sum(m.score for m in weval.module_evals) // len(weval.module_evals)
            step3_passed = step3_score >= 70 if parsed["score"] == 0 else parsed["pass"]

            weval.overall_passed = step3_passed and weval.classification_ok and all_modules_pass
            weval.overall_score = step3_score
            weval.overall_feedback = parsed["feedback"]

            (w_j_dir / "step3-overall.md").write_text(
                f"# {jid} → {w.worker_id} Overall\n\n"
                f"Classification OK: {weval.classification_ok}\n"
                f"All Modules Pass: {all_modules_pass}\n"
                f"Overall Pass: {weval.overall_passed}\n"
                f"Score: {weval.overall_score}\n\n{weval.overall_feedback}",
                encoding="utf-8")

            self._emit("judge_eval", task_id, judge_id=jid,
                        worker_id=w.worker_id,
                        passed=weval.overall_passed,
                        score=weval.overall_score)

            j_result.evaluations.append(weval)

        # Summary (多 worker 时对比)
        if len(round_workers) >= 2:
            summary_prompt = self._build_judge_summary_prompt(j_result.evaluations)
            ar = await run_agent(prompt=summary_prompt, cwd=str(j_dir), **base_kwargs)
            j_result.token_usage += ar.token_usage
            parsed = _parse_summary_md(ar.output)
            j_result.summary = JudgeSummary(**parsed)
        else:
            ev = j_result.evaluations[0]
            j_result.summary = JudgeSummary(
                best_worker_id=ev.worker_id,
                reasoning=ev.overall_feedback,
                overall_passed=ev.overall_passed,
            )

        self._emit("judge_summary", task_id, judge_id=jid,
                    best=j_result.summary.best_worker_id,
                    overall_passed=j_result.summary.overall_passed)

        return j_result

    # ═══════════════════════════════════════════════════════════════════════
    # Prompt 构建
    # ═══════════════════════════════════════════════════════════════════════

    def _build_phase_a_prompt(self, task, target_dir, rnd, feedback,
                              worker_id: str = "", other_classifications: str = ""):
        parts = [
            f"# Phase A: 文件分析与模块分类\n\n你是 **{worker_id}**。\n\n{task}",
            "解包文件位于固定路径: `/data/target`",
            "请完成以下工作：\n"
            "1. 使用 `bash` 和 `read` 工具遍历 `/data/target` 下所有文件\n"
            "2. 分析每个文件的功能、类型（配置/二进制/脚本/库/服务等）\n"
            "3. **细粒度分类**：按具体协议/服务/功能划分模块，而非笼统分类。例如：\n"
            "   - ✗ 错误: `network/` (太笼统)\n"
            "   - ✓ 正确: `bgp/`, `ospf/`, `ike/`, `ipsec/`, `ssh/`, `snmp/`, `http_server/` 等\n"
            "   - ✗ 错误: `monitor/` (太笼统)\n"
            "   - ✓ 正确: `bgp_monitor/`, `interface_monitor/`, `health_check/` 等\n"
            "   - ✗ 错误: `crypto/` (太笼统)\n"
            "   - ✓ 正确: `tls/`, `ike/`, `pki/` 等\n"
            "   - 共享库/通用工具可归入 `common/` 或 `platform/`\n"
            "   - 无法确定功能的文件归入 `unknown/`\n"
            "4. 为每个模块创建子目录，并在其中创建 `files.list` 文件，\n"
            "   每行写入一个属于该模块的文件的绝对路径：\n"
            "   ```bash\n"
            "   mkdir -p <模块名>\n"
            "   echo '/data/target/path/to/file1' >> <模块名>/files.list\n"
            "   ```\n"
            "   **注意：不要拷贝文件，只记录绝对路径到 files.list 中。**\n"
            "5. 一个文件只能属于一个模块，不要遗漏任何文件\n"
            "6. 模块命名使用小写英文+下划线，如 `bgp`, `ike_vpn`, `web_server`\n\n"
            "完成后用 `<result>...</result>` 包裹你的分类摘要。",
        ]
        if rnd > 1 and feedback:
            parts.insert(1,
                f"# 上一轮反馈（请注意你是 {worker_id}）\n\n{feedback}\n\n"
                "请根据反馈改进你的分类和分析。")
        if rnd > 1 and other_classifications:
            parts.insert(2 if rnd > 1 and feedback else 1,
                f"# 其他 Worker 的分类结果（供参考）\n\n{other_classifications}")
        return "\n\n".join(parts)

    def _build_phase_b_prompt(self, module_name, worker_out_dir,
                              worker_id: str = "",
                              prev_report: str = "", judge_feedback: str = ""):
        parts = [
            f"# Phase B: 模块分析 — {module_name}\n\n"
            f"你是 **{worker_id}**。\n\n"
            f"模块目录: `{module_name}/`\n"
            f"文件列表: `{module_name}/files.list`（内含绝对路径，可直接用 read 读取）\n\n"
            "请完成以下工作：\n\n"
            f"1. 使用 `read` 读取 `{module_name}/files.list` 获取该模块的文件列表\n"
            "2. 使用 `read` 工具按 files.list 中的绝对路径逐个读取每个文件\n"
            f"3. 将分析结果写入 `{module_name}/module_report.md`"
        ]

        if prev_report:
            parts.append(
                f"\n## 上一轮该模块的分析报告\n\n"
                f"<details>\n<summary>展开上轮报告</summary>\n\n{prev_report}\n\n</details>")

        if judge_feedback:
            parts.append(
                f"\n## Judge 对该模块的评审反馈\n\n{judge_feedback}\n\n"
                f"请重点关注 Judge 指出的问题并改进。")

        parts.append(
            "module_report.md 必须包含：\n\n"
            "## 1. 模块功能分析\n"
            "- 该模块包含哪些文件，每个文件的作用\n"
            "- 模块的整体功能和职责\n"
            "- 模块对外提供的接口/服务\n\n"
            "## 2. 威胁分析 (STRIDE)\n"
            "- 识别所有攻击面（外部输入、网络接口、文件操作等）\n"
            "- 按 STRIDE 分类每个威胁\n"
            "- 每个威胁标注: 位置、触发条件、影响、风险等级(🔴高/🟡中/🟢低)\n\n"
            "## 3. 对外暴露面评估\n"
            "- 该模块暴露了哪些接口给外部\n"
            "- 网络端口、文件路径、IPC 通道等\n"
            "- 综合风险评分 (0-100)"
        )
        return "\n\n".join(parts)

    def _build_judge_step1_prompt(self, target_dir, worker_out_dir, modules):
        return (
            f"# Step 1: 文件分类完整性检查\n\n"
            f"原始解包目录: `/data/target`\n"
            f"Worker 创建的模块目录: {', '.join(modules)}\n\n"
            "请完成以下检查：\n"
            "1. 使用 `bash` 列出 `/data/target` 下所有文件\n"
            "2. 读取每个模块的 `files.list`，汇总所有已分类文件\n"
            "3. 检查是否有遗漏的文件（在 /data/target 中但不在任何 files.list 中）\n"
            "4. 检查是否有文件被重复分类\n\n"
            "⚠️ **你必须在回复末尾输出以下格式，不可省略：**\n\n"
            "## 评分: <0-100>\n"
            "## 通过: <是/否>\n"
            "## 评审意见\n"
            "<遗漏的文件列表，或确认全部覆盖>"
        )

    def _build_judge_step2_prompt(self, module_name, mod_dir):
        return (
            f"# Step 2: 模块评审 — {module_name}\n\n"
            f"模块目录: 当前工作目录\n\n"
            "请检查：\n"
            "1. 使用 `read` 读取 `files.list` 获取文件列表（内含绝对路径）\n"
            "2. 使用 `read` 按绝对路径逐个读取源文件（二进制文件无法读取时跳过，根据文件名推断即可）\n"
            "3. 使用 `read` 读取 `module_report.md`（如存在）\n"
            "4. 验证：\n"
            "   a. 文件划分是否合理（files.list 中的文件确实属于同一模块吗）\n"
            "   b. module_report.md 的功能描述是否准确\n"
            "   c. 威胁分析是否正确（威胁是否真实、有无遗漏关键威胁）\n"
            "   d. 风险评分是否合理\n\n"
            "如果 module_report.md 不存在或为空，直接给 30 分并判定不通过。\n\n"
            "⚠️ **你必须在回复末尾输出以下格式，不可省略：**\n\n"
            "## 评分: <0-100>\n"
            "## 通过: <是/否>\n"
            "## 评审意见\n"
            "<文件划分评价 + 报告质量评价 + 威胁分析评价>"
        )

    def _build_judge_step3_prompt(self, worker_id, classification_ok, module_evals, eval_files):
        parts = [
            f"# Step 3: 综合评分 — {worker_id}\n",
            f"文件分类完整性: {'✅ 通过' if classification_ok else '❌ 未通过'}\n",
            "各模块评审结果：\n",
        ]
        for me in module_evals:
            parts.append(
                f"- **{me.module_name}**: {'✅' if me.passed else '❌'} "
                f"Score {me.score}")
        parts.append(
            f"\n详细评审文件: {', '.join(eval_files)}\n"
            "请使用 `read` 工具读取上述评审文件，然后给出综合评分。\n\n"
            "**判定规则：所有模块必须通过且文件分类完整，才能投通过票。**\n\n"
            "⚠️ **你必须在回复末尾输出以下格式，不可省略：**\n\n"
            "## 评分: <0-100>\n"
            "## 通过: <是/否>\n"
            "## 评审意见\n"
            "<综合评价>"
        )
        return "\n".join(parts)

    def _build_judge_summary_prompt(self, evals):
        parts = ["# 对比所有 Workers\n"]
        for ev in evals:
            parts.append(
                f"- **{ev.worker_id}**: Overall {'PASS' if ev.overall_passed else 'FAIL'} "
                f"(Score {ev.overall_score}, "
                f"Modules: {sum(1 for m in ev.module_evals if m.passed)}/{len(ev.module_evals)})")
        parts.append(
            "\n请对比后按以下格式输出：\n\n"
            "## 最佳Worker: <worker-X>\n"
            "## 整体通过: <是/否>\n"
            "## 对比理由\n"
            "<为什么这个 worker 最好>")
        return "\n".join(parts)

    # ═══════════════════════════════════════════════════════════════════════
    # Feedback
    # ═══════════════════════════════════════════════════════════════════════

    def _build_feedback_md(self, workers, judges, best_wid, rnd):
        lines = [f"# Round {rnd} Feedback\n", f"**Best Worker**: {best_wid}\n"]
        for j in judges:
            for ev in j.evaluations:
                lines.append(f"## {j.judge_id} → {ev.worker_id}")
                lines.append(f"Classification: {'✅' if ev.classification_ok else '❌'}")
                for me in ev.module_evals:
                    lines.append(f"- {me.module_name}: {'✅' if me.passed else '❌'} "
                                 f"({me.score}) {me.feedback[:200]}")
                lines.append(f"Overall: {'PASS' if ev.overall_passed else 'FAIL'} "
                             f"({ev.overall_score})")
                lines.append(f"{ev.overall_feedback[:500]}\n")
        return "\n".join(lines)

    def _report(self, result: TaskResult) -> str:
        lines = [
            f"# Task Report: {result.task_id}\n",
            f"Status: {result.status.value}",
            f"Rounds: {len(result.rounds)}",
            f"Duration: {result.total_duration_ms / 1000:.1f}s\n",
        ]
        for rnd in result.rounds:
            lines.append(f"## Round {rnd.round}")
            for w in rnd.worker_results:
                lines.append(f"- {w.worker_id}: {len(w.modules)} modules ({', '.join(w.modules[:5])})")
            for j in rnd.judge_results:
                for ev in j.evaluations:
                    lines.append(f"- {j.judge_id}→{ev.worker_id}: "
                                 f"{'PASS' if ev.overall_passed else 'FAIL'} ({ev.overall_score})")
        return "\n".join(lines)
