"""
system_analyse — 五阶段流水线编排器

Stage 1: Worker 全局分类 (session 累积)
Stage 2: Judge 脚本检查分类完整性 (✗→回Stage1)
Stage 3: 遍历子文件夹 — Worker细分 + Judge评审 (✗→重做该模块)
Stage 4: 遍历子文件夹 — Worker分析 + Judge评审 (分类问题→回Stage3, 其他→重做)
Stage 5: Judge 脚本最终检查 (✗→回Stage3处理缺失模块)
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import re
import shutil
import time
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


# ─── 工具函数 ─────────────────────────────────────────────────────────────────

def _extract_result(output: str) -> str:
    m = re.search(r"<result>(.*?)</result>", output, re.DOTALL)
    return m.group(1).strip() if m else output


def _discover_modules(workspace: str) -> list[str]:
    modules = []
    ws = Path(workspace)
    if ws.is_dir():
        for d in sorted(ws.iterdir()):
            if d.is_dir() and not d.name.startswith(".") and (d / "files.list").exists():
                modules.append(d.name)
    return modules


def _parse_eval_md(output: str) -> dict:
    """从 Judge markdown 输出解析评审结果。多层 fallback。"""
    score = 0
    passed = False
    feedback = ""

    for pat in [
        r'##\s*评分[::=：]\s*(\d+)',
        r'##\s*[Ss]core[::=：]\s*(\d+)',
        r'\*{0,2}评分\*{0,2}[::=：]\s*(\d+)',
        r'[Ss]core[::=：]\s*(\d+)',
    ]:
        m = re.search(pat, output)
        if m:
            score = min(int(m.group(1)), 100)
            break

    for pat in [
        r'##\s*通过[::=：]\s*(是|否|true|false|yes|no|pass|fail)',
        r'##\s*[Pp]ass[::=：]\s*(是|否|true|false|yes|no)',
        r'\*{0,2}通过\*{0,2}[::=：]\s*(是|否|true|false)',
        r'[Pp]ass[::=：]\s*(是|否|true|false|yes|no)',
    ]:
        m = re.search(pat, output, re.IGNORECASE)
        if m:
            passed = m.group(1).lower() in ('是', 'true', 'yes', 'pass')
            break
    else:
        if score >= 70:
            passed = True

    m = re.search(r'##\s*(?:评审意见|评审|反馈|[Ff]eedback)\s*\n(.*?)(?=\n##|$)', output, re.DOTALL)
    if m:
        feedback = m.group(1).strip()

    if score > 0:
        return {"pass": passed, "score": score, "feedback": feedback or output[:500]}

    # JSON fallback
    for i, ch in enumerate(output):
        if ch != '{':
            continue
        if '"pass"' not in output[i:i+100]:
            continue
        depth = 0
        in_str = False
        esc = False
        for j in range(i, len(output)):
            c = output[j]
            if esc:
                esc = False
                continue
            if c == '\\' and in_str:
                esc = True
                continue
            if c == '"':
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
                        obj = json.loads(output[i:j+1])
                        if "pass" in obj:
                            return {"pass": bool(obj["pass"]), "score": int(obj.get("score", 0)),
                                    "feedback": str(obj.get("feedback", ""))}
                    except (json.JSONDecodeError, ValueError):
                        pass
                    break

    # 文本分数 fallback
    sm = re.search(r'(\d{1,3})\s*/\s*100|\b(\d{2,3})分', output)
    if sm:
        score = int(sm.group(1) or sm.group(2))
        return {"pass": score >= 70, "score": score, "feedback": output[:500]}

    return {"pass": False, "score": 0, "feedback": output[:500]}


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

    async def execute(self, task_id: str | None = None) -> TaskResult:
        cfg = self.cfg
        task_id = task_id or make_id()
        start = time.time()
        self._cancel_event = asyncio.Event()

        out_dir = Path(os.path.abspath(cfg.output_dir)) / task_id
        out_dir.mkdir(parents=True, exist_ok=True)
        sess_dir = out_dir / "sessions"
        sess_dir.mkdir(exist_ok=True)
        workspace = out_dir / "workspace"
        workspace.mkdir(exist_ok=True)

        result = TaskResult(task_id=task_id, status=TaskStatus.RUNNING,
                            task=cfg.task, config_snapshot=cfg.model_dump())

        self._emit("task_start", task_id, task=cfg.task)

        # Worker/Judge 配置
        w_cfg = cfg.workers.agents[0] if cfg.workers.agents else AgentInstanceConfig(model="")
        w_prompt_dir = cfg.workers.system_prompt_dir
        j_cfgs = cfg.judges.agents

        w_base = {
            "model": w_cfg.model,
            "tools": w_cfg.tools or cfg.workers.default_tools,
            "cwd": str(workspace),
            "thinking_level": w_cfg.thinking_level or cfg.workers.default_thinking_level,
            "cancel_event": self._cancel_event,
            "max_retries": cfg.agent_max_retries,
            "retry_delay": cfg.agent_retry_delay,
        }

        j_base = {
            "tools": cfg.judges.default_tools,
            "cwd": str(workspace),
            "thinking_level": cfg.judges.default_thinking_level or "off",
            "cancel_event": self._cancel_event,
            "max_retries": cfg.agent_max_retries,
            "retry_delay": cfg.agent_retry_delay,
            "session_file": None,
        }

        tokens = TokenUsage()
        MAX_RETRIES = cfg.max_rounds

        try:
            # ═══════════════════════════════════════════════════
            # Stage 1+2: 全局分类 + 完整性检查
            # ═══════════════════════════════════════════════════
            classify_session = str(sess_dir / "classify.jsonl")
            classify_prompt_text = self._load_prompt(w_prompt_dir, "step1_classify")
            judge_classify_prompt = self._load_prompt(cfg.judges.system_prompt_dir, "step2_check_classify")

            reflect_classify_text = self._load_prompt(w_prompt_dir, "reflect_classify")

            for attempt in range(MAX_RETRIES):
                self._emit("stage", task_id, stage=1, attempt=attempt + 1)

                # Stage 1: Worker 分类
                feedback_part = ""
                if attempt > 0:
                    feedback_part = f"\n\n# 上一次的评审意见\n\n{stage2_feedback}\n\n请根据评审意见修正分类。"

                ar = await run_agent(
                    prompt=f"{cfg.task}{feedback_part}",
                    system_prompt=classify_prompt_text,
                    session_file=classify_session,
                    **w_base,
                )
                tokens += ar.token_usage
                result.final_output = _extract_result(ar.output)

                modules = _discover_modules(str(workspace))
                self._emit("stage_result", task_id, stage=1,
                           modules=modules, module_count=len(modules))

                # Stage 2: Judge 检查
                self._emit("stage", task_id, stage=2, attempt=attempt + 1)

                stage2_passed = False
                stage2_feedback = ""

                for j_idx, j_cfg in enumerate(j_cfgs):
                    ar = await run_agent(
                        prompt="请运行检查脚本验证分类完整性。",
                        model=j_cfg.model,
                        system_prompt=judge_classify_prompt,
                        **j_base,
                    )
                    tokens += ar.token_usage
                    parsed = _parse_eval_md(ar.output)

                    self._emit("judge_eval", task_id, stage=2,
                               judge_id=f"judge-{j_idx}",
                               passed=parsed["pass"], score=parsed["score"])

                    if parsed["pass"]:
                        stage2_passed = True
                    else:
                        stage2_feedback += f"\njudge-{j_idx}: {parsed['feedback'][:500]}"

                    # 归档
                    (out_dir / f"stage2-attempt{attempt+1}-judge{j_idx}.md").write_text(
                        f"Score: {parsed['score']}\nPass: {parsed['pass']}\n\n"
                        f"{parsed['feedback']}\n\n---\nRaw: {ar.output[:2000]}",
                        encoding="utf-8")

                if stage2_passed:
                    # 强制反思：通过后 Worker 自查确认
                    self._emit("reflect", task_id, stage=1)
                    ar = await run_agent(
                        prompt=reflect_classify_text,
                        system_prompt=classify_prompt_text,
                        session_file=classify_session,
                        **w_base,
                    )
                    tokens += ar.token_usage
                    break
            else:
                self._emit("stage_fail", task_id, stage=2,
                           message="分类完整性检查未通过，已达最大重试次数")

            # ═══════════════════════════════════════════════════
            # Stage 3: 子文件夹细分
            # ═══════════════════════════════════════════════════
            refine_prompt_text = self._load_prompt(w_prompt_dir, "step3_refine")
            judge_refine_prompt = self._load_prompt(cfg.judges.system_prompt_dir, "step3_check_refine")

            modules_to_refine = list(_discover_modules(str(workspace)))
            refined_modules: set[str] = set()

            while modules_to_refine:
                mod_name = modules_to_refine.pop(0)
                if mod_name in refined_modules:
                    continue
                mod_dir = workspace / mod_name
                if not (mod_dir / "files.list").exists():
                    continue

                reflect_refine_text = self._load_prompt(w_prompt_dir, "reflect_refine")

                for attempt in range(MAX_RETRIES):
                    self._emit("stage", task_id, stage=3,
                               module=mod_name, attempt=attempt + 1)

                    # 3.1 Worker 细分
                    refine_session = str(sess_dir / f"refine-{mod_name}.jsonl")
                    feedback_part = ""
                    if attempt > 0:
                        feedback_part = f"\n\n# 评审意见\n\n{s3_feedback}\n\n请根据意见修正。"

                    ar = await run_agent(
                        prompt=f"检查模块 `{mod_name}` 是否需要细分。{feedback_part}",
                        system_prompt=refine_prompt_text,
                        session_file=refine_session,
                        **w_base,
                    )
                    tokens += ar.token_usage

                    # 检查是否产生了新模块
                    new_modules = _discover_modules(str(workspace))
                    new_ones = [m for m in new_modules if m not in refined_modules and m != mod_name]

                    self._emit("stage_result", task_id, stage=3,
                               module=mod_name,
                               split=bool(new_ones and mod_name not in new_modules),
                               new_modules=new_ones)

                    # 3.2 Judge 评审
                    s3_passed = False
                    s3_feedback = ""
                    eval_cwd = str(mod_dir) if mod_dir.exists() else str(workspace)

                    for j_idx, j_cfg in enumerate(j_cfgs):
                        ar = await run_agent(
                            prompt=f"评审 Worker 对模块 `{mod_name}` 的细分判断。",
                            model=j_cfg.model,
                            system_prompt=judge_refine_prompt,
                            cwd=eval_cwd,
                            tools=j_base["tools"],
                            thinking_level=j_base["thinking_level"],
                            cancel_event=self._cancel_event,
                            max_retries=cfg.agent_max_retries,
                            retry_delay=cfg.agent_retry_delay,
                            session_file=None,
                        )
                        tokens += ar.token_usage
                        parsed = _parse_eval_md(ar.output)

                        self._emit("judge_eval", task_id, stage=3,
                                   judge_id=f"judge-{j_idx}", module=mod_name,
                                   passed=parsed["pass"], score=parsed["score"])

                        if parsed["pass"]:
                            s3_passed = True
                        else:
                            s3_feedback += f"\njudge-{j_idx}: {parsed['feedback'][:500]}"

                    if s3_passed:
                        # 强制反思：通过后 Worker 自查确认
                        self._emit("reflect", task_id, stage=3, module=mod_name)
                        ar = await run_agent(
                            prompt=reflect_refine_text,
                            system_prompt=refine_prompt_text,
                            session_file=refine_session,
                            **w_base,
                        )
                        tokens += ar.token_usage

                        # 如果模块被拆分了，新模块加入待处理队列
                        post_reflect_mods = _discover_modules(str(workspace))
                        if mod_name not in post_reflect_mods:
                            for nm in post_reflect_mods:
                                if nm not in refined_modules:
                                    modules_to_refine.append(nm)
                        refined_modules.add(mod_name)
                        break
                else:
                    refined_modules.add(mod_name)  # 达到最大重试，跳过

            # ═══════════════════════════════════════════════════
            # Stage 4: 子文件夹分析
            # ═══════════════════════════════════════════════════
            analyse_prompt_text = self._load_prompt(w_prompt_dir, "step4_analyse")
            judge_analyse_prompt = self._load_prompt(cfg.judges.system_prompt_dir, "step4_check_analyse")

            final_modules = _discover_modules(str(workspace))
            modules_needing_reclassify: list[str] = []

            reflect_analyse_text = self._load_prompt(w_prompt_dir, "reflect_analyse")

            for mod_name in final_modules:
                mod_dir = workspace / mod_name

                for attempt in range(MAX_RETRIES):
                    self._emit("stage", task_id, stage=4,
                               module=mod_name, attempt=attempt + 1)

                    # 4.1 Worker 分析
                    analyse_session = str(sess_dir / f"analyse-{mod_name}.jsonl")
                    feedback_part = ""
                    if attempt > 0:
                        feedback_part = f"\n\n# 评审意见\n\n{s4_feedback}\n\n请根据意见修正分析。"

                    ar = await run_agent(
                        prompt=f"分析模块 `{mod_name}` 的所有文件。{feedback_part}",
                        system_prompt=analyse_prompt_text,
                        session_file=analyse_session,
                        **w_base,
                    )
                    tokens += ar.token_usage

                    self._emit("stage_result", task_id, stage=4, module=mod_name)

                    # 4.2 Judge 评审
                    s4_passed = False
                    s4_feedback = ""
                    has_reclassify = False

                    for j_idx, j_cfg in enumerate(j_cfgs):
                        ar = await run_agent(
                            prompt=f"评审模块 `{mod_name}` 的分析报告。",
                            model=j_cfg.model,
                            system_prompt=judge_analyse_prompt,
                            cwd=str(mod_dir),
                            tools=j_base["tools"],
                            thinking_level=j_base["thinking_level"],
                            cancel_event=self._cancel_event,
                            max_retries=cfg.agent_max_retries,
                            retry_delay=cfg.agent_retry_delay,
                            session_file=None,
                        )
                        tokens += ar.token_usage
                        parsed = _parse_eval_md(ar.output)

                        self._emit("judge_eval", task_id, stage=4,
                                   judge_id=f"judge-{j_idx}", module=mod_name,
                                   passed=parsed["pass"], score=parsed["score"])

                        if "[需要重新分类]" in ar.output or "[需要重新分类]" in parsed["feedback"]:
                            has_reclassify = True

                        if parsed["pass"]:
                            s4_passed = True
                        else:
                            s4_feedback += f"\njudge-{j_idx}: {parsed['feedback'][:500]}"

                    # 4.3 分类问题 → 回 Stage 3
                    if has_reclassify:
                        self._emit("reclassify", task_id, module=mod_name)
                        modules_needing_reclassify.append(mod_name)
                        break

                    # 4.4 通过 → 强制反思后进入下一模块
                    if s4_passed:
                        self._emit("reflect", task_id, stage=4, module=mod_name)
                        ar = await run_agent(
                            prompt=reflect_analyse_text,
                            system_prompt=analyse_prompt_text,
                            session_file=analyse_session,
                            **w_base,
                        )
                        tokens += ar.token_usage
                        break

            # 如果有模块需要重新分类，回 Stage 3 处理
            if modules_needing_reclassify:
                self._emit("stage", task_id, stage="3-redo",
                           modules=modules_needing_reclassify)
                # 简化处理：标记但不无限循环

            # ═══════════════════════════════════════════════════
            # Stage 5: 最终检查
            # ═══════════════════════════════════════════════════
            judge_final_prompt = self._load_prompt(cfg.judges.system_prompt_dir, "step5_final_check")

            self._emit("stage", task_id, stage=5)
            for j_idx, j_cfg in enumerate(j_cfgs):
                ar = await run_agent(
                    prompt="运行最终检查脚本，验证所有模块输出完整性。",
                    model=j_cfg.model,
                    system_prompt=judge_final_prompt,
                    **j_base,
                )
                tokens += ar.token_usage
                parsed = _parse_eval_md(ar.output)

                self._emit("judge_eval", task_id, stage=5,
                           judge_id=f"judge-{j_idx}",
                           passed=parsed["pass"], score=parsed["score"])

            # ═══════════════════════════════════════════════════
            # 完成
            # ═══════════════════════════════════════════════════
            result.status = TaskStatus.PASSED
            result.total_tokens = tokens

            # 汇总最终模块列表
            final_mods = _discover_modules(str(workspace))
            result.final_output = self._build_final_summary(str(workspace), final_mods)

        except Exception as e:
            result.status = TaskStatus.ERROR
            result.error = str(e)
            self._emit("error", task_id, error=str(e))

        result.total_duration_ms = (time.time() - start) * 1000

        # 归档
        (out_dir / "result.json").write_text(result.model_dump_json(indent=2), encoding="utf-8")

        result_dir = Path(os.path.abspath(cfg.result_dir))
        result_dir.mkdir(parents=True, exist_ok=True)
        fname = f"{cfg.source_file}_{cfg.function_name}"

        result_md = result_dir / f"{fname}.md"
        result_md.write_text(
            f"---\ntask_id: {task_id}\nstatus: {result.status.value}\n"
            f"modules: {final_mods if 'final_mods' in dir() else []}\n"
            f"duration: {result.total_duration_ms / 1000:.1f}s\n---\n\n"
            f"{result.final_output}",
            encoding="utf-8")

        archive_path = str(result_dir / f"{fname}_log")
        shutil.make_archive(archive_path, "zip", str(out_dir.parent), out_dir.name)

        self._emit("task_end", task_id, status=result.status.value,
                    archive=f"{archive_path}.zip", result_file=str(result_md))

        try:
            shutil.rmtree(str(out_dir))
        except OSError:
            pass

        return result

    # ═══════════════════════════════════════════════════════════════════════
    # 工具方法
    # ═══════════════════════════════════════════════════════════════════════

    def _load_prompt(self, prompt_dir: str, name: str) -> str:
        for ext in [".md", ".txt", ""]:
            p = Path(prompt_dir) / f"{name}{ext}"
            if p.exists():
                return p.read_text(encoding="utf-8").strip()
        return ""

    def _build_final_summary(self, workspace: str, modules: list[str]) -> str:
        lines = ["# 系统模块分析报告\n"]
        lines.append(f"| 模块 | 文件数 | 报告 | 风险 |")
        lines.append(f"|------|--------|------|------|")

        for mod in modules:
            mod_dir = Path(workspace) / mod
            flist = mod_dir / "files.list"
            report = mod_dir / "module_report.md"
            fc = sum(1 for l in flist.read_text(encoding="utf-8").splitlines() if l.strip()) if flist.exists() else 0
            has_report = "✅" if report.exists() and report.stat().st_size > 100 else "❌"

            # 提取风险评分
            risk = "-"
            if report.exists():
                try:
                    content = report.read_text(encoding="utf-8")
                    m = re.search(r'(?:风险评分|risk)[::：]\s*(\d+)', content, re.IGNORECASE)
                    if m:
                        score = int(m.group(1))
                        risk = f"🔴 {score}" if score >= 70 else f"🟡 {score}" if score >= 40 else f"🟢 {score}"
                except OSError:
                    pass

            lines.append(f"| {mod} | {fc} | {has_report} | {risk} |")

        lines.append(f"\n**总计 {len(modules)} 个模块**")
        return "\n".join(lines)
