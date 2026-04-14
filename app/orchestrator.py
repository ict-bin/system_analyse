"""
system_analyse — 四阶段流水线编排器 v2

Stage 1: Worker 全局分类 + Judge 脚本检查
Stage 2: 遍历子文件夹 — Worker 细分 + Judge 评审
Stage 3: 遍历子文件夹 — Worker 分析 + Judge 评审
Stage 4: Judge 脚本最终检查

每个 stage 有独立 min_rounds / max_rounds / pass_mode
投票: majority(>50% judge 通过) 或 all(全部通过)
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

from .models import (
    AgentInstanceConfig,
    StageLoopConfig,
    SwarmEvent,
    TaskConfig,
    TaskResult,
    TaskStatus,
    TokenUsage,
    make_id,
)
from .runner import run_agent


# ─── 异常 ────────────────────────────────────────────────────────────────────

class StageError(Exception):
    """某阶段达到 max_rounds 仍未通过"""
    pass


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

    # ── 提取分数 ──
    for pat in [
        r'##\s*评分[::=：]\s*(\d+)',
        r'##\s*[Ss]core[::=：]\s*(\d+)',
        r'\*{0,2}评分\*{0,2}[::=：]\s*(\d+)',
        r'评分[::=：]\s*(\d+)',
        r'[Ss]core[::=：]\s*(\d+)',
        r'(\d{1,3})\s*/\s*100',
        r'\b(\d{2,3})\s*分',
    ]:
        m = re.search(pat, output)
        if m:
            score = min(int(m.group(1)), 100)
            break

    # ── 提取通过/不通过 ──
    for pat in [
        r'##\s*通过[::=：]\s*(是|否|true|false|yes|no|pass|fail)',
        r'##\s*[Pp]ass[::=：]\s*(是|否|true|false|yes|no)',
        r'\*{0,2}通过\*{0,2}[::=：]\s*(是|否|true|false)',
        r'通过[::=：]\s*(是|否|true|false)',
        r'[Pp]ass[::=：]\s*(是|否|true|false|yes|no)',
        r'RESULT[::=：]\s*(PASS|FAIL)',
    ]:
        m = re.search(pat, output, re.IGNORECASE)
        if m:
            passed = m.group(1).lower() in ('是', 'true', 'yes', 'pass')
            break
    else:
        if score >= 70:
            passed = True

    # ── 提取反馈 ──
    m = re.search(r'##\s*(?:评审意见|评审|反馈|[Ff]eedback)\s*\n(.*?)(?=\n##|$)', output, re.DOTALL)
    if m:
        feedback = m.group(1).strip()

    if score > 0:
        return {"pass": passed, "score": score, "feedback": feedback or output[:500]}

    # ── JSON fallback ──
    for i, ch in enumerate(output):
        if ch != '{':
            continue
        if '"pass"' not in output[i:i+200]:
            continue
        depth = 0
        in_str = False
        esc = False
        for j in range(i, min(i + 2000, len(output))):
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
                            return {"pass": bool(obj["pass"]),
                                    "score": int(obj.get("score", 0)),
                                    "feedback": str(obj.get("feedback", ""))}
                    except (json.JSONDecodeError, ValueError):
                        pass
                    break

    # ── 语义 fallback: 如果输出中包含正面判断词 ──
    positive_words = ['合理', '正确', '完整', '通过', '没有问题', 'pass', 'correct', 'reasonable']
    negative_words = ['不合理', '不正确', '遗漏', '缺失', '不通过', 'fail', 'incorrect', 'missing']
    text_lower = output.lower()
    pos_count = sum(1 for w in positive_words if w in text_lower)
    neg_count = sum(1 for w in negative_words if w in text_lower)
    if pos_count > neg_count and pos_count >= 2:
        return {"pass": True, "score": 75, "feedback": f"[语义推断] {output[:500]}"}

    return {"pass": False, "score": 0, "feedback": output[:500]}


def _check_voting(results: list[dict], pass_mode: str, judge_count: int) -> bool:
    """根据投票模式判断是否通过"""
    pass_count = sum(1 for r in results if r["pass"])
    if pass_mode == "all":
        return pass_count == judge_count
    else:  # majority
        return pass_count > judge_count / 2


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

    def _archive(self, out_dir: Path, name: str, content: str):
        """归档内容到文件"""
        try:
            (out_dir / name).write_text(content, encoding="utf-8")
        except OSError:
            pass

    def _max_iter(self, stage_cfg: StageLoopConfig) -> int:
        """max_rounds=-1 时返回一个很大的数"""
        return stage_cfg.max_rounds if stage_cfg.max_rounds > 0 else 999999

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
        j_count = len(j_cfgs)

        w_base = {
            "model": w_cfg.model,
            "tools": w_cfg.tools or cfg.workers.default_tools,
            "cwd": str(workspace),
            "thinking_level": w_cfg.thinking_level or cfg.workers.default_thinking_level,
            "cancel_event": self._cancel_event,
            "max_retries": cfg.agent_max_retries,
            "retry_delay": cfg.agent_retry_delay,
        }

        j_base_kw = {
            "thinking_level": cfg.judges.default_thinking_level or "off",
            "cancel_event": self._cancel_event,
            "max_retries": cfg.agent_max_retries,
            "retry_delay": cfg.agent_retry_delay,
            "session_file": None,
        }

        tokens = TokenUsage()

        try:
            # ═══════════════════════════════════════════════════
            # Stage 1: 全局分类 + 完整性检查
            # ═══════════════════════════════════════════════════
            s_cfg = cfg.stages.classify
            classify_session = str(sess_dir / "classify.jsonl")
            w_sys_prompt = self._load_prompt(w_prompt_dir, "step1_classify")
            j_sys_prompt = self._load_prompt(cfg.judges.system_prompt_dir, "step1_check_classify")
            reflect_prompt = self._load_prompt(w_prompt_dir, "reflect_classify")

            feedback = ""
            passed_count = 0  # 连续通过计数（用于 min_rounds）

            for attempt in range(self._max_iter(s_cfg)):
                self._emit("stage", task_id, stage=1, attempt=attempt + 1)

                # Worker 工作
                prompt_parts = [cfg.task]
                if feedback:
                    prompt_parts.append(f"\n\n{feedback}")
                ar = await run_agent(
                    prompt="\n".join(prompt_parts),
                    system_prompt=w_sys_prompt,
                    session_file=classify_session,
                    **w_base,
                )
                tokens += ar.token_usage
                result.final_output = _extract_result(ar.output)

                modules = _discover_modules(str(workspace))
                self._emit("stage_result", task_id, stage=1,
                           modules=modules, module_count=len(modules))

                # Judge 检查
                judge_results = []
                for j_idx, j_cfg_item in enumerate(j_cfgs):
                    ar = await run_agent(
                        prompt="请运行检查脚本验证分类完整性。",
                        model=j_cfg_item.model,
                        system_prompt=j_sys_prompt,
                        tools=cfg.judges.default_tools,
                        cwd=str(workspace),
                        **j_base_kw,
                    )
                    tokens += ar.token_usage
                    parsed = _parse_eval_md(ar.output)
                    judge_results.append(parsed)

                    self._emit("judge_eval", task_id, stage=1,
                               judge_id=f"judge-{j_idx}",
                               passed=parsed["pass"], score=parsed["score"])

                    self._archive(out_dir,
                        f"s1-a{attempt+1}-j{j_idx}.md",
                        f"Score: {parsed['score']}\nPass: {parsed['pass']}\n\n"
                        f"{parsed['feedback']}\n\n---\n## Raw Output\n\n{ar.output[:3000]}")

                voted_pass = _check_voting(judge_results, s_cfg.pass_mode, j_count)

                if voted_pass:
                    passed_count += 1
                    if passed_count >= s_cfg.min_rounds:
                        break  # 达到最少轮数 → 真正完成
                    else:
                        # 还没达到 min_rounds → 强制反思
                        self._emit("reflect", task_id, stage=1,
                                   round=passed_count, min_rounds=s_cfg.min_rounds)
                        feedback = f"# 自查要求（第 {passed_count} 次通过，需至少 {s_cfg.min_rounds} 次）\n\n{reflect_prompt}"
                else:
                    passed_count = 0  # 重置连续通过
                    fail_fb = "\n".join(f"judge-{i}: {r['feedback'][:500]}"
                                        for i, r in enumerate(judge_results) if not r["pass"])
                    feedback = f"# 评审意见（未通过）\n\n{fail_fb}\n\n请根据评审意见修正。"
            else:
                raise StageError(f"Stage 1 分类检查未通过，已达最大轮数 {s_cfg.max_rounds}")

            # ═══════════════════════════════════════════════════
            # Stage 2: 子文件夹细分
            # ═══════════════════════════════════════════════════
            s_cfg = cfg.stages.refine
            w_sys_prompt = self._load_prompt(w_prompt_dir, "step2_refine")
            j_sys_prompt = self._load_prompt(cfg.judges.system_prompt_dir, "step2_check_refine")
            reflect_prompt = self._load_prompt(w_prompt_dir, "reflect_refine")

            modules_to_refine = list(_discover_modules(str(workspace)))
            refined_modules: set[str] = set()

            while modules_to_refine:
                mod_name = modules_to_refine.pop(0)
                if mod_name in refined_modules:
                    continue
                mod_dir = workspace / mod_name
                if not (mod_dir / "files.list").exists():
                    continue

                refine_session = str(sess_dir / f"refine-{mod_name}.jsonl")
                feedback = ""
                passed_count = 0

                for attempt in range(self._max_iter(s_cfg)):
                    self._emit("stage", task_id, stage=2,
                               module=mod_name, attempt=attempt + 1)

                    # Worker 细分
                    prompt_parts = [f"检查模块 `{mod_name}` 是否需要细分。"]
                    if feedback:
                        prompt_parts.append(f"\n\n{feedback}")
                    ar = await run_agent(
                        prompt="\n".join(prompt_parts),
                        system_prompt=w_sys_prompt,
                        session_file=refine_session,
                        **w_base,
                    )
                    tokens += ar.token_usage

                    new_modules = _discover_modules(str(workspace))
                    new_ones = [m for m in new_modules if m not in refined_modules and m != mod_name]
                    was_split = mod_name not in new_modules and bool(new_ones)

                    self._emit("stage_result", task_id, stage=2,
                               module=mod_name, split=was_split, new_modules=new_ones)

                    # Judge 评审
                    judge_results = []
                    eval_cwd = str(mod_dir) if mod_dir.exists() else str(workspace)

                    for j_idx, j_cfg_item in enumerate(j_cfgs):
                        ar = await run_agent(
                            prompt=f"评审 Worker 对模块 `{mod_name}` 的细分判断。",
                            model=j_cfg_item.model,
                            system_prompt=j_sys_prompt,
                            tools=cfg.judges.default_tools,
                            cwd=eval_cwd,
                            **j_base_kw,
                        )
                        tokens += ar.token_usage
                        parsed = _parse_eval_md(ar.output)
                        judge_results.append(parsed)

                        self._emit("judge_eval", task_id, stage=2,
                                   judge_id=f"judge-{j_idx}", module=mod_name,
                                   passed=parsed["pass"], score=parsed["score"])

                        self._archive(out_dir,
                            f"s2-{mod_name}-a{attempt+1}-j{j_idx}.md",
                            f"Score: {parsed['score']}\nPass: {parsed['pass']}\n\n"
                            f"{parsed['feedback']}\n\n---\n## Raw Output\n\n{ar.output[:3000]}")

                    voted_pass = _check_voting(judge_results, s_cfg.pass_mode, j_count)

                    if voted_pass:
                        passed_count += 1
                        if passed_count >= s_cfg.min_rounds:
                            # 完成：如果模块被拆分，新模块加入队列
                            post_mods = _discover_modules(str(workspace))
                            if mod_name not in post_mods:
                                for nm in post_mods:
                                    if nm not in refined_modules:
                                        modules_to_refine.append(nm)
                            refined_modules.add(mod_name)
                            break
                        else:
                            self._emit("reflect", task_id, stage=2,
                                       module=mod_name, round=passed_count)
                            feedback = f"# 自查要求（第 {passed_count} 次通过，需至少 {s_cfg.min_rounds} 次）\n\n{reflect_prompt}"
                    else:
                        passed_count = 0
                        fail_fb = "\n".join(f"judge-{i}: {r['feedback'][:500]}"
                                            for i, r in enumerate(judge_results) if not r["pass"])
                        feedback = f"# 评审意见（未通过）\n\n{fail_fb}\n\n请根据意见修正。"
                else:
                    raise StageError(
                        f"Stage 2 模块 {mod_name} 细分未通过，已达最大轮数 {s_cfg.max_rounds}")

            # ═══════════════════════════════════════════════════
            # Stage 3: 子文件夹分析
            # ═══════════════════════════════════════════════════
            s_cfg = cfg.stages.analyse
            w_sys_prompt = self._load_prompt(w_prompt_dir, "step3_analyse")
            j_sys_prompt = self._load_prompt(cfg.judges.system_prompt_dir, "step3_check_analyse")
            reflect_prompt = self._load_prompt(w_prompt_dir, "reflect_analyse")

            final_modules = _discover_modules(str(workspace))
            modules_needing_reclassify: list[str] = []

            for mod_name in final_modules:
                mod_dir = workspace / mod_name
                analyse_session = str(sess_dir / f"analyse-{mod_name}.jsonl")
                feedback = ""
                passed_count = 0

                for attempt in range(self._max_iter(s_cfg)):
                    self._emit("stage", task_id, stage=3,
                               module=mod_name, attempt=attempt + 1)

                    # Worker 分析
                    prompt_parts = [f"分析模块 `{mod_name}` 的所有文件。"]
                    if feedback:
                        prompt_parts.append(f"\n\n{feedback}")
                    ar = await run_agent(
                        prompt="\n".join(prompt_parts),
                        system_prompt=w_sys_prompt,
                        session_file=analyse_session,
                        **w_base,
                    )
                    tokens += ar.token_usage
                    self._emit("stage_result", task_id, stage=3, module=mod_name)

                    # Judge 评审
                    judge_results = []
                    has_reclassify = False

                    for j_idx, j_cfg_item in enumerate(j_cfgs):
                        ar = await run_agent(
                            prompt=f"评审模块 `{mod_name}` 的分析报告。",
                            model=j_cfg_item.model,
                            system_prompt=j_sys_prompt,
                            tools=cfg.judges.default_tools,
                            cwd=str(mod_dir),
                            **j_base_kw,
                        )
                        tokens += ar.token_usage
                        parsed = _parse_eval_md(ar.output)
                        judge_results.append(parsed)

                        if "[需要重新分类]" in ar.output or "[需要重新分类]" in parsed["feedback"]:
                            has_reclassify = True

                        self._emit("judge_eval", task_id, stage=3,
                                   judge_id=f"judge-{j_idx}", module=mod_name,
                                   passed=parsed["pass"], score=parsed["score"])

                        self._archive(out_dir,
                            f"s3-{mod_name}-a{attempt+1}-j{j_idx}.md",
                            f"Score: {parsed['score']}\nPass: {parsed['pass']}\n"
                            f"Reclassify: {has_reclassify}\n\n"
                            f"{parsed['feedback']}\n\n---\n## Raw Output\n\n{ar.output[:3000]}")

                    # 分类问题 → 投票确认是否真需要重分类
                    if has_reclassify:
                        reclass_votes = sum(1 for r in judge_results
                                           if "[需要重新分类]" in r.get("feedback", ""))
                        if _check_voting(
                            [{"pass": True}] * reclass_votes + [{"pass": False}] * (j_count - reclass_votes),
                            s_cfg.pass_mode, j_count
                        ):
                            self._emit("reclassify", task_id, module=mod_name)
                            modules_needing_reclassify.append(mod_name)
                            break  # 跳出 attempt 循环，后面统一处理

                    voted_pass = _check_voting(judge_results, s_cfg.pass_mode, j_count)

                    if voted_pass:
                        passed_count += 1
                        if passed_count >= s_cfg.min_rounds:
                            break
                        else:
                            self._emit("reflect", task_id, stage=3,
                                       module=mod_name, round=passed_count)
                            feedback = f"# 自查要求（第 {passed_count} 次通过，需至少 {s_cfg.min_rounds} 次）\n\n{reflect_prompt}"
                    else:
                        passed_count = 0
                        fail_fb = "\n".join(f"judge-{i}: {r['feedback'][:500]}"
                                            for i, r in enumerate(judge_results) if not r["pass"])
                        feedback = f"# 评审意见（未通过）\n\n{fail_fb}\n\n请根据意见修正分析。"
                else:
                    if mod_name not in modules_needing_reclassify:
                        raise StageError(
                            f"Stage 3 模块 {mod_name} 分析未通过，已达最大轮数 {s_cfg.max_rounds}")

            # ── Stage 3 后处理：需要重分类的模块回 Stage 2 ──
            if modules_needing_reclassify:
                self._emit("stage", task_id, stage="2-redo",
                           modules=modules_needing_reclassify)

                s_cfg_redo = cfg.stages.refine
                w_sys_refine = self._load_prompt(w_prompt_dir, "step2_refine")
                j_sys_refine = self._load_prompt(cfg.judges.system_prompt_dir, "step2_check_refine")
                reflect_refine = self._load_prompt(w_prompt_dir, "reflect_refine")

                for mod_name in modules_needing_reclassify:
                    mod_dir = workspace / mod_name
                    if not mod_dir.exists():
                        continue

                    refine_session = str(sess_dir / f"refine-redo-{mod_name}.jsonl")
                    feedback = f"# 重分类要求\n\nStage 3 分析发现该模块分类不合理，需要重新细分。"
                    passed_count = 0

                    for attempt in range(self._max_iter(s_cfg_redo)):
                        self._emit("stage", task_id, stage="2-redo",
                                   module=mod_name, attempt=attempt + 1)

                        ar = await run_agent(
                            prompt=f"重新检查模块 `{mod_name}` 并细分。\n\n{feedback}",
                            system_prompt=w_sys_refine,
                            session_file=refine_session,
                            **w_base,
                        )
                        tokens += ar.token_usage

                        judge_results = []
                        eval_cwd = str(mod_dir) if mod_dir.exists() else str(workspace)
                        for j_idx, j_cfg_item in enumerate(j_cfgs):
                            ar = await run_agent(
                                prompt=f"评审模块 `{mod_name}` 的重新细分。",
                                model=j_cfg_item.model,
                                system_prompt=j_sys_refine,
                                tools=cfg.judges.default_tools,
                                cwd=eval_cwd,
                                **j_base_kw,
                            )
                            tokens += ar.token_usage
                            parsed = _parse_eval_md(ar.output)
                            judge_results.append(parsed)
                            self._emit("judge_eval", task_id, stage="2-redo",
                                       judge_id=f"judge-{j_idx}", module=mod_name,
                                       passed=parsed["pass"], score=parsed["score"])

                        voted_pass = _check_voting(judge_results, s_cfg_redo.pass_mode, j_count)
                        if voted_pass:
                            passed_count += 1
                            if passed_count >= s_cfg_redo.min_rounds:
                                break
                            feedback = f"# 自查要求\n\n{reflect_refine}"
                        else:
                            passed_count = 0
                            fail_fb = "\n".join(f"judge-{i}: {r['feedback'][:500]}"
                                                for i, r in enumerate(judge_results) if not r["pass"])
                            feedback = f"# 评审意见\n\n{fail_fb}"
                    else:
                        raise StageError(
                            f"Stage 2-redo 模块 {mod_name} 重分类未通过")

                # 重分类后的模块也需要 Stage 3 分析
                new_mods = _discover_modules(str(workspace))
                redo_analyse = [m for m in new_mods if m not in final_modules or m in modules_needing_reclassify]
                if redo_analyse:
                    self._emit("stage", task_id, stage="3-redo", modules=redo_analyse)
                    s_cfg_a = cfg.stages.analyse
                    w_sys_analyse = self._load_prompt(w_prompt_dir, "step3_analyse")
                    j_sys_analyse = self._load_prompt(cfg.judges.system_prompt_dir, "step3_check_analyse")
                    reflect_analyse = self._load_prompt(w_prompt_dir, "reflect_analyse")

                    for mod_name in redo_analyse:
                        mod_dir = workspace / mod_name
                        analyse_session = str(sess_dir / f"analyse-redo-{mod_name}.jsonl")
                        feedback = ""
                        passed_count = 0
                        for attempt in range(self._max_iter(s_cfg_a)):
                            self._emit("stage", task_id, stage="3-redo",
                                       module=mod_name, attempt=attempt + 1)
                            prompt_parts = [f"分析模块 `{mod_name}` 的所有文件。"]
                            if feedback:
                                prompt_parts.append(f"\n\n{feedback}")
                            ar = await run_agent(
                                prompt="\n".join(prompt_parts),
                                system_prompt=w_sys_analyse,
                                session_file=analyse_session,
                                **w_base,
                            )
                            tokens += ar.token_usage

                            judge_results = []
                            for j_idx, j_cfg_item in enumerate(j_cfgs):
                                ar = await run_agent(
                                    prompt=f"评审模块 `{mod_name}` 的分析报告。",
                                    model=j_cfg_item.model,
                                    system_prompt=j_sys_analyse,
                                    tools=cfg.judges.default_tools,
                                    cwd=str(mod_dir),
                                    **j_base_kw,
                                )
                                tokens += ar.token_usage
                                parsed = _parse_eval_md(ar.output)
                                judge_results.append(parsed)
                                self._emit("judge_eval", task_id, stage="3-redo",
                                           judge_id=f"judge-{j_idx}", module=mod_name,
                                           passed=parsed["pass"], score=parsed["score"])

                            voted_pass = _check_voting(judge_results, s_cfg_a.pass_mode, j_count)
                            if voted_pass:
                                passed_count += 1
                                if passed_count >= s_cfg_a.min_rounds:
                                    break
                                feedback = f"# 自查要求\n\n{reflect_analyse}"
                            else:
                                passed_count = 0
                                fail_fb = "\n".join(f"judge-{i}: {r['feedback'][:500]}"
                                                    for i, r in enumerate(judge_results) if not r["pass"])
                                feedback = f"# 评审意见\n\n{fail_fb}"
                        else:
                            raise StageError(
                                f"Stage 3-redo 模块 {mod_name} 分析未通过")

            # ═══════════════════════════════════════════════════
            # Stage 4: 最终检查
            # ═══════════════════════════════════════════════════
            s_cfg = cfg.stages.final_check
            j_sys_prompt = self._load_prompt(cfg.judges.system_prompt_dir, "step4_final_check")

            self._emit("stage", task_id, stage=4)
            judge_results = []
            for j_idx, j_cfg_item in enumerate(j_cfgs):
                ar = await run_agent(
                    prompt="运行最终检查脚本，验证所有模块输出完整性。",
                    model=j_cfg_item.model,
                    system_prompt=j_sys_prompt,
                    tools=cfg.judges.default_tools,
                    cwd=str(workspace),
                    **j_base_kw,
                )
                tokens += ar.token_usage
                parsed = _parse_eval_md(ar.output)
                judge_results.append(parsed)

                self._emit("judge_eval", task_id, stage=4,
                           judge_id=f"judge-{j_idx}",
                           passed=parsed["pass"], score=parsed["score"])

                self._archive(out_dir,
                    f"s4-j{j_idx}.md",
                    f"Score: {parsed['score']}\nPass: {parsed['pass']}\n\n"
                    f"{parsed['feedback']}\n\n---\n## Raw Output\n\n{ar.output[:3000]}")

            s5_pass = _check_voting(judge_results, s_cfg.pass_mode, j_count)
            if not s5_pass:
                raise StageError("Stage 4 最终检查未通过")

            # ═══════════════════════════════════════════════════
            # 完成
            # ═══════════════════════════════════════════════════
            result.status = TaskStatus.PASSED
            result.total_tokens = tokens
            final_mods = _discover_modules(str(workspace))
            result.final_output = self._build_final_summary(str(workspace), final_mods)

        except StageError as e:
            result.status = TaskStatus.FAILED
            result.error = str(e)
            result.total_tokens = tokens
            self._emit("stage_fail", task_id, error=str(e))

        except Exception as e:
            result.status = TaskStatus.ERROR
            result.error = str(e)
            result.total_tokens = tokens
            self._emit("error", task_id, error=str(e))

        result.total_duration_ms = (time.time() - start) * 1000

        # 归档
        final_mods = _discover_modules(str(workspace))
        (out_dir / "result.json").write_text(result.model_dump_json(indent=2), encoding="utf-8")

        result_dir = Path(os.path.abspath(cfg.result_dir))
        result_dir.mkdir(parents=True, exist_ok=True)
        fname = f"{cfg.source_file}_{cfg.function_name}"

        result_md = result_dir / f"{fname}.md"
        result_md.write_text(
            f"---\ntask_id: {task_id}\nstatus: {result.status.value}\n"
            f"modules: {final_mods}\n"
            f"duration: {result.total_duration_ms / 1000:.1f}s\n---\n\n"
            f"{result.final_output or result.error or ''}",
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
        lines.append("| 模块 | 文件数 | 报告 | 风险 |")
        lines.append("|------|--------|------|------|")

        for mod in modules:
            mod_dir = Path(workspace) / mod
            flist = mod_dir / "files.list"
            report = mod_dir / "module_report.md"
            fc = sum(1 for l in flist.read_text(encoding="utf-8").splitlines()
                     if l.strip()) if flist.exists() else 0
            has_report = "✅" if report.exists() and report.stat().st_size > 100 else "❌"

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
