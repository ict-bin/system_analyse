"""
system_analyse — 四阶段流水线编排器 v2

Stage 1: Worker 全局分类 + Judge 脚本检查
Stage 2: 遍历子文件夹 — Worker 细分 + Judge 评审
Stage 3: 遍历子文件夹 — Worker 分析 + Judge 评审
Stage 4: Judge 完整性检查(缺失回 Stage 2) + Worker 生成报告 + Judge 评审报告

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
from .runner import run_agent, run_agents_parallel, AgentResult


# ─── 异常 ────────────────────────────────────────────────────────────────────

class StageError(Exception):
    pass


class PiFatalError(StageError):
    """pi 进程致命错误（模型未找到、配置错误等不可重试错误）"""
    pass


def _check_agent_result(ar: AgentResult, context: str = "") -> None:
    """检查 run_agent 返回结果，致命错误立即抛异常。"""
    if getattr(ar, "fatal", False):
        msg = f"pi 致命错误"
        if context:
            msg += f" [{context}]"
        msg += f": {ar.error or 'unknown'}"
        raise PiFatalError(msg)


# ─── 工具函数 ─────────────────────────────────────────────────────────────────

async def _run_agent_checked(context: str = "", **kwargs) -> AgentResult:
    """run_agent 的包装：执行后自动检查致命错误。"""
    ar = await run_agent(**kwargs)
    _check_agent_result(ar, context)
    return ar


def _extract_result(output: str) -> str:
    m = re.search(r"<result>(.*?)</result>", output, re.DOTALL)
    return m.group(1).strip() if m else output


def _get_modules_root(workspace: str) -> Path:
    """获取模块所在的实际根目录。
    兼容两种布局:
      workspace/<module>/files.list          -> 返回 workspace
      workspace/modules/<module>/files.list  -> 返回 workspace/modules
    """
    ws = Path(workspace)
    modules_subdir = ws / "modules"
    if modules_subdir.is_dir():
        for d in modules_subdir.iterdir():
            if d.is_dir() and (d / "files.list").exists():
                return modules_subdir
    return ws


def _discover_modules(workspace: str) -> list[str]:
    """发现 workspace 下的模块目录名列表。"""
    modules = []
    root = _get_modules_root(workspace)
    if not root.is_dir():
        return modules
    for d in sorted(root.iterdir()):
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
        _ms = list(re.finditer(pat, output))
        if _ms:
            score = min(int(_ms[-1].group(1)), 100)
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
        _ms = list(re.finditer(pat, output, re.IGNORECASE))
        if _ms:
            passed = _ms[-1].group(1).lower() in ('是', 'true', 'yes', 'pass')
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

    # score=0 且明确"不通过" → 直接返回，不走语义推断
    _fail_pats = [r'通过[::=：]\s*否', r'[Pp]ass[::=：]\s*(?:no|false|fail)', r'RESULT[::=：]\s*FAIL']
    if score == 0 and any(re.search(p, output, re.IGNORECASE) for p in _fail_pats):
        return {"pass": False, "score": 0, "feedback": feedback or output[:500]}

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

        # flag 文件：立即写 0（失败），只有完全成功才改为 1
        result_dir = Path(os.path.abspath(cfg.result_dir))
        result_dir.mkdir(parents=True, exist_ok=True)
        flag_path = result_dir / "flag"
        flag_path.write_text("0", encoding="utf-8")

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
            "pi_max_retries": cfg.pi_max_retries,
            "pi_retry_delay": cfg.pi_retry_delay,
        }

        j_base_kw = {
            "thinking_level": cfg.judges.default_thinking_level or "off",
            "cancel_event": self._cancel_event,
            "max_retries": cfg.agent_max_retries,
            "retry_delay": cfg.agent_retry_delay,
            "pi_max_retries": cfg.pi_max_retries,
            "pi_retry_delay": cfg.pi_retry_delay,
            "session_file": None,
        }

        tokens = TokenUsage()

        try:
            # ═══════════════════════════════════════════════════
            # Stage 0: 文件类型过滤
            # ═══════════════════════════════════════════════════
            filter_script = "/opt/system_analyse/scripts/filter_files.sh"
            if os.path.isfile(filter_script):
                types_str = " ".join(cfg.analyse_targets)
                self._emit("stage", task_id, stage="filter", types=types_str)
                proc = await asyncio.create_subprocess_exec(
                    "bash", filter_script, cfg.target_dir,
                    str(workspace / "filtered_files.txt"), *cfg.analyse_targets,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, _ = await proc.communicate()
                filter_count = 0
                filtered_path = workspace / "filtered_files.txt"
                if filtered_path.exists():
                    filter_count = sum(
                        1 for l in filtered_path.read_text("utf-8").splitlines() if l.strip())
                self._emit("stage_result", task_id, stage="filter",
                           types=cfg.analyse_targets, file_count=filter_count)

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

            # ── Step A: Worker 探索目录生成关键词列表 ──
            explore_prompt = self._load_prompt(w_prompt_dir, "step1_explore")
            prescan_summary = ""
            if explore_prompt:
                self._emit("stage", task_id, stage="explore")
                explore_session = str(sess_dir / "explore.jsonl")
                ar = await _run_agent_checked(
                    prompt=cfg.task,
                    system_prompt=explore_prompt,
                    session_file=explore_session,
                    **w_base,
                )
                tokens += ar.token_usage

                # Step B: 用 Worker 生成的 keywords.txt 跑预扫描脚本
                keywords_file = workspace / "keywords.txt"
                prescan_script = "/opt/system_analyse/scripts/prescan_files.sh"
                if keywords_file.exists() and os.path.isfile(prescan_script):
                    self._emit("stage", task_id, stage="prescan")
                    try:
                        proc = await asyncio.create_subprocess_exec(
                            "bash", prescan_script, cfg.target_dir, str(workspace),
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                        )
                        stdout, stderr = await proc.communicate()
                        summary_file = workspace / "keyword_summary.txt"
                        if summary_file.exists():
                            prescan_summary = summary_file.read_text("utf-8")
                            self._emit("stage_result", task_id, stage="prescan",
                                       summary_lines=prescan_summary.count(chr(10)))
                    except Exception as e:
                        self._emit("error", task_id, error=f"预扫描失败: {e}")

            for attempt in range(self._max_iter(s_cfg)):
                self._emit("stage", task_id, stage=1, attempt=attempt + 1)

                # Worker 工作
                prompt_parts = [cfg.task]
                # 第一轮附带预扫描摘要（如果有）
                if attempt == 0 and prescan_summary:
                    prompt_parts.append(
                        f"\n\n# 预扫描摘要（已自动生成，请基于此分类）\n\n"
                        f"{prescan_summary}\n\n"
                        f"预扫描已将文件按关键词分组到 `prescan/` 目录下，"
                        f"每个 `prescan/<keyword>.list` 包含对应文件列表。\n"
                        f"你可以直接用脚本将 prescan/*.list 移入模块目录。")
                if feedback:
                    prompt_parts.append(f"\n\n{feedback}")
                ar = await _run_agent_checked(
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
                    ar = await _run_agent_checked(
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
                        _jfb = "\n".join(f"judge-{i}: {r['feedback'][:500]}" for i, r in enumerate(judge_results))
                        feedback += f"\n\n## Judge 上轮意见\n\n{_jfb}"
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
                mod_dir = _get_modules_root(str(workspace)) / mod_name
                if not (mod_dir / "files.list").exists():
                    continue

                # 计算文件数（供主从模式判断用）
                try:
                    _fc = sum(1 for l in (mod_dir / "files.list").read_text("utf-8").splitlines() if l.strip())
                except OSError:
                    _fc = 0

                refine_session = str(sess_dir / f"refine-{mod_name}.jsonl")
                feedback = ""
                passed_count = 0

                for attempt in range(self._max_iter(s_cfg)):
                    self._emit("stage", task_id, stage=2,
                               module=mod_name, attempt=attempt + 1)

                    # 记录 Worker 执行前的模块快照
                    mods_before = set(_discover_modules(str(workspace)))

                    # Worker 细分
                    prompt_parts = [f"检查模块 `{mod_name}` 是否需要细分。"]
                    if feedback:
                        prompt_parts.append(f"\n\n{feedback}")
                    ar = await _run_agent_checked(
                        prompt="\n".join(prompt_parts),
                        system_prompt=w_sys_prompt,
                        session_file=refine_session,
                        **w_base,
                    )
                    tokens += ar.token_usage

                    # 用快照差集计算本次 Worker 真正新增的模块
                    mods_after = set(_discover_modules(str(workspace)))
                    new_ones = sorted(mods_after - mods_before)
                    was_split = mod_name not in mods_after and bool(new_ones)

                    self._emit("stage_result", task_id, stage=2,
                               module=mod_name, split=was_split, new_modules=new_ones)

                    # Judge 评审（cwd=workspace，check_classification.sh 需全局 */files.list）
                    judge_results = []
                    eval_cwd = str(workspace)

                    for j_idx, j_cfg_item in enumerate(j_cfgs):
                        ar = await _run_agent_checked(
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
                            # 完成：如果模块被拆分，将本次新增的模块加入队列
                            if was_split and new_ones:
                                for nm in new_ones:
                                    if nm not in refined_modules and nm not in modules_to_refine:
                                        modules_to_refine.append(nm)
                            refined_modules.add(mod_name)
                            break
                        else:
                            self._emit("reflect", task_id, stage=2,
                                       module=mod_name, round=passed_count)
                            feedback = f"# 自查要求（第 {passed_count} 次通过，需至少 {s_cfg.min_rounds} 次）\n\n{reflect_prompt}"
                            _jfb = "\n".join(f"judge-{i}: {r['feedback'][:500]}" for i, r in enumerate(judge_results))
                            feedback += f"\n\n## Judge 上轮意见\n\n{_jfb}"
                    else:
                        passed_count = 0
                        fail_fb = "\n".join(f"judge-{i}: {r['feedback'][:500]}"
                                            for i, r in enumerate(judge_results) if not r["pass"])
                        # 区分文件丢失和拆分不合理，给出明确指导
                        if "missing" in fail_fb.lower() or "丢失" in fail_fb or "遗漏" in fail_fb:
                            guidance = (
                                "\n\n⚠️ **文件丢失！** 请修复文件覆盖问题，不要改变拆分策略。"
                                "\n运行 check_classification.sh 查看遗漏文件，将它们归入合适的模块。")
                        else:
                            guidance = "\n\n请根据评审意见调整拆分策略。"
                        feedback = f"# 评审意见（未通过）\n\n{fail_fb}{guidance}"
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
                mod_dir = _get_modules_root(str(workspace)) / mod_name
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
                    ar = await _run_agent_checked(
                        prompt="\n".join(prompt_parts),
                        system_prompt=w_sys_prompt,
                        session_file=analyse_session,
                        **w_base,
                    )
                    tokens += ar.token_usage
                    self._emit("stage_result", task_id, stage=3, module=mod_name)

                    # Judge 评审
                    judge_results = []

                    for j_idx, j_cfg_item in enumerate(j_cfgs):
                        ar = await _run_agent_checked(
                            prompt=f"评审模块 `{mod_name}` 的分析报告。",
                            model=j_cfg_item.model,
                            system_prompt=j_sys_prompt,
                            tools=cfg.judges.default_tools,
                            cwd=str(mod_dir) if mod_dir.exists() else str(workspace),
                            **j_base_kw,
                        )
                        tokens += ar.token_usage
                        parsed = _parse_eval_md(ar.output)
                        judge_results.append(parsed)

                        self._emit("judge_eval", task_id, stage=3,
                                   judge_id=f"judge-{j_idx}", module=mod_name,
                                   passed=parsed["pass"], score=parsed["score"])

                        self._archive(out_dir,
                            f"s3-{mod_name}-a{attempt+1}-j{j_idx}.md",
                            f"Score: {parsed['score']}\nPass: {parsed['pass']}\n\n"
                            f"{parsed['feedback']}\n\n---\n## Raw Output\n\n{ar.output[:3000]}")

                    # 故障注入（必须在 reclassify 检测前）

                    # 分类问题 → 投票确认是否真需要重分类
                    has_reclassify = any("[需要重新分类]" in r.get("feedback", "")
                                        for r in judge_results)
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
                            _jfb = "\n".join(f"judge-{i}: {r['feedback'][:500]}" for i, r in enumerate(judge_results))
                            feedback += f"\n\n## Judge 上轮意见\n\n{_jfb}"
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
                    mod_dir = _get_modules_root(str(workspace)) / mod_name
                    if not mod_dir.exists():
                        continue

                    refine_session = str(sess_dir / f"refine-redo-{mod_name}.jsonl")
                    feedback = f"# 重分类要求\n\nStage 3 分析发现该模块分类不合理，需要重新细分。"
                    passed_count = 0

                    for attempt in range(self._max_iter(s_cfg_redo)):
                        self._emit("stage", task_id, stage="2-redo",
                                   module=mod_name, attempt=attempt + 1)

                        ar = await _run_agent_checked(
                            prompt=f"重新检查模块 `{mod_name}` 并细分。\n\n{feedback}",
                            system_prompt=w_sys_refine,
                            session_file=refine_session,
                            **w_base,
                        )
                        tokens += ar.token_usage

                        judge_results = []
                        eval_cwd = str(mod_dir) if mod_dir.exists() else str(workspace)
                        for j_idx, j_cfg_item in enumerate(j_cfgs):
                            ar = await _run_agent_checked(
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

                        voted_pass = _check_voting(
                            s_cfg_redo.pass_mode, j_count)
                        if voted_pass:
                            passed_count += 1
                            if passed_count >= s_cfg_redo.min_rounds:
                                break
                            feedback = f"# 自查要求\n\n{reflect_refine}"
                            _jfb = "\n".join(f"judge-{i}: {r['feedback'][:500]}" for i, r in enumerate(judge_results))
                            feedback += f"\n\n## Judge 上轮意见\n\n{_jfb}"
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
                        mod_dir = _get_modules_root(str(workspace)) / mod_name
                        analyse_session = str(sess_dir / f"analyse-redo-{mod_name}.jsonl")
                        feedback = ""
                        passed_count = 0
                        for attempt in range(self._max_iter(s_cfg_a)):
                            self._emit("stage", task_id, stage="3-redo",
                                       module=mod_name, attempt=attempt + 1)
                            prompt_parts = [f"分析模块 `{mod_name}` 的所有文件。"]
                            if feedback:
                                prompt_parts.append(f"\n\n{feedback}")
                            ar = await _run_agent_checked(
                                prompt="\n".join(prompt_parts),
                                system_prompt=w_sys_analyse,
                                session_file=analyse_session,
                                **w_base,
                            )
                            tokens += ar.token_usage

                            judge_results = []
                            for j_idx, j_cfg_item in enumerate(j_cfgs):
                                ar = await _run_agent_checked(
                                    prompt=f"评审模块 `{mod_name}` 的分析报告。",
                                    model=j_cfg_item.model,
                                    system_prompt=j_sys_analyse,
                                    tools=cfg.judges.default_tools,
                                    cwd=str(mod_dir) if mod_dir.exists() else str(workspace),
                                    **j_base_kw,
                                )
                                tokens += ar.token_usage
                                parsed = _parse_eval_md(ar.output)
                                judge_results.append(parsed)
                                self._emit("judge_eval", task_id, stage="3-redo",
                                           judge_id=f"judge-{j_idx}", module=mod_name,
                                           passed=parsed["pass"], score=parsed["score"])

                            voted_pass = _check_voting(
                                s_cfg_a.pass_mode, j_count)
                            if voted_pass:
                                passed_count += 1
                                if passed_count >= s_cfg_a.min_rounds:
                                    break
                                feedback = f"# 自查要求\n\n{reflect_analyse}"
                                _jfb = "\n".join(f"judge-{i}: {r['feedback'][:500]}" for i, r in enumerate(judge_results))
                                feedback += f"\n\n## Judge 上轮意见\n\n{_jfb}"
                            else:
                                passed_count = 0
                                fail_fb = "\n".join(f"judge-{i}: {r['feedback'][:500]}"
                                                    for i, r in enumerate(judge_results) if not r["pass"])
                                feedback = f"# 评审意见\n\n{fail_fb}"
                        else:
                            raise StageError(
                                f"Stage 3-redo 模块 {mod_name} 分析未通过")



            # ═══════════════════════════════════════════════════
            # Stage 4a: Judge 完整性检查（缺失模块回 Stage 2+3）
            # ═══════════════════════════════════════════════════
            j_completeness_prompt = self._load_prompt(
                cfg.judges.system_prompt_dir, "step4_check_completeness")

            self._emit("stage", task_id, stage="4a")
            judge_results = []
            missing_modules = []
            for j_idx, j_cfg_item in enumerate(j_cfgs):
                ar = await _run_agent_checked(
                    prompt="运行 check_outputs.sh 检查所有模块是否都有 module_report.md。",
                    model=j_cfg_item.model,
                    system_prompt=j_completeness_prompt,
                    tools=cfg.judges.default_tools,
                    cwd=str(workspace),
                    **j_base_kw,
                )
                tokens += ar.token_usage
                parsed = _parse_eval_md(ar.output)
                judge_results.append(parsed)

                self._emit("judge_eval", task_id, stage="4a",
                           judge_id=f"judge-{j_idx}",
                           passed=parsed["pass"], score=parsed["score"])

                self._archive(out_dir,
                    f"s4a-j{j_idx}.md",
                    f"Score: {parsed['score']}\nPass: {parsed['pass']}\n\n"
                    f"{parsed['feedback']}\n\n---\n## Raw Output\n\n{ar.output[:3000]}")

                # 从脚本输出提取缺失模块名（❌ module_name）
                if not parsed["pass"]:
                    for m in re.findall(r'\u274c\s+(\S+)', ar.output):
                        if m not in missing_modules:
                            missing_modules.append(m)

            s4a_pass = _check_voting(judge_results, "all", j_count)

            # 有缺失模块 → 回 Stage 2+3 补做
            if not s4a_pass and missing_modules:
                self._emit("stage", task_id, stage="2-redo-s4",
                           modules=missing_modules)

                s_cfg_redo = cfg.stages.refine
                w_sys_refine = self._load_prompt(w_prompt_dir, "step2_refine")
                j_sys_refine = self._load_prompt(
                    cfg.judges.system_prompt_dir, "step2_check_refine")
                w_sys_analyse = self._load_prompt(w_prompt_dir, "step3_analyse")
                j_sys_analyse = self._load_prompt(
                    cfg.judges.system_prompt_dir, "step3_check_analyse")
                s_cfg_a = cfg.stages.analyse

                for mod_name in missing_modules:
                    mod_dir = _get_modules_root(str(workspace)) / mod_name
                    if not mod_dir.exists() or not (mod_dir / "files.list").exists():
                        continue

                    # Stage 2 补做
                    refine_session = str(sess_dir / f"refine-s4-{mod_name}.jsonl")
                    ar = await _run_agent_checked(
                        prompt=f"检查模块 `{mod_name}` 是否需要细分。",
                        system_prompt=w_sys_refine,
                        session_file=refine_session,
                        **w_base,
                    )
                    tokens += ar.token_usage

                    # Stage 3 补做
                    analyse_session = str(sess_dir / f"analyse-s4-{mod_name}.jsonl")
                    feedback = ""
                    for attempt in range(self._max_iter(s_cfg_a)):
                        prompt_parts = [f"分析模块 `{mod_name}` 的所有文件。"]
                        if feedback:
                            prompt_parts.append(f"\n\n{feedback}")
                        ar = await _run_agent_checked(
                            prompt="\n".join(prompt_parts),
                            system_prompt=w_sys_analyse,
                            session_file=analyse_session,
                            **w_base,
                        )
                        tokens += ar.token_usage

                        judge_results = []
                        for j_idx, j_cfg_item in enumerate(j_cfgs):
                            ar = await _run_agent_checked(
                                prompt=f"评审模块 `{mod_name}` 的分析报告。",
                                model=j_cfg_item.model,
                                system_prompt=j_sys_analyse,
                                tools=cfg.judges.default_tools,
                                cwd=str(mod_dir) if mod_dir.exists() else str(workspace),
                                **j_base_kw,
                            )
                            tokens += ar.token_usage
                            parsed = _parse_eval_md(ar.output)
                            judge_results.append(parsed)
                            self._emit("judge_eval", task_id, stage="3-redo-s4",
                                       judge_id=f"judge-{j_idx}", module=mod_name,
                                       passed=parsed["pass"], score=parsed["score"])

                        if _check_voting(judge_results, s_cfg_a.pass_mode, j_count):
                            break
                        fail_fb = "\n".join(
                            f"judge-{i}: {r['feedback'][:500]}"
                            for i, r in enumerate(judge_results) if not r["pass"])
                        feedback = f"# 评审意见\n\n{fail_fb}"
                    else:
                        raise StageError(
                            f"Stage 4a 补做模块 {mod_name} 分析未通过")

            # ═══════════════════════════════════════════════════
            # Stage 4b: Worker 生成最终报告 + Judge 评审
            # ═══════════════════════════════════════════════════
            s_cfg = cfg.stages.final_check
            report_sys_prompt = self._load_prompt(w_prompt_dir, "step4_final_report")
            j_report_prompt = self._load_prompt(
                cfg.judges.system_prompt_dir, "step4_check_report")
            reflect_report = self._load_prompt(w_prompt_dir, "reflect_report")
            report_session = str(sess_dir / "final_report.jsonl")

            feedback = ""
            passed_count = 0

            for attempt in range(self._max_iter(s_cfg)):
                self._emit("stage", task_id, stage="4b", attempt=attempt + 1)

                # Worker 生成报告
                prompt_parts = [
                    "读取所有模块的 module_report.md，生成最终分析总报告 final_report.md。"]
                if feedback:
                    prompt_parts.append(f"\n\n{feedback}")
                ar = await _run_agent_checked(
                    prompt="\n".join(prompt_parts),
                    system_prompt=report_sys_prompt,
                    session_file=report_session,
                    **w_base,
                )
                tokens += ar.token_usage

                has_report = (workspace / "final_report.md").exists()
                self._emit("stage_result", task_id, stage="4b",
                           has_report=has_report)

                # Judge 评审报告
                judge_results = []
                for j_idx, j_cfg_item in enumerate(j_cfgs):
                    ar = await _run_agent_checked(
                        prompt="评审 final_report.md 的质量和完整性。",
                        model=j_cfg_item.model,
                        system_prompt=j_report_prompt,
                        tools=cfg.judges.default_tools,
                        cwd=str(workspace),
                        **j_base_kw,
                    )
                    tokens += ar.token_usage
                    parsed = _parse_eval_md(ar.output)
                    judge_results.append(parsed)

                    self._emit("judge_eval", task_id, stage="4b",
                               judge_id=f"judge-{j_idx}",
                               passed=parsed["pass"], score=parsed["score"])

                    self._archive(out_dir,
                        f"s4b-a{attempt+1}-j{j_idx}.md",
                        f"Score: {parsed['score']}\nPass: {parsed['pass']}\n\n"
                        f"{parsed['feedback']}\n\n---\n## Raw Output\n\n{ar.output[:3000]}")

                voted_pass = _check_voting(judge_results, s_cfg.pass_mode, j_count)

                if voted_pass:
                    passed_count += 1
                    if passed_count >= s_cfg.min_rounds:
                        break
                    else:
                        self._emit("reflect", task_id, stage="4b",
                                   round=passed_count, min_rounds=s_cfg.min_rounds)
                        feedback = (f"# 自查要求（第 {passed_count} 次通过，"
                                    f"需至少 {s_cfg.min_rounds} 次）\n\n{reflect_report}")
                else:
                    passed_count = 0
                    fail_fb = "\n".join(
                        f"judge-{i}: {r['feedback'][:500]}"
                        for i, r in enumerate(judge_results) if not r["pass"])
                    feedback = (f"# 评审意见（未通过）\n\n{fail_fb}"
                                f"\n\n请根据意见修正 final_report.md。")
            else:
                raise StageError(
                    f"Stage 4b 最终报告未通过，已达最大轮数 {s_cfg.max_rounds}")


            # ═══════════════════════════════════════════════════
            # 完成
            # ═══════════════════════════════════════════════════
            result.status = TaskStatus.PASSED
            result.total_tokens = tokens

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

        # ── 组装输出目录 ─────────────────────────────────────
        final_mods = _discover_modules(str(workspace))

        # 1) modules/ — 分类后的模块文件夹 (files.list + module_report.md)
        modules_out = result_dir / "modules"
        if modules_out.exists():
            shutil.rmtree(str(modules_out))
        modules_out.mkdir(parents=True, exist_ok=True)
        for mod in final_mods:
            src = _get_modules_root(str(workspace)) / mod
            dst = modules_out / mod
            if src.is_dir():
                shutil.copytree(str(src), str(dst))

        # 2) final_report.md — 最终分析报告
        report_src = workspace / "final_report.md"
        report_dst = result_dir / "final_report.md"
        if report_src.exists():
            shutil.copy2(str(report_src), str(report_dst))
        elif result.status in (TaskStatus.FAILED, TaskStatus.ERROR):
            # 失败/错误时也输出 final_report.md，记录失败原因和已完成的进度
            self._write_failure_report(
                report_dst, result, final_mods,
                str(_get_modules_root(str(workspace))))

        # 3) modules.list — 按风险等级排序的全模块列表
        self._generate_modules_list(modules_out, result_dir / "modules.list")

        # 5) 路径清洗 — 去除 /data/target/ 前缀
        self._strip_target_prefix(modules_out, cfg.target_dir)
        if report_dst.exists():
            self._strip_target_prefix(report_dst.parent, cfg.target_dir)

        # 6) archive.zip — 所有中间件 (judge评审、session、原始workspace)
        (out_dir / "result.json").write_text(
            result.model_dump_json(indent=2), encoding="utf-8")
        archive_path = str(result_dir / "archive")
        shutil.make_archive(archive_path, "zip", str(out_dir.parent), out_dir.name)

        # 写最终 flag: 成功=1, 失败/错误=0
        try:
            flag_path.write_text(
                "1" if result.status == TaskStatus.PASSED else "0",
                encoding="utf-8")
        except OSError:
            pass

        self._emit("task_end", task_id, status=result.status.value,
                    report=str(report_dst),
                    modules=str(modules_out),
                    archive=f"{archive_path}.zip")

        try:
            shutil.rmtree(str(out_dir))
        except OSError:
            pass

        return result


    # ═══════════════════════════════════════════════════════════════════════
    # 工具方法
    # ═══════════════════════════════════════════════════════════════════════


    @staticmethod
    def _write_failure_report(
        report_path: Path,
        result: "TaskResult",
        modules: list[str],
        modules_root: str,
    ) -> None:
        """任务失败/错误时生成 final_report.md，记录失败原因和已完成进度。"""
        lines = [
            "# 固件系统威胁分析总报告",
            "",
            f"> ⚠️ **任务状态：{result.status.value.upper()}**",
            "",
            "## 失败原因",
            "",
            f"```",
            f"{result.error or 'unknown error'}",
            f"```",
            "",
            f"- 任务ID: {result.task_id}",
            f"- 耗时: {result.total_duration_ms / 1000:.1f}s",
            "",
            "## 已完成的模块",
            "",
        ]
        if modules:
            lines.append("| 模块 | 文件数 | 报告 |")
            lines.append("|------|--------|------|")
            for mod in modules:
                mod_dir = Path(modules_root) / mod
                flist = mod_dir / "files.list"
                report = mod_dir / "module_report.md"
                fc = 0
                if flist.exists():
                    try:
                        fc = sum(1 for l in flist.read_text("utf-8").splitlines() if l.strip())
                    except OSError:
                        pass
                has_report = "✅" if report.exists() and report.stat().st_size > 100 else "❌"
                lines.append(f"| {mod} | {fc} | {has_report} |")
            lines.append("")
            lines.append(f"**已发现 {len(modules)} 个模块**")
        else:
            lines.append("*尚未完成模块分类*")
        lines.append("")

        try:
            report_path.write_text("\n".join(lines), encoding="utf-8")
        except OSError:
            pass

    @staticmethod
    def _generate_modules_list(modules_dir: Path, output_path: Path) -> None:
        """生成 modules.list：按风险等级排序，每行一个模块名。"""
        RISK_ORDER = {"严重": 0, "高": 1, "中": 2, "低": 3, "信息": 4, "未知": 5}
        entries: list[tuple[str, int, str]] = []

        for mod_dir in sorted(modules_dir.iterdir()):
            if not mod_dir.is_dir():
                continue
            mod_name = mod_dir.name
            risk_level = "未知"
            risk_score = 0
            report = mod_dir / "module_report.md"
            if report.exists():
                text = report.read_text("utf-8", errors="replace")[:2000]
                import re as _re
                m = _re.search(r'RISK_LEVEL:\s*(.+?)\s*-->', text)
                if m:
                    risk_level = m.group(1).strip()
                m = _re.search(r'RISK_SCORE:\s*(\d+)', text)
                if m:
                    risk_score = min(int(m.group(1)), 100)
            entries.append((risk_level, risk_score, mod_name))

        # 按风险等级排序（严重在前），同等级按分数降序
        entries.sort(key=lambda e: (RISK_ORDER.get(e[0], 5), -e[1]))
        output_path.write_text(
            "\n".join(name for _, _, name in entries) + "\n", encoding="utf-8")

    @staticmethod
    def _strip_target_prefix(output_dir: Path, target_dir: str) -> None:
        """将输出文件中的容器绝对路径 /data/target/... 替换为相对路径。

        执行期间 Worker 需要绝对路径来 read 文件，但最终输出应使用相对路径，
        因为 /data/target/ 是容器内挂载点，对用户无意义。
        """
        prefix = target_dir.rstrip("/") + "/"  # e.g. "/data/target/"
        for p in output_dir.rglob("*"):
            if not p.is_file():
                continue
            if p.suffix not in (".list", ".md", ".txt", ".json"):
                continue
            try:
                text = p.read_text(encoding="utf-8")
                if prefix in text:
                    p.write_text(text.replace(prefix, ""), encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                pass

    # ─── 主从模式：子 Worker 并行读文件 ────────────────────────────────────

    SUB_BATCH_SIZE = 20       # 每个子 Worker 处理的文件数
    SUB_WORKER_THRESHOLD = 20 # 文件数超过此值启用主从模式

    async def _collect_file_summaries(
        self, task_id: str, mod_name: str, mod_dir: Path,
        w_base: dict, tokens: "TokenUsage",
        sub_prompt_template: str,
    ) -> str:
        """串行启动子 Worker 逐批读取文件，返回合并后的文件摘要文本。

        每个 batch 完成后立即可用，节省算力（不并行占用多个 GPU slot）。
        """
        flist_path = mod_dir / "files.list"
        files = [l.strip() for l in flist_path.read_text("utf-8").splitlines() if l.strip()]

        # 分 batch
        batches: list[list[str]] = []
        for i in range(0, len(files), self.SUB_BATCH_SIZE):
            batches.append(files[i:i + self.SUB_BATCH_SIZE])

        self._emit("stage", task_id, stage="2-sub",
                    module=mod_name, batches=len(batches), files=len(files))

        # 串行执行：逐 batch 调用子 Worker
        summaries = []
        for idx, batch in enumerate(batches):
            file_list_text = chr(10).join(batch)
            prompt = (f"请逐个读取以下 {len(batch)} 个文件并输出摘要："
                      f"{chr(10)}{chr(10)}{file_list_text}")

            self._emit("stage", task_id, stage="2-sub",
                        module=mod_name, batch=idx + 1, total=len(batches))

            ar = await _run_agent_checked(
                prompt=prompt,
                model=w_base["model"],
                tools=w_base["tools"],
                system_prompt=sub_prompt_template,
                cwd=w_base["cwd"],
                thinking_level=w_base.get("thinking_level", "off"),
                session_file=None,
                cancel_event=w_base.get("cancel_event"),
                max_retries=w_base.get("max_retries", 3),
                retry_delay=w_base.get("retry_delay", 10),
                pi_max_retries=w_base.get("pi_max_retries", -1),
                pi_retry_delay=w_base.get("pi_retry_delay", 10),
            )
            tokens += ar.token_usage

            if ar.output:
                summaries.append(
                    f"# Batch {idx+1} ({len(batch)} files){chr(10)}{ar.output}")
            else:
                fallback = chr(10).join(
                    f"{f} | unknown | (读取失败)" for f in batch)
                summaries.append(
                    f"# Batch {idx+1} (fallback){chr(10)}{fallback}")

        merged = (chr(10) * 2).join(summaries)
        self._emit("stage_result", task_id, stage="2-sub",
                    module=mod_name, summary_lines=merged.count(chr(10)))
        return merged

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
