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

    def _make_stream_handler(self, task_id: str, stage: str):
        """Returns an on_stream callback that batches LLM text and emits agent_stream events."""
        buf: list[str] = []
        buf_size = [0]

        def handler(text: str) -> None:
            buf.append(text)
            buf_size[0] += len(text)
            # Flush on newline or every 400 chars to provide live progress
            if "\n" in text or buf_size[0] >= 400:
                chunk = "".join(buf).strip()
                buf.clear()
                buf_size[0] = 0
                if chunk:
                    self._emit("agent_stream", task_id, stage=stage,
                               text=chunk[:600])

        return handler

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

        # ── resume_workspace: 直接使用已有 workspace（跳过 Stage 1/2）──
        if cfg.resume_workspace and cfg.start_stage > 1:
            workspace = Path(os.path.abspath(cfg.resume_workspace))
            out_dir = workspace.parent
            task_id = out_dir.name  # 继承原 task_id
            sess_dir = out_dir / "sessions"
            sess_dir.mkdir(exist_ok=True)
            task_tmp = workspace / "tmp"
            task_tmp.mkdir(exist_ok=True)
        else:
            out_dir = Path(os.path.abspath(cfg.output_dir)) / task_id
            out_dir.mkdir(parents=True, exist_ok=True)
            sess_dir = out_dir / "sessions"
            sess_dir.mkdir(exist_ok=True)
            workspace = out_dir / "workspace"
            workspace.mkdir(exist_ok=True)
            # Per-task workspace isolation: private tmp dir + read-only target symlink
            task_tmp = workspace / "tmp"
            task_tmp.mkdir(exist_ok=True)
            target_link = workspace / "target"
            if not target_link.exists():
                try:
                    target_link.symlink_to(os.path.abspath(cfg.target_dir))
                except OSError:
                    pass

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
            "tools": w_cfg.tools or cfg.workers.default_tools,
            "cwd": str(workspace),
            "env": {**os.environ, "TMPDIR": str(task_tmp), "HOME": str(workspace)},
            "thinking_level": w_cfg.thinking_level or cfg.workers.default_thinking_level,
            "cancel_event": self._cancel_event,
            "max_retries": cfg.agent_max_retries,
            "retry_delay": cfg.agent_retry_delay,
            "pi_max_retries": cfg.pi_max_retries,
            "pi_retry_delay": cfg.pi_retry_delay,
        }

        # 阶段模型获取助手（未配置 stage_models 时回退到 agents[0])
        def _wm(stage: str) -> str:
            return cfg.workers.model_for(stage)

        def _jm(stage: str, j_item: "AgentInstanceConfig") -> str:
            sm = cfg.judges.model_for(stage)
            return sm if sm else j_item.model

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
            # ── resume: start_stage>=3 时跳过 Stage 0-2 ──────────
            _skip_s12 = (cfg.start_stage >= 3 and bool(cfg.resume_workspace))

            # ═══════════════════════════════════════════════════
            if not _skip_s12:
                # Stage 0: 文件类型过滤
                # ═══════════════════════════════════════════════════
                filter_script = "/app/scripts/filter_files.sh"
                if os.path.isfile(filter_script):
                    types_str = " ".join(cfg.analyse_targets)
                    arch_str = " ".join(cfg.binary_arch)
                    self._emit("stage", task_id, stage="filter", types=types_str, arch=arch_str)
                    proc = await asyncio.create_subprocess_exec(
                        "bash", filter_script, cfg.target_dir,
                        str(workspace / "filtered_files.txt"),
                        "--arch", arch_str,
                        *cfg.analyse_targets,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                        env={**os.environ, "TMPDIR": str(task_tmp)},
                    )
                    stdout, stderr_bytes = await proc.communicate()
                    # Emit script output for visibility
                    _out = (stdout or b"").decode("utf-8", errors="replace").strip()
                    _err = (stderr_bytes or b"").decode("utf-8", errors="replace").strip()
                    _cli = (_out + ("\n" + _err if _err else "")).strip()
                    if _cli:
                        self._emit("cli_output", task_id, stage="filter", text=_cli[:3000])
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

                # ── Step A: Worker 探索目录生成关键词列表 ──
                explore_prompt = self._load_prompt(w_prompt_dir, "step1_explore")
                prescan_summary = ""
                if explore_prompt:
                    self._emit("stage", task_id, stage="explore")
                    self._emit("model", task_id, stage="explore", model=_wm("explore"))
                    explore_session = str(sess_dir / "explore.jsonl")
                    ar = await _run_agent_checked(
                        prompt=cfg.task,
                        model=_wm("explore"),
                        system_prompt=explore_prompt,
                        session_file=explore_session,
                        on_stream=self._make_stream_handler(task_id, "explore"),
                        **w_base,
                    )
                    tokens += ar.token_usage
                    # Emit the final agent output snippet for debugging
                    if ar.output:
                        self._emit("agent_output", task_id, stage="explore",
                                   output=ar.output[-1200:])

                    # Step B: 用 Worker 生成的 keywords.txt 跑预扫描脚本
                    keywords_file = workspace / "keywords.txt"
                    prescan_script = "/app/scripts/prescan_files.py"
                    if not os.path.isfile(prescan_script):
                        prescan_script = "/app/scripts/prescan_files.sh"
                    if keywords_file.exists() and os.path.isfile(prescan_script):
                        self._emit("stage", task_id, stage="prescan")
                        try:
                            cmd = (["python3", prescan_script] if prescan_script.endswith(".py")
                                   else ["bash", prescan_script])
                            proc = await asyncio.create_subprocess_exec(
                                *cmd, cfg.target_dir, str(workspace),
                                stdout=asyncio.subprocess.PIPE,
                                stderr=asyncio.subprocess.PIPE,
                                env={**os.environ, "TMPDIR": str(task_tmp)},
                            )
                            stdout, stderr = await proc.communicate()
                            # Emit prescan CLI output
                            _pout = (stdout or b"").decode("utf-8", errors="replace").strip()
                            _perr = (stderr or b"").decode("utf-8", errors="replace").strip()
                            _pcli = (_pout + ("\n" + _perr if _perr else "")).strip()
                            if _pcli:
                                self._emit("cli_output", task_id, stage="prescan", text=_pcli[:3000])
                            summary_file = workspace / "keyword_summary.txt"
                            if summary_file.exists():
                                prescan_summary = summary_file.read_text("utf-8")
                                self._emit("stage_result", task_id, stage="prescan",
                                           summary_lines=prescan_summary.count(chr(10)))
                        except Exception as e:
                            self._emit("error", task_id, error=f"预扫描失败: {e}")

                for attempt in range(self._max_iter(s_cfg)):
                    self._emit("stage", task_id, stage=1, attempt=attempt + 1)
                    self._emit("model", task_id, stage="classify",
                               worker=_wm("classify"), judge=_jm("classify", j_cfgs[0]) if j_cfgs else "?")

                    # Worker 工作
                    prompt_parts = [cfg.task]
                    # 始终注入工作目录和目标目录绝对路径，防止模型迷失
                    prompt_parts.append(
                        f"\n\n## 📂 关键路径（必须遵守）\n"
                        f"- **工作目录**（创建模块结构的位置）: `{workspace}`\n"
                        f"- **过滤文件列表**: `{workspace}/filtered_files.txt`\n"
                        f"- **目标文件目录**（仅用于读取文件内容）: `{cfg.target_dir}`\n\n"
                        f"⚠️ 所有 `modules/` 子目录必须创建在 `{workspace}/modules/` 下。\n"
                        f"⚠️ 执行 bash 命令时，始终先 `cd {workspace}` 再创建目录，切勿在目标目录下创建文件。"
                    )
                    # 第一轮：告知 Worker 过滤后的文件列表（如果有过滤）
                    filtered_path = workspace / "filtered_files.txt"
                    if attempt == 0 and filtered_path.exists():
                        fc = sum(1 for l in filtered_path.read_text("utf-8").splitlines() if l.strip())
                        prompt_parts.append(
                            chr(10)*2 +
                            f"❗ 当前配置已开启文件类型过滤，" +
                            f"`{workspace}/filtered_files.txt` 包含将要分析的 {fc} 个文件（相对于目标目录的相对路径）。" +
                            chr(10)*2 +
                            f"你必须且只能对这 {fc} 个文件进行分类，" +
                            "不要超出范围扫描其他文件。" +
                            chr(10)*2 +
                            f"分类时用 `cat {workspace}/filtered_files.txt` 作为输入源，" +
                            f"而不是 `find {cfg.target_dir} -type f`。" +
                            chr(10)*2 +
                            "每个模块的 files.list 写的是相对于目标目录的相对路径（与 filtered_files.txt 格式一致）。"
                        )
                    # 附带预扫描摘要（如果有）
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
                        model=_wm("classify"),
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
                            model=_jm("classify", j_cfg_item),
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
                        if attempt + 1 >= s_cfg.min_rounds:
                            break
                        else:
                            self._emit("reflect", task_id, stage=1,
                                round=attempt+1, min_rounds=s_cfg.min_rounds)
                            feedback = ("# 自查要求（第 " + str(attempt+1) +
                                " 轮，需至少 " + str(s_cfg.min_rounds) + " 轮）" +
                                chr(10)*2 + reflect_prompt)
                            _jfb = chr(10).join(
                                f"judge-{i}: {r['feedback'][:500]}" for i, r in enumerate(judge_results))
                            feedback += chr(10)*2 + "## Judge 上轮意见" + chr(10)*2 + _jfb
                        fail_fb = "\n".join(f"judge-{i}: {r['feedback'][:500]}"
                                            for i, r in enumerate(judge_results) if not r["pass"])
                        feedback = f"# 评审意见（未通过）\n\n{fail_fb}\n\n请根据评审意见修正。"
                else:
                    raise StageError(f"Stage 1 分类检查未通过，已达最大轮数 {s_cfg.max_rounds}")

                # ═══════════════════════════════════════════════════
                # Stage 2: 子文件夹细分（parallel_modules 并行）
                # ═══════════════════════════════════════════════════
                s_cfg = cfg.stages.refine
                w_sys_prompt = self._load_prompt(w_prompt_dir, "step2_refine")
                j_sys_prompt = self._load_prompt(cfg.judges.system_prompt_dir, "step2_check_refine")
                reflect_prompt = self._load_prompt(w_prompt_dir, "reflect_refine")

                parallel_s2 = max(1, cfg.parallel_modules)
                s2_queue: asyncio.Queue[str] = asyncio.Queue()
                refined_modules: set[str] = set()
                in_progress_s2: set[str] = set()
                s2_errors: list[BaseException] = []

                for _m in _discover_modules(str(workspace)):
                    await s2_queue.put(_m)

                async def _refine_one(mod_name: str) -> None:
                    nonlocal tokens
                    mod_dir = _get_modules_root(str(workspace)) / mod_name
                    if not (mod_dir / "files.list").exists():
                        return
                    fc = sum(1 for l in (mod_dir / "files.list").read_text("utf-8").splitlines() if l.strip())
                    # 空模块：Stage 1 创建但过滤后无文件（如 chassis 只有 lua 脚本）
                    if fc == 0:
                        self._emit("log", task_id, level="warn",
                                   msg=f"[跳过] {mod_name} 过滤后 0 个文件，自动移除空模块")
                        import shutil as _sh
                        _sh.rmtree(str(mod_dir), ignore_errors=True)
                        return
                    refine_session = str(sess_dir / f"refine-{mod_name}.jsonl")
                    feedback = ""

                    # ── 拆分前保存快照到 workspace/.s2_snapshots/（不随模块目录删除）──
                    snapshots_dir = workspace / ".s2_snapshots"
                    snapshots_dir.mkdir(exist_ok=True)
                    snapshot_path = snapshots_dir / f"{mod_name}.snapshot"
                    if not snapshot_path.exists():  # 只在第一次保存，重试时不覆盖
                        import shutil as _sh2
                        _sh2.copy2(str(mod_dir / "files.list"), str(snapshot_path))

                    # 超过阈値时，先用子 Worker 收集文件摘要
                    sub_prompt = self._load_prompt(w_prompt_dir, "step2_sub_read")
                    file_summary = ""
                    if sub_prompt and fc > self.SUB_WORKER_THRESHOLD:
                        file_summary = await self._collect_file_summaries(
                            task_id, mod_name, mod_dir, w_base, tokens,
                            sub_prompt, parallel=cfg.parallel_sub_workers,
                            sub_model=cfg.workers.model_for("sub_read"),
                            target_dir=cfg.target_dir)

                    for attempt in range(self._max_iter(s_cfg)):
                        self._emit("stage", task_id, stage=2,
                                   module=mod_name, attempt=attempt + 1)

                        mods_before = set(_discover_modules(str(workspace)))
                        prompt_parts = [f"检查模块 `{mod_name}` 是否需要细分。"]
                        if file_summary:
                            prompt_parts.append(chr(10)*2 + "## 文件摘要（子 Worker 已分析）" + chr(10)*2 + file_summary)
                        if feedback:
                            prompt_parts.append(chr(10)*2 + feedback)
                        ar = await _run_agent_checked(
                            prompt=chr(10).join(prompt_parts),
                            model=_wm("refine"),
                            system_prompt=w_sys_prompt,
                            session_file=refine_session,
                            **w_base,
                        )
                        tokens += ar.token_usage

                        mods_after = set(_discover_modules(str(workspace)))
                        # 只计入当前模块产生的新子模块（排除已在 refined/in_progress 中的）
                        new_ones = sorted(
                            (mods_after - mods_before) - refined_modules - in_progress_s2
                        )
                        was_split = mod_name not in mods_after and bool(
                            mods_after - mods_before - refined_modules - in_progress_s2
                        )
                        self._emit("stage_result", task_id, stage=2,
                                   module=mod_name, split=was_split, new_modules=new_ones)

                        judge_results = []
                        for j_idx, j_cfg_item in enumerate(j_cfgs):
                            ar = await _run_agent_checked(
                                prompt=f"评审 Worker 对模块 `{mod_name}` 的细分判断。",
                                model=_jm("refine", j_cfg_item),
                                system_prompt=j_sys_prompt,
                                tools=cfg.judges.default_tools,
                                cwd=str(workspace),
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
                            if attempt + 1 >= s_cfg.min_rounds:
                                # 完成：将拆分出的新模块加入队列
                                if was_split and new_ones:
                                    for nm in new_ones:
                                        if nm not in refined_modules and nm not in in_progress_s2:
                                            in_progress_s2.add(nm)
                                            await s2_queue.put(nm)
                                refined_modules.add(mod_name)
                                return  # 正常完成
                            else:
                                # 还未到 min_rounds 轮 → 强制反思
                                self._emit("reflect", task_id, stage=2,
                                           module=mod_name, round=attempt+1)
                                feedback = (
                                    "# 自查要求（第 " + str(attempt+1) +
                                    " 轮，需至少 " + str(s_cfg.min_rounds) + " 轮）" +
                                    chr(10)*2 + reflect_prompt)
                                _jfb = chr(10).join(
                                    f"judge-{i}: {r['feedback'][:500]}"
                                    for i, r in enumerate(judge_results))
                                feedback += chr(10)*2 + "## Judge 上轮意见" + chr(10)*2 + _jfb
                        else:
                            fail_fb = chr(10).join(
                                f"judge-{i}: {r['feedback'][:500]}"
                                for i, r in enumerate(judge_results) if not r["pass"])
                            if "missing" in fail_fb.lower() or "丢失" in fail_fb or "遗漏" in fail_fb:
                                guidance = (
                                    chr(10)*2 + "⚠️ **文件丢失！** 请修复文件覆盖问题，不要改变拆分策略。" +
                                    chr(10) + "运行 check_classification.sh 查看遗漏文件，将它们归入合适的模块。")
                            else:
                                guidance = chr(10)*2 + "请根据评审意见调整拆分策略。"
                            feedback = "# 评审意见（未通过）" + chr(10)*2 + fail_fb + guidance

                    # for 循环耗尽（未 return）→ 超出最大轮数
                    raise StageError(
                        f"Stage 2 模块 {mod_name} 细分未通过，已达最大轮数 {s_cfg.max_rounds}")

                async def _s2_worker() -> None:
                    while True:
                        mod_name = await s2_queue.get()
                        try:
                            if mod_name not in refined_modules:
                                await _refine_one(mod_name)
                        except (StageError, PiFatalError) as e:
                            s2_errors.append(e)
                        finally:
                            s2_queue.task_done()

                _s2_workers = [asyncio.create_task(_s2_worker())
                               for _ in range(parallel_s2)]
                await s2_queue.join()
                for _w in _s2_workers:
                    _w.cancel()
                await asyncio.gather(*_s2_workers, return_exceptions=True)
                if s2_errors:
                    raise s2_errors[0]

                # ═══════════════════════════════════════════════════
                # Stage 2 后：全局完整性检查 + 遗漏文件补分类（W+J）
                # ═══════════════════════════════════════════════════
                filtered_txt = workspace / "filtered_files.txt"
                if filtered_txt.exists():
                    all_target = set(
                        l.strip() for l in filtered_txt.read_text("utf-8").splitlines() if l.strip()
                    )
                    mods_root = _get_modules_root(str(workspace))
                    all_classified = set()
                    for flist in mods_root.glob("*/files.list"):
                        if flist.name == "files.list.snapshot":
                            continue
                        for l in flist.read_text("utf-8").splitlines():
                            l = l.strip()
                            if l:
                                all_classified.add(l)
                    missing_files = sorted(all_target - all_classified)

                    if missing_files:
                        self._emit("log", task_id, level="warn",
                                   msg=f"Stage2 全局检查: {len(missing_files)} 个文件未归类，启动补分类")

                        # 构建现有模块摘要
                        mod_summary_lines = ["## 已有模块（名称 | 示例文件）"]
                        for flist in sorted(mods_root.glob("*/files.list")):
                            mod_name_s2 = flist.parent.name
                            sample = next(
                                (l.strip() for l in flist.read_text("utf-8").splitlines() if l.strip()),
                                "(空)"
                            )
                            mod_summary_lines.append(f"- {mod_name_s2} | {Path(sample).name}")
                        mod_summary = chr(10).join(mod_summary_lines)

                        reclass_prompt_tmpl = self._load_prompt(w_prompt_dir, "step2_reclassify")
                        _nl = chr(10)
                        reclass_prompt = (
                            f"## 待归类文件（{len(missing_files)} 个）{_nl}{_nl}"
                            + _nl.join(missing_files)
                            + f"{_nl}{_nl}{mod_summary}"
                        )

                        s2rc_cfg = cfg.stages.refine
                        for rc_attempt in range(min(3, self._max_iter(s2rc_cfg))):
                            rc_ar = await _run_agent_checked(
                                prompt=reclass_prompt,
                                model=_wm("classify"),
                                tools=w_base["tools"],
                                system_prompt=reclass_prompt_tmpl,
                                cwd=str(workspace),
                                thinking_level=w_base.get("thinking_level", "off"),
                                session_file=None,
                                cancel_event=w_base.get("cancel_event"),
                                max_retries=w_base.get("max_retries", 3),
                                retry_delay=w_base.get("retry_delay", 10),
                                pi_max_retries=w_base.get("pi_max_retries", -1),
                                pi_retry_delay=w_base.get("pi_retry_delay", 10),
                            )
                            tokens += rc_ar.token_usage

                            # 重新统计
                            all_classified2 = set()
                            for flist in mods_root.glob("*/files.list"):
                                for l in flist.read_text("utf-8").splitlines():
                                    l = l.strip()
                                    if l:
                                        all_classified2.add(l)
                            still_missing = sorted(all_target - all_classified2)
                            self._emit("log", task_id, level="info",
                                       msg=f"补分类第{rc_attempt+1}轮: 剩余 {len(still_missing)} 个未归类")
                            if not still_missing:
                                break
                            missing_files = still_missing
                            reclass_prompt = (
                                f"## 仍未归类文件（{len(missing_files)} 个）{_nl}{_nl}"
                                + _nl.join(missing_files)
                                + f"{_nl}{_nl}{mod_summary}"
                            )
                    else:
                        self._emit("log", task_id, level="info",
                                   msg=f"Stage2 全局检查: 全部 {len(all_target)} 个文件已归类 ✅")

                # ═══════════════════════════════════════════════════

            # ── resume 时从已有 workspace 加载模块列表 ──
            if _skip_s12:
                _mods_root = _get_modules_root(str(workspace))
                final_modules = [
                    d.name for d in _mods_root.iterdir()
                    if d.is_dir() and (d / "files.list").exists()
                ]
                self._emit("log", task_id, level="info",
                           msg=f"resume: 从 workspace 加载 {len(final_modules)} 个模块，跳过 Stage 0-2")

            # Stage 3: 子文件夹分析（parallel_modules 并行）
            # ═══════════════════════════════════════════════════
            s_cfg = cfg.stages.analyse
            w_sys_prompt = self._load_prompt(w_prompt_dir, "step3_analyse")
            j_sys_prompt = self._load_prompt(cfg.judges.system_prompt_dir, "step3_check_analyse")
            reflect_prompt = self._load_prompt(w_prompt_dir, "reflect_analyse")

            final_modules = _discover_modules(str(workspace))
            modules_needing_reclassify: list[str] = []
            s3_errors: list[BaseException] = []

            s3_sem = asyncio.Semaphore(max(1, cfg.parallel_modules))

            async def _analyse_one(mod_name: str) -> None:
                nonlocal tokens
                async with s3_sem:
                    mod_dir = _get_modules_root(str(workspace)) / mod_name
                    analyse_session = str(sess_dir / f"analyse-{mod_name}.jsonl")
                    feedback = ""

                    # 预读所有文件（Python侧，无需 LLM tool call）
                    loop = asyncio.get_event_loop()
                    pre_read_content = await loop.run_in_executor(
                        None, self._pre_read_module, cfg.target_dir, mod_dir
                    )
                    # 解析前缀标记：是否含非-ELF 文本文件
                    has_text = pre_read_content.startswith('__HAS_TEXT__' + chr(10))
                    if has_text:
                        pre_read_content = pre_read_content[len('__HAS_TEXT__' + chr(10)):]
                    # 将模板占位符替换
                    w_sys = w_sys_prompt.replace(
                        "{{PRE_READ_CONTENT}}", pre_read_content
                    ).replace(
                        "{{MODULE_NAME}}", mod_name
                    )
                    # 纯 ELF 模块: 只需写报告
                    # 含文本文件: 需要 read 工具读取其他内容
                    w_tools_s3 = ["read", "write"] if has_text else ["write"]

                    for attempt in range(self._max_iter(s_cfg)):
                        self._emit("stage", task_id, stage=3,
                                   module=mod_name, attempt=attempt + 1)

                        _nl = chr(10)
                        prompt_parts = [
                            f"现在将模块 `{mod_name}` 的分析报告写入 `modules/{mod_name}/module_report.md`。",
                            f"文件内容已在 system prompt 中提供，直接写报告即可。",
                        ]
                        if feedback:
                            prompt_parts.append(_nl*2 + feedback)
                        ar = await _run_agent_checked(
                            prompt=_nl.join(prompt_parts),
                            model=_wm("analyse"),
                            system_prompt=w_sys,
                            tools=w_tools_s3,
                            session_file=analyse_session,
                            cwd=str(workspace),
                            max_retries=w_base.get("max_retries", 1),
                            retry_delay=w_base.get("retry_delay", 0),
                        )
                        tokens += ar.token_usage
                        self._emit("stage_result", task_id, stage=3, module=mod_name)

                        judge_results = []
                        for j_idx, j_cfg_item in enumerate(j_cfgs):
                            ar = await _run_agent_checked(
                                prompt=f"评审模块 `{mod_name}` 的分析报告。",
                                model=_jm("analyse", j_cfg_item),
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

                        # 重分类检测
                        has_reclassify = any("[需要重新分类]" in r.get("feedback", "")
                                            for r in judge_results)
                        if has_reclassify:
                            reclass_votes = sum(1 for r in judge_results
                                               if "[需要重新分类]" in r.get("feedback", ""))
                            if _check_voting(
                                [{"pass": True}] * reclass_votes +
                                [{"pass": False}] * (j_count - reclass_votes),
                                s_cfg.pass_mode, j_count,
                            ):
                                self._emit("reclassify", task_id, module=mod_name)
                                modules_needing_reclassify.append(mod_name)
                                return  # 跳出，后面统一重分类

                        voted_pass = _check_voting(judge_results, s_cfg.pass_mode, j_count)
                        if voted_pass:
                            if attempt + 1 >= s_cfg.min_rounds:
                                return  # 正常完成
                            else:
                                self._emit("reflect", task_id, stage=3,
                                           module=mod_name, round=attempt+1)
                                feedback = (
                                    "# 自查要求（第 " + str(attempt+1) +
                                    " 轮，需至少 " + str(s_cfg.min_rounds) + " 轮）" +
                                    chr(10)*2 + reflect_prompt)
                                _jfb = chr(10).join(
                                    f"judge-{i}: {r['feedback'][:500]}"
                                    for i, r in enumerate(judge_results))
                                feedback += chr(10)*2 + "## Judge 上轮意见" + chr(10)*2 + _jfb
                        else:
                            fail_fb = chr(10).join(
                                f"judge-{i}: {r['feedback'][:500]}"
                                for i, r in enumerate(judge_results) if not r["pass"])
                            feedback = "# 评审意见（未通过）" + chr(10)*2 + fail_fb + chr(10)*2 + "请根据意见修正分析。"

                    # for 循环耗尽 → 超出最大轮数
                    if mod_name not in modules_needing_reclassify:
                        raise StageError(
                            f"Stage 3 模块 {mod_name} 分析未通过，已达最大轮数 {s_cfg.max_rounds}")

            _s3_results = await asyncio.gather(
                *[_analyse_one(m) for m in final_modules],
                return_exceptions=True,
            )
            for _r in _s3_results:
                if isinstance(_r, PiFatalError):
                    raise _r
            for _r in _s3_results:
                if isinstance(_r, StageError):
                    raise _r
                if isinstance(_r, Exception) and not isinstance(_r, asyncio.CancelledError):
                    raise _r

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

                    for attempt in range(self._max_iter(s_cfg_redo)):
                        self._emit("stage", task_id, stage="2-redo",
                                   module=mod_name, attempt=attempt + 1)

                        ar = await _run_agent_checked(
                            model=_wm("refine"),
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
                                model=_jm("refine", j_cfg_item),
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
                            judge_results, s_cfg_redo.pass_mode, j_count)
                        if voted_pass:
                            if attempt + 1 >= s_cfg_redo.min_rounds:
                                break
                            feedback = "# 自查要求" + chr(10)*2 + reflect_refine
                            _jfb = chr(10).join(
                                f"judge-{i}: {r['feedback'][:500]}" for i, r in enumerate(judge_results))
                            feedback += chr(10)*2 + "## Judge 上轮意见" + chr(10)*2 + _jfb
                            fail_fb = "\n".join(f"judge-{i}: {r['feedback'][:500]}"
                                                for i, r in enumerate(judge_results) if not r["pass"])
                            feedback = f"# 评审意见\n\n{fail_fb}"
                    else:
                        raise StageError(
                            f"Stage 2-redo 模块 {mod_name} 重分类未通过")

                # Stage 3-redo: 只处理两类模块：
                #   1. 重分类产生的新子模块（在 new_mods 但不在 final_modules）
                #   2. 原始需重分类模块中 files.list 非空的（避免空壳）
                new_mods = _discover_modules(str(workspace))
                mods_root = _get_modules_root(str(workspace))
                redo_analyse = []
                for _m in new_mods:
                    if _m not in final_modules:
                        # 新产生的子模块
                        redo_analyse.append(_m)
                    elif _m in modules_needing_reclassify:
                        # 原始模块：files.list 非空才重分析（排除空壳）
                        _flist = mods_root / _m / "files.list"
                        if _flist.exists() and _flist.stat().st_size > 0:
                            redo_analyse.append(_m)
                if redo_analyse:
                    self._emit("stage", task_id, stage="3-redo", modules=redo_analyse)
                    s_cfg_a = cfg.stages.analyse
                    w_sys_analyse = self._load_prompt(w_prompt_dir, "step3_analyse")
                    j_sys_analyse = self._load_prompt(cfg.judges.system_prompt_dir, "step3_check_analyse")
                    reflect_analyse = self._load_prompt(w_prompt_dir, "reflect_analyse")

                    for mod_name in redo_analyse:
                        mod_dir = _get_modules_root(str(workspace)) / mod_name
                        analyse_session = str(sess_dir / f"analyse-redo-{mod_name}.jsonl")
                        # 预读文件内容
                        pre_content = await asyncio.get_event_loop().run_in_executor(
                            None, self._pre_read_module, cfg.target_dir, mod_dir
                        )
                        # 解析前缀标记
                        _has_text_redo = pre_content.startswith('__HAS_TEXT__' + chr(10))
                        if _has_text_redo:
                            pre_content = pre_content[len('__HAS_TEXT__' + chr(10)):]
                        w_sys_redo = w_sys_analyse.replace(
                            "{{PRE_READ_CONTENT}}", pre_content
                        ).replace(
                            "{{MODULE_NAME}}", mod_name
                        )
                        feedback = ""
                        for attempt in range(self._max_iter(s_cfg_a)):
                            self._emit("stage", task_id, stage="3-redo",
                                       module=mod_name, attempt=attempt + 1)
                            prompt_parts = [
                                f"将模块 `{mod_name}` 的分析报告写入 `modules/{mod_name}/module_report.md`。",
                                f"文件内容已在 system prompt 中提供。",
                            ]
                            if feedback:
                                prompt_parts.append(f"\n\n{feedback}")
                            ar = await _run_agent_checked(
                                model=_wm("analyse"),
                                prompt="\n".join(prompt_parts),
                                system_prompt=w_sys_redo,
                                tools=["read", "write"] if _has_text_redo else ["write"],
                                session_file=analyse_session,
                                cwd=str(workspace),
                                max_retries=w_base.get("max_retries", 1),
                                retry_delay=w_base.get("retry_delay", 0),
                            )
                            tokens += ar.token_usage

                            judge_results = []
                            for j_idx, j_cfg_item in enumerate(j_cfgs):
                                ar = await _run_agent_checked(
                                    prompt=f"评审模块 `{mod_name}` 的分析报告。",
                                    model=_jm("analyse", j_cfg_item),
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
                                judge_results, s_cfg_a.pass_mode, j_count)
                            if voted_pass:
                                if attempt + 1 >= s_cfg_a.min_rounds:
                                    break
                                feedback = "# 自查要求" + chr(10)*2 + reflect_analyse
                                _jfb = chr(10).join(
                                    f"judge-{i}: {r['feedback'][:500]}" for i, r in enumerate(judge_results))
                                feedback += chr(10)*2 + "## Judge 上轮意见" + chr(10)*2 + _jfb
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
                    model=_jm("completeness", j_cfg_item),
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
                        model=_wm("refine"),
                        prompt=f"检查模块 `{mod_name}` 是否需要细分。",
                        system_prompt=w_sys_refine,
                        session_file=refine_session,
                        **w_base,
                    )
                    tokens += ar.token_usage

                    # Stage 3 补做（预读内容）
                    analyse_session = str(sess_dir / f"analyse-s4-{mod_name}.jsonl")
                    pre_content_s4 = await asyncio.get_event_loop().run_in_executor(
                        None, self._pre_read_module, cfg.target_dir, mod_dir
                    )
                    w_sys_s4 = w_sys_analyse.replace(
                        "{{PRE_READ_CONTENT}}", pre_content_s4
                    ).replace(
                        "{{MODULE_NAME}}", mod_name
                    )
                    feedback = ""
                    for attempt in range(self._max_iter(s_cfg_a)):
                        prompt_parts = [
                            f"将模块 `{mod_name}` 的分析报告写入 `modules/{mod_name}/module_report.md`。",
                            f"文件内容已在 system prompt 中提供。",
                        ]
                        if feedback:
                            prompt_parts.append(f"\n\n{feedback}")
                        ar = await _run_agent_checked(
                            model=_wm("analyse"),
                            prompt="\n".join(prompt_parts),
                            system_prompt=w_sys_s4,
                            tools=["write"],
                            session_file=analyse_session,
                            cwd=str(workspace),
                            max_retries=w_base.get("max_retries", 1),
                            retry_delay=w_base.get("retry_delay", 0),
                        )
                        tokens += ar.token_usage

                        judge_results = []
                        for j_idx, j_cfg_item in enumerate(j_cfgs):
                            ar = await _run_agent_checked(
                                prompt=f"评审模块 `{mod_name}` 的分析报告。",
                                model=_jm("analyse", j_cfg_item),
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

            for attempt in range(self._max_iter(s_cfg)):
                self._emit("stage", task_id, stage="4b", attempt=attempt + 1)

                # Worker 生成报告
                prompt_parts = [
                    "读取所有模块的 module_report.md，生成最终分析总报告 final_report.md。"]
                if feedback:
                    prompt_parts.append(f"\n\n{feedback}")
                ar = await _run_agent_checked(
                    model=_wm("report"),
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
                        model=_jm("report", j_cfg_item),
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
                    if attempt + 1 >= s_cfg.min_rounds:
                        break
                    else:
                        self._emit("reflect", task_id, stage="4b",
                            round=attempt+1, min_rounds=s_cfg.min_rounds)
                        feedback = ("# 自查要求（第 " + str(attempt+1) +
                            " 轮，需至少 " + str(s_cfg.min_rounds) + " 轮）" +
                            chr(10)*2 + reflect_report)
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

    # ── 预读单个文件（同步，在线程池中执行）────────────────────────────────
    @staticmethod
    def _pre_read_file(fullpath: str) -> tuple[str, list[str]]:
        """返回 (file_type, top_strings)。ELF只读前128KB，文本读全文（限4MB）。"""
        ELF_MAGIC = b"\x7fELF"
        MIN_STR = 5
        MAX_ELF = 131072
        MAX_TEXT = 4 * 1024 * 1024

        def _strings(data: bytes) -> list[str]:
            out, cur = [], []
            for b in data:
                c = chr(b)
                if c.isprintable() and c not in ('\n', '\r'):
                    cur.append(c)
                else:
                    if len(cur) >= MIN_STR:
                        out.append(''.join(cur))
                    cur = []
            if len(cur) >= MIN_STR:
                out.append(''.join(cur))
            return out

        try:
            with open(fullpath, 'rb') as f:
                magic = f.read(4)
                if magic == ELF_MAGIC:
                    f.seek(0)
                    data = f.read(MAX_ELF)
                    strs = _strings(data)
                    # 过滤纯路径/版本号噪声，保留有意义的符号
                    filtered = [s for s in strs
                                if len(s) >= 5
                                and not s.startswith('/')
                                and not s.startswith('.')
                                and ' ' not in s[:3]]  # 排除编译器路径等
                    return ('ELF', filtered[:200])  # 200条 strings 够分析用
                else:
                    f.seek(0)
                    raw = f.read(MAX_TEXT)
                    try:
                        text = raw.decode('utf-8', errors='ignore')
                    except Exception:
                        return ('binary', [])
                    lines = [l.strip() for l in text.splitlines() if l.strip()][:120]
                    return ('text', lines)
        except (OSError, IOError):
            return ('unknown', [])

    @staticmethod
    def _read_one_elf(fullpath: str) -> dict:
        """ELF 三层提取：nm 导出/导入符号 + readelf 依赖库 + strings 头部。"""
        import subprocess as sp, re as _re
        res = {"exports": [], "imports": [], "needed": [], "strings_head": []}
        try:
            r = sp.run(["nm", "-D", fullpath], capture_output=True, text=True, timeout=15)
            for line in r.stdout.splitlines():
                p = line.split()
                if len(p) >= 3:
                    st, sn = p[-2], p[-1]
                    if st in ('T', 't'): res["exports"].append(sn)
                    elif st == 'U':      res["imports"].append(sn)
                elif len(p) == 2 and p[0] == 'U':
                    res["imports"].append(p[1])
            res["exports"] = res["exports"][:300]
            res["imports"] = res["imports"][:150]
            r = sp.run(["readelf", "-d", fullpath], capture_output=True, text=True, timeout=15)
            for line in r.stdout.splitlines():
                if "NEEDED" in line:
                    m = _re.search(r'\[([^\]]+)\]', line)
                    if m: res["needed"].append(m.group(1))
            r = sp.run(["strings", "-n", "6", fullpath], capture_output=True, text=True, timeout=15)
            res["strings_head"] = r.stdout.splitlines()[:50]
        except Exception:
            pass
        return res

    @staticmethod
    def _pre_read_module(target_dir: str, mod_dir: "Path") -> str:
        """预读模块所有文件，注入结构化内容到 prompt。

        ELF: nm 导出符号(攻击面) + 导入符号(危险函数) + readelf 依赖库 + strings头部。
        实测: 29文件模块约 25K tokens，GLM-5 上限 202K，安全，无需文件数上限。
        Worker 可以设置 tools=["write"]，无需再用 nm/readelf/strings。
        """
        import concurrent.futures
        _cls = Orchestrator
        try:
            flist = (mod_dir / "files.list").read_text("utf-8").strip().splitlines()
        except OSError:
            return "(files.list 不可读)"
        files = [l.strip() for l in flist if l.strip()]
        if not files:
            return "(模块文件列表为空)"

        def _read_one(relpath: str):
            fp = str(Path(target_dir) / relpath)
            try:
                with open(fp, 'rb') as f:
                    magic = f.read(4)
            except OSError:
                return relpath, 'missing', {}
            if magic == b'ELF':
                return relpath, 'ELF', _cls._read_one_elf(fp)
            else:
                try:
                    with open(fp, encoding='utf-8', errors='replace') as f:
                        content_full = f.read()
                    return relpath, 'text', {"content": content_full}
                except Exception:
                    return relpath, 'binary', {}

        # 并行读取全部文件（无数量上限）
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            futs = [(rp, pool.submit(_read_one, rp)) for rp in files]

        # 非-ELF 文本内容共享总字符上限，防止大模块 OOM
        # ELF 符号表已在 _read_one_elf 内截断，不占此预算
        TEXT_TOTAL_CHAR_LIMIT = 150_000   # ~150KB 总上限
        TEXT_FILE_CHAR_LIMIT  = 8_000     # 单文件最多展示 8KB
        text_chars_used = 0
        has_text_files = False
        truncated_files: list[str] = []

        parts = []
        for rp, fut in futs:
            try:
                _, ftype, data = fut.result(timeout=20)
            except Exception:
                ftype, data = 'unknown', {}
            parts.append(f"### {rp}")
            if ftype == 'ELF':
                exports = data.get('exports', [])
                imports = data.get('imports', [])
                needed  = data.get('needed', [])
                sh      = data.get('strings_head', [])
                parts.append("类型: ELF 共享库 (AArch64)")
                if needed:
                    parts.append(f"依赖库: {', '.join(needed)}")
                if exports:
                    parts.append(f"导出函数 ({len(exports)}个, 对外攻击面):")
                    parts.append("```"); parts.extend(exports); parts.append("```")
                if imports:
                    parts.append(f"外部调用 ({len(imports)}个, 含潜在危险函数):")
                    parts.append("```"); parts.extend(imports); parts.append("```")
                if sh:
                    parts.append(f"strings头部 ({len(sh)}行):")
                    parts.append("```"); parts.extend(sh); parts.append("```")
            elif ftype == 'text':
                has_text_files = True
                full = data.get('content', '')
                if text_chars_used >= TEXT_TOTAL_CHAR_LIMIT:
                    # 总预算耗尽：只列路径，提示可用 read 工具
                    truncated_files.append(rp)
                    parts.append(chr(10).join([
                        "类型: 文本文件",
                        "〔内容已略去（总预算已满），可用 read 工具获取完整内容〕",
                    ]))
                else:
                    remaining = TEXT_TOTAL_CHAR_LIMIT - text_chars_used
                    take = min(len(full), TEXT_FILE_CHAR_LIMIT, remaining)
                    snippet = full[:take]
                    total_lines = full.count(chr(10)) + 1
                    shown_lines = snippet.count(chr(10)) + 1
                    text_chars_used += take
                    is_cut = take < len(full)
                    cut_note = (f"  (前{shown_lines}行/{total_lines}行，已截断"
                                f"，余下内容可用 read 工具获取)") if is_cut else f"  ({total_lines}行)"
                    parts.append(f"类型: 文本文件{cut_note}:")
                    parts.append("```"); parts.extend(snippet.splitlines()); parts.append("```")
            elif ftype == 'missing':
                parts.append("(文件不存在 target_dir)")
            else:
                parts.append(f"类型: {ftype}")

        if truncated_files:
            parts.append("")
            parts.append(f"⚠️ 以下 {len(truncated_files)} 个文件因总内容超限未展示，"
                         f"可用 read 工具直接读取：")
            for tf in truncated_files:
                parts.append(f"  - /data/target/{tf}")

        result_str = chr(10).join(parts)
        # 前缀标记是否含非-ELF 文件，供调用方决定 worker tools
        prefix = '__HAS_TEXT__' + chr(10) if has_text_files else ''
        return prefix + result_str

    async def _collect_file_summaries(
        self, task_id: str, mod_name: str, mod_dir: Path,
        w_base: dict, tokens: "TokenUsage",
        sub_prompt_template: str,
        parallel: int = 1,
        sub_model: str = "",
        target_dir: str = "/data/target",
    ) -> str:
        """Python预读文件内容注入prompt，子Worker只做分析不调工具。
        parallel 控制并发批次数。
        """
        flist_path = mod_dir / "files.list"
        files = [l.strip() for l in flist_path.read_text("utf-8").splitlines() if l.strip()]

        batches: list[list[str]] = []
        for i in range(0, len(files), self.SUB_BATCH_SIZE):
            batches.append(files[i:i + self.SUB_BATCH_SIZE])

        self._emit("stage", task_id, stage="2-sub",
                   module=mod_name, batches=len(batches), files=len(files),
                   parallel=parallel)

        semaphore = asyncio.Semaphore(max(1, parallel))
        results: list[str | None] = [None] * len(batches)

        async def _run_batch(idx: int, batch: list[str]) -> None:
            nonlocal tokens
            async with semaphore:
                self._emit("stage", task_id, stage="2-sub",
                           module=mod_name, batch=idx + 1, total=len(batches))

                # ── Python 预读每个文件内容 ──────────────────────────────
                loop = asyncio.get_event_loop()
                pre_reads: list[tuple[str, list[str]]] = []
                for relpath in batch:
                    fullpath = os.path.join(target_dir, relpath)
                    ftype, lines = await loop.run_in_executor(
                        None, self._pre_read_file, fullpath)
                    pre_reads.append((ftype, lines))

                # ── 构建带内容的 prompt（子Worker无需tool调用）──────────
                parts = [f"以下是 {len(batch)} 个文件的内容摘要，直接分析，无需再读文件：\n"]
                for relpath, (ftype, lines) in zip(batch, pre_reads):
                    fname = os.path.basename(relpath)
                    parts.append(f"\n=== {fname} ({ftype}) ===")
                    parts.append(f"路径: {relpath}")
                    if lines:
                        content_preview = '\n'.join(lines[:40])
                        parts.append(f"内容:\n{content_preview}")
                    else:
                        parts.append("内容: (空文件或无法读取)")
                prompt = '\n'.join(parts)

                ar = await _run_agent_checked(
                    prompt=prompt,
                    model=sub_model or w_base.get("model", ""),
                    tools=[],   # 内容已预读，无需工具
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
                    raw = re.sub(r'<result>.*?</result>', '', ar.output, flags=re.DOTALL).strip()
                    results[idx] = raw
                else:
                    results[idx] = chr(10).join(
                        f"{f} | unknown | (分析失败) | -" for f in batch)

        await asyncio.gather(*[_run_batch(i, b) for i, b in enumerate(batches)])

        all_lines = []
        for r in results:
            if r:
                for line in r.splitlines():
                    line = line.strip()
                    if line and '|' in line:
                        all_lines.append(line)

        header = (f"文件清单（共 {len(all_lines)} 个文件）\n"
                  f"格式: 路径 | 类型 | 功能摘要 | 核心技术标识 | 建议子模块")
        merged = header + chr(10) + chr(10).join(all_lines)
        self._emit("stage_result", task_id, stage="2-sub",
                   module=mod_name, file_count=len(all_lines))
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
