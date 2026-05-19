"""
system_analyse — Agent 子进程执行器

两种执行模式：
  1. Worker（保持上下文）：使用 --session <file> 保持会话历史
  2. Judge（重置上下文）：使用 --no-session 每轮全新

重试机制（双层）：
  外层 — pi 进程级重试（pi_max_retries）：
    进程拉起失败、崩溃、信号杀死 → 重新拉起
    致命错误（Model not found, Unauthorized）→ 不重试，立即终止
  内层 — API 级重试（max_retries）：
    连接超时、限流、服务器错误 → 重新调用
  两层独立计数、独立退避，-1 表示无限重试
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import Callable, Optional

from .models import TokenUsage

logger = logging.getLogger("sa.runner")

_MAX_BACKOFF = 30  # 退避上限 30s
_BACKOFF_SCHEDULE = (3.0, 5.0, 10.0, 15.0, 30.0)  # 固定退避序列
_QUERY_ENGINE_401_MAX_RETRIES = 10

# ── per-pi-process 卡死检测默认值 ─────────────────────────────────────────────
# 全局配置通过 task_config.model_stuck_timeout / model_stuck_max_activations 注入
# 环境变量可覆盖代码默认值（与 config.json 同级，优先级：config.json > 环境变量 > 代码默认）
_DEFAULT_PI_STUCK_TIMEOUT: float = max(
    30.0,
    float(os.environ.get("SECFLOW_SA_MODEL_STUCK_TIMEOUT", "1800")),
)
_DEFAULT_PI_STUCK_MAX_ACTIVATIONS: int = max(
    1,
    int(os.environ.get("SECFLOW_SA_MODEL_STUCK_MAX_ACTIVATIONS", "5")),
)
_DEFAULT_CONTEXT_WINDOW = 128_000
_SINGLE_INPUT_CONTEXT_RATIO = 0.75
_PROMPT_TOKEN_OVERHEAD = 128
_COMPACTION_TRIGGER_PROMPT = (
    "请立即触发一次当前会话的自动压缩（compaction），"
    "仅保留后续继续执行任务所需的关键结论、约束和待办。"
    "不要继续业务分析，只回复 COMPACTION_OK。"
)
_CONTEXT_WINDOW_BY_MODEL = {
    "gpt-5.4": 128_000,
    "gpt-5.4-mini": 128_000,
    "gpt-5.5": 256_000,
    "gpt-5.3-codex": 128_000,
    "gpt-5.2": 200_000,
    "minimax/minimax-m2.5": 163_804,
    "minimax-m2.5": 163_804,
    "minimax-m2.7": 128_000,
    "glm-5.1": 128_000,
    "zai-org/glm-5": 128_000,
}


# ─── 结果类 ───────────────────────────────────────────────────────────────────


class AgentResult:
    """单个 Agent 执行的结果。"""

    def __init__(self):
        self.output: str = ""
        self.messages: list[dict] = []
        self.token_usage = TokenUsage()
        self.exit_code: int = 0
        self.error: str | None = None
        self.fatal: bool = False  # 致命错误（配置/环境问题，不可重试）


# ─── 内部异常 ─────────────────────────────────────────────────────────────────


class _PiProcessError(Exception):
    """pi 进程级错误（非 API 错误），由内层向外层传递。"""

    pass


class PiFatalError(Exception):
    """pi 致命错误（不可重试），调用者应终止流水线。"""

    pass


# ─── 日志工具 ─────────────────────────────────────────────────────────────────


def _log_error(msg: str) -> None:
    logger.error(msg)


def _log_warn(msg: str) -> None:
    logger.warning(msg)


def _log_info(msg: str) -> None:
    logger.info(msg)


# ─── 工具函数 ─────────────────────────────────────────────────────────────────


def _backoff(base_delay: float, attempt: int) -> float:
    """固定退避序列：3s → 5s → 10s → 15s → 30s，之后保持 30s。

    base_delay 参数保留以兼容调用方签名，但不影响实际退避值。
    """
    idx = max(0, min(attempt - 1, len(_BACKOFF_SCHEDULE) - 1))
    return _BACKOFF_SCHEDULE[idx]


def _fmt_max(n: int) -> str:
    return "∞" if n < 0 else str(n)


def _should_retry(
    failures: int, max_retries: int, cancel: asyncio.Event | None
) -> bool:
    if cancel and cancel.is_set():
        return False
    if max_retries < 0:
        return True
    return failures <= max_retries


def _cmd_preview(args: list[str]) -> str:
    """命令预览（截断过长参数）。"""
    parts = []
    for a in args:
        parts.append(a[:80] + "…" if len(a) > 100 else a)
    return " ".join(parts)


def _estimate_tokens(text: str) -> int:
    if not text:
        return 0
    ascii_chars = sum(1 for ch in text if ord(ch) < 128)
    non_ascii_chars = len(text) - ascii_chars
    return max(1, (ascii_chars + 3) // 4 + non_ascii_chars)


def _model_context_window(model: str) -> int:
    normalized = str(model or "").strip().lower()
    for key, value in _CONTEXT_WINDOW_BY_MODEL.items():
        if key in normalized:
            return value
    return _DEFAULT_CONTEXT_WINDOW


def _single_input_token_estimate(system_prompt: str, prompt: str) -> int:
    return _estimate_tokens(system_prompt) + _estimate_tokens(prompt) + _PROMPT_TOKEN_OVERHEAD


def _single_input_token_limit(context_window: int) -> int:
    return max(1, int(context_window * _SINGLE_INPUT_CONTEXT_RATIO))


def _parse_context_overflow_details(error_text: str | None) -> dict[str, int]:
    text = str(error_text or "")
    lowered = text.lower()
    details = {
        "input_tokens": 0,
        "requested_output_tokens": 0,
        "context_length": 0,
        "max_input_tokens": 0,
    }
    if "context length" not in lowered and "input tokens" not in lowered:
        return details

    patterns = {
        "input_tokens": r"passed\s+(\d+)\s+input tokens",
        "requested_output_tokens": r"requested\s+(\d+)\s+output tokens",
        "context_length": r"context length is only\s+(\d+)\s+tokens",
        "max_input_tokens": r"maximum input length(?: of)?\s+(\d+)\s+tokens",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            details[key] = int(match.group(1))
    return details


def _is_context_overflow_error(error_text: str | None) -> bool:
    details = _parse_context_overflow_details(error_text)
    if details["context_length"] > 0:
        return True
    lowered = str(error_text or "").lower()
    return (
        "context length" in lowered
        and "input tokens" in lowered
        and ("badrequesterror" in lowered or "400" in lowered)
    )


def _format_context_overflow_failure(
    original_error: str | None,
    *,
    context_window: int,
    single_input_tokens: int,
    single_input_limit: int,
    compaction_attempted: bool,
) -> str:
    action = "已先触发一次会话自动压缩并重试" if compaction_attempted else "未能触发会话自动压缩"
    return (
        f"{action}，但当前单次输入估算约 {single_input_tokens} tokens，"
        f"超过上下文窗口 75% 阈值 {single_input_limit}/{context_window}，"
        f"本次请求不再继续重试。原始错误: {original_error or 'unknown'}"
    )

def _find_pi_command() -> list[str]:
    pi_bin = os.environ.get("PI_BIN")
    if pi_bin and os.path.isfile(pi_bin):
        return [pi_bin]
    pi_path = shutil.which("pi")
    if pi_path:
        return [pi_path]
    npx = shutil.which("npx")
    if npx:
        return [npx, "pi"]
    raise FileNotFoundError(
        "找不到 'pi'。请安装: npm install -g @mariozechner/pi-coding-agent"
    )


def _build_args(
    pi_cmd: list[str],
    model: str,
    tools: list[str],
    thinking_level: str,
    session_file: str | None,
) -> list[str]:
    """构造 pi RPC 模式启动参数（不含 system_prompt 和 prompt）。

    使用 --mode rpc：pi 保持运行，prompt 通过 stdin JSONL 发送，
    彻底绕过 Linux ARG_MAX 限制，支持任意大小的 prompt/system_prompt。
    """
    args = [*pi_cmd, "--mode", "rpc"]
    if session_file:
        args.extend(["--session", session_file])
    else:
        args.append("--no-session")
    if model:
        args.extend(["--model", model])
    if tools:
        args.extend(["--tools", ",".join(tools)])
    if thinking_level and thinking_level != "off":
        args.extend(["--thinking", thinking_level])
    return args


def _write_temp_markdown(
    tmp_dir: str | None,
    prefix: str,
    filename: str,
    content: str,
) -> tuple[str, str]:
    """将 prompt 写入临时 markdown 文件，返回 (tmp_dir, file_path)。"""
    if tmp_dir is None:
        tmp_dir = tempfile.mkdtemp(prefix=prefix)
    file_path = os.path.join(tmp_dir, filename)
    Path(file_path).write_text(content, encoding="utf-8")
    return tmp_dir, file_path


# ─── 错误分类 ─────────────────────────────────────────────────────────────────

# 致命错误：配置/环境问题，重试无意义
_FATAL_PATTERNS = [
    ("model", "not found"),
    ("not found", "use --list"),
    ("invalid", "model"),
    ("invalid", "api key"),
    ("invalid", "api_key"),
    ("unauthorized",),
    ("authentication", "failed"),
]

# API 可重试错误
_RETRYABLE_API_PATTERNS = [
    "connection",
    "timeout",
    "timed out",
    "ECONNREFUSED",
    "ECONNRESET",
    "ETIMEDOUT",
    "ENOTFOUND",
    "socket hang up",
    "fetch failed",
    "rate limit",
    "429",
    "503",
    "502",
    "500",
    "overloaded",
    "capacity",
    "temporarily unavailable",
    "server error",
    "internal error",
    "bad gateway",
    "service unavailable",
    "request failed",
    "network_error",        # gptplus5 并发超限
    "finish_reason",        # provider finish_reason: network_error
    "too many requests",    # 429 另一种表达
    "ENOBUFS",              # pipe buffer 满（大响应导致）
    "EPIPE",                # 管道断裂
]

_RETRYABLE_QUERY_ENGINE_401_PATTERNS = [
    ("401", "authentication error"),
    ("client is not connected to the query engine",),
    ("must call `connect()` before attempting to query data",),
]

# 速率限制模式：这些关键词匹配时延长待机时间
_RATE_LIMIT_PATTERNS = ["rate limit", "429", "too many requests", "network_error", "finish_reason"]
_RATE_LIMIT_EXTRA_DELAY = 60   # 限流时额外等彥60s

# pi 进程崩溃关键词
_PI_CRASH_PATTERNS = [
    "cannot find module",
    "module not found",
    "syntaxerror",
    "referenceerror",
    "typeerror",
    "segmentation fault",
    "segfault",
    "killed",
    "signal",
    "enoent",
    "eacces",
    "eperm",
    "heap out of memory",
    "allocation failed",
    "oom",
    "out of memory",
    "spawn",
    "execvp",
    "core dump",
    "bus error",
    "permission denied",
    "no such file",
]


def _is_fatal_error(result: AgentResult) -> bool:
    """致命错误：配置/环境问题，不可重试。"""
    error_text = (result.error or "").lower()
    for pattern in _FATAL_PATTERNS:
        if all(p in error_text for p in pattern):
            return True
    return False


def _is_retryable_api_error(result: AgentResult) -> bool:
    """API 级可重试错误。"""
    if result.exit_code == 0 and not result.error:
        return False
    error_text = (result.error or "").lower()
    for pattern in _RETRYABLE_API_PATTERNS:
        if pattern in error_text:
            return True
    return False


def _is_retryable_query_engine_401_error(result: AgentResult) -> bool:
    """query engine 会话态 401：按 API 超时机制重试，但有单独次数上限。"""
    if result.exit_code == 0 and not result.error:
        return False
    error_text = (result.error or "").lower()
    for pattern in _RETRYABLE_QUERY_ENGINE_401_PATTERNS:
        if all(p in error_text for p in pattern):
            return True
    return False


def _is_pi_crash(result: AgentResult) -> bool:
    """pi 进程级崩溃（非 API 错误）。"""
    if result.exit_code == 0:
        return False
    # 有正常消息输出 → pi 本身正常运行
    if result.messages:
        return False
    # API 错误交给内层处理
    if _is_retryable_api_error(result):
        return False
    # 无消息 + 非零退出 = 进程崩溃
    return True


async def _run_with_context_overflow_recovery(
    *,
    pi_cmd: list[str],
    args: list[str],
    prompt: str,
    system_prompt: str,
    model: str,
    tools: list[str],
    thinking_level: str,
    session_file: str | None,
    cwd: str,
    env: dict[str, str] | None,
    on_stream: Callable[[str], None] | None,
    cancel_event: asyncio.Event | None,
    max_retries: int,
    retry_delay: float,
    pi_max_retries: int,
    pi_retry_delay: float,
    model_stuck_timeout: float | None = None,
    model_stuck_max_activations: int | None = None,
) -> AgentResult:
    result = await _run_with_pi_retry(
        args=args,
        cwd=cwd,
        env=env,
        prompt=prompt,
        cancel_event=cancel_event,
        on_stream=on_stream,
        max_retries=max_retries,
        retry_delay=retry_delay,
        pi_max_retries=pi_max_retries,
        pi_retry_delay=pi_retry_delay,
        session_file=session_file,
        model_stuck_timeout=model_stuck_timeout,
        model_stuck_max_activations=model_stuck_max_activations,
    )
    if not _is_context_overflow_error(result.error):
        return result

    overflow = _parse_context_overflow_details(result.error)
    context_window = overflow["context_length"] or _model_context_window(model)
    single_input_tokens = _single_input_token_estimate(system_prompt, prompt)
    single_input_limit = _single_input_token_limit(context_window)
    compaction_attempted = False

    if session_file:
        compaction_attempted = True
        msg = (
            "检测到智能体单次请求触发上下文超限，先触发一次会话自动压缩，"
            "随后重试原请求。"
        )
        _log_warn(msg)
        if on_stream:
            on_stream(f"\n⚠️ {msg}\n")
        compaction_args = _build_args(pi_cmd, model, tools, thinking_level, session_file)
        await _run_with_pi_retry(
            args=compaction_args,
            cwd=cwd,
            env=env,
            prompt=_COMPACTION_TRIGGER_PROMPT,
            cancel_event=cancel_event,
            on_stream=None,
            max_retries=max_retries,
            retry_delay=retry_delay,
            pi_max_retries=pi_max_retries,
            pi_retry_delay=pi_retry_delay,
        )

    if single_input_tokens > single_input_limit:
        result.error = _format_context_overflow_failure(
            result.error,
            context_window=context_window,
            single_input_tokens=single_input_tokens,
            single_input_limit=single_input_limit,
            compaction_attempted=compaction_attempted,
        )
        return result

    if not session_file:
        return result

    retry_result = await _run_with_pi_retry(
        args=args,
        cwd=cwd,
        env=env,
        prompt=prompt,
        cancel_event=cancel_event,
        on_stream=on_stream,
        max_retries=max_retries,
        retry_delay=retry_delay,
        pi_max_retries=pi_max_retries,
        pi_retry_delay=pi_retry_delay,
    )
    return retry_result


# ═════════════════════════════════════════════════════════════════════════════
# 公开接口
# ═════════════════════════════════════════════════════════════════════════════


async def run_agent(
    prompt: str,
    *,
    model: str,
    tools: list[str],
    system_prompt: str = "",
    cwd: str = ".",
    env: dict[str, str] | None = None,
    thinking_level: str = "off",
    session_file: str | None = None,
    on_stream: Callable[[str], None] | None = None,
    cancel_event: asyncio.Event | None = None,
    max_retries: int = 3,
    retry_delay: float = 10.0,
    pi_max_retries: int = -1,
    pi_retry_delay: float = 10.0,
    model_stuck_timeout: float | None = None,
    model_stuck_max_activations: int | None = None,
) -> AgentResult:
    """
    运行单个 pi Agent 子进程（双层重试 + 致命错误检测 + per-pi-process stuck 监测）。

    外层：pi 进程级重试（拉起失败、崩溃、被 kill）
    内层：API 级重试（连接超时、限流、服务器错误）
    内层：per-pi stuck 监测（并行进程间独立计时）
    致命：Model not found / Unauthorized → 不重试，result.fatal=True
    """
    try:
        pi_cmd = _find_pi_command()
    except FileNotFoundError as e:
        _log_error(f"pi 可执行文件未找到: {e}")
        r = AgentResult()
        r.error = str(e)
        r.exit_code = -1
        r.fatal = True
        return r

    args = _build_args(pi_cmd, model, tools, thinking_level, session_file)

    # System/User Prompt → 临时文件，避免超长 argv 导致 Argument list too long
    tmp_dir: str | None = None
    sys_tmp_file: str | None = None
    prompt_tmp_file: str | None = None
    if system_prompt.strip():
        tmp_dir, sys_tmp_file = _write_temp_markdown(
            tmp_dir, "sa-", "system.md", system_prompt
        )
        args.extend(["--append-system-prompt", sys_tmp_file])

    try:
        return await _run_with_context_overflow_recovery(
            pi_cmd=pi_cmd,
            args=args,
            prompt=prompt,
            system_prompt=system_prompt,
            model=model,
            tools=tools,
            thinking_level=thinking_level,
            session_file=session_file,
            cwd=os.path.abspath(cwd),
            env=env,
            cancel_event=cancel_event,
            on_stream=on_stream,
            max_retries=max_retries,
            retry_delay=retry_delay,
            pi_max_retries=pi_max_retries,
            pi_retry_delay=pi_retry_delay,
            model_stuck_timeout=model_stuck_timeout,
            model_stuck_max_activations=model_stuck_max_activations,
        )
    finally:
        if sys_tmp_file and os.path.exists(sys_tmp_file):
            try:
                os.unlink(sys_tmp_file)
            except OSError:
                pass
        if prompt_tmp_file and os.path.exists(prompt_tmp_file):
            try:
                os.unlink(prompt_tmp_file)
            except OSError:
                pass
        if tmp_dir and os.path.exists(tmp_dir):
            try:
                os.rmdir(tmp_dir)
            except OSError:
                pass


# ─── 外层：pi 进程级重试 ─────────────────────────────────────────────────────


async def _run_with_pi_retry(
    *,
    args: list[str],
    cwd: str,
    env: dict[str, str] | None,
    prompt: str,
    cancel_event: asyncio.Event | None,
    on_stream: Callable[[str], None] | None,
    max_retries: int,
    retry_delay: float,
    pi_max_retries: int,
    pi_retry_delay: float,
    session_file: str | None = None,
    model_stuck_timeout: float | None = None,
    model_stuck_max_activations: int | None = None,
) -> AgentResult:
    """外层循环：处理 pi 进程拉起失败、崩溃、致命错误。"""
    # cwd 不存在是致命错误（目录被删除等），不进入重试
    if not os.path.isdir(cwd):
        _log_error(f"cwd 目录不存在（不可重试）: {cwd}")
        r = AgentResult()
        r.error = f"cwd directory does not exist: {cwd}"
        r.exit_code = -1
        r.fatal = True
        return r

    pi_attempt = 0
    # ── 连续快速崩溃检测（crash-loop 保护）──────────────────────────────────
    # 若进程在极短时间内（< _PI_CRASH_LOOP_WINDOW 秒）连续崩溃 _PI_CRASH_LOOP_MAX 次，
    # 判定为程序 bug（非网络抖动），直接终止任务，避免无限重试掩盖错误。
    _PI_CRASH_LOOP_MAX: int = int(os.environ.get("SECFLOW_SA_PI_CRASH_LOOP_MAX", "5"))
    _PI_CRASH_LOOP_WINDOW: float = float(os.environ.get("SECFLOW_SA_PI_CRASH_LOOP_WINDOW", "60"))
    _crash_times: list[float] = []

    while True:
        if cancel_event and cancel_event.is_set():
            r = AgentResult()
            r.error = "cancelled"
            return r

        try:
            result = await _run_with_api_retry(
                args=args,
                cwd=cwd,
                env=env,
                prompt=prompt,
                cancel_event=cancel_event,
                on_stream=on_stream,
                max_retries=max_retries,
                retry_delay=retry_delay,
                session_file=session_file,
                model_stuck_timeout=model_stuck_timeout,
                model_stuck_max_activations=model_stuck_max_activations,
            )

            # ── 致命错误检测（在 pi 进程重试前拦截）──
            if _is_fatal_error(result):
                result.fatal = True
                _log_error(f"pi 致命错误（不可重试）: {result.error}")
                return result

            # ── pi 进程崩溃 → 交由外层重试 ──
            if _is_pi_crash(result):
                raise _PiProcessError(
                    f"exit_code={result.exit_code}: "
                    f"{result.error or '(no error message)'}"
                )

            return result

        except (OSError, FileNotFoundError, PermissionError, _PiProcessError) as exc:
            pi_attempt += 1
            label = f"{pi_attempt}/{_fmt_max(pi_max_retries)}"

            if cancel_event and cancel_event.is_set():
                _log_error(f"pi 进程失败 (cancelled): {exc}")
                r = AgentResult()
                r.error = f"cancelled after pi error: {exc}"
                return r

            # ── 检查 stderr 中是否藏着致命错误 ──
            err_lower = str(exc).lower()
            for pattern in _FATAL_PATTERNS:
                if all(p in err_lower for p in pattern):
                    _log_error(f"pi 致命错误（不可重试）[{label}]: {exc}")
                    r = AgentResult()
                    r.error = str(exc)
                    r.exit_code = -1
                    r.fatal = True
                    return r

            if _should_retry(pi_attempt, pi_max_retries, cancel_event):
                delay = _backoff(pi_retry_delay, pi_attempt)

                # ── crash-loop 检测：记录本次崩溃时刻，清理过期窗口外的记录 ──
                _now = time.monotonic()
                _crash_times.append(_now)
                _crash_times[:] = [t for t in _crash_times if _now - t <= _PI_CRASH_LOOP_WINDOW]
                if len(_crash_times) >= _PI_CRASH_LOOP_MAX:
                    _msg = (
                        f"pi 进程在 {_PI_CRASH_LOOP_WINDOW:.0f}s 内连续崩溃 {len(_crash_times)} 次，"
                        f"判定为程序 bug（非网络抖动），终止任务。\n"
                        f"最近一次错误: {exc}"
                    )
                    _log_error(_msg)
                    r = AgentResult()
                    r.exit_code = -1
                    r.error = _msg
                    r.fatal = True  # 标记为 fatal 以便上层结束任务
                    return r

                _log_warn(
                    f"pi 进程失败 [{label}], {delay:.0f}s 后重试: {exc}\n"
                    f"    命令: {_cmd_preview(args)}"
                )
                if on_stream:
                    on_stream(
                        f"\n❌ pi 进程失败 (exit={getattr(exc, 'exit_code', '?')})，"
                        f"{delay:.0f}s 后重试 ({label})...\n"
                    )
                await asyncio.sleep(delay)
                continue
            else:
                _log_error(f"pi 进程重试耗尽 [{label}]: {exc}")
                r = AgentResult()
                r.exit_code = -1
                r.error = f"pi process failed after {pi_attempt} retries: {exc}"
                return r


# ─── 内层：API 级重试 ────────────────────────────────────────────────────────




def _session_has_assistant_content(session_file: "str | None") -> bool:
    """检查 session jsonl 中是否已有完整的 assistant 消息输出。

    用于判断 pi 重试时应发原始 prompt 还是短指令「继续」。
    """
    if not session_file:
        return False
    try:
        p = Path(session_file)
        if not p.exists() or p.stat().st_size < 50:
            return False
        with open(p, encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                msg = obj.get("message") or {}
                if isinstance(msg, dict) and msg.get("role") == "assistant":
                    content = msg.get("content") or []
                    if isinstance(content, list):
                        text = "".join(
                            c.get("text", "") for c in content if isinstance(c, dict)
                        )
                    else:
                        text = str(content)
                    if len(text.strip()) > 10:
                        return True
    except Exception:
        pass
    return False

async def _run_with_api_retry(
    *,
    args: list[str],
    cwd: str,
    env: dict[str, str] | None,
    prompt: str,
    cancel_event: asyncio.Event | None,
    on_stream: Callable[[str], None] | None,
    max_retries: int,
    retry_delay: float,
    session_file: str | None = None,
    model_stuck_timeout: float | None = None,
    model_stuck_max_activations: int | None = None,
) -> AgentResult:
    """内层循环：启动 pi 子进程，处理 API 级错误重试 + per-pi-process stuck 监测。

    stuck 监测：每个 pi 子进程有独立的 mtime 计时器，并行进程不干扰。
    激活阶段：kill pi + 新 pi + 「继续」或原 prompt。
    重启阶段：激活超次限，重启 pi 继承 session 发「继续」。
    """
    # ── stuck 参数 ──
    _stuck_timeout: float = (
        float(model_stuck_timeout) if model_stuck_timeout is not None
        else _DEFAULT_PI_STUCK_TIMEOUT
    )
    _stuck_max_act: int = (
        max(0, int(model_stuck_max_activations)) if model_stuck_max_activations is not None
        else _DEFAULT_PI_STUCK_MAX_ACTIVATIONS
    )
    activation_count: int = 0
    restart_count: int = 0

    api_attempt = 0
    query_engine_401_failures = 0
    effective_prompt = prompt

    while True:
        result = AgentResult()

        # ── 拉起子进程（OSError 由外层 catch）──
        proc = await asyncio.create_subprocess_exec(
            *args,
            cwd=cwd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE,  # RPC: 通过 stdin 发送 prompt
        )

        cancel_task = None
        if cancel_event:

            async def _cancel_monitor():
                await cancel_event.wait()
                try:
                    proc.terminate()
                except ProcessLookupError:
                    pass

            cancel_task = asyncio.create_task(_cancel_monitor())

        # ── RPC: 发送 prompt，读取事件直到 agent_end ──
        agent_ended = False
        try:
            assert proc.stdin is not None
            assert proc.stdout is not None

            # 发送初始 prompt（无 ARG_MAX 限制）
            prompt_cmd = json.dumps(
                {"type": "prompt", "message": effective_prompt},
                ensure_ascii=False,
            ) + chr(10)
            proc.stdin.write(prompt_cmd.encode("utf-8"))
            await proc.stdin.drain()

            # ── per-pi-process stuck 监测：记录 session mtime 基线 ──
            _pi_last_mtime: float = 0.0
            _pi_last_active: float = time.monotonic()
            if _stuck_timeout > 0 and session_file:
                try:
                    _pi_last_mtime = os.path.getmtime(session_file)
                except OSError:
                    _pi_last_mtime = 0.0
            _pi_stuck_triggered: bool = False

            buffer = b""
            while True:
                # 2s 轮询粒度读取 stdout，超时仅用于 stuck 检测，不代表 EOF
                _is_timeout = False
                try:
                    chunk = await asyncio.wait_for(proc.stdout.read(4096), timeout=2.0)
                except asyncio.TimeoutError:
                    chunk = b""
                    _is_timeout = True

                # ── mtime 变化 → 有 token 输出 → 重置计时器 ──
                if _stuck_timeout > 0 and session_file:
                    try:
                        _cur_mtime = os.path.getmtime(session_file)
                    except OSError:
                        _cur_mtime = _pi_last_mtime
                    if _cur_mtime != _pi_last_mtime:
                        _pi_last_mtime = _cur_mtime
                        _pi_last_active = time.monotonic()

                # ── stuck 检测 ──
                if _stuck_timeout > 0:
                    _idle = time.monotonic() - _pi_last_active
                    if _idle >= _stuck_timeout:
                        _pi_stuck_triggered = True
                        break

                # timeout 不是 EOF，继续等待；只有真正的 pipe EOF（chunk=b"" 且非 timeout）才退出
                if _is_timeout:
                    continue
                if not chunk:
                    break
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    ended = _process_line(
                        line.decode("utf-8", errors="replace"), result, on_stream
                    )
                    if ended:
                        agent_ended = True
                        break
                if agent_ended:
                    break

            # ── stuck 触发 → kill pi，决定激活还是重启 ──
            if _pi_stuck_triggered:
                _idle_secs = time.monotonic() - _pi_last_active
                try:
                    proc.terminate()
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                if activation_count < _stuck_max_act:
                    activation_count += 1
                    _action = f"激活(第{activation_count}/{_stuck_max_act}次)"
                else:
                    restart_count += 1
                    activation_count = 0
                    _action = f"重启(第{restart_count}次，已达激活上限)"
                _log_warn(
                    f"[stuck] pi 进程 {_idle_secs:.0f}s 无 token 输出，"
                    f"{_action}，继承 session 发送激活指令"
                )
                if on_stream:
                    on_stream(
                        f"\n⚠️ 后端模型 {_idle_secs:.0f}s 无响应，"
                        f"{_action}...\n"
                    )
                if session_file and _session_has_assistant_content(session_file):
                    effective_prompt = "继续"
                else:
                    effective_prompt = prompt
                api_attempt = 0
                query_engine_401_failures = 0
                continue
            if buffer.strip():
                _process_line(
                    buffer.decode("utf-8", errors="replace"), result, on_stream
                )

            # agent_ended 后必须继续 drain stdout 直到 EOF，
            # 否则 pi 继续写导致 pipe buffer 满 → ENOBUFS
            if agent_ended:
                try:
                    async def _drain_stdout():
                        assert proc.stdout is not None
                        while True:
                            chunk = await proc.stdout.read(65536)
                            if not chunk:
                                break
                    await asyncio.wait_for(_drain_stdout(), timeout=10.0)
                except (asyncio.TimeoutError, Exception):
                    pass

            # 关闭 stdin → pi 检测 EOF 后退出
            try:
                proc.stdin.close()
            except Exception:
                pass

            assert proc.stderr is not None
            try:
                stderr_data = await asyncio.wait_for(proc.stderr.read(), timeout=10.0)
                stderr_text = stderr_data.decode("utf-8", errors="replace").strip()
                if stderr_text and not result.error:
                    result.error = stderr_text
            except asyncio.TimeoutError:
                pass

            try:
                await asyncio.wait_for(proc.wait(), timeout=15.0)
                result.exit_code = proc.returncode or 0
            except asyncio.TimeoutError:
                _log_warn("pi 进程未在 15s 内退出，强制终止")
                proc.kill()
                await proc.wait()
                result.exit_code = -1

        except asyncio.CancelledError:
            try:
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
            raise
        except Exception as e:
            # 管道断裂、进程被杀等
            _log_warn(f"pi 进程读取异常: {e}")
            result.error = f"pi process read error: {e}"
            result.exit_code = -1
            try:
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

        finally:
            if cancel_task:
                cancel_task.cancel()
                try:
                    await cancel_task
                except asyncio.CancelledError:
                    pass

        # ── 提取输出 ──
        for msg in reversed(result.messages):
            if msg.get("role") == "assistant":
                texts = [
                    c["text"]
                    for c in (msg.get("content") or [])
                    if c.get("type") == "text"
                ]
                result.output = "\n".join(texts)
                break

        if cancel_event and cancel_event.is_set():
            return result

        # ── pi 崩溃 → 不在内层重试，交给外层 ──
        if _is_pi_crash(result):
            if result.error:
                _log_warn(
                    f"pi 进程崩溃 (exit={result.exit_code}): {result.error[:300]}"
                )
            return result

        # ── 致命错误 → 不重试，直接返回让外层处理 ──
        if _is_fatal_error(result):
            return result

        # ── Query engine 401：使用 API 超时同款退避，但单独限制连续 10 次 ──
        if _is_retryable_query_engine_401_error(result):
            query_engine_401_failures += 1
            if query_engine_401_failures <= _QUERY_ENGINE_401_MAX_RETRIES:
                delay = _backoff(retry_delay, query_engine_401_failures)
                label = f"{query_engine_401_failures}/{_QUERY_ENGINE_401_MAX_RETRIES}"
                _log_warn(
                    f"query engine 401 [{label}], {delay:.0f}s 后重试: "
                    f"{(result.error or '')[:200]}"
                )
                if on_stream:
                    on_stream(
                        f"\n⚠️ Query engine 连接失效，{delay:.0f}s 后重试 "
                        f"({label})...\n"
                    )
                if _session_has_assistant_content(session_file):
                    effective_prompt = "继续完成上次未完成的任务。"
                    _log_warn("session 已有内容，重试时发送「继续」而非重复完整 prompt")
                await asyncio.sleep(delay)
                continue
            _log_error(
                f"query engine 401 重试耗尽 "
                f"[{query_engine_401_failures}/{_QUERY_ENGINE_401_MAX_RETRIES}]: "
                f"{(result.error or '')[:200]}"
            )
            result.error = (
                (result.error or "")
                + f" [query engine 401 连续重试耗尽: {query_engine_401_failures} 次失败]"
            )
            return result
        query_engine_401_failures = 0

        # ── API 可重试错误 ──
        if _is_retryable_api_error(result):
            api_attempt += 1
            can_retry = (max_retries == -1) or (api_attempt <= max_retries)
            if can_retry:
                delay = _backoff(retry_delay, api_attempt)
                # 限流错误额外等待，避免连续冲击并发限制
                err_lower = (result.error or "").lower()
                is_rate_limit = any(p in err_lower for p in _RATE_LIMIT_PATTERNS)
                if is_rate_limit:
                    delay = max(delay, _RATE_LIMIT_EXTRA_DELAY)
                label = f"{api_attempt}/{_fmt_max(max_retries)}"
                kind = "限流" if is_rate_limit else "API"
                _log_warn(
                    f"{kind}错误 [{label}], {delay:.0f}s 后重试: "
                    f"{(result.error or '')[:200]}"
                )
                if on_stream:
                    on_stream(f"\n⚠️ {kind}错误，{delay:.0f}s 后重试 ({label})...\n")
                # session 已有内容时发「继续」而非重复完整 prompt
                if _session_has_assistant_content(session_file):
                    effective_prompt = "继续完成上次未完成的任务。"
                    _log_warn("session 已有内容，重试时发送「继续」而非重复完整 prompt")
                await asyncio.sleep(delay)
                continue
            else:
                _log_error(
                    f"API 重试耗尽 [{api_attempt}/{max_retries}]: "
                    f"{(result.error or '')[:200]}"
                )
                result.error = (
                    result.error or ""
                ) + f" [API 重试耗尽: {api_attempt} 次失败]"
                return result

        # ── 成功或不可重试的未知错误 ──
        if result.exit_code != 0 and result.error:
            err_lower = (result.error or "").lower()
            # ENOBUFS/EPIPE 是可重试的管道错误，不属于“不可重试”
            if any(p in err_lower for p in ("enobufs", "epipe", "broken pipe")):
                api_attempt += 1
                can_retry = (max_retries == -1) or (api_attempt <= max_retries)
                if can_retry:
                    delay = _backoff(retry_delay, api_attempt)
                    _log_warn(
                        f"管道错误 [{api_attempt}/{_fmt_max(max_retries)}], {delay:.0f}s 后重试: "
                        f"{(result.error or '')[:200]}"
                    )
                    await asyncio.sleep(delay)
                    continue
            _log_warn(
                f"pi 退出码 {result.exit_code} (有输出，不重试): {result.error[:200]}"
            )
        return result


# ─── JSON Lines 解析 ──────────────────────────────────────────────────────────


def _process_line(
    line: str,
    result: AgentResult,
    on_stream: Callable[[str], None] | None,
) -> bool:
    """解析一行 JSONL。返回 True 表示收到 agent_end（调用方应停止读取）。"""
    line = line.strip()
    if not line:
        return False
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return False

    etype = event.get("type")

    # RPC mode: 过滤命令响应和与 agent 无关的事件
    if etype in (
        "response",
        "session",
        "queue_update",
        "compaction_start",
        "compaction_end",
        "auto_retry_start",
        "auto_retry_end",
    ):
        return False

    # agent_end 信号本轮完成
    if etype == "agent_end":
        # agent_end 含全量 messages，可备用但不重复处理
        return True

    if etype == "message_update":
        ae = event.get("assistantMessageEvent", {})
        if ae.get("type") == "text_delta" and on_stream:
            on_stream(ae.get("delta", ""))

    if etype == "message_end" and event.get("message"):
        msg = event["message"]
        result.messages.append(msg)

        if msg.get("role") == "assistant":
            usage = msg.get("usage", {})
            result.token_usage.input += usage.get("input", 0)
            result.token_usage.output += usage.get("output", 0)
            result.token_usage.cache_read += usage.get("cacheRead", 0)
            result.token_usage.cache_write += usage.get("cacheWrite", 0)
            cost = usage.get("cost", {})
            if isinstance(cost, dict):
                result.token_usage.cost += cost.get("total", 0)
            elif isinstance(cost, (int, float)):
                result.token_usage.cost += cost

            if msg.get("stopReason") == "error":
                result.error = msg.get("errorMessage", "Unknown error")

    return False


# ─── 并行执行 ────────────────────────────────────────────────────────────────


async def run_agents_parallel(
    tasks: list[dict],
    concurrency: int = 4,
) -> list[AgentResult]:
    semaphore = asyncio.Semaphore(concurrency)
    results: list[AgentResult | None] = [None] * len(tasks)

    async def _run(index: int, kwargs: dict):
        async with semaphore:
            results[index] = await run_agent(**kwargs)

    await asyncio.gather(*[_run(i, t) for i, t in enumerate(tasks)])
    return results  # type: ignore[return-value]
