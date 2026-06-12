"""
system_analyse — Agent 子进程执行器 (Threading version)

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

import json
import logging
import os
import queue
import re
import shutil
import subprocess
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, Optional

from .agent_process import AgentProcessHandle, find_pi_command, _wait_with_timeout
from .models import TokenUsage
from .service.agent_runtime_registry import (
    register_agent_runtime,
    touch_agent_runtime,
    unregister_agent_runtime,
)

logger = logging.getLogger("sa.runner")

_MAX_BACKOFF = 30
_BACKOFF_SCHEDULE = (3.0, 5.0, 10.0, 15.0, 30.0)
_QUERY_ENGINE_401_MAX_RETRIES = 10

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
        self.fatal: bool = False


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
    idx = max(0, min(attempt - 1, len(_BACKOFF_SCHEDULE) - 1))
    return _BACKOFF_SCHEDULE[idx]


def _fmt_max(n: int) -> str:
    return "∞" if n < 0 else str(n)


def _normalize_timeout_seconds(timeout_seconds: float | int | None) -> float | None:
    if timeout_seconds is None:
        return None
    try:
        value = float(timeout_seconds)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _should_retry(
    failures: int, max_retries: int, cancel: threading.Event | None
) -> bool:
    if cancel and cancel.is_set():
        return False
    if max_retries < 0:
        return True
    return failures <= max_retries


def _cmd_preview(args: list[str]) -> str:
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


def _build_agent_env(
    base_env: dict[str, str] | None,
    *,
    task_pi_dir: str | None = None,
) -> dict[str, str] | None:
    payload = dict(base_env or {})
    normalized_pi_dir = str(task_pi_dir or "").strip()
    if normalized_pi_dir:
        payload["PI_CODING_AGENT_DIR"] = normalized_pi_dir
        payload["PI_MODELS_JSON"] = str(Path(normalized_pi_dir) / "models.json")
    return payload or None


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
    return find_pi_command()


def _build_args(
    pi_cmd: list[str],
    model: str,
    tools: list[str],
    thinking_level: str,
    session_file: str | None,
) -> list[str]:
    args = [*pi_cmd, "--mode", "rpc"]
    if session_file:
        args.extend(["--session", session_file])
    else:
        args.append("--no-session")
    if model:
        args.extend(["--model", model])
    if tools:
        args.extend(["--tools", ",".join(tools)])
    if thinking_level:
        args.extend(["--thinking", thinking_level])
    return args


def _write_temp_markdown(
    tmp_dir: str | None,
    prefix: str,
    filename: str,
    content: str,
) -> tuple[str, str]:
    if tmp_dir is None:
        tmp_dir = tempfile.mkdtemp(prefix=prefix)
    file_path = os.path.join(tmp_dir, filename)
    Path(file_path).write_text(content, encoding="utf-8")
    return tmp_dir, file_path


# ─── 错误分类 ─────────────────────────────────────────────────────────────────

_FATAL_PATTERNS = [
    ("model", "not found"),
    ("not found", "use --list"),
    ("invalid", "model"),
    ("invalid", "api key"),
    ("invalid", "api_key"),
    ("unauthorized",),
    ("authentication", "failed"),
]

_RETRYABLE_API_PATTERNS = [
    "connection", "timeout", "timed out",
    "ECONNREFUSED", "ECONNRESET", "ETIMEDOUT", "ENOTFOUND",
    "socket hang up", "fetch failed", "rate limit", "429",
    "503", "502", "500", "overloaded", "capacity",
    "temporarily unavailable", "server error", "internal error",
    "bad gateway", "service unavailable", "request failed",
    "network_error", "finish_reason", "too many requests",
    "ENOBUFS", "EPIPE",
]

_RETRYABLE_QUERY_ENGINE_401_PATTERNS = [
    ("401", "authentication error"),
    ("client is not connected to the query engine",),
    ("must call `connect()` before attempting to query data",),
]

_RATE_LIMIT_PATTERNS = ["rate limit", "429", "too many requests", "network_error", "finish_reason"]
_RATE_LIMIT_EXTRA_DELAY = 60

_PI_CRASH_PATTERNS = [
    "cannot find module", "module not found", "syntaxerror",
    "referenceerror", "typeerror", "segmentation fault", "segfault",
    "killed", "signal", "enoent", "eacces", "eperm",
    "heap out of memory", "allocation failed", "oom", "out of memory",
    "spawn", "execvp", "core dump", "bus error",
    "permission denied", "no such file",
]


def _is_fatal_error(result: AgentResult) -> bool:
    error_text = (result.error or "").lower()
    for pattern in _FATAL_PATTERNS:
        if all(p in error_text for p in pattern):
            return True
    return False


def _is_retryable_api_error(result: AgentResult) -> bool:
    if result.exit_code == 0 and not result.error:
        return False
    error_text = (result.error or "").lower()
    for pattern in _RETRYABLE_API_PATTERNS:
        if pattern in error_text:
            return True
    return False


def _is_retryable_query_engine_401_error(result: AgentResult) -> bool:
    if result.exit_code == 0 and not result.error:
        return False
    error_text = (result.error or "").lower()
    for pattern in _RETRYABLE_QUERY_ENGINE_401_PATTERNS:
        if all(p in error_text for p in pattern):
            return True
    return False


def _is_pi_crash(result: AgentResult) -> bool:
    if result.exit_code == 0:
        return False
    if result.messages:
        return False
    if _is_retryable_api_error(result):
        return False
    return True


# ─── Subprocess stdout reader thread ──────────────────────────────────────────


class _StdoutReader:
    """Read stdout from a subprocess in a background thread, pushing lines to a queue."""
    def __init__(self, stdout):
        self.stdout = stdout
        self.line_queue: queue.Queue = queue.Queue()
        self.done = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self):
        self._thread = threading.Thread(target=self._read_loop, daemon=True)
        self._thread.start()

    def _read_loop(self):
        try:
            buf = b""
            while True:
                chunk = self.stdout.read(4096)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    self.line_queue.put(line.decode("utf-8", errors="replace"))
            if buf.strip():
                self.line_queue.put(buf.decode("utf-8", errors="replace"))
        except Exception:
            pass
        finally:
            self.done.set()

    def read_line(self, timeout: float = 2.0) -> str | None:
        """Read a line with timeout. Returns None on timeout, or the line."""
        try:
            return self.line_queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def drain_remaining(self, timeout: float = 10.0) -> list[str]:
        """Drain all remaining lines after agent_ended."""
        lines: list[str] = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                line = self.line_queue.get(timeout=min(1.0, deadline - time.monotonic()))
                lines.append(line)
            except queue.Empty:
                if self.done.is_set() and self.line_queue.empty():
                    break
        return lines


# ─── Stderr reader thread ────────────────────────────────────────────────────


class _StderrReader:
    """Read stderr from a subprocess in a background thread."""
    def __init__(self, stderr):
        self.stderr = stderr
        self.result = threading.Event()
        self.data: bytes = b""
        self._thread: threading.Thread | None = None

    def start(self):
        self._thread = threading.Thread(target=self._read_loop, daemon=True)
        self._thread.start()

    def _read_loop(self):
        try:
            self.data = self.stderr.read()
        except Exception:
            pass
        finally:
            self.result.set()

    def get(self, timeout: float = 10.0) -> bytes:
        if self.result.wait(timeout=timeout):
            return self.data
        return b""


# ═════════════════════════════════════════════════════════════════════════════
# 公开接口
# ═════════════════════════════════════════════════════════════════════════════


def run_agent(
    prompt: str,
    *,
    model: str,
    tools: list[str],
    system_prompt: str = "",
    cwd: str = ".",
    env: dict[str, str] | None = None,
    task_pi_dir: str | None = None,
    thinking_level: str = "off",
    session_file: str | None = None,
    on_stream: Callable[[str], None] | None = None,
    cancel_event: threading.Event | None = None,
    max_retries: int = 3,
    retry_delay: float = 10.0,
    run_timeout_seconds: float | int | None = None,
    timeout_retry_enabled: bool = True,
    timeout_max_retries: int = 3,
    pi_max_retries: int = -1,
    pi_retry_delay: float = 10.0,
    model_stuck_timeout: float | None = None,
    model_stuck_max_activations: int | None = None,
) -> AgentResult:
    """
    运行单个 pi Agent 子进程（双层重试 + 致命错误检测 + per-pi-process stuck 监测）。
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
    env = _build_agent_env(env, task_pi_dir=task_pi_dir)

    tmp_dir: str | None = None
    sys_tmp_file: str | None = None
    prompt_tmp_file: str | None = None
    if system_prompt.strip():
        tmp_dir, sys_tmp_file = _write_temp_markdown(
            tmp_dir, "sa-", "system.md", system_prompt
        )
        args.extend(["--append-system-prompt", sys_tmp_file])

    timeout_seconds = _normalize_timeout_seconds(run_timeout_seconds)
    timeout_failures = 0
    try:
        while True:
            try:
                result = _run_with_context_overflow_recovery(
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
                    timeout_seconds=timeout_seconds,
                    model_stuck_timeout=model_stuck_timeout,
                    model_stuck_max_activations=model_stuck_max_activations,
                )
                return result
            except TimeoutError:
                timeout_failures += 1
                result = AgentResult()
                result.error = (
                    f"agent run idle timed out after {timeout_seconds:.0f}s"
                    if timeout_seconds else
                    "agent run idle timed out"
                )
                result.exit_code = -1
                can_retry = timeout_retry_enabled and (
                    timeout_max_retries < 0 or timeout_failures <= timeout_max_retries
                )
                if not can_retry or (cancel_event and cancel_event.is_set()):
                    return result
                delay = _backoff(retry_delay, timeout_failures)
                _log_warn(
                    f"agent 单次输入空闲超时 [{timeout_failures}/{_fmt_max(timeout_max_retries)}], "
                    f"{delay:.0f}s 后重试: {result.error}"
                )
                if on_stream:
                    on_stream(
                        f"\n⏱️ 智能体空闲超时，{delay:.0f}s 后重试 "
                        f"({timeout_failures}/{_fmt_max(timeout_max_retries)})...\n"
                    )
                time.sleep(delay)
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


def _run_with_context_overflow_recovery(
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
    cancel_event: threading.Event | None,
    max_retries: int,
    retry_delay: float,
    pi_max_retries: int,
    pi_retry_delay: float,
    timeout_seconds: float | None = None,
    model_stuck_timeout: float | None = None,
    model_stuck_max_activations: int | None = None,
) -> AgentResult:
    result = _run_with_pi_retry(
        args=args, cwd=cwd, env=env, prompt=prompt,
        cancel_event=cancel_event, on_stream=on_stream,
        max_retries=max_retries, retry_delay=retry_delay,
        pi_max_retries=pi_max_retries, pi_retry_delay=pi_retry_delay,
        timeout_seconds=timeout_seconds, session_file=session_file,
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
        _run_with_pi_retry(
            args=compaction_args, cwd=cwd, env=env,
            prompt=_COMPACTION_TRIGGER_PROMPT,
            cancel_event=cancel_event, on_stream=None,
            max_retries=max_retries, retry_delay=retry_delay,
            pi_max_retries=pi_max_retries, pi_retry_delay=pi_retry_delay,
            timeout_seconds=None,
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

    retry_result = _run_with_pi_retry(
        args=args, cwd=cwd, env=env, prompt=prompt,
        cancel_event=cancel_event, on_stream=on_stream,
        max_retries=max_retries, retry_delay=retry_delay,
        pi_max_retries=pi_max_retries, pi_retry_delay=pi_retry_delay,
        timeout_seconds=None,
    )
    return retry_result


# ─── 外层：pi 进程级重试 ─────────────────────────────────────────────────────


def _run_with_pi_retry(
    *,
    args: list[str],
    cwd: str,
    env: dict[str, str] | None,
    prompt: str,
    cancel_event: threading.Event | None,
    on_stream: Callable[[str], None] | None,
    max_retries: int,
    retry_delay: float,
    pi_max_retries: int,
    pi_retry_delay: float,
    timeout_seconds: float | None = None,
    session_file: str | None = None,
    model_stuck_timeout: float | None = None,
    model_stuck_max_activations: int | None = None,
) -> AgentResult:
    if not os.path.isdir(cwd):
        _log_error(f"cwd 目录不存在（不可重试）: {cwd}")
        r = AgentResult()
        r.error = f"cwd directory does not exist: {cwd}"
        r.exit_code = -1
        r.fatal = True
        return r

    pi_attempt = 0
    _PI_CRASH_LOOP_MAX: int = int(os.environ.get("SECFLOW_SA_PI_CRASH_LOOP_MAX", "5"))
    _PI_CRASH_LOOP_WINDOW: float = float(os.environ.get("SECFLOW_SA_PI_CRASH_LOOP_WINDOW", "60"))
    _crash_times: list[float] = []

    while True:
        if cancel_event and cancel_event.is_set():
            r = AgentResult()
            r.error = "cancelled"
            return r

        try:
            result = _run_with_api_retry(
                args=args, cwd=cwd, env=env, prompt=prompt,
                cancel_event=cancel_event, on_stream=on_stream,
                max_retries=max_retries, retry_delay=retry_delay,
                session_file=session_file, timeout_seconds=timeout_seconds,
                model_stuck_timeout=model_stuck_timeout,
                model_stuck_max_activations=model_stuck_max_activations,
            )

            if _is_fatal_error(result):
                result.fatal = True
                _log_error(f"pi 致命错误（不可重试）: {result.error}")
                return result

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
                    r.fatal = True
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
                time.sleep(delay)
                continue
            else:
                _log_error(f"pi 进程重试耗尽 [{label}]: {exc}")
                r = AgentResult()
                r.exit_code = -1
                r.error = f"pi process failed after {pi_attempt} retries: {exc}"
                return r


# ─── 内层：API 级重试 ────────────────────────────────────────────────────────


def _session_has_assistant_content(session_file: "str | None") -> bool:
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


def _run_with_api_retry(
    *,
    args: list[str],
    cwd: str,
    env: dict[str, str] | None,
    prompt: str,
    cancel_event: threading.Event | None,
    on_stream: Callable[[str], None] | None,
    max_retries: int,
    retry_delay: float,
    session_file: str | None = None,
    timeout_seconds: float | None = None,
    model_stuck_timeout: float | None = None,
    model_stuck_max_activations: int | None = None,
) -> AgentResult:
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
        registered_session_file: str | None = None

        # ── 拉起子进程 ──
        handle = AgentProcessHandle.spawn(
            *args,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
            logger=_log_warn,
            label="system-agent",
        )
        proc = handle.proc
        if session_file:
            register_agent_runtime(
                session_file=session_file,
                cwd=cwd,
                pid=int(proc.pid),
                command=" ".join(args),
                runtime_kind="pi",
            )
            registered_session_file = session_file

        # Start cancel monitor thread
        cancel_stop = threading.Event()

        def _cancel_monitor():
            if cancel_event:
                cancel_event.wait()
                if not cancel_stop.is_set():
                    handle.terminate_tree(reason="cancel_event")

        cancel_thread = None
        if cancel_event:
            cancel_thread = threading.Thread(target=_cancel_monitor, daemon=True)
            cancel_thread.start()

        # Start stdout reader thread
        stdout_reader = _StdoutReader(proc.stdout)
        stdout_reader.start()

        # Start stderr reader thread
        stderr_reader = _StderrReader(proc.stderr)
        stderr_reader.start()

        agent_ended = False
        try:
            # Send initial prompt via stdin
            prompt_cmd = json.dumps(
                {"type": "prompt", "message": effective_prompt},
                ensure_ascii=False,
            ) + chr(10)
            proc.stdin.write(prompt_cmd.encode("utf-8"))
            proc.stdin.flush()

            # Per-pi-process stuck monitoring
            _pi_last_mtime: float = 0.0
            _pi_last_active: float = time.monotonic()
            last_activity_at: float = time.monotonic()
            if _stuck_timeout > 0 and session_file:
                try:
                    _pi_last_mtime = os.path.getmtime(session_file)
                except OSError:
                    _pi_last_mtime = 0.0
            _pi_stuck_triggered: bool = False

            def _mark_activity() -> None:
                nonlocal last_activity_at
                last_activity_at = time.monotonic()

            buffer = b""
            _read_timeout = 2.0
            while True:
                # Check stuck detection via session mtime
                if _stuck_timeout > 0 and session_file:
                    try:
                        _cur_mtime = os.path.getmtime(session_file)
                    except OSError:
                        _cur_mtime = _pi_last_mtime
                    if _cur_mtime != _pi_last_mtime:
                        _pi_last_mtime = _cur_mtime
                        _pi_last_active = time.monotonic()
                        _mark_activity()

                if _stuck_timeout > 0:
                    _idle = time.monotonic() - _pi_last_active
                    if _idle >= _stuck_timeout:
                        _pi_stuck_triggered = True
                        break

                # Read line from stdout with timeout
                line = stdout_reader.read_line(timeout=_read_timeout)

                if line is None:
                    # Timeout - check if process is still alive
                    if cancel_event and cancel_event.is_set():
                        break
                    if timeout_seconds and (time.monotonic() - last_activity_at) >= timeout_seconds:
                        raise TimeoutError("agent idle timeout")
                    if stdout_reader.done.is_set() and stdout_reader.line_queue.empty():
                        break
                    continue

                _mark_activity()
                touch_agent_runtime(session_file=session_file, cwd=cwd, command=" ".join(args))
                ended = _process_line(line, result, on_stream, _mark_activity)
                if ended:
                    agent_ended = True
                    break

            # Handle stuck trigger
            if _pi_stuck_triggered:
                _idle_secs = time.monotonic() - _pi_last_active
                handle.terminate_tree(reason="stuck_recovery")
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
                cancel_stop.set()
                if registered_session_file:
                    unregister_agent_runtime(session_file=registered_session_file, cwd=cwd, command=" ".join(args))
                continue

            # Drain stdout after agent_ended
            if agent_ended:
                stdout_reader.drain_remaining(timeout=10.0)

            # Close stdin
            try:
                proc.stdin.close()
            except Exception:
                pass

            # Get stderr
            stderr_data = stderr_reader.get(timeout=10.0)
            stderr_text = stderr_data.decode("utf-8", errors="replace").strip()
            if stderr_text and not result.error:
                result.error = stderr_text

            # Wait for process
            try:
                proc.wait(timeout=15.0)
                result.exit_code = proc.returncode or 0
            except subprocess.TimeoutExpired:
                _log_warn("pi 进程未在 15s 内退出，强制终止")
                handle.terminate_tree(reason="exit_timeout")
                result.exit_code = -1

        except Exception as e:
            _log_warn(f"pi 进程读取异常: {e}")
            result.error = f"pi process read error: {e}"
            result.exit_code = -1
            handle.terminate_tree(reason=f"read_exception:{type(e).__name__}")

        finally:
            cancel_stop.set()
            if registered_session_file:
                unregister_agent_runtime(session_file=registered_session_file, cwd=cwd, command=" ".join(args))
            handle.terminate_tree(
                reason="finally_cleanup",
                term_timeout=2.0,
                kill_timeout=2.0,
            )

        # Extract output
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

        if _is_pi_crash(result):
            if result.error:
                _log_warn(
                    f"pi 进程崩溃 (exit={result.exit_code}): {result.error[:300]}"
                )
            return result

        if _is_fatal_error(result):
            return result

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
                time.sleep(delay)
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

        if _is_retryable_api_error(result):
            api_attempt += 1
            can_retry = (max_retries == -1) or (api_attempt <= max_retries)
            if can_retry:
                delay = _backoff(retry_delay, api_attempt)
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
                if _session_has_assistant_content(session_file):
                    effective_prompt = "继续完成上次未完成的任务。"
                    _log_warn("session 已有内容，重试时发送「继续」而非重复完整 prompt")
                time.sleep(delay)
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

        if result.exit_code != 0 and result.error:
            err_lower = (result.error or "").lower()
            if any(p in err_lower for p in ("enobufs", "epipe", "broken pipe")):
                api_attempt += 1
                can_retry = (max_retries == -1) or (api_attempt <= max_retries)
                if can_retry:
                    delay = _backoff(retry_delay, api_attempt)
                    _log_warn(
                        f"管道错误 [{api_attempt}/{_fmt_max(max_retries)}], {delay:.0f}s 后重试: "
                        f"{(result.error or '')[:200]}"
                    )
                    time.sleep(delay)
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
    on_activity: Callable[[], None] | None = None,
) -> bool:
    """解析一行 JSONL。返回 True 表示收到 agent_end（调用方应停止读取）。"""
    line = line.strip()
    if not line:
        return False
    if on_activity:
        on_activity()
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return False

    etype = event.get("type")

    if etype in (
        "response", "session", "queue_update",
        "compaction_start", "compaction_end",
        "auto_retry_start", "auto_retry_end",
    ):
        return False

    if etype == "agent_end":
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


def run_agents_parallel(
    tasks: list[dict],
    concurrency: int = 4,
) -> list[AgentResult]:
    """Run multiple agents in parallel using ThreadPoolExecutor."""
    results: list[AgentResult | None] = [None] * len(tasks)
    semaphore = threading.BoundedSemaphore(max(1, concurrency))

    def _run(index: int, kwargs: dict):
        with semaphore:
            results[index] = run_agent(**kwargs)

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
        futures = [executor.submit(_run, i, t) for i, t in enumerate(tasks)]
        for f in futures:
            try:
                f.result()
            except Exception:
                pass

    return results  # type: ignore[return-value]
