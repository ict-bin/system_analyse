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
# API key 认证错误（wsk/sk 无效）重试上限：3 次后判 fatal 退出任务，不无限重试
_KEY_AUTH_MAX_RETRIES = max(1, int(os.environ.get("SECFLOW_SA_KEY_AUTH_MAX_RETRIES", "3")))

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
# pi 原生 compact RPC 命令的自定义指令（压缩时聚焦保留什么）。
# 注意：压缩通过 pi 的 `{"type":"compact"}` RPC 命令触发，
# 绝不能用 prompt 帧伪装——否则模型只会回一句"COMPACTION_OK"，
# pi 的原生压缩机制不会执行，上下文不降反升，导致无限循环。
_COMPACT_CUSTOM_INSTRUCTIONS = (
    "仅保留后续继续执行任务所需的关键结论、约束和待办，丢弃已完成步骤的细节。"
)
# 单次 compact 子进程的最大等待时长（压缩本身要调一次 LLM 做摘要）
_COMPACT_TIMEOUT = max(60.0, float(os.environ.get("SECFLOW_SA_COMPACT_TIMEOUT", "300")))
# 上下文溢出后 compact+retry 的最大轮数，超过即判失败，杜绝无限循环
_MAX_OVERFLOW_COMPACT_ATTEMPTS = max(1, int(os.environ.get("SECFLOW_SA_MAX_OVERFLOW_COMPACT_ATTEMPTS", "3")))
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
        self.rate_limited: bool = False
        self.consecutive_rate_limit_count: int = 0
        self.retry_delay_seconds: int = 0
        self.rate_limit_event_due: bool = False
        self.api_retry_event_due: bool = False
        self.consecutive_api_retry_count: int = 0
        self.api_retry_reason: str | None = None
        self.fatal_retry_event_due: bool = False
        self.consecutive_fatal_retry_count: int = 0
        self.fatal_retry_reason: str | None = None
        self.agent_role: str | None = None
        self.runtime_dir: str | None = None
        self.context_window: int = 0
        self.proxy_reserved_tokens: int = 0
        self.compaction_requested: bool = False
        self.compaction_completed: bool = False
        self.context_overflow_retrying: bool = False
        self.context_budget_exceeded_preflight: bool = False
        self.context_overflow_failed_after_compaction: bool = False
        self.context_overflow_retry_count: int = 0
        self.context_overflow_retry_event_due: bool = False


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


def _should_emit_api_retry_event(consecutive_retries: int, delay_seconds: float) -> bool:
    retries = max(0, int(consecutive_retries or 0))
    delay = max(0.0, float(delay_seconds or 0))
    return delay >= 30.0 and retries > 0 and retries % 10 == 0


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


def _effective_context_limit(context_window: int, proxy_reserved_tokens: int = 0) -> int:
    reserve = max(int(proxy_reserved_tokens or 0), 4096)
    response_headroom = 4096
    return max(1, int(context_window) - reserve - response_headroom)


def _preflight_context_token_limit(context_window: int, proxy_reserved_tokens: int = 0) -> int:
    return max(1, int(_effective_context_limit(context_window, proxy_reserved_tokens) * _SINGLE_INPUT_CONTEXT_RATIO))


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
        "actual_input_tokens": 0,
        "requested_output_tokens": 0,
        "context_length": 0,
        "provider_reported_context_length": 0,
        "max_input_tokens": 0,
        "proxy_reserved_tokens": 0,
    }
    if "context length" not in lowered and "input tokens" not in lowered and "prefill_context_length_exceeded" not in lowered:
        return details

    patterns = {
        "input_tokens": r"passed\s+(\d+)\s+input tokens",
        "actual_input_tokens": r"input has\s+(\d+)\s+tokens",
        "requested_output_tokens": r"requested\s+(\d+)\s+output tokens",
        "context_length": r"context length is only\s+(\d+)\s+tokens",
        "provider_reported_context_length": r"maximum context length is\s+(\d+)\s+tokens",
        "max_input_tokens": r"maximum input length(?: of)?\s+(\d+)\s+tokens",
        "proxy_reserved_tokens": r"reserves\s+(\d+)\s+safety-buffer tokens",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            details[key] = int(match.group(1))
    if details["provider_reported_context_length"] and not details["context_length"]:
        details["context_length"] = details["provider_reported_context_length"]
    if details["actual_input_tokens"] and not details["input_tokens"]:
        details["input_tokens"] = details["actual_input_tokens"]
    return details


def _is_context_overflow_error(error_text: str | None) -> bool:
    details = _parse_context_overflow_details(error_text)
    if details["context_length"] > 0:
        return True
    lowered = str(error_text or "").lower()
    return (
        ("context length" in lowered or "prefill_context_length_exceeded" in lowered)
        and ("input tokens" in lowered or "input has" in lowered)
        and ("badrequesterror" in lowered or "400" in lowered)
    )


def _format_context_overflow_failure(
    original_error: str | None,
    *,
    context_window: int,
    single_input_tokens: int,
    single_input_limit: int,
    compaction_attempted: bool,
    proxy_reserved_tokens: int = 0,
) -> str:
    action = "已先触发一次会话自动压缩并重试" if compaction_attempted else "未能触发会话自动压缩"
    return (
        f"{action}，但当前单次输入估算约 {single_input_tokens} tokens，"
        f"超过有效预算阈值 75%: {single_input_limit}/{context_window}（proxy_reserved={max(int(proxy_reserved_tokens or 0), 4096)}），"
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
    ("invalid model",),
    ("unknown model",),
    ("model does not exist",),
    ("unsupported model",),
]

# API key 认证失败模式（从 _FATAL_PATTERNS 移出，走 3 次重试后 fatal 的专用路径）
_KEY_AUTH_PATTERNS = [
    ("invalid", "api key"),
    ("invalid", "api_key"),
    ("unauthorized",),
    ("authentication", "failed"),
    ("401",),
    ("invalid_api_key",),
]

# 无可用模型/部署不可用错误（网关 cooldown、no deployments available）。
# 这类错误是持续性的（部署不会在几秒内恢复），直接失败不重试。
_NO_MODEL_AVAILABLE_PATTERNS = [
    "no deployments available",
    "cooldown_list",
    "no models available",
    "no available model",
    "model not available",
    "no model available",
    "no deployments",
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
_RATE_LIMIT_EXTRA_DELAY = 30
_FATAL_RETRY_DELAY_SECONDS = 30.0

_INFINITE_RETRY_API_PATTERNS = [
    "connection error",
    "econnrefused",
    "econnreset",
    "etimedout",
    "enotfound",
    "socket hang up",
    "fetch failed",
    "temporarily unavailable",
    "service unavailable",
    "bad gateway",
    "server error",
    "internal error",
]


def _should_emit_rate_limit_event(streak: int) -> bool:
    streak = max(0, int(streak or 0))
    return streak == 1 or (streak > 0 and streak % 10 == 0)


def _should_emit_infinite_retry_event(streak: int) -> bool:
    streak = max(0, int(streak or 0))
    return streak > 0 and streak % 10 == 0


def _mark_infinite_retry(result: AgentResult, *, kind: str, count: int, reason: str, delay_seconds: float = 30.0) -> None:
    result.fatal = False
    result.retry_delay_seconds = int(delay_seconds)
    if kind == "fatal":
        result.consecutive_fatal_retry_count = int(count)
        result.fatal_retry_reason = reason
        result.fatal_retry_event_due = _should_emit_infinite_retry_event(count)
    else:
        result.context_overflow_retry_count = int(count)
        result.context_overflow_retrying = True
        result.context_overflow_retry_event_due = _should_emit_infinite_retry_event(count)


def _is_infinite_retry_api_error(result: "AgentResult") -> bool:
    error_text = (result.error or "").lower()
    if not error_text:
        return False
    return any(pattern in error_text for pattern in _INFINITE_RETRY_API_PATTERNS)

_PI_CRASH_PATTERNS = [
    "cannot find module", "module not found", "syntaxerror",
    "referenceerror", "typeerror", "segmentation fault", "segfault",
    "killed", "signal", "enoent", "eacces", "eperm",
    "heap out of memory", "allocation failed", "oom", "out of memory",
    "spawn", "execvp", "core dump", "bus error",
    "permission denied", "no such file",
]


def _is_fatal_error(result: AgentResult) -> bool:
    if _is_context_overflow_error(result.error):
        return False
    error_text = (result.error or "").lower()
    for pattern in _FATAL_PATTERNS:
        if all(p in error_text for p in pattern):
            return True
    return False


def _is_key_auth_error(result: AgentResult) -> bool:
    """API key 认证失败（wsk/sk 无效/401/unauthorized）。走专用 3 次重试路径。"""
    if result.exit_code == 0 and not result.error:
        return False
    error_text = (result.error or "").lower()
    for pattern in _KEY_AUTH_PATTERNS:
        if all(p in error_text for p in pattern):
            return True
    return False


def _is_no_model_available_error(result: AgentResult) -> bool:
    """无可用模型/部署不可用（网关 cooldown、no deployments available）。
    持续性错误，直接失败不重试。"""
    if result.exit_code == 0 and not result.error:
        return False
    error_text = (result.error or "").lower()
    for pattern in _NO_MODEL_AVAILABLE_PATTERNS:
        if pattern in error_text:
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
            import select
            fd = self.stdout.fileno()
            buf = b""
            while True:
                # Use select with 1s timeout to detect EOF without blocking forever
                ready, _, _ = select.select([fd], [], [], 1.0)
                if not ready:
                    # No data available; keep waiting
                    continue
                try:
                    chunk = os.read(fd, 4096)
                except OSError:
                    break
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    self.line_queue.put(line.decode("utf-8", errors="replace"))
            if buf.strip():
                self.line_queue.put(buf.decode("utf-8", errors="replace"))
        except Exception:
            import traceback
            traceback.print_exc()
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
            import select
            fd = self.stderr.fileno()
            chunks = []
            while True:
                ready, _, _ = select.select([fd], [], [], 1.0)
                if not ready:
                    continue
                try:
                    chunk = os.read(fd, 4096)
                except OSError:
                    break
                if not chunk:
                    break
                chunks.append(chunk)
            self.data = b"".join(chunks)
        except Exception:
            import traceback
            traceback.print_exc()
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
    agent_role: str = "",
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
    fatal_max_retries: int = -1,
) -> AgentResult:
    """
    运行单个 pi Agent 子进程（双层重试 + 致命错误检测 + per-pi-process stuck 监测）。

    fatal_max_retries: pi 致命错误（非 no-model/key-auth）的重试上限。-1=无限（默认，
    供 pipeline 自愈）；0=不重试立即返回（供 failure_debug 等一次性调用）。
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
                    agent_role=agent_role,
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
                    task_pi_dir=task_pi_dir,
                    timeout_seconds=timeout_seconds,
                    model_stuck_timeout=model_stuck_timeout,
                    model_stuck_max_activations=model_stuck_max_activations,
                    fatal_max_retries=fatal_max_retries,
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


def _run_compact_command(
    *,
    pi_cmd: list[str],
    model: str,
    tools: list[str],
    thinking_level: str,
    session_file: str,
    cwd: str,
    env: dict[str, str] | None,
    cancel_event: threading.Event | None,
    max_retries: int,
    retry_delay: float,
    pi_max_retries: int,
    pi_retry_delay: float,
) -> dict:
    """通过 pi RPC `compact` 命令触发原生上下文压缩。

    spawn `pi --mode rpc --session <file>` → stdin 发 `{"type":"compact",...}` →
    读 stdout 直到 `response`(command=compact) 或 compaction_end 事件 → 解析 success。
    这是 pi 的原生压缩（调 LLM 摘要旧消息 + 截断会话），
    不是发 prompt 让模型"回复 COMPACTION_OK"。

    返回 {success, tokens_before, estimated_tokens_after, error}。
    """
    import uuid as _uuid
    args = _build_args(pi_cmd, model, tools, thinking_level, session_file)
    pi_crash_count = 0
    while True:
        if cancel_event and cancel_event.is_set():
            return {"success": False, "tokens_before": None,
                    "estimated_tokens_after": None, "error": "cancelled"}
        handle = None
        proc = None
        registered = False
        try:
            handle = AgentProcessHandle.spawn(
                *args, cwd=cwd, env=env,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE,
                logger=_log_warn, label="system-agent-compact",
            )
            proc = handle.proc
            if session_file:
                register_agent_runtime(session_file=session_file, cwd=cwd, pid=int(proc.pid), command=" ".join(args))
                registered = True
            stdout_reader = _StdoutReader(proc.stdout)
            stdout_reader.start()
            stderr_reader = _StderrReader(proc.stderr)
            stderr_reader.start()

            req_id = _uuid.uuid4().hex[:12]
            cmd = json.dumps(
                {"type": "compact", "id": req_id,
                 "customInstructions": _COMPACT_CUSTOM_INSTRUCTIONS},
                ensure_ascii=False,
            ) + chr(10)
            proc.stdin.write(cmd.encode("utf-8"))
            proc.stdin.flush()

            deadline = time.monotonic() + _COMPACT_TIMEOUT
            compact_resp = None
            while True:
                if cancel_event and cancel_event.is_set():
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                line = stdout_reader.read_line(timeout=min(2.0, remaining))
                if line is None:
                    if stdout_reader.done.is_set() and stdout_reader.line_queue.empty():
                        break
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue
                etype = ev.get("type")
                if etype == "compaction_start":
                    _log_info(f"pi compaction 开始: reason={ev.get('reason')}")
                elif etype == "compaction_end":
                    res = ev.get("result") or {}
                    _log_info(
                        f"pi compaction 完成: tokensBefore={res.get('tokensBefore')} "
                        f"estimatedAfter={res.get('estimatedTokensAfter')} "
                        f"aborted={ev.get('aborted')} willRetry={ev.get('willRetry')}"
                    )
                elif etype == "response" and ev.get("command") == "compact":
                    compact_resp = ev
                    break
                elif etype == "extension_error":
                    _log_warn(f"pi compaction extension error: {ev.get('error')}")
            try:
                proc.stdin.close()
            except Exception:
                pass
            try:
                proc.wait(timeout=15.0)
            except subprocess.TimeoutExpired:
                handle.terminate_tree(reason="compact_exit_timeout")
            if compact_resp is not None:
                data = compact_resp.get("data") or {}
                return {
                    "success": bool(compact_resp.get("success")),
                    "tokens_before": data.get("tokensBefore"),
                    "estimated_tokens_after": data.get("estimatedTokensAfter"),
                    "error": compact_resp.get("error"),
                }
            # 没收到 response —— 多半是 pi 进程崩溃或致命错误
            stderr_text = stderr_reader.get(timeout=5.0).decode("utf-8", errors="replace").strip()
            err_msg = stderr_text or "compact: no response from pi"
            pi_crash_count += 1
            if not _should_retry(pi_crash_count, pi_max_retries, cancel_event):
                return {"success": False, "tokens_before": None,
                        "estimated_tokens_after": None, "error": err_msg}
            delay = _backoff(pi_retry_delay, pi_crash_count)
            _log_warn(f"compact pi 进程异常 [{pi_crash_count}], {delay:.0f}s 后重试: {err_msg[:200]}")
            time.sleep(delay)
        except Exception as e:
            pi_crash_count += 1
            if not _should_retry(pi_crash_count, pi_max_retries, cancel_event):
                return {"success": False, "tokens_before": None,
                        "estimated_tokens_after": None, "error": f"compact exception: {e}"}
            delay = _backoff(pi_retry_delay, pi_crash_count)
            _log_warn(f"compact 异常 [{pi_crash_count}], {delay:.0f}s 后重试: {e}")
            time.sleep(delay)
        finally:
            if handle is not None:
                if registered:
                    unregister_agent_runtime(session_file=session_file, cwd=cwd, command=" ".join(args))
                handle.terminate_tree(reason="compact_finally", term_timeout=2.0, kill_timeout=2.0)
            if proc is not None:
                for pipe in (proc.stdin, proc.stdout, proc.stderr):
                    try:
                        if pipe:
                            pipe.close()
                    except Exception:
                        pass


def _run_with_context_overflow_recovery(
    *,
    pi_cmd: list[str],
    args: list[str],
    prompt: str,
    system_prompt: str,
    model: str,
    agent_role: str,
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
    task_pi_dir: str | None = None,
    timeout_seconds: float | None = None,
    model_stuck_timeout: float | None = None,
    model_stuck_max_activations: int | None = None,
    fatal_max_retries: int = -1,
) -> AgentResult:
    runtime_dir = str(task_pi_dir or (env.get("PI_CODING_AGENT_DIR") if env else "")).strip() or None
    overflow_attempts = 0
    fatal_attempts = 0
    base_context_window = _model_context_window(model)
    while True:
        current_context_window = base_context_window
        current_proxy_reserved_tokens = 0
        preflight_limit = _preflight_context_token_limit(current_context_window)
        preflight_tokens = _single_input_token_estimate(system_prompt, prompt)
        if preflight_tokens > preflight_limit:
            if not session_file:
                preflight_result = AgentResult()
                preflight_result.agent_role = agent_role or None
                preflight_result.runtime_dir = runtime_dir
                preflight_result.context_window = current_context_window
                preflight_result.proxy_reserved_tokens = 4096
                preflight_result.context_budget_exceeded_preflight = True
                preflight_result.error = _format_context_overflow_failure(
                    "preflight_context_budget_exceeded_without_session",
                    context_window=current_context_window,
                    single_input_tokens=preflight_tokens,
                    single_input_limit=preflight_limit,
                    compaction_attempted=False,
                    proxy_reserved_tokens=4096,
                )
                preflight_result.context_overflow_failed_after_compaction = True
                return preflight_result
            overflow_attempts += 1
            if overflow_attempts > _MAX_OVERFLOW_COMPACT_ATTEMPTS:
                preflight_result = AgentResult()
                preflight_result.agent_role = agent_role or None
                preflight_result.runtime_dir = runtime_dir
                preflight_result.context_window = current_context_window
                preflight_result.proxy_reserved_tokens = 4096
                preflight_result.context_budget_exceeded_preflight = True
                preflight_result.context_overflow_failed_after_compaction = True
                preflight_result.error = _format_context_overflow_failure(
                    "preflight_compaction_exhausted",
                    context_window=current_context_window,
                    single_input_tokens=preflight_tokens,
                    single_input_limit=preflight_limit,
                    compaction_attempted=True,
                    proxy_reserved_tokens=4096,
                )
                return preflight_result
            msg = f"单次输入超出上下文预算，触发 pi 原生 compact [{overflow_attempts}/{_MAX_OVERFLOW_COMPACT_ATTEMPTS}]"
            _log_warn(msg)
            if on_stream:
                on_stream(f"\n⚠️ {msg}\n")
            compact = _run_compact_command(
                pi_cmd=pi_cmd, model=model, tools=tools, thinking_level=thinking_level,
                session_file=session_file, cwd=cwd, env=env, cancel_event=cancel_event,
                max_retries=max_retries, retry_delay=retry_delay,
                pi_max_retries=pi_max_retries, pi_retry_delay=pi_retry_delay,
            )
            if not compact.get("success"):
                preflight_result = AgentResult()
                preflight_result.agent_role = agent_role or None
                preflight_result.runtime_dir = runtime_dir
                preflight_result.context_window = current_context_window
                preflight_result.proxy_reserved_tokens = 4096
                preflight_result.context_budget_exceeded_preflight = True
                preflight_result.context_overflow_failed_after_compaction = True
                preflight_result.error = _format_context_overflow_failure(
                    "preflight_compaction_failed",
                    context_window=current_context_window,
                    single_input_tokens=preflight_tokens,
                    single_input_limit=preflight_limit,
                    compaction_attempted=True,
                    proxy_reserved_tokens=4096,
                )
                preflight_result.error = (preflight_result.error or "") + (
                    f" [compact failed: {compact.get('error')}]"
                )
                return preflight_result
            continue
        result = _run_with_pi_retry(
            args=args, cwd=cwd, env=env, prompt=prompt,
            cancel_event=cancel_event, on_stream=on_stream,
            max_retries=max_retries, retry_delay=retry_delay,
            pi_max_retries=pi_max_retries, pi_retry_delay=pi_retry_delay,
            timeout_seconds=timeout_seconds, session_file=session_file,
            model_stuck_timeout=model_stuck_timeout,
            model_stuck_max_activations=model_stuck_max_activations,
            fatal_max_retries=fatal_max_retries,
        )
        result.agent_role = agent_role or None
        result.runtime_dir = runtime_dir
        result.context_window = current_context_window
        if not _is_context_overflow_error(result.error):
            return result
        overflow = _parse_context_overflow_details(result.error)
        current_context_window = overflow["context_length"] or _model_context_window(model)
        current_proxy_reserved_tokens = overflow["proxy_reserved_tokens"]
        result.agent_role = agent_role or None
        result.runtime_dir = runtime_dir
        result.context_window = current_context_window
        result.proxy_reserved_tokens = current_proxy_reserved_tokens
        if not session_file:
            result.context_overflow_failed_after_compaction = True
            return result
        overflow_attempts += 1
        result.compaction_requested = True
        msg = (
            f"检测到智能体单次请求触发上下文超限，触发 pi 原生 compact "
            f"[{overflow_attempts}/{_MAX_OVERFLOW_COMPACT_ATTEMPTS}]"
        )
        _log_warn(msg)
        if on_stream:
            on_stream(f"\n⚠️ {msg}\n")
        if overflow_attempts > _MAX_OVERFLOW_COMPACT_ATTEMPTS:
            result.context_overflow_failed_after_compaction = True
            result.error = (
                (result.error or "")
                + f" [compaction recovery 已达上限 {_MAX_OVERFLOW_COMPACT_ATTEMPTS} 次仍溢出]"
            )
            return result
        compact = _run_compact_command(
            pi_cmd=pi_cmd, model=model, tools=tools, thinking_level=thinking_level,
            session_file=session_file, cwd=cwd, env=env, cancel_event=cancel_event,
            max_retries=max_retries, retry_delay=retry_delay,
            pi_max_retries=pi_max_retries, pi_retry_delay=pi_retry_delay,
        )
        result.compaction_completed = bool(compact.get("success"))
        result.context_overflow_retrying = True
        result.context_overflow_retry_count = overflow_attempts
        result.context_overflow_retry_event_due = _should_emit_infinite_retry_event(overflow_attempts)
        if not compact.get("success"):
            result.context_overflow_failed_after_compaction = True
            result.error = (
                (result.error or "")
                + f" [compact failed: {compact.get('error')}]"
            )
            return result
        if result.context_overflow_retry_event_due:
            _log_warn(
                f"overflow 压缩后重试 [{overflow_attempts}], "
                f"estimated_after={compact.get('estimated_tokens_after')}: "
                f"{(result.error or '')[:200]}"
            )
        continue


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
    fatal_max_retries: int = -1,
) -> AgentResult:
    if not os.path.isdir(cwd):
        _log_error(f"cwd 目录不存在（不可重试）: {cwd}")
        r = AgentResult()
        r.error = f"cwd directory does not exist: {cwd}"
        r.exit_code = -1
        r.fatal = True
        return r

    pi_attempt = 0
    fatal_retry_count = 0
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
                fatal_max_retries=fatal_max_retries,
            )

            if _is_key_auth_error(result):
                # API key 认证失败是终端错误（无效 key 不会自愈），不进入 fatal 无限重试
                return result

            if _is_no_model_available_error(result):
                # 无可用模型/部署不可用是终端错误（不会自愈），不进入 fatal 无限重试
                return result

            if _is_fatal_error(result) or result.fatal:
                fatal_retry_count += 1
                reason = str(result.error or "").strip() or "fatal error"
                if fatal_max_retries >= 0 and fatal_retry_count > fatal_max_retries:
                    _log_warn(f"pi 基础设施异常 [{fatal_retry_count}/{_fmt_max(fatal_max_retries)}] 达上限，终止重试: {reason[:200]}")
                    return result
                _mark_infinite_retry(result, kind="fatal", count=fatal_retry_count, reason=reason)
                _log_warn(
                    f"pi 基础设施异常 [{fatal_retry_count}/{_fmt_max(fatal_max_retries)}], 30s 后重试: {reason[:200]}"
                )
                if on_stream:
                    on_stream("\n⚠️ 智能体基础设施异常，30 秒后自动重试...\n")
                time.sleep(30)
                continue

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
                    fatal_retry_count += 1
                    r = AgentResult()
                    r.error = str(exc)
                    r.exit_code = -1
                    if fatal_max_retries >= 0 and fatal_retry_count > fatal_max_retries:
                        _log_warn(f"pi 基础设施异常 [{fatal_retry_count}/{_fmt_max(fatal_max_retries)}] 达上限，终止重试: {exc}")
                        return r
                    _mark_infinite_retry(r, kind="fatal", count=fatal_retry_count, reason=str(exc))
                    _log_warn(f"pi 基础设施异常 [{fatal_retry_count}/{_fmt_max(fatal_max_retries)}], 30s 后重试: {exc}")
                    if on_stream:
                        on_stream("\n⚠️ 智能体基础设施异常，30 秒后自动重试...\n")
                    time.sleep(30)
                    break
            else:
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
                    import traceback
                    traceback.print_exc()
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
        import traceback
        traceback.print_exc()
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
    fatal_max_retries: int = -1,
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
    rate_limit_streak = 0
    query_engine_401_failures = 0
    key_auth_failures = 0
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
                # 轮询：等取消事件 OR 调用结束(cancel_stop)，任一触发即退出。
                # 不能直接 cancel_event.wait() 永久阻塞——finally 只 set cancel_stop
                # 不 set cancel_event，否则 _cancel_monitor 永不退出 → 线程泄漏。
                while not cancel_stop.is_set():
                    if cancel_event.wait(timeout=0.5):
                        break
                if cancel_event.is_set() and not cancel_stop.is_set():
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
                import traceback
                traceback.print_exc()
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
            # Close all pipes to prevent fd leak (each pipe = 2 fds in parent)
            for pipe in (proc.stdin, proc.stdout, proc.stderr):
                try:
                    if pipe:
                        pipe.close()
                except Exception:
                    pass

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

        # API key 认证错误（wsk/sk 无效/401/unauthorized）：重试 3 次后判 fatal 退出
        if _is_key_auth_error(result):
            key_auth_failures += 1
            if key_auth_failures <= _KEY_AUTH_MAX_RETRIES:
                delay = _backoff(retry_delay, key_auth_failures)
                label = f"{key_auth_failures}/{_KEY_AUTH_MAX_RETRIES}"
                _log_warn(
                    f"API key 认证失败 [{label}], {delay:.0f}s 后重试: "
                    f"{(result.error or '')[:200]}"
                )
                if on_stream:
                    on_stream(
                        f"\n⚠️ API key 认证失败，{delay:.0f}s 后重试 ({label})...\n"
                    )
                if _session_has_assistant_content(session_file):
                    effective_prompt = "继续完成上次未完成的任务。"
                time.sleep(delay)
                continue
            _log_error(
                f"API key 认证连续失败 {key_auth_failures} 次，任务终止: "
                f"{(result.error or '')[:200]}"
            )
            result.fatal = True
            result.error = (
                (result.error or "")
                + f" [API key 认证连续失败 {key_auth_failures} 次，已达上限，任务终止]"
            )
            return result

        # 无可用模型/部署不可用（网关 cooldown、no deployments available）：持续性错误，直接失败不重试
        if _is_no_model_available_error(result):
            result.fatal = True
            result.error = (
                (result.error or "")
                + " [无可用模型/部署不可用，直接失败不重试]"
            )
            _log_error(f"无可用模型，任务直接失败: {(result.error or '')[:200]}")
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
            err_lower = (result.error or "").lower()
            is_rate_limit = any(p in err_lower for p in _RATE_LIMIT_PATTERNS)
            if is_rate_limit:
                rate_limit_streak += 1
                delay = _RATE_LIMIT_EXTRA_DELAY
                result.rate_limited = True
                result.consecutive_rate_limit_count = rate_limit_streak
                result.retry_delay_seconds = int(delay)
                result.rate_limit_event_due = _should_emit_rate_limit_event(rate_limit_streak)
                _log_warn(
                    f"限流错误 [streak={rate_limit_streak}], {delay:.0f}s 后重试: "
                    f"{(result.error or '')[:200]}"
                )
                if on_stream:
                    on_stream(f"\n⚠️ 限流错误，{delay:.0f}s 后重试 (连续第 {rate_limit_streak} 次)...\n")
                if _session_has_assistant_content(session_file):
                    effective_prompt = "继续完成上次未完成的任务。"
                    _log_warn("session 已有内容，重试时发送「继续」而非重复完整 prompt")
                time.sleep(delay)
                continue
            rate_limit_streak = 0
            api_attempt += 1
            infinite_retry = True
            can_retry = True
            if can_retry:
                delay = _backoff(retry_delay, api_attempt)
                result.retry_delay_seconds = int(delay)
                result.consecutive_api_retry_count = int(api_attempt)
                result.api_retry_reason = str(result.error or "").strip()[:500] or None
                result.api_retry_event_due = _should_emit_api_retry_event(api_attempt, delay)
                label = f"{api_attempt}/{_fmt_max(-1 if infinite_retry else max_retries)}"
                _log_warn(
                    f"API错误 [{label}], {delay:.0f}s 后重试: "
                    f"{(result.error or '')[:200]}"
                )
                if on_stream:
                    on_stream(f"\n⚠️ API错误，{delay:.0f}s 后重试 ({label})...\n")
                if _session_has_assistant_content(session_file):
                    effective_prompt = "继续完成上次未完成的任务。"
                    _log_warn("session 已有内容，重试时发送「继续」而非重复完整 prompt")
                time.sleep(delay)
                continue
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
                import traceback
                traceback.print_exc()
                pass

    return results  # type: ignore[return-value]
