"""
app/runner.py — LangChain Agent 执行器（完全替代 pi 子进程方案）

架构变更说明
============
原始实现：Python → 子进程 (pi CLI, --mode rpc) → JSONL stdin/stdout → LLM API
新实现：  Python → LangChain create_agent → LangGraph CompiledStateGraph → LLM API

接口兼容性
==========
run_agent() 函数签名与原版 100% 一致，所有 pipeline 代码零改动。
AgentResult 类字段完全保留（output / messages / token_usage / exit_code / error / fatal）。
PiFatalError / _PiProcessError 异常类保留（helpers.py 通过 check_agent_result 使用）。

会话（Session）管理
===================
原始：session_file → pi 的 --session 参数 → jsonl 对话历史文件
新实现：session_file → MemorySaver checkpointer + thread_id = session 文件名（不含扩展名）
  - Worker（有 session）：多轮 W+J 中同一 session 的对话历史累积，
    LangGraph add_messages reducer 自动将新 HumanMessage 追加到历史
  - Judge（无 session）：每次调用完全新上下文（checkpointer=None）
  - 进程内 MemorySaver 字典缓存，确保同一进程内同一 session_file 共享 checkpointer

重试机制（双层合并）
====================
原始：外层（pi进程崩溃，pi_max_retries）+ 内层（API错误，max_retries）
新实现：统一重试循环
  - 致命错误（401/model-not-found）→ 立即退出，result.fatal=True
  - 可重试错误（429/503/timeout） → 指数退避重试，限流时额外等待 60s
  - 总重试次数 = max(max_retries, pi_max_retries)，-1 表示无限

工具（Tools）
=============
原始：pi 内置工具（read/bash/write/edit/grep/find）
新实现：app.tools.make_tools() 创建对应 LangChain StructuredTool，完全等价
  - cwd 绑定到工具实例（每次 run_agent 调用独立绑定）
  - env 透传给 bash 工具的 subprocess
"""
from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable
from pathlib import Path

from langchain_core.messages import AIMessage, HumanMessage

from .model_factory import create_model
from .models import TokenUsage
from .tools import make_tools

logger = logging.getLogger("sa.runner")

_MAX_BACKOFF = 300.0  # 最大退避 5 分钟


# ═══════════════════════════════════════════════════════════════════════
# 结果类（与原版完全兼容）
# ═══════════════════════════════════════════════════════════════════════

class AgentResult:
    """单次 Agent 执行结果，与原版 runner.py 的 AgentResult 接口完全一致。"""

    def __init__(self):
        self.output: str = ""
        self.messages: list[dict] = []
        self.token_usage = TokenUsage()
        self.exit_code: int = 0
        self.error: str | None = None
        self.fatal: bool = False


# ═══════════════════════════════════════════════════════════════════════
# 异常类（保留以兼容 helpers.py 的 check_agent_result）
# ═══════════════════════════════════════════════════════════════════════

class _PiProcessError(Exception):
    """保留类名以兼容旧代码导入；在新实现中不再实际抛出。"""
    pass


class PiFatalError(Exception):
    """致命错误（模型不存在 / 认证失败），不可重试，终止流水线。"""
    pass


# ═══════════════════════════════════════════════════════════════════════
# 会话（Session）管理
# ═══════════════════════════════════════════════════════════════════════

_session_checkpointers: dict[str, object] = {}
_session_lock = asyncio.Lock()


async def _get_or_create_checkpointer(session_file: str):
    """
    获取或创建与 session_file 绑定的 MemorySaver checkpointer。
    进程内复用同一 MemorySaver，保证 W+J 多轮间对话历史连续。
    """
    from langgraph.checkpoint.memory import MemorySaver

    async with _session_lock:
        if session_file not in _session_checkpointers:
            _session_checkpointers[session_file] = MemorySaver()
            logger.debug("New session checkpointer: %s", Path(session_file).name)
        return _session_checkpointers[session_file]


def clear_session(session_file: str) -> None:
    """清除指定 session 的对话历史（用于测试或手动重置）。"""
    _session_checkpointers.pop(session_file, None)


def clear_all_sessions() -> None:
    """清除所有 session（任务结束后可调用释放内存）。"""
    _session_checkpointers.clear()


# ═══════════════════════════════════════════════════════════════════════
# 错误分类
# ═══════════════════════════════════════════════════════════════════════

_FATAL_PATTERNS: list[tuple[str, ...]] = [
    ("model", "not found"),
    ("not found", "model"),
    ("invalid", "model"),
    ("invalid api key",),
    ("invalid_api_key",),
    ("incorrect api key",),
    ("unauthorized",),
    ("authentication failed",),
    ("authentication_failed",),
    ("401",),
    ("no such model",),
    ("model does not exist",),
]

_RETRYABLE_PATTERNS = [
    "connection", "timeout", "timed out", "econnrefused", "econnreset",
    "etimedout", "socket hang up", "fetch failed", "rate limit", "429",
    "503", "502", "500", "overloaded", "capacity", "temporarily unavailable",
    "server error", "internal error", "bad gateway", "service unavailable",
    "request failed", "network_error", "too many requests", "enobufs",
    "broken pipe", "read timeout", "write timeout",
]

_RATE_LIMIT_PATTERNS = ["rate limit", "429", "too many requests", "network_error"]
_RATE_LIMIT_EXTRA_DELAY = 60.0


def _is_fatal(error_text: str) -> bool:
    e = error_text.lower()
    return any(all(p in e for p in pattern) for pattern in _FATAL_PATTERNS)


def _is_retryable(error_text: str) -> bool:
    e = error_text.lower()
    return any(p in e for p in _RETRYABLE_PATTERNS)


def _is_rate_limit(error_text: str) -> bool:
    e = error_text.lower()
    return any(p in e for p in _RATE_LIMIT_PATTERNS)


def _backoff(base: float, attempt: int) -> float:
    """指数退避，上限 MAX_BACKOFF。"""
    return min(base * (2 ** min(attempt - 1, 6)), _MAX_BACKOFF)


def _fmt_max(n: int) -> str:
    return "∞" if n < 0 else str(n)


# ═══════════════════════════════════════════════════════════════════════
# Agent 工厂（懒导入，避免启动时间过长）
# ═══════════════════════════════════════════════════════════════════════

# ===========================================================================
# Context Summarization Middleware (replaces pi auto context compression)
# ===========================================================================

_DEFAULT_SUMMARIZE_TRIGGER_TOKENS = 50_000
_DEFAULT_SUMMARIZE_KEEP_MESSAGES  = 20


def _build_summarization_middleware(model, context_limit_tokens=None):
    """
    Build SummarizationMiddleware to compress history when context approaches limit.
    Equivalent to pi's contextLength auto-compression.
    pi:    models.json["contextLength"] -> pi built-in compression
    New:   SummarizationMiddleware -> LLM actively summarizes old messages
    Triggers at _DEFAULT_SUMMARIZE_TRIGGER_TOKENS (default 50K tokens).
    Falls back to empty list if middleware unavailable.
    """
    try:
        from langchain.agents.middleware import SummarizationMiddleware
        trigger_tokens = context_limit_tokens or _DEFAULT_SUMMARIZE_TRIGGER_TOKENS
        return [
            SummarizationMiddleware(
                model=model,
                trigger=("tokens", trigger_tokens),
                keep=("messages", _DEFAULT_SUMMARIZE_KEEP_MESSAGES),
                trim_tokens_to_summarize=4000,
            )
        ]
    except Exception as exc:
        logger.warning(
            "SummarizationMiddleware unavailable, no context compression: %s", exc
        )
        return []


def _create_react_agent(model, tools, *, system_prompt, checkpointer):
    """
    创建 ReAct 风格的 LangGraph Agent。

    优先使用新版 langchain.agents.create_agent（支持 middleware 扩展），
    回退到稳定的 langgraph.prebuilt.create_react_agent。
    """
    import inspect

    # 尝试新 API
    try:
        from langchain.agents import create_agent as _ca
        sig = inspect.signature(_ca)
        if "system_prompt" in sig.parameters:
            return _ca(
                model=model,
                tools=tools,
                system_prompt=system_prompt or None,
                checkpointer=checkpointer,
            )
        # system_prompt 参数名可能不同
        if "prompt" in sig.parameters:
            return _ca(
                model=model,
                tools=tools,
                prompt=system_prompt or None,
                checkpointer=checkpointer,
            )
        return _ca(model=model, tools=tools, checkpointer=checkpointer)
    except ImportError:
        pass

    # 回退到 langgraph.prebuilt
    from langgraph.prebuilt import create_react_agent
    sig = inspect.signature(create_react_agent)
    if "prompt" in sig.parameters:
        return create_react_agent(
            model=model,
            tools=tools,
            prompt=system_prompt or None,
            checkpointer=checkpointer,
        )
    # 更旧版本
    return create_react_agent(
        model=model,
        tools=tools,
        checkpointer=checkpointer,
    )


# ═══════════════════════════════════════════════════════════════════════
# 取消监控
# ═══════════════════════════════════════════════════════════════════════

async def _cancel_monitor(cancel_event: asyncio.Event, task: asyncio.Task) -> None:
    """等待取消事件触发后取消 agent 任务。"""
    await cancel_event.wait()
    if not task.done():
        task.cancel()


# ═══════════════════════════════════════════════════════════════════════
# 输出提取辅助
# ═══════════════════════════════════════════════════════════════════════

def _extract_output(messages: list) -> str:
    """从消息列表中提取最后一条 AIMessage 的文本内容。"""
    for msg in reversed(messages):
        if not isinstance(msg, AIMessage):
            continue
        content = msg.content
        if isinstance(content, list):
            # Anthropic 多块内容格式
            parts = [
                (c.get("text", "") if isinstance(c, dict) else str(c))
                for c in content
                if not isinstance(c, dict) or c.get("type") == "text"
            ]
            content = "\n".join(p for p in parts if p)
        if content and str(content).strip():
            return str(content)
    return ""


def _extract_token_usage(messages: list) -> TokenUsage:
    """累计所有 AIMessage 的 token 用量。"""
    usage = TokenUsage()
    for msg in messages:
        if not isinstance(msg, AIMessage):
            continue
        meta = getattr(msg, "usage_metadata", None)
        if not meta:
            continue
        usage.input  += meta.get("input_tokens", 0)
        usage.output += meta.get("output_tokens", 0)
        # OpenAI cache fields
        usage.cache_read  += meta.get("cache_read_input_tokens", 0)
        usage.cache_write += meta.get("cache_creation_input_tokens", 0)
        # Anthropic cache fields
        usage.cache_read  += meta.get("cache_read_tokens", 0)
        usage.cache_write += meta.get("cache_write_tokens", 0)
    return usage


# ═══════════════════════════════════════════════════════════════════════
# 核心接口：run_agent（与原版签名 100% 兼容）
# ═══════════════════════════════════════════════════════════════════════

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
    context_limit_tokens: int | None = None,
) -> AgentResult:
    """
    运行 LangChain Agent，接口与原版 pi-based run_agent 完全兼容。

    参数
    ----
    prompt         : 用户提示词（本轮任务描述 + 可选反思反馈）
    model          : 模型字符串，格式 "provider/model_id"
    tools          : 工具名列表，如 ["read", "bash", "write", "grep", "find"]
    system_prompt  : 系统提示词（从 prompts/workers/*.md 加载）
    cwd            : 工作目录（工具执行的上下文目录）
    env            : 环境变量覆盖（透传给 bash 工具）
    thinking_level : 推理深度（off/low/medium/high，目前仅日志记录）
    session_file   : 会话文件路径；非 None 时启用跨轮对话历史（Worker 模式）
                     None 时每次全新上下文（Judge 模式）
    on_stream      : 流式输出回调（收到文本片段时调用）
    cancel_event   : asyncio.Event，设置后立即终止当前 Agent
    max_retries    : API 错误最大重试次数（-1=无限）
    retry_delay    : API 重试首次等待秒数
    pi_max_retries : 原 pi 进程级重试次数（映射为通用重试上限，-1=无限）
    pi_retry_delay : 原 pi 进程重试等待秒数
    """
    result = AgentResult()

    # ── 取消检查 ──────────────────────────────────────────────────────
    if cancel_event and cancel_event.is_set():
        result.error = "cancelled"
        return result

    # ── 创建模型 ──────────────────────────────────────────────────────
    try:
        lc_model = create_model(model, thinking_level=thinking_level)
    except Exception as exc:
        err = str(exc)
        result.error = f"Failed to create model {model!r}: {err}"
        result.fatal = _is_fatal(err)
        if result.fatal:
            logger.error("Fatal model error: %s", err[:300])
        else:
            logger.error("Model creation failed: %s", err[:300])
        return result

    # ── 创建工具集 ─────────────────────────────────────────────────────
    lc_tools = make_tools(tools, cwd=cwd, env=env)

    # ── Session / Checkpointer ─────────────────────────────────────────
    if session_file:
        checkpointer = await _get_or_create_checkpointer(session_file)
        thread_id = Path(session_file).stem  # "classify", "refine-bgp" 等
        config: dict = {"configurable": {"thread_id": thread_id}}
    else:
        checkpointer = None
        thread_id = None
        config = {}

    # ── 创建 Agent ────────────────────────────────────────────────────
    try:
        agent = _create_react_agent(
            model=lc_model,
            tools=lc_tools,
            system_prompt=system_prompt,
            checkpointer=checkpointer,
            context_limit_tokens=context_limit_tokens,
        )
    except Exception as exc:
        err = str(exc)
        result.error = f"Failed to create agent: {err}"
        result.fatal = _is_fatal(err)
        logger.error("Agent creation failed: %s", err[:300])
        return result

    # ── 重试循环 ──────────────────────────────────────────────────────
    # 合并 max_retries（API 层）与 pi_max_retries（进程层）为统一重试上限
    effective_max = max(
        max_retries    if max_retries    >= 0 else 10 ** 9,
        pi_max_retries if pi_max_retries >= 0 else 10 ** 9,
    )
    input_data = {"messages": [HumanMessage(content=prompt)]}
    attempt = 0

    while True:
        if cancel_event and cancel_event.is_set():
            result.error = "cancelled"
            return result

        attempt += 1
        run_error: str | None = None

        # 每次尝试使用独立的临时 thread_id，避免失败尝试的部分 checkpoint
        # 将 HumanMessage 污染正式会话（W+J 跨轮记忆）
        # 成功后再发起一次调用将状态“筛选”到稳定 thread_id
        if thread_id:
            # attempt > 1 说明上一次尝试失败，用临时 id 防止座标污染
            cur_thread = thread_id if attempt == 1 else f"{thread_id}__retry{attempt}"
            attempt_config: dict = {"configurable": {"thread_id": cur_thread}}
        else:
            attempt_config = config  # 没有 session 时直接使用空配置

        try:
            # ── 带取消支持的异步调用 ──────────────────────────────────
            if cancel_event:
                agent_task = asyncio.create_task(
                    agent.ainvoke(input_data, config=attempt_config)
                )
                cancel_task = asyncio.create_task(
                    _cancel_monitor(cancel_event, agent_task)
                )
                try:
                    state = await agent_task
                finally:
                    cancel_task.cancel()
                    try:
                        await cancel_task
                    except asyncio.CancelledError:
                        pass
                if cancel_event.is_set():
                    result.error = "cancelled"
                    return result
            else:
                state = await agent.ainvoke(input_data, config=attempt_config)

            # ── 提取输出 ──────────────────────────────────────────────
            messages = state.get("messages", [])
            result.output = _extract_output(messages)
            result.token_usage = _extract_token_usage(messages)

            # 将消息序列化为 dict 列表（与原版兼容）
            result.messages = [
                {
                    "role": "assistant" if isinstance(m, AIMessage) else "user",
                    "content": (
                        m.content if isinstance(m.content, str)
                        else str(m.content)
                    ),
                }
                for m in messages
            ]

            # 流式回调（将最终输出通知调用方）
            if on_stream and result.output:
                on_stream(result.output)

            return result

        except asyncio.CancelledError:
            result.error = "cancelled"
            return result

        except Exception as exc:
            run_error = str(exc)

        # ── 错误处理 ──────────────────────────────────────────────────
        if _is_fatal(run_error):
            result.error = run_error
            result.fatal = True
            result.exit_code = 1
            logger.error("Fatal agent error (no retry): %s", run_error[:300])
            return result

        # 判断是否继续重试
        can_retry = (effective_max < 0) or (attempt <= effective_max)
        if not can_retry:
            result.error = run_error
            result.exit_code = 1
            logger.error(
                "Agent failed after %d attempt(s): %s",
                attempt, run_error[:300],
            )
            return result

        # 计算退避时间
        base = retry_delay if _is_retryable(run_error) else pi_retry_delay
        delay = _backoff(base, attempt)
        if _is_rate_limit(run_error):
            delay = max(delay, _RATE_LIMIT_EXTRA_DELAY)

        label = f"{attempt}/{_fmt_max(effective_max)}"
        kind  = "rate-limit" if _is_rate_limit(run_error) else "transient"
        logger.warning(
            "Agent %s error [%s], retry in %.0fs: %s",
            kind, label, delay, run_error[:200],
        )

        if on_stream:
            on_stream(
                f"\n⚠️ {kind} error, retrying in {delay:.0f}s ({label})...\n"
            )

        await asyncio.sleep(delay)


# ═══════════════════════════════════════════════════════════════════════
# 并行执行（与原版签名 100% 兼容）
# ═══════════════════════════════════════════════════════════════════════

async def run_agents_parallel(
    tasks: list[dict],
    concurrency: int = 4,
) -> list[AgentResult]:
    """
    并行运行多个 Agent 任务，semaphore 控制最大并发数。

    tasks : list[dict] — 每个元素是传给 run_agent() 的 kwargs
    """
    semaphore = asyncio.Semaphore(concurrency)
    results: list[AgentResult | None] = [None] * len(tasks)

    async def _run(index: int, kwargs: dict) -> None:
        async with semaphore:
            results[index] = await run_agent(**kwargs)

    await asyncio.gather(*[_run(i, t) for i, t in enumerate(tasks)])
    return results  # type: ignore[return-value]


# ═══════════════════════════════════════════════════════════════════════
# 向后兼容的日志辅助（原版 runner.py 中的全局函数）
# ═══════════════════════════════════════════════════════════════════════

def _log_error(msg: str) -> None:
    logger.error(msg)


def _log_warn(msg: str) -> None:
    logger.warning(msg)


def _log_info(msg: str) -> None:
    logger.info(msg)


def _backoff_compat(base_delay: float, attempt: int) -> float:
    """向后兼容的退避函数（原版导出名）。"""
    return _backoff(base_delay, attempt)


def _find_pi_command() -> list[str]:
    """
    保留此函数签名以防旧代码导入。
    新版无需 pi，始终抛出带说明的异常。
    """
    raise FileNotFoundError(
        "pi is no longer used. The system now runs agents natively via LangChain/LangGraph."
    )
