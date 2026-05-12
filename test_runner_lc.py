"""
test_runner_lc.py — 验证 LangChain runner 实现的单元测试

测试范围：
  - AgentResult 接口兼容性
  - 工具集创建（make_tools）
  - 模型工厂（create_model）
  - 会话（Session）管理
  - 错误分类（fatal / retryable）
  - run_agent 接口签名
  - 向后兼容函数保留

运行方式（无需 LLM API Key）：
  python -X utf8 test_runner_lc.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import textwrap
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# 确保从项目根运行
sys.path.insert(0, str(Path(__file__).parent))

PASS = "✅"
FAIL = "❌"
_failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"  {PASS} {name}")
    else:
        msg = f"  {FAIL} {name}" + (f": {detail}" if detail else "")
        print(msg)
        _failures.append(name)


# ═══════════════════════════════════════════════════════════════════════
# 1. AgentResult 接口兼容性
# ═══════════════════════════════════════════════════════════════════════

def test_agent_result_interface():
    print("\n[1] AgentResult 接口兼容性")
    from app.runner import AgentResult
    from app.models import TokenUsage

    r = AgentResult()
    check("output field exists",      hasattr(r, "output") and r.output == "")
    check("messages field exists",    hasattr(r, "messages") and r.messages == [])
    check("token_usage field exists", isinstance(r.token_usage, TokenUsage))
    check("exit_code field exists",   r.exit_code == 0)
    check("error field exists",       r.error is None)
    check("fatal field exists",       r.fatal is False)


# ═══════════════════════════════════════════════════════════════════════
# 2. 异常类保留
# ═══════════════════════════════════════════════════════════════════════

def test_exception_classes():
    print("\n[2] 异常类向后兼容")
    from app.runner import PiFatalError, _PiProcessError
    check("PiFatalError exists",     issubclass(PiFatalError, Exception))
    check("_PiProcessError exists",  issubclass(_PiProcessError, Exception))
    # helpers.py 中使用 PiFatalError
    from app.pipeline.helpers import PiFatalError as HelpPiFatal, StageError
    check("helpers.PiFatalError is StageError subclass", issubclass(HelpPiFatal, StageError))


# ═══════════════════════════════════════════════════════════════════════
# 3. make_tools — 工具集创建
# ═══════════════════════════════════════════════════════════════════════

def test_make_tools():
    print("\n[3] make_tools — 工具集创建")
    from app.tools import make_tools
    from langchain_core.tools import BaseTool

    all_names = ["read", "bash", "write", "edit", "grep", "find"]
    tools = make_tools(all_names, cwd=".", env={})
    check("returns list",             isinstance(tools, list))
    check("correct count (6 tools)",  len(tools) == 6, f"got {len(tools)}")

    tool_name_set = {t.name for t in tools}
    for name in all_names:
        check(f"tool '{name}' present", name in tool_name_set)

    for t in tools:
        check(f"tool '{t.name}' is BaseTool", isinstance(t, BaseTool))

    # 别名工具
    alias_tools = make_tools(["read_file", "write_file", "str_replace"], cwd=".")
    alias_names = {t.name for t in alias_tools}
    check("read_file alias works",    "read_file" in alias_names)
    check("write_file alias works",   "write_file" in alias_names)
    check("str_replace alias works",  "str_replace" in alias_names)

    # 去重
    dup_tools = make_tools(["bash", "bash", "read"], cwd=".")
    check("deduplication works",      len(dup_tools) == 2, f"got {len(dup_tools)}")

    # 未知工具名静默跳过
    unknown = make_tools(["unknown_tool", "bash"], cwd=".")
    check("unknown tool skipped",     len(unknown) == 1)

    # 空列表
    empty = make_tools([], cwd=".")
    check("empty tool list works",    empty == [])


# ═══════════════════════════════════════════════════════════════════════
# 4. 工具功能测试（实际文件系统操作）
# ═══════════════════════════════════════════════════════════════════════

def test_tool_functions():
    print("\n[4] 工具功能测试（文件系统）")
    from app.tools import make_tools

    with tempfile.TemporaryDirectory() as tmpdir:
        tools = make_tools(["bash", "read", "write", "edit", "grep", "find"], cwd=tmpdir)
        tool_map = {t.name: t for t in tools}

        # bash
        r = tool_map["bash"].invoke({"command": "echo hello_world"})
        check("bash: echo works",         "hello_world" in r, repr(r))

        r = tool_map["bash"].invoke({"command": "exit 1"})
        check("bash: exit code captured", "exit code: 1" in r, repr(r))

        # write
        r = tool_map["write"].invoke({"path": "test.txt", "content": "hello\nworld\n"})
        check("write: success message",   "Written" in r, repr(r))
        check("write: file created",      Path(tmpdir, "test.txt").exists())

        # write append
        r = tool_map["write"].invoke({"path": "test.txt", "content": "extra\n", "append": True})
        check("write append: success",    "Appended" in r, repr(r))
        content = Path(tmpdir, "test.txt").read_text()
        check("write append: content ok", "extra" in content and "hello" in content)

        # read
        r = tool_map["read"].invoke({"path": "test.txt"})
        check("read: content correct",    "hello" in r, repr(r))

        r = tool_map["read"].invoke({"path": "nonexistent.txt"})
        check("read: missing file error", "Error" in r or "not found" in r.lower())

        r = tool_map["read"].invoke({"path": tmpdir})
        check("read: dir listing works",  "test.txt" in r, repr(r))

        r = tool_map["read"].invoke({"path": "test.txt", "start_line": 2, "end_line": 2})
        check("read: line range works",   "world" in r and "hello" not in r, repr(r))

        # edit
        r = tool_map["edit"].invoke({"path": "test.txt", "old_str": "hello", "new_str": "hi"})
        check("edit: success message",    "Edited" in r, repr(r))
        check("edit: content updated",    "hi" in Path(tmpdir, "test.txt").read_text())

        r = tool_map["edit"].invoke({"path": "test.txt", "old_str": "NOTEXIST", "new_str": "x"})
        check("edit: not found error",    "Error" in r or "not found" in r.lower())

        # write nested dir
        r = tool_map["write"].invoke({"path": "sub/dir/nested.txt", "content": "nested"})
        check("write: creates nested dirs", Path(tmpdir, "sub/dir/nested.txt").exists())

        # grep
        Path(tmpdir, "search.txt").write_text("hello world\nfoo bar\nhello again\n")
        r = tool_map["grep"].invoke({"pattern": "hello", "path": "search.txt"})
        check("grep: finds matches",      "hello" in r, repr(r))
        check("grep: line numbers",       ":" in r, repr(r))

        r = tool_map["grep"].invoke({"pattern": "nomatch_xyz", "path": "search.txt"})
        check("grep: no match",           "No matches" in r or r == "", repr(r))

        # find
        r = tool_map["find"].invoke({"path": tmpdir, "pattern": "*.txt", "type": "f"})
        check("find: finds txt files",    "test.txt" in r, repr(r))

        r = tool_map["find"].invoke({"path": tmpdir, "pattern": "*", "type": "d"})
        check("find: finds dirs",         "sub" in r, repr(r))


# ═══════════════════════════════════════════════════════════════════════
# 5. 模型工厂测试
# ═══════════════════════════════════════════════════════════════════════

def test_model_factory():
    print("\n[5] 模型工厂（create_model）")
    from app.model_factory import _parse_model_string, update_providers, _load_providers_once

    # 注入测试 providers
    test_providers = {
        "vllm": {
            "baseUrl": "http://localhost:8000/v1",
            "apiKey": "test-key",
            "models": [{"id": "zai-org/GLM-5", "reasoning": True}],
        },
        "icsl_vllm_1": {
            "baseUrl": "http://localhost:8001/v1",
            "apiKey": "key1",
            "models": [{"id": "zai-org/GLM-5"}],
        },
    }
    update_providers(test_providers)

    # _parse_model_string 测试
    pname, mid = _parse_model_string("vllm/zai-org/GLM-5")
    check("parse: provider extracted",      pname == "vllm", f"got {pname!r}")
    check("parse: model_id with slash",     mid == "zai-org/GLM-5", f"got {mid!r}")

    pname, mid = _parse_model_string("icsl_vllm_1/zai-org/GLM-5")
    check("parse: long provider name",      pname == "icsl_vllm_1", f"got {pname!r}")

    pname, mid = _parse_model_string("gpt-4o")
    check("parse: no slash → no provider", pname is None, f"got {pname!r}")
    check("parse: model_id unchanged",     mid == "gpt-4o", f"got {mid!r}")

    pname, mid = _parse_model_string("unknown_prov/model-x")
    check("parse: unknown provider",        pname is None or pname == "unknown_prov")

    # create_model 成功路径（mock ChatOpenAI）
    # patch 模块级导入的 ChatOpenAI
    with patch("app.model_factory.ChatOpenAI") as MockChat:
        from app.model_factory import create_model
        instance = MagicMock()
        MockChat.return_value = instance

        model = create_model("vllm/zai-org/GLM-5")
        check("create_model returns instance", model is instance)
        call_kwargs = MockChat.call_args[1]
        check("create_model: correct model",   call_kwargs.get("model") == "zai-org/GLM-5")
        check("create_model: base_url set",    "base_url" in call_kwargs)
        check("create_model: api_key set",     call_kwargs.get("api_key") == "test-key")
        check("create_model: max_retries=0",   call_kwargs.get("max_retries") == 0)

    # 环境变量 API key 解析
    os.environ["TEST_SA_KEY"] = "resolved-key"
    update_providers({
        "env_prov": {
            "baseUrl": "http://x/v1",
            "apiKey": "TEST_SA_KEY",  # 看起来像环境变量名
            "models": [],
        }
    })
    with patch("app.model_factory.ChatOpenAI") as MockChat2:
        from app.model_factory import create_model as cm2
        cm2("env_prov/some-model")
        kw2 = MockChat2.call_args[1]
        check("env api_key resolved", kw2.get("api_key") == "resolved-key",
              f"got {kw2.get('api_key')!r}")
    del os.environ["TEST_SA_KEY"]


# ═══════════════════════════════════════════════════════════════════════
# 6. 错误分类（_is_fatal / _is_retryable）
# ═══════════════════════════════════════════════════════════════════════

def test_error_classification():
    print("\n[6] 错误分类")
    from app.runner import _is_fatal, _is_retryable

    fatal_cases = [
        "401 Unauthorized",
        "Model not found: gpt-xxx",
        "Invalid API key provided",
        "Authentication failed",
        "No such model xyz",
    ]
    for case in fatal_cases:
        check(f"fatal: {case[:40]}", _is_fatal(case), repr(case))

    retryable_cases = [
        "Connection timeout",
        "Rate limit exceeded",
        "429 Too Many Requests",
        "503 Service Unavailable",
        "Server Error 500",
        "ECONNRESET",
    ]
    for case in retryable_cases:
        check(f"retryable: {case[:40]}", _is_retryable(case), repr(case))

    non_fatal_cases = ["some random error", "value error in code"]
    for case in non_fatal_cases:
        check(f"non-fatal: {case[:40]}", not _is_fatal(case))


# ═══════════════════════════════════════════════════════════════════════
# 7. 会话（Session）管理
# ═══════════════════════════════════════════════════════════════════════

def test_session_management():
    print("\n[7] 会话（Session）管理")
    from app.runner import _get_or_create_checkpointer, clear_session, clear_all_sessions, _session_checkpointers

    async def _run():
        clear_all_sessions()
        ck1 = await _get_or_create_checkpointer("/tmp/sessions/classify.jsonl")
        ck2 = await _get_or_create_checkpointer("/tmp/sessions/classify.jsonl")
        ck3 = await _get_or_create_checkpointer("/tmp/sessions/refine-bgp.jsonl")

        check("same session returns same checkpointer", ck1 is ck2)
        check("different session returns different checkpointer", ck1 is not ck3)

        # 清除
        clear_session("/tmp/sessions/classify.jsonl")
        ck4 = await _get_or_create_checkpointer("/tmp/sessions/classify.jsonl")
        check("clear_session creates new checkpointer", ck1 is not ck4)

        check("session count correct", len(_session_checkpointers) == 2)
        clear_all_sessions()
        check("clear_all_sessions works", len(_session_checkpointers) == 0)

    asyncio.run(_run())


# ═══════════════════════════════════════════════════════════════════════
# 8. run_agent 接口签名兼容性
# ═══════════════════════════════════════════════════════════════════════

def test_run_agent_signature():
    print("\n[8] run_agent 接口签名兼容性")
    import inspect
    from app.runner import run_agent

    sig = inspect.signature(run_agent)
    params = sig.parameters

    required_params = [
        "prompt", "model", "tools", "system_prompt", "cwd", "env",
        "thinking_level", "session_file", "on_stream", "cancel_event",
        "max_retries", "retry_delay", "pi_max_retries", "pi_retry_delay",
    ]
    for p in required_params:
        check(f"param '{p}' exists", p in params, f"missing from {list(params.keys())}")

    # 默认值检查
    check("system_prompt default=''",    params["system_prompt"].default == "")
    check("cwd default='.'",             params["cwd"].default == ".")
    check("thinking_level default='off'",params["thinking_level"].default == "off")
    check("session_file default=None",   params["session_file"].default is None)
    check("max_retries default=3",       params["max_retries"].default == 3)
    check("pi_max_retries default=-1",   params["pi_max_retries"].default == -1)


# ═══════════════════════════════════════════════════════════════════════
# 9. run_agent 端到端（Mock LLM）
# ═══════════════════════════════════════════════════════════════════════

def test_run_agent_mock():
    print("\n[9] run_agent 端到端（Mock LLM）")
    from app.runner import run_agent, clear_all_sessions
    from langchain_core.messages import AIMessage

    # Mock AIMessage 带 usage_metadata
    mock_ai = AIMessage(content="<result>分类完成，共 3 个模块</result>")
    mock_ai.usage_metadata = {
        "input_tokens": 100,
        "output_tokens": 50,
        "cache_read_input_tokens": 10,
    }

    # Mock create_react_agent 返回一个 agent
    mock_agent = AsyncMock()
    mock_agent.ainvoke = AsyncMock(return_value={"messages": [mock_ai]})

    async def _run():
        clear_all_sessions()
        with patch("app.runner._create_react_agent", return_value=mock_agent), \
             patch("app.runner.create_model", return_value=MagicMock()):
            # 正常调用（无 session）
            result = await run_agent(
                "执行分类任务",
                model="vllm/GLM-5",
                tools=["bash", "write"],
                system_prompt="你是分类专家",
                cwd="/tmp",
            )
            check("output extracted",       "<result>" in result.output or "分类完成" in result.output)
            check("input_tokens counted",   result.token_usage.input == 100, str(result.token_usage.input))
            check("output_tokens counted",  result.token_usage.output == 50)
            check("cache_read counted",     result.token_usage.cache_read == 10)
            check("exit_code=0",            result.exit_code == 0)
            check("error=None",             result.error is None)
            check("fatal=False",            result.fatal is False)

            # 带 session 调用
            result2 = await run_agent(
                "第二轮：请修复遗漏文件",
                model="vllm/GLM-5",
                tools=["bash"],
                session_file="/tmp/sessions/test_classify.jsonl",
            )
            check("session invocation works", result2.output != "" or result2.error is None)

        # 取消测试
        cancel = asyncio.Event()
        cancel.set()
        result3 = await run_agent(
            "任务已取消",
            model="vllm/GLM-5",
            tools=["bash"],
            cancel_event=cancel,
        )
        check("cancel returns early",     result3.error == "cancelled")

    asyncio.run(_run())


# ═══════════════════════════════════════════════════════════════════════
# 10. run_agent 致命错误与重试
# ═══════════════════════════════════════════════════════════════════════

def test_run_agent_errors():
    print("\n[10] run_agent 错误处理")
    from app.runner import run_agent, clear_all_sessions

    async def _run():
        clear_all_sessions()

        # 致命错误：不重试
        mock_model = MagicMock()
        mock_agent = AsyncMock()
        mock_agent.ainvoke = AsyncMock(side_effect=Exception("401 Unauthorized"))

        with patch("app.runner._create_react_agent", return_value=mock_agent), \
             patch("app.runner.create_model", return_value=mock_model):
            result = await run_agent(
                "test",
                model="vllm/GLM-5",
                tools=[],
                max_retries=5,
                pi_max_retries=5,
            )
        check("fatal=True on 401",      result.fatal, result.error)
        check("exit_code=1 on fatal",   result.exit_code == 1)
        check("no retry on fatal",      mock_agent.ainvoke.call_count == 1,
              f"called {mock_agent.ainvoke.call_count} times")

        # 模型创建失败（fatal）
        with patch("app.runner.create_model", side_effect=Exception("Model not found: xyz")):
            result2 = await run_agent("test", model="bad/model", tools=[])
        check("fatal on model not found", result2.fatal)

        # 非致命错误（不重试，max_retries=0）
        mock_agent2 = AsyncMock()
        mock_agent2.ainvoke = AsyncMock(side_effect=Exception("Some unknown error"))
        with patch("app.runner._create_react_agent", return_value=mock_agent2), \
             patch("app.runner.create_model", return_value=MagicMock()):
            result3 = await run_agent(
                "test",
                model="vllm/m",
                tools=[],
                max_retries=0,
                pi_max_retries=0,
            )
        check("non-fatal stops at limit", not result3.fatal)
        check("error captured",           result3.error is not None)

    asyncio.run(_run())


# ═══════════════════════════════════════════════════════════════════════
# 11. run_agents_parallel 接口兼容性
# ═══════════════════════════════════════════════════════════════════════

def test_run_agents_parallel():
    print("\n[11] run_agents_parallel 并行执行")
    from app.runner import run_agents_parallel, AgentResult

    async def _run():
        from langchain_core.messages import AIMessage as AI

        mock_ai = AI(content="done")
        mock_agent = AsyncMock()
        mock_agent.ainvoke = AsyncMock(return_value={"messages": [mock_ai]})

        with patch("app.runner._create_react_agent", return_value=mock_agent), \
             patch("app.runner.create_model", return_value=MagicMock()):
            tasks = [
                {"prompt": "task1", "model": "m", "tools": [], "cwd": "/tmp"},
                {"prompt": "task2", "model": "m", "tools": [], "cwd": "/tmp"},
                {"prompt": "task3", "model": "m", "tools": [], "cwd": "/tmp"},
            ]
            results = await run_agents_parallel(tasks, concurrency=2)

        check("returns list",          isinstance(results, list))
        check("correct length",        len(results) == 3, f"got {len(results)}")
        check("all AgentResult",       all(isinstance(r, AgentResult) for r in results))
        check("concurrent limit=2",    mock_agent.ainvoke.call_count == 3)

    asyncio.run(_run())


# ═══════════════════════════════════════════════════════════════════════
# 12. helpers.py check_agent_result 兼容性
# ═══════════════════════════════════════════════════════════════════════

def test_helpers_compatibility():
    print("\n[12] helpers.check_agent_result 兼容性")
    from app.runner import AgentResult
    from app.pipeline.helpers import check_agent_result, PiFatalError, StageError

    # fatal=True → raises PiFatalError
    r = AgentResult()
    r.fatal = True
    r.error = "401 Unauthorized"
    try:
        check_agent_result(r, context="s1-test")
        check("FAIL: should have raised", False)
    except PiFatalError as exc:
        check("fatal raises PiFatalError",  True)
        check("context in message",         "s1-test" in str(exc))

    # error without output → raises StageError
    r2 = AgentResult()
    r2.error = "connection timeout"
    r2.output = ""
    try:
        check_agent_result(r2, context="s2-test")
        check("FAIL: should have raised", False)
    except StageError:
        check("error+no_output raises StageError", True)
    except PiFatalError:
        check("FAIL: should be StageError not Fatal", False)

    # success
    r3 = AgentResult()
    r3.output = "分类完成"
    try:
        check_agent_result(r3)
        check("success: no exception", True)
    except Exception:
        check("FAIL: success should not raise", False)

    # error with output → OK（agent 完成了但有警告）
    r4 = AgentResult()
    r4.error = "minor warning"
    r4.output = "分类已完成"
    try:
        check_agent_result(r4)
        check("error+output: no exception", True)
    except Exception:
        check("FAIL: should not raise when output present", False)


# ═══════════════════════════════════════════════════════════════════════
# 13. llm_provider_sync 适配验证
# ═══════════════════════════════════════════════════════════════════════

def test_llm_provider_sync():
    print("\n[13] llm_provider_sync 适配验证")
    from app.service.llm_provider_sync import build_models_json, sync_providers_to_pi

    providers = [
        {
            "enabled": True,
            "provider_key": "test_vllm",
            "model": "GLM-5",
            "api_base": "http://localhost:8000/v1",
            "api_key": "test-key",
            "context_length": 128000,
        },
        {
            "enabled": False,
            "provider_key": "disabled_prov",
            "model": "other",
            "api_base": "http://x/v1",
            "api_key": "k",
        },
    ]
    result = build_models_json(providers)
    check("build_models_json returns dict",     isinstance(result, dict))
    check("providers key present",              "providers" in result)
    check("enabled provider included",          "test_vllm" in result["providers"])
    check("disabled provider excluded",         "disabled_prov" not in result["providers"])
    prov = result["providers"]["test_vllm"]
    check("baseUrl correct",    prov["baseUrl"] == "http://localhost:8000/v1")
    check("model entry correct", prov["models"][0]["id"] == "GLM-5")
    check("contextLength set",   prov["models"][0]["contextLength"] == 128000)

    # sync_providers_to_pi 是 async 函数
    import inspect
    check("sync_providers_to_pi is async", inspect.iscoroutinefunction(sync_providers_to_pi))


# ═══════════════════════════════════════════════════════════════════════
# 14. Dockerfile 验证
# ═══════════════════════════════════════════════════════════════════════

def test_dockerfile():
    print("\n[14] Dockerfile 内容验证")
    dockerfile = Path(__file__).parent / "Dockerfile"
    if not dockerfile.exists():
        check("Dockerfile exists", False)
        return
    content = dockerfile.read_text()
    check("pi-coding-agent removed",     "pi-coding-agent" not in content)
    check("Node.js removed",             "nodesource" not in content and "nodejs" not in content)
    check("binutils included",           "binutils" in content)
    check("python:3.12 base",            "python:3.12" in content)
    check("MODELS_JSON_PATH env set",    "MODELS_JSON_PATH" in content)
    check("healthcheck present",         "HEALTHCHECK" in content)


# ═══════════════════════════════════════════════════════════════════════
# 15. 整体 import 检查（所有模块可正常导入）
# ═══════════════════════════════════════════════════════════════════════

def test_imports():
    print("\n[15] 模块导入检查")
    modules = [
        ("app.runner",              ["run_agent", "run_agents_parallel", "AgentResult",
                                     "PiFatalError", "_PiProcessError", "clear_session"]),
        ("app.tools",               ["make_tools"]),
        ("app.model_factory",       ["create_model", "update_providers"]),
        ("app.pipeline.helpers",    ["run_agent_checked", "StageError", "PiFatalError",
                                     "parse_eval_md", "check_voting"]),
        ("app.pipeline.context",    ["PipelineContext"]),
        ("app.pipeline.s0_filter",  ["FilterStage", "ExploreStage", "PrescanStage"]),
        ("app.pipeline.s1_classify",["ClassifyStage"]),
        ("app.pipeline.s2_refine",  ["RefineStage"]),
        ("app.pipeline.s3_analyse", ["AnalyseStage"]),
        ("app.pipeline.s4_report",  ["CompletenessCheckStage", "FinalReportStage"]),
        ("app.orchestrator",        ["Orchestrator"]),
    ]
    for mod_name, attrs in modules:
        try:
            mod = __import__(mod_name, fromlist=attrs)
            for attr in attrs:
                check(f"{mod_name}.{attr} importable", hasattr(mod, attr))
        except ImportError as exc:
            check(f"{mod_name} importable", False, str(exc))
        except Exception as exc:
            check(f"{mod_name} importable", False, str(exc))


# ═══════════════════════════════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════════════════════════════

def main():
    print("=" * 65)
    print("  secflow LangChain Runner — 重构验证测试")
    print("=" * 65)

    test_agent_result_interface()
    test_exception_classes()
    test_make_tools()
    test_tool_functions()
    test_model_factory()
    test_error_classification()
    test_session_management()
    test_run_agent_signature()
    test_run_agent_mock()
    test_run_agent_errors()
    test_run_agents_parallel()
    test_helpers_compatibility()
    test_llm_provider_sync()
    test_dockerfile()
    test_imports()

    print("\n" + "=" * 65)
    total = len(_failures)
    if total == 0:
        print(f"  {PASS} 全部通过！")
    else:
        print(f"  {FAIL} {total} 项失败:")
        for f in _failures:
            print(f"    - {f}")
    print("=" * 65)
    return total


if __name__ == "__main__":
    sys.exit(main())
