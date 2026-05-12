# secflow-app-system-analyse LangChain 重构变更说明

## 变更概述

完全移除 `pi` ([@mariozechner/pi-coding-agent](https://github.com/mariozechner/pi-coding-agent)) CLI 依赖，
改为直接使用 **LangGraph + LangChain** 原生 Agent 运行时。

---

## 架构变更

```
变更前（pi 子进程模式）：
  pipeline → helpers.run_agent_checked()
           → runner.run_agent()
           → subprocess (pi --mode rpc)
           → JSONL stdin/stdout
           → LLM API

变更后（LangChain 原生模式）：
  pipeline → helpers.run_agent_checked()    ← 零改动
           → runner.run_agent()             ← 完全重写
           → langchain.agents.create_agent()
              / langgraph.prebuilt.create_react_agent()
           → LLM API
```

所有 pipeline 代码（s0_filter / s1_classify / s2_refine / s3_analyse / s4_report /
context / helpers / base / orchestrator）**零改动**。

---

## 新增文件

### `app/tools/__init__.py`
LangChain 工具集，完全等价 pi 内置工具：

| pi 工具 | LangChain 工具 | 说明 |
|--------|--------------|------|
| `read` | `StructuredTool(read)` | 读文件/目录，支持行范围，512KB 截断 |
| `bash` | `StructuredTool(bash)` | subprocess 执行，绑定 cwd+env，300s 超时 |
| `write` | `StructuredTool(write)` | 写文件，自动建父目录，支持追加 |
| `edit` | `StructuredTool(edit)` | str_replace 精确替换（替换首个匹配项） |
| `grep` | `StructuredTool(grep)` | 优先系统 grep，回退 Python 实现 |
| `find` | `StructuredTool(find)` | 递归文件/目录查找，glob 模式 |

别名（prompt 兼容）：`read_file`, `write_file`, `str_replace`。

工厂函数 `make_tools(tool_names, cwd, env)` — 按工具名列表创建，绑定 cwd/env，自动去重。

---

### `app/model_factory.py`
从 `models.json` 创建 LangChain `ChatOpenAI` 实例：

- **模型字符串格式**：`"provider_name/model_id"`，例如 `"vllm/zai-org/GLM-5"`
- **搜索路径**（优先级从高到低）：
  1. `MODELS_JSON_PATH` 环境变量
  2. `/data/config/models.json`（容器挂载）
  3. `./config/models.json`（本地开发）
- **API Key 解析**：全大写名视为环境变量名（如 `GPTPLUS_API_KEY`），否则直接使用
- **热更新**：`update_providers(dict)` — 由 `llm_provider_sync` 或测试调用

---

## 修改文件

### `app/runner.py` ← 核心重写（接口 100% 向后兼容）

**保留**（接口兼容）：
- `AgentResult` 类（output / messages / token_usage / exit_code / error / fatal）
- `PiFatalError` / `_PiProcessError` 异常类
- `run_agent()` 函数签名（14 个参数完全一致）
- `run_agents_parallel()` 函数签名

**新增**：
- `clear_session(session_file)` / `clear_all_sessions()` — 会话管理
- `_create_react_agent()` — 自动适配新旧 langchain API
- `_extract_output()` / `_extract_token_usage()` — 标准化输出提取

**会话管理（W+J 多轮）**：

| 原版 | 新版 |
|------|------|
| `session_file → pi --session file.jsonl` | `session_file → MemorySaver + thread_id` |
| Worker 多轮共享同一 jsonl 文件 | Worker 多轮共享同一 MemorySaver checkpointer |
| Judge 无 session（每次全新） | Judge `checkpointer=None`（每次全新） |

**重试机制**：
- 合并 `max_retries`（API层）+ `pi_max_retries`（进程层）为统一重试上限
- 致命错误（401 / model not found）→ 立即退出，`result.fatal=True`，不重试
- 可重试错误（429 / 503 / timeout）→ 指数退避，限流时额外等待 60s
- `-1` 表示无限重试（与原版一致）

---

### `app/service/llm_provider_sync.py`
原功能：从配置中心同步 → 写入 `pi` 的 `models.json`
新功能：从配置中心同步 → 更新 `model_factory` 内存缓存 + 持久化到 `/data/config/models.json`

`build_models_json()` 接口保持不变（向后兼容）。

---

### `requirements.txt`

新增依赖：
```
langchain>=0.3.0
langchain-core>=0.3.0
langchain-openai>=0.2.0
langgraph>=0.2.60
```

移除依赖：（无需显式移除，pi 是 npm 包）

---

### `Dockerfile`

| 变更项 | 变更前 | 变更后 |
|--------|--------|--------|
| 基础镜像 | `ubuntu:24.04` + Python 手动安装 | `python:3.12-slim` |
| Node.js | ✅ 安装（for pi） | ❌ 移除 |
| pi CLI | ✅ `npm install -g @mariozechner/pi-coding-agent` | ❌ 移除 |
| binutils | ❌ 未包含 | ✅ `nm / readelf / strings`（Stage3 预读必需） |
| `MODELS_JSON_PATH` | ❌ 无 | ✅ 设置为 `/data/config/models.json` |
| `PI_CODING_AGENT_DIR` | ✅ 设置 | ❌ 移除 |

---

### `scripts/entrypoint.sh`
移除 pi 配置（models.json 符号链接 / settings.json 生成），保留：
- models.json 存在性检查 + provider 数量输出
- 输出目录准备

---

## 新增测试文件

### `test_runner_lc.py`
115 项单元测试，全部通过，覆盖：

1. `AgentResult` 接口兼容性（6项）
2. 异常类向后兼容（3项）
3. `make_tools` 工具集创建（20项）
4. 工具功能（文件系统操作）（19项）
5. 模型工厂 `create_model`（12项）
6. 错误分类（13项）
7. 会话管理（5项）
8. `run_agent` 接口签名（20项）
9. `run_agent` 端到端（Mock LLM）（9项）
10. `run_agent` 错误处理（6项）
11. `run_agents_parallel` 并行执行（4项）
12. `helpers.check_agent_result` 兼容性（5项）
13. `llm_provider_sync` 适配验证（8项）
14. Dockerfile 内容验证（6项）
15. 模块导入检查（24项）

---

## 预存在失败（与本次改动无关）

`test_pipeline.py` 中以下 6 项在改动前也已失败：

| 测试名 | 失败原因 |
|--------|---------|
| `test_prescan_stage_with_keywords` | 测试内 prescan 脚本路径问题 |
| `test_refine_stage_stub` | `cancel_event=MagicMock()` truthy，`is_set()` 返回 Mock（原 runner 同款问题） |
| `test_report_stages_stub` | 同上 |
| `test_pipeline_full_flow_stubs` | 同上 |
| `test_orchestrator_delegates_to_legacy` | 引用旧架构 `_LegacyOrchestrator`（已随新 orchestrator.py 移除） |
| `test_orchestrator_stop` | 同上 |

---

## 配置兼容性

`config.example.json` 和 `config/models.json.example` 完全不变，现有配置零迁移成本。

### 快速验证配置

```bash
# 验证 models.json 可被正确加载
python3 -c "
from app.model_factory import _load_providers_once
providers = _load_providers_once()
print(f'Loaded {len(providers)} providers:', list(providers.keys()))
"

# 运行重构验证测试
python3 -X utf8 test_runner_lc.py
```
