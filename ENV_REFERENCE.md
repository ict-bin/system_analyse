# 环境变量参考

## 挂载约定

| 容器路径 | 说明 | 权限 |
|---------|------|------|
| `/data/target` | 待分析的固件/源码目录 | 只读 |
| `/data/config` | 配置目录（`config.json` + `models.json`）| 只读 |
| `/data/output` | 分析结果输出目录 | 读写 |

## 环境变量

| 变量 | 说明 | 示例 |
|------|------|------|
| `GLM_API_KEY` | GLM/vLLM 服务 API 密钥 | `1234` |
| `MINIMAX_API_KEY` | MiniMax vLLM 服务 API 密钥 | `12345` |
| `OPENAI_API_KEY` | OpenAI API 密钥 | `sk-xxx` |

> 环境变量名由 `models.json` 中的 `"apiKey"` 字段指定（存的是变量名，不是密钥本身）。

---

## 配置文件

### `config/models.json` — 模型 Provider 配置

**命名规则**：`provider名` + `/` + `model id` = 实际发给 API 的模型名。

把 provider 命名为模型名的第一段，`provider/id` 拼起来正好就是原始模型名，
在 config.json 中引用时无需额外前缀。

**双模型示例（GLM-5 大模型 + MiniMax-M2.5 快模型）：**

```json
{
    "providers": {
        "zai-org": {
            "baseUrl": "http://your-vllm-host:8000/v1",
            "api": "openai-completions",
            "apiKey": "GLM_API_KEY",
            "compat": {
                "supportsDeveloperRole": false,
                "supportsReasoningEffort": false
            },
            "models": [
                {
                    "id": "GLM-5",
                    "name": "GLM-5",
                    "reasoning": false,
                    "input": ["text"],
                    "contextWindow": 128000,
                    "maxTokens": 8192
                }
            ]
        },
        "MiniMax": {
            "baseUrl": "http://your-vllm-host:8003/v1",
            "api": "openai-completions",
            "apiKey": "MINIMAX_API_KEY",
            "compat": {
                "supportsDeveloperRole": false,
                "supportsReasoningEffort": false
            },
            "models": [
                {
                    "id": "MiniMax-M2.5",
                    "name": "MiniMax-M2.5",
                    "reasoning": false,
                    "input": ["text"],
                    "contextWindow": 163804,
                    "maxTokens": 8192
                }
            ]
        }
    }
}
```

引用方式：
- `zai-org/GLM-5` → provider=`zai-org`, id=`GLM-5` → API 收到 `zai-org/GLM-5` ✅
- `MiniMax/MiniMax-M2.5` → provider=`MiniMax`, id=`MiniMax-M2.5` → API 收到 `MiniMax/MiniMax-M2.5` ✅

**其他关键字段**：
- `"apiKey": "GLM_API_KEY"` — 这里写**环境变量名**，不是密钥本身
- `"contextWindow"` — 根据实际模型调整，影响子 Worker 每批文件数

### `config/config.json` — 分析配置

完整字段说明见 [USAGE.md](USAGE.md)，关键字段：

```json
{
    "analyse_targets": ["binary"],
    "binary_arch": ["arm", "aarch64"],
    "parallel_modules": 2,
    "parallel_sub_workers": 2,
    "workers": {
        "agents": [{"model": "zai-org/GLM-5"}],
        "stage_models": {
            "explore":  "MiniMax/MiniMax-M2.5",
            "sub_read": "MiniMax/MiniMax-M2.5"
        }
    },
    "judges": {
        "agents": [{"model": "zai-org/GLM-5"}],
        "stage_models": {
            "classify":     "MiniMax/MiniMax-M2.5",
            "refine":       "MiniMax/MiniMax-M2.5",
            "completeness": "MiniMax/MiniMax-M2.5"
        }
    }
}
```

---

## 典型部署示例

### 双模型：快/慢分工（推荐）

```bash
docker run -d --name system_analyse --network host \
  -v /firmware/extracted:/data/target:ro \
  -v ~/config:/data/config:ro \
  -v ~/output:/data/output \
  -e GLM_API_KEY=1234 \
  -e MINIMAX_API_KEY=12345 \
  system_analyse \
  python3 cli.py "对解包后的固件进行系统模块分类和安全威胁分析"
```

### 单模型（全用 GLM-5）

```json
{
    "workers": {"agents": [{"model": "zai-org/GLM-5"}]},
    "judges":  {"agents": [{"model": "zai-org/GLM-5"}]}
}
```

### 测试反思逻辑（min_rounds=2）

```json
{
    "stages": {
        "classify":    {"min_rounds": 2, "max_rounds": -1, "pass_mode": "all"},
        "refine":      {"min_rounds": 2, "max_rounds": -1, "pass_mode": "all"}
    }
}
```

> **注意**：`min_rounds=2` 会强制每个模块至少运行两轮，生产环境请使用 `min_rounds=1`。
