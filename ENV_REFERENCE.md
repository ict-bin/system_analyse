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
| `GAIASEC_API_KEY` | GaiaSec 平台 API 密钥 | `sk-xxx` |
| `OPENAI_API_KEY` | OpenAI API 密钥 | `sk-xxx` |

> 环境变量名由 `models.json` 中的 `"apiKey"` 字段指定（存的是变量名，不是密钥本身）。

---

## 配置文件

### `config/models.json` — 模型 Provider 配置

**本地 vLLM（GLM-5）：**

```json
{
    "providers": {
        "glm": {
            "baseUrl": "http://your-vllm-host:8000/v1",
            "api": "openai-completions",
            "apiKey": "GLM_API_KEY",
            "compat": {
                "supportsDeveloperRole": false,
                "supportsReasoningEffort": false
            },
            "models": [
                {
                    "id": "vllm/zai-org/GLM-5",
                    "name": "GLM-5",
                    "reasoning": false,
                    "input": ["text"],
                    "contextWindow": 128000,
                    "maxTokens": 8192
                }
            ]
        }
    }
}
```

**关键字段说明：**
- `"apiKey": "GLM_API_KEY"` — 这里写**环境变量名**，不是密钥本身
- `"id"` — 在 `config.json` 的 `agents[].model` 中引用此 ID
- `"contextWindow"` — 根据实际模型调整，影响子 Worker 每批文件数

### `config/config.json` — 分析配置

完整字段说明见 [USAGE.md](USAGE.md)，关键新增字段：

```json
{
    "analyse_targets": ["binary"],      // 文件类型过滤
    "binary_arch": ["arm", "aarch64"], // ELF 架构过滤（仅 binary 类型）
    "parallel_modules": 2,              // 模块间并行度
    "parallel_sub_workers": 2           // 模块内子Worker并行度
}
```

---

## 典型部署示例

### 分析 ARM 二进制（2×2 并行）

```bash
docker run -d --name system_analyse --network host \
  -v /firmware/extracted:/data/target:ro \
  -v ~/config:/data/config:ro \
  -v ~/output:/data/output \
  -e GLM_API_KEY=1234 \
  system_analyse \
  python3 cli.py "对解包后的固件进行系统模块分类和安全威胁分析"
```

`~/config/config.json`:
```json
{
    "analyse_targets": ["binary"],
    "binary_arch": ["arm", "aarch64"],
    "parallel_modules": 2,
    "parallel_sub_workers": 2,
    ...
}
```

### 分析全部文件（串行，保守）

```json
{
    "analyse_targets": ["all"],
    "parallel_modules": 1,
    "parallel_sub_workers": 1,
    ...
}
```

### 测试反思逻辑（min_rounds=2）

```json
{
    "stages": {
        "classify":    {"min_rounds": 2, "max_rounds": -1, "pass_mode": "all"},
        "refine":      {"min_rounds": 2, "max_rounds": -1, "pass_mode": "all"},
        ...
    }
}
```

> **注意**：`min_rounds=2` 会强制每个模块至少运行两轮，生产环境请使用 `min_rounds=1`。
