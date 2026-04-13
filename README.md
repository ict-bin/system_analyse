# system_analyse

基于多 Agent 协作的系统模块威胁分析系统。多个 Worker 并行分析同一模块，多个 Judge 独立评审，迭代优化直到通过。

## 架构

```
┌─────────────────────────────────────────────────────────┐
│                    Orchestrator                          │
│                                                         │
│  Round 1 (min_rounds=2, 即使通过也强制反思)              │
│  ┌───────────┐  ┌───────────┐                           │
│  │ Worker-0  │  │ Worker-1  │  ← 并行，独立工作目录      │
│  │ (session) │  │ (session) │  ← 保持上下文跨轮累积      │
│  └─────┬─────┘  └─────┬─────┘                           │
│        │ threat-*.md   │ threat-*.md                     │
│        ▼               ▼                                │
│  ┌─────────────────────────────────────────┐            │
│  │           文件交换层                      │            │
│  └─────────────────────────────────────────┘            │
│        │                     │                          │
│  ┌─────┴──────┐  ┌──────────┴─┐                         │
│  │  Judge-0   │  │  Judge-1   │  ← 并行，独立上下文      │
│  │ eval w-0   │  │ eval w-0   │                          │
│  │ eval w-1   │  │ eval w-1   │                          │
│  │ summary    │  │ summary    │                          │
│  └────────────┘  └────────────┘                         │
│        │                                                │
│  投票 → 反思 → 迭代                                      │
└─────────────────────────────────────────────────────────┘
```

### Worker 任务：威胁分析

- 基于 **STRIDE** 模型识别威胁（Spoofing/Tampering/Repudiation/InfoDisclosure/DoS/EoP）
- 分析攻击面：外部输入接口、内存安全、逻辑安全、信任边界
- 输出 `threat-<模块名>.md` 结构化威胁报告

### 关键设计

| 特性 | 说明 |
|------|------|
| Worker 并行 | 多个 Worker 同时分析，各自独立工作目录 |
| Worker 保持上下文 | `--session` 跨轮累积，反思轮能看到完整历史 |
| Judge 独立上下文 | 每次评审新起上下文，防止交叉影响 |
| Judge 读文件评审 | Worker 输出以文件传递，Judge 用 read 工具读取 |
| 最小轮数 | `min_rounds=2`：强制反思迭代 |
| 错误重试 | API 失败自动重试（可配置） |

## 快速开始

### 1. 配置文件

`config.json`（管理员配置一次）：

```json
{
    "max_rounds": 3,
    "min_rounds": 2,
    "pass_threshold": 2,
    "agent_max_retries": 100,
    "agent_retry_delay": 30,
    "workers": {
        "agents": [
            { "model": "vllm/zai-org/GLM-5" },
            { "model": "vllm/zai-org/GLM-5" }
        ]
    },
    "judges": {
        "agents": [
            { "model": "vllm/zai-org/GLM-5" },
            { "model": "vllm/zai-org/GLM-5" }
        ]
    }
}
```

### 2. 运行分析

```bash
docker run --rm --network host \
  -v /path/to/source:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  system_analyse \
  python3 cli.py "对 firmware.c 的 handle_packet 模块进行威胁分析"
```

### 3. 查看结果

```
output/
├── firmware_handle_packet.md          # 最终威胁分析报告
└── firmware_handle_packet_log.zip     # 完整过程归档
```

## 输出格式

最终输出为结构化威胁分析报告（`.md`），包含：

- **模块概述**：功能、信任边界
- **攻击面清单**：所有外部输入接口
- **威胁详情**：每个威胁的 STRIDE 分类、位置、触发条件、影响、风险等级
  - 🔴 高风险 — 可远程利用或导致代码执行
  - 🟡 中风险 — 需要特定条件触发
  - 🟢 低风险 — 影响有限
- **风险矩阵**：按等级汇总
- **关键发现摘要**：按优先级排列
- **修复建议**：针对每个威胁的具体修复方案

## 配置说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `max_rounds` | 3 | 最大迭代轮数 |
| `min_rounds` | 2 | 最少执行轮数（强制反思） |
| `pass_threshold` | `ceil(judges/2)` | 通过所需投票数 |
| `agent_max_retries` | 100 | API 错误最大重试次数 |
| `agent_retry_delay` | 30 | 首次重试等待秒数 |

## 挂载说明

| 容器路径 | 说明 | 模式 |
|----------|------|------|
| `/data/target` | 待分析的源代码 | 只读 |
| `/data/config` | 服务配置 | 只读 |
| `/data/output` | 分析结果输出 | 读写 |
