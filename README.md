# system_analyse

基于 [pi-coding-agent](https://github.com/mariozechner/pi-coding-agent) 的容器化多 Agent 系统，对解包固件进行**模块分类 → 迭代细分 → STRIDE 威胁分析**，Worker 执行、Judge 评审，每一步强制反思，直到全部通过。

## 架构

```
┌──────────────────────────────────────────────────────────────┐
│                     四阶段流水线                                │
│                                                              │
│  Stage 1 ─ 全局分类                                          │
│  │  Worker: 编写脚本批量扫描 /data/target，按功能创建模块      │
│  │  Judge:  运行 check_classification.sh 验证文件覆盖率        │
│  │                                                           │
│  Stage 2 ─ 模块细分（per module）                             │
│  │  ≤5 文件: 自动跳过                                        │
│  │  >20 文件: 子 Worker 串行读取 → 文件摘要 → 主 Worker 决策  │
│  │  Judge:  脚本校验文件零丢失 + LLM 评审合理性                │
│  │                                                           │
│  Stage 3 ─ 模块分析（per module）                             │
│  │  Worker: 逐文件 read → 输出 module_report.md（STRIDE）     │
│  │  Judge:  验证覆盖率、准确性、分类合理性                      │
│  │  ↩ 若 Judge 标记 [需要重新分类] → 回 Stage 2 重做          │
│  │                                                           │
│  Stage 4 ─ 完整性检查 + 最终报告                               │
│  │  4a: Judge 运行 check_outputs.sh（缺失 → 补做 S2+S3）     │
│  │  4b: Worker 汇总 → final_report.md → Judge 评审           │
│  └───────────────────────────────────────────────────────────┘
│                                                              │
│  投票: majority（过半通过）或 all（全票通过），per-stage 配置  │
│  反思: 首次通过后带自查 prompt 重做，连续 min_rounds 次才放行  │
│  失败: max_rounds 仍未通过 → 终止，输出失败报告 + flag=0      │
└──────────────────────────────────────────────────────────────┘
```

### 核心设计

| 特性 | 说明 |
|------|------|
| **Session 累积** | Worker 使用 `--session` 跨轮保持上下文；Judge 每次全新上下文 |
| **Per-stage 循环** | 每个 stage 独立配置 `min_rounds`、`max_rounds`、`pass_mode` |
| **脚本化检查** | Stage 1/2/4 的 Judge 调用 bash 脚本做确定性验证，非纯 LLM 判断 |
| **主从模式** | Stage 2 大模块（>20文件）：子 Worker 串行读文件出摘要 → 主 Worker 基于摘要决策拆分 |
| **文件零丢失** | Stage 2 每次拆分后脚本校验文件数，丢失立即 0 分打回 |
| **双层重试** | pi 进程级重试 + API 级重试，独立计数独立退避，`-1` = 无限 |
| **致命错误检测** | Model not found / Unauthorized 等不可重试错误立即终止，不死循环 |
| **flag 文件** | 启动即写 `flag=0`，全流程通过后改 `flag=1`，便于脚本对接 |
| **失败报告** | 失败/错误时同样输出 `final_report.md`，记录失败原因和已完成进度 |
| **路径清洗** | 输出文件中 `/data/target/` 容器路径自动替换为相对路径 |

## 快速开始

### 1. 准备配置

```json
{
    "agent_max_retries": 100,
    "agent_retry_delay": 30,
    "pi_max_retries": -1,
    "pi_retry_delay": 10,
    "stages": {
        "classify":    {"min_rounds": 1, "max_rounds": -1, "pass_mode": "all"},
        "refine":      {"min_rounds": 1, "max_rounds": -1, "pass_mode": "all"},
        "analyse":     {"min_rounds": 1, "max_rounds": -1, "pass_mode": "all"},
        "final_check": {"min_rounds": 1, "max_rounds": -1, "pass_mode": "all"}
    },
    "workers": {
        "system_prompt_dir": "/opt/system_analyse/prompts/workers",
        "agents": [{ "model": "vllm/zai-org/GLM-5" }]
    },
    "judges": {
        "system_prompt_dir": "/opt/system_analyse/prompts/judges",
        "agents": [
            { "model": "vllm/zai-org/GLM-5" },
            { "model": "vllm/zai-org/GLM-5" }
        ]
    }
}
```

> 完整配置参考 [`config.example.json`](config.example.json)，模型配置参考 [`ENV_REFERENCE.md`](ENV_REFERENCE.md)

### 2. 构建镜像

```bash
docker build -t system_analyse .
```

### 3. 运行

```bash
docker run --rm --network host \
  -v /path/to/extracted_firmware:/data/target:ro \
  -v /path/to/config_dir:/data/config:ro \
  -v /path/to/output_dir:/data/output \
  -e GLM_API_KEY=1234 \
  system_analyse \
  python3 cli.py "对解包后的所有文件进行威胁分析与模块分析"
```

### 4. 查看结果

```
output/
├── flag                    # 0=失败, 1=成功（脚本对接用）
├── final_report.md         # 最终报告（失败时记录原因）
├── modules/                # 各模块目录
│   ├── bgp/
│   │   ├── files.list      # 文件列表（相对路径）
│   │   └── module_report.md
│   └── ...
└── archive.zip             # 完整归档
```

## CLI 输出示例

```
╔══════════════════════════════════════════════╗
║            system_analyse                    ║
╠══════════════════════════════════════════════╣
║  Workers: 1    Judges: 1                     ║
║  分类: min=1 max=-1  all                     ║
║  细分: min=1 max=-1  all                     ║
║  分析: min=1 max=-1  all                     ║
╚══════════════════════════════════════════════╝

────────────────────────────────────────────────
🚀 对解包后的固件脚本文件进行系统模块分类和安全威胁分析
────────────────────────────────────────────────

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  📌 分类    [0s]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    📂 14 个模块: clock_mgmt, database_mgmt, ... (+8)
  ✅ judge-0=100

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  📌 细分    [2m23s]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ▸ clock_mgmt
      ↳ 拆分 → clock_ko, model_pkg_mgmt
  ✅ judge-0=75  6m35s
  ▸ database_mgmt
  ✅ judge-0=75  2m59s

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  📌 分析    [25m10s]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ▸ bgp
  ✅ judge-0=92  3m15s
  ...

═══════════════════════════════════════════════
  ✅ PASSED    [1h23m]
═══════════════════════════════════════════════
  📄 /data/output/final_report.md
  📂 /data/output/modules
  📦 /data/output/archive.zip

  ⏱  1h23m    💰 $0.0000
```

## 配置参数

### Stage 循环控制

| 参数 | 默认 | 说明 |
|------|------|------|
| `min_rounds` | 2 | 连续通过次数（强制反思） |
| `max_rounds` | 5 | 最大迭代次数，`-1` = 无限 |
| `pass_mode` | `"majority"` | `"majority"` = 过半；`"all"` = 全票 |

### 重试参数

| 参数 | 默认 | 说明 |
|------|------|------|
| `agent_max_retries` | 100 | API 错误（连接/限流/500）重试上限，`-1` = 无限 |
| `agent_retry_delay` | 30 | API 重试首次等待秒数（指数退避，上限 300s） |
| `pi_max_retries` | -1 | pi 进程启动/崩溃重试上限，`-1` = 无限 |
| `pi_retry_delay` | 10 | pi 进程重试首次等待秒数 |

### 容器挂载

| 容器路径 | 说明 | 模式 |
|----------|------|------|
| `/data/target` | 解包后的固件文件系统 | 只读 |
| `/data/config` | 配置目录（含 `config.json`、`models.json`） | 只读 |
| `/data/output` | 结果输出 | 读写 |

## 项目结构

```
system_analyse/
├── app/
│   ├── orchestrator.py      # 四阶段流水线编排（核心）
│   ├── models.py            # 数据模型（StageLoopConfig, TaskConfig, ...）
│   ├── config.py            # 配置加载
│   ├── runner.py            # pi-coding-agent 子进程封装（双层重试 + 致命检测）
│   └── server.py            # HTTP API（可选）
├── prompts/
│   ├── workers/
│   │   ├── step1_classify.md       # Stage 1: 全局文件分类（脚本批处理）
│   │   ├── step2_refine.md         # Stage 2: 主 Worker 细分决策
│   │   ├── step2_sub_read.md       # Stage 2: 子 Worker 逐文件读取摘要
│   │   ├── step3_analyse.md        # Stage 3: 模块详细分析（STRIDE）
│   │   ├── step4_final_report.md   # Stage 4: 生成最终报告
│   │   ├── reflect_classify.md     # Stage 1 反思自查
│   │   ├── reflect_refine.md       # Stage 2 反思自查
│   │   ├── reflect_analyse.md      # Stage 3 反思自查
│   │   └── reflect_report.md       # Stage 4 反思自查
│   └── judges/
│       ├── step1_check_classify.md # Stage 1: 脚本检查分类覆盖率
│       ├── step2_check_refine.md   # Stage 2: 脚本检查文件完整 + 评审合理性
│       ├── step3_check_analyse.md  # Stage 3: 评审分析准确性
│       ├── step4_check_completeness.md  # Stage 4a: 脚本检查报告完整性
│       └── step4_check_report.md   # Stage 4b: 评审最终报告质量
├── scripts/
│   ├── check_classification.sh     # 比对 target/ vs files.list
│   └── check_outputs.sh            # 检查 module_report.md 存在性
├── docs/
│   └── step2_optimization.md       # Stage 2 优化设计文档
├── cli.py                          # 命令行入口
├── main.py                         # REST API 入口
├── config.example.json             # 配置示例
├── Dockerfile                      # 容器构建（基于 dfa-base）
├── Dockerfile.full                 # 完整构建（含基础层）
└── README.md
```

## 输出格式

### flag 文件

```
0   # 任务失败或进行中
1   # 任务成功完成
```

### final_report.md

成功时包含完整的 7 章节分析报告；失败时记录失败原因和已完成进度。

### 模块报告 module_report.md

每个模块包含：
1. **文件清单** — 路径 | 类型 | 功能（相对路径）
2. **模块功能概述** — 整体职责和对外接口
3. **分类合理性自检** — `[分类合理]` 或 `[分类问题]`
4. **STRIDE 威胁分析** — 位置、触发条件、影响、风险等级
5. **对外暴露面评估** — 端口/路径/IPC、综合风险评分

## License

MIT
