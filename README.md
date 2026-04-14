# system_analyse

基于 [pi-coding-agent](https://github.com/mariozechner/pi-coding-agent) 的容器化多 Agent 系统，对解包固件进行**模块分类 → 迭代细分 → STRIDE 威胁分析**，Worker 执行、Judge 评审，每一步强制反思，直到全部通过。

## 架构

```
┌──────────────────────────────────────────────────────────────┐
│                     四阶段流水线                                │
│                                                              │
│  Stage 1 ─ 全局分类                                          │
│  │  Worker: 遍历 /data/target，按协议/服务创建 模块/files.list │
│  │  Judge:  运行 check_classification.sh 验证文件完整性        │
│  │  循环:   min_rounds 次连续通过才放行（强制反思）              │
│  │                                                           │
│  Stage 2 ─ 模块细分（per module）                             │
│  │  Worker: 读取 files.list，判断能否按协议拆分               │
│  │  Judge:  验证 Worker 是否真正读取了文件、拆分是否合理        │
│  │                                                           │
│  Stage 3 ─ 模块分析（per module）                             │
│  │  Worker: 逐文件 read → 输出 module_report.md（STRIDE）     │
│  │  Judge:  验证文件覆盖、分析准确性、分类合理性                │
│  │  ↩ 若 Judge 标记 [需要重新分类] → 回 Stage 2 重做          │
│  │                                                           │
│  Stage 4 ─ 最终检查                                          │
│  │  Judge:  运行 check_outputs.sh 验证所有模块输出完整         │
│  └───────────────────────────────────────────────────────────┘
│                                                              │
│  投票: majority（过半通过）或 all（全票通过），per-stage 配置  │
│  反思: 每步首次通过后，Worker 带自查 prompt 重做，Judge 再验证 │
│  失败: 任一 stage 达 max_rounds 仍未通过 → 终止并报错         │
└──────────────────────────────────────────────────────────────┘
```

### 核心设计

| 特性 | 说明 |
|------|------|
| **Session 累积** | Worker 使用 `--session`，跨轮保持上下文；Judge 每次全新上下文 |
| **Per-stage 循环控制** | 每个 stage 独立配置 `min_rounds`（强制反思）、`max_rounds`（超限终止）、`pass_mode` |
| **脚本化检查** | Stage 1 和 Stage 4 的 Judge 调用 bash 脚本做确定性验证（非 LLM 判断） |
| **Reclassify 回退** | Stage 3 发现分类不合理 → Judge 投票确认 → 回 Stage 2 重做 → 再回 Stage 3 |
| **故障注入测试** | `fault_inject` + `dry_run` 模式，秒级验证所有失败分支的控制流正确性 |
| **错误重试** | API/网络错误自动重试（100 次，指数退避） |

### 文件不拷贝，路径引用

Worker 不拷贝源文件到工作目录，而是在 `files.list` 中记录 `/data/target/...` 的绝对路径，分析时按路径 `read`。节省存储，避免大文件系统下的拷贝开销。

## 快速开始

### 1. 准备配置

```json
{
    "stages": {
        "classify":    {"min_rounds": 2, "max_rounds": 5, "pass_mode": "majority"},
        "refine":      {"min_rounds": 2, "max_rounds": 3, "pass_mode": "majority"},
        "analyse":     {"min_rounds": 2, "max_rounds": 5, "pass_mode": "majority"},
        "final_check": {"min_rounds": 1, "max_rounds": 1, "pass_mode": "all"}
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

> 完整配置参考 [`config.example.json`](config.example.json)

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
  system_analyse \
  python3 cli.py "对解包后的所有文件进行威胁分析与模块分析"
```

### 4. 查看结果

```
output/
├── firmware_analyse.md            # 最终报告（模块列表 + 风险评分）
└── firmware_analyse_log.zip       # 完整归档（judge评审、session、workspace）
```

## 配置参数

### Stage 循环控制

| 参数 | 默认 | 说明 |
|------|------|------|
| `min_rounds` | 2 | 连续通过次数（首次通过后用反思 prompt 重做，达标才放行） |
| `max_rounds` | 5 | 最大迭代次数，`-1` = 无限循环直到通过 |
| `pass_mode` | `"majority"` | `"majority"` = 过半 judge 通过；`"all"` = 全票通过 |

### 全局参数

| 参数 | 默认 | 说明 |
|------|------|------|
| `agent_max_retries` | 100 | 单次 agent 调用的 API 错误重试上限 |
| `agent_retry_delay` | 30 | 首次重试等待秒数（指数退避） |

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
│   ├── models.py            # 数据模型（StageLoopConfig, FaultInjectConfig, ...）
│   ├── config.py            # 配置加载
│   ├── runner.py            # pi-coding-agent 子进程封装
│   └── server.py            # HTTP API（可选）
├── prompts/
│   ├── workers/
│   │   ├── step1_classify.md       # Stage 1: 全局文件分类
│   │   ├── step2_refine.md         # Stage 2: 模块细分判断
│   │   ├── step3_analyse.md        # Stage 3: 模块详细分析
│   │   ├── reflect_classify.md     # Stage 1 反思自查
│   │   ├── reflect_refine.md       # Stage 2 反思自查
│   │   └── reflect_analyse.md      # Stage 3 反思自查
│   └── judges/
│       ├── step1_check_classify.md # Stage 1: 调脚本检查分类完整性
│       ├── step2_check_refine.md   # Stage 2: 评审细分合理性
│       ├── step3_check_analyse.md  # Stage 3: 评审分析准确性
│       └── step4_final_check.md    # Stage 4: 调脚本最终检查
├── scripts/
│   ├── check_classification.sh     # 比对 target/ vs files.list
│   └── check_outputs.sh            # 检查 module_report.md 存在性
├── examples/
│   └── 1w1j_normal/                # 1Worker+1Judge 完整测试记录
├── cli.py                          # 命令行入口
├── config.example.json             # 配置示例
├── Dockerfile                      # 容器构建
└── README.md
```

## 故障注入测试

通过 `fault_inject` 配置验证所有失败分支，`dry_run: true` 跳过 LLM 推理，秒级完成：

```json
{
    "fault_inject": {
        "enabled": true,
        "dry_run": true,
        "stage_1_fail_until": 99
    },
    "stages": { "classify": {"max_rounds": 3} }
}
```

**已验证的失败场景：**

| 场景 | 注入方式 | 预期行为 | 状态 |
|------|----------|----------|------|
| Stage 1 耗尽 | `stage_1_fail_until: 99` | 3 轮后 `status=failed` 退出 | ✅ |
| Stage 2 模块失败 | `stage_2_fail_module: mod_c` | 指定模块重试耗尽后终止 | ✅ |
| Stage 3 重分类 | `stage_3_force_reclassify: mod_b` | → Stage 2-redo → Stage 3-redo → 通过 | ✅ |
| Stage 4 最终失败 | `stage_4_fail: true` | 最终检查 `status=failed` | ✅ |

## 输出格式

最终报告 `firmware_analyse.md`：

```markdown
| 模块 | 文件数 | 报告 | 风险 |
|------|--------|------|------|
| cpld_firmware | 5 | ✅ | 🔴 72 |
| database_upgrade | 4 | ✅ | 🔴 85 |
| security_config | 1 | ✅ | 🟢 35 |
```

每个模块的 `module_report.md` 包含：

1. **文件清单** — 每文件的类型和功能
2. **模块功能概述** — 整体职责和对外接口
3. **分类合理性自检** — `[分类合理]` 或 `[分类问题] 文件X应归入模块Y`
4. **STRIDE 威胁分析** — 每个威胁标注位置、触发条件、影响、风险等级
5. **对外暴露面评估** — 网络端口、文件路径、IPC、综合风险评分

## License

MIT
