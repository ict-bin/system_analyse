# system_analyse

固件安全威胁分析系统，基于多 Agent 流水线对解包后的固件（或源码树）执行：
**模块分类 → 精细化拆分 → STRIDE 威胁分析 → 汇总报告**。

---

## 系统架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         system_analyse 容器                              │
│                                                                         │
│  /data/target (固件目录, RO)                                             │
│       │                                                                 │
│  ┌────▼──────────────────────────────────────────────────────────────┐  │
│  │                      4 阶段流水线                                  │  │
│  │                                                                   │  │
│  │  Stage 0: 预处理                                                  │  │
│  │  ┌────────────┐  ┌──────────────┐  ┌───────────────────┐         │  │
│  │  │ 文件过滤   │→ │ 探索目录     │→ │ 关键词预扫描      │         │  │
│  │  │ 类型+架构  │  │ (MiniMax)    │  │ prescan_files.py  │         │  │
│  │  │ 1157 文件  │  │ keywords.txt │  │ 词频统计+黑名单   │         │  │
│  │  └────────────┘  └──────────────┘  └───────────────────┘         │  │
│  │          │                                                        │  │
│  │  Stage 1: 全局分类 (W+J 循环)                                     │  │
│  │  ┌────────────────────────────────────────────┐                  │  │
│  │  │  Worker(GLM-5)          Judge(GLM-5)        │                  │  │
│  │  │  step1_classify.md  →   step1_check.md     │                  │  │
│  │  │  创建 modules/ 目录树   零遗漏铁律验证      │                  │  │
│  │  │  min_rounds=1           Missing>0 → 0分    │                  │  │
│  │  └────────────────────────────────────────────┘                  │  │
│  │          │                                                        │  │
│  │  Stage 2: 模块精细化拆分 (并行 × parallel_modules)               │  │
│  │  ┌──────────────────────────────────────────────────────────┐    │  │
│  │  │                asyncio.Queue (零遗漏)                     │    │  │
│  │  │                                                          │    │  │
│  │  │  ┌─────────────────────────────────────────────────┐    │    │  │
│  │  │  │ 文件数 > 20 时：主从模式                         │    │    │  │
│  │  │  │                                                 │    │    │  │
│  │  │  │  SubWorker(batch1) ─┐                           │    │    │  │
│  │  │  │  SubWorker(batch2) ─┤ parallel_sub_workers=N   │    │    │  │
│  │  │  │  SubWorker(batch3) ─┤→ 5列摘要表               │    │    │  │
│  │  │  │       ...           │   路径|类型|功能|标识|子模块│   │    │  │
│  │  │  │  SubWorker(batchN) ─┘         │                 │    │    │  │
│  │  │  │                               ▼                 │    │    │  │
│  │  │  │  Master Worker(GLM-5) ──────→ 细分决策          │    │    │  │
│  │  │  │       │  step2_refine.md      拆分/保留/合并     │    │    │  │
│  │  │  │       ▼                              │           │    │    │  │
│  │  │  │  Judge(GLM-5)                        │           │    │    │  │
│  │  │  │  step2_check.md                      │           │    │    │  │
│  │  │  │  check_module.sh(快照对比)            │           │    │    │  │
│  │  │  │  通过 → 子模块入队 ←─────────────────┘           │    │    │  │
│  │  │  └─────────────────────────────────────────────────┘    │    │  │
│  │  │                                                          │    │  │
│  │  │  Stage2 完成后全局校验: filtered_files.txt vs 所有 files.list│   │  │
│  │  │  缺失文件 → W+J 补分类 (最多3轮)                         │    │  │
│  │  └──────────────────────────────────────────────────────────┘    │  │
│  │          │                                                        │  │
│  │  Stage 3: 模块安全分析 (并行 × parallel_modules)                 │  │
│  │  ┌──────────────────────────────────────────────────────────┐    │  │
│  │  │  Python 预读 (无 LLM)          Worker(GLM-5, tools=write) │    │  │
│  │  │  ┌────────────────────────┐   ┌────────────────────────┐ │    │  │
│  │  │  │ nm -D 导出符号(攻击面) │   │ 直接写 module_report.md│ │    │  │
│  │  │  │ nm -D 导入符号(危险fn) │→  │ STRIDE 威胁分析        │ │    │  │
│  │  │  │ readelf -d NEEDED(依赖)│   │ 风险等级+评分          │ │    │  │
│  │  │  │ strings head-50(上下文)│   │ 无 tool call (1次完成) │ │    │  │
│  │  │  └────────────────────────┘   └────────────────────────┘ │    │  │
│  │  │              │                           │                 │    │  │
│  │  │              └──────────────────────────►│                 │    │  │
│  │  │                    system_prompt 预注入   │                 │    │  │
│  │  │                                          ▼                 │    │  │
│  │  │  Judge(GLM-5) step3_check.md                               │    │  │
│  │  │  通过 → 记录结果                                            │    │  │
│  │  │  [分类问题] → modules_needing_reclassify → Stage2-redo     │    │  │
│  │  └──────────────────────────────────────────────────────────┘    │  │
│  │          │                                                        │  │
│  │  Stage 4: 最终报告                                               │  │
│  │  ┌───────────────────────────────────────────┐                   │  │
│  │  │ 4a: 完整性检查 (Judge + check_outputs.sh) │                   │  │
│  │  │     缺失模块 → Stage2+3 补做              │                   │  │
│  │  │ 4b: 生成总报告 (Worker step4_final_report) │                   │  │
│  │  │     Judge step4_check_report 质量验证     │                   │  │
│  │  └───────────────────────────────────────────┘                   │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                │                                                         │
│  /data/output (结果目录, RW)                                             │
│  ├── flag                 # 1=成功 0=失败                                │
│  ├── final_report.md      # 总安全报告（STRIDE 汇总+暴露面+修复建议）    │
│  ├── modules.list         # 按风险排序的模块名                           │
│  ├── modules/<mod>/                                                      │
│  │   ├── files.list       # 相对路径，每行一个                           │
│  │   └── module_report.md # STRIDE 分析 + 风险等级                      │
│  └── archive.zip          # 工作过程归档                                 │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Agent 架构

```
┌─────────────────────────────────────────────────────┐
│                  runner.py (RPC Mode)                │
│                                                      │
│  pi --mode rpc [--session <file>] [--model <m>]     │
│       ↑ stdin: {"type":"prompt","message":"..."}    │
│       ↓ stdout: JSONL events (message_end 等)       │
│                                                      │
│  双层重试:                                           │
│    外层: pi 进程崩溃/启动失败 → 重新拉起             │
│    内层: API 超时/限流/503   → 等待重试              │
│    致命: 401/model not found → 立即终止              │
│                                                      │
│  ⚠ RPC mode 解决 ARG_MAX 限制:                      │
│    prompt 经 stdin 传入，无命令行长度限制             │
│    system_prompt 经 --append-system-prompt 文件传入  │
└─────────────────────────────────────────────────────┘

Worker (step* prompts)          Judge (check* prompts)
─────────────────────────       ─────────────────────────
• 持久 session (--session)      • 无 session (--no-session)
• 完整工具集: read,bash,write   • 只读工具: read,bash,grep
• Stage3: tools=["write"]       • 输出固定格式:
  (预注入内容，无需读文件)         ## 评分: 0-100
                                  ## 通过: 是/否
                                  ## 评审意见: ...
```

---

## W+J 循环

每个阶段的核心控制流：

```
                    ┌──────────────────────────┐
                    │     W+J 轮次循环          │
                    │                          │
  ──────────────►  Worker 执行任务             │
                    │        │                 │
                    │        ▼                 │
                    │   Judge 评审             │
                    │   score / pass?          │
                    │        │                 │
          ┌─────────┤  pass=是 ─────────────► │──► 下一阶段
          │         │  且 round >= min_rounds   │
          │ pass=否  │                          │
          │ 或轮次不足│  pass=是 且 round < min  │
          │         │  → Worker 继续下一轮      │
          ▼         │        ↑                 │
    反思提示注入     │        └─────────────────┘
    reflect_*.md    │                          │
          │         │  round > max_rounds      │
          └────────►│  → StageError (报警)     │
                    └──────────────────────────┘

投票模式 (pass_mode):
  all      → 所有 Judge 全部通过
  majority → 超半数 Judge 通过
  any      → 至少 1 个 Judge 通过
```

---

## 目录结构

```
system_analyse/
├── app/
│   ├── config.py              # ServiceConfig → TaskConfig 转换
│   ├── models.py              # 配置模型（ANALYSE_TYPES、BINARY_ARCH）
│   ├── orchestrator.py        # 薄层，委托 _orchestrator_legacy
│   ├── _orchestrator_legacy.py# 完整四阶段编排实现
│   ├── runner.py              # pi RPC 进程管理 + 双层重试
│   ├── server.py              # REST API
│   └── pipeline/              # 新架构（逐步迁移中）
│       ├── context.py         # PipelineContext（全局状态）
│       ├── base.py            # BaseStage + Pipeline（start_stage skip）
│       ├── helpers.py         # parse_eval_md / check_voting / discover_modules
│       ├── s0_filter.py       # FilterStage / ExploreStage / PrescanStage
│       ├── s1_classify.py     # ClassifyStage
│       ├── s2_refine.py       # RefineStage (骨架)
│       ├── s3_analyse.py      # AnalyseStage (骨架)
│       └── s4_report.py       # CompletenessCheckStage / FinalReportStage
├── prompts/
│   ├── workers/
│   │   ├── step1_explore.md       # 探索目录，生成 keywords.txt
│   │   ├── step1_classify.md      # 全局模块分类
│   │   ├── reflect_classify.md    # 分类反思
│   │   ├── step2_refine.md        # Master：细分决策（只允许 split 不允许跨模块移）
│   │   ├── step2_sub_read.md      # Sub Worker：批量文件摘要（5列格式）
│   │   ├── step2_reclassify.md    # Stage2-redo 补分类
│   │   ├── reflect_refine.md      # 细分反思
│   │   ├── step3_analyse.md       # 模块安全分析（预注入模板）
│   │   ├── reflect_analyse.md     # 分析反思
│   │   ├── step4_final_report.md  # 生成总报告
│   │   └── reflect_report.md      # 报告反思
│   └── judges/
│       ├── step1_check_classify.md    # 零遗漏验证（Missing>0 → 0分）
│       ├── step2_check_refine.md      # 细分合理性+文件完整性
│       ├── step3_check_analyse.md     # 分析质量+[分类问题]检测
│       ├── step4_check_completeness.md# 完整性检查
│       └── step4_check_report.md      # 报告质量验证
├── scripts/
│   ├── entrypoint.sh            # models.json 链接 + settings.json 生成
│   ├── filter_files.sh          # 按类型+架构过滤（binary/source/config/...）
│   ├── prescan_files.py         # Python 预扫描（多进程+ELF magic）
│   ├── check_classification.sh  # 零遗漏验证
│   ├── check_module.sh          # Stage2 快照对比验证
│   └── check_outputs.sh         # 报告完整性验证
├── docs/
│   └── stage3_performance_analysis.md  # 性能分析（tool call 实测数据）
├── cli.py               # 命令行入口（进度显示）
├── main.py              # REST API 入口
├── config.example.json  # 完整配置示例
├── config/
│   └── models.json.example   # models.json 模板
├── test_orchestrator.py # 调度逻辑 dry-run 测试（11 场景）
├── test_pipeline.py     # pipeline/ 新架构测试（38 场景）
├── Dockerfile
└── docker-compose.yml
```

---

## 快速开始

### 1. 目录准备

```
~/my-analysis/
├── firmware/        # 固件解包目录（只读挂载）
├── config/
│   ├── config.json
│   └── models.json  # 见下方示例
└── output/
```

### 2. 配置文件

**`config/models.json`**（providers 中性命名防止 pi 路由到云端 API）：

```json
{
  "providers": {
    "icsl_vllm_1": {
      "baseUrl": "http://your-vllm-host:8000/v1/",
      "api": "openai-completions",
      "apiKey": "your-key",
      "models": [
        { "id": "zai-org/GLM-5", "reasoning": true }
      ]
    },
    "icsl_vllm_2": {
      "baseUrl": "http://your-vllm-host:8003/v1/",
      "api": "openai-completions",
      "apiKey": "your-key",
      "models": [
        { "id": "MiniMax/MiniMax-M2.5", "reasoning": true }
      ]
    }
  }
}
```

> **provider 命名规范**：使用自定义中性名（如 `icsl_vllm_1`），避免 pi 将 `minimax`/`openai` 等关键字路由至云端 API。

**`config/config.json`**（完整示例见 [config.example.json](config.example.json)）：

```json
{
  "analyse_targets": ["binary"],
  "binary_arch": ["arm", "aarch64"],
  "parallel_modules": 2,
  "parallel_sub_workers": 2,
  "stages": {
    "classify":    {"min_rounds": 1, "max_rounds": -1, "pass_mode": "all"},
    "refine":      {"min_rounds": 1, "max_rounds": -1, "pass_mode": "all"},
    "analyse":     {"min_rounds": 1, "max_rounds": -1, "pass_mode": "all"},
    "final_check": {"min_rounds": 1, "max_rounds": -1, "pass_mode": "all"}
  },
  "workers": {
    "default_tools": ["read", "bash", "edit", "write", "grep", "find"],
    "system_prompt_dir": "/opt/system_analyse/prompts/workers",
    "agents": [{"model": "icsl_vllm_1/zai-org/GLM-5"}],
    "stage_models": {
      "explore": "icsl_vllm_2/MiniMax/MiniMax-M2.5"
    }
  },
  "judges": {
    "default_tools": ["read", "bash", "grep", "find"],
    "system_prompt_dir": "/opt/system_analyse/prompts/judges",
    "agents": [{"model": "icsl_vllm_1/zai-org/GLM-5"}]
  },
  "output_dir": "/data/output",
  "archive_dir": "/data/output",
  "result_dir": "/data/output"
}
```

### 3. 构建并运行

```bash
docker build -t system_analyse .

docker run -d --name system_analyse \
  --network host \
  -v /path/to/firmware:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  -e GLM_API_KEY=your_glm_key \
  -e MINIMAX_API_KEY=your_minimax_key \
  system_analyse \
  python3 cli.py "对解包后的固件进行系统模块分类和安全威胁分析"

# 实时查看进度
docker logs -f system_analyse
```

### 4. 从 Stage 3 恢复（跳过已完成的 S0-S2）

```bash
docker run -d --name resume_s3 \
  --network host \
  -v /path/to/firmware:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  -e GLM_API_KEY=your_key \
  system_analyse \
  python3 cli.py "..." \
  --config /data/config/config_resume.json
```

`config_resume.json` 额外字段：
```json
{
  "start_stage": 3,
  "resume_workspace": "/data/output/task-xxxx/workspace"
}
```

---

## 配置项详解

### `analyse_targets` — 文件类型过滤

| 值 | 说明 | 扩展名 |
|----|------|--------|
| `binary` | ELF 可执行文件、共享库、内核模块 | `.so .ko .o .a .elf` |
| `source` | C/C++ 源码、汇编 | `.c .h .cpp .cc .hpp .inc .S .asm` |
| `script` | Shell/Python/Lua 等脚本 | `.sh .py .lua .pl .rb .tcl` |
| `config` | 配置文件 | `.conf .json .yaml .xml .ini` |
| `firmware` | 固件/Boot/DTB | `.bin .img .dtb .dts .fw .fpga` |
| `crypto` | 证书/密钥/签名 | `.pem .crt .key .sig .p12` |
| `database` | 数据库/Schema | `.db .sqlite .sql` |
| `web` | Web 前后端 | `.html .css .js .php .ts .vue` |
| `network_model` | 网络模型/协议定义 | `.yang .mib .proto .asn` |
| `document` | 文档/日志 | `.md .txt .log .csv` |
| `archive` | 压缩包/安装包 | `.tar .gz .rpm .deb .ipk` |
| `all` | 不过滤（默认）| — |

### `binary_arch` — 架构过滤

通过读取 ELF header `e_machine` 字段（偏移 0x12）实现，无需 `file` 命令：

| 值 | 架构 | e_machine |
|----|------|-----------|
| `arm` | ARM 32位 | 40 |
| `aarch64` | ARM 64位 | 183 |
| `x86` | x86 32位 | 3 |
| `x86_64` | x86 64位 | 62 |
| `mips` | MIPS | 8 |
| `ppc` / `ppc64` | PowerPC | 20 / 21 |
| `riscv` | RISC-V | 243 |
| `s390` | IBM S/390 | 22 |

路径含架构关键词（如 `aarch64/`）时直接判定，不读文件，加速过滤。

### 并行配置

```
parallel_modules=2, parallel_sub_workers=2 时的调度:

Module A (100 files)          Module B (80 files)
  Sub1(1-20)  ─┐               Sub1(1-20)  ─┐
  Sub2(21-40) ─┤ parallel=2    Sub2(21-40) ─┤ parallel=2
  ...          ─┘→ Master       ...          ─┘→ Master
  
最大并发: 2×2 = 4 个 LLM 调用
```

### 阶段控制

| 字段 | 说明 |
|------|------|
| `min_rounds` | 最少轮次（`1`=通过即止，`2`=强制反思验证） |
| `max_rounds` | 最大轮次（`-1`=无限重试） |
| `pass_mode` | `all` 全通过 / `majority` 多数通过 / `any` 一票通过 |

### 重试控制

| 字段 | 说明 | 默认 |
|------|------|------|
| `agent_max_retries` | API 错误重试（`-1`=无限） | `100` |
| `agent_retry_delay` | 首次重试等待秒（指数退避，上限 300s） | `30` |
| `pi_max_retries` | pi 进程崩溃重试（`-1`=无限） | `-1` |
| `pi_retry_delay` | pi 进程重试首次等待秒 | `10` |

---

## 关键设计决策

### Stage 3 文件预注入

Stage 3 Worker 使用 `tools=["write"]`，文件内容由 Python 预注入 system_prompt：

```
Python 预读:
  nm -D 导出符号 (前300个) → 对外攻击面
  nm -D 导入符号 (前150个) → 危险函数调用
  readelf -d NEEDED        → 依赖库（安全库识别）
  strings head-50          → 错误消息/协议字符串

注入 system_prompt → Worker 直接写报告 (1次完成)
上轮对比: 42.6 tool calls/模块 → 1次，耗时 9min → 1.5min
```

### Stage 2 完整性保障

```
零遗漏铁律：所有 Judge 统一判定
  missing_count > 0 → score = 0，不通过
  
快照机制：细分前保存 files.list 快照
  check_module.sh 对比快照 vs (子模块 ∪ 迁移目标)
  真正缺失 = 快照 - 所有模块并集
  
全局校验：Stage2 完成后
  filtered_files.txt vs 所有 files.list 并集
  缺失文件 → W+J 补分类（最多3轮）
```

### 重分类流程

```
Stage3 Worker → [分类问题] 标记
     ↓
Stage3 Judge → [需要重新分类] 标记（严重性判断由 LLM 决定）
     ↓
modules_needing_reclassify 列表
     ↓
Stage2-redo: 对问题模块重新细分
     ↓
Stage3-redo: 仅处理:
  ① 新产生的子模块 (not in final_modules)
  ② 原始模块 files.list 非空者（排除空壳）
```

---

## 输出说明

### `final_report.md` 结构

```markdown
# 固件系统威胁分析总报告
## 1. 分析概况（模块数、威胁总数、风险分布）
## 2. 模块清单（按风险排序）
## 3. 高风险威胁汇总（P0/P1/P2）
## 4. 攻击面汇总（网络端口、本地接口）
## 5. STRIDE 统计（S/T/R/I/D/E 分类计数）
## 6. 修复建议（按优先级）
## 7. 结论
```

### `modules/<mod>/module_report.md` 结构

```markdown
<!-- RISK_LEVEL: 高 -->
<!-- RISK_SCORE: 85 -->

## 1. 模块风险等级
## 2. 文件清单（路径|类型|功能）
## 3. 模块功能概述（基于导出函数分析）
## 4. 分类合理性自检
## 5. 威胁分析 STRIDE（攻击面|触发条件|影响|风险等级）
## 6. 对外暴露面评估
<result>摘要</result>
```

### `flag`

| 值 | 含义 |
|----|------|
| `1` | 全流程通过，`final_report.md` 有效 |
| `0` | 失败或中断，查看日志定位原因 |

---

## 测试

```bash
# 全部 dry-run 测试（无需 GPU/API）
python3 -X utf8 test_orchestrator.py   # 调度逻辑 11 场景
python3 -X utf8 test_pipeline.py       # 新架构 38 场景

# 期望输出: 所有 ✅，0 失败
```

---

## 性能参考（实测 NE8000 固件，1157 个 AArch64 ELF）

| 阶段 | 模块数 | 并行数 | 耗时 |
|------|--------|--------|------|
| S0（过滤+探索+预扫描） | — | 1 | ~11min |
| S1（分类） | ~18 顶层模块 | 1 | ~15min |
| S2（细分） | ~200 子模块 | 2 | ~2h |
| S3（分析，旧版） | 202 | 2 | ~13h |
| S3（分析，预注入优化后） | 202 | 2 | ~3h (预计) |
| S4（报告） | — | 1 | ~20min |

> S3 优化依赖 `nm -D` + `readelf` 预注入，消除 LLM tool call（42.6次/模块→1次）。
