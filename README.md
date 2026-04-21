# system_analyse

`system_analyse` 对解包后的固件文件树或源码树做系统级安全分析：模块划分 → 细分 → STRIDE 威胁分析 → 汇总报告。

## 核心流程

```
/data/target
  Stage 0: 文件过滤（按类型 + 架构）
  Stage 1: 探索目录 → 预扫描关键词 → 全局分类（Judge 零遗漏校验）
  Stage 2: 逐模块细分（主/子 Worker 主从模式，并行）
  Stage 3: 逐模块安全分析（STRIDE，并行）
  Stage 4: 完整性检查 + 生成最终报告
  ↓
/data/output
  ├── flag               # 1=成功 0=失败
  ├── final_report.md    # 总安全报告
  ├── modules.list       # 按风险等级排序的模块名（每行一个）
  ├── modules/
  │   └── <module>/
  │       ├── files.list        # 相对路径，每行一个
  │       └── module_report.md  # 含 STRIDE 分析 + 风险等级
  └── archive.zip        # 工作过程归档
```

## 目录结构

```
system_analyse/
├── app/
│   ├── config.py          # ServiceConfig → TaskConfig 转换
│   ├── models.py          # 所有配置模型（含 ANALYSE_TYPES、BINARY_ARCH）
│   ├── orchestrator.py    # 四阶段编排器（并行调度）
│   ├── runner.py          # pi 进程管理 + 双层重试
│   └── server.py          # REST API
├── prompts/
│   ├── workers/
│   │   ├── step1_explore.md       # 探索目录、生成 keywords.txt
│   │   ├── step1_classify.md      # 全局分类
│   │   ├── step2_refine.md        # Master：细分决策
│   │   ├── step2_sub_read.md      # Sub Worker：批量读文件生成摘要
│   │   ├── step3_analyse.md       # 模块安全分析
│   │   ├── step4_final_report.md  # 生成总报告
│   │   └── reflect_*.md           # 各阶段反思提示
│   └── judges/
│       ├── step1_check_classify.md    # 零遗漏校验（Missing>0 → 0分）
│       ├── step2_check_refine.md      # 细分合理性 + 零遗漏
│       ├── step3_check_analyse.md     # 分析报告质量
│       ├── step4_check_completeness.md
│       └── step4_check_report.md
├── scripts/
│   ├── filter_files.sh          # 按类型+架构过滤目标文件
│   ├── prescan_files.sh         # 关键词批量预扫描
│   ├── check_classification.sh  # 零遗漏验证脚本
│   ├── check_outputs.sh         # 报告完整性验证
│   └── entrypoint.sh
├── cli.py               # 命令行入口（带进度展示）
├── main.py              # REST API 入口
├── config.example.json  # 完整配置示例
├── test_orchestrator.py # 调度逻辑 dry-run 测试（11 个场景）
├── Dockerfile
└── docker-compose.yml
```

## 快速开始

### 1. 准备目录

```
~/my-analysis/
├── target/          # 固件解包目录（只读挂载）
├── config/
│   ├── config.json
│   └── models.json
└── output/
```

### 2. 配置文件

`config/config.json`（完整示例见 [config.example.json](config.example.json)）：

```json
{
    "analyse_targets": ["binary"],
    "binary_arch": ["arm", "aarch64"],
    "parallel_modules": 2,
    "parallel_sub_workers": 2,
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
        "default_tools": ["read", "bash", "edit", "write", "grep", "find"],
        "system_prompt_dir": "/opt/system_analyse/prompts/workers",
        "agents": [{"model": "vllm/zai-org/GLM-5"}]
    },
    "judges": {
        "default_tools": ["read", "bash", "grep", "find"],
        "system_prompt_dir": "/opt/system_analyse/prompts/judges",
        "agents": [{"model": "vllm/zai-org/GLM-5"}]
    },
    "output_dir": "/data/output",
    "archive_dir": "/data/output",
    "result_dir": "/data/output"
}
```

### 3. 运行

```bash
docker build -t system_analyse .

docker run -d --name system_analyse --network host \
  -v /path/to/target:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  -e GLM_API_KEY=your_key \
  system_analyse \
  python3 cli.py "对解包后的固件进行系统模块分类和安全威胁分析"
```

查看进度：

```bash
docker logs -f system_analyse
```

---

## 配置项详解

### 文件过滤

#### `analyse_targets`（默认 `["all"]`）

选择要分析的文件类型，可任意组合：

| 值 | 说明 | 扩展名 |
|----|------|--------|
| `binary` | ELF 可执行文件、共享库、内核模块 | `.so .ko .o .a .elf` |
| `script` | Shell/Python/Lua 等脚本 | `.sh .py .lua .pl .rb` |
| `config` | 配置文件 | `.conf .json .yaml .xml .ini` |
| `firmware` | 固件/Boot/硬件镜像 | `.bin .img .dtb .fw .fpga` |
| `crypto` | 证书/密钥/签名 | `.pem .crt .key .sig` |
| `database` | 数据库/Schema | `.db .sqlite .sql` |
| `web` | Web 前后端 | `.html .css .js .php` |
| `network_model` | 网络模型/协议定义 | `.yang .mib .proto` |
| `document` | 文档/日志 | `.md .txt .log .csv` |
| `archive` | 压缩包/安装包 | `.tar .gz .rpm .deb` |
| `all` | 不过滤（默认）| — |

```json
"analyse_targets": ["binary", "script"]   // 只分析二进制和脚本
```

#### `binary_arch`（默认 `["all"]`）

仅当 `analyse_targets` 包含 `"binary"` 时生效，通过读取 ELF header 的 `e_machine` 字段过滤架构：

| 值 | 架构 |
|----|------|
| `arm` | ARM 32位（e_machine=40） |
| `aarch64` | ARM 64位（e_machine=183） |
| `x86` | x86 32位（e_machine=3） |
| `x86_64` | x86 64位（e_machine=62） |
| `mips` | MIPS（e_machine=8） |
| `ppc` | PowerPC（e_machine=20） |
| `riscv` | RISC-V（e_machine=243） |
| `s390` | IBM S/390（e_machine=22） |
| `all` | 不过滤（默认）| |

```json
"analyse_targets": ["binary"],
"binary_arch": ["arm", "aarch64"]   // 只分析 ARM 32/64 位二进制
```

> **原理**：直接读取 ELF magic bytes + 偏移 0x12 处的 2 字节 e_machine 字段，不依赖 `file` 命令。路径中含架构关键词（如 `aarch64/`）时直接判定，无需读文件。

### 并行配置

#### `parallel_modules`（默认 `1`）

Stage 2 和 Stage 3 同时处理的模块数。

- `1`：串行（默认，向后兼容）
- `2~4`：推荐值（单 GPU 下有效）

**Stage 2 使用 asyncio.Queue + Worker 模式**，队列动态管理：模块拆分产生的子模块自动入队，`queue.join()` 保证零遗漏。

#### `parallel_sub_workers`（默认 `1`）

单模块内子 Worker 批次并行数。当模块文件数 > 20（`SUB_WORKER_THRESHOLD`）时启用主/子 Worker 主从模式：

```
Module A（100 文件 = 5 batches）
  SubWorker batch1(1-20)  ─┐
  SubWorker batch2(21-40) ─┤  parallel_sub_workers=2
  SubWorker batch3(41-60) ─┤→ asyncio.gather → 汇总文件清单 → Master Worker
  SubWorker batch4(61-80) ─┤
  SubWorker batch5(81-100)─┘
```

子 Worker 只输出 `路径 | 类型 | 功能` 一行摘要，Master 收到的是去除 batch 标题的完整文件清单表，保证不遗漏。

**并发上限**：`parallel_modules × parallel_sub_workers`，不要超过 GPU 同时推理能力。

```json
// 推荐：单 GPU
"parallel_modules": 2,
"parallel_sub_workers": 2    // 最多 4 个并发 LLM 调用

// 保守：串行
"parallel_modules": 1,
"parallel_sub_workers": 1
```

### 阶段控制

```json
"stages": {
    "classify":    {"min_rounds": 1, "max_rounds": -1, "pass_mode": "all"},
    "refine":      {"min_rounds": 1, "max_rounds": -1, "pass_mode": "all"},
    "analyse":     {"min_rounds": 1, "max_rounds": -1, "pass_mode": "all"},
    "final_check": {"min_rounds": 1, "max_rounds": -1, "pass_mode": "all"}
}
```

| 字段 | 说明 |
|------|------|
| `min_rounds` | 最少运行轮次。`1`=通过即止；`2`=至少跑两轮（用于验证反思逻辑） |
| `max_rounds` | 最大轮次，`-1`=无限重试 |
| `pass_mode` | `"all"` 所有 Judge 通过；`"majority"` 超半数通过 |

> **`min_rounds` 语义**：总轮次 ≥ min_rounds 且最后一轮通过即止。失败轮不计入，不强制额外反思。

### 重试控制

| 字段 | 说明 | 默认 |
|------|------|------|
| `agent_max_retries` | API 错误（连接/限流/500）重试次数，`-1`=无限 | `100` |
| `agent_retry_delay` | 首次重试等待秒数（指数退避，上限 300s） | `30` |
| `pi_max_retries` | pi 进程崩溃/启动失败重试次数，`-1`=无限 | `-1` |
| `pi_retry_delay` | pi 进程重试首次等待秒数 | `10` |

---

## 输出说明

### `modules.list`

按风险等级排序的模块名（每行一个），从 `module_report.md` 的 `<!-- RISK_LEVEL: 高 -->` 注释提取：

```
ipsec
authentication
crypto
bgp
dhcp
system_core
```

风险顺序：严重 → 高 → 中 → 低 → 信息 → 未知

### `modules/<module>/files.list`

每行一个**相对路径**（不含 `/data/target/` 前缀）：

```
lib/libbgp.so
lib/libospf.so
usr/bin/bgpd
```

### `flag`

- `1`：全流程通过
- `0`：失败或中断（`final_report.md` 记录失败原因）

---

## 内置检查脚本

所有脚本位于容器内 `/opt/system_analyse/scripts/`：

| 脚本 | 用途 |
|------|------|
| `filter_files.sh` | 按类型+架构过滤，输出 `filtered_files.txt`（相对路径） |
| `prescan_files.sh` | 用 Worker 生成的 `keywords.txt` 批量扫描文件关键词 |
| `check_classification.sh` | 验证零遗漏（`Missing>0` → FAIL）|
| `check_outputs.sh` | 验证所有模块都有 `module_report.md` |

**零遗漏铁律**：所有 Judge 都以"Missing > 0 → 0分/不通过"为硬判定，没有覆盖率折中。

---

## 调度逻辑测试

```bash
# 在本地运行 dry-run 测试（无需 GPU/API，全 mock）
python3 test_orchestrator.py
```

涵盖 11 个场景：

1. `_parse_eval_md` 评分解析（含多次出现取最后、score=0 边界）
2. `_check_voting` all/majority 模式
3. 边界情况（空输出、JSON fallback）
4. 正常完整流程 Stage 0→1→2→3→4a→4b
5. `min_rounds=2` 反思循环（验证 Judge 意见传递）
6. `max_rounds=2` 超限 → StageError
7. Stage 2 拆分 + 新模块自动入队（零遗漏）
8. Stage 2-redo 触发
9. `parallel_modules=2` 并行处理
10. 子 Worker 摘要接入（大模块 vs 小模块）
11. `2×2` 并行（模块并行 × 子 Worker 并行）
