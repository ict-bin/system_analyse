# system_analyse 使用手册

## 1. 目录结构

```
~/my-analysis/
├── target/          # 固件解包目录（只读挂载到 /data/target）
├── config/
│   ├── config.json  # 分析配置
│   └── models.json  # 模型 provider 配置
└── output/          # 分析结果输出
```

## 2. 最小配置

```json
{
    "analyse_targets": ["all"],
    "parallel_modules": 1,
    "parallel_sub_workers": 1,
    "agent_max_retries": -1,
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
        "agents": [{"model": "vllm/your-model"}]
    },
    "judges": {
        "default_tools": ["read", "bash", "grep", "find"],
        "system_prompt_dir": "/opt/system_analyse/prompts/judges",
        "agents": [{"model": "vllm/your-model"}]
    },
    "output_dir": "/data/output",
    "archive_dir": "/data/output",
    "result_dir": "/data/output"
}
```

完整配置示例见 [config.example.json](config.example.json)。

## 3. 文件过滤配置

### 按类型过滤 `analyse_targets`

```json
"analyse_targets": ["all"]                    // 不过滤（默认）
"analyse_targets": ["binary"]                 // 只分析 ELF 二进制
"analyse_targets": ["binary", "script"]       // 二进制 + 脚本
"analyse_targets": ["config", "network_model"] // 配置 + 网络模型
```

支持的类型：`binary` `script` `config` `firmware` `crypto` `database` `web` `network_model` `document` `archive` `all`

### 按架构过滤 `binary_arch`（仅 binary 类型）

```json
"analyse_targets": ["binary"],
"binary_arch": ["all"]              // 不过滤（默认）
"binary_arch": ["arm", "aarch64"]  // 只分析 ARM 32/64 位
"binary_arch": ["x86_64"]          // 只分析 x86_64
```

支持：`arm` `aarch64` `x86` `x86_64` `mips` `mips64` `ppc` `ppc64` `riscv` `s390` `all`

> **注意**：通过读取 ELF header（e_machine 字段）判断架构，不依赖 `file` 命令。

## 4. 并行配置

### `parallel_modules` — 模块间并行

```json
"parallel_modules": 1   // 串行（默认）
"parallel_modules": 2   // 同时处理 2 个模块
"parallel_modules": 4   // 同时处理 4 个模块
```

Stage 2/3 的模块处理完全独立，可安全并行。拆分出的子模块自动入队，不遗漏。

### `parallel_sub_workers` — 模块内批次并行

```json
"parallel_sub_workers": 1   // 串行（默认）
"parallel_sub_workers": 2   // 每模块内 2 个批次并行
```

文件数 > 20 的模块启用主/子 Worker 模式。子 Worker 并行读取文件，Master Worker 接收汇总后的文件清单表做决策。

### 推荐配置

```json
// 单 GPU，均衡
"parallel_modules": 2,
"parallel_sub_workers": 2   // 最多 4 个并发 LLM 调用

// 单 GPU，保守
"parallel_modules": 2,
"parallel_sub_workers": 1   // 最多 2 个并发 LLM 调用

// 多 GPU 或高吞吐推理服务
"parallel_modules": 4,
"parallel_sub_workers": 2   // 最多 8 个并发 LLM 调用
```

## 5. 阶段配置

### `min_rounds` 语义

总运行轮次 ≥ min_rounds 且最后一轮通过即止：

```
min_rounds=1: 第1轮通过 → 结束（推荐用于生产）
min_rounds=2: 第1轮失败+第2轮通过 → 结束（已满足2轮）
              第1轮通过+第2轮通过 → 结束（2轮均通过）
              第1轮通过 → 强制反思 → 第2轮通过 → 结束
```

> 设置 `min_rounds=2` 主要用于**测试反思逻辑**是否正确，生产环境建议 `min_rounds=1`。

### `pass_mode`

- `"all"` — 所有 Judge 都通过才算通过（默认，严格）
- `"majority"` — 超半数 Judge 通过即可（宽松，适合多 Judge 场景）

## 6. 运行方式

### CLI

```bash
docker run -d --name system_analyse --network host \
  -v /path/to/target:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  -e GLM_API_KEY=your_key \
  system_analyse \
  python3 cli.py "对解包后的固件进行系统模块分类和安全威胁分析"

# 查看进度
docker logs -f system_analyse
```

### REST API

```bash
docker run -d --name system_analyse -p 3000:3000 \
  -v /path/to/target:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  -e GLM_API_KEY=your_key \
  system_analyse

# 提交任务
curl -X POST http://localhost:3000/analyse \
  -H "Content-Type: application/json" \
  -d '{"prompt": "对解包后的固件进行系统模块分类和安全威胁分析"}'
```

## 7. 输出说明

```
output/
├── flag                    # "1"=成功，"0"=失败
├── final_report.md         # 总安全报告（含失败时的错误信息）
├── modules.list            # 按风险等级排序的模块名，每行一个
│                           # 严重→高→中→低→信息→未知
├── modules/
│   └── <module>/
│       ├── files.list      # 相对路径（不含 /data/target/），每行一个
│       └── module_report.md # STRIDE 分析，含 RISK_LEVEL/RISK_SCORE
└── archive.zip             # 所有中间产物（session、judge评审、workspace）
```

## 8. 验证测试

调度逻辑可在无 GPU/API 的环境下运行 dry-run 测试：

```bash
python3 test_orchestrator.py
# 结果: 11 通过, 0 失败
```
