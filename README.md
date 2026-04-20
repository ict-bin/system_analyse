# system_analyse

`system_analyse` 用于对解包后的固件文件树或恢复后的源码树做系统级分析，核心任务是：

- 建立模块划分
- 对大模块继续细分
- 生成每个模块的 STRIDE 威胁分析
- 汇总输出总报告

它既可以独立运行，也可以作为 `softhack` 链式流水线中的 `01-system` 阶段运行。

## 核心流程

```text
输入目录 (/data/target)
  -> Stage 1 分类
  -> Stage 2 模块细分
  -> Stage 3 模块分析
  -> Stage 4 完整性检查 + 最终报告
  -> 输出 modules/ + final_report.md + archive.zip
```

其中：

- Worker 负责产出分类、细分和分析结果
- Judge 负责独立评审，并在未通过时驱动下一轮修正
- 部分关键检查由脚本完成，避免完全依赖 LLM 自评

## 目录结构

```text
01-system_analyse/
├── app/
│   ├── config.py
│   ├── models.py
│   ├── runner.py
│   ├── orchestrator.py
│   └── server.py
├── prompts/
│   ├── workers/
│   └── judges/
├── scripts/
│   ├── check_classification.sh
│   ├── check_outputs.sh
│   └── ...
├── cli.py
├── main.py
├── chained_runner.py
├── config.example.json
├── ENV_REFERENCE.md
├── USAGE.md
├── Dockerfile
├── Dockerfile.chain
└── docker-compose.yml
```

## 输入与输出

### 输入

默认输入目录：

```text
/data/target
```

适合放入：

- 固件解包后的完整文件树
- 已经整理好的源码目录
- 任何需要做模块划分与威胁分析的目标目录

### 输出

默认输出目录：

```text
/data/output
```

典型产物：

```text
output/
├── flag
├── final_report.md
├── modules/
│   └── <module>/
│       ├── files.list
│       └── module_report.md
└── archive.zip
```

说明：

- `files.list` 是后续 `entry_analyse` 的标准输入之一
- `flag` 为 `1` 表示整体通过，`0` 表示失败或中断
- `archive.zip` 保存工作过程与归档结果

## 快速开始

### 1. 准备配置

最小可用配置可以从 [config.example.json](config.example.json) 复制：

```json
{
  "analyse_targets": ["all"],
  "agent_max_retries": 100,
  "agent_retry_delay": 30,
  "pi_max_retries": -1,
  "pi_retry_delay": 10,
  "stages": {
    "classify": { "min_rounds": 2, "max_rounds": -1, "pass_mode": "all" },
    "refine": { "min_rounds": 2, "max_rounds": -1, "pass_mode": "all" },
    "analyse": { "min_rounds": 2, "max_rounds": -1, "pass_mode": "all" },
    "final_check": { "min_rounds": 2, "max_rounds": -1, "pass_mode": "all" }
  },
  "workers": {
    "system_prompt_dir": "/opt/system_analyse/prompts/workers",
    "agents": [{ "model": "gaiasec/auto" }]
  },
  "judges": {
    "system_prompt_dir": "/opt/system_analyse/prompts/judges",
    "agents": [{ "model": "gaiasec/auto" }, { "model": "gaiasec/auto" }]
  },
  "output_dir": "/data/output",
  "archive_dir": "/data/output",
  "result_dir": "/data/output"
}
```

模型配置和环境变量说明见 [ENV_REFERENCE.md](ENV_REFERENCE.md)。

### 2. CLI 运行

```bash
docker build -t system_analyse .

docker run --rm --network host \
  -v /path/to/source:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  -e GAIASEC_API_KEY=xxx \
  system_analyse \
  python3 cli.py "对解包后的所有文件进行威胁分析与模块分析" \
  --config /data/config/config.json
```

CLI 支持自动搜索配置文件；如果不传 `--config`，会按这些路径查找：

- `/data/config/config.json`
- `/opt/system_analyse/config.example.json`
- `./config.json`
- `./config.example.json`

### 3. REST API 运行

```bash
docker run -d --name system-analyse \
  -p 3000:3000 \
  -v /path/to/source:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  -e GAIASEC_API_KEY=xxx \
  system_analyse
```

提交任务：

```bash
curl -X POST http://localhost:3000/analyse \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "对解包后的所有文件进行威胁分析与模块分析",
    "cwd": "/data/target"
  }'
```

常用接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/health` | 健康检查 |
| `POST` | `/analyse` | 提交任务 |
| `GET` | `/task/{id}` | 查看结果 |
| `GET` | `/task/{id}/stream` | SSE 实时事件 |
| `POST` | `/task/{id}/abort` | 中止任务 |
| `GET` | `/tasks` | 列出内存中的任务 |

## 链式模式中的位置

在根目录链式流水线中，本模块对应 `01-system` 阶段。

链式 runner 的行为大致是：

1. 优先读取 `00-unpack/output/unpacked`
2. 没有解包产物时回退到 `/app`
3. 执行本模块 CLI
4. 把结果写到 `/app/.run/01-system/output`
5. 为 `02-re` 准备下一阶段请求

后续阶段会消费：

- `modules/<module>/files.list`
- `modules/<module>/module_report.md`
- `modules.json`

## 关键配置项

| 字段 | 作用 |
| --- | --- |
| `stages.classify/refine/analyse/final_check` | 控制各阶段最少轮数、最大轮数和通过模式 |
| `agent_max_retries` | Agent/API 侧重试次数 |
| `pi_max_retries` | pi 进程拉起失败时的重试次数 |
| `workers.agents` | 生产结果的 Agent 列表 |
| `judges.agents` | 评审结果的 Agent 列表 |
| `output_dir/archive_dir/result_dir` | 输出、归档、结果路径 |

## 相关文档

- [USAGE.md](USAGE.md)
- [ENV_REFERENCE.md](ENV_REFERENCE.md)
- [仓库 README](../README.md)
- [链式流水线](../CHAINED_PIPELINE.md)
