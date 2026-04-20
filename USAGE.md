# system_analyse 使用手册

这份文档只讲独立运行 `system_analyse`。如果你要跑整个 `softhack` 流水线，请看根目录 [CHAINED_PIPELINE.md](../CHAINED_PIPELINE.md)。

## 1. 准备目录

```text
~/my-analysis/
├── target/
│   └── ...                  # 固件解包目录或恢复后的源码目录
├── config/
│   ├── config.json
│   └── models.json
└── output/
```

挂载约定：

- `target` -> `/data/target`
- `config` -> `/data/config`
- `output` -> `/data/output`

## 2. 准备配置

`config/config.json` 示例：

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
    "default_tools": ["read", "bash", "edit", "write", "grep", "find"],
    "system_prompt_dir": "/opt/system_analyse/prompts/workers",
    "default_thinking_level": "off",
    "agents": [{ "model": "gaiasec/auto" }]
  },
  "judges": {
    "default_tools": ["read", "bash", "grep", "find"],
    "system_prompt_dir": "/opt/system_analyse/prompts/judges",
    "default_thinking_level": "off",
    "agents": [{ "model": "gaiasec/auto" }, { "model": "gaiasec/auto" }]
  },
  "output_dir": "/data/output",
  "archive_dir": "/data/output",
  "result_dir": "/data/output"
}
```

`config/models.json` 需要提供当前环境可用的模型 provider 配置。

## 3. 运行 CLI

```bash
docker run --rm --network host \
  -v ~/my-analysis/target:/data/target:ro \
  -v ~/my-analysis/config:/data/config:ro \
  -v ~/my-analysis/output:/data/output \
  -e GAIASEC_API_KEY=xxx \
  system_analyse \
  python3 cli.py "对解包后的所有文件进行威胁分析与模块分析" \
  --config /data/config/config.json
```

如果你已经把 `config.json` 放在默认搜索路径，也可以省略 `--config`。

## 4. 运行 REST API

```bash
docker run -d --name system-analyse \
  -p 3000:3000 \
  -v ~/my-analysis/target:/data/target:ro \
  -v ~/my-analysis/config:/data/config:ro \
  -v ~/my-analysis/output:/data/output \
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

查看任务：

```bash
curl http://localhost:3000/task/<task_id>
curl -N http://localhost:3000/task/<task_id>/stream
```

## 5. 输出说明

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

判断结果时优先看：

- `flag`
- `final_report.md`
- `modules/<module>/module_report.md`

## 6. 常见问题

### 没有生成模块

先检查：

- 输入目录是否真的包含可分析文件
- prompt 是否指向了整个目录而不是单个函数
- 模型配置和 API key 是否可用

### 结果目录里只有兜底报告

说明主流程没有形成稳定模块输出。常见原因是：

- 输入树质量太差
- 模型返回异常
- Judge 长期未通过

这时先看 `archive.zip` 中的过程日志。
