# system-analyse 微服务执行流程与代码架构

## 一、整体架构（三层角色）

```text
┌─────────────────────────────────────────────────────────────┐
│  API Pod (role=api)                                         │
│  接收 HTTP 请求 → 写入 DB → 返回 task_id                    │
│  /api/app/system-analyse/tasks POST/GET/cancel/restart/...  │
└──────────────┬──────────────────────────────────────────────┘
               │ MySQL (pending tasks)
               ▼
┌─────────────────────────────────────────────────────────────┐
│  Worker Pod (role=manager)  — 1 副本                        │
│  WorkerDispatcher._dispatch_once()                          │
│  扫描 pending → 选择 runner → claim lease → dispatch        │
│  不执行 LLM，只做调度                                        │
└──────────────┬──────────────────────────────────────────────┘
               │ dispatcher_instance_id = <runner pod name>
               │ lease_epoch += 1, lease_expires_at = now + 300s
               ▼
┌─────────────────────────────────────────────────────────────┐
│  Runner Pod (role=runner)  — N 副本，每个只跑 1 个任务       │
│  TaskRunner.execute_task(task_id, lease_epoch)              │
│  → Orchestrator.execute(task_id)                            │
│  → Pipeline.run(ctx) → 14 个 Stage 串行                     │
└─────────────────────────────────────────────────────────────┘
```

## 二、任务生命周期

```
create → pending → dispatched → running → passed/failed/error/cancelled
         │                                │
         └─ Worker 扫描认领 ──────────────┘
```

### 2.1 关键入口

| 文件 | 职责 |
|------|------|
| `app/api/tasks.py:1004` | `create_task` → `task_service.create_task()` |
| `app/service/task_service.py` | TaskService 统筹 CRUD、cancel/restart/resume |
| `app/service/worker_dispatcher.py` | WorkerDispatcher 调度 pending→dispatched |
| `app/service/task_runner.py` | TaskRunner 执行：lock→cleanup→orchestrate→persist |
| `app/orchestrator.py` | Orchestrator 组装 Pipeline + 上下文 |
| `app/pipeline/base.py` | Pipeline 串联 14 个 Stage |

### 2.2 Runner 执行流程（task_runner.py）

```
execute_task(task_id, lease_epoch)
  ├─ agent_cleanup pre_task (杀残留 pi/python)
  ├─ _prepare_task_execution (获取 DB 行→build_task_config→persist resolved)
  ├─ Orchestrator.execute(task_id)
  │   ├─ cwd=target_dir, sess_dir, workspace 初始化
  │   ├─ .pi/settings.json 写入 (compaction + thinking off)
  │   ├─ Pipeline.run(ctx)  ← 14 个 Stage
  │   └─ 报告组装 (final_report.md, archive.zip, flag)
  ├─ _persist_task_result (DB finalize + events.jsonl __final__)
  └─ agent_cleanup post_task + clear lock
```

## 三、Pipeline 全貌 — 14 个 Stage

```text
S0 预处理 (7 stages)
  FilterStage           → workspace/filtered_files.txt
  TypeClassifyStage     → workspace/file_catalog.json
  UnknownCheckerStage   → (补充 UNKNOWN 类型)
  ExploreStage          → workspace/keywords.txt
  PrescanStage          → workspace/keyword_summary.txt
  PathGroupStage        → workspace/path_groups.json
  SubReaderStage        → workspace/details/*.json (每个文件一份摘要)
  ValidateDetailsStage  → workspace/details_validation.json

S1 分类 (2 stages)
  ClassifyStage         → workspace/modules/<mod>/files.list
  SecurityFocusFilter   → 过滤掉非安全模块 + 空模块

S2 细分 (1 stage, 并行 × parallel_modules=4)
  RefineStage           → workspace/modules/<mod>/ 拆分/合并
                          → workspace/.s2_snapshots/<mod>.snapshot
                          → workspace/.checkpoint/s2_modules/<mod>.done

S3 分析 (1 stage, 并行 × parallel_modules=4)
  AnalyseStage          → workspace/modules/<mod>/module_report.md
                          → workspace/.checkpoint/s3_modules/<mod>.done

S4 报告 (2 stages)
  CompletenessCheckStage → 完整性校验
  FinalReportStage      → output/final_report.md
                          → output/modules.list
                          → output/archive.zip
```

## 四、各阶段产物传递（全局状态：PipelineContext）

所有 Stage 通过同一个 `PipelineContext` 对象共享状态，无中间文件传递。

### 4.1 PipelineContext 关键字段

| 字段 | 类型 | 写入 Stage | 读取 Stage |
|------|------|-----------|-----------|
| `ctx.workspace` | Path | Orchestrator | 所有 |
| `ctx.target_dir` | str | Orchestrator | Filter～S4 |
| `ctx.filtered_files` | list[str] | Filter | S1, S2 |
| `ctx.file_catalog` | dict | TypeClassify | SubReader, S1 |
| `ctx.refined_modules` | list[str] | Refine | Analyse, S4 |
| `ctx.analysed_modules` | list[str] | Analyse | S4 |
| `ctx.tokens` | TokenUsage | 所有 LLM stage | 任务结束汇总 |
| `ctx.checkpoint` | CheckpointManager | Refine, Analyse | 断点续跑 |
| `ctx.soft_failed_modules` | list[dict] | Analyse | 结果汇总 |
| `ctx.program_error_modules` | list[dict] | Refine | 结果汇总 |
| `ctx.modules_needing_reclassify` | list[str] | Analyse | S2-S3 redo |

### 4.2 阶段产物文件位置（全部在 workspace/ 下）

```text
workspace/                              # = {output_path}/{task_id}/run/workspace/
├── filtered_files.txt                  # S0 Filter 产出
├── file_catalog.json                   # S0 TypeClassify 产出
├── keywords.txt                        # S0 Explore 产出
├── keyword_summary.txt                 # S0 Prescan 产出
├── path_groups.json                    # S0 PathGroup 产出
├── details/                            # S0 SubReader 产出
│   └── <相对路径>.json                 #    每文件一份摘要JSON
├── details_validation.json             # S0 ValidateDetails 产出
├── classify_context.md                 # S1 上下文
├── modules/                            # S1 Classify + S2 Refine 产出
│   ├── <mod_name>/
│   │   ├── files.list                   #   本模块包含的文件路径
│   │   ├── files.list.lock             #   S2 Refine 写锁（0字节标记）
│   │   ├── module_report.md            #   S3 Analyse 产出
│   │   ├── split/                      #   S2 Refine 拆分草稿
│   │   │   ├── <child>/files.list
│   │   │   └── _merge_to/<target>/files.list
│   │   └── deleted/files.list          #   S2 Refine 排除文件
│   └── ...
├── .s2_snapshots/                      # S2 Refine 快照
│   └── <mod_name>.snapshot             #   split前的 files.list 拷贝
├── .checkpoint/                        # 断点续跑
│   ├── s0_filter.done
│   ├── s1_classify.done
│   ├── s2_modules/
│   │   └── <mod_name>.done
│   └── s3_modules/
│       └── <mod_name>.done
├── deleted.list                        # 全局已确认排除文件
├── judge_output/                       # Judge 反馈归档
│   ├── s1_classify/
│   ├── s2_refine/<mod>/
│   ├── s3_analyse/<mod>/
│   └── ...
└── .pi/settings.json                   # pi agent 配置
```

## 五、各阶段详细分析

### 5.1 S0 预处理（7 个 stage，不使用 LLM 或只用无 session 的 LLM）

| Stage | 输入 | 产出 | LLM? |
|-------|------|------|------|
| FilterStage | target_dir | filtered_files.txt, ctx.filtered_files | ❌ Python/Shell |
| TypeClassifyStage | target_dir, filtered_files | file_catalog.json | ❌ Python/Shell |
| UnknownCheckerStage | file_catalog + target_dir | 修正 UNKNOWN 类型 | ✅ MiniMax (read only) |
| ExploreStage | target_dir | keywords.txt | ✅ MiniMax (read only) |
| PrescanStage | target_dir, keywords | keyword_summary.txt | ❌ Shell 脚本 |
| PathGroupStage | filtered_files | path_groups.json | ❌ Python |
| SubReaderStage | file_catalog, path_groups | details/*.json (4557个文件) | ✅ MiniMax × parallel_sub_workers=4 |
| ValidateDetailsStage | details/*.json | details_validation.json | ❌ Python 校验 |

**耗时特征**：
- Filter/TypeClassify/PathGroup/ValidateDetails：秒级，纯 Python
- UnknownChecker/Explore：分钟级，短 LLM
- **SubReader**：4557 文件 × 每个读源码摘要 ≈ **最耗时阶段**（14 分钟）

### 5.2 S1 分类（2 个 stage）

| Stage | 输入 | 产出 | LLM? |
|-------|------|------|------|
| ClassifyStage | filtered_files.txt, details/, classify_context.md | modules/<mod>/files.list (126 个模块) | ✅ W+J 多轮 |
| SecurityFocusFilterStage | modules/ | 6 个模块被移除，剩 120 个 | ✅ MiniMax (coarse filter) |

**产物**：126 → 120 个模块，每个有 `files.list`。

### 5.3 S2 细分（RefineStage）

| 输入 | 产出 | LLM? |
|------|------|------|
| modules/<mod>/files.list, .s2_snapshots/ | split 草稿 → commit → 新模块/合并 | ✅ W+J (parallel=4) |

**核心逻辑**：
```text
for each module (通过 asyncio.Queue, 4 workers 并行):
  _refine_one(mod_name):
    1. 创建快照 .s2_snapshots/<mod>.snapshot = copy of files.list
    2. 从 details/ 加载文件摘要 → file_summary
    3. for attempt in range(max_iter):  # max_rounds=-1 → 无限
       a. Worker(step2_refine.md): 读 files.list + 摘要 → split 草稿
       b. Judge(step2_check_refine.md): check_module.sh 校验 → score
       c. if passed and min_rounds met → commit_split_plan → 新子模块入队 → 写 checkpoint
       d. if failed → write_judge_feedback → 删除 split → 注入反思 → 下一轮
    4. 全局完整性检查 _global_completeness_check:
       filtered_files vs 所有 modules/*/files.list 求差 → reclassify 补分类
       → 新模块入队 → 补快照
```

**耗时特征**：120 个模块 × 每个 1 次 W+J ≈ **最耗时 LLM 阶段**（数小时）

### 5.4 S3 分析（AnalyseStage）

| 输入 | 产出 | LLM? |
|------|------|------|
| modules/<mod>/files.list, details/ | modules/<mod>/module_report.md | ✅ W+J (parallel=4) |

**核心逻辑**：
```text
for each module (asyncio.Semaphore 并行):
  _analyse_module(mod_name):
    1. 从 details/ 加载 pre_read_content → system_prompt
    2. 工具集: tools=["read", "write"]
    3. for attempt in range(max_iter):  # max_rounds=-1 → 无限
       a. Worker(step3_analyse.md): 强制 write → module_report.md
       b. Judge(step3_check_analyse.md): 校验报告完整性 → score
       c. 重分类检测: 如果 [需要重新分类]:
          → ctx.modules_needing_reclassify.append(mod_name)
          → _redo_s2_s3: 重新 S2 refine → 重新 S3 analyse
```

### 5.5 S4 报告（2 个 stage）

| Stage | 输入 | 产出 | LLM? |
|-------|------|------|------|
| CompletenessCheckStage | modules/*/module_report.md | 缺失模块列表 | ✅ Judge |
| FinalReportStage | 所有 module_report.md | final_report.md, modules.list | ✅ Worker + Judge |

## 六、代码架构关键文件

```text
app/
├── orchestrator.py          # Pipeline 组装 + 上下文初始化 + 结果汇总
├── runner.py                # pi agent 进程管理 (RPC mode) + 双层重试
├── config.py                # ServiceYaml → TaskConfig 转换
├── models.py                # 数据模型 (TaskConfig, AgentConfig, TokenUsage)
├── agent_process.py         # pi 进程句柄 (spawn/terminate_tree)
├── probe_server.py          # 独立探针 HTTP server (18080)
├── server.py                # FastAPI 应用主入口
├── api/                     # REST API 路由
│   ├── tasks.py             #   /tasks CRUD, cancel, restart, resume
│   ├── prompts.py           #   Prompt 模板管理
│   ├── config.py            #   项目配置
│   └── admin.py             #   运维控制
├── service/                 # 业务逻辑层
│   ├── task_service.py      #   TaskService 统筹 (CRUD, restore, 锁, 文件操作)
│   ├── task_runner.py       #   TaskRunner 执行 (lock → cleanup → orchestrate)
│   ├── task_repository.py   #   DB 操作 (claim/stale/restart/cancel/finalize)
│   ├── task_query_service.py#   查询/列表/结果/session
│   ├── worker_dispatcher.py #   WorkerDispatcher 调度
│   ├── agent_cleanup.py     #   杀残留 pi/python 进程
│   ├── agent_observability.py#  Agent 进程监控
│   ├── runner_registry_service.py # Runner 注册/心跳
│   ├── worker_slot_snapshot.py    # Worker 容量快照
│   ├── config_service.py    #   项目/运行时配置 DB 存储
│   ├── session_index.py     #   Session 索引构建
│   ├── runtime_bootstrap.py #   启动编排 (DB init → router → registry → worker loop)
│   └── event_log.py         #   events.jsonl 读写 + __final__ 标记
└── pipeline/                # 流水线 14 个 Stage
    ├── base.py              #   BaseStage, Pipeline
    ├── context.py           #   PipelineContext (所有 Stage 共享状态)
    ├── helpers.py           #   共享工具函数 (parse_eval_md, commit_split_plan, ...)
    ├── checkpoint.py        #   CheckpointManager
    ├── evaluation.py        #   EvaluationRecorder
    ├── filter_engine.py     #   文件过滤引擎
    ├── module_dependency.py #   模块依赖图
    ├── self_reflection.py   #   自省分析
    ├── s0_filter.py         #   S0 Filter, Explore, Prescan
    ├── s0_path_group.py     #   S0 PathGroup
    ├── s0_sub_reader.py     #   S0 SubReader
    ├── s0_type_classify.py  #   S0 TypeClassify
    ├── s0_unknown_checker.py#   S0 UnknownChecker
    ├── s0_validate_details.py#  S0 ValidateDetails
    ├── s1_classify.py       #   S1 Classify
    ├── s1_security_filter.py#   S1 SecurityFilter
    ├── s2_refine.py         #   S2 Refine (含全局完整性检查)
    ├── s3_analyse.py        #   S3 Analyse (含 S2/S3 redo 回溯)
    └── s4_report.py         #   S4 CompletenessCheck + FinalReport
```

## 七、关键数据流（任务配置 → 运行配置）

```text
K8s ConfigMap service.yaml
  └→ app/config.py:load_service_yaml() → ServiceYaml
       └→ config_service.py:get_config(project_id) → DB 项目配置
            └→ config_service.py:get_runtime_settings(project_id) → 合并默认值
                 └→ app/config.py:build_task_config(svc, prompt, cwd)
                      └→ TaskConfig(analyse_targets, binary_arch, stages, workers, judges, ...)
                           └→ Orchestrator → PipelineContext
```

## 八、LLM 调用模式

所有 LLM 调用通过 `runner.py` → `pi --mode rpc` 子进程：

```text
run_agent(prompt, model, tools, system_prompt, session_file, ...)
  ├─ _normalize_timeout_seconds(run_timeout_seconds=idle timeout)
  ├─ _run_with_context_overflow_recovery (context overflow → compaction → retry)
  │   └─ _run_with_pi_retry (外层：pi 进程崩溃重试)
  │       └─ _run_with_api_retry (内层：API 错误重试 + stuck 检测)
  │           └─ pi --mode rpc --session <file> --model <m> --tools <t1,t2>
  │               stdin: {"type":"prompt","message":"..."}
  │               stdout: JSONL events → _process_line() 解析
```

**工具集差异**：
| 角色 | 工具集 |
|------|--------|
| Worker (S1, S2) | read, bash, edit, write, grep, find |
| Judge (S1, S2, S3, S4) | read, bash, grep, find |
| Worker (S3) | read, write |
| SubReader (S0) | read |

**Session 模式**：
| 角色 | Session | 原因 |
|------|---------|------|
| Worker | `--session <file>` | 多轮保持上下文 |
| Judge | `--no-session` | 每轮独立判定 |
| SubReader | 无 session | 一次性批量摘要 |
