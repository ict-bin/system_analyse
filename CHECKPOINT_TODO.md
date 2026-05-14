# 断点续跑重设计 Todo List

## 状态说明
- [ ] 待完成
- [x] 已完成
- [-] 已删除/跳过

---

## Step 1: 新建 checkpoint.py
- [ ] 新建 `app/pipeline/checkpoint.py`
  - [ ] `CheckpointManager.__init__(workspace)` — 创建 `.checkpoint/` 目录结构
  - [ ] `mark_done(name, **extra)` — 原子写入（tmp→rename）
  - [ ] `is_done(name)` — 检查标记是否存在
  - [ ] `clear(name)` — 清除标记（redo场景）
  - [ ] `list_done_modules(stage)` — 列出某stage已完成模块集合
  - [ ] `load_summary()` — 加载所有checkpoint状态（供API查询）
  - [ ] `clear_all()` — 清除所有checkpoint（restart场景）

## Step 2: 改造 PipelineContext
- [ ] `app/pipeline/context.py`
  - [ ] 新增 `checkpoint: Optional[CheckpointManager]` 字段（可选，兼容无checkpoint场景）

## Step 3: 改造 orchestrator.py
- [ ] `app/orchestrator.py`
  - [ ] 删除 `resume_workspace/start_stage` 分支（if cfg.resume_workspace...）
  - [ ] 统一目录初始化逻辑（复用已有workspace）
  - [ ] 初始化 `CheckpointManager` 并注入 `ctx.checkpoint`
  - [ ] `Pipeline.run()` 不再传 `start_stage`

## Step 4: 改造 pipeline/base.py
- [ ] `app/pipeline/base.py`
  - [ ] `Pipeline.run()` 删除 `start_stage` 参数和跳过逻辑
  - [ ] 各stage自主通过checkpoint决定是否执行

## Step 5: 改造 s0_filter.py（4个子阶段）
- [ ] `FilterStage.execute()` — checkpoint: `s0_filter`
  - [ ] 开头检查，跳过时从磁盘重建 `ctx.filtered_files/filter_count`
  - [ ] 完成后写 checkpoint
- [ ] `ExploreStage.execute()` — checkpoint: `s0_explore`
  - [ ] 开头检查跳过
  - [ ] 完成后写 checkpoint
- [ ] `PrescanStage.execute()` — checkpoint: `s0_prescan`
  - [ ] 开头检查跳过（从磁盘重建 `ctx.prescan_summary`）
  - [ ] 完成后写 checkpoint
- [ ] `PathGroupStage.execute()` — checkpoint: `s0_pathgroup`（纯Python可快速重跑，跳过）

## Step 6: 改造 s1_classify.py
- [ ] `ClassifyStage.execute()` — checkpoint: `s1_classify`
  - [ ] 开头检查，跳过时重建 `ctx.classified_modules`
  - [ ] 完成后写 checkpoint

## Step 7: 改造 s1_security_filter.py
- [ ] `SecurityFocusFilterStage.execute()` — checkpoint: `s1_security_filter`
  - [ ] 开头检查跳过
  - [ ] 完成后写 checkpoint

## Step 8: 改造 s2_refine.py（最复杂）
- [ ] `RefineStage.execute()`
  - [ ] 检查整体 `s2_refine.done` → 跳过整体
  - [ ] 读取 `list_done_modules("s2")` 初始化 `self._refined`
  - [ ] 只将未完成模块入队（增量续跑）
  - [ ] `_global_completeness_check` 有独立 `s2_global_check` checkpoint
  - [ ] 整体完成后写 `s2_refine.done`
- [ ] `_refine_one(mod_name)`
  - [ ] 开头检查 `s2_modules/{mod_name}` checkpoint，存在则跳过
  - [ ] judge通过后原子写 `s2_modules/{mod_name}.done`

## Step 9: 改造 s3_analyse.py
- [ ] `AnalyseStage.execute()`
  - [ ] 检查整体 `s3_analyse.done` → 跳过整体，重建 `ctx.analysed_modules`
  - [ ] S2→S3 redo时清除相关模块的S3 checkpoint
  - [ ] 整体完成后写 `s3_analyse.done`
- [ ] `_analyse_module(mod_name)`
  - [ ] 双重保护：checkpoint + report文件都存在才跳过
  - [ ] checkpoint存在但文件丢失 → 清除checkpoint重做
  - [ ] 文件存在但无checkpoint → 删除脏文件重做
  - [ ] judge通过后写 `s3_modules/{mod_name}.done`

## Step 10: 改造 s4_report.py
- [ ] `CompletenessCheckStage.execute()` — checkpoint: `s4_completeness`
  - [ ] 开头检查跳过
  - [ ] 完成后写 checkpoint
- [ ] `FinalReportStage.execute()` — checkpoint: `s4_report`
  - [ ] 开头检查（checkpoint+final_report.md都存在才跳过）
  - [ ] 完成后写 checkpoint（在归档前）

## Step 11: 改造 task_repository.py
- [ ] 删除旧 `resume_task_in_place(resume_workspace=...)` 参数
- [ ] 新版 `resume_task_in_place(db, row)` — 不设置 start_stage/resume_workspace，不清除 started_at/stages_json

## Step 12: 改造 task_service.py
- [ ] `resume_task()` — 新语义：验证 `.checkpoint/` 存在，调用新版 `resume_task_in_place()`
- [ ] `restart_task()` — 新增：清除 workspace 下 `.checkpoint/` 目录
- [ ] `_execute_task()` — 删除对 `start_stage/resume_workspace` 的读取

## Step 13: 改造 models.py 和 config.py
- [ ] `ServiceConfig` — 保留 `start_stage/resume_workspace` 字段但标记为deprecated（兼容）
- [ ] `TaskConfig` — 同上
- [ ] `config.py` `build_task_config()` — 不再传递 `start_stage/resume_workspace`（始终为默认值）

## Step 14: 新增 checkpoint API
- [ ] `app/api/tasks.py` — 新增 `GET /tasks/{id}/checkpoint`
  - [ ] 读取 workspace `.checkpoint/` 目录
  - [ ] 返回各阶段完成状态、时间戳、模块级进度

## Step 15: 改造前端
- [ ] `SystemAnalysisTaskDetailPage.tsx` — 新增断点状态面板
- [ ] `appSystemAnalyse.ts` — 新增 `getTaskCheckpoint()` API调用

## Step 16: 导出新函数 / __init__.py
- [ ] `app/pipeline/__init__.py` — 导出 `CheckpointManager`

## Step 17: 提交代码
- [ ] git add + commit + push (submodule)
- [ ] bump submodule pointer in parent repo

---
*Generated: 2025-05-14*
