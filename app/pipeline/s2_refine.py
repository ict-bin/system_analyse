"""
pipeline/s2_refine.py — Stage 2: 细分类

入: ctx.classified_modules
    workspace/modules/*/files.list
出: ctx.refined_modules
    workspace/modules/*/files.list (可能重组)
    workspace/.s2_snapshots/*.snapshot

核心流程:
  对每个模块并行运行:
    Python预读 → Worker(step2_refine.md) → Judge(step2_check_refine.md)
  Stage 2 后全局检查: filtered_files.txt vs 所有 files.list
  遗漏文件用 W+J 补分类(step2_reclassify.md)

并发控制: asyncio.Queue + parallel_modules 个 worker
分裂检测: 快照前后 modules 目录对比
"""
from __future__ import annotations

# 注意: 当前 Stage 2 完整实现仍在 _orchestrator_legacy.py
# 本文件为架构占位，标记接口契约
# 迁移计划:
#   1. 把 _refine_one() 迁移到此文件的 RefineStage._refine_one()
#   2. 把 _s2_worker() 改为 asyncio.Queue 的 consumer
#   3. 把全局补分类移到 _global_completeness_check()

from .base import BaseStage
from .context import PipelineContext
from .helpers import discover_modules


class RefineStage(BaseStage):
    """Stage 2: 细分类（含子Worker摘要生成 + 全局补分类）"""

    stage_num = 2
    stage_name = "细分"

    async def execute(self, ctx: PipelineContext) -> None:
        # TODO: 从 _orchestrator_legacy.py 迁移 Stage 2 逻辑
        # 当前由 legacy orchestrator 负责执行，此处仅更新 ctx
        ctx.refined_modules = discover_modules(str(ctx.workspace))
