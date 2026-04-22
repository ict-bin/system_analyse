"""
pipeline/s3_analyse.py — Stage 3: 模块分析 (STRIDE)

入: ctx.refined_modules
    workspace/modules/*/files.list
出: ctx.analysed_modules
    workspace/modules/*/module_report.md
    ctx.modules_needing_reclassify → 触发 Stage 2 重做

核心流程:
  对每个模块并行运行:
    (可选)子Worker摘要 → Worker(step3_analyse.md) → Judge(step3_check_analyse.md)
  重分类检测: judge 输出含 [需要重新分类] → 回 Stage 2

并发控制: asyncio.Semaphore(parallel_modules)

关键修复点:
  - cwd=workspace（非 mod_dir），prompt 中注入完整路径
  - 二进制文件用 bash strings，不用 read
"""
from __future__ import annotations

# TODO: 从 _orchestrator_legacy.py 迁移 Stage 3 逻辑
# 迁移重点:
#   1. _analyse_one() → AnalyseStage._analyse_one()
#   2. cwd=workspace + prompt 路径注入（已在 legacy 修复）
#   3. modules_needing_reclassify 写入 ctx
#   4. 重分类后回调 RefineStage + 再次 AnalyseStage

from .base import BaseStage
from .context import PipelineContext
from pathlib import Path


class AnalyseStage(BaseStage):
    """Stage 3: 模块 STRIDE 分析"""

    stage_num = 3
    stage_name = "分析"

    async def execute(self, ctx: PipelineContext) -> None:
        # TODO: 从 legacy 迁移
        # 迁移后 ctx.analysed_modules 和 ctx.modules_needing_reclassify 由此填充
        mods_root = ctx.modules_root()
        ctx.analysed_modules = [
            d.name for d in mods_root.iterdir()
            if d.is_dir() and (d / "module_report.md").exists()
        ]
