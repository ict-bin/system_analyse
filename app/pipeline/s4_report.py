"""
pipeline/s4_report.py — Stage 4: 最终报告

入: ctx.analysed_modules
    workspace/modules/*/module_report.md
出: ctx.final_report_path
    output_dir/final_report.md
    output_dir/modules.list
    output_dir/archive.zip

核心流程:
  Stage 4a: Judge 完整性检查
    → 缺失模块回 Stage 2+3 补做
  Stage 4b: Worker(step4_final_report.md) 生成总报告
    → Judge(step4_check_report.md) 评审
  后处理: 生成 modules.list、归档 zip、写 flag=1
"""
from __future__ import annotations

# TODO: 从 _orchestrator_legacy.py 迁移 Stage 4 逻辑
# 迁移重点:
#   1. Stage 4a 完整性检查 → CompletenessCheckStage
#   2. Stage 4b 报告生成 → FinalReportStage
#   3. 缺失模块回填逻辑用 RefineStage + AnalyseStage 直接调用

from .base import BaseStage
from .context import PipelineContext


class CompletenessCheckStage(BaseStage):
    """Stage 4a: 完整性检查（缺失模块回 Stage 2+3）"""

    stage_num = 4
    stage_name = "完整性检查"

    async def execute(self, ctx: PipelineContext) -> None:
        # TODO: 从 legacy 迁移
        pass


class FinalReportStage(BaseStage):
    """Stage 4b: 生成最终安全分析报告"""

    stage_num = 4
    stage_name = "生成报告"

    async def execute(self, ctx: PipelineContext) -> None:
        # TODO: 从 legacy 迁移
        pass
