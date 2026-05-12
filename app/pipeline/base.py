"""
pipeline/base.py — 阶段基类
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from .context import PipelineContext


class BaseStage(ABC):
    """每个阶段实现此基类，execute() 负责完成阶段工作并更新 ctx。"""

    @property
    @abstractmethod
    def stage_num(self) -> int:
        """阶段编号（0-4），用于 start_stage 跳过判断。"""

    @property
    @abstractmethod
    def stage_name(self) -> str:
        """阶段名（用于日志）。"""

    @abstractmethod
    async def execute(self, ctx: PipelineContext) -> None:
        """执行阶段逻辑，就地修改 ctx。"""


class Pipeline:
    """将多个 BaseStage 串联成流水线，支持从指定阶段恢复。"""

    def __init__(self, stages: list[BaseStage]):
        self._stages = sorted(stages, key=lambda s: s.stage_num)

    async def run(self, ctx: PipelineContext, start_stage: int = 0) -> PipelineContext:
        for stage in self._stages:
            if stage.stage_num < start_stage:
                ctx.emit_event(
                    "log",
                    level="info",
                    msg=f"[跳过] Stage {stage.stage_num} ({stage.stage_name})，resume start_stage={start_stage}",
                )
                continue
            # Stage 2（细分类）在粗粒度模式下跳过，直接继承 Stage 1 的分类结果
            if stage.stage_num == 2 and getattr(ctx.cfg, "module_granularity", "fine") == "coarse":
                ctx.refined_modules = list(ctx.classified_modules)
                ctx.emit_event(
                    "log",
                    level="info",
                    msg=(
                        f"[跳过] Stage 2 ({stage.stage_name})：module_granularity=coarse，"
                        "粗粒度模式不细分模块，直接继承 Stage 1 分类结果。"
                    ),
                )
                continue
            await stage.execute(ctx)
            # Stage 0 过滤后无文件 → 终止流水线，避免后续阶段空跑
            if stage.stage_num == 0 and ctx.filter_stage_executed and ctx.filter_count == 0:
                ctx.emit_event(
                    "log",
                    level="warning",
                    msg="[终止] Stage 0 过滤结果为 0 个文件，请检查 binary_arch / analyse_targets 配置是否与实际固件架构匹配，流水线终止。",
                )
                break
        return ctx
