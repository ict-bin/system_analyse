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
    """将多个 BaseStage 串联成流水线。

    各 Stage 通过自身的 checkpoint 逆辑决定是否执行，无需外部传入 start_stage。
    """

    def __init__(self, stages: list[BaseStage]):
        self._stages = sorted(stages, key=lambda s: s.stage_num)

    async def run(self, ctx: PipelineContext) -> PipelineContext:
        for stage in self._stages:
            await stage.execute(ctx)
            # Stage 0 过滤后无文件 → 终止流水线，避免后续阶段空跑
            if stage.stage_num == 0 and ctx.filter_count == 0:
                ctx.emit_event(
                    "log",
                    level="warning",
                    msg="[终止] Stage 0 过滤结果为 0 个文件，请检查 binary_arch / analyse_targets 配置是否与实际固件架构匹配，流水线终止。",
                )
                break
        return ctx
