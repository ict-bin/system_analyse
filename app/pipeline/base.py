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

    async def run(self, ctx: PipelineContext, start_stage: int = 1) -> PipelineContext:
        for stage in self._stages:
            if stage.stage_num < start_stage:
                ctx.emit_event(
                    "log",
                    level="info",
                    msg=f"[跳过] Stage {stage.stage_num} ({stage.stage_name})，resume start_stage={start_stage}",
                )
                continue
            await stage.execute(ctx)
        return ctx
