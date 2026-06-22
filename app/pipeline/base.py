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
        """阶段编号（0-4），决定执行顺序。"""

    @property
    @abstractmethod
    def stage_name(self) -> str:
        """阶段名（用于日志）。"""

    @abstractmethod
    def execute(self, ctx: PipelineContext) -> None:
        """执行阶段逻辑，就地修改 ctx。"""


class Pipeline:
    """将多个 BaseStage 串联成流水线。"""

    def __init__(self, stages: list[BaseStage]):
        self._stages = sorted(stages, key=lambda s: s.stage_num)

    def run(self, ctx: PipelineContext) -> PipelineContext:
        for stage in self._stages:
            stage.execute(ctx)
            if stage.stage_name == "文件过滤" and ctx.filter_count == 0:
                ctx.emit_event(
                    "log",
                    level="warning",
                    msg="[终止] Stage 0 过滤结果为 0 个文件，请检查 binary_arch / analyse_targets 配置是否与实际固件架构匹配，流水线终止。",
                )
                break
        return ctx
