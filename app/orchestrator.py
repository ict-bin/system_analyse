"""
orchestrator.py — 薄层流水线编排器 v3

职责：
  1. 从 TaskConfig 构建 PipelineContext
  2. 组装 Stage 列表并运行 Pipeline
  3. 处理收尾（归档、flag、报告路径）

各阶段逻辑分别在：
  pipeline/s0_filter.py   Stage 0 — 文件过滤 + 探索 + 预扫描
  pipeline/s1_classify.py Stage 1 — 粗分类
  pipeline/s2_refine.py   Stage 2 — 细分类（含子Worker + 全局补分类）
  pipeline/s3_analyse.py  Stage 3 — 模块分析（含重分类回S2）
  pipeline/s4_report.py   Stage 4 — 最终报告
"""
from __future__ import annotations

import asyncio
import time
import uuid
from pathlib import Path
from typing import Callable, Any

from .models import TaskConfig, TaskResult, TaskStatus, TokenUsage, SwarmEvent
from .pipeline import (
    PipelineContext, Pipeline,
    FilterStage, ExploreStage, PrescanStage,
)

# ── 尚未迁移的阶段暂时从 legacy 引入 ──────────────────────────────────────────
from ._orchestrator_legacy import (
    Orchestrator as _LegacyOrchestrator,
)


def make_id() -> str:
    return f"task-{int(time.time())}-{uuid.uuid4().hex[:8]}"


class Orchestrator:
    """
    薄层编排器：组装流水线并执行。

    当前过渡状态：
    - Stage 0（Filter/Explore/Prescan）已迁移到 pipeline/ 模块
    - Stage 1-4 仍使用 legacy orchestrator（_orchestrator_legacy.py）
    - 后续逐步把 s1/s2/s3/s4 迁移到各自 pipeline/sN_*.py
    """

    def __init__(
        self,
        config: TaskConfig,
        on_event: Callable[[SwarmEvent], None] | None = None,
    ):
        self.cfg = config
        self._on_event = on_event
        # 过渡期：使用 legacy 保证完整功能
        self._legacy = _LegacyOrchestrator(config, on_event)

    async def execute(self, task_id: str | None = None) -> TaskResult:
        """执行完整流水线。"""
        return await self._legacy.execute(task_id)

    def stop(self) -> None:
        self._legacy.stop()
