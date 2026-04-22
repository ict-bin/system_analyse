"""
pipeline/context.py — 流水线上下文（各阶段共享的状态容器）
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from ..models import TaskConfig, TokenUsage, SwarmEvent


@dataclass
class PipelineContext:
    # ── 基本标识 ──────────────────────────────────────────────
    task_id: str
    task: str

    # ── 配置与路径 ────────────────────────────────────────────
    cfg: "TaskConfig"
    workspace: Path          # workspace 根目录
    output_dir: Path         # task 输出目录（含 workspace/sessions 等）
    sess_dir: Path           # session jsonl 存储目录

    # ── 事件发射（供 CLI 渲染） ───────────────────────────────
    emit: Callable[["SwarmEvent"], None]

    # ── Token 计量 ────────────────────────────────────────────
    tokens: "TokenUsage"

    # ── 取消事件 ──────────────────────────────────────────────
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)

    # ══════════════════════════════════════════════════════════
    # Stage 0 输出
    # ══════════════════════════════════════════════════════════
    filtered_files: list[str] = field(default_factory=list)
    """过滤后的相对路径列表（二进制/文本等）"""

    filter_count: int = 0
    """过滤后文件总数"""

    # ══════════════════════════════════════════════════════════
    # Stage 1 输出
    # ══════════════════════════════════════════════════════════
    classified_modules: list[str] = field(default_factory=list)
    """粗分类后的模块名列表（workspace/modules/<name>/files.list）"""

    # ══════════════════════════════════════════════════════════
    # Stage 2 输出
    # ══════════════════════════════════════════════════════════
    refined_modules: list[str] = field(default_factory=list)
    """细分类后的叶节点模块名列表"""

    # ══════════════════════════════════════════════════════════
    # Stage 3 输出
    # ══════════════════════════════════════════════════════════
    analysed_modules: list[str] = field(default_factory=list)
    """已生成 module_report.md 的模块名列表"""

    modules_needing_reclassify: list[str] = field(default_factory=list)
    """Stage 3 要求重新细分的模块"""

    # ══════════════════════════════════════════════════════════
    # Stage 4 输出
    # ══════════════════════════════════════════════════════════
    final_report_path: str = ""
    """最终报告路径"""

    # ── 辅助方法 ──────────────────────────────────────────────
    def modules_root(self) -> Path:
        """返回 modules 目录（workspace/modules 或 workspace）"""
        m = self.workspace / "modules"
        return m if m.exists() else self.workspace

    def module_dir(self, mod_name: str) -> Path:
        return self.modules_root() / mod_name

    def emit_event(self, event_type: str, **data):
        """便捷 emit，自动带 task_id"""
        from ..models import SwarmEvent
        self.emit(SwarmEvent(type=event_type, data={"task_id": self.task_id, **data}))
