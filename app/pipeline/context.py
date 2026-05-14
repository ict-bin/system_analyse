"""
pipeline/context.py — 流水线上下文（各阶段共享的状态容器）
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from ..models import TaskConfig, TokenUsage, SwarmEvent, AgentInstanceConfig
    from .evaluation import EvaluationRecorder
    from .checkpoint import CheckpointManager


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

    # ── 多轮评估记录 ────────────────────────────────────────
    evaluator: "EvaluationRecorder | None" = None

    # ── 断点续跑管理器 ──────────────────────────────
    checkpoint: "CheckpointManager | None" = None
    """orchestrator 初始化后注入。None 表示未启用断点续跑。"""

    # ── 取消事件 ──────────────────────────────────────────────
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)

    # ── 输出目录（最终交付件） ────────────────────────────────
    final_out_dir: Path = field(default_factory=lambda: Path("."))
    """最终交付件目录（{task_id}/output/）"""

    flag_path: Path = field(default_factory=lambda: Path("flag"))
    """完成标志文件（0=运行中/失败，1=成功）"""

    # ── Stage 0 → Stage 1 传递的预扫描摘要 ────────────────────
    prescan_summary: str = ""

    # ══════════════════════════════════════════════════════════
    # Stage 0 输出
    # ══════════════════════════════════════════════════════════
    filtered_files: list[str] = field(default_factory=list)
    """过滤后的相对路径列表（二进制/文本等）"""

    filter_count: int = 0
    """过滤后文件总数"""

    selected_filter_engine: str = "script"
    """配置选择的过滤引擎"""

    effective_filter_engine: str = "script"
    """实际执行成功的过滤引擎"""

    filter_fallback_reason: str = ""
    """agent 引擎回退到脚本引擎的原因"""

    # ══════════════════════════════════════════════════════════
    # Stage 1 输出
    # ══════════════════════════════════════════════════════════

    # ── 预处理新增字段 ────────────────────────────────────────────────
    file_catalog: dict = field(default_factory=dict)
    # TypeClassifyStage 产出的 file_catalog.json 解析结果

    details_dir: Path | None = None
    # workspace/details/ 目录，SubReaderStage 完成后注入，None=尚未生成

    classify_context_path: Path | None = None
    # workspace/classify_context.md，SubReaderStage 生成，ClassifyStage 注入 prompt

    invalid_detail_files: list[str] = field(default_factory=list)
    # ValidateDetailsStage 发现的无效 details JSON 列表

    unknown_files: list[str] = field(default_factory=list)
    # TypeClassifyStage 识别出的 UNKNOWN 类型文件列表

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

    @property
    def deleted_list_path(self) -> Path:
        """workspace/deleted.list：全局已确认排除文件（append-only，asyncio.Lock 保护）。"""
        return self.workspace / "deleted.list"

    def load_confirmed_deleted(self) -> set[str]:
        """从 workspace/deleted.list 读取已确认排除文件集合；文件不存在返回空集合。"""
        p = self.deleted_list_path
        if not p.exists():
            return set()
        return {ln.strip() for ln in p.read_text("utf-8", errors="replace").splitlines() if ln.strip()}

    def modules_root(self) -> Path:
        """返回 modules 目录（workspace/modules 或 workspace）"""
        m = self.workspace / "modules"
        return m if m.exists() else self.workspace

    def module_dir(self, mod_name: str) -> Path:
        return self.modules_root() / mod_name

    def session_path(self, *parts: str) -> str:
        """返回 session 文件路径，并确保父目录存在。"""
        path = self.sess_dir.joinpath(*parts)
        path.parent.mkdir(parents=True, exist_ok=True)
        return str(path)

    def emit_event(self, event_type: str, **data):
        """便捷 emit，自动带 task_id"""
        from ..models import SwarmEvent
        self.emit(SwarmEvent(type=event_type, task_id=self.task_id, data=data))

    def record_evaluation_round(self, **kwargs):
        """Best-effort round evaluation persistence; never breaks analysis."""
        if not self.evaluator:
            return None
        try:
            return self.evaluator.record_round(**kwargs)
        except Exception as exc:
            self.emit_event(
                "log",
                level="warn",
                msg=f"evaluation round write failed: {exc}",
            )
            return None

    # ── Worker / Judge 参数构建 ────────────────────────────────

    @property
    def task_tmp(self) -> Path:
        """临时目录 workspace/tmp"""
        return self.workspace / "tmp"

    @property
    def j_cfgs(self) -> "list[AgentInstanceConfig]":
        return self.cfg.judges.agents

    @property
    def j_count(self) -> int:
        return len(self.cfg.judges.agents)

    def wm(self, stage: str) -> str:
        """获取 Worker 在指定阶段使用的模型。"""
        return self.cfg.workers.model_for(stage)

    def jm(self, stage: str, j_item: "AgentInstanceConfig") -> str:
        """获取 Judge 在指定阶段使用的模型。"""
        sm = self.cfg.judges.model_for(stage)
        return sm if sm else j_item.model

    def make_w_base(self) -> dict:
        """构建 Worker 的公共 kwargs（tools/cwd/env/thinking_level 等）。"""
        from ..models import AgentInstanceConfig
        w_cfg = (self.cfg.workers.agents[0] if self.cfg.workers.agents
                 else AgentInstanceConfig(model=""))
        return {
            "tools": w_cfg.tools or self.cfg.workers.default_tools,
            "cwd": str(self.workspace),
            "env": {**os.environ,
                    "TMPDIR": str(self.task_tmp),
                    "HOME": str(self.workspace)},
            "thinking_level": w_cfg.thinking_level or self.cfg.workers.default_thinking_level,
            "cancel_event": self.cancel_event,
            "max_retries": self.cfg.agent_max_retries,
            "retry_delay": self.cfg.agent_retry_delay,
            "run_timeout_seconds": self.cfg.agent_run_timeout_seconds,
            "timeout_retry_enabled": self.cfg.agent_timeout_retry_enabled,
            "timeout_max_retries": self.cfg.agent_timeout_max_retries,
            "pi_max_retries": self.cfg.pi_max_retries,
            "pi_retry_delay": self.cfg.pi_retry_delay,
        }

    def make_j_base(self) -> dict:
        """构建 Judge 的公共 kwargs（无 tools/cwd）。session_file 由各阶段按需单独传入。"""
        return {
            "thinking_level": self.cfg.judges.default_thinking_level or "off",
            "cancel_event": self.cancel_event,
            "max_retries": self.cfg.agent_max_retries,
            "retry_delay": self.cfg.agent_retry_delay,
            "run_timeout_seconds": self.cfg.agent_run_timeout_seconds,
            "timeout_retry_enabled": self.cfg.agent_timeout_retry_enabled,
            "timeout_max_retries": self.cfg.agent_timeout_max_retries,
            "pi_max_retries": self.cfg.pi_max_retries,
            "pi_retry_delay": self.cfg.pi_retry_delay,
        }
