"""
pipeline/context.py — 流水线上下文（各阶段共享的状态容器）
"""
from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from ..models import TaskConfig, TokenUsage, SwarmEvent, AgentInstanceConfig
    from .evaluation import EvaluationRecorder


logger = logging.getLogger("sa.pipeline")


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

    # ── 取消事件 ──────────────────────────────────────────────
    cancel_event: threading.Event = field(default_factory=threading.Event)

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

    total_input_file_count: int = 0
    """过滤前输入文件总数"""

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
    """TypeClassifyStage 产出的 file_catalog.json 解析结果"""

    details_dir: Path | None = None
    """workspace/details/ 目录，SubReaderStage 完成后注入，None=尚未生成"""

    classify_context_path: Path | None = None
    """workspace/classify_context.md，SubReaderStage 生成，ClassifyStage 注入 prompt"""

    path_group_map: dict[str, str] = field(default_factory=dict)
    """PathGroupStage v2 产出: 文件路径 → 路径推断模块名 的映射"""

    # ── 模块依赖图 ──────────────────────────────────────────────────
    module_dependency_graph: dict | None = None
    """S3 用: orchestrator 在 S2 后构建，S3 做风险排序"""

    invalid_detail_files: list[str] = field(default_factory=list)
    """ValidateDetailsStage 发现的无效 details JSON 列表"""

    unknown_files: list[str] = field(default_factory=list)
    """TypeClassifyStage 识别出的 UNKNOWN 类型文件列表"""

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

    soft_failed_modules: list[dict] = field(default_factory=list)
    """记录被允许跳过的模块级失败，供汇总与日志使用"""

    program_error_modules: list[dict] = field(default_factory=list)
    """记录触发了 fatal 异常的程序性错误，供前端展示和数据库存证"""

    # ══════════════════════════════════════════════════════════
    # Stage 4 输出
    # ══════════════════════════════════════════════════════════
    final_report_path: str = ""
    """最终报告路径"""

    # ── 辅助方法 ──────────────────────────────────────────────

    @property
    def deleted_list_path(self) -> Path:
        """workspace/deleted.list：全局已确认排除文件（append-only，threading.Lock 保护）。"""
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

    def judge_feedback_path(
        self,
        stage_key: str,
        module_name: "str | None",
        attempt: int,
    ) -> str:
        """返回 judge feedback 文件的相对路径字符串（相对于 workspace）。"""
        if module_name:
            return f"judge_output/{stage_key}/{module_name}/feedback_a{attempt}.md"
        else:
            return f"judge_output/{stage_key}/feedback_a{attempt}.md"

    def emit_event(self, event_type: str, **data):
        """便捷 emit，自动带 task_id"""
        from ..models import SwarmEvent
        self.emit(SwarmEvent(type=event_type, task_id=self.task_id, data=data))
        self._log_module_event(event_type, data)

    def _log_module_event(self, event_type: str, data: dict) -> None:
        """将带模块信息的阶段事件镜像到服务日志，便于排查卡在哪个模块。"""
        module_name = str(data.get("module") or data.get("module_name") or "").strip()
        modules = [str(item).strip() for item in (data.get("modules") or []) if str(item).strip()]
        if not module_name and not modules:
            return

        stage = data.get("stage")
        prefix = f"[{stage}]" if stage not in (None, "") else f"[{event_type}]"
        level_name = str(data.get("level") or "info").lower()
        level = {
            "debug": logging.DEBUG,
            "warn": logging.WARNING,
            "warning": logging.WARNING,
            "error": logging.ERROR,
        }.get(level_name, logging.INFO)

        if event_type == "stage":
            if module_name:
                attempt = data.get("attempt")
                suffix = f" 第{attempt}轮" if attempt else ""
                logger.log(level, "%s 开始处理模块: %s%s", prefix, module_name, suffix)
                return
            logger.log(level, "%s 开始处理模块集合: %s", prefix, ", ".join(modules))
            return

        if event_type == "stage_result":
            if module_name:
                extra_parts: list[str] = []
                if "split" in data:
                    extra_parts.append(f"split={bool(data.get('split'))}")
                if data.get("new_modules"):
                    extra_parts.append("new_modules=" + ", ".join(str(item) for item in (data.get("new_modules") or [])))
                suffix = f" ({'; '.join(extra_parts)})" if extra_parts else ""
                logger.log(level, "%s 模块处理完成: %s%s", prefix, module_name, suffix)
                return
            logger.log(level, "%s 模块集合处理完成: %s", prefix, ", ".join(modules))
            return

        if event_type == "judge_eval" and module_name:
            judge_id = data.get("judge_id") or "judge"
            passed = data.get("passed")
            score = data.get("score")
            logger.log(level, "%s 模块评审: %s judge=%s passed=%s score=%s", prefix, module_name, judge_id, passed, score)
            return

        if event_type == "reflect" and module_name:
            round_no = data.get("round")
            suffix = f" 第{round_no}轮" if round_no else ""
            logger.log(level, "%s 模块进入反思: %s%s", prefix, module_name, suffix)
            return

        if event_type == "reclassify" and module_name:
            logger.log(level, "%s 模块需要重新分类: %s", prefix, module_name)
            return

        logger.log(level, "%s 模块相关事件: %s", prefix, module_name or ", ".join(modules))

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

    @property
    def continue_on_module_failure(self) -> bool:
        return bool(getattr(self.cfg, "continue_on_module_failure", True))

    def record_soft_module_failure(
        self,
        *,
        stage: str,
        module_name: str,
        error: str,
        session_file: str = "",
        artifact_paths: list[str] | None = None,
        extra: dict | None = None,
        record_round: bool = True,
    ) -> None:
        payload = {
            "stage": stage,
            "module_name": module_name,
            "error": error,
            "session_file": session_file,
            "artifact_paths": artifact_paths or [],
        }
        if extra:
            payload["extra"] = extra
        self.soft_failed_modules.append(payload)
        self.emit_event(
            "log",
            level="warn",
            msg=f"[{stage}] 模块 {module_name} 失败，但任务按配置继续推进: {error}",
        )
        if record_round:
            now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
            self.record_evaluation_round(
                module_name=module_name,
                stage=stage,
                stage_round=0,
                status="failed",
                started_at=now,
                ended_at=now,
                duration_ms=0.0,
                worker={
                    "model": self.wm("analyse" if stage in {"analyse", "3-redo-s4"} else "refine"),
                    "session_file": session_file,
                    "token_usage": None,
                    "error": error,
                },
                judges=[],
                passed_by_vote=False,
                module_completed=False,
                completion_reason="error",
                artifact_paths=artifact_paths or [],
                extra=extra,
            )

    def record_module_program_error(
        self,
        *,
        stage: str,
        module_name: str,
        error_type: str,
        error_message: str,
        traceback_text: str = "",
        artifact_paths: list[str] | None = None,
    ) -> None:
        payload = {
            "stage": stage,
            "module_name": module_name,
            "error_type": error_type,
            "error_message": error_message,
            "traceback": traceback_text,
            "artifact_paths": artifact_paths or [],
        }
        self.program_error_modules.append(payload)
        self.emit_event(
            "log",
            level="error",
            msg=f"[{stage}] 模块 {module_name} 程序性错误 ({error_type}): {error_message}",
        )

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
            "pi_max_retries": self.cfg.pi_max_retries,
            "pi_retry_delay": self.cfg.pi_retry_delay,
            "task_pi_dir": self.cfg.role_pi_dir("workers"),
            "agent_role": "workers",
            "model_stuck_timeout": getattr(self.cfg, "model_stuck_timeout", 1800.0),
            "model_stuck_max_activations": getattr(self.cfg, "model_stuck_max_activations", 5),
        }

    def make_j_base(self) -> dict:
        """构建 Judge 的公共 kwargs。"""
        return {
            "thinking_level": self.cfg.judges.default_thinking_level or "off",
            "cancel_event": self.cancel_event,
            "max_retries": self.cfg.agent_max_retries,
            "retry_delay": self.cfg.agent_retry_delay,
            "pi_max_retries": self.cfg.pi_max_retries,
            "pi_retry_delay": self.cfg.pi_retry_delay,
            "task_pi_dir": self.cfg.role_pi_dir("judges"),
            "agent_role": "judges",
            "model_stuck_timeout": getattr(self.cfg, "model_stuck_timeout", 1800.0),
            "model_stuck_max_activations": getattr(self.cfg, "model_stuck_max_activations", 5),
        }
