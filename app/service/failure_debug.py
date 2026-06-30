"""failure_debug.py — 任务失败时 LLM 自动调试，生成故障定位报告。

独立角色 `debugger`（单独 Pod），不影响调度器/worker。后台轮询 DB 中失败任务
（status=failed/error），对每个尚无报告的任务启动一次 pi Agent 调试，输出：
问题现象 / 问题根因 / 解决方法 / 代码现场 / 补丁代码。

报告存放：NFS 默认输出目录 {OUTPUT_DIR}/{task_id}/output/failure_debug_report.{md,json}
索引：DB 表 secflow_app_sa_failure_debug（供前端列表/详情/下载）。

约束：纯 threading + time.sleep，无 asyncio。
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Any

from app.config import OUTPUT_DIR
from app.db import _SessionLocal
from app.db.models import AppSaFailureDebug, AppSaTask
from app.service.event_log import read_events

logger = logging.getLogger("sa.failure_debug")

POLL_INTERVAL = float(os.environ.get("SA_FAILURE_DEBUG_POLL_INTERVAL", "30"))
BATCH_SIZE = int(os.environ.get("SA_FAILURE_DEBUG_BATCH", "5"))
DEBUG_MODEL = os.environ.get("SA_FAILURE_DEBUG_MODEL", "").strip()
SOURCE_ROOT = os.environ.get("SA_FAILURE_DEBUG_SOURCE_ROOT", "/app")
PI_DIR = os.environ.get("PI_CODING_AGENT_DIR", "/root/.pi/agent")
MAX_EVENT_CONTEXT = int(os.environ.get("SA_FAILURE_DEBUG_MAX_EVENTS", "60"))
RUN_TIMEOUT = float(os.environ.get("SA_FAILURE_DEBUG_TIMEOUT", "600"))

# 失败状态集合
_FAILED_STATUSES = ("failed", "error")

_instance: "FailureDebugService | None" = None
_lock = threading.Lock()


class FailureDebugService:
    """单例：后台轮询失败任务并执行 LLM 调试。"""

    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop, name="sa_failure_debug", daemon=True
        )
        self._thread.start()
        logger.info("FailureDebugService started (poll=%ss batch=%s)", POLL_INTERVAL, BATCH_SIZE)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
        self._thread = None

    # ── 主循环 ────────────────────────────────────────────────────────────
    def _loop(self) -> None:
        # 启动时等 DB 就绪（runtime_bootstrap 在 DB ready 后才 start，但留个保险）
        while not self._stop_event.wait(timeout=5.0):
            if _SessionLocal is None:
                continue
            try:
                self._poll_and_debug()
            except Exception:
                logger.exception("failure_debug loop error")
            self._stop_event.wait(timeout=POLL_INTERVAL)

    def _poll_and_debug(self) -> None:
        db = _SessionLocal()
        try:
            # 查失败任务中尚无报告（或报告处于 error 可重试）的
            from sqlalchemy import select

            existing = select(AppSaFailureDebug.task_id).where(
                AppSaFailureDebug.status.in_(("pending", "running", "done"))
            )
            tasks = (
                db.query(AppSaTask)
                .filter(
                    AppSaTask.status.in_(_FAILED_STATUSES),
                    AppSaTask.is_deleted == False,  # noqa: E712
                    ~AppSaTask.task_id.in_(existing),
                )
                # MySQL 不支持 NULLS LAST；用 IS NULL 把无 finished_at 的排到最后
                .order_by(AppSaTask.finished_at.is_(None), AppSaTask.finished_at.desc(), AppSaTask.created_at.desc())
                .limit(BATCH_SIZE)
                .all()
            )
            for t in tasks:
                if self._stop_event.is_set():
                    break
                try:
                    self._debug_one(db, t)
                except Exception:
                    logger.exception("debug failed for task %s", t.task_id)
        finally:
            db.close()

    # ── 单任务调试 ────────────────────────────────────────────────────────
    def _debug_one(self, db, task: AppSaTask) -> None:
        # 创建/获取报告行
        row = db.query(AppSaFailureDebug).filter(AppSaFailureDebug.task_id == task.task_id).first()
        if row is None:
            row = AppSaFailureDebug(
                task_id=task.task_id,
                project_id=task.project_id,
                task_name=task.task_name,
                status="running",
            )
            db.add(row)
        else:
            row.status = "running"
            row.debug_error = None
        db.commit()
        db.refresh(row)
        report_id = row.id

        try:
            context = self._collect_context(task)
            report = self._run_llm_debug(task, context)
            self._save_report(task, report)
            row.status = "done"
            row.error_kind = context.get("error_kind")
            row.failing_stage = context.get("failing_stage")
            row.summary = (report.get("phenomenon") or "")[:500]
            row.report_path = self._report_md_path(task)
            row.report_json = report
            row.debug_error = None
            db.commit()
            logger.info("failure debug done for task %s (report_id=%s)", task.task_id, report_id)
        except Exception as exc:
            db.rollback()
            # 重新取行（rollback 后可能 expired）
            row = db.query(AppSaFailureDebug).filter(AppSaFailureDebug.task_id == task.task_id).first()
            if row:
                row.status = "error"
                row.debug_error = str(exc)[:2000]
                db.commit()
            logger.exception("failure debug error for task %s: %s", task.task_id, exc)

    # ── 收集错误上下文 ────────────────────────────────────────────────────
    def _collect_context(self, task: AppSaTask) -> dict[str, Any]:
        output_path = task.output_path or OUTPUT_DIR
        task_root = Path(output_path) / task.task_id
        events_path = task_root / "run" / "events.jsonl"
        ev_data = read_events(events_path if events_path.is_file() else None)
        events = ev_data.get("events") or []
        # 取最后 N 条
        tail = events[-MAX_EVENT_CONTEXT:] if len(events) > MAX_EVENT_CONTEXT else events

        # 推断失败阶段 + error_kind
        failing_stage = None
        error_kind = None
        for ev in reversed(events):
            etype = str(ev.get("event_type") or ev.get("type") or "")
            level = str(ev.get("level") or "")
            stage = ev.get("stage") or ev.get("stage_name")
            if level in ("error", "warn") or "error" in etype or "fail" in etype or etype == "stage_error":
                failing_stage = failing_stage or (str(stage) if stage else None)
                if not error_kind:
                    error_kind = etype or level
                break
            if stage and not failing_stage:
                failing_stage = str(stage)

        # 异常原因 JSON
        abnormal = task.latest_abnormal_reason_json or {}

        return {
            "task_id": task.task_id,
            "task_name": task.task_name,
            "project_id": task.project_id,
            "status": task.status,
            "error_msg": task.error or "",
            "analysis_mode": task.analysis_mode or "",
            "error_kind": error_kind or (abnormal.get("error_kind") if isinstance(abnormal, dict) else None),
            "failing_stage": failing_stage or (abnormal.get("stage") if isinstance(abnormal, dict) else None),
            "abnormal_reason": json.dumps(abnormal, ensure_ascii=False) if abnormal else "",
            "events_tail": tail,
            "events_total": len(events),
        }

    # ── 运行 LLM 调试 ─────────────────────────────────────────────────────
    def _run_llm_debug(self, task: AppSaTask, context: dict[str, Any]) -> dict[str, Any]:
        from app.runner import run_agent  # 延迟导入避免循环

        model = DEBUG_MODEL or self._pick_default_model()
        if not model:
            raise RuntimeError("无可用 LLM 模型（models.json 为空或未配置 SA_FAILURE_DEBUG_MODEL）")

        events_text = self._format_events(context.get("events_tail") or [])
        prompt = self._build_prompt(context, events_text)

        ar = run_agent(
            prompt=prompt,
            model=model,
            tools=["read", "bash", "grep", "glob"],
            system_prompt=self._system_prompt(),
            cwd=SOURCE_ROOT,
            task_pi_dir=PI_DIR,
            agent_role="failure_debugger",
            max_retries=2,
            retry_delay=10.0,
            run_timeout_seconds=RUN_TIMEOUT,
            timeout_retry_enabled=False,
            pi_max_retries=1,
        )

        output = (ar.output or "").strip()
        if not output and ar.error:
            raise RuntimeError(f"pi 调试无输出，错误: {ar.error}")
        if ar.fatal:
            raise RuntimeError(f"pi 致命错误: {ar.error or output[:200]}")

        report = self._parse_report(output)
        report["_model"] = model
        report["_raw_output"] = output[:8000]
        return report

    def _pick_default_model(self) -> str:
        """从 models.json 选第一个可用模型 id。"""
        try:
            models_path = Path(PI_DIR) / "models.json"
            if not models_path.is_file():
                return ""
            data = json.loads(models_path.read_text(encoding="utf-8"))
            for _key, prov in (data.get("providers") or {}).items():
                for m in prov.get("models") or []:
                    mid = m.get("id")
                    if mid:
                        return str(mid)
        except Exception:
            logger.exception("pick_default_model failed")
        return ""

    # ── prompt 构建 ───────────────────────────────────────────────────────
    def _system_prompt(self) -> str:
        return (
            "你是系统分析服务（secflow-app-system-analyse）的故障调试专家。\n"
            "当一个分析任务失败时，你需要：\n"
            "1. 使用 read/bash/grep/glob 工具检查 /app 下的服务源码（Python）\n"
            "2. 结合错误信息和事件时间线，定位导致失败的代码位置\n"
            "3. 给出问题根因分析和修复建议\n"
            "4. 最终只输出一个 JSON 代码块，严格按指定格式，不要输出其他内容\n"
        )

    def _build_prompt(self, ctx: dict[str, Any], events_text: str) -> str:
        return (
            f"# 任务失败调试\n\n"
            f"## 任务信息\n"
            f"- task_id: {ctx.get('task_id')}\n"
            f"- task_name: {ctx.get('task_name')}\n"
            f"- 分析模式: {ctx.get('analysis_mode') or '未知'}\n"
            f"- 失败阶段: {ctx.get('failing_stage') or '未知'}\n"
            f"- 错误类型: {ctx.get('error_kind') or '未知'}\n\n"
            f"## 错误信息\n```\n{ctx.get('error_msg') or '(无)'}\n```\n\n"
            f"## 异常原因(JSON)\n```\n{ctx.get('abnormal_reason') or '(无)'}\n```\n\n"
            f"## 事件时间线(最后{len(ctx.get('events_tail') or [])}条，共{ctx.get('events_total',0)}条)\n"
            f"{events_text}\n\n"
            f"## 你的任务\n"
            f"服务源码位于 /app（app/pipeline/ 下是各阶段实现，app/runner.py 是 pi 调用，"
            f"app/orchestrator.py 是编排，app/service/ 是服务层）。\n"
            f"请用 read/bash/grep/glob 工具检查相关源码，定位失败根因，然后输出报告。\n\n"
            f"## 输出格式（只输出一个 JSON 代码块，不要任何额外文字）\n"
            f"```json\n"
            f"{{\n"
            f'  "phenomenon": "问题现象：观察到的错误现象，结合事件时间线描述",\n'
            f'  "root_cause": "问题根因：为什么会发生此失败，涉及哪个组件/代码逻辑",\n'
            f'  "solution": "解决方法：如何修复，步骤清晰",\n'
            f'  "code_scene": "代码现场：文件路径:行号 + 相关代码片段（用```包裹）",\n'
            f'  "patch_code": "补丁代码：建议的修复补丁（diff 或完整函数代码，用```包裹）"\n'
            f"}}\n"
            f"```\n"
        )

    def _format_events(self, events: list[dict]) -> str:
        if not events:
            return "(无事件)"
        lines = []
        for ev in events:
            ts = ev.get("ts") or ev.get("timestamp") or ev.get("created_at") or ""
            etype = ev.get("event_type") or ev.get("type") or ""
            level = ev.get("level") or "info"
            stage = ev.get("stage") or ev.get("stage_name") or ""
            msg = ev.get("message") or ev.get("msg") or ""
            if isinstance(msg, (dict, list)):
                msg = json.dumps(msg, ensure_ascii=False)
            lines.append(f"[{ts}] [{level}] {stage}/{etype}: {msg}")
        return "\n".join(lines)

    # ── 解析 pi 输出 ──────────────────────────────────────────────────────
    def _parse_report(self, output: str) -> dict[str, Any]:
        # 优先找 ```json ... ``` 代码块
        m = re.search(r"```json\s*(\{.*?\})\s*```", output, re.DOTALL)
        if not m:
            # 回退：找第一个 { ... } 平衡块
            m = re.search(r"(\{.*\})", output, re.DOTALL)
        if not m:
            # 整个输出当 phenomenon
            return {"phenomenon": output[:4000], "root_cause": "", "solution": "",
                    "code_scene": "", "patch_code": ""}
        try:
            data = json.loads(m.group(1))
        except json.JSONDecodeError:
            return {"phenomenon": output[:4000], "root_cause": "", "solution": "",
                    "code_scene": "", "patch_code": ""}
        # 补全缺失字段
        for k in ("phenomenon", "root_cause", "solution", "code_scene", "patch_code"):
            if k not in data or not isinstance(data[k], str):
                data[k] = str(data.get(k, "")) if data.get(k) is not None else ""
        return data

    # ── 报告存储 ──────────────────────────────────────────────────────────
    def _report_md_path(self, task: AppSaTask) -> str:
        output_path = task.output_path or OUTPUT_DIR
        return str(Path(output_path) / task.task_id / "output" / "failure_debug_report.md")

    def _save_report(self, task: AppSaTask, report: dict[str, Any]) -> None:
        output_path = task.output_path or OUTPUT_DIR
        out_dir = Path(output_path) / task.task_id / "output"
        out_dir.mkdir(parents=True, exist_ok=True)
        # JSON
        json_path = out_dir / "failure_debug_report.json"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        # Markdown
        md_path = out_dir / "failure_debug_report.md"
        md_path.write_text(self._render_md(task, report), encoding="utf-8")

    def _render_md(self, task: AppSaTask, report: dict[str, Any]) -> str:
        lines = [
            f"# 任务失败调试报告",
            "",
            f"- **任务ID**: {task.task_id}",
            f"- **任务名称**: {task.task_name}",
            f"- **项目**: {task.project_id}",
            f"- **分析模式**: {task.analysis_mode or '未知'}",
            f"- **状态**: {task.status}",
            f"- **模型**: {report.get('_model', '未知')}",
            "",
            "## 问题现象",
            "",
            report.get("phenomenon") or "(无)",
            "",
            "## 问题根因",
            "",
            report.get("root_cause") or "(无)",
            "",
            "## 解决方法",
            "",
            report.get("solution") or "(无)",
            "",
            "## 代码现场",
            "",
            report.get("code_scene") or "(无)",
            "",
            "## 补丁代码",
            "",
            report.get("patch_code") or "(无)",
            "",
        ]
        return "\n".join(lines)


def get_failure_debug_service() -> FailureDebugService:
    global _instance
    with _lock:
        if _instance is None:
            _instance = FailureDebugService()
        return _instance
