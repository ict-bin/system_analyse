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
import queue
import shutil
import tempfile
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
SEGMENT_TIMEOUT = float(os.environ.get("SA_FAILURE_DEBUG_SEGMENT_TIMEOUT", "240"))

# 失败状态集合
_FAILED_STATUSES = ("failed", "error")

_instance: "FailureDebugService | None" = None
_lock = threading.Lock()


class FailureDebugService:
    """单例：被动接收调度器下发的调试任务（不主动轮询任务表）。

    调度器在任务失败时 POST /internal/failure-debug {task_id} → submit()。
    本服务用内存队列 + worker 线程串行处理。启动时扫一次 pending/
    stale-running 行（处理重启前已下发但未处理的任务）。
    """

    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._queue: "queue.Queue[str]" = queue.Queue()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._reset_stale_running_on_startup()
        self._enqueue_pending_rows()
        self._thread = threading.Thread(
            target=self._worker_loop, name="sa_failure_debug", daemon=True
        )
        self._thread.start()
        logger.info("FailureDebugService started (notify-driven, model=%s)", DEBUG_MODEL or "auto")

    def stop(self) -> None:
        self._stop_event.set()
        try:
            self._queue.put_nowait("")
        except Exception:
            pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
        self._thread = None

    def submit(self, task_id: str) -> None:
        """调度器下发：入队一个调试任务。"""
        if not task_id:
            return
        self._queue.put(task_id)
        logger.info("failure-debug task submitted: %s", task_id)

    def _reset_stale_running_on_startup(self) -> None:
        try:
            if _SessionLocal is None:
                return
            db = _SessionLocal()
            try:
                n = db.query(AppSaFailureDebug).filter(
                    AppSaFailureDebug.status == "running"
                ).update(
                    {AppSaFailureDebug.status: "error",
                     AppSaFailureDebug.debug_error: "startup_reset: stale running"},
                    synchronize_session=False,
                )
                db.commit()
                if n:
                    logger.info("startup: reset %d stale running failure_debug rows to error", n)
            finally:
                db.close()
        except Exception:
            logger.exception("startup stale running reset failed")

    def _enqueue_pending_rows(self) -> None:
        """启动扫描：把已下发(pending)/重试(error)的行入队处理。"""
        try:
            if _SessionLocal is None:
                return
            db = _SessionLocal()
            try:
                rows = db.query(AppSaFailureDebug).filter(
                    AppSaFailureDebug.status.in_(("pending", "error"))
                ).all()
                for r in rows:
                    self._queue.put(r.task_id)
                if rows:
                    logger.info("startup: enqueued %d pending/error failure_debug rows", len(rows))
            finally:
                db.close()
        except Exception:
            logger.exception("startup pending enqueue failed")

    # ── worker 循环（被动消费队列，不轮询任务表）────────────────────────────
    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                task_id = self._queue.get(timeout=5.0)
            except queue.Empty:
                continue
            if not task_id:
                continue
            try:
                self._debug_one_by_id(task_id)
            except Exception:
                logger.exception("debug failed for task %s", task_id)
            try:
                self._queue.task_done()
            except Exception:
                pass

    def _debug_one_by_id(self, task_id: str) -> None:
        """从 DB 加载任务并调试。"""
        if _SessionLocal is None:
            logger.warning("DB not ready, skip debug for %s", task_id)
            return
        db = _SessionLocal()
        try:
            task = db.query(AppSaTask).filter(AppSaTask.task_id == task_id).first()
            if task is None:
                logger.warning("task %s not found in DB, skip debug", task_id)
                return
            self._debug_one(db, task)
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
        err_msg = task.error or ""
        # 兜底：从错误信息模式匹配 error_kind（早期失败无 error 事件时）
        if not error_kind:
            error_kind = self._classify_error_kind(err_msg) or (abnormal.get("error_kind") if isinstance(abnormal, dict) else None)
        if not failing_stage:
            failing_stage = (abnormal.get("stage") if isinstance(abnormal, dict) else None) or self._guess_stage_from_error(err_msg)

        return {
            "task_id": task.task_id,
            "task_name": task.task_name,
            "project_id": task.project_id,
            "status": task.status,
            "error_msg": err_msg,
            "analysis_mode": task.analysis_mode or "",
            "error_kind": error_kind,
            "failing_stage": failing_stage,
            "abnormal_reason": json.dumps(abnormal, ensure_ascii=False) if abnormal else "",
            "events_tail": tail,
            "events_total": len(events),
        }

    # ── 错误分类（从错误消息模式匹配）────────────────────────────────────
    def _classify_error_kind(self, err_msg: str) -> str | None:
        """从 task.error 文本推断错误类型（早期失败无 error 事件时的兜底）。"""
        if not err_msg:
            return None
        e = err_msg.lower()
        if "name '" in e and "is not defined" in e:
            return "NameError"
        if "[errno 17]" in e or "file exists" in e:
            return "FileExistsError"
        if "[errno 39]" in e or "directory not empty" in e:
            return "DirectoryNotEmptyError"
        if "[errno" in e:
            return "OSError"
        if "提交前校验失败" in err_msg or "pre_submit" in e:
            return "PreSubmitValidationError"
        if "task_subprocess_exit" in e:
            return "SubprocessCrash"
        if "context length" in e or "input tokens" in e:
            return "ContextOverflow"
        if "key authentication" in e or "401" in e:
            return "AuthError"
        if "no model" in e or "model" in e and "not found" in e:
            return "NoModelError"
        if "timeout" in e or "timed out" in e:
            return "TimeoutError"
        if "connection" in e and ("refused" in e or "reset" in e or "unreachable" in e):
            return "ConnectionError"
        return None

    def _guess_stage_from_error(self, err_msg: str) -> str | None:
        """从错误消息猜失败阶段。"""
        if not err_msg:
            return None
        e = err_msg.lower()
        if "s0" in e or "filter" in e:
            return "S0_filter"
        if "s1" in e or "classify" in e:
            return "S1_classify"
        if "s2" in e or "refine" in e or "提交前校验" in err_msg:
            return "S2_refine"
        if "s3" in e or "analyse" in e:
            return "S3_analyse"
        if "s4" in e or "report" in e:
            return "S4_report"
        if "orchestrat" in e or "modules_out" in e:
            return "orchestrator"
        return None

    # ── 运行 LLM 调试（分段多轮会话）───────────────────────────────
    def _run_llm_debug(self, task: AppSaTask, context: dict[str, Any]) -> dict[str, Any]:
        from app.runner import run_agent  # 延迟导入避免循环

        model = DEBUG_MODEL or self._pick_default_model()
        if not model:
            raise RuntimeError("无可用 LLM 模型（models.json 为空或未配置 SA_FAILURE_DEBUG_MODEL）")

        events_text = self._format_events(context.get("events_tail") or [])
        tmp_dir = tempfile.mkdtemp(prefix="sa_fdebug_")
        session_file = str(Path(tmp_dir) / "debug_session.json")
        try:
            common = dict(
                model=model,
                tools=["read", "bash", "grep", "glob"],
                system_prompt=self._system_prompt(),
                cwd=SOURCE_ROOT,
                task_pi_dir=PI_DIR,
                agent_role="failure_debugger",
                max_retries=2,
                retry_delay=10.0,
                timeout_retry_enabled=False,
                pi_max_retries=1,
                fatal_max_retries=0,
            )
            # Turn 0: 上下文 + 检查源码（本轮不产出报告，只建立会话上下文）
            ar = run_agent(
                prompt=self._build_intro_prompt(context, events_text),
                session_file=session_file,
                run_timeout_seconds=RUN_TIMEOUT,
                **common,
            )
            self._check_agent_error(ar, "intro")
            # 分段产出：每段一个 user 消息，格式不对则 user 指出重做
            sections = [
                ("phenomenon", "问题现象", "结合错误信息和事件时间线，描述观察到的失败现象"),
                ("root_cause", "问题根因", "分析为什么会发生此失败，涉及哪个组件/代码逻辑"),
                ("solution", "解决方法", "给出清晰的修复步骤"),
                ("code_scene", "代码现场", "定位文件路径:行号，给出相关代码片段（用```包裹）"),
                ("patch_code", "补丁代码", "给出建议修复补丁（diff 或完整函数代码，用```包裹）"),
            ]
            report: dict[str, Any] = {}
            for key, title, instruction in sections:
                report[key] = self._produce_segment(run_agent, session_file, common, title, instruction)
            report["_model"] = model
            return report
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def _produce_segment(
        self, run_agent, session_file: str, common: dict, title: str, instruction: str,
    ) -> str:
        """一轮产出一段；格式问题用 user 指出后重做一次。"""
        prompt = (
            f"现在请输出【{title}】：{instruction}\n"
            f"只输出本段内容，不要重复其他段，不要额外说明。"
        )
        ar = run_agent(prompt=prompt, session_file=session_file, run_timeout_seconds=SEGMENT_TIMEOUT, **common)
        self._check_agent_error(ar, title)
        text = self._clean_segment((ar.output or "").strip())
        issue = self._validate_segment(title, text)
        if not issue:
            return text
        # 格式问题 → user 指出，重做
        redo = (
            f"你上一段【{title}】输出有问题：{issue}。"
            f"请重新输出【{title}】，只输出本段内容。"
        )
        logger.info("segment %s redo (issue=%s)", title, issue)
        ar = run_agent(prompt=redo, session_file=session_file, run_timeout_seconds=SEGMENT_TIMEOUT, **common)
        self._check_agent_error(ar, title + " redo")
        text = self._clean_segment((ar.output or "").strip())
        return text

    def _check_agent_error(self, ar, label: str) -> None:
        if ar.fatal:
            raise RuntimeError(f"pi 致命错误[{label}]: {ar.error or (ar.output or '')[:200]}")
        if not (ar.output or "").strip() and ar.error:
            raise RuntimeError(f"pi 无输出[{label}]: {ar.error}")

    def _clean_segment(self, text: str) -> str:
        """去掉 LLM 可能加的首尾占位文字（如 '好的：'），保留正文。"""
        text = text.strip()
        # 去掉常见的“好的/以下是”开头客套
        for prefix in ("好的。", "好的：", "好的,", "以下是", "好的，"):
            if text.startswith(prefix):
                text = text[len(prefix):].lstrip()
        return text.strip()

    def _validate_segment(self, title: str, text: str) -> str | None:
        """返回问题描述（None=合格）。"""
        if not text or len(text) < 20:
            return "内容过短或为空"
        return None

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
            "你可以使用 read/bash/grep/glob 工具检查 /app 下的服务源码（Python）。\n"
            "本次调试分多轮进行：先检查源码理解失败，随后按用户要求逐段产出报告各部分。\n"
            "每轮只输出当轮要求的那一段，不要输出其他段，不要输出多余说明。\n"
        )

    def _build_intro_prompt(self, ctx: dict[str, Any], events_text: str) -> str:
        """Turn 0：给上下文 + 要求检查源码（不产出报告）。"""
        return (
            f"# 任务失败调试\n\n"
            f"## 任务信息\n"
            f"- task_id: {ctx.get('task_id')}\n"
            f"- task_name: {ctx.get('task_name')}\n"
            f"- 分析模式: {ctx.get('analysis_mode') or '未知'}\n"
            f"- 失败阶段: {ctx.get('failing_stage') or '未知'}\n"
            f"- 错误类型: {ctx.get('error_kind') or '未知'}\n\n"
            f"## 错误信息\n`````\n{ctx.get('error_msg') or '(无)'}\n`````\n\n"
            f"## 异常原因(JSON)\n`````\n{ctx.get('abnormal_reason') or '(无)'}\n`````\n\n"
            f"## 事件时间线(最后{len(ctx.get('events_tail') or [])}条，共{ctx.get('events_total',0)}条)\n"
            f"{events_text}\n\n"
            f"## 本轮任务\n"
            f"服务源码位于 /app（app/pipeline/ 下是各阶段实现，app/runner.py 是 pi 调用，"
            f"app/orchestrator.py 是编排，app/service/ 是服务层）。\n"
            f"请用 read/bash/grep/glob 工具检查相关源码，定位导致失败的具体代码位置，"
            f"在脑中形成完整理解。**本轮不要输出报告**，只需简短确认你已定位到问题代码"
            f"（给出文件:行号即可）。后续我会逐段让你输出报告。\n"
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
