from __future__ import annotations

import asyncio
import logging
import time as _time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Callable

from sqlalchemy.orm import Session

from app.config import build_task_config
from app.db.models import AppSaTask
from app.logging_utils import log_event
from app.models import SwarmEvent
from app.orchestrator import Orchestrator
from app.service.event_log import append_events, write_final, events_path
from app.service.agent_cleanup import AgentCleanupService
from app.service.task_repository import TaskRepository
from app.service.worker_dispatcher import WORKER_INSTANCE_ID, lease_deadline

logger = logging.getLogger("sa.task_runner")
LEASE_HEARTBEAT_FAILURE_TOLERANCE = 3


@dataclass
class TaskRunnerDependencies:
    get_db: Callable[[], object]
    acquire_execution_lock: Callable[[Session, str | None, str, int], object | None]
    clear_task_execution_lock: Callable[[str | None, str], None]
    flush_stages: Callable[[str, list[dict]], None]  # kept for legacy _execute_task path
    load_svc_config_from_db: Callable[[Session, str], object]
    infer_analysis_mode: Callable[[AppSaTask], str]
    security_filter_log_payload_resolved: Callable[[dict | None], dict]
    write_models_json_from_db: Callable[[Session], None]
    write_task_result_json: Callable[[object, dict], str | None]
    lightweight_result_json: Callable[[object, dict | None, str | None], dict | None]
    remove_running_task: Callable[[str], None]
    record_timeline_event: Callable[..., None]
    task_repository: TaskRepository
    merge_result_json: Callable[[dict | None, dict | None], dict | None]


@dataclass
class TaskRunnerSettings:
    source_mode_default_analyse_targets: list[str]
    task_stage_flush_batch_size: int
    task_stage_flush_min_interval_seconds: float
    task_cancel_poll_interval_seconds: float
    task_lease_heartbeat_seconds: float


class TaskRunner:
    def __init__(self, *, deps: TaskRunnerDependencies, settings: TaskRunnerSettings) -> None:
        self._deps = deps
        self._settings = settings
        self._agent_cleanup = AgentCleanupService()

    async def execute_task(self, task_id: str, lease_epoch: int) -> None:
        event_buffer: list[dict] = []
        output_path_for_lock: str | None = None
        last_stage_flush_ts = 0.0
        last_stage_flush_count = 0
        emitted_stage_states: set[tuple[str, str]] = set()
        task_snapshot = None
        pre_cleanup_report: dict | None = None
        # events_file 在 _prepare_task_execution 返回后才可知，先用 None
        events_file: Path | None = None

        def _normalize_stage_name(raw: object) -> str | None:
            text = str(raw or "").strip()
            return text or None

        def _build_stage_payload(data: dict) -> dict | None:
            payload: dict[str, object] = {}
            for src_key, dst_key in (
                ("module", "module"),
                ("module_name", "module_name"),
                ("attempt", "attempt"),
                ("modules", "modules"),
                ("split", "split"),
                ("new_modules", "new_modules"),
                ("duration_ms", "duration_ms"),
                ("duration_seconds", "duration_seconds"),
                ("elapsed_ms", "elapsed_ms"),
                ("elapsed_seconds", "elapsed_seconds"),
                ("file_count", "file_count"),
                ("module_count", "module_count"),
                ("lease_epoch", "lease_epoch"),
            ):
                value = data.get(src_key)
                if value not in (None, "", [], {}):
                    payload[dst_key] = value
            if events_file is not None:
                payload["events_file"] = str(events_file)
            return payload or None

        def _message_for_stage(event_type: str, stage_name: str, data: dict) -> str:
            module_name = str(data.get("module") or data.get("module_name") or "").strip()
            suffix = f"（模块: {module_name}）" if module_name else ""
            mapping = {
                "stage_started": f"阶段开始: {stage_name}{suffix}",
                "stage_finished": f"阶段完成: {stage_name}{suffix}",
                "stage_failed": f"阶段失败: {stage_name}{suffix}",
                "stage_timeout": f"阶段超时: {stage_name}{suffix}",
                "stage_skipped": f"阶段跳过: {stage_name}{suffix}",
            }
            return mapping.get(event_type, f"阶段事件: {stage_name}{suffix}")

        def _record_stage_timeline_if_needed(event: SwarmEvent) -> None:
            stage_name = _normalize_stage_name(event.data.get("stage"))
            if not stage_name:
                return
            raw_type = str(event.type or "").strip().lower()
            data = dict(event.data or {})
            timeline_type: str | None = None
            if raw_type == "stage":
                timeline_type = "stage_started"
            elif raw_type == "stage_result":
                if bool(data.get("skipped")):
                    timeline_type = "stage_skipped"
                elif bool(data.get("timeout")):
                    timeline_type = "stage_timeout"
                elif data.get("error") or str(data.get("status") or "").strip().lower() in {"failed", "error"}:
                    timeline_type = "stage_failed"
                else:
                    timeline_type = "stage_finished"
            elif raw_type in {"stage_timeout", "timeout"}:
                timeline_type = "stage_timeout"
            elif raw_type in {"stage_failed", "stage_error"}:
                timeline_type = "stage_failed"
            elif raw_type == "stage_skipped":
                timeline_type = "stage_skipped"
            if not timeline_type:
                return
            dedupe_key = (timeline_type, stage_name)
            if dedupe_key in emitted_stage_states:
                return
            emitted_stage_states.add(dedupe_key)
            payload = _build_stage_payload(data)
            if data.get("error"):
                payload = {**(payload or {}), "error": str(data.get("error"))}
            self._deps.record_timeline_event(
                task_id=task_id,
                project_id=getattr(task_snapshot, "project_id", None),
                stage_name=stage_name,
                event_type=timeline_type,
                message=_message_for_stage(timeline_type, stage_name, data),
                level="error" if timeline_type in {"stage_failed", "stage_timeout"} else "info",
                payload=payload,
            )

        def on_event(event: SwarmEvent) -> None:
            nonlocal last_stage_flush_ts, last_stage_flush_count
            # 心跳事件仅用于实时监控，不写入 events.jsonl
            # （它们是 "Worker 还在运行" 信号，不是业务状态变化）
            if event.type == "heartbeat":
                return
            event_buffer.append({"ts": _time.time(), "type": event.type, "data": dict(event.data)})
            _record_stage_timeline_if_needed(event)
            now_ts = _time.time()
            buffered_count = len(event_buffer) - last_stage_flush_count
            if (
                buffered_count >= self._settings.task_stage_flush_batch_size
                or (last_stage_flush_ts > 0.0 and (now_ts - last_stage_flush_ts) >= self._settings.task_stage_flush_min_interval_seconds)
            ):
                # 增量写文件（存在 events_file），否则降级写 DB
                if events_file is not None:
                    persisted = append_events(events_file, event_buffer[last_stage_flush_count:])
                    if not persisted:
                        self._deps.flush_stages(task_id, event_buffer)
                else:
                    self._deps.flush_stages(task_id, event_buffer)
                last_stage_flush_ts = now_ts
                last_stage_flush_count = len(event_buffer)

        try:
            pre_cleanup_report = self._run_agent_cleanup(task_id=task_id, project_id=None, phase="pre_task")
            task_snapshot, cfg = self._prepare_task_execution(task_id, lease_epoch, on_event)
            if pre_cleanup_report is not None:
                pre_cleanup_report["project_id"] = task_snapshot.project_id
            output_path_for_lock = task_snapshot.output_path
            self._deps.record_timeline_event(
                task_id=task_id,
                project_id=task_snapshot.project_id,
                event_type="task_started",
                message="任务开始执行",
                payload={
                    "runner_instance_id": WORKER_INSTANCE_ID,
                    "lease_epoch": lease_epoch,
                },
            )
            # 现在可以确定 events_file 路径
            events_file = events_path(task_snapshot.output_path, task_id)
            # 如果在 _prepare 期间 on_event 已经被触发过（极少发生），补刷一次
            if event_buffer:
                persisted = append_events(events_file, event_buffer)
                if not persisted:
                    self._deps.flush_stages(task_id, event_buffer)
                last_stage_flush_count = len(event_buffer)
            orch = Orchestrator(config=cfg, on_event=on_event)
            task_supervisor = asyncio.create_task(
                self._supervise_running_task(task_id, lease_epoch, orch),
                name=f"sa_supervise_{task_id}",
            )
            try:
                result = await orch.execute(task_id)
            finally:
                task_supervisor.cancel()
                try:
                    await task_supervisor
                except asyncio.CancelledError:
                    pass
            # 最终增量刷新剩余 events
            if events_file is not None:
                persisted = append_events(events_file, event_buffer[last_stage_flush_count:])
                if not persisted:
                    self._deps.flush_stages(task_id, event_buffer)
            else:
                self._deps.flush_stages(task_id, event_buffer)
            self._persist_task_result(task_id, lease_epoch, task_snapshot, result, event_buffer, events_file)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log_event(
                logger,
                logging.ERROR,
                "task execution failed",
                event="task_error",
                task_id=task_id,
                error=str(exc),
            )
            self._persist_task_error(task_id, lease_epoch, event_buffer, exc, events_file, pre_cleanup_report=pre_cleanup_report)
        finally:
            post_project_id = getattr(task_snapshot, "project_id", None) if task_snapshot is not None else None
            post_cleanup_report = self._run_agent_cleanup(task_id=task_id, project_id=post_project_id, phase="post_task")
            self._attach_cleanup_report(task_id=task_id, pre_cleanup_report=pre_cleanup_report, post_cleanup_report=post_cleanup_report)
            self._deps.remove_running_task(task_id)
            self._deps.clear_task_execution_lock(output_path_for_lock, task_id)

    def _run_agent_cleanup(self, *, task_id: str, project_id: str | None, phase: str) -> dict[str, object]:
        self._deps.record_timeline_event(
            task_id=task_id,
            project_id=project_id,
            event_type="agent_cleanup_started",
            message="任务启动前智能体清理开始" if phase == "pre_task" else "任务结束后智能体清理开始",
            level="info",
            payload={"cleanup_phase": phase, "runner_instance_id": WORKER_INSTANCE_ID},
        )
        report = self._agent_cleanup.run_cleanup(phase=phase)
        event_type = "agent_cleanup_failed" if bool(report.get("cleanup_failed")) else "agent_cleanup_completed"
        self._deps.record_timeline_event(
            task_id=task_id,
            project_id=project_id,
            event_type=event_type,
            message=(
                "任务启动前智能体清理失败，任务继续执行"
                if phase == "pre_task" and bool(report.get("cleanup_failed"))
                else "任务启动前智能体清理完成"
                if phase == "pre_task"
                else "任务结束后智能体清理失败"
                if bool(report.get("cleanup_failed"))
                else "任务结束后智能体清理完成"
            ),
            level=str(report.get("level") or "info"),
            payload=report,
        )
        return report

    def _attach_cleanup_report(
        self,
        *,
        task_id: str,
        pre_cleanup_report: dict | None,
        post_cleanup_report: dict | None,
    ) -> None:
        db_gen = self._deps.get_db()
        db: Session = next(db_gen)
        try:
            row = self._deps.task_repository.get_task(db, task_id)
            if row is None:
                return
            cleanup_payload = {
                "pre": pre_cleanup_report,
                "post": post_cleanup_report,
            }
            row.result_json = self._deps.merge_result_json(row.result_json, {"agent_cleanup": cleanup_payload})
            db.commit()
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _prepare_task_execution(self, task_id: str, lease_epoch: int, on_event: Callable[[SwarmEvent], None]) -> tuple[SimpleNamespace, object]:
        db_gen = self._deps.get_db()
        db: Session = next(db_gen)
        try:
            row = self._deps.task_repository.get_task(db, task_id)
            if not row or row.status == "cancelled":
                raise RuntimeError("task no longer runnable")
            if row.dispatcher_instance_id != WORKER_INSTANCE_ID or int(row.lease_epoch or 0) != lease_epoch:
                logger.warning(
                    "skip task %s because active lease belongs to another worker or epoch changed: owner=%s epoch=%s",
                    task_id,
                    row.dispatcher_instance_id,
                    row.lease_epoch,
                )
                raise RuntimeError("task lease lost before execute")
            self._deps.acquire_execution_lock(db, row.output_path, task_id, lease_epoch)
            svc = self._deps.load_svc_config_from_db(db, row.project_id)
            tcfg = dict(row.task_config_json or {})
            if tcfg.get("analyse_targets"):
                svc.analyse_targets = tcfg["analyse_targets"]
            elif self._deps.infer_analysis_mode(row) == "source":
                svc.analyse_targets = list(self._settings.source_mode_default_analyse_targets)
            if tcfg.get("binary_arch"):
                svc.binary_arch = tcfg["binary_arch"]
            if tcfg.get("security_focus_categories"):
                svc.security_focus_categories = tcfg["security_focus_categories"]
            if tcfg.get("module_granularity"):
                svc.module_granularity = tcfg["module_granularity"]
            # 断点续跑由文件系统 .checkpoint/ 驱动，不再读取 start_stage/resume_workspace
            if tcfg.get("filter_engine"):
                svc.filter_engine = tcfg["filter_engine"]
            if "enable_final_check" in tcfg:
                svc.enable_final_check = bool(tcfg["enable_final_check"])
            if "continue_on_module_failure" in tcfg:
                svc.continue_on_module_failure = bool(tcfg["continue_on_module_failure"])
            # 断点续跑由文件系统 .checkpoint/ 驱动，不再读取 start_stage/resume_workspace
            if row.output_path:
                svc.output_dir = row.output_path
                svc.archive_dir = row.output_path
                svc.result_dir = row.output_path
            task_snapshot = SimpleNamespace(
                task_id=row.task_id,
                project_id=row.project_id,
                prompt_content=row.prompt_content,
                input_path=row.input_path,
                output_path=row.output_path,
                analysis_mode=row.analysis_mode,
                task_origin_type=row.task_origin_type,
                task_config_json=tcfg,
                result_json=row.result_json,
                stages_json=row.stages_json,
            )
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

        cfg = build_task_config(svc, task_snapshot.prompt_content, cwd=task_snapshot.input_path)
        resolved_snapshot = cfg.model_dump(mode="json")
        log_event(
            logger,
            logging.INFO,
            "security filter resolved",
            event="security_filter_resolved",
            task_id=task_id,
            project_id=task_snapshot.project_id,
            analysis_mode=task_snapshot.analysis_mode,
            task_origin_type=task_snapshot.task_origin_type,
            **self._deps.security_filter_log_payload_resolved(resolved_snapshot),
        )

        db_gen = self._deps.get_db()
        db = next(db_gen)
        try:
            updated = self._deps.task_repository.save_resolved_config_snapshot(
                db,
                task_id=task_id,
                lease_epoch=lease_epoch,
                worker_instance_id=WORKER_INSTANCE_ID,
                task_config_json=task_snapshot.task_config_json,
                resolved_snapshot=resolved_snapshot,
                lease_deadline=lease_deadline,
            )
            if not updated:
                raise RuntimeError("task lease lost when persisting resolved config")
            self._deps.write_models_json_from_db(db)
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
        return task_snapshot, cfg

    def _persist_task_result(
        self,
        task_id: str,
        lease_epoch: int,
        task_snapshot: object,
        result: object,
        event_buffer: list[dict],
        events_file: Path | None = None,
    ) -> None:
        # 先写文件（包含 __final__ 标记）
        if not write_final(events_file, event_buffer):
            self._deps.flush_stages(task_id, event_buffer)
        db_gen = self._deps.get_db()
        db = next(db_gen)
        try:
            row = self._deps.task_repository.get_task(db, task_id)
            if not row or row.status == "cancelled":
                if row and row.status == "cancelled":
                    self._deps.record_timeline_event(
                        task_id=task_id,
                        project_id=row.project_id,
                        event_type="task_failed",
                        message="任务已取消",
                        level="warning",
                        payload={
                            "runner_instance_id": WORKER_INSTANCE_ID,
                            "lease_epoch": lease_epoch,
                            "status": row.status,
                        },
                    )
                return
            result_json = None
            result_error = None
            program_errors: list[dict] = []
            soft_failed_modules: list[dict] = []
            if result:
                result_payload = result.model_dump(mode="json")
                result_file = self._deps.write_task_result_json(task_snapshot, result_payload)
                result_json = self._deps.lightweight_result_json(task_snapshot, result_payload, result_file)
                if result.error:
                    result_error = result.error
            program_errors = list(getattr(task_snapshot, "program_error_modules", []) or [])
            soft_failed_modules = list(getattr(task_snapshot, "soft_failed_modules", []) or [])
            if program_errors:
                if not result_error:
                    first = program_errors[0]
                    result_error = (
                        f"程序性错误 ({first.get('error_type')}) in {first.get('stage')} "
                        f"{first.get('module_name')}: {first.get('error_message')}"
                    )
            updated = self._deps.task_repository.finalize_task_result(
                db,
                task_id=task_id,
                lease_epoch=lease_epoch,
                worker_instance_id=WORKER_INSTANCE_ID,
                result_status=result.status.value if result else "error",
                result_json=result_json,
                result_error=result_error,
            )
            if not updated:
                return
            final_status = result.status.value if result else "error"
            event_type = "task_finished" if final_status == "passed" else "task_failed"
            level = "info" if event_type == "task_finished" else "error"
            payload: dict[str, object] = {
                "runner_instance_id": WORKER_INSTANCE_ID,
                "lease_epoch": lease_epoch,
                "status": final_status,
            }
            duration_ms = getattr(result, "total_duration_ms", None) if result is not None else None
            if duration_ms not in (None, ""):
                payload["duration_ms"] = duration_ms
            if result_error:
                payload["error"] = str(result_error)
            self._deps.record_timeline_event(
                task_id=task_id,
                project_id=row.project_id,
                event_type=event_type,
                message="任务执行完成" if event_type == "task_finished" else "任务执行失败",
                level=level,
                payload=payload,
            )
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _persist_task_error(
        self, task_id: str, lease_epoch: int,
        event_buffer: list[dict], exc: Exception,
        events_file: Path | None = None,
        pre_cleanup_report: dict | None = None,
    ) -> None:
        # 先写文件（包含 __final__ 标记）
        if not write_final(events_file, event_buffer):
            self._deps.flush_stages(task_id, event_buffer)
        try:
            db_gen = self._deps.get_db()
            db = next(db_gen)
            try:
                row = self._deps.task_repository.get_task(db, task_id)
                project_id = getattr(row, "project_id", None)
                self._deps.task_repository.finalize_task_error(
                    db,
                    task_id=task_id,
                    lease_epoch=lease_epoch,
                    error=str(exc),
                )
                self._deps.record_timeline_event(
                    task_id=task_id,
                    project_id=project_id,
                    event_type="task_error",
                    message="任务执行异常结束",
                    level="error",
                    payload={
                        "runner_instance_id": WORKER_INSTANCE_ID,
                        "lease_epoch": lease_epoch,
                        "error": str(exc),
                        "events_file": str(events_file) if events_file is not None else None,
                    },
                )
                if row is not None:
                    row.result_json = self._deps.merge_result_json(row.result_json, {"agent_cleanup": {"pre": pre_cleanup_report}})
                    db.commit()
            finally:
                try:
                    next(db_gen)
                except StopIteration:
                    pass
        except Exception:
            pass

    async def _supervise_running_task(self, task_id: str, lease_epoch: int, orch: Orchestrator) -> None:
        loop_interval = max(1.0, min(self._settings.task_cancel_poll_interval_seconds, self._settings.task_lease_heartbeat_seconds))
        last_heartbeat_ts = 0.0
        heartbeat_failures = 0
        while True:
            await asyncio.sleep(loop_interval)
            db_gen = self._deps.get_db()
            db: Session = next(db_gen)
            try:
                row = self._deps.task_repository.get_task(db, task_id)
                if not row or row.status == "cancelled":
                    orch.stop()
                    return
                if row.dispatcher_instance_id != WORKER_INSTANCE_ID or int(row.lease_epoch or 0) != lease_epoch:
                    orch.stop()
                    return
                now_ts = _time.time()
                if now_ts - last_heartbeat_ts >= self._settings.task_lease_heartbeat_seconds:
                    updated = self._deps.task_repository.heartbeat_task_lease(
                        db,
                        task_id=task_id,
                        lease_epoch=lease_epoch,
                        worker_instance_id=WORKER_INSTANCE_ID,
                        lease_deadline=lease_deadline,
                    )
                    if not updated:
                        heartbeat_failures += 1
                        self._deps.record_timeline_event(
                            task_id=task_id,
                            project_id=getattr(row, "project_id", None),
                            event_type="task_lease_heartbeat_degraded",
                            message="任务租约续租失败，进入降级观察",
                            level="warning",
                            payload={
                                "lease_epoch": lease_epoch,
                                "dispatcher_instance_id": WORKER_INSTANCE_ID,
                                "heartbeat_failures": heartbeat_failures,
                            },
                        )
                        if heartbeat_failures >= LEASE_HEARTBEAT_FAILURE_TOLERANCE:
                            self._deps.record_timeline_event(
                                task_id=task_id,
                                project_id=getattr(row, "project_id", None),
                                event_type="task_lease_lost_stop_requested",
                                message="任务租约连续续租失败，停止当前执行",
                                level="warning",
                                payload={
                                    "lease_epoch": lease_epoch,
                                    "dispatcher_instance_id": WORKER_INSTANCE_ID,
                                    "heartbeat_failures": heartbeat_failures,
                                },
                            )
                            orch.stop()
                            return
                        continue
                    heartbeat_failures = 0
                    last_heartbeat_ts = now_ts
            finally:
                try:
                    next(db_gen)
                except StopIteration:
                    pass
