from __future__ import annotations

import asyncio
import logging
import time as _time
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Callable

from sqlalchemy.orm import Session

from app.config import build_task_config
from app.db.models import AppSaTask
from app.logging_utils import log_event
from app.models import SwarmEvent
from app.orchestrator import Orchestrator
from app.service.task_repository import TaskRepository
from app.service.worker_dispatcher import WORKER_INSTANCE_ID, lease_deadline

logger = logging.getLogger("sa.task_runner")


@dataclass
class TaskRunnerDependencies:
    get_db: Callable[[], object]
    acquire_execution_lock: Callable[[str | None, str, int], object | None]
    clear_task_execution_lock: Callable[[str | None, str], None]
    flush_stages: Callable[[str, list[dict]], None]
    load_svc_config_from_db: Callable[[Session, str], object]
    infer_analysis_mode: Callable[[AppSaTask], str]
    security_filter_log_payload_resolved: Callable[[dict | None], dict]
    write_models_json_from_db: Callable[[Session], None]
    write_task_result_json: Callable[[object, dict], str | None]
    lightweight_result_json: Callable[[object, dict | None, str | None], dict | None]
    remove_running_task: Callable[[str], None]
    task_repository: TaskRepository


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

    async def execute_task(self, task_id: str, lease_epoch: int) -> None:
        event_buffer: list[dict] = []
        output_path_for_lock: str | None = None
        last_stage_flush_ts = 0.0
        last_stage_flush_count = 0

        def on_event(event: SwarmEvent) -> None:
            nonlocal last_stage_flush_ts, last_stage_flush_count
            event_buffer.append({"ts": _time.time(), "type": event.type, "data": dict(event.data)})
            now_ts = _time.time()
            buffered_count = len(event_buffer) - last_stage_flush_count
            if (
                buffered_count >= self._settings.task_stage_flush_batch_size
                or (last_stage_flush_ts > 0.0 and (now_ts - last_stage_flush_ts) >= self._settings.task_stage_flush_min_interval_seconds)
            ):
                self._deps.flush_stages(task_id, event_buffer)
                last_stage_flush_ts = now_ts
                last_stage_flush_count = len(event_buffer)

        try:
            task_snapshot, cfg = self._prepare_task_execution(task_id, lease_epoch, on_event)
            output_path_for_lock = task_snapshot.output_path
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
            self._deps.flush_stages(task_id, event_buffer)
            self._persist_task_result(task_id, lease_epoch, task_snapshot, result, event_buffer)
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
            self._persist_task_error(task_id, lease_epoch, event_buffer, exc)
        finally:
            self._deps.remove_running_task(task_id)
            self._deps.clear_task_execution_lock(output_path_for_lock, task_id)

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
            self._deps.acquire_execution_lock(row.output_path, task_id, lease_epoch)
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
            if tcfg.get("filter_engine"):
                svc.filter_engine = tcfg["filter_engine"]
            if "enable_final_check" in tcfg:
                svc.enable_final_check = bool(tcfg["enable_final_check"])
            svc.start_stage = tcfg["start_stage"] if tcfg.get("start_stage") else 0
            svc.resume_workspace = tcfg.get("resume_workspace") or ""
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
    ) -> None:
        db_gen = self._deps.get_db()
        db = next(db_gen)
        try:
            row = self._deps.task_repository.get_task(db, task_id)
            if not row or row.status == "cancelled":
                return
            result_json = None
            result_error = None
            if result:
                result_payload = result.model_dump(mode="json")
                result_file = self._deps.write_task_result_json(task_snapshot, result_payload)
                result_json = self._deps.lightweight_result_json(task_snapshot, result_payload, result_file)
                if result.error:
                    result_error = result.error
            updated = self._deps.task_repository.finalize_task_result(
                db,
                task_id=task_id,
                lease_epoch=lease_epoch,
                worker_instance_id=WORKER_INSTANCE_ID,
                result_status=result.status.value if result else "error",
                result_json=result_json,
                result_error=result_error,
                stages_json={"events": [dict(e) for e in event_buffer], "final": True},
            )
            if not updated:
                return
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _persist_task_error(self, task_id: str, lease_epoch: int, event_buffer: list[dict], exc: Exception) -> None:
        try:
            db_gen = self._deps.get_db()
            db = next(db_gen)
            try:
                self._deps.task_repository.finalize_task_error(
                    db,
                    task_id=task_id,
                    lease_epoch=lease_epoch,
                    error=str(exc),
                    stages_json={"events": [dict(e) for e in event_buffer], "final": True},
                )
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
                        orch.stop()
                        return
                    last_heartbeat_ts = now_ts
            finally:
                try:
                    next(db_gen)
                except StopIteration:
                    pass
