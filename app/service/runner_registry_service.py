from __future__ import annotations

import threading
import time
import logging
import os
from typing import Any, Callable

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.db.models import AppSaModelsConfig, AppSaTask
from app.service.config_service import get_worker_task_concurrency as _get_worker_task_concurrency_from_db
from app.service.service_role import is_runner_role
from app.service.worker_dispatcher import WORKER_INSTANCE_ID
from app.time_utils import now_local

logger = logging.getLogger("sa.runner_registry")

RUNNER_HEARTBEAT_INTERVAL_SECONDS = max(
    3,
    int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_RUNNER_HEARTBEAT_INTERVAL_SECONDS", "10")),
)
RUNNER_STALE_TIMEOUT_SECONDS = max(
    RUNNER_HEARTBEAT_INTERVAL_SECONDS * 3,
    int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_RUNNER_STALE_TIMEOUT_SECONDS", "45")),
)
RUNNER_STATUS_ACTIVE = "active"
RUNNER_STATUS_DRAINING = "draining"
POD_NAME = os.environ.get("POD_NAME") or os.environ.get("HOSTNAME") or ""
POD_IP = os.environ.get("SA_POD_IP") or os.environ.get("POD_IP") or ""
IDLE_PI_REAPER_ENABLED = os.environ.get("SECFLOW_SYSTEM_ANALYSE_IDLE_PI_REAPER_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
IDLE_PI_REAPER_INTERVAL_SECONDS = max(
    5,
    int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_IDLE_PI_REAPER_INTERVAL_SECONDS", "30")),
)
IDLE_PI_REAPER_CONFIRM_ROUNDS = max(
    1,
    int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_IDLE_PI_REAPER_CONFIRM_ROUNDS", "2")),
)
_RUNNING_TASK_STATUSES = {"pending", "queued", "running"}


def runner_registry_key(instance_id: str) -> str:
    return f"runner:{instance_id}"


class RunnerRegistryService:
    def __init__(
        self,
        *,
        get_db: Callable[[], object],
        get_running_tasks_count: Callable[[], int],
        cleanup_idle_runtime: Callable[[], dict[str, Any] | None] | None = None,
    ) -> None:
        self._get_db = get_db
        self._get_running_tasks_count = get_running_tasks_count
        self._cleanup_idle_runtime = cleanup_idle_runtime
        self._running = False
        self._task: object | None = None
        self._stop_event = threading.Event()
        self._idle_pi_reaper_thread: object | None = None
        self._last_idle_pi_reaper_at = 0.0
        self._last_idle_pi_reaper_killed_count = 0
        self._idle_pi_reaper_runs_total = 0
        self._idle_pi_reaper_failures_total = 0
        self._idle_pi_reaper_idle_streak = 0

    def start(self) -> None:
        if not is_runner_role() or (self._task and self._task.is_alive()):
            return
        self._running = True
        self._stop_event = threading.Event()
        self._task = threading.Thread(target=self._heartbeat_loop, name="sa_runner_registry_heartbeat", daemon=True)
        self._task.start()
        if IDLE_PI_REAPER_ENABLED and self._cleanup_idle_runtime is not None:
            self._idle_pi_reaper_thread = threading.Thread(
                target=self._idle_pi_reaper_loop,
                name="sa_idle_pi_reaper",
                daemon=True,
            )
            self._idle_pi_reaper_thread.start()
        logger.info(
            "runner registry heartbeat started (instance_id=%s interval=%ss)",
            WORKER_INSTANCE_ID,
            RUNNER_HEARTBEAT_INTERVAL_SECONDS,
        )

    def stop(self) -> None:
        self._running = False
        self._stop_event.set()
        task = self._task
        if task and task.is_alive():
            task.join(timeout=2)
        self._task = None
        idle_task = self._idle_pi_reaper_thread
        if idle_task and idle_task.is_alive():
            idle_task.join(timeout=2)
        self._idle_pi_reaper_thread = None

    def _heartbeat_loop(self) -> None:
        while self._running and not self._stop_event.wait(RUNNER_HEARTBEAT_INTERVAL_SECONDS):
            try:
                self._heartbeat_once()
            except Exception as exc:
                logger.warning("runner registry heartbeat failed: %s", exc)

    def _heartbeat_once(self) -> None:
        db_gen = self._get_db()
        db: Session = next(db_gen)
        try:
            key = runner_registry_key(WORKER_INSTANCE_ID)
            capacity = 1
            payload = {
                "instance_id": WORKER_INSTANCE_ID,
                "status": RUNNER_STATUS_ACTIVE,
                "capacity": capacity,
                "running_tasks": self._get_running_tasks_count(),
                "role": "runner",
                "pod_name": POD_NAME,
                "pod_ip": POD_IP,
                "http_port": int(os.environ.get("PORT") or 8080),
                "heartbeat_ts": now_local().isoformat(),
            }
            row = db.query(AppSaModelsConfig).filter_by(config_key=key).first()
            if row:
                row.config_json = payload
                flag_modified(row, "config_json")
            else:
                row = AppSaModelsConfig(config_key=key, config_json=payload)
                db.add(row)
            db.commit()
            from app.service.worker_slot_snapshot import invalidate_worker_slot_summary_cache

            invalidate_worker_slot_summary_cache()
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def list_active_runners(self, db: Session) -> list[dict]:
        try:
            rows = (
                db.query(AppSaModelsConfig)
                .filter(AppSaModelsConfig.config_key.like("runner:%"))
                .all()
            )
        except SQLAlchemyError as exc:
            logger.warning("list_active_runners failed: %s", exc)
            return []
        active: list[dict] = []
        for row in rows:
            payload = dict(row.config_json) if isinstance(row.config_json, dict) else {}
            instance_id = str(payload.get("instance_id") or "").strip()
            if not instance_id or str(payload.get("status") or "").strip() != RUNNER_STATUS_ACTIVE:
                continue
            if not row.updated_at:
                continue
            active.append(
                {
                    "instance_id": instance_id,
                    "status": RUNNER_STATUS_ACTIVE,
                    "capacity": max(1, int(payload.get("capacity") or 1)),
                    "running_tasks": max(0, int(payload.get("running_tasks") or 0)),
                    "pod_name": str(payload.get("pod_name") or "").strip() or None,
                    "pod_ip": str(payload.get("pod_ip") or "").strip() or None,
                    "http_port": max(1, int(payload.get("http_port") or 8080)),
                    "updated_at": row.updated_at,
                }
            )
        now = now_local()
        filtered: list[dict] = []
        for item in active:
            updated_at = item["updated_at"]
            age_seconds = max(0.0, (now - updated_at).total_seconds())
            if age_seconds > RUNNER_STALE_TIMEOUT_SECONDS:
                continue
            item["age_seconds"] = age_seconds
            filtered.append(item)
        return filtered

    def idle_pi_reaper_status(self) -> dict[str, object]:
        return {
            "thread_alive": bool(self._idle_pi_reaper_thread and self._idle_pi_reaper_thread.is_alive()),
            "last_idle_pi_reaper_at": self._last_idle_pi_reaper_at or None,
            "last_idle_pi_reaper_killed_count": self._last_idle_pi_reaper_killed_count,
            "idle_pi_reaper_runs_total": self._idle_pi_reaper_runs_total,
            "idle_pi_reaper_failures_total": self._idle_pi_reaper_failures_total,
            "idle_pi_reaper_idle_streak": self._idle_pi_reaper_idle_streak,
        }

    def _worker_idle_for_pi_reaping(self) -> bool:
        if int(self._get_running_tasks_count() or 0) > 0:
            self._idle_pi_reaper_idle_streak = 0
            return False
        db_gen = self._get_db()
        db: Session = next(db_gen)
        try:
            active_owned = (
                db.query(AppSaTask)
                .filter(
                    AppSaTask.is_deleted.is_(False),
                    AppSaTask.dispatcher_instance_id == WORKER_INSTANCE_ID,
                    AppSaTask.status.in_(_RUNNING_TASK_STATUSES),
                )
                .count()
            )
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
        if int(active_owned or 0) != 0:
            self._idle_pi_reaper_idle_streak = 0
            return False
        self._idle_pi_reaper_idle_streak += 1
        return self._idle_pi_reaper_idle_streak >= IDLE_PI_REAPER_CONFIRM_ROUNDS

    def _worker_has_residual_pi_for_reaping(self) -> bool:
        from app.service.agent_observability import AgentObservabilityService

        db_gen = self._get_db()
        db: Session = next(db_gen)
        try:
            snapshot = AgentObservabilityService().build_snapshot(db, project_id=None)
            summary = dict(snapshot.get("summary") or {})
            residual_count = int(
                summary.get("residual_pi_process_count")
                or summary.get("residual_processes")
                or 0
            )
            unknown_count = int(
                summary.get("unknown_pi_process_count")
                or summary.get("unknown_processes")
                or 0
            )
            return (residual_count + unknown_count) > 0
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _idle_pi_reaper_loop(self) -> None:
        while self._running and not self._stop_event.wait(IDLE_PI_REAPER_INTERVAL_SECONDS):
            self._idle_pi_reaper_runs_total += 1
            if self._cleanup_idle_runtime is None:
                continue
            try:
                if not self._worker_idle_for_pi_reaping():
                    continue
                if not self._worker_has_residual_pi_for_reaping():
                    self._idle_pi_reaper_idle_streak = 0
                    continue
                logger.info("idle_pi_reaper_scan_started: worker_instance_id=%s", WORKER_INSTANCE_ID)
                report = self._cleanup_idle_runtime() or {}
                self._last_idle_pi_reaper_at = time.time()
                self._last_idle_pi_reaper_killed_count = int(
                    report.get("killed_process_count")
                    or report.get("killed_pid_count")
                    or 0
                )
                self._idle_pi_reaper_idle_streak = 0
                logger.info(
                    "idle_pi_reaper_cleanup_finished: worker_instance_id=%s killed_count=%s",
                    WORKER_INSTANCE_ID,
                    self._last_idle_pi_reaper_killed_count,
                )
            except Exception as exc:
                self._idle_pi_reaper_failures_total += 1
                logger.warning("idle_pi_reaper_cleanup_failed: worker_instance_id=%s error=%s", WORKER_INSTANCE_ID, exc)


_runner_registry_service: RunnerRegistryService | None = None


def init_runner_registry_service(
    *,
    get_db: Callable[[], object],
    get_running_tasks_count: Callable[[], int],
    cleanup_idle_runtime: Callable[[], dict[str, Any] | None] | None = None,
) -> RunnerRegistryService:
    global _runner_registry_service
    if _runner_registry_service is None:
        _runner_registry_service = RunnerRegistryService(
            get_db=get_db,
            get_running_tasks_count=get_running_tasks_count,
            cleanup_idle_runtime=cleanup_idle_runtime,
        )
    return _runner_registry_service


def get_runner_registry_service() -> RunnerRegistryService:
    global _runner_registry_service
    if _runner_registry_service is None:
        from app.db import get_db

        _runner_registry_service = RunnerRegistryService(
            get_db=get_db,
            get_running_tasks_count=lambda: 0,
        )
    return _runner_registry_service
