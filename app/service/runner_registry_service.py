from __future__ import annotations

import threading
import time
import logging
import os
from typing import Callable

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.db.models import AppSaModelsConfig
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


def runner_registry_key(instance_id: str) -> str:
    return f"runner:{instance_id}"


class RunnerRegistryService:
    def __init__(self, *, get_db: Callable[[], object], get_running_tasks_count: Callable[[], int]) -> None:
        self._get_db = get_db
        self._get_running_tasks_count = get_running_tasks_count
        self._running = False
        self._task: object | None = None

    def start(self) -> None:
        if not is_runner_role() or (self._task and self._task.is_alive()):
            return
        self._running = True
        self._task = threading.Thread(target=self._heartbeat_loop, name="sa_runner_registry_heartbeat", daemon=True)
        self._task.start()
        logger.info(
            "runner registry heartbeat started (instance_id=%s interval=%ss)",
            WORKER_INSTANCE_ID,
            RUNNER_HEARTBEAT_INTERVAL_SECONDS,
        )

    def stop(self) -> None:
        self._running = False
        task = self._task
        if task and task.is_alive():
            self._stop_event.set()
            try:
                task
            except Exception:
                pass
        self._task = None

    def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                self._heartbeat_once()
            except Exception as exc:
                logger.warning("runner registry heartbeat failed: %s", exc)
            time.sleep(RUNNER_HEARTBEAT_INTERVAL_SECONDS)

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


_runner_registry_service: RunnerRegistryService | None = None


def init_runner_registry_service(
    *,
    get_db: Callable[[], object],
    get_running_tasks_count: Callable[[], int],
) -> RunnerRegistryService:
    global _runner_registry_service
    if _runner_registry_service is None:
        _runner_registry_service = RunnerRegistryService(
            get_db=get_db,
            get_running_tasks_count=get_running_tasks_count,
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
