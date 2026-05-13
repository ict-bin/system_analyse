from __future__ import annotations

import asyncio
import logging
import os
import random
import time as _time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable

from sqlalchemy.orm import Session

from app.db.models import AppSaTask
from app.service.task_repository import TaskRepository
from app.time_utils import now_local

logger = logging.getLogger("sa.worker_dispatcher")

WORKER_POLL_INTERVAL_SECONDS = float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_WORKER_POLL_INTERVAL", "3"))
WORKER_POLL_JITTER_SECONDS = max(0.0, float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_WORKER_POLL_JITTER", "2")))
WORKER_TASK_CONCURRENCY = max(1, int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_WORKER_TASK_CONCURRENCY", "1")))
TASK_LEASE_TIMEOUT_SECONDS = max(30, int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_LEASE_TIMEOUT_SECONDS", "300")))
WORKER_OVERLOAD_COOLDOWN_SECONDS = max(5.0, float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_WORKER_OVERLOAD_COOLDOWN", "30")))
WORKER_IDLE_BACKOFF_MAX_SECONDS = max(
    WORKER_POLL_INTERVAL_SECONDS,
    float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_WORKER_IDLE_BACKOFF_MAX", "15")),
)
WORKER_STALE_SWEEP_INTERVAL_SECONDS = max(
    5.0,
    float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_WORKER_STALE_SWEEP_INTERVAL", "30")),
)
MAX_RUNNING_TASKS_GLOBAL = max(0, int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_MAX_RUNNING_TASKS_GLOBAL", "0")))
GLOBAL_CLAIM_LOCK_KEY = str(
    os.environ.get("SECFLOW_SYSTEM_ANALYSE_GLOBAL_CLAIM_LOCK_KEY") or "secflow:system-analyse:claim"
).strip() or "secflow:system-analyse:claim"
GLOBAL_CLAIM_LOCK_TIMEOUT_SECONDS = max(
    0,
    int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_GLOBAL_CLAIM_LOCK_TIMEOUT_SECONDS", "1")),
)
WORKER_INSTANCE_ID = (
    str(os.environ.get("SECFLOW_SYSTEM_ANALYSE_INSTANCE_ID") or "").strip()
    or str(os.environ.get("POD_NAME") or "").strip()
    or f"sa-worker-{uuid.uuid4().hex[:8]}"
)


def lease_deadline() -> datetime:
    return now_local() + timedelta(seconds=TASK_LEASE_TIMEOUT_SECONDS)


def worker_sleep_seconds(base_sleep: float | None = None) -> float:
    sleep_seconds = WORKER_POLL_INTERVAL_SECONDS if base_sleep is None else max(WORKER_POLL_INTERVAL_SECONDS, base_sleep)
    if WORKER_POLL_JITTER_SECONDS <= 0:
        return sleep_seconds
    return sleep_seconds + random.uniform(0, WORKER_POLL_JITTER_SECONDS)


@dataclass
class WorkerRuntimeState:
    last_tick_ts: float = 0.0
    last_success_ts: float = 0.0
    last_error: str | None = None
    pause_claim_until_ts: float = 0.0
    last_stale_recovery_ts: float = 0.0
    last_claim_attempt_ts: float = 0.0
    last_claim_success_ts: float = 0.0
    last_claimed_task_id: str | None = None
    last_global_running_tasks: int = 0
    last_global_capacity_remaining: int | None = None
    global_limit_reached: bool = False
    global_claim_lock_skipped: bool = False
    control_claim_enabled: bool = True
    control_drain_mode: bool = False
    control_pause_claim_until_ts: float = 0.0
    control_reason: str | None = None
    control_updated_at: str | None = None

    def snapshot(self, running_tasks_count: int) -> dict:
        now_ts = _time.time()
        max_gap = max(10.0, WORKER_POLL_INTERVAL_SECONDS + WORKER_POLL_JITTER_SECONDS + 10.0)
        loop_fresh = (self.last_tick_ts > 0.0) and ((now_ts - self.last_tick_ts) <= max_gap)
        return {
            "worker_running_tasks": running_tasks_count,
            "worker_loop_last_tick_ts": self.last_tick_ts or None,
            "worker_loop_last_success_ts": self.last_success_ts or None,
            "worker_loop_last_error": self.last_error,
            "worker_loop_fresh": loop_fresh if self.last_tick_ts > 0.0 else self.last_error is None,
            "worker_pause_claim_until_ts": self.pause_claim_until_ts or None,
            "worker_last_stale_recovery_ts": self.last_stale_recovery_ts or None,
            "worker_last_claim_attempt_ts": self.last_claim_attempt_ts or None,
            "worker_last_claim_success_ts": self.last_claim_success_ts or None,
            "worker_last_claimed_task_id": self.last_claimed_task_id,
            "worker_last_global_running_tasks": self.last_global_running_tasks,
            "worker_last_global_capacity_remaining": self.last_global_capacity_remaining,
            "worker_global_limit_reached": self.global_limit_reached,
            "worker_global_claim_lock_skipped": self.global_claim_lock_skipped,
            "worker_control_claim_enabled": self.control_claim_enabled,
            "worker_control_drain_mode": self.control_drain_mode,
            "worker_control_pause_claim_until_ts": self.control_pause_claim_until_ts or None,
            "worker_control_reason": self.control_reason,
            "worker_control_updated_at": self.control_updated_at,
            "worker_poll_interval_seconds": WORKER_POLL_INTERVAL_SECONDS,
            "worker_poll_jitter_seconds": WORKER_POLL_JITTER_SECONDS,
            "worker_task_concurrency": WORKER_TASK_CONCURRENCY,
            "worker_idle_backoff_max_seconds": WORKER_IDLE_BACKOFF_MAX_SECONDS,
            "worker_stale_sweep_interval_seconds": WORKER_STALE_SWEEP_INTERVAL_SECONDS,
            "worker_max_running_tasks_global": MAX_RUNNING_TASKS_GLOBAL,
        }


_runtime_state = WorkerRuntimeState()


def get_worker_runtime_health(running_tasks_count: int) -> dict:
    return _runtime_state.snapshot(running_tasks_count)


class WorkerDispatcher:
    def __init__(
        self,
        *,
        get_db: Callable[[], object],
        clear_task_execution_lock: Callable[[str | None, str], None],
        claim_task_lease: Callable[[Session, AppSaTask, str], int | None],
        spawn_task: Callable[[str, int, str], None],
        select_dispatch_target: Callable[[Session], str | None],
        get_running_tasks_count: Callable[[], int],
        load_runtime_control: Callable[[Session], dict],
        task_repository: TaskRepository,
    ) -> None:
        self._get_db = get_db
        self._clear_task_execution_lock = clear_task_execution_lock
        self._claim_task_lease = claim_task_lease
        self._spawn_task = spawn_task
        self._select_dispatch_target = select_dispatch_target
        self._get_running_tasks_count = get_running_tasks_count
        self._load_runtime_control = load_runtime_control
        self._task_repository = task_repository
        self._running = False
        self._task: asyncio.Task | None = None
        self._idle_sleep_seconds = WORKER_POLL_INTERVAL_SECONDS

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._running = True
        self._idle_sleep_seconds = WORKER_POLL_INTERVAL_SECONDS
        self._task = asyncio.create_task(self._run_forever(), name="sa_worker_dispatcher")
        logger.info(
            "worker dispatcher started (poll_interval=%ss concurrency=%s stale_sweep_interval=%ss)",
            WORKER_POLL_INTERVAL_SECONDS,
            WORKER_TASK_CONCURRENCY,
            WORKER_STALE_SWEEP_INTERVAL_SECONDS,
        )

    async def stop(self) -> None:
        self._running = False
        task = self._task
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def _run_forever(self) -> None:
        while self._running:
            try:
                _runtime_state.last_tick_ts = _time.time()
                claimed_count = self._dispatch_once()
                _runtime_state.last_success_ts = _time.time()
                _runtime_state.last_error = None
                if claimed_count > 0:
                    self._idle_sleep_seconds = WORKER_POLL_INTERVAL_SECONDS
                else:
                    self._idle_sleep_seconds = min(
                        WORKER_IDLE_BACKOFF_MAX_SECONDS,
                        max(WORKER_POLL_INTERVAL_SECONDS, self._idle_sleep_seconds * 2),
                    )
                await asyncio.sleep(worker_sleep_seconds(self._idle_sleep_seconds))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _runtime_state.last_error = str(exc)
                _runtime_state.pause_claim_until_ts = _time.time() + WORKER_OVERLOAD_COOLDOWN_SECONDS
                logger.exception("worker dispatcher loop failed: %s", exc)
                await asyncio.sleep(worker_sleep_seconds())

    def _dispatch_once(self) -> int:
        now_ts = _time.time()
        paused = _runtime_state.pause_claim_until_ts > now_ts
        if paused:
            return 0
        available_slots = max(0, WORKER_TASK_CONCURRENCY - self._get_running_tasks_count())
        if available_slots <= 0:
            return 0

        db_gen = self._get_db()
        db: Session = next(db_gen)
        try:
            now = now_local()
            self._recover_stale_tasks_if_due(db, now, now_ts)
            self._apply_runtime_control(self._load_runtime_control(db), now_ts)
            if self._control_blocks_claim(now_ts):
                return 0
            if MAX_RUNNING_TASKS_GLOBAL <= 0:
                _runtime_state.last_global_capacity_remaining = None
                _runtime_state.global_limit_reached = False
                _runtime_state.global_claim_lock_skipped = False
                return self._claim_pending_tasks(db, available_slots)
            if not self._task_repository.try_acquire_global_claim_lock(
                db,
                lock_key=GLOBAL_CLAIM_LOCK_KEY,
                timeout_seconds=GLOBAL_CLAIM_LOCK_TIMEOUT_SECONDS,
            ):
                _runtime_state.global_claim_lock_skipped = True
                return 0
            _runtime_state.global_claim_lock_skipped = False
            try:
                return self._claim_pending_tasks_with_global_limit(db, available_slots)
            finally:
                self._task_repository.release_global_claim_lock(db, lock_key=GLOBAL_CLAIM_LOCK_KEY)
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    @staticmethod
    def _apply_runtime_control(control: dict | None, now_ts: float) -> None:
        payload = control or {}
        _runtime_state.control_claim_enabled = bool(payload.get("claim_enabled", True))
        _runtime_state.control_drain_mode = bool(payload.get("drain_mode", False))
        pause_until = payload.get("pause_claim_until_ts")
        try:
            _runtime_state.control_pause_claim_until_ts = max(0.0, float(pause_until or 0.0))
        except (TypeError, ValueError):
            _runtime_state.control_pause_claim_until_ts = 0.0
        _runtime_state.control_reason = str(payload.get("reason") or "").strip() or None
        _runtime_state.control_updated_at = str(payload.get("updated_at") or "").strip() or None
        if _runtime_state.control_pause_claim_until_ts <= now_ts:
            _runtime_state.control_pause_claim_until_ts = 0.0

    @staticmethod
    def _control_blocks_claim(now_ts: float) -> bool:
        if not _runtime_state.control_claim_enabled:
            return True
        if _runtime_state.control_drain_mode:
            return True
        return _runtime_state.control_pause_claim_until_ts > now_ts

    def _recover_stale_tasks_if_due(self, db: Session, now: datetime, now_ts: float) -> None:
        if (
            _runtime_state.last_stale_recovery_ts > 0.0
            and (now_ts - _runtime_state.last_stale_recovery_ts) < WORKER_STALE_SWEEP_INTERVAL_SECONDS
        ):
            return
        self._task_repository.recover_stale_running_tasks(
            db,
            now=now,
            lease_timeout_seconds=TASK_LEASE_TIMEOUT_SECONDS,
            clear_task_execution_lock=self._clear_task_execution_lock,
        )
        _runtime_state.last_stale_recovery_ts = now_ts

    def _claim_pending_tasks(self, db: Session, available_slots: int) -> int:
        pending_rows = self._task_repository.list_pending_tasks(db, available_slots)
        claimed_count = 0
        _runtime_state.last_claim_attempt_ts = _time.time()
        for row in pending_rows:
            if self._get_running_tasks_count() >= WORKER_TASK_CONCURRENCY:
                break
            dispatch_target = self._select_dispatch_target(db)
            if not dispatch_target:
                break
            lease_epoch = self._claim_task_lease(db, row, dispatch_target)
            if lease_epoch is None:
                continue
            claimed_count += 1
            _runtime_state.last_claim_success_ts = _time.time()
            _runtime_state.last_claimed_task_id = row.task_id
            self._spawn_task(row.task_id, lease_epoch, dispatch_target)
        return claimed_count

    def _claim_pending_tasks_with_global_limit(self, db: Session, available_slots: int) -> int:
        global_running = self._task_repository.count_running_tasks(db)
        _runtime_state.last_global_running_tasks = global_running
        remaining_global_capacity = MAX_RUNNING_TASKS_GLOBAL - global_running
        _runtime_state.last_global_capacity_remaining = max(0, remaining_global_capacity)
        _runtime_state.global_limit_reached = remaining_global_capacity <= 0
        if remaining_global_capacity <= 0:
            return 0
        return self._claim_pending_tasks(db, min(available_slots, remaining_global_capacity))
