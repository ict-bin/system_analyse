import json
import tempfile
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.service.task_execution_lock import RUNNER_BOOT_ID, RUNNER_PROCESS_TOKEN, TaskExecutionLockConflict
from app.service.task_service import TaskService
from app.service.worker_dispatcher import WORKER_INSTANCE_ID
from app.time_utils import now_local


class _FakeQuery:
    def __init__(self, row):
        self._row = row

    def filter_by(self, **kwargs):
        del kwargs
        return self

    def first(self):
        return self._row


class _FakeDb:
    def __init__(self, row):
        self._row = row

    def query(self, model):
        del model
        return _FakeQuery(self._row)


def _lock_path(output_root: str, task_id: str) -> Path:
    return Path(output_root) / task_id / "run" / "task.execution.lock"


def test_acquire_execution_lock_recovers_stale_lock_from_old_epoch():
    task_id = "sat_lock_recover"
    with tempfile.TemporaryDirectory() as tmp:
        lock_path = _lock_path(tmp, task_id)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.write_text(
            json.dumps(
                {
                    "task_id": task_id,
                    "worker_instance_id": "worker-old",
                    "lease_epoch": 4,
                    "acquired_at": "2026-05-19T00:00:00",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        row = SimpleNamespace(
            task_id=task_id,
            status="running",
            dispatcher_instance_id=WORKER_INSTANCE_ID,
            lease_epoch=5,
            lease_expires_at=now_local() + timedelta(minutes=5),
        )

        TaskService._acquire_execution_lock(_FakeDb(row), tmp, task_id, 5)

        payload = json.loads(lock_path.read_text("utf-8"))
        assert payload["task_id"] == task_id
        assert payload["worker_instance_id"] == WORKER_INSTANCE_ID
        assert payload["lease_epoch"] == 5
        assert payload["runner_process_token"] == RUNNER_PROCESS_TOKEN
        assert payload["runner_boot_id"] == RUNNER_BOOT_ID


def test_acquire_execution_lock_rejects_active_matching_lock():
    task_id = "sat_lock_active"
    with tempfile.TemporaryDirectory() as tmp:
        lock_path = _lock_path(tmp, task_id)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.write_text(
            json.dumps(
                {
                    "task_id": task_id,
                    "worker_instance_id": WORKER_INSTANCE_ID,
                    "lease_epoch": 7,
                    "acquired_at": "2026-05-19T00:00:00",
                    "runner_boot_id": RUNNER_BOOT_ID,
                    "runner_process_token": RUNNER_PROCESS_TOKEN,
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        row = SimpleNamespace(
            task_id=task_id,
            status="running",
            dispatcher_instance_id=WORKER_INSTANCE_ID,
            lease_epoch=7,
            lease_expires_at=now_local() + timedelta(minutes=5),
        )

        with pytest.raises(TaskExecutionLockConflict, match="task execution lock already exists"):
            TaskService._acquire_execution_lock(_FakeDb(row), tmp, task_id, 7)


def test_acquire_execution_lock_recovers_stale_lock_from_same_worker_old_process():
    task_id = "sat_lock_same_worker_old_process"
    with tempfile.TemporaryDirectory() as tmp:
        lock_path = _lock_path(tmp, task_id)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.write_text(
            json.dumps(
                {
                    "task_id": task_id,
                    "worker_instance_id": WORKER_INSTANCE_ID,
                    "lease_epoch": 9,
                    "acquired_at": "2026-05-19T00:00:00",
                    "runner_boot_id": RUNNER_BOOT_ID,
                    "runner_process_token": "old-process-token",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        row = SimpleNamespace(
            task_id=task_id,
            status="running",
            dispatcher_instance_id=WORKER_INSTANCE_ID,
            lease_epoch=9,
            lease_expires_at=now_local() + timedelta(minutes=5),
        )
        observed: list[tuple[str, dict]] = []

        TaskService._acquire_execution_lock(
            _FakeDb(row),
            tmp,
            task_id,
            9,
            observer=lambda event_type, payload: observed.append((event_type, payload)),
        )

        payload = json.loads(lock_path.read_text("utf-8"))
        assert payload["runner_process_token"] == RUNNER_PROCESS_TOKEN
        assert payload["worker_instance_id"] == WORKER_INSTANCE_ID
        event_types = [item[0] for item in observed]
        assert "task_execution_lock_stale_detected" in event_types
        assert "task_execution_lock_cleared" in event_types
        assert "task_execution_lock_reacquired" in event_types
