import json
import tempfile
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

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

        with pytest.raises(RuntimeError, match="task execution lock already exists"):
            TaskService._acquire_execution_lock(_FakeDb(row), tmp, task_id, 7)
