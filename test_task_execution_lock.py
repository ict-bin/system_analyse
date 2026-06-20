import json
import tempfile
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.service.task_execution_lock import RUNNER_BOOT_ID, RUNNER_PROCESS_TOKEN, TaskExecutionLockConflict
from app.service import task_service as ts
from app.service.task_service import RunnerAssignmentRuntimeState, TaskService
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


class _FakeAliveThread:
    def __init__(self, name: str = "alive-thread"):
        self.name = name

    def is_alive(self):
        return True


class _FakeDeadThread:
    def __init__(self, name: str = "dead-thread"):
        self.name = name

    def is_alive(self):
        return False


class _FakeSpawnThread:
    start_calls = 0

    def __init__(self, target=None, args=(), name=None, daemon=None):
        self._target = target
        self._args = args
        self.name = name or "spawn-thread"
        self.daemon = daemon
        self._alive = False

    def start(self):
        type(self).start_calls += 1
        self._alive = True

    def is_alive(self):
        return self._alive

    @classmethod
    def reset(cls):
        cls.start_calls = 0


def _reset_runtime_tracking():
    with ts._running_tasks_guard:
        ts._running_tasks.clear()
        ts._running_task_epochs.clear()
    ts._runner_assignment_runtime_state = RunnerAssignmentRuntimeState()


def test_acquire_execution_lock_reports_reentry_for_same_process_instance():
    task_id = "sat_lock_reentry"
    with tempfile.TemporaryDirectory() as tmp:
        lock_path = _lock_path(tmp, task_id)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.write_text(
            json.dumps(
                {
                    "task_id": task_id,
                    "worker_instance_id": WORKER_INSTANCE_ID,
                    "lease_epoch": 11,
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
            lease_epoch=11,
            lease_expires_at=now_local() + timedelta(minutes=5),
        )

        with pytest.raises(TaskExecutionLockConflict) as exc_info:
            TaskService._acquire_execution_lock(_FakeDb(row), tmp, task_id, 11)

        assert exc_info.value.conflict_kind == "execution_lock_reentry"


def test_run_task_locally_skips_duplicate_same_epoch_alive_thread():
    _reset_runtime_tracking()
    service = object.__new__(TaskService)
    execute_calls = []
    service._runner = SimpleNamespace(execute_task=lambda task_id, lease_epoch: execute_calls.append((task_id, lease_epoch)))
    service._record_timeline_event = lambda **kwargs: None
    with ts._running_tasks_guard:
        ts._running_tasks["sat-1"] = _FakeAliveThread("existing-thread")
        ts._running_task_epochs["sat-1"] = 3

    service._run_task_locally("sat-1", 3)

    assert execute_calls == []
    with ts._running_tasks_guard:
        assert ts._running_task_epochs["sat-1"] == 3
        assert ts._running_tasks["sat-1"].name == "existing-thread"


def test_run_task_locally_replaces_stale_epoch_and_starts_thread():
    _reset_runtime_tracking()
    service = object.__new__(TaskService)
    service._runner = SimpleNamespace(execute_task=lambda task_id, lease_epoch: None)
    service._record_timeline_event = lambda **kwargs: None
    with ts._running_tasks_guard:
        ts._running_tasks["sat-2"] = _FakeAliveThread("old-thread")
        ts._running_task_epochs["sat-2"] = 2

    _FakeSpawnThread.reset()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(ts.threading, "Thread", _FakeSpawnThread)
        service._run_task_locally("sat-2", 4)

    assert _FakeSpawnThread.start_calls == 1
    with ts._running_tasks_guard:
        assert ts._running_task_epochs["sat-2"] == 4
        assert ts._running_tasks["sat-2"].name == "sa_task_sat-2"


def test_start_runner_assignment_loop_is_idempotent():
    _reset_runtime_tracking()
    service = object.__new__(TaskService)
    service._runner_assignment_task = None
    service._runner_assignment_loop_running = False
    service._runner_assignment_loop = lambda: None

    _FakeSpawnThread.reset()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(ts.threading, "Thread", _FakeSpawnThread)
        service._start_runner_assignment_loop()
        first_thread = service._runner_assignment_task
        service._start_runner_assignment_loop()

    assert _FakeSpawnThread.start_calls == 1
    assert service._runner_assignment_task is first_thread
    assert ts._runner_assignment_runtime_state.loop_start_count == 1
    assert ts._runner_assignment_runtime_state.loop_running is True
    assert ts._runner_assignment_runtime_state.thread_alive is True
