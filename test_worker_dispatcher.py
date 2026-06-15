import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.service import worker_dispatcher as wd


def _fake_db_gen():
    yield SimpleNamespace()


class FakeRepository:
    def __init__(self, *, pending_rows=None, running_count=0, lock_ok=True):
        self.pending_rows = list(pending_rows or [])
        self.running_count = running_count
        self.lock_ok = lock_ok
        self.stale_rows = []
        self.release_calls = 0
        self.lock_calls = 0

    def recover_stale_running_tasks(self, db, *, now, lease_timeout_seconds, clear_task_execution_lock, cleanup_resume_files=None, should_recover=None):
        recovered = []
        for row in list(self.stale_rows):
            if should_recover is not None and not should_recover(row):
                continue
            recovered.append(row)
            clear_task_execution_lock(getattr(row, "output_path", None), row.task_id)
            if cleanup_resume_files is not None:
                cleanup_resume_files(getattr(row, "output_path", None), row.task_id)
        return recovered

    def list_pending_tasks(self, db, limit: int):
        return list(self.pending_rows[:limit])

    def count_running_tasks(self, db):
        return self.running_count

    def repair_task_runtime_binding(self, db, *, task_id: str, worker_instance_id: str, lease_deadline):
        del db, task_id, worker_instance_id, lease_deadline
        return True

    def try_acquire_global_claim_lock(self, db, *, lock_key: str, timeout_seconds: int = 1):
        self.lock_calls += 1
        return self.lock_ok

    def release_global_claim_lock(self, db, *, lock_key: str):
        self.release_calls += 1


class WorkerDispatcherGlobalLimitTests(unittest.TestCase):
    def setUp(self):
        wd._runtime_state = wd.WorkerRuntimeState()

    def test_dispatch_skips_when_global_limit_reached(self):
        repo = FakeRepository(
            pending_rows=[SimpleNamespace(task_id="task-1"), SimpleNamespace(task_id="task-2")],
            running_count=2,
            lock_ok=True,
        )
        claimed = []
        spawned = []
        dispatcher = wd.WorkerDispatcher(
            get_db=_fake_db_gen,
            clear_task_execution_lock=lambda output_path, task_id: None,
            cleanup_resume_files=lambda output_path, task_id: None,
            claim_task_lease=lambda db, row, dispatch_target: claimed.append((row.task_id, dispatch_target)) or 1,
            spawn_task=lambda task_id, lease_epoch, dispatch_target: spawned.append((task_id, lease_epoch, dispatch_target)),
            record_timeline_event=lambda **kwargs: None,
            select_dispatch_target=lambda db: "runner-a",
            get_running_tasks_count=lambda: 0,
            load_runtime_control=lambda db: {},
            task_repository=repo,
        )
        dispatcher._agent_observability = SimpleNamespace(build_snapshot=lambda db, project_id=None: {"processes": []})
        with patch.object(wd, "MAX_RUNNING_TASKS_GLOBAL", 2), patch.object(wd, "WORKER_TASK_CONCURRENCY", 2):
            claimed_count = dispatcher._dispatch_once()

        self.assertEqual(claimed_count, 0)
        self.assertEqual(claimed, [])
        self.assertEqual(spawned, [])
        self.assertTrue(wd._runtime_state.global_limit_reached)
        self.assertEqual(wd._runtime_state.last_global_capacity_remaining, 0)
        self.assertEqual(repo.lock_calls, 1)
        self.assertEqual(repo.release_calls, 1)

    def test_resolve_worker_task_concurrency_is_pinned_to_one(self):
        self.assertEqual(wd.WorkerDispatcher._resolve_worker_task_concurrency(None), 1)

    def test_dispatch_skips_when_global_claim_lock_is_busy(self):
        repo = FakeRepository(
            pending_rows=[SimpleNamespace(task_id="task-1")],
            running_count=0,
            lock_ok=False,
        )
        dispatcher = wd.WorkerDispatcher(
            get_db=_fake_db_gen,
            clear_task_execution_lock=lambda output_path, task_id: None,
            cleanup_resume_files=lambda output_path, task_id: None,
            claim_task_lease=lambda db, row, dispatch_target: 1,
            spawn_task=lambda task_id, lease_epoch, dispatch_target: None,
            record_timeline_event=lambda **kwargs: None,
            select_dispatch_target=lambda db: "runner-a",
            get_running_tasks_count=lambda: 0,
            load_runtime_control=lambda db: {},
            task_repository=repo,
        )
        dispatcher._agent_observability = SimpleNamespace(build_snapshot=lambda db, project_id=None: {"processes": []})
        with patch.object(wd, "MAX_RUNNING_TASKS_GLOBAL", 2), patch.object(wd, "WORKER_TASK_CONCURRENCY", 1):
            claimed_count = dispatcher._dispatch_once()

        self.assertEqual(claimed_count, 0)
        self.assertTrue(wd._runtime_state.global_claim_lock_skipped)
        self.assertEqual(repo.lock_calls, 1)
        self.assertEqual(repo.release_calls, 0)

    def test_dispatch_claims_only_remaining_global_capacity(self):
        repo = FakeRepository(
            pending_rows=[
                SimpleNamespace(task_id="task-1"),
                SimpleNamespace(task_id="task-2"),
                SimpleNamespace(task_id="task-3"),
            ],
            running_count=1,
            lock_ok=True,
        )
        claimed = []
        spawned = []
        dispatcher = wd.WorkerDispatcher(
            get_db=_fake_db_gen,
            clear_task_execution_lock=lambda output_path, task_id: None,
            cleanup_resume_files=lambda output_path, task_id: None,
            claim_task_lease=lambda db, row, dispatch_target: claimed.append((row.task_id, dispatch_target)) or len(claimed),
            spawn_task=lambda task_id, lease_epoch, dispatch_target: spawned.append((task_id, lease_epoch, dispatch_target)),
            record_timeline_event=lambda **kwargs: None,
            select_dispatch_target=lambda db: "runner-a",
            get_running_tasks_count=lambda: 0,
            load_runtime_control=lambda db: {},
            task_repository=repo,
        )
        dispatcher._agent_observability = SimpleNamespace(build_snapshot=lambda db, project_id=None: {"processes": []})
        with patch.object(wd, "MAX_RUNNING_TASKS_GLOBAL", 2), patch.object(wd, "WORKER_TASK_CONCURRENCY", 3):
            claimed_count = dispatcher._dispatch_once()

        self.assertEqual(claimed_count, 1)
        self.assertEqual(claimed, [("task-1", "runner-a")])
        self.assertEqual(spawned, [("task-1", 1, "runner-a")])
        self.assertFalse(wd._runtime_state.global_limit_reached)
        self.assertEqual(wd._runtime_state.last_global_running_tasks, 1)
        self.assertEqual(wd._runtime_state.last_global_capacity_remaining, 1)
        self.assertEqual(repo.release_calls, 1)

    def test_dispatch_skips_when_runtime_control_disables_claim(self):
        repo = FakeRepository(
            pending_rows=[SimpleNamespace(task_id="task-1")],
            running_count=0,
            lock_ok=True,
        )
        claimed = []
        dispatcher = wd.WorkerDispatcher(
            get_db=_fake_db_gen,
            clear_task_execution_lock=lambda output_path, task_id: None,
            cleanup_resume_files=lambda output_path, task_id: None,
            claim_task_lease=lambda db, row, dispatch_target: claimed.append((row.task_id, dispatch_target)) or 1,
            spawn_task=lambda task_id, lease_epoch, dispatch_target: None,
            record_timeline_event=lambda **kwargs: None,
            select_dispatch_target=lambda db: "runner-a",
            get_running_tasks_count=lambda: 0,
            load_runtime_control=lambda db: {"claim_enabled": False, "reason": "maintenance"},
            task_repository=repo,
        )
        dispatcher._agent_observability = SimpleNamespace(build_snapshot=lambda db, project_id=None: {"processes": []})
        with patch.object(wd, "MAX_RUNNING_TASKS_GLOBAL", 4), patch.object(wd, "WORKER_TASK_CONCURRENCY", 1):
            claimed_count = dispatcher._dispatch_once()

        self.assertEqual(claimed_count, 0)
        self.assertEqual(claimed, [])
        self.assertFalse(wd._runtime_state.control_claim_enabled)
        self.assertEqual(wd._runtime_state.control_reason, "maintenance")

    def test_dispatch_skips_when_runtime_control_drain_mode_is_enabled(self):
        repo = FakeRepository(
            pending_rows=[SimpleNamespace(task_id="task-1")],
            running_count=0,
            lock_ok=True,
        )
        claimed = []
        dispatcher = wd.WorkerDispatcher(
            get_db=_fake_db_gen,
            clear_task_execution_lock=lambda output_path, task_id: None,
            cleanup_resume_files=lambda output_path, task_id: None,
            claim_task_lease=lambda db, row, dispatch_target: claimed.append((row.task_id, dispatch_target)) or 1,
            spawn_task=lambda task_id, lease_epoch, dispatch_target: None,
            record_timeline_event=lambda **kwargs: None,
            select_dispatch_target=lambda db: "runner-a",
            get_running_tasks_count=lambda: 0,
            load_runtime_control=lambda db: {"claim_enabled": True, "drain_mode": True, "reason": "draining"},
            task_repository=repo,
        )
        dispatcher._agent_observability = SimpleNamespace(build_snapshot=lambda db, project_id=None: {"processes": []})
        with patch.object(wd, "MAX_RUNNING_TASKS_GLOBAL", 4), patch.object(wd, "WORKER_TASK_CONCURRENCY", 1):
            claimed_count = dispatcher._dispatch_once()

        self.assertEqual(claimed_count, 0)
        self.assertEqual(claimed, [])
        self.assertTrue(wd._runtime_state.control_drain_mode)
        self.assertEqual(wd._runtime_state.control_reason, "draining")

    def test_stale_running_task_with_runtime_evidence_is_preserved(self):
        repo = FakeRepository()
        repo.stale_rows = [
            SimpleNamespace(
                task_id="task-1",
                project_id="p1",
                output_path="/tmp/out",
                dispatcher_instance_id="runner-a",
                lease_epoch=2,
                lease_expires_at=None,
            )
        ]
        events = []
        dispatcher = wd.WorkerDispatcher(
            get_db=_fake_db_gen,
            clear_task_execution_lock=lambda output_path, task_id: (_ for _ in ()).throw(AssertionError("should not clear lock")),
            cleanup_resume_files=lambda output_path, task_id: (_ for _ in ()).throw(AssertionError("should not cleanup files")),
            claim_task_lease=lambda db, row, dispatch_target: None,
            spawn_task=lambda task_id, lease_epoch, dispatch_target: None,
            record_timeline_event=lambda **kwargs: events.append(kwargs),
            select_dispatch_target=lambda db: None,
            get_running_tasks_count=lambda: 0,
            load_runtime_control=lambda db: {},
            task_repository=repo,
        )
        dispatcher._agent_observability = SimpleNamespace(build_snapshot=lambda db, project_id=None: {
            "processes": [{"task_id": "task-1", "owner_kind": "lease_drifted_active", "runtime_evidence": {"live_runtime_evidence": True}}]
        })
        dispatcher._recover_stale_tasks_if_due(SimpleNamespace(), wd.now_local(), 1000.0)
        self.assertTrue(any(item["event_type"] == "task_lease_drift_preserved_due_to_runtime_evidence" for item in events))

    def test_stale_running_task_without_runtime_evidence_is_recovered(self):
        repo = FakeRepository()
        repo.stale_rows = [
            SimpleNamespace(
                task_id="task-1",
                project_id="p1",
                output_path="/tmp/out",
                dispatcher_instance_id="runner-a",
                lease_epoch=2,
                lease_expires_at=None,
            )
        ]
        cleared = []
        cleaned = []
        events = []
        dispatcher = wd.WorkerDispatcher(
            get_db=_fake_db_gen,
            clear_task_execution_lock=lambda output_path, task_id: cleared.append((output_path, task_id)),
            cleanup_resume_files=lambda output_path, task_id: cleaned.append((output_path, task_id)),
            claim_task_lease=lambda db, row, dispatch_target: None,
            spawn_task=lambda task_id, lease_epoch, dispatch_target: None,
            record_timeline_event=lambda **kwargs: events.append(kwargs),
            select_dispatch_target=lambda db: None,
            get_running_tasks_count=lambda: 0,
            load_runtime_control=lambda db: {},
            task_repository=repo,
        )
        dispatcher._agent_observability = SimpleNamespace(build_snapshot=lambda db, project_id=None: {"processes": []})
        dispatcher._recover_stale_tasks_if_due(SimpleNamespace(), wd.now_local(), 1000.0)
        self.assertEqual(cleared, [("/tmp/out", "task-1")])
        self.assertEqual(cleaned, [("/tmp/out", "task-1")])
        self.assertTrue(any(item["event_type"] == "task_lease_recovered" for item in events))

    def test_task_auto_recovered_only_emits_for_recently_recovered_task(self):
        repo = FakeRepository(
            pending_rows=[SimpleNamespace(task_id="task-1", project_id="p1"), SimpleNamespace(task_id="task-2", project_id="p1")],
            running_count=0,
            lock_ok=True,
        )
        events = []
        spawned = []
        dispatcher = wd.WorkerDispatcher(
            get_db=_fake_db_gen,
            clear_task_execution_lock=lambda output_path, task_id: None,
            cleanup_resume_files=lambda output_path, task_id: None,
            claim_task_lease=lambda db, row, dispatch_target: 7,
            spawn_task=lambda task_id, lease_epoch, dispatch_target: spawned.append((task_id, lease_epoch, dispatch_target)),
            record_timeline_event=lambda **kwargs: events.append(kwargs),
            select_dispatch_target=lambda db: "runner-a",
            get_running_tasks_count=lambda: 0,
            load_runtime_control=lambda db: {},
            task_repository=repo,
        )
        dispatcher._agent_observability = SimpleNamespace(build_snapshot=lambda db, project_id=None: {"processes": []})
        wd._runtime_state.recently_recovered_task_ids = {"task-1"}
        claimed = dispatcher._claim_pending_tasks(SimpleNamespace(), available_slots=2, current_concurrency=2)
        self.assertEqual(claimed, 2)
        self.assertEqual(len(spawned), 2)
        auto_recovered = [item for item in events if item["event_type"] == "task_auto_recovered"]
        self.assertEqual(len(auto_recovered), 1)
        self.assertEqual(auto_recovered[0]["task_id"], "task-1")
        self.assertEqual(auto_recovered[0]["payload"]["reason"], "lease_recovered_and_reclaimed")


if __name__ == "__main__":
    unittest.main()
