from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from app.service.worker_slot_snapshot import build_worker_slot_cluster_snapshot
from app.time_utils import now_local


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *args, **kwargs):
        return self

    def all(self):
        return list(self._rows)


class _FakeDb:
    def __init__(self, rows):
        self._rows = rows

    def query(self, _model):
        return _FakeQuery(self._rows)


def _row(**kwargs):
    now = now_local()
    payload = {
        "task_id": "sat_1",
        "task_name": "system-analysis-demo",
        "status": "running",
        "analysis_mode": "binary",
        "parent_task_id": None,
        "parent_task_type": None,
        "task_origin_type": "manual",
        "input_path": "/tmp/demo",
        "started_at": now,
        "updated_at": now,
        "dispatch_started_at": now,
        "dispatcher_instance_id": None,
        "lease_expires_at": None,
        "lease_epoch": 1,
        "project_id": "p1",
        "is_deleted": False,
    }
    payload.update(kwargs)
    return SimpleNamespace(**payload)


class WorkerSlotSnapshotTests(TestCase):
    def test_dynamic_runner_registry_worker_is_included_without_task(self):
        db = _FakeDb([])
        now = now_local()
        active_runners = [{
            "instance_id": "sa-runner-a:abcd1234",
            "status": "active",
            "capacity": 3,
            "running_tasks": 0,
            "updated_at": now,
            "age_seconds": 1.0,
        }]
        with (
            patch("app.service.worker_slot_snapshot.get_runner_registry_service") as get_registry,
            patch("app.service.worker_slot_snapshot.get_worker_runtime_settings", return_value={"worker_task_concurrency": 2}),
        ):
            get_registry.return_value.list_active_runners.return_value = active_runners
            snapshot = build_worker_slot_cluster_snapshot(db, project_id="p1")

        self.assertEqual(1, snapshot.worker_count)
        self.assertEqual(1, snapshot.healthy_workers)
        self.assertEqual("sa-runner-a:abcd1234", snapshot.workers[0].worker_id)
        self.assertEqual(3, snapshot.workers[0].max_concurrent_jobs)
        self.assertEqual("runner_registry", snapshot.workers[0].source)

    def test_task_lease_fallback_keeps_worker_visible_when_registry_temporarily_missing(self):
        now = now_local()
        rows = [
            _row(
                task_id="sat_live",
                dispatcher_instance_id="sa-runner-b:efgh5678",
                lease_expires_at=now + timedelta(seconds=30),
            )
        ]
        db = _FakeDb(rows)
        with (
            patch("app.service.worker_slot_snapshot.get_runner_registry_service") as get_registry,
            patch("app.service.worker_slot_snapshot.get_worker_runtime_settings", return_value={"worker_task_concurrency": 4}),
        ):
            get_registry.return_value.list_active_runners.return_value = []
            snapshot = build_worker_slot_cluster_snapshot(db, project_id="p1")

        self.assertEqual(1, snapshot.worker_count)
        worker = snapshot.workers[0]
        self.assertTrue(worker.healthy)
        self.assertEqual("task_lease_fallback", worker.source)
        self.assertEqual(1, worker.running_jobs)
        self.assertEqual(3, worker.available_slots)

    def test_pending_without_owner_only_counts_as_queue(self):
        rows = [_row(task_id="sat_pending", status="pending", dispatcher_instance_id=None, lease_expires_at=None)]
        db = _FakeDb(rows)
        with (
            patch("app.service.worker_slot_snapshot.get_runner_registry_service") as get_registry,
            patch("app.service.worker_slot_snapshot.get_worker_runtime_settings", return_value={"worker_task_concurrency": 2}),
        ):
            get_registry.return_value.list_active_runners.return_value = []
            snapshot = build_worker_slot_cluster_snapshot(db, project_id="p1")

        self.assertEqual(1, snapshot.queued_jobs)
        self.assertEqual(0, snapshot.worker_count)
