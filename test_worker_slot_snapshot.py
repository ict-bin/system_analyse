from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from app.service.worker_slot_snapshot import (
    build_worker_slot_cluster_detail,
    build_worker_slot_cluster_snapshot,
    build_worker_slot_cluster_summary,
    invalidate_worker_slot_summary_cache,
)
from app.time_utils import now_local


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *args, **kwargs):
        return self

    def count(self):
        return len(self._rows)

    def all(self):
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)


class _FakeDb:
    def __init__(self, rows):
        self._rows = rows

    def query(self, _model):
        return _FakeQuery(self._rows)


class _FakeInspect:
    def __init__(self, ping=None, active=None):
        self._ping = ping or {}
        self._active = active or {}

    def ping(self):
        return self._ping

    def active(self):
        return self._active


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
        "project_id": "p1",
        "is_deleted": False,
        "celery_task_id": None,
        "execution_owner_id": None,
        "execution_lease_until": None,
        "execution_epoch": 1,
        "execution_heartbeat_at": now,
    }
    payload.update(kwargs)
    return SimpleNamespace(**payload)


class WorkerSlotSnapshotTests(TestCase):
    def setUp(self):
        invalidate_worker_slot_summary_cache()

    def test_summary_snapshot_omits_active_jobs(self):
        db = _FakeDb([
            _row(
                task_id="sat_live",
                celery_task_id="celery-1",
            )
        ])
        inspect = _FakeInspect(
            ping={"worker@pod-a": {"ok": "pong"}},
            active={"worker@pod-a": [{"id": "celery-1"}]},
        )
        with patch("app.celery_app.app.control.inspect", return_value=inspect), patch(
            "app.service.worker_slot_snapshot._count_queued_jobs",
            return_value=0,
        ):
            snapshot = build_worker_slot_cluster_summary(db, project_id="p1")

        assert snapshot.worker_count == 1
        assert snapshot.workers[0].active_jobs == []

    def test_detail_snapshot_includes_active_jobs(self):
        db = _FakeDb([
            _row(
                task_id="sat_live",
                celery_task_id="celery-1",
                execution_owner_id="worker@pod-a",
            )
        ])
        inspect = _FakeInspect(
            ping={"worker@pod-a": {"ok": "pong"}},
            active={"worker@pod-a": [{"id": "celery-1"}]},
        )
        with patch("app.celery_app.app.control.inspect", return_value=inspect), patch(
            "app.service.worker_slot_snapshot._count_queued_jobs",
            return_value=0,
        ):
            snapshot = build_worker_slot_cluster_detail(db, project_id="p1")

        assert snapshot.worker_count == 1
        assert len(snapshot.workers[0].active_jobs) == 1
        assert snapshot.workers[0].active_jobs[0].task_id == "sat_live"

    def test_running_db_fallback_keeps_worker_visible_when_inspect_empty(self):
        now = now_local()
        db = _FakeDb([
            _row(
                task_id="sat_live",
                execution_owner_id="sa-runner-b",
                execution_lease_until=now + timedelta(seconds=30),
            )
        ])
        inspect = _FakeInspect()
        with patch("app.celery_app.app.control.inspect", return_value=inspect), patch(
            "app.service.worker_slot_snapshot._count_queued_jobs",
            return_value=0,
        ):
            snapshot = build_worker_slot_cluster_snapshot(db, project_id="p1")

        assert snapshot.worker_count == 1
        worker = snapshot.workers[0]
        assert worker.source == "db_running_fallback"
        assert worker.running_jobs == 1

    def test_pending_queue_is_counted_separately(self):
        db = _FakeDb([])
        inspect = _FakeInspect()
        with patch("app.celery_app.app.control.inspect", return_value=inspect), patch(
            "app.service.worker_slot_snapshot._count_queued_jobs",
            return_value=3,
        ):
            snapshot = build_worker_slot_cluster_snapshot(db, project_id="p1")

        assert snapshot.queued_jobs == 3
        assert snapshot.worker_count == 0

    def test_project_cache_invalidation_refreshes_cached_summary(self):
        inspect = _FakeInspect()
        with patch("app.celery_app.app.control.inspect", return_value=inspect), patch(
            "app.service.worker_slot_snapshot._count_queued_jobs",
            return_value=0,
        ):
            initial = build_worker_slot_cluster_summary(_FakeDb([_row(task_id="sat_a", execution_owner_id="w1")]), project_id="p1")
            assert initial.worker_count == 1
            invalidate_worker_slot_summary_cache(project_id="p1")
            refreshed = build_worker_slot_cluster_summary(
                _FakeDb([_row(task_id="sat_a", execution_owner_id="w1"), _row(task_id="sat_b", execution_owner_id="w2")]),
                project_id="p1",
            )
        assert refreshed.worker_count == 2
