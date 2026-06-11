import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from app.models import SwarmEvent, TaskResult, TaskStatus
from app.service.task_runner import TaskRunner, TaskRunnerDependencies, TaskRunnerSettings
from app.service.task_service import TaskService


class _FakeTaskQuery:
    def __init__(self, rows):
        self._rows = rows
        self._deleted = 0

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def delete(self, synchronize_session=False):
        del synchronize_session
        deleted = len(self._rows)
        self._rows.clear()
        self._deleted = deleted
        return deleted


class _FakeDb:
    def __init__(self, rows=None):
        self.rows = rows or []

    def query(self, model):
        del model
        return _FakeTaskQuery(self.rows)


def test_get_timeline_returns_events_in_order():
    service = object.__new__(TaskService)
    service._get_or_404 = lambda db, task_id: SimpleNamespace(task_id=task_id)

    rows = [
        SimpleNamespace(
            id="e1",
            task_id="sat_1",
            project_id="p1",
            stage_name=None,
            level="info",
            event_type="task_created",
            message="任务已创建",
            payload_json={"analysis_mode": "binary"},
            created_at=None,
        ),
        SimpleNamespace(
            id="e2",
            task_id="sat_1",
            project_id="p1",
            stage_name="1",
            level="info",
            event_type="stage_started",
            message="阶段开始: 1",
            payload_json={"module_count": 3},
            created_at=None,
        ),
    ]

    payload = TaskService.get_timeline(service, _FakeDb(rows), "sat_1")

    assert payload["task_id"] == "sat_1"
    assert [item["id"] for item in payload["events"]] == ["e1", "e2"]
    assert payload["events"][1]["stage_name"] == "1"
    assert payload["events"][1]["payload"] == {"module_count": 3}
    assert payload["events"][1]["payload_json"] == {"module_count": 3}


def test_clear_timeline_deletes_all_events():
    service = object.__new__(TaskService)
    service._get_or_404 = lambda db, task_id: SimpleNamespace(task_id=task_id, project_id="p1")
    rows = [
        SimpleNamespace(id="e1"),
        SimpleNamespace(id="e2"),
    ]

    deleted = TaskService.clear_timeline(service, _FakeDb(rows), "sat_1")

    assert deleted == 2
    assert rows == []


def test_delete_timeline_event_deletes_single_event():
    service = object.__new__(TaskService)
    service._get_or_404 = lambda db, task_id: SimpleNamespace(task_id=task_id, project_id="p1")

    class _SingleDeleteQuery(_FakeTaskQuery):
        def delete(self, synchronize_session=False):
            del synchronize_session
            if self._rows:
                self._rows.pop(0)
                return 1
            return 0

    class _SingleDeleteDb(_FakeDb):
        def query(self, model):
            del model
            return _SingleDeleteQuery(self.rows)

    rows = [
        SimpleNamespace(id="evt-1", event_type="task_created", stage_name=None, created_at=None),
        SimpleNamespace(id="evt-2", event_type="task_started", stage_name="1", created_at=None),
    ]

    deleted = TaskService.delete_timeline_event(service, _SingleDeleteDb(rows), "sat_1", "evt-1")

    assert deleted == 1
    assert len(rows) == 1


def test_clear_timeline_recreates_audit_event(monkeypatch):
    service = object.__new__(TaskService)
    service._get_or_404 = lambda db, task_id: SimpleNamespace(task_id=task_id, project_id="p1")
    recorded = []
    monkeypatch.setattr(TaskService, "_record_task_operation_event", classmethod(lambda cls, **kwargs: recorded.append(kwargs)))
    rows = [SimpleNamespace(id="e1"), SimpleNamespace(id="e2")]

    deleted = TaskService.clear_timeline(service, _FakeDb(rows), "sat_1")

    assert deleted == 2
    assert rows == []
    assert recorded[-1]["event_type"] == "timeline_cleared"
    assert recorded[-1]["payload"]["deleted_event_count"] == 2


def test_delete_timeline_event_recreates_audit_event(monkeypatch):
    service = object.__new__(TaskService)
    service._get_or_404 = lambda db, task_id: SimpleNamespace(task_id=task_id, project_id="p1")
    recorded = []
    monkeypatch.setattr(TaskService, "_record_task_operation_event", classmethod(lambda cls, **kwargs: recorded.append(kwargs)))

    class _EventDeleteQuery(_FakeTaskQuery):
        def first(self):
            return self._rows[0] if self._rows else None

        def delete(self, synchronize_session=False):
            del synchronize_session
            if self._rows:
                self._rows.pop(0)
                return 1
            return 0

    class _EventDeleteDb(_FakeDb):
        def query(self, model):
            del model
            return _EventDeleteQuery(self.rows)

    rows = [
        SimpleNamespace(id="evt-1", event_type="task_created", stage_name=None, created_at=None),
        SimpleNamespace(id="evt-2", event_type="task_started", stage_name="1", created_at=None),
    ]

    deleted = TaskService.delete_timeline_event(service, _EventDeleteDb(rows), "sat_1", "evt-1")

    assert deleted == 1
    assert len(rows) == 1
    assert recorded[-1]["event_type"] == "timeline_event_deleted"
    assert recorded[-1]["payload"]["deleted_event_id"] == "evt-1"
    assert recorded[-1]["payload"]["deleted_event_type"] == "task_created"


def test_cancel_terminal_task_records_noop_timeline_event(monkeypatch):
    service = object.__new__(TaskService)
    row = SimpleNamespace(task_id="sat_1", project_id="p1", status="passed")
    service._get_or_404 = lambda db, task_id: row
    service._row_to_dict = lambda current: {"task_id": current.task_id, "status": current.status}
    recorded = []
    monkeypatch.setattr(TaskService, "_record_task_operation_event", classmethod(lambda cls, **kwargs: recorded.append(kwargs)))

    payload = TaskService.cancel_task(service, object(), "sat_1")

    assert payload["status"] == "passed"
    assert recorded[-1]["event_type"] == "task_cancel_requested_noop"
    assert recorded[-1]["payload"]["reason"] == "task_already_terminal"


def test_repair_task_origin_records_timeline_event(monkeypatch):
    service = object.__new__(TaskService)
    row = SimpleNamespace(
        task_id="sat_1",
        project_id="p1",
        status="failed",
        analysis_mode="binary",
        task_origin_type="manual",
        task_config_json={"resolved_config_snapshot": {"a": 1}, "keep": True},
    )
    service._get_or_404 = lambda db, task_id: row
    service._row_to_dict = lambda current: {"task_id": current.task_id, "analysis_mode": current.analysis_mode}
    recorded = []
    monkeypatch.setattr(TaskService, "_record_task_operation_event", classmethod(lambda cls, **kwargs: recorded.append(kwargs)))

    class _Db:
        def commit(self):
            return None

        def refresh(self, current):
            return None

    payload = TaskService.repair_task_origin(service, _Db(), "sat_1", "source")

    assert payload["analysis_mode"] == "source"
    assert recorded[-1]["event_type"] == "task_origin_repaired"
    assert recorded[-1]["payload"]["previous_analysis_mode"] == "binary"
    assert recorded[-1]["payload"]["analysis_mode"] == "source"
    assert recorded[-1]["payload"]["resolved_config_snapshot_cleared"] is True


def test_rejected_repair_origin_records_rejected_event(monkeypatch):
    service = object.__new__(TaskService)
    row = SimpleNamespace(task_id="sat_1", project_id="p1", status="running", analysis_mode="binary", task_origin_type="manual")
    service._get_or_404 = lambda db, task_id: row
    recorded = []
    monkeypatch.setattr(TaskService, "_record_task_operation_event", classmethod(lambda cls, **kwargs: recorded.append(kwargs)))

    with pytest.raises(HTTPException):
        TaskService.repair_task_origin(service, object(), "sat_1", "source")

    assert recorded[-1]["event_type"] == "task_operation_rejected"
    assert recorded[-1]["payload"]["reason"] == "task_running"


def test_delete_task_records_timeline_event(monkeypatch):
    service = object.__new__(TaskService)
    row = SimpleNamespace(task_id="sat_1", project_id="p1", status="failed", output_path="/tmp/out", is_deleted=False)
    service._get_or_404 = lambda db, task_id: row
    recorded = []
    monkeypatch.setattr(TaskService, "_record_task_operation_event", classmethod(lambda cls, **kwargs: recorded.append(kwargs)))
    monkeypatch.setattr("app.service.task_service._invalidate_slot_summary_cache", lambda project_id: None)
    monkeypatch.setattr("os.path.isdir", lambda path: True)
    removed = []
    monkeypatch.setattr("shutil.rmtree", lambda path: removed.append(path))

    class _Db:
        def commit(self):
            return None

    TaskService.delete_task(service, _Db(), "sat_1", delete_files=True)

    assert row.is_deleted is True
    assert removed == ["/tmp/out/sat_1"]
    assert recorded[-1]["event_type"] == "task_deleted"
    assert recorded[-1]["payload"]["delete_files"] is True
    assert recorded[-1]["payload"]["files_deleted"] is True


def test_rejected_delete_running_task_records_rejected_event(monkeypatch):
    service = object.__new__(TaskService)
    row = SimpleNamespace(task_id="sat_1", project_id="p1", status="running", output_path="/tmp/out")
    service._get_or_404 = lambda db, task_id: row
    recorded = []
    monkeypatch.setattr(TaskService, "_record_task_operation_event", classmethod(lambda cls, **kwargs: recorded.append(kwargs)))

    with pytest.raises(HTTPException):
        TaskService.delete_task(service, object(), "sat_1", delete_files=True)

    assert recorded[-1]["event_type"] == "task_operation_rejected"
    assert recorded[-1]["payload"]["reason"] == "task_running"


def test_record_timeline_event_sanitizes_large_payload(monkeypatch):
    captured = {}

    class _RecordingDb:
        def add(self, event):
            captured["event"] = event

        def commit(self):
            captured["committed"] = True

        def rollback(self):
            captured["rolled_back"] = True

    def _fake_get_db():
        yield _RecordingDb()

    monkeypatch.setattr("app.db.get_db", _fake_get_db)

    TaskService._record_timeline_event(
        task_id="sat_x",
        project_id="p1",
        event_type="task_created",
        message="任务已创建",
        payload={"large": "x" * 3000, "items": list(range(30))},
    )

    event = captured["event"]
    assert captured["committed"] is True
    assert event.payload_json["large"].endswith("...")
    assert len(event.payload_json["items"]) == 20


class _FakeRepo:
    def __init__(self):
        self.finalize_result_calls = []
        self.finalize_error_calls = []
        self.saved_snapshot_calls = []

    def get_task(self, db, task_id):
        return SimpleNamespace(
            task_id=task_id,
            project_id="p1",
            status="running",
            output_path="/tmp/out",
            dispatcher_instance_id="runner-1",
            lease_epoch=2,
            task_config_json={},
            prompt_content="prompt",
            input_path="/tmp/in",
            analysis_mode="binary",
            task_origin_type="manual",
            result_json=None,
            stages_json=None,
        )

    def save_resolved_config_snapshot(self, db, **kwargs):
        self.saved_snapshot_calls.append(kwargs)
        return True

    def finalize_task_result(self, db, **kwargs):
        self.finalize_result_calls.append(kwargs)
        return True

    def finalize_task_error(self, db, **kwargs):
        self.finalize_error_calls.append(kwargs)

    def heartbeat_task_lease(self, db, **kwargs):
        return True


class _FakeOrchestrator:
    def __init__(self, config, on_event):
        del config
        self._on_event = on_event

    async def execute(self, task_id):
        self._on_event(SwarmEvent(type="stage", task_id=task_id, data={"stage": "1", "module": "auth"}))
        self._on_event(SwarmEvent(type="stage_result", task_id=task_id, data={"stage": "1", "module": "auth"}))
        result = TaskResult(task_id=task_id, task="analyse", status=TaskStatus.PASSED, total_duration_ms=1234)
        return result

    def stop(self):
        return None


def _fake_get_db():
    yield _FakeDb()


@pytest.mark.asyncio
async def test_task_runner_records_task_and_stage_timeline(monkeypatch):
    recorded = []
    repo = _FakeRepo()

    deps = TaskRunnerDependencies(
        get_db=_fake_get_db,
        acquire_execution_lock=lambda db, output_path, task_id, lease_epoch: None,
        clear_task_execution_lock=lambda output_path, task_id: None,
        flush_stages=lambda task_id, events: None,
        load_svc_config_from_db=lambda db, project_id: SimpleNamespace(),
        infer_analysis_mode=lambda row: "binary",
        security_filter_log_payload_resolved=lambda payload: {},
        write_models_json_from_db=lambda db: None,
        write_task_result_json=lambda snapshot, payload: "/tmp/result.json",
        lightweight_result_json=lambda snapshot, payload, result_file: {"path": result_file},
        remove_running_task=lambda task_id: None,
        record_timeline_event=lambda **kwargs: recorded.append(kwargs),
        task_repository=repo,
        merge_result_json=lambda existing, patch: {**(existing or {}), **(patch or {})},
    )
    settings = TaskRunnerSettings(
        source_mode_default_analyse_targets=["source"],
        task_stage_flush_batch_size=10,
        task_stage_flush_min_interval_seconds=60,
        task_cancel_poll_interval_seconds=1,
        task_lease_heartbeat_seconds=30,
    )
    runner = TaskRunner(deps=deps, settings=settings)

    monkeypatch.setattr("app.service.task_runner.build_task_config", lambda svc, prompt, cwd: SimpleNamespace(model_dump=lambda mode="json": {}))
    monkeypatch.setattr("app.service.task_runner.Orchestrator", _FakeOrchestrator)
    monkeypatch.setattr("app.service.task_runner.append_events", lambda path, events: True)
    monkeypatch.setattr("app.service.task_runner.write_final", lambda path, events: True)
    monkeypatch.setattr("app.service.task_runner.events_path", lambda output_path, task_id: Path("/tmp/events.jsonl"))
    monkeypatch.setattr("app.service.task_runner.WORKER_INSTANCE_ID", "runner-1")
    monkeypatch.setattr(
        runner._agent_cleanup,
        "run_cleanup",
        lambda phase: {
            "cleanup_phase": phase,
            "runner_instance_id": "runner-1",
            "scanned_process_count": 0,
            "killed_process_count": 0,
            "failed_process_count": 0,
            "surviving_process_count": 0,
            "cleanup_failed": False,
            "level": "info",
            "task_continued": True,
            "items": [],
        },
    )

    async def _fake_supervise(self, task_id, lease_epoch, orch):
        del task_id, lease_epoch, orch
        await asyncio.sleep(0)

    monkeypatch.setattr(TaskRunner, "_supervise_running_task", _fake_supervise)

    await runner.execute_task("sat_runner_1", 2)

    event_types = [item["event_type"] for item in recorded]
    assert "task_started" in event_types
    assert "stage_started" in event_types
    assert "stage_finished" in event_types
    assert "task_finished" in event_types
    assert event_types.count("stage_started") == 1
    assert event_types.count("stage_finished") == 1
    assert "agent_cleanup_started" in event_types
    assert "agent_cleanup_completed" in event_types


@pytest.mark.asyncio
async def test_task_runner_records_task_error(monkeypatch):
    recorded = []
    repo = _FakeRepo()

    deps = TaskRunnerDependencies(
        get_db=_fake_get_db,
        acquire_execution_lock=lambda db, output_path, task_id, lease_epoch: None,
        clear_task_execution_lock=lambda output_path, task_id: None,
        flush_stages=lambda task_id, events: None,
        load_svc_config_from_db=lambda db, project_id: SimpleNamespace(),
        infer_analysis_mode=lambda row: "binary",
        security_filter_log_payload_resolved=lambda payload: {},
        write_models_json_from_db=lambda db: None,
        write_task_result_json=lambda snapshot, payload: "/tmp/result.json",
        lightweight_result_json=lambda snapshot, payload, result_file: {"path": result_file},
        remove_running_task=lambda task_id: None,
        record_timeline_event=lambda **kwargs: recorded.append(kwargs),
        task_repository=repo,
        merge_result_json=lambda existing, patch: {**(existing or {}), **(patch or {})},
    )
    settings = TaskRunnerSettings(
        source_mode_default_analyse_targets=["source"],
        task_stage_flush_batch_size=10,
        task_stage_flush_min_interval_seconds=60,
        task_cancel_poll_interval_seconds=1,
        task_lease_heartbeat_seconds=30,
    )
    runner = TaskRunner(deps=deps, settings=settings)

    monkeypatch.setattr("app.service.task_runner.build_task_config", lambda svc, prompt, cwd: SimpleNamespace(model_dump=lambda mode="json": {}))

    class _ErrorOrchestrator:
        def __init__(self, config, on_event):
            del config, on_event

        async def execute(self, task_id):
            raise RuntimeError("boom")

        def stop(self):
            return None

    monkeypatch.setattr("app.service.task_runner.Orchestrator", _ErrorOrchestrator)
    monkeypatch.setattr("app.service.task_runner.append_events", lambda path, events: True)
    monkeypatch.setattr("app.service.task_runner.write_final", lambda path, events: True)
    monkeypatch.setattr("app.service.task_runner.events_path", lambda output_path, task_id: Path("/tmp/events.jsonl"))

    async def _fake_supervise(self, task_id, lease_epoch, orch):
        del task_id, lease_epoch, orch
        await asyncio.sleep(0)

    monkeypatch.setattr(TaskRunner, "_supervise_running_task", _fake_supervise)

    await runner.execute_task("sat_runner_2", 2)

    assert any(item["event_type"] == "task_error" for item in recorded)
    assert repo.finalize_error_calls


@pytest.mark.asyncio
async def test_task_runner_continues_when_pre_cleanup_fails(monkeypatch):
    recorded = []
    repo = _FakeRepo()

    deps = TaskRunnerDependencies(
        get_db=_fake_get_db,
        acquire_execution_lock=lambda db, output_path, task_id, lease_epoch: None,
        clear_task_execution_lock=lambda output_path, task_id: None,
        flush_stages=lambda task_id, events: None,
        load_svc_config_from_db=lambda db, project_id: SimpleNamespace(),
        infer_analysis_mode=lambda row: "binary",
        security_filter_log_payload_resolved=lambda payload: {},
        write_models_json_from_db=lambda db: None,
        write_task_result_json=lambda snapshot, payload: "/tmp/result.json",
        lightweight_result_json=lambda snapshot, payload, result_file: {"path": result_file},
        remove_running_task=lambda task_id: None,
        record_timeline_event=lambda **kwargs: recorded.append(kwargs),
        task_repository=repo,
        merge_result_json=lambda existing, patch: {**(existing or {}), **(patch or {})},
    )
    settings = TaskRunnerSettings(
        source_mode_default_analyse_targets=["source"],
        task_stage_flush_batch_size=10,
        task_stage_flush_min_interval_seconds=60,
        task_cancel_poll_interval_seconds=1,
        task_lease_heartbeat_seconds=30,
    )
    runner = TaskRunner(deps=deps, settings=settings)

    monkeypatch.setattr("app.service.task_runner.build_task_config", lambda svc, prompt, cwd: SimpleNamespace(model_dump=lambda mode="json": {}))
    monkeypatch.setattr("app.service.task_runner.Orchestrator", _FakeOrchestrator)
    monkeypatch.setattr("app.service.task_runner.append_events", lambda path, events: True)
    monkeypatch.setattr("app.service.task_runner.write_final", lambda path, events: True)
    monkeypatch.setattr("app.service.task_runner.events_path", lambda output_path, task_id: Path("/tmp/events.jsonl"))
    monkeypatch.setattr("app.service.task_runner.WORKER_INSTANCE_ID", "runner-1")

    async def _fake_supervise(self, task_id, lease_epoch, orch):
        del task_id, lease_epoch, orch
        await asyncio.sleep(0)

    monkeypatch.setattr(TaskRunner, "_supervise_running_task", _fake_supervise)
    monkeypatch.setattr(
        runner._agent_cleanup,
        "run_cleanup",
        lambda phase: {
            "cleanup_phase": phase,
            "runner_instance_id": "runner-1",
            "scanned_process_count": 1,
            "killed_process_count": 0 if phase == "pre_task" else 1,
            "failed_process_count": 1 if phase == "pre_task" else 0,
            "surviving_process_count": 1 if phase == "pre_task" else 0,
            "cleanup_failed": phase == "pre_task",
            "level": "critical" if phase == "pre_task" else "info",
            "task_continued": phase == "pre_task",
            "items": [],
        },
    )

    await runner.execute_task("sat_runner_3", 2)

    event_types = [item["event_type"] for item in recorded]
    assert "agent_cleanup_failed" in event_types
    assert "task_started" in event_types
