from types import SimpleNamespace
import importlib
import os
import sys
import types
from unittest.mock import patch

from app.api import tasks as tasks_api
from app.service import agent_observability
from app.service import runner_registry_service


def test_build_agent_runtime_aggregate_counts_unknown_and_residual_processes() -> None:
    snapshot = {
        "summary": {
            "aggregate_partial": True,
            "aggregate_sources": 3,
            "aggregate_fanout_errors": 1,
            "aggregate_failed_targets": ["sa-worker-2"],
            "scanned_at": 456.0,
        },
        "pods": [
            {"pod_name": "sa-worker-1", "healthy": True},
            {"pod_name": "sa-worker-2", "healthy": False},
        ],
        "processes": [
            {"pid": 10, "owner_kind": "tracked", "kill_allowed": False},
            {"pid": 20, "owner_kind": "residual", "kill_allowed": True},
            {"pid": 30, "owner_kind": "unknown", "kill_allowed": True},
        ],
        "tasks": [{"task_id": "sat_1"}],
    }

    runtime = tasks_api._build_agent_runtime_aggregate(snapshot)

    assert runtime["summary"]["total_pods"] == 2
    assert runtime["summary"]["healthy_pods"] == 1
    assert runtime["summary"]["total_processes"] == 3
    assert runtime["summary"]["residual_processes"] == 1
    assert runtime["summary"]["unknown_processes"] == 1
    assert runtime["summary"]["killable_unknown_processes"] == 1
    assert runtime["summary"]["aggregate_partial"] is True
    assert runtime["summary"]["aggregate_sources"] == 3
    assert runtime["summary"]["aggregate_failed_targets"] == ["sa-worker-2"]


def test_agent_snapshot_marks_unmatched_process_as_killable_unknown(monkeypatch) -> None:
    monkeypatch.setattr(agent_observability, "_iter_agent_processes", lambda: [{
        "pid": 4321,
        "ppid": 1,
        "pgid": 4321,
        "command": "node /usr/bin/pi",
        "cwd": "/tmp/sa-orphan-agent",
        "rss_bytes": 2048,
    }])
    monkeypatch.setattr(
        agent_observability,
        "build_worker_slot_cluster_snapshot",
        lambda _db, project_id=None: SimpleNamespace(workers=[]),
    )

    class _TaskQuery:
        def filter(self, *args, **kwargs):
            return self

        def all(self):
            return []

    class _Db:
        def query(self, model):
            del model
            return _TaskQuery()

    snapshot = agent_observability.AgentObservabilityService().build_snapshot(_Db(), project_id="p1")

    assert len(snapshot["processes"]) == 1
    row = snapshot["processes"][0]
    assert row["owner_kind"] == "unknown"
    assert row["kill_allowed"] is False
    assert row["kill_block_reason"]
    assert snapshot["summary"]["killable_unknown_processes"] == 0


def test_path_belongs_to_root_ignores_external_system_paths() -> None:
    assert agent_observability._path_belongs_to_root("/app", "/data/files/project/app/secflow-app-system-analyse/task-1") is False
    assert agent_observability._path_belongs_to_root(
        "/app/python3.11",
        "/data/files/project/app/secflow-app-system-analyse/task-1/run",
    ) is False
    assert agent_observability._path_belongs_to_root(
        "/data/files/project/app/secflow-app-system-analyse/task-1/run/session.jsonl",
        "/data/files/project/app/secflow-app-system-analyse/task-1",
    ) is True


def test_agent_snapshot_marks_non_running_task_with_runtime_evidence_as_lease_drifted_active(monkeypatch) -> None:
    monkeypatch.setattr(agent_observability, "_iter_agent_processes", lambda: [{
        "pid": 4321,
        "ppid": 1,
        "pgid": 4321,
        "command": "node /usr/bin/pi --session /tmp/ws/task-1/session.jsonl",
        "cwd": "/tmp/ws/task-1",
        "rss_bytes": 2048,
        "runtime_kind": "pi",
        "session_arg_path": "/tmp/ws/task-1/session.jsonl",
        "open_paths": [],
        "started_at_ts": 1,
    }])
    monkeypatch.setattr(
        agent_observability,
        "build_worker_slot_cluster_snapshot",
        lambda _db, project_id=None: SimpleNamespace(workers=[]),
    )
    monkeypatch.setattr(agent_observability, "_path_mtime", lambda _path: agent_observability.datetime.now())

    row = SimpleNamespace(
        task_id="task-1",
        task_name="Task 1",
        status="pending",
        project_id="p1",
        output_path="/tmp/ws",
        input_path="/tmp/ws/task-1",
        dispatcher_instance_id="sa-worker",
        lease_epoch=3,
        lease_expires_at=None,
        updated_at=None,
    )

    class _TaskQuery:
        def filter(self, *args, **kwargs):
            return self

        def all(self):
            return [row]

    class _Db:
        def query(self, model):
            del model
            return _TaskQuery()

    monkeypatch.setattr(
        sys.modules["app.service.task_service"],
        "get_runtime_tracking_snapshot",
        lambda: {"task-1": 3},
    )

    snapshot = agent_observability.AgentObservabilityService().build_snapshot(_Db(), project_id="p1")
    process = snapshot["processes"][0]
    assert process["owner_kind"] == "tracked"
    assert process["kill_allowed"] is False
    assert process["owner_reason"] == "active_task_with_runtime_evidence"


def test_agent_snapshot_uses_session_descriptor_for_subagent_metadata(monkeypatch) -> None:
    session_path = "/tmp/ws/task-1/run/sessions/analyse/mod-a.jsonl"
    monkeypatch.setattr(agent_observability, "_iter_agent_processes", lambda: [{
        "pid": 4321,
        "ppid": 1,
        "pgid": 4321,
        "command": f"node /usr/bin/pi --session {session_path}",
        "cwd": "/tmp/ws/task-1/run",
        "rss_bytes": 2048,
        "runtime_kind": "pi",
        "session_arg_path": session_path,
        "open_paths": [],
        "started_at_ts": 1,
    }])
    monkeypatch.setattr(
        agent_observability,
        "build_worker_slot_cluster_snapshot",
        lambda _db, project_id=None: SimpleNamespace(workers=[]),
    )
    monkeypatch.setattr(agent_observability, "_path_mtime", lambda _path: agent_observability.datetime.now())
    monkeypatch.setattr(
        agent_observability,
        "_session_descriptor_map",
        lambda _row: {
            session_path: {
                "stage_key": "analyse",
                "stage_group": "analyse",
                "family_key": "analyse::mod-a",
                "parallel_group": "analyse::mod-a::a1",
                "role": "sub_worker",
            }
        },
    )

    row = SimpleNamespace(
        task_id="task-1",
        task_name="Task 1",
        status="running",
        project_id="p1",
        output_path="/tmp/ws",
        input_path="/tmp/ws/task-1",
        dispatcher_instance_id="sa-worker",
        lease_epoch=3,
        lease_expires_at=None,
        updated_at=None,
    )

    class _TaskQuery:
        def filter(self, *args, **kwargs):
            return self

        def all(self):
            return [row]

    class _Db:
        def query(self, model):
            del model
            return _TaskQuery()

    monkeypatch.setattr(
        sys.modules["app.service.task_service"],
        "get_runtime_tracking_snapshot",
        lambda: {"task-1": 3},
    )

    snapshot = agent_observability.AgentObservabilityService().build_snapshot(_Db(), project_id="p1")
    process = snapshot["processes"][0]
    assert process["owner_kind"] == "tracked"
    assert process["role_kind"] == "sub_worker"
    assert process["stage_key"] == "analyse"
    assert process["stage_group"] == "analyse"
    assert process["family_key"] == "analyse::mod-a"
    assert process["parallel_group"] == "analyse::mod-a::a1"


def test_resolve_worker_targets_prefers_pod_ip_then_pod_name() -> None:
    assert tasks_api._resolve_worker_targets(pod_ip="10.0.0.9", pod_name="sa-worker-1") == ["10.0.0.9", "sa-worker-1"]
    assert tasks_api._resolve_worker_targets(pod_ip=None, pod_name="sa-worker-1") == ["sa-worker-1"]


def test_aggregate_base_urls_prefers_worker_http_port() -> None:
    worker = SimpleNamespace(pod_ip="10.0.0.9", pod_name="sa-worker-1", http_port=8080)
    assert tasks_api._aggregate_base_urls(worker) == [
        "http://10.0.0.9:8080/api/app/system-analyse",
        "http://sa-worker-1:8080/api/app/system-analyse",
    ]


def test_build_agent_runtime_aggregate_exposes_failed_target_details() -> None:
    snapshot = {
        "summary": {
            "aggregate_partial": True,
            "aggregate_sources": 1,
            "aggregate_fanout_errors": 1,
            "aggregate_failed_targets": ["sa-worker-2"],
            "aggregate_failed_target_details": [
                {
                    "pod_name": "sa-worker-2",
                    "pod_ip": "10.0.0.10",
                    "http_port": 8080,
                    "attempted_urls": ["http://10.0.0.10:8080/api/app/system-analyse"],
                    "error_kind": "connection_refused",
                    "status_code": None,
                    "message": "connection refused",
                }
            ],
            "aggregate_all_sources_failed": False,
        },
        "pods": [],
        "processes": [],
        "tasks": [],
    }

    runtime = tasks_api._build_agent_runtime_aggregate(snapshot)

    assert runtime["summary"]["aggregate_failed_target_details"][0]["http_port"] == 8080
    assert runtime["summary"]["aggregate_failed_target_details"][0]["error_kind"] == "connection_refused"


def test_internal_observability_router_exposes_snapshot_path() -> None:
    paths = {route.path for route in tasks_api.internal_observability_router.routes}
    assert "/api/app/system-analyse/agent-observability/snapshot" in paths


def test_runner_role_includes_internal_observability_router(monkeypatch) -> None:
    monkeypatch.setenv("SECFLOW_SYSTEM_ANALYSE_ROLE", "runner")
    fake_sse = types.ModuleType("sse_starlette")
    fake_sse_sse = types.ModuleType("sse_starlette.sse")
    fake_sse_sse.EventSourceResponse = object
    monkeypatch.setitem(sys.modules, "sse_starlette", fake_sse)
    monkeypatch.setitem(sys.modules, "sse_starlette.sse", fake_sse_sse)
    import app.server as server_module

    reloaded = importlib.reload(server_module)
    try:
        paths = {route.path for route in reloaded.app.routes}
        assert "/api/app/system-analyse/agent-observability/snapshot" in paths
    finally:
        monkeypatch.delenv("SECFLOW_SYSTEM_ANALYSE_ROLE", raising=False)
        importlib.reload(reloaded)


def test_runner_registry_prefers_sa_pod_ip(monkeypatch) -> None:
    monkeypatch.setattr(runner_registry_service, "POD_NAME", "sa-runner-1")
    monkeypatch.setattr(runner_registry_service, "POD_IP", "10.0.0.9")

    class _Query:
        def filter_by(self, **kwargs):
            del kwargs
            return self

        def first(self):
            return None

    class _Db:
        def __init__(self):
            self.added = []
            self.committed = False

        def query(self, model):
            del model
            return _Query()

        def add(self, row):
            self.added.append(row)

        def commit(self):
            self.committed = True

    db = _Db()

    def _db_gen():
        yield db

    service = runner_registry_service.RunnerRegistryService(
        get_db=_db_gen,
        get_running_tasks_count=lambda: 2,
    )

    with patch.object(runner_registry_service, "_get_worker_task_concurrency_from_db", return_value=4), patch(
        "app.service.worker_slot_snapshot.invalidate_worker_slot_summary_cache",
        return_value=None,
    ):
        service._heartbeat_once()

    assert db.committed is True
    assert len(db.added) == 1
    payload = db.added[0].config_json
    assert payload["pod_name"] == "sa-runner-1"
    assert payload["pod_ip"] == "10.0.0.9"
