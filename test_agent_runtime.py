from types import SimpleNamespace

from app.api import tasks as tasks_api
from app.service import agent_observability


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
    assert row["kill_allowed"] is True
    assert row["kill_block_reason"] is None
    assert snapshot["summary"]["killable_unknown_processes"] == 1


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
