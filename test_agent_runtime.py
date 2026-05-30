from types import SimpleNamespace

from app.api import tasks as tasks_api
from app.service import agent_observability


def test_build_agent_runtime_aggregate_counts_suspected_orphans() -> None:
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
            {"pid": 20, "owner_kind": "orphan", "kill_allowed": True},
            {"pid": 30, "owner_kind": "unknown", "kill_allowed": True},
        ],
        "sessions": [
            {"session_file": "a", "orphan_session": True},
            {"session_file": "b", "orphan_session": False},
        ],
        "tasks": [{"task_id": "sat_1"}],
    }

    runtime = tasks_api._build_agent_runtime_aggregate(snapshot)

    assert runtime["summary"]["total_pods"] == 2
    assert runtime["summary"]["healthy_pods"] == 1
    assert runtime["summary"]["total_processes"] == 3
    assert runtime["summary"]["orphan_processes"] == 1
    assert runtime["summary"]["suspected_orphan_processes"] == 1
    assert runtime["summary"]["killable_suspected_orphan_processes"] == 1
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
    assert snapshot["summary"]["killable_suspected_orphan_processes"] == 1


def test_resolve_worker_targets_prefers_pod_ip_only() -> None:
    assert tasks_api._resolve_worker_targets(pod_ip="10.0.0.9", pod_name="sa-worker-1") == ["10.0.0.9"]
    assert tasks_api._resolve_worker_targets(pod_ip=None, pod_name="sa-worker-1") == []
