import json
from pathlib import Path
from types import SimpleNamespace

from app import metrics


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

    def query(self, model):
        return _FakeQuery(self._rows)


def _install_fake_db(monkeypatch, rows):
    def _fake_get_db():
        yield _FakeDb(rows)

    monkeypatch.setattr("app.db.get_db", _fake_get_db)


def test_render_metrics_exposes_system_analysis_effectiveness_and_checkpoint(monkeypatch, tmp_path: Path):
    task_root = tmp_path / "sat_metrics"
    run_root = task_root / "run"
    workspace = run_root / "workspace"
    checkpoint_dir = workspace / ".checkpoint"
    (run_root / "round_001").mkdir(parents=True)
    (run_root / "round_002").mkdir(parents=True)
    (checkpoint_dir / "s2_modules").mkdir(parents=True)
    (checkpoint_dir / "s3_modules").mkdir(parents=True)

    (run_root / "evaluation_summary.json").write_text(
        json.dumps(
            {
                "module_count": 4,
                "completed_module_count": 3,
                "failed_module_count": 1,
                "round_count": 3,
                "effectiveness": {
                    "first_round_pass_rate": 0.5,
                    "final_module_pass_rate": 0.75,
                    "multi_round_pass_rate": 1.0,
                    "reflection_round_count": 2,
                    "reclassify_count": 1,
                },
            }
        ),
        encoding="utf-8",
    )
    (run_root / "round_001" / "auth.s1.json").write_text(
        json.dumps(
            {
                "stage": "classify",
                "status": "passed",
                "stage_round": 1,
                "duration_ms": 1200,
                "metrics": {
                    "token_total": 100,
                    "cost": 0.12,
                    "avg_judge_score": 88.5,
                    "review_pass_rate": 1.0,
                    "passed_by_vote": True,
                },
                "worker": {"session_file": "worker.jsonl"},
                "judges": [{"session_file": "judge.jsonl"}],
            }
        ),
        encoding="utf-8",
    )
    (run_root / "round_002" / "auth.s2.json").write_text(
        json.dumps(
            {
                "stage": "refine",
                "status": "failed",
                "stage_round": 2,
                "duration_ms": 2400,
                "metrics": {
                    "token_total": 200,
                    "cost": 0.34,
                    "avg_judge_score": 61.0,
                    "review_pass_rate": 0.0,
                    "passed_by_vote": False,
                },
                "worker": {"session_file": "worker2.jsonl"},
                "judges": [{"session_file": "judge2.jsonl"}],
            }
        ),
        encoding="utf-8",
    )
    (checkpoint_dir / "s2_refine.done").write_text(json.dumps({"completed_at": "2026-01-01T00:00:00+00:00"}), encoding="utf-8")
    (checkpoint_dir / "s2_modules" / "auth.done").write_text(json.dumps({"completed_at": "2026-01-01T00:00:01+00:00"}), encoding="utf-8")
    (checkpoint_dir / "s3_modules" / "auth.done").write_text(json.dumps({"completed_at": "2026-01-01T00:00:02+00:00"}), encoding="utf-8")

    row = SimpleNamespace(
        task_id="sat_metrics",
        status="passed",
        created_at=None,
        started_at=None,
        finished_at=None,
        result_json={},
        error=None,
        output_path=str(tmp_path),
        is_deleted=False,
    )
    _install_fake_db(monkeypatch, [row])
    monkeypatch.setattr(metrics, "get_worker_runtime_health", lambda: {"running_task_count": 1})
    monkeypatch.setattr(metrics, "get_worker_runtime_settings", lambda: {"worker_task_concurrency": 2})

    rendered = metrics.render_metrics()

    assert 'secflow_sa_module_total 4' in rendered
    assert 'secflow_sa_module_completed_total 3' in rendered
    assert 'secflow_sa_module_failed_total 1' in rendered
    assert 'secflow_sa_effectiveness_first_round_pass_rate_count 1' in rendered
    assert 'secflow_sa_effectiveness_first_round_pass_rate_sum 0.500000' in rendered
    assert 'secflow_sa_effectiveness_final_module_pass_rate_sum 0.750000' in rendered
    assert 'secflow_sa_effectiveness_reflection_round_total 2' in rendered
    assert 'secflow_sa_effectiveness_reclassify_total 1' in rendered
    assert 'secflow_sa_stage_vote_pass_total{stage="classify",status="passed"} 1' in rendered
    assert 'secflow_sa_stage_vote_fail_total{stage="refine",status="failed"} 1' in rendered
    assert 'secflow_sa_stage_judge_score_sum{stage="classify",status="passed"} 88.500000' in rendered
    assert 'secflow_sa_stage_round_index_sum{stage="refine",status="failed"} 2.000000' in rendered
    assert 'secflow_sa_checkpoint_tasks{state="any"} 1' in rendered
    assert 'secflow_sa_checkpoint_tasks{state="partial"} 1' in rendered
    assert 'secflow_sa_checkpoint_module_done_total{stage="s2"} 1' in rendered
    assert 'secflow_sa_checkpoint_module_done_total{stage="s3"} 1' in rendered
