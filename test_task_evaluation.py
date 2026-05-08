import json
import tempfile
from pathlib import Path
from types import SimpleNamespace

from app.service.task_service import TaskService


def _service_with_row(row):
    service = TaskService()
    service._get_or_404 = lambda db, task_id: row
    return service


def test_get_task_evaluation_reads_summary_and_sorted_rounds():
    with tempfile.TemporaryDirectory() as tmp:
        task_root = Path(tmp) / "sat_eval"
        run_root = task_root / "run"
        (run_root / "round_002").mkdir(parents=True)
        (run_root / "round_001").mkdir(parents=True)
        (run_root / "evaluation_summary.json").write_text(
            json.dumps({"round_count": 2, "total_tokens": 30}),
            encoding="utf-8",
        )
        (run_root / "round_002" / "b.analyse.json").write_text(
            json.dumps({"task_id": "sat_eval", "round": 2, "module_name": "b", "stage": "analyse"}),
            encoding="utf-8",
        )
        (run_root / "round_001" / "a.classify.json").write_text(
            json.dumps({"task_id": "sat_eval", "round": 1, "module_name": "__task__", "stage": "classify"}),
            encoding="utf-8",
        )

        row = SimpleNamespace(task_id="sat_eval", status="passed", output_path=tmp)
        result = _service_with_row(row).get_task_evaluation(None, "sat_eval")

        assert result["available"] is True
        assert result["summary"]["total_tokens"] == 30
        assert [item["round"] for item in result["rounds"]] == [1, 2]
        assert result["warnings"] == []


def test_get_task_evaluation_handles_missing_summary_and_bad_round():
    with tempfile.TemporaryDirectory() as tmp:
        run_root = Path(tmp) / "sat_eval" / "run"
        (run_root / "round_001").mkdir(parents=True)
        (run_root / "round_001" / "good.json").write_text(
            json.dumps({"round": 1, "module_name": "m", "stage": "analyse"}),
            encoding="utf-8",
        )
        (run_root / "round_002").mkdir()
        (run_root / "round_002" / "bad.json").write_text("{bad", encoding="utf-8")

        row = SimpleNamespace(task_id="sat_eval", status="failed", output_path=tmp)
        result = _service_with_row(row).get_task_evaluation(None, "sat_eval")

        assert result["available"] is True
        assert result["summary"] is None
        assert len(result["rounds"]) == 1
        assert result["warnings"]


def test_get_task_evaluation_missing_run_dir_is_empty():
    with tempfile.TemporaryDirectory() as tmp:
        row = SimpleNamespace(task_id="sat_eval", status="running", output_path=tmp)
        result = _service_with_row(row).get_task_evaluation(None, "sat_eval")

        assert result == {
            "task_id": "sat_eval",
            "status": "running",
            "available": False,
            "summary": None,
            "rounds": [],
            "warnings": [],
        }
