from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

from app.service.task_service import TaskService


class _FakeQuery:
    def __init__(self, rows):
        self._rows = list(rows)

    def filter(self, *args, **kwargs):
        del args, kwargs
        return self

    def options(self, *args, **kwargs):
        del args, kwargs
        return self

    def order_by(self, *args, **kwargs):
        del args, kwargs
        return self

    def offset(self, *args, **kwargs):
        del args, kwargs
        return self

    def limit(self, *args, **kwargs):
        del args, kwargs
        return self

    def count(self):
        return len(self._rows)

    def all(self):
        return list(self._rows)


class _FakeDb:
    def __init__(self, rows):
        self._rows = rows

    def query(self, _model):
        return _FakeQuery(self._rows)


def _task(**kwargs):
    payload = {
        "task_id": "sat-1",
        "project_id": "p1",
        "task_origin_type": "manual",
        "analysis_mode": "binary",
        "parent_project_id": None,
        "parent_task_id": None,
        "parent_task_type": None,
        "parent_stage_name": None,
        "parent_stage_item_id": None,
        "parent_stage_item_key": None,
        "task_name": "demo",
        "task_description": None,
        "input_path": "/tmp/in",
        "output_path": "/tmp/out",
        "prompt_template_id": None,
        "status": "failed",
        "error": "boom",
        "created_by": "u",
        "created_at": datetime(2026, 1, 1, 0, 0, 0),
        "updated_at": datetime(2026, 1, 1, 0, 0, 0),
        "started_at": None,
        "finished_at": None,
        "dispatcher_instance_id": None,
        "dispatch_started_at": None,
        "lease_epoch": 0,
        "lease_expires_at": None,
        "latest_abnormal_reason_json": {"title": "任务异常结束", "code": "orchestration_failed"},
        "stages_json": None,
        "task_config_json": None,
        "prompt_content": "prompt",
        "result_json": None,
    }
    payload.update(kwargs)
    return SimpleNamespace(**payload)


def test_list_tasks_uses_lightweight_abnormal_reason_without_reading_events():
    service = TaskService()
    db = _FakeDb([_task()])

    with patch("app.service.task_service.read_events", side_effect=AssertionError("should not read events for list")):
        payload = service.list_tasks(
            db,
            project_id="p1",
            page=1,
            per_page=20,
            analysis_mode="binary",
        )

    assert payload["total"] == 1
    item = payload["items"][0]
    assert item["analysis_mode"] == "binary"
    assert item["abnormal_reason"]["code"] == "orchestration_failed"
    assert "stages_json" not in item
    assert "task_config_json" not in item
    assert "prompt_content" not in item
    assert "result_json" not in item
    assert "input_path" not in item
    assert "output_path" not in item
