from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from app.service.task_service import _task_abnormal_reason


def test_execution_lock_message_maps_to_execution_lock_conflict():
    row = SimpleNamespace(
        task_id="sat-abnormal-1",
        status="error",
        error=(
            "task execution lock already exists: /tmp/task.execution.lock "
            "(lock_worker_instance_id=runner-a, lock_lease_epoch=1, "
            "lock_runner_boot_id=boot-a, lock_runner_process_token=token-a, "
            "row_status=running, row_worker_instance_id=runner-a, row_lease_epoch=1)"
        ),
        result_json=None,
        output_path="/tmp/out",
        stages_json={"events": []},
        started_at=datetime(2026, 6, 11, 7, 18, 9),
        finished_at=datetime(2026, 6, 11, 7, 18, 57),
        updated_at=datetime(2026, 6, 11, 7, 18, 57),
        latest_abnormal_reason_json=None,
    )

    reason = _task_abnormal_reason(row)

    assert reason is not None
    assert reason["code"] == "execution_lock_conflict"
    assert reason["title"] == "任务执行锁冲突"
