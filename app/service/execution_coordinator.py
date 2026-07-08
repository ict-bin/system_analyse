"""Celery 执行协调器: claim / renew_lease / commit_terminal / still_owner / recover。

所有操作都是 DB CAS (Compare-And-Swap), 用 owner_id + epoch + control_version 防双跑。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from sqlalchemy.orm import Session

from app.db.models import AppSaTask
from app.time_utils import now_local

LEASE_TTL_SECONDS = int(__import__("os").environ.get("SA_LEASE_TTL_SECONDS", "300"))
HEARTBEAT_INTERVAL_SECONDS = int(__import__("os").environ.get("SA_HEARTBEAT_INTERVAL_SECONDS", "60"))


@dataclass
class ClaimedTask:
    task_id: str
    epoch: int
    control_version: int
    dispatch_status: str | None = None
    output_path: str | None = None
    project_id: str | None = None


@dataclass
class ExecutionSnapshot:
    task_id: str
    status: str
    execution_owner_id: str | None
    execution_epoch: int
    control_version: int
    dispatch_status: str | None
    execution_lease_until: object | None
    execution_heartbeat_at: object | None


def _lease_deadline():
    return now_local() + timedelta(seconds=LEASE_TTL_SECONDS)


def claim_specific_task(db: Session, owner_id: str, task_id: str) -> ClaimedTask | None:
    """Celery worker 收到消息后按 task_id 认领 (非竞争性)。

    pending → 设 owner/epoch/lease, 返回 ClaimedTask
    running + 租约过期 → 孤儿重抢, 回 pending 再认领
    running + 租约新鲜 → 返回 None (别的 worker 在跑)
    已终态 → 返回 None
    """
    now = now_local()
    candidate = (
        db.query(AppSaTask)
        .filter(AppSaTask.task_id == task_id, AppSaTask.is_deleted.is_(False))
        .first()
    )
    if candidate is None:
        return None
    status = str(candidate.status or "pending")
    if status == "pending":
        expected_status = "pending"
    elif status == "running" and (
        candidate.execution_lease_until is None or candidate.execution_lease_until < now
    ):
        expected_status = "running"
    else:
        return None

    new_epoch = int(candidate.execution_epoch or 0) + 1
    update_fields = {
        AppSaTask.execution_owner_id: owner_id,
        AppSaTask.execution_lease_until: _lease_deadline(),
        AppSaTask.execution_heartbeat_at: now,
        AppSaTask.execution_epoch: new_epoch,
        AppSaTask.dispatch_status: "leased",
        AppSaTask.started_at: now,
        AppSaTask.finished_at: None,
        AppSaTask.error: None,
    }
    if expected_status == "running":
        update_fields[AppSaTask.status] = "pending"

    lease_cond = (
        (AppSaTask.execution_lease_until.is_(None)) | (AppSaTask.execution_lease_until < now)
    ) if expected_status == "running" else AppSaTask.status.is_not(None)

    updated = (
        db.query(AppSaTask)
        .filter(
            AppSaTask.id == candidate.id,
            AppSaTask.is_deleted.is_(False),
            AppSaTask.status == expected_status,
            lease_cond,
        )
        .update(update_fields, synchronize_session=False)
    )
    db.commit()
    if not updated:
        return None
    return ClaimedTask(
        task_id=str(candidate.task_id),
        epoch=new_epoch,
        control_version=int(candidate.control_version or 0),
        dispatch_status="leased",
        output_path=candidate.output_path,
        project_id=candidate.project_id,
    )


def renew_lease(db: Session, task_id: str, owner_id: str, epoch: int) -> bool:
    updated = (
        db.query(AppSaTask)
        .filter(
            AppSaTask.task_id == task_id,
            AppSaTask.execution_owner_id == owner_id,
            AppSaTask.execution_epoch == epoch,
            AppSaTask.is_deleted.is_(False),
            AppSaTask.status == "running",
        )
        .update({
            AppSaTask.execution_lease_until: _lease_deadline(),
            AppSaTask.execution_heartbeat_at: now_local(),
        }, synchronize_session=False)
    )
    db.commit()
    return bool(updated)


def release_lease(db: Session, task_id: str, owner_id: str, epoch: int) -> bool:
    updated = (
        db.query(AppSaTask)
        .filter(
            AppSaTask.task_id == task_id,
            AppSaTask.execution_owner_id == owner_id,
            AppSaTask.execution_epoch == epoch,
        )
        .update({
            AppSaTask.execution_owner_id: None,
            AppSaTask.execution_lease_until: None,
            AppSaTask.dispatch_status: None,
        }, synchronize_session=False)
    )
    db.commit()
    return bool(updated)


def begin_execution_if_owner(db: Session, task_id: str, owner_id: str, epoch: int, control_version: int, *, started_at) -> bool:
    updated = (
        db.query(AppSaTask)
        .filter(
            AppSaTask.task_id == task_id,
            AppSaTask.execution_owner_id == owner_id,
            AppSaTask.execution_epoch == epoch,
            AppSaTask.control_version == control_version,
            AppSaTask.is_deleted.is_(False),
            AppSaTask.status.in_(["pending", "running"]),
        )
        .update({
            AppSaTask.status: "running",
            AppSaTask.dispatch_status: "running",
            AppSaTask.started_at: started_at,
            AppSaTask.finished_at: None,
            AppSaTask.error: None,
        }, synchronize_session=False)
    )
    db.commit()
    return bool(updated)


def commit_terminal_state_if_owner(
    db: Session, task_id: str, owner_id: str, epoch: int, control_version: int, *,
    status: str, finished_at, stages_json: dict, result_json: dict | None, error: str | None,
) -> bool:
    updated = (
        db.query(AppSaTask)
        .filter(
            AppSaTask.task_id == task_id,
            AppSaTask.execution_owner_id == owner_id,
            AppSaTask.execution_epoch == epoch,
            AppSaTask.control_version == control_version,
            AppSaTask.is_deleted.is_(False),
            AppSaTask.status == "running",
        )
        .update({
            AppSaTask.status: status,
            AppSaTask.finished_at: finished_at,
            AppSaTask.stages_json: stages_json,
            AppSaTask.result_json: result_json,
            AppSaTask.error: error,
            AppSaTask.execution_owner_id: None,
            AppSaTask.execution_lease_until: None,
            AppSaTask.execution_heartbeat_at: None,
            AppSaTask.dispatch_status: None,
            AppSaTask.celery_task_id: None,
        }, synchronize_session=False)
    )
    db.commit()
    return bool(updated)


def still_owner(db: Session, task_id: str, owner_id: str, epoch: int, control_version: int) -> bool:
    row = db.query(AppSaTask).filter(
        AppSaTask.task_id == task_id, AppSaTask.is_deleted.is_(False)
    ).first()
    if row is None:
        return False
    return (
        row.execution_owner_id == owner_id
        and int(row.execution_epoch or 0) == int(epoch)
        and int(row.control_version or 0) == int(control_version)
        and row.status in {"pending", "running"}
    )


def load_execution_snapshot(db: Session, task_id: str) -> ExecutionSnapshot | None:
    row = db.query(AppSaTask).filter(
        AppSaTask.task_id == task_id, AppSaTask.is_deleted.is_(False)
    ).first()
    if row is None:
        return None
    return ExecutionSnapshot(
        task_id=row.task_id,
        status=str(row.status or ""),
        execution_owner_id=row.execution_owner_id,
        execution_epoch=int(row.execution_epoch or 0),
        control_version=int(row.control_version or 0),
        dispatch_status=row.dispatch_status,
        execution_lease_until=row.execution_lease_until,
        execution_heartbeat_at=row.execution_heartbeat_at,
    )


def recover_running_task_for_cleanup(
    db: Session, task_id: str, owner_id: str, epoch: int, control_version: int, *, reason: str = "worker_cleanup",
) -> bool:
    """Worker finally 兜底: DB still running + owner==self → reset pending (让 dispatcher 重发)."""
    updated = (
        db.query(AppSaTask)
        .filter(
            AppSaTask.task_id == task_id,
            AppSaTask.execution_owner_id == owner_id,
            AppSaTask.execution_epoch == epoch,
            AppSaTask.control_version == control_version,
            AppSaTask.is_deleted.is_(False),
            AppSaTask.status == "running",
        )
        .update({
            AppSaTask.status: "pending",
            AppSaTask.error: None,
            AppSaTask.result_json: None,
            AppSaTask.finished_at: None,
            AppSaTask.stages_json: None,
            AppSaTask.latest_abnormal_reason_json: None,
            AppSaTask.execution_owner_id: None,
            AppSaTask.execution_lease_until: None,
            AppSaTask.execution_heartbeat_at: None,
            AppSaTask.dispatch_status: "pending",
            AppSaTask.celery_task_id: None,
        }, synchronize_session=False)
    )
    db.commit()
    return bool(updated)
