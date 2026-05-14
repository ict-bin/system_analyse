from __future__ import annotations

from datetime import datetime, timedelta
from typing import Callable

from sqlalchemy import func, or_, text
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.db.models import AppSaTask
from app.service.event_log import clear_events, events_path
from app.time_utils import now_local


class TaskRepository:
    @staticmethod
    def count_running_tasks(db: Session) -> int:
        return int(
            db.query(func.count(AppSaTask.id)).filter(
                AppSaTask.is_deleted.is_(False),
                AppSaTask.status == "running",
            ).scalar()
            or 0
        )

    @staticmethod
    def get_status_counts(db: Session) -> dict[str, int]:
        rows = (
            db.query(AppSaTask.status, func.count(AppSaTask.id))
            .filter(AppSaTask.is_deleted.is_(False))
            .group_by(AppSaTask.status)
            .all()
        )
        return {str(status): int(count or 0) for status, count in rows}

    @staticmethod
    def get_oldest_pending_created_at(db: Session) -> datetime | None:
        return (
            db.query(func.min(AppSaTask.created_at))
            .filter(
                AppSaTask.is_deleted.is_(False),
                AppSaTask.status == "pending",
            )
            .scalar()
        )

    @staticmethod
    def list_running_tasks(db: Session, limit: int = 20) -> list[AppSaTask]:
        return (
            db.query(AppSaTask)
            .filter(
                AppSaTask.is_deleted.is_(False),
                AppSaTask.status == "running",
            )
            .order_by(AppSaTask.dispatch_started_at.asc(), AppSaTask.created_at.asc())
            .limit(max(1, limit))
            .all()
        )

    @staticmethod
    def list_tasks_assigned_to_instance(db: Session, *, instance_id: str, limit: int) -> list[AppSaTask]:
        return (
            db.query(AppSaTask)
            .filter(
                AppSaTask.is_deleted.is_(False),
                AppSaTask.status == "running",
                AppSaTask.dispatcher_instance_id == instance_id,
            )
            .order_by(AppSaTask.dispatch_started_at.asc(), AppSaTask.created_at.asc())
            .limit(max(1, limit))
            .all()
        )

    @staticmethod
    def get_running_task_counts_by_instance(db: Session, instance_ids: list[str]) -> dict[str, int]:
        normalized_ids = [str(instance_id).strip() for instance_id in instance_ids if str(instance_id).strip()]
        if not normalized_ids:
            return {}
        rows = (
            db.query(AppSaTask.dispatcher_instance_id, func.count(AppSaTask.id))
            .filter(
                AppSaTask.is_deleted.is_(False),
                AppSaTask.status == "running",
                AppSaTask.dispatcher_instance_id.in_(normalized_ids),
            )
            .group_by(AppSaTask.dispatcher_instance_id)
            .all()
        )
        return {
            str(instance_id): int(count or 0)
            for instance_id, count in rows
            if instance_id
        }

    @staticmethod
    def try_acquire_global_claim_lock(db: Session, *, lock_key: str, timeout_seconds: int = 1) -> bool:
        bind = db.get_bind()
        dialect_name = str(getattr(getattr(bind, "dialect", None), "name", "") or "").lower()
        if dialect_name != "mysql":
            return True
        result = db.execute(
            text("SELECT GET_LOCK(:lock_key, :timeout_seconds)"),
            {"lock_key": lock_key, "timeout_seconds": max(0, int(timeout_seconds))},
        ).scalar()
        return bool(result)

    @staticmethod
    def release_global_claim_lock(db: Session, *, lock_key: str) -> None:
        bind = db.get_bind()
        dialect_name = str(getattr(getattr(bind, "dialect", None), "name", "") or "").lower()
        if dialect_name != "mysql":
            return
        try:
            db.execute(text("SELECT RELEASE_LOCK(:lock_key)"), {"lock_key": lock_key})
        except Exception:
            db.rollback()

    @staticmethod
    def get_task(db: Session, task_id: str) -> AppSaTask | None:
        return db.query(AppSaTask).filter_by(task_id=task_id).first()

    @staticmethod
    def get_task_not_deleted(db: Session, task_id: str) -> AppSaTask | None:
        return db.query(AppSaTask).filter(
            AppSaTask.task_id == task_id,
            AppSaTask.is_deleted.is_(False),
        ).first()

    @staticmethod
    def list_pending_tasks(db: Session, limit: int) -> list[AppSaTask]:
        return db.query(AppSaTask).filter(
            AppSaTask.is_deleted.is_(False),
            AppSaTask.status == "pending",
        ).order_by(AppSaTask.created_at.asc()).limit(limit).all()

    @staticmethod
    def restart_task_in_place(db: Session, row: AppSaTask) -> AppSaTask:
        clean_config = {
            k: v for k, v in (row.task_config_json or {}).items()
            if k not in ("start_stage", "resume_workspace", "resolved_config_snapshot")
        } or None
        row.task_config_json = clean_config
        row.status = "pending"
        row.started_at = None
        row.finished_at = None
        row.stages_json = None  # 清除旧 DB 字段
        row.result_json = None
        row.error = None
        row.dispatcher_instance_id = None
        row.dispatch_started_at = None
        row.lease_expires_at = None
        flag_modified(row, "task_config_json")
        db.commit()
        db.refresh(row)
        # 同步清除 events.jsonl（重跑从头）
        clear_events(events_path(row.output_path, row.task_id))
        return row

    @staticmethod
    def resume_task_in_place(db: Session, row: AppSaTask) -> AppSaTask:
        """断点续跑：保留 workspace 和 .checkpoint/，不设置 start_stage/resume_workspace。

        与旧版的区别：
        - 不清除 started_at（续跑保留原始开始时间）
        - 不清除 stages_json（续跑保留历史事件流）
        - 不向 task_config_json 写入 start_stage/resume_workspace
        - 断点由文件系统 .checkpoint/ 目录驱动，无需 DB 字段控制
        """
        clean_config = {
            k: v for k, v in (row.task_config_json or {}).items()
            if k not in ("start_stage", "resume_workspace", "resolved_config_snapshot")
        } or None
        row.task_config_json = clean_config
        row.status = "pending"
        row.finished_at = None
        row.result_json = None
        row.error = None
        row.dispatcher_instance_id = None
        row.dispatch_started_at = None
        row.lease_expires_at = None
        # 保留 started_at 和 stages_json（续跑不重置历史）
        flag_modified(row, "task_config_json")
        db.commit()
        db.refresh(row)
        return row

    @staticmethod
    def cancel_task_in_place(db: Session, row: AppSaTask) -> AppSaTask:
        row.status = "cancelled"
        row.finished_at = now_local()
        row.dispatcher_instance_id = None
        row.dispatch_started_at = None
        row.lease_expires_at = None
        db.commit()
        db.refresh(row)
        return row

    @staticmethod
    def soft_delete_task(db: Session, row: AppSaTask) -> None:
        row.is_deleted = True
        db.commit()

    @staticmethod
    def recover_stale_running_tasks(
        db: Session,
        *,
        now: datetime,
        lease_timeout_seconds: int,
        clear_task_execution_lock: Callable[[str | None, str], None],
    ) -> int:
        stale_rows = db.query(AppSaTask).filter(
            AppSaTask.is_deleted.is_(False),
            AppSaTask.status == "running",
            or_(
                AppSaTask.lease_expires_at < now,
                (
                    AppSaTask.lease_expires_at.is_(None)
                    & AppSaTask.dispatch_started_at.is_not(None)
                    & (AppSaTask.dispatch_started_at < now - timedelta(seconds=lease_timeout_seconds))
                ),
            ),
        ).all()
        for stale in stale_rows:
            stale.status = "pending"
            stale.error = "任务租约过期，已重新排队"
            stale.dispatcher_instance_id = None
            stale.dispatch_started_at = None
            stale.lease_expires_at = None
            stale.finished_at = None
            clear_task_execution_lock(stale.output_path, stale.task_id)
        if stale_rows:
            db.commit()
        return len(stale_rows)

    @staticmethod
    def claim_task_lease(
        db: Session,
        row: AppSaTask,
        *,
        worker_instance_id: str,
        lease_deadline: Callable[[], datetime],
    ) -> int | None:
        lease_epoch = int(getattr(row, "lease_epoch", 0) or 0) + 1
        now = now_local()
        claimed = db.query(AppSaTask).filter(
            AppSaTask.task_id == row.task_id,
            AppSaTask.is_deleted.is_(False),
            AppSaTask.status == "pending",
        ).update(
            {
                "status": "running",
                "started_at": row.started_at or now,
                "finished_at": None,
                "error": None,
                "dispatcher_instance_id": worker_instance_id,
                "dispatch_started_at": now,
                "lease_epoch": lease_epoch,
                "lease_expires_at": lease_deadline(),
            },
            synchronize_session=False,
        )
        if not claimed:
            db.rollback()
            return None
        db.commit()
        return lease_epoch

    @staticmethod
    def save_resolved_config_snapshot(
        db: Session,
        *,
        task_id: str,
        lease_epoch: int,
        worker_instance_id: str,
        task_config_json: dict,
        resolved_snapshot: dict,
        lease_deadline: Callable[[], datetime],
    ) -> bool:
        updated = db.query(AppSaTask).filter(
            AppSaTask.task_id == task_id,
            AppSaTask.is_deleted.is_(False),
            AppSaTask.status == "running",
            AppSaTask.dispatcher_instance_id == worker_instance_id,
            AppSaTask.lease_epoch == lease_epoch,
        ).update(
            {
                "task_config_json": {
                    **task_config_json,
                    "resolved_config_snapshot": resolved_snapshot,
                },
                "dispatch_started_at": now_local(),
                "lease_expires_at": lease_deadline(),
            },
            synchronize_session=False,
        )
        if not updated:
            db.rollback()
            return False
        db.commit()
        return True

    @staticmethod
    def heartbeat_task_lease(
        db: Session,
        *,
        task_id: str,
        lease_epoch: int,
        worker_instance_id: str,
        lease_deadline: Callable[[], datetime],
    ) -> bool:
        updated = db.query(AppSaTask).filter(
            AppSaTask.task_id == task_id,
            AppSaTask.is_deleted.is_(False),
            AppSaTask.status == "running",
            AppSaTask.dispatcher_instance_id == worker_instance_id,
            AppSaTask.lease_epoch == lease_epoch,
        ).update(
            {
                "dispatch_started_at": now_local(),
                "lease_expires_at": lease_deadline(),
            },
            synchronize_session=False,
        )
        db.commit()
        return bool(updated)

    @staticmethod
    @staticmethod
    def finalize_task_result(
        db: Session,
        *,
        task_id: str,
        lease_epoch: int,
        worker_instance_id: str,
        result_status: str,
        result_json: dict | None,
        result_error: str | None,
    ) -> bool:
        """Finalize task status/result. stages_json is no longer written to DB
        (events are persisted to {output_path}/{task_id}/run/events.jsonl by event_log)."""
        values = {
            "status": result_status,
            "finished_at": now_local(),
            "dispatcher_instance_id": None,
            "dispatch_started_at": None,
            "lease_expires_at": None,
        }
        if result_json is not None:
            values["result_json"] = result_json
        if result_error:
            values["error"] = result_error
        updated = db.query(AppSaTask).filter(
            AppSaTask.task_id == task_id,
            AppSaTask.is_deleted.is_(False),
            AppSaTask.status == "running",
            AppSaTask.dispatcher_instance_id == worker_instance_id,
            AppSaTask.lease_epoch == lease_epoch,
        ).update(values, synchronize_session=False)
        if not updated:
            db.rollback()
            return False
        db.commit()
        return True

    @staticmethod
    @staticmethod
    def finalize_task_error(
        db: Session,
        *,
        task_id: str,
        lease_epoch: int,
        error: str,
    ) -> bool:
        """Finalize task as error. stages_json no longer written to DB
        (events persisted to events.jsonl by event_log)."""
        db.rollback()
        row = db.query(AppSaTask).filter_by(task_id=task_id).first()
        if not row or row.status != "running":
            return False
        row.status = "error"
        row.error = error
        row.finished_at = now_local()
        row.dispatcher_instance_id = None
        row.dispatch_started_at = None
        row.lease_expires_at = None
        if int(row.lease_epoch or 0) != lease_epoch:
            return False
        db.commit()
        return True
