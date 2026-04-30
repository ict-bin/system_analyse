"""Task management service for secflow-app-system-analyse.

Bridges the FastAPI management layer with the existing Orchestrator engine.
Each task is persisted in MySQL and executed asynchronously.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from app.config import build_task_config, get_service_yaml, load_service_config
from app.db.models import AppSaTask
from app.logging_utils import log_event
from app.models import TaskStatus
from app.orchestrator import Orchestrator

logger = logging.getLogger("sa.task_service")

SERVICE_CONFIG_PATH = os.environ.get("SERVICE_CONFIG", "/app/config.json")

# Running asyncio tasks keyed by task_id so we can cancel them
_running_tasks: dict[str, asyncio.Task] = {}


def _load_svc_config():
    for p in [SERVICE_CONFIG_PATH, "/opt/system_analyse/config.example.json"]:
        if os.path.isfile(p):
            return load_service_config(p)
    raise RuntimeError(f"Service config not found: {SERVICE_CONFIG_PATH}")


def generate_prompt_from_path(input_path: str) -> str:
    """Generate a default Chinese analysis prompt from the input path."""
    path_lower = input_path.lower()
    if any(kw in path_lower for kw in ("firmware", "unpacked", "squashfs", "rootfs")):
        subject = "固件解包后的所有文件"
    elif any(kw in path_lower for kw in ("binary", "bin", "elf")):
        subject = "二进制可执行文件"
    elif any(kw in path_lower for kw in ("script", "sh", "py", "lua")):
        subject = "脚本文件"
    elif any(kw in path_lower for kw in ("config", "conf", "cfg", "etc")):
        subject = "配置文件"
    elif any(kw in path_lower for kw in ("source", "src", ".c", ".cpp", ".h")):
        subject = "源代码文件"
    else:
        subject = "目标文件"

    return (
        f"对路径 `{input_path}` 下的{subject}进行系统性安全分析，"
        "重点关注：威胁识别、模块功能分类、安全漏洞、敏感信息暴露及风险等级评估。"
    )


class TaskService:
    def list_tasks(
        self,
        db: Session,
        *,
        project_id: str,
        page: int = 1,
        per_page: int = 20,
        status: Optional[str] = None,
    ) -> dict:
        query = db.query(AppSaTask).filter(
            AppSaTask.project_id == project_id,
            AppSaTask.is_deleted.is_(False),
        )
        if status:
            query = query.filter(AppSaTask.status == status)
        total = query.count()
        rows = (
            query.order_by(AppSaTask.created_at.desc())
            .offset((page - 1) * per_page)
            .limit(per_page)
            .all()
        )
        return {
            "items": [self._row_to_dict(r) for r in rows],
            "total": total,
            "page": page,
            "per_page": per_page,
        }

    def get_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        return self._row_to_dict(row)

    def create_task(
        self,
        db: Session,
        *,
        project_id: str,
        task_name: str,
        input_path: str,
        output_path: Optional[str] = None,
        task_description: Optional[str] = None,
        prompt_template_id: Optional[str] = None,
        prompt_content: str,
        created_by: Optional[str] = None,
    ) -> dict:
        task_id = f"sat_{uuid.uuid4().hex[:16]}"
        effective_output = output_path or os.environ.get("OUTPUT_DIR", "/data/output")

        row = AppSaTask(
            task_id=task_id,
            project_id=project_id,
            task_name=task_name,
            task_description=task_description,
            input_path=input_path,
            output_path=effective_output,
            prompt_template_id=prompt_template_id,
            prompt_content=prompt_content,
            status="pending",
            created_by=created_by,
        )
        db.add(row)
        db.commit()
        db.refresh(row)

        # Fire background execution
        asyncio_task = asyncio.create_task(
            self._execute_task(task_id),
            name=f"sa_task_{task_id}",
        )
        _running_tasks[task_id] = asyncio_task

        log_event(logger, logging.INFO, "task created", event="task_created", task_id=task_id, project_id=project_id)
        return self._row_to_dict(row)

    def restart_task(self, db: Session, task_id: str) -> dict:
        """Create and immediately start a new task cloned from an existing one.

        The new task inherits all parameters (input_path, prompt_content, etc.)
        but runs against the current service configuration.
        Active tasks (pending / running) cannot be restarted; cancel them first.
        """
        row = self._get_or_404(db, task_id)
        if row.status in ("pending", "running"):
            from fastapi import HTTPException
            raise HTTPException(400, "任务仍在运行中，请先取消后再重启")

        new_task_id = f"sat_{uuid.uuid4().hex[:16]}"
        effective_output = row.output_path or os.environ.get("OUTPUT_DIR", "/data/output")

        new_row = AppSaTask(
            task_id=new_task_id,
            project_id=row.project_id,
            task_name=row.task_name,
            task_description=row.task_description,
            input_path=row.input_path,
            output_path=effective_output,
            prompt_template_id=row.prompt_template_id,
            prompt_content=row.prompt_content,
            status="pending",
            created_by=row.created_by,
        )
        db.add(new_row)
        db.commit()
        db.refresh(new_row)

        asyncio_task = asyncio.create_task(
            self._execute_task(new_task_id),
            name=f"sa_task_{new_task_id}",
        )
        _running_tasks[new_task_id] = asyncio_task

        log_event(
            logger, logging.INFO, "task restarted",
            event="task_restarted",
            task_id=new_task_id,
            original_task_id=task_id,
            project_id=row.project_id,
        )
        return self._row_to_dict(new_row)

    def cancel_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        if row.status in ("passed", "failed", "error", "cancelled"):
            return self._row_to_dict(row)

        # Cancel the asyncio task if still running
        at = _running_tasks.get(task_id)
        if at and not at.done():
            at.cancel()

        row.status = "cancelled"
        row.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
        db.commit()
        db.refresh(row)
        return self._row_to_dict(row)

    # ── private ──────────────────────────────────────────────────────────────

    async def _execute_task(self, task_id: str) -> None:
        """Run the Orchestrator engine and persist results."""
        from app.db import get_db
        # Get a fresh DB session for background task
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            row = db.query(AppSaTask).filter_by(task_id=task_id).first()
            if not row or row.status == "cancelled":
                return

            row.status = "running"
            row.started_at = datetime.now(timezone.utc).replace(tzinfo=None)
            db.commit()

            svc = _load_svc_config()
            cfg = build_task_config(svc, row.prompt_content, cwd=row.input_path)

            orch = Orchestrator(config=cfg)
            result = await orch.execute(task_id)

            # Re-fetch row in case it was cancelled externally
            db.expire(row)
            db.refresh(row)
            if row.status == "cancelled":
                return

            row.status = result.status.value if result else "error"
            row.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
            if result:
                row.result_json = result.model_dump(mode="json")
                if result.error:
                    row.error = result.error
            db.commit()

        except asyncio.CancelledError:
            # Task was cancelled externally; status already set by cancel_task()
            pass
        except Exception as exc:
            log_event(logger, logging.ERROR, "task execution failed", event="task_error", task_id=task_id, error=str(exc))
            try:
                db.rollback()
                r = db.query(AppSaTask).filter_by(task_id=task_id).first()
                if r and r.status == "running":
                    r.status = "error"
                    r.error = str(exc)
                    r.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
                    db.commit()
            except Exception:
                pass
        finally:
            _running_tasks.pop(task_id, None)
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _get_or_404(self, db: Session, task_id: str) -> AppSaTask:
        row = db.query(AppSaTask).filter(
            AppSaTask.task_id == task_id,
            AppSaTask.is_deleted.is_(False),
        ).first()
        if not row:
            from fastapi import HTTPException
            raise HTTPException(404, f"任务不存在: {task_id}")
        return row

    @staticmethod
    def _row_to_dict(row: AppSaTask) -> dict:
        def fmt(dt: datetime | None) -> str | None:
            return dt.isoformat() if dt else None

        return {
            "task_id": row.task_id,
            "project_id": row.project_id,
            "task_name": row.task_name,
            "task_description": row.task_description,
            "input_path": row.input_path,
            "output_path": row.output_path,
            "prompt_template_id": row.prompt_template_id,
            "prompt_content": row.prompt_content,
            "status": row.status,
            "error": row.error,
            "result_json": row.result_json,
            "created_by": row.created_by,
            "created_at": fmt(row.created_at),
            "updated_at": fmt(row.updated_at),
            "started_at": fmt(row.started_at),
            "finished_at": fmt(row.finished_at),
        }


_task_service: TaskService | None = None


def get_task_service() -> TaskService:
    global _task_service
    if _task_service is None:
        _task_service = TaskService()
    return _task_service
