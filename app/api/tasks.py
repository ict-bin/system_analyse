"""Task management API routes."""

from __future__ import annotations

from typing import Optional

from fastapi import Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db import get_db
from app.service.task_service import generate_prompt_from_path, get_task_service

from . import router


class TaskCreateRequest(BaseModel):
    project_id: str
    task_name: str
    input_path: str
    output_path: Optional[str] = None
    task_description: Optional[str] = None
    prompt_template_id: Optional[str] = None
    prompt_content: Optional[str] = None  # If omitted, auto-generated from input_path
    analyse_targets: Optional[list[str]] = None  # Override service-level analyse_targets
    binary_arch: Optional[list[str]] = None      # Override service-level binary_arch


class GeneratePromptRequest(BaseModel):
    input_path: str


@router.post("/tasks", status_code=201)
async def create_task(body: TaskCreateRequest, db: Session = Depends(get_db)):
    prompt = body.prompt_content
    if not prompt or not prompt.strip():
        prompt = generate_prompt_from_path(body.input_path)

    svc = get_task_service()
    task_config: dict | None = None
    if body.analyse_targets is not None or body.binary_arch is not None:
        task_config = {}
        if body.analyse_targets is not None:
            task_config["analyse_targets"] = body.analyse_targets
        if body.binary_arch is not None:
            task_config["binary_arch"] = body.binary_arch
    return svc.create_task(
        db,
        project_id=body.project_id,
        task_name=body.task_name,
        input_path=body.input_path,
        output_path=body.output_path,
        task_description=body.task_description,
        prompt_template_id=body.prompt_template_id,
        prompt_content=prompt,
        task_config_json=task_config,
    )


@router.get("/tasks")
async def list_tasks(
    project_id: str = Query(...),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    return get_task_service().list_tasks(db, project_id=project_id, page=page, per_page=per_page, status=status)


@router.get("/tasks/{task_id}")
async def get_task(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task(db, task_id)


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().cancel_task(db, task_id)


@router.post("/tasks/{task_id}/restart", status_code=201)
async def restart_task(task_id: str, db: Session = Depends(get_db)):
    """Clone an existing task and start it immediately with the current service config."""
    return get_task_service().restart_task(db, task_id)


@router.get("/tasks/{task_id}/logs")
async def get_task_logs(task_id: str, db: Session = Depends(get_db)):
    """Return stages_json for the task (stage events used as structured log stream)."""
    from app.db.models import AppSaTask
    row = db.query(AppSaTask).filter(
        AppSaTask.task_id == task_id,
        AppSaTask.is_deleted.is_(False),
    ).first()
    if not row:
        from fastapi import HTTPException
        raise HTTPException(404, f"任务不存在: {task_id}")
    return {
        "task_id": task_id,
        "status": row.status,
        "stages_json": row.stages_json or {"events": []},
    }


@router.post("/generate-prompt")
async def generate_prompt(body: GeneratePromptRequest):
    """Auto-generate a prompt from an input path."""
    return {"prompt": generate_prompt_from_path(body.input_path)}
