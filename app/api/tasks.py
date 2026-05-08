"""Task management API routes."""

from __future__ import annotations

import asyncio
from typing import Optional

from fastapi import Depends, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db import get_db
from app.service.task_service import (
    _parse_session_jsonl_lines,
    _resolve_session_path,
    _task_sessions_root,
    generate_prompt_from_path,
    get_task_service,
)
from app.db.models import AppSaTask

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
    task_origin_type: Optional[str] = None
    parent_project_id: Optional[str] = None
    parent_task_id: Optional[str] = None
    parent_task_type: Optional[str] = None
    parent_stage_name: Optional[str] = None
    parent_stage_item_id: Optional[str] = None
    parent_stage_item_key: Optional[str] = None


class GeneratePromptRequest(BaseModel):
    input_path: str


class TaskResultSummaryResponse(BaseModel):
    module_count: int = 0
    high_risk_module_count: int = 0
    medium_risk_module_count: int = 0
    low_risk_module_count: int = 0
    total_file_count: int = 0
    threat_count: int = 0


class TaskResultModuleSectionResponse(BaseModel):
    level: int
    title: str
    anchor: str


class TaskResultModuleResponse(BaseModel):
    module_name: str
    rank: int
    module_dir_path: Optional[str] = None
    files_list_path: Optional[str] = None
    module_report_path: Optional[str] = None
    module_report_markdown: Optional[str] = None
    files: list[str] = Field(default_factory=list)
    file_count: int = 0
    risk_level: Optional[str] = None
    risk_score: Optional[int] = None
    report_sections: list[TaskResultModuleSectionResponse] = Field(default_factory=list)
    report_preview: Optional[str] = None


class TaskResultResponse(BaseModel):
    task_id: str
    available: bool
    status: str
    output_root: Optional[str] = None
    final_report_path: Optional[str] = None
    modules_list_path: Optional[str] = None
    final_report_markdown: Optional[str] = None
    modules: list[TaskResultModuleResponse] = Field(default_factory=list)
    summary: TaskResultSummaryResponse
    warnings: list[str] = Field(default_factory=list)


class TaskSessionMetaResponse(BaseModel):
    session_id: str
    session_name: str
    relative_path: str
    stage_group: str
    role_name: str
    size: int
    mtime: float
    event_count: int = 0
    line_count: int = 0
    is_active: bool = False
    display_name: str
    warnings: list[str] = Field(default_factory=list)


class TaskSessionFileResponse(BaseModel):
    path: str
    session_meta: dict = Field(default_factory=dict)
    events: list[dict] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    line_count: int = 0


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
        task_origin_type=body.task_origin_type,
        parent_project_id=body.parent_project_id,
        parent_task_id=body.parent_task_id,
        parent_task_type=body.parent_task_type,
        parent_stage_name=body.parent_stage_name,
        parent_stage_item_id=body.parent_stage_item_id,
        parent_stage_item_key=body.parent_stage_item_key,
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


@router.get("/tasks/{task_id}/result", response_model=TaskResultResponse)
async def get_task_result(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task_result(db, task_id)


@router.get("/tasks/{task_id}/sessions", response_model=list[TaskSessionMetaResponse])
async def list_task_sessions(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().list_task_sessions(db, task_id)


@router.get("/tasks/{task_id}/sessions/file", response_model=TaskSessionFileResponse)
async def get_task_session_file(task_id: str, path: str = Query(...), db: Session = Depends(get_db)):
    return get_task_service().get_task_session_file(db, task_id, path)


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().cancel_task(db, task_id)


@router.post("/tasks/{task_id}/restart", status_code=201)
async def restart_task(task_id: str, db: Session = Depends(get_db)):
    """Reset and restart an existing task in-place, reusing the same task ID."""
    return get_task_service().restart_task(db, task_id)


@router.post("/tasks/{task_id}/resume", status_code=201)
async def resume_task(task_id: str, db: Session = Depends(get_db)):
    """Resume a task from Stage 3 (断点续跑), reusing the same task ID."""
    return get_task_service().resume_task(db, task_id)


@router.delete("/tasks/{task_id}", status_code=204)
async def delete_task(
    task_id: str,
    delete_files: bool = True,
    db: Session = Depends(get_db),
):
    """删除任务记录（软删除），并可选同步删除输出目录下的任务文件。"""
    get_task_service().delete_task(db, task_id, delete_files=delete_files)


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


@router.websocket("/tasks/{task_id}/sessions/ws")
async def stream_task_session(task_id: str, websocket: WebSocket, db: Session = Depends(get_db)):
    await websocket.accept()
    svc = get_task_service()
    row = db.query(AppSaTask).filter(
        AppSaTask.task_id == task_id,
        AppSaTask.is_deleted.is_(False),
    ).first()
    if not row:
        await websocket.send_json({"type": "error", "message": f"任务不存在: {task_id}"})
        await websocket.close(code=4404)
        return

    sessions_root = _task_sessions_root(row)
    if not sessions_root or not sessions_root.is_dir():
        await websocket.send_json({"type": "error", "message": "会话目录不存在"})
        await websocket.close(code=4404)
        return

    try:
        initial = await websocket.receive_json()
    except WebSocketDisconnect:
        return
    except Exception:
        await websocket.send_json({"type": "error", "message": "订阅消息格式错误"})
        await websocket.close(code=4400)
        return

    if initial.get("type") != "subscribe":
        await websocket.send_json({"type": "error", "message": "首次消息必须为 subscribe"})
        await websocket.close(code=4400)
        return

    try:
        relative_path = str(initial.get("path") or "")
        offset = int(initial.get("offset") or 0)
        target = _resolve_session_path(sessions_root, relative_path)
    except ValueError as exc:
        await websocket.send_json({"type": "error", "message": str(exc)})
        await websocket.close(code=4400)
        return

    if not target.is_file():
        await websocket.send_json({"type": "error", "message": f"会话文件不存在: {relative_path}"})
        await websocket.close(code=4404)
        return

    try:
        snapshot = svc.get_task_session_file(db, task_id, relative_path)
    except Exception as exc:
        await websocket.send_json({"type": "error", "message": str(exc)})
        await websocket.close(code=4500)
        return

    await websocket.send_json({
        "type": "session_snapshot",
        "path": snapshot["path"],
        "session_meta": snapshot["session_meta"],
        "warnings": snapshot["warnings"],
        "line_count": snapshot["line_count"],
        "event_count": len(snapshot["events"]),
    })

    current_offset = max(0, offset)
    if current_offset < snapshot["line_count"]:
        lines = target.read_text("utf-8", errors="replace").splitlines()
        _, delta_events, delta_warnings, _ = _parse_session_jsonl_lines(lines[current_offset:], start_line=current_offset + 1)
        if delta_events or delta_warnings:
            await websocket.send_json({
                "type": "session_delta",
                "path": snapshot["path"],
                "offset": current_offset,
                "line_count": snapshot["line_count"],
                "events": delta_events,
                "warnings": delta_warnings,
            })
        current_offset = snapshot["line_count"]

    last_keepalive = asyncio.get_running_loop().time()
    try:
        while True:
            try:
                message = await asyncio.wait_for(websocket.receive_json(), timeout=1.0)
                msg_type = message.get("type")
                if msg_type == "ping":
                    await websocket.send_json({"type": "pong"})
                elif msg_type == "subscribe":
                    relative_path = str(message.get("path") or relative_path)
                    offset = int(message.get("offset") or 0)
                    target = _resolve_session_path(sessions_root, relative_path)
                    snapshot = svc.get_task_session_file(db, task_id, relative_path)
                    await websocket.send_json({
                        "type": "session_snapshot",
                        "path": snapshot["path"],
                        "session_meta": snapshot["session_meta"],
                        "warnings": snapshot["warnings"],
                        "line_count": snapshot["line_count"],
                        "event_count": len(snapshot["events"]),
                    })
                    current_offset = max(0, offset)
                    if current_offset < snapshot["line_count"]:
                        lines = target.read_text("utf-8", errors="replace").splitlines()
                        _, delta_events, delta_warnings, _ = _parse_session_jsonl_lines(lines[current_offset:], start_line=current_offset + 1)
                        await websocket.send_json({
                            "type": "session_delta",
                            "path": snapshot["path"],
                            "offset": current_offset,
                            "line_count": snapshot["line_count"],
                            "events": delta_events,
                            "warnings": delta_warnings,
                        })
                        current_offset = snapshot["line_count"]
            except asyncio.TimeoutError:
                pass

            if not target.exists():
                await websocket.send_json({"type": "error", "message": f"会话文件不存在: {relative_path}"})
                return

            lines = target.read_text("utf-8", errors="replace").splitlines()
            current_line_count = sum(1 for line in lines if line.strip())
            if current_line_count < current_offset:
                current_offset = 0
                await websocket.send_json({
                    "type": "session_rotated",
                    "path": relative_path,
                    "message": "会话文件已重置，请重新加载",
                })
            elif current_line_count > current_offset:
                _, delta_events, delta_warnings, _ = _parse_session_jsonl_lines(lines[current_offset:], start_line=current_offset + 1)
                await websocket.send_json({
                    "type": "session_delta",
                    "path": relative_path,
                    "offset": current_offset,
                    "line_count": current_line_count,
                    "events": delta_events,
                    "warnings": delta_warnings,
                })
                current_offset = current_line_count

            now = asyncio.get_running_loop().time()
            if now - last_keepalive >= 15:
                await websocket.send_json({"type": "pong"})
                last_keepalive = now
    except WebSocketDisconnect:
        return
