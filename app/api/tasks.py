"""Task management API routes."""

from __future__ import annotations

from typing import Any, Optional

from fastapi import Depends, Query
from pydantic import BaseModel, Field
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
    analysis_mode: Optional[str] = None
    analyse_targets: Optional[list[str]] = None  # Override service-level analyse_targets
    binary_arch: Optional[list[str]] = None      # Override service-level binary_arch
    security_focus_categories: Optional[list[str]] = None  # Override S1 category filter
    module_granularity: Optional[str] = None               # Override module split granularity
    filter_engine: Optional[str] = None                    # Override filter engine
    enable_final_check: Optional[bool] = None              # Override service-level final_check enable flag
    task_origin_type: Optional[str] = None
    parent_project_id: Optional[str] = None
    parent_task_id: Optional[str] = None
    parent_task_type: Optional[str] = None
    parent_stage_name: Optional[str] = None
    parent_stage_item_id: Optional[str] = None
    parent_stage_item_key: Optional[str] = None


class GeneratePromptRequest(BaseModel):
    input_path: str


class TaskOriginRepairRequest(BaseModel):
    analysis_mode: str


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


class TaskSessionIndexNodeResponse(BaseModel):
    node_id: str
    relative_path: str
    session_name: str
    display_name: str
    role: str
    role_label: str
    status: str
    is_active: bool = False
    stage_key: str
    stage_label: str
    stage_order: int
    stage_group: str
    module_name: Optional[str] = None
    attempt: Optional[int] = None
    judge_index: Optional[int] = None
    batch_index: Optional[int] = None
    parent_relative_path: Optional[str] = None
    parallel_group: Optional[str] = None
    family_key: Optional[str] = None
    flow_kind: Optional[str] = None
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    started_ts: Optional[float] = None
    last_event_at: Optional[str] = None
    last_event_ts: Optional[float] = None
    mtime: float
    size: int
    event_count: int = 0
    line_count: int = 0
    warnings: list[str] = Field(default_factory=list)
    session_header: dict = Field(default_factory=dict)
    cwd: Optional[str] = None
    model: Optional[str] = None
    latest_round_ref: Optional[dict[str, Any]] = None
    round_refs: list[dict[str, Any]] = Field(default_factory=list)
    attempts_seen: list[int] = Field(default_factory=list)


class TaskSessionIndexEdgeResponse(BaseModel):
    edge_id: str
    source_node_id: str
    target_node_id: str
    kind: str
    label: str


class TaskSessionIndexGroupResponse(BaseModel):
    group_id: str
    kind: str
    label: str
    stage_key: Optional[str] = None
    module_name: Optional[str] = None
    node_ids: list[str] = Field(default_factory=list)


class TaskSessionIndexResponse(BaseModel):
    task_id: str
    status: str
    sessions_root: Optional[str] = None
    index_path: Optional[str] = None
    generated_at: Optional[str] = None
    summary: dict[str, Any] = Field(default_factory=dict)
    nodes: list[TaskSessionIndexNodeResponse] = Field(default_factory=list)
    edges: list[TaskSessionIndexEdgeResponse] = Field(default_factory=list)
    groups: list[TaskSessionIndexGroupResponse] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class TaskSessionFileResponse(BaseModel):
    path: str
    session_meta: dict = Field(default_factory=dict)
    events: list[dict] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    line_count: int = 0


class TaskEvaluationResponse(BaseModel):
    task_id: str
    status: str
    available: bool
    summary: Optional[dict[str, Any]] = None
    rounds: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


@router.post("/tasks", status_code=201)
async def create_task(body: TaskCreateRequest, db: Session = Depends(get_db)):
    analysis_mode = body.analysis_mode or body.parent_task_type
    prompt = body.prompt_content
    if not prompt or not prompt.strip():
        prompt = generate_prompt_from_path(body.input_path, analysis_mode)

    svc = get_task_service()
    task_config: dict | None = None
    _override_fields = (
        body.analyse_targets, body.binary_arch,
        body.security_focus_categories, body.module_granularity, body.filter_engine, body.enable_final_check,
    )
    if any(f is not None for f in _override_fields):
        task_config = {}
        if body.analyse_targets is not None:
            task_config["analyse_targets"] = body.analyse_targets
        if body.binary_arch is not None:
            task_config["binary_arch"] = body.binary_arch
        if body.security_focus_categories is not None:
            task_config["security_focus_categories"] = body.security_focus_categories
        if body.module_granularity is not None:
            task_config["module_granularity"] = body.module_granularity
        if body.filter_engine is not None:
            task_config["filter_engine"] = body.filter_engine
        if body.enable_final_check is not None:
            task_config["enable_final_check"] = bool(body.enable_final_check)
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
        analysis_mode=analysis_mode,
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
    per_page: int = Query(100, ge=1, le=1000),
    status: Optional[str] = Query(None),
    analysis_mode: Optional[str] = Query(None),
    sort_by: str = Query("created_at"),
    sort_order: str = Query("desc"),
    db: Session = Depends(get_db),
):
    return get_task_service().list_tasks(
        db,
        project_id=project_id,
        page=page,
        per_page=per_page,
        status=status,
        analysis_mode=analysis_mode,
        sort_by=sort_by,
        sort_order=sort_order,
    )


@router.get("/tasks/{task_id}")
async def get_task(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task(db, task_id)


@router.put("/tasks/{task_id}/origin")
async def repair_task_origin(task_id: str, body: TaskOriginRepairRequest, db: Session = Depends(get_db)):
    return get_task_service().repair_task_origin(db, task_id, body.analysis_mode)


@router.get("/tasks/{task_id}/result", response_model=TaskResultResponse)
async def get_task_result(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task_result(db, task_id)


@router.get("/tasks/{task_id}/sessions", response_model=list[TaskSessionMetaResponse])
async def list_task_sessions(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().list_task_sessions(db, task_id)


@router.get("/tasks/{task_id}/sessions/index", response_model=TaskSessionIndexResponse)
async def get_task_session_index(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task_session_index(db, task_id)


@router.get("/tasks/{task_id}/sessions/file", response_model=TaskSessionFileResponse)
async def get_task_session_file(task_id: str, path: str = Query(...), db: Session = Depends(get_db)):
    return get_task_service().get_task_session_file(db, task_id, path)


@router.get("/tasks/{task_id}/evaluation", response_model=TaskEvaluationResponse)
async def get_task_evaluation(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task_evaluation(db, task_id)


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


@router.get("/tasks/{task_id}/reflection")
async def get_task_reflection(task_id: str, db: Session = Depends(get_db)):
    """返回任务的自省分析报告列表和最新报告内容。"""
    from app.db.models import AppSaTask
    from fastapi import HTTPException
    row = db.query(AppSaTask).filter(
        AppSaTask.task_id == task_id,
        AppSaTask.is_deleted.is_(False),
    ).first()
    if not row:
        raise HTTPException(404, f"任务不存在: {task_id}")

    # 获取 self_reflection.output_dir 配置
    from app.db import get_db as _get_db
    from app.service.config_service import get_config_service
    reflection_dir = "/data/self-reflection"  # default
    try:
        cfg_data = get_config_service().get_config(db, row.project_id)
        reflection_dir = (
            cfg_data.get("self_reflection", {}).get("output_dir", "/data/self-reflection")
            or "/data/self-reflection"
        )
    except Exception:
        pass

    from pathlib import Path
    import os
    out_dir = Path(reflection_dir)
    reports: list[dict] = []
    latest_content = ""
    if out_dir.is_dir():
        for p in sorted(out_dir.glob(f"{task_id}_*.md"), reverse=True):
            stat = p.stat()
            reports.append({
                "filename": p.name,
                "created_at": __import__('datetime').datetime.fromtimestamp(
                    stat.st_mtime
                ).isoformat(),
                "size_bytes": stat.st_size,
            })
        if reports:
            try:
                latest_content = (out_dir / reports[0]["filename"]).read_text(
                    "utf-8", errors="replace"
                )
            except Exception:
                pass
    return {
        "task_id": task_id,
        "reflection_dir": str(out_dir),
        "reports": reports,
        "content": latest_content,
    }


@router.get("/tasks/{task_id}/logs")
async def get_task_logs(task_id: str, db: Session = Depends(get_db)):
    """Return events stream for the task (from events.jsonl file, with DB fallback)."""
    from app.db.models import AppSaTask
    from app.service.event_log import read_events, events_path
    row = db.query(AppSaTask).filter(
        AppSaTask.task_id == task_id,
        AppSaTask.is_deleted.is_(False),
    ).first()
    if not row:
        from fastapi import HTTPException
        raise HTTPException(404, f"任务不存在: {task_id}")
    data = read_events(
        events_path(row.output_path, task_id),
        row.stages_json,
    )
    return {
        "task_id": task_id,
        "status": row.status,
        "stages_json": data,
    }


@router.post("/generate-prompt")
async def generate_prompt(body: GeneratePromptRequest):
    """Auto-generate a prompt from an input path."""
    return {"prompt": generate_prompt_from_path(body.input_path)}


@router.get("/tasks/{task_id}/checkpoint")
async def get_task_checkpoint(task_id: str, db: Session = Depends(get_db)):
    """返回任务的断点续跑状态摘要。

    用于前端展示各阶段/模块的完成情况。
    """
    import os as _os
    from pathlib import Path as _Path
    from app.db.models import AppSaTask
    from app.pipeline.checkpoint import CheckpointManager

    row = db.query(AppSaTask).filter(
        AppSaTask.task_id == task_id,
        AppSaTask.is_deleted.is_(False),
    ).first()
    if not row:
        from fastapi import HTTPException
        raise HTTPException(404, f"任务不存在: {task_id}")

    if not row.output_path:
        return {"task_id": task_id, "available": False, "reason": "no_output_path"}

    workspace = _Path(row.output_path) / task_id / "run" / "workspace"
    checkpoint_dir = workspace / ".checkpoint"

    if not checkpoint_dir.exists():
        return {
            "task_id": task_id,
            "available": False,
            "reason": "no_checkpoint_dir",
            "workspace": str(workspace),
        }

    cp = CheckpointManager(workspace)
    summary = cp.load_summary()

    return {
        "task_id": task_id,
        "available": True,
        "workspace": str(workspace),
        "checkpoint_dir": str(checkpoint_dir),
        **summary,
    }

