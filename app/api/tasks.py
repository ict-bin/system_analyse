"""Task management API routes."""

from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db import get_db
from app.service.worker_slot_snapshot import (
    build_worker_slot_cluster_detail,
    build_worker_slot_cluster_summary,
)
from app.time_utils import isoformat_local
from app.service.task_service import generate_prompt_from_path, get_task_service
from app.db.models import AppSaTask
from .deps import ensure_admin_user, ensure_project_access, get_current_user

from . import router

logger = logging.getLogger(__name__)


def _audit_agent_kill_event(
    *,
    db: Session,
    project_id: str | None,
    operator: str,
    event_type: str,
    message: str,
    payload: dict[str, object],
    task_id: str | None = None,
) -> None:
    if not task_id or not project_id:
        return
    row = db.query(AppSaTask).filter(AppSaTask.task_id == task_id, AppSaTask.is_deleted.is_(False)).first()
    if row is None:
        return
    from app.service.task_service import TaskQueryService

    TaskQueryService._record_timeline_event(
        task_id=task_id,
        project_id=project_id,
        event_type=event_type,
        message=message,
        level="warning",
        stage_name="agent_observability",
        payload={
            "operator": operator,
            **payload,
        },
    )


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
    continue_on_module_failure: Optional[bool] = None      # Override service-level module-failure policy
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
    report_generation_type: Optional[str] = None
    report_generation_label: Optional[str] = None
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


class TaskTimelineEventResponse(BaseModel):
    id: str
    task_id: str
    project_id: str
    stage_name: Optional[str] = None
    level: str
    event_type: str
    message: str
    payload: Optional[dict[str, Any]] = None
    payload_json: Optional[dict[str, Any]] = None
    created_at: Optional[str] = None


class TaskTimelineResponse(BaseModel):
    task_id: str
    events: list[TaskTimelineEventResponse] = Field(default_factory=list)


class TaskActionResponse(BaseModel):
    status: str = "ok"
    task_id: str
    message: str
    deleted_event_count: int = 0


class TaskListItemResponse(BaseModel):
    task_id: str
    project_id: str
    analysis_mode: str | None = None
    analysis_mode_label: str | None = None
    task_origin_type: str | None = None
    parent_project_id: str | None = None
    parent_task_id: str | None = None
    parent_task_type: str | None = None
    parent_stage_name: str | None = None
    parent_stage_item_id: str | None = None
    parent_stage_item_key: str | None = None
    origin_label: str | None = None
    parent_task_display: str | None = None
    task_name: str
    status: str
    abnormal_reason: dict[str, Any] | None = None
    abnormal_reason_title: str | None = None
    abnormal_reason_code: str | None = None
    abnormal_reason_category: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    dispatcher_instance_id: str | None = None
    dispatch_started_at: str | None = None
    lease_epoch: int = 0
    lease_expires_at: str | None = None


class TaskListResponse(BaseModel):
    items: list[TaskListItemResponse] = Field(default_factory=list)
    total: int = 0
    page: int = 1
    per_page: int = 100


class WorkerActiveJobResponse(BaseModel):
    task_id: str
    task_name: str
    status: str
    analysis_mode: str | None = None
    parent_task_id: str | None = None
    parent_task_type: str | None = None
    task_origin_type: str | None = None
    input_path: str
    started_at: str | None = None
    updated_at: str | None = None
    dispatch_started_at: str | None = None
    execution_owner_id: str | None = None
    execution_lease_until: str | None = None
    lease_epoch: int = 0
    mapped: bool = True
    mapping_reason: str = "matched_dispatcher_instance_id"


class WorkerCapacityResponse(BaseModel):
    worker_id: str
    host_name: str
    healthy: bool
    max_concurrent_jobs: int
    running_jobs: int = 0
    available_slots: int = 0
    source: str = "runner_registry"
    last_heartbeat_at: str | None = None
    active_jobs: list[WorkerActiveJobResponse] = Field(default_factory=list)
    error: str | None = None


class WorkerClusterCapacityResponse(BaseModel):
    worker_count: int = 0
    healthy_workers: int = 0
    stale_workers: int = 0
    total_capacity: int = 0
    busy_slots: int = 0
    available_slots: int = 0
    queued_jobs: int = 0
    updated_at: str | None = None
    workers: list[WorkerCapacityResponse] = Field(default_factory=list)


class WorkerClusterCapacitySummaryResponse(BaseModel):
    worker_count: int = 0
    healthy_workers: int = 0
    stale_workers: int = 0
    total_capacity: int = 0
    busy_slots: int = 0
    available_slots: int = 0
    queued_jobs: int = 0
    updated_at: str | None = None


class AgentProcessSnapshotResponse(BaseModel):
    pod_name: str
    pid: int
    pgid: Optional[int] = None
    ppid: Optional[int] = None
    command: str
    cwd: Optional[str] = None
    rss_bytes: Optional[int] = None
    session_file: Optional[str] = None
    session_id: Optional[str] = None
    task_id: Optional[str] = None
    task_name: Optional[str] = None
    task_status: Optional[str] = None
    stage_key: Optional[str] = None
    role_kind: Optional[str] = None
    owner_kind: str
    owner_reason: str
    kill_allowed: bool = False
    kill_block_reason: Optional[str] = None
    termination_state: str


class AgentSessionSnapshotResponse(BaseModel):
    pod_name: str
    session_file: str
    session_id: Optional[str] = None
    task_id: Optional[str] = None
    task_name: Optional[str] = None
    stage_key: Optional[str] = None
    role_kind: Optional[str] = None
    display_name: str
    line_count: int = 0
    last_event_at: Optional[str] = None
    live: bool = False
    has_process: bool = False
    process_pid: Optional[int] = None
    orphan_session: bool = False
    parse_warnings: list[str] = Field(default_factory=list)


class AgentTaskOwnershipSnapshotResponse(BaseModel):
    task_id: str
    task_name: str
    task_status: str
    stage_key: Optional[str] = None
    pod_name: str
    process_count: int = 0
    session_count: int = 0
    agent_roles: list[str] = Field(default_factory=list)
    process_pids: list[int] = Field(default_factory=list)
    session_ids: list[str] = Field(default_factory=list)
    ownership_status: str


class AgentPodSnapshotResponse(BaseModel):
    pod_name: str
    process_count: int = 0
    orphan_process_count: int = 0
    session_count: int = 0
    orphan_session_count: int = 0


class AgentObservabilitySummaryResponse(BaseModel):
    pod_name: str
    active_processes: int = 0
    orphan_processes: int = 0
    unknown_processes: int = 0
    killable_orphan_processes: int = 0
    orphan_sessions: int = 0
    scanned_at: Optional[float] = None
    scan_errors: int = 0


class AgentProcessKillItemResponse(BaseModel):
    pid: int
    pgid: Optional[int] = None
    status: str
    reason: Optional[str] = None


class AgentProcessKillResponse(BaseModel):
    requested: int
    matched: int
    succeeded: int
    failed: int
    skipped: int
    items: list[AgentProcessKillItemResponse] = Field(default_factory=list)


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
        body.security_focus_categories, body.module_granularity, body.filter_engine,
        body.enable_final_check, body.continue_on_module_failure,
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
        if body.continue_on_module_failure is not None:
            task_config["continue_on_module_failure"] = bool(body.continue_on_module_failure)
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


@router.get("/tasks", response_model=TaskListResponse)
async def list_tasks(
    project_id: str = Query(...),
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=1000),
    status: Optional[str] = Query(None),
    analysis_mode: Optional[str] = Query(None),
    parent_task_id: Optional[str] = Query(None),
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
        parent_task_id=parent_task_id,
        sort_by=sort_by,
        sort_order=sort_order,
    )


@router.get("/workers/cluster-capacity/summary", response_model=WorkerClusterCapacitySummaryResponse)
async def get_worker_cluster_capacity_summary(
    project_id: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    snapshot = build_worker_slot_cluster_summary(db, project_id=project_id)
    return WorkerClusterCapacitySummaryResponse(
        worker_count=snapshot.worker_count,
        healthy_workers=snapshot.healthy_workers,
        stale_workers=snapshot.stale_workers,
        total_capacity=snapshot.total_capacity,
        busy_slots=snapshot.busy_slots,
        available_slots=snapshot.available_slots,
        queued_jobs=snapshot.queued_jobs,
        updated_at=isoformat_local(snapshot.updated_at),
    )


@router.get("/workers/cluster-capacity", response_model=WorkerClusterCapacityResponse)
async def get_worker_cluster_capacity(
    project_id: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    snapshot = build_worker_slot_cluster_detail(db, project_id=project_id)
    return WorkerClusterCapacityResponse(
        worker_count=snapshot.worker_count,
        healthy_workers=snapshot.healthy_workers,
        stale_workers=snapshot.stale_workers,
        total_capacity=snapshot.total_capacity,
        busy_slots=snapshot.busy_slots,
        available_slots=snapshot.available_slots,
        queued_jobs=snapshot.queued_jobs,
        updated_at=isoformat_local(snapshot.updated_at),
        workers=[
            WorkerCapacityResponse(
                worker_id=worker.worker_id,
                host_name=worker.host_name,
                healthy=worker.healthy,
                max_concurrent_jobs=worker.max_concurrent_jobs,
                running_jobs=worker.running_jobs,
                available_slots=worker.available_slots,
                source=worker.source,
                last_heartbeat_at=isoformat_local(worker.last_heartbeat_at),
                active_jobs=[
                    WorkerActiveJobResponse(
                        task_id=job.task_id,
                        task_name=job.task_name,
                        status=job.status,
                        analysis_mode=job.analysis_mode,
                        parent_task_id=job.parent_task_id,
                        parent_task_type=job.parent_task_type,
                        task_origin_type=job.task_origin_type,
                        input_path=job.input_path,
                        started_at=isoformat_local(job.started_at),
                        updated_at=isoformat_local(job.updated_at),
                        dispatch_started_at=isoformat_local(job.dispatch_started_at),
                        execution_owner_id=job.execution_owner_id,
                        execution_lease_until=isoformat_local(job.execution_lease_until),
                        lease_epoch=job.lease_epoch,
                        mapped=job.mapped,
                        mapping_reason=job.mapping_reason,
                    )
                    for job in worker.active_jobs
                ],
                error=worker.error,
            )
            for worker in snapshot.workers
        ],
    )


@router.get("/agent-observability/summary", response_model=AgentObservabilitySummaryResponse)
async def get_agent_observability_summary(
    project_id: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    if project_id:
        await ensure_project_access(project_id, token)
    from app.service.agent_observability import get_agent_observability_service

    return get_agent_observability_service().build_snapshot(db, project_id=project_id)["summary"]


@router.get("/agent-observability/processes", response_model=list[AgentProcessSnapshotResponse])
async def list_agent_processes(
    project_id: Optional[str] = Query(None),
    pod: Optional[str] = Query(None),
    task_id: Optional[str] = Query(None),
    stage_key: Optional[str] = Query(None),
    role_kind: Optional[str] = Query(None),
    owner_kind: Optional[str] = Query(None),
    kill_allowed: Optional[bool] = Query(None),
    orphan_only: bool = Query(False),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    if project_id:
        await ensure_project_access(project_id, token)
    from app.service.agent_observability import get_agent_observability_service

    rows = list(get_agent_observability_service().build_snapshot(db, project_id=project_id)["processes"])
    if pod:
        rows = [row for row in rows if str(row.get("pod_name") or "") == pod]
    if task_id:
        rows = [row for row in rows if str(row.get("task_id") or "") == task_id]
    if stage_key:
        rows = [row for row in rows if str(row.get("stage_key") or "") == stage_key]
    if role_kind:
        rows = [row for row in rows if str(row.get("role_kind") or "") == role_kind]
    if owner_kind:
        rows = [row for row in rows if str(row.get("owner_kind") or "") == owner_kind]
    if kill_allowed is not None:
        rows = [row for row in rows if bool(row.get("kill_allowed")) is bool(kill_allowed)]
    if orphan_only:
        rows = [row for row in rows if str(row.get("owner_kind") or "") == "orphan"]
    return rows


@router.get("/agent-observability/sessions", response_model=list[AgentSessionSnapshotResponse])
async def list_agent_sessions(
    project_id: Optional[str] = Query(None),
    pod: Optional[str] = Query(None),
    task_id: Optional[str] = Query(None),
    stage_key: Optional[str] = Query(None),
    role_kind: Optional[str] = Query(None),
    live_only: bool = Query(False),
    orphan_only: bool = Query(False),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    if project_id:
        await ensure_project_access(project_id, token)
    from app.service.agent_observability import get_agent_observability_service

    rows = list(get_agent_observability_service().build_snapshot(db, project_id=project_id)["sessions"])
    if pod:
        rows = [row for row in rows if str(row.get("pod_name") or "") == pod]
    if task_id:
        rows = [row for row in rows if str(row.get("task_id") or "") == task_id]
    if stage_key:
        rows = [row for row in rows if str(row.get("stage_key") or "") == stage_key]
    if role_kind:
        rows = [row for row in rows if str(row.get("role_kind") or "") == role_kind]
    if live_only:
        rows = [row for row in rows if bool(row.get("live"))]
    if orphan_only:
        rows = [row for row in rows if bool(row.get("orphan_session"))]
    return rows


@router.get("/agent-observability/sessions/content")
async def get_agent_session_content(
    project_id: Optional[str] = Query(None),
    task_id: str = Query(...),
    session_file: str = Query(...),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    if project_id:
      await ensure_project_access(project_id, token)
    return get_task_service().get_task_session_file(db, task_id, session_file)


@router.get("/agent-observability/tasks", response_model=list[AgentTaskOwnershipSnapshotResponse])
async def list_agent_tasks(
    project_id: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    if project_id:
        await ensure_project_access(project_id, token)
    from app.service.agent_observability import get_agent_observability_service

    return get_agent_observability_service().build_snapshot(db, project_id=project_id)["tasks"]


@router.get("/agent-observability/pods", response_model=list[AgentPodSnapshotResponse])
async def list_agent_pods(
    project_id: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    if project_id:
        await ensure_project_access(project_id, token)
    from app.service.agent_observability import get_agent_observability_service

    return get_agent_observability_service().build_snapshot(db, project_id=project_id)["pods"]


@router.post("/agent-observability/processes/{pid}/kill", response_model=AgentProcessKillResponse)
async def kill_agent_process(
    pid: int,
    project_id: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    user, token = user_and_token
    ensure_admin_user(user)
    if project_id:
        await ensure_project_access(project_id, token)
    from app.service.agent_observability import get_agent_observability_service

    snapshot = get_agent_observability_service().build_snapshot(db, project_id=project_id)
    matched = [row for row in snapshot["processes"] if int(row.get("pid") or -1) == pid]
    if not matched:
        return AgentProcessKillResponse(requested=1, matched=0, succeeded=0, failed=0, skipped=1, items=[])
    row = matched[0]
    if not row.get("kill_allowed"):
        return AgentProcessKillResponse(
            requested=1,
            matched=1,
            succeeded=0,
            failed=0,
            skipped=1,
            items=[AgentProcessKillItemResponse(pid=pid, pgid=row.get("pgid"), status="skipped", reason=row.get("kill_block_reason"))],
        )
    logger.warning(
        "system-agent-manual-kill operator=%s project_id=%s pid=%s pgid=%s task_id=%s session_file=%s owner_reason=%s",
        user.get("username") or user.get("name") or "unknown",
        project_id,
        pid,
        row.get("pgid"),
        row.get("task_id"),
        row.get("session_file"),
        row.get("owner_reason"),
    )
    _audit_agent_kill_event(
        db=db,
        project_id=project_id,
        operator=user.get("username") or user.get("name") or "unknown",
        event_type="agent_process_manual_kill",
        message=f"管理员手工终止孤儿智能体进程 pid={pid}",
        payload={
            "pid": pid,
            "pgid": row.get("pgid"),
            "pod_name": row.get("pod_name"),
            "session_file": row.get("session_file"),
            "owner_reason": row.get("owner_reason"),
            "kill_mode": "local",
        },
        task_id=row.get("task_id"),
    )
    result = get_agent_observability_service().kill_process(pid)
    return AgentProcessKillResponse(
        requested=1,
        matched=1,
        succeeded=1 if result.get("status") in {"killed", "gone"} else 0,
        failed=1 if result.get("status") == "failed" else 0,
        skipped=0,
        items=[AgentProcessKillItemResponse(**result)],
    )


@router.post("/agent-observability/processes/kill-all-orphans", response_model=AgentProcessKillResponse)
async def kill_all_orphan_processes(
    project_id: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    user, token = user_and_token
    ensure_admin_user(user)
    if project_id:
        await ensure_project_access(project_id, token)
    from app.service.agent_observability import get_agent_observability_service

    snapshot = get_agent_observability_service().build_snapshot(db, project_id=project_id)
    killable = [row for row in snapshot["processes"] if row.get("owner_kind") == "orphan" and row.get("kill_allowed")]
    logger.warning(
        "system-agent-bulk-kill operator=%s project_id=%s count=%s pids=%s",
        user.get("username") or user.get("name") or "unknown",
        project_id,
        len(killable),
        [row.get("pid") for row in killable],
    )
    for row in killable:
        _audit_agent_kill_event(
            db=db,
            project_id=project_id,
            operator=user.get("username") or user.get("name") or "unknown",
            event_type="agent_process_bulk_manual_kill",
            message=f"管理员批量终止孤儿智能体进程 pid={int(row.get('pid') or 0)}",
            payload={
                "pid": int(row.get("pid") or 0),
                "pgid": row.get("pgid"),
                "pod_name": row.get("pod_name"),
                "session_file": row.get("session_file"),
                "owner_reason": row.get("owner_reason"),
                "kill_mode": "local_bulk",
            },
            task_id=row.get("task_id"),
        )
    items = [get_agent_observability_service().kill_process(int(row["pid"])) for row in killable]
    succeeded = sum(1 for item in items if item.get("status") in {"killed", "gone"})
    failed = sum(1 for item in items if item.get("status") == "failed")
    return AgentProcessKillResponse(
        requested=len(killable),
        matched=len(killable),
        succeeded=succeeded,
        failed=failed,
        skipped=0,
        items=[AgentProcessKillItemResponse(**item) for item in items],
    )


@router.get("/tasks/{task_id}")
async def get_task(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task(db, task_id)


@router.get("/tasks/{task_id}/timeline", response_model=TaskTimelineResponse)
async def get_task_timeline(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_timeline(db, task_id)


@router.delete("/tasks/{task_id}/timeline", response_model=TaskActionResponse)
async def clear_task_timeline(task_id: str, db: Session = Depends(get_db)):
    deleted_event_count = get_task_service().clear_timeline(db, task_id)
    db.commit()
    return TaskActionResponse(status="ok", task_id=task_id, message="任务时间线已清空", deleted_event_count=deleted_event_count)


@router.delete("/tasks/{task_id}/timeline/{event_id}", response_model=TaskActionResponse)
async def delete_task_timeline_event(task_id: str, event_id: str, db: Session = Depends(get_db)):
    deleted_event_count = get_task_service().delete_timeline_event(db, task_id, event_id)
    db.commit()
    return TaskActionResponse(status="ok", task_id=task_id, message="事件已删除", deleted_event_count=deleted_event_count)


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


@router.get("/tasks/{task_id}/resume-check")
async def get_task_resume_check(task_id: str, db: Session = Depends(get_db)):
    """返回任务当前是否适合断点续跑，以及缺失的关键产物。"""
    return get_task_service().get_resume_check(db, task_id)


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
