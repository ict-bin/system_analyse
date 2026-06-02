"""Task management API routes."""

from __future__ import annotations

import logging
import os
import time
import asyncio
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, Query
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
internal_observability_router = APIRouter(prefix="/api/app/system-analyse")

AGGREGATE_HTTP_PORT = int(os.environ.get("SA_AGENT_AGGREGATE_PORT", os.environ.get("PORT", "3000")))
AGGREGATE_HTTP_TIMEOUT_SECONDS = max(2.0, float(os.environ.get("SA_AGENT_AGGREGATE_TIMEOUT_SECONDS", "60")))
AGGREGATE_CACHE_TTL_SECONDS = max(2.0, float(os.environ.get("SA_AGENT_AGGREGATE_CACHE_TTL_SECONDS", "5")))
_AGENT_AGGREGATE_CACHE: dict[str, dict[str, Any]] = {}
_AGENT_AGGREGATE_SUMMARY_CACHE: dict[str, dict[str, Any]] = {}
_LAST_AGENT_AGGREGATE_META: dict[str, Any] = {
    "partial": False,
    "sources": 0,
    "fanout_errors": 0,
    "duration_seconds": 0.0,
    "cache_hit": False,
    "cache_age_seconds": 0.0,
    "failed_targets": [],
    "failed_target_details": [],
    "cache_hits": 0,
    "cache_misses": 0,
}
AGGREGATE_CONCURRENCY = max(1, int(os.environ.get("SA_AGENT_AGGREGATE_CONCURRENCY", "8")))


def _summary_with_meta(summary: dict[str, Any], *, cache_hit: bool, cache_age_seconds: float = 0.0) -> dict[str, Any]:
    row = dict(summary or {})
    row["aggregate_cache_hit"] = cache_hit
    row["aggregate_cache_age_seconds"] = cache_age_seconds
    return row


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


class TaskListStatsResponse(BaseModel):
    total: int = 0
    pending: int = 0
    running: int = 0
    passed: int = 0
    failed: int = 0
    error: int = 0
    cancelled: int = 0


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
    pod_name: str | None = None
    pod_ip: str | None = None
    http_port: int | None = None
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
    exe: Optional[str] = None
    rss_bytes: Optional[int] = None
    runtime_kind: Optional[str] = None
    match_source: Optional[str] = None
    match_confidence: Optional[str] = None
    workspace_root: Optional[str] = None
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


class AgentTaskOwnershipSnapshotResponse(BaseModel):
    task_id: str
    task_name: str
    task_status: str
    stage_key: Optional[str] = None
    pod_name: str
    process_count: int = 0
    agent_roles: list[str] = Field(default_factory=list)
    process_pids: list[int] = Field(default_factory=list)
    ownership_status: str


class AgentPodSnapshotResponse(BaseModel):
    pod_name: str
    worker_id: Optional[str] = None
    healthy: bool = True
    process_count: int = 0
    tracked_process_count: int = 0
    residual_process_count: int = 0
    unknown_process_count: int = 0
    task_count: int = 0
    running_task_count: int = 0
    residual_task_count: int = 0
    last_scanned_at: Optional[float] = None
    scan_errors: int = 0
    processes: list[AgentProcessSnapshotResponse] = Field(default_factory=list)
    tasks: list[AgentTaskOwnershipSnapshotResponse] = Field(default_factory=list)


class AgentObservabilitySummaryResponse(BaseModel):
    pod_name: str
    active_processes: int = 0
    residual_processes: int = 0
    unknown_processes: int = 0
    killable_residual_processes: int = 0
    killable_unknown_processes: int = 0
    scanned_at: Optional[float] = None
    scan_errors: int = 0
    aggregate_mode: Optional[str] = None
    aggregate_partial: Optional[bool] = None
    aggregate_sources: Optional[int] = None
    aggregate_fanout_errors: Optional[int] = None
    aggregate_duration_seconds: Optional[float] = None
    aggregate_cache_hit: Optional[bool] = None
    aggregate_cache_age_seconds: Optional[float] = None
    aggregate_failed_targets: list[str] = Field(default_factory=list)
    aggregate_failed_target_details: list[dict[str, Any]] = Field(default_factory=list)
    aggregate_all_sources_failed: Optional[bool] = None
    total_pods: Optional[int] = None
    healthy_pods: Optional[int] = None


class AgentRuntimeAggregateSummaryResponse(BaseModel):
    total_pods: int = 0
    healthy_pods: int = 0
    total_processes: int = 0
    tracked_processes: int = 0
    residual_processes: int = 0
    unknown_processes: int = 0
    killable_residual_processes: int = 0
    killable_unknown_processes: int = 0
    aggregate_partial: bool = False
    aggregate_sources: int = 0
    aggregate_fanout_errors: int = 0
    aggregate_failed_targets: list[str] = Field(default_factory=list)
    aggregate_failed_target_details: list[dict[str, Any]] = Field(default_factory=list)
    aggregate_all_sources_failed: bool = False
    scanned_at: Optional[float] = None


class AgentRuntimeAggregateResponse(BaseModel):
    summary: AgentRuntimeAggregateSummaryResponse
    pods: list[AgentPodSnapshotResponse] = Field(default_factory=list)
    processes: list[AgentProcessSnapshotResponse] = Field(default_factory=list)
    tasks: list[AgentTaskOwnershipSnapshotResponse] = Field(default_factory=list)


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


def _auth_headers_from_token(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _snapshot_query_params() -> dict[str, Any]:
    return {}


def _resolve_worker_targets(*, pod_ip: str | None, pod_name: str | None) -> list[str]:
    targets: list[str] = []
    normalized_ip = str(pod_ip or "").strip()
    if normalized_ip:
        targets.append(normalized_ip)
    normalized_name = str(pod_name or "").strip()
    if normalized_name and normalized_name not in targets:
        targets.append(normalized_name)
    return targets


def _resolve_worker_http_port(worker: Any) -> int:
    try:
        return max(1, int(getattr(worker, "http_port", 0) or 8080))
    except Exception:
        return 8080


def _aggregate_base_urls(worker: Any) -> list[str]:
    targets: list[str] = []
    pod_ip = str(getattr(worker, "pod_ip", "") or "").strip()
    pod_name = str(getattr(worker, "pod_name", "") or "").strip()
    http_port = _resolve_worker_http_port(worker)
    for host in _resolve_worker_targets(pod_ip=pod_ip, pod_name=pod_name):
        if not host:
            continue
        targets.append(f"http://{host}:{http_port}/api/app/system-analyse")
    return targets


def _agent_cache_key() -> str:
    return "cluster"


def _invalidate_agent_aggregate_cache() -> None:
    _AGENT_AGGREGATE_CACHE.clear()
    _AGENT_AGGREGATE_SUMMARY_CACHE.clear()


async def _fanout_get_json(urls: list[str], *, path: str, token: str, params: dict[str, Any]) -> tuple[Any | None, str | None, dict[str, Any] | None]:
    headers = _auth_headers_from_token(token)
    async with httpx.AsyncClient(timeout=AGGREGATE_HTTP_TIMEOUT_SECONDS) as client:
        for base_url in urls:
            url = f"{base_url}{path}"
            try:
                response = await client.get(url, headers=headers, params=params)
                if response.status_code == 200:
                    return response.json(), base_url, None
                logger.warning("system-agent-fanout http_error url=%s status=%s body=%s", url, response.status_code, response.text[:200])
                return None, None, {"attempted_url": url, "error_kind": "http_error", "status_code": response.status_code, "message": response.text[:200]}
            except httpx.ConnectTimeout:
                logger.warning("system-agent-fanout connect_timeout url=%s", url)
                return None, None, {"attempted_url": url, "error_kind": "connect_timeout", "status_code": None, "message": "connect timeout"}
            except httpx.ConnectError:
                logger.warning("system-agent-fanout connection_refused url=%s", url)
                return None, None, {"attempted_url": url, "error_kind": "connection_refused", "status_code": None, "message": "connection refused"}
            except Exception as exc:
                logger.exception("system-agent-fanout transport_error url=%s", url)
                return None, None, {"attempted_url": url, "error_kind": "transport_error", "status_code": None, "message": str(exc)}
    return None, None, {"attempted_url": None, "error_kind": "no_target", "status_code": None, "message": "no target responded"}


async def _fanout_post_json(urls: list[str], *, path: str, token: str, params: dict[str, Any]) -> tuple[Any | None, str | None, dict[str, Any] | None]:
    headers = _auth_headers_from_token(token)
    async with httpx.AsyncClient(timeout=AGGREGATE_HTTP_TIMEOUT_SECONDS) as client:
        for base_url in urls:
            url = f"{base_url}{path}"
            try:
                response = await client.post(url, headers=headers, params=params)
                if response.status_code == 200:
                    return response.json(), base_url, None
                logger.warning("system-agent-fanout post_http_error url=%s status=%s body=%s", url, response.status_code, response.text[:200])
                return None, None, {"attempted_url": url, "error_kind": "http_error", "status_code": response.status_code, "message": response.text[:200]}
            except httpx.ConnectTimeout:
                logger.warning("system-agent-fanout post_connect_timeout url=%s", url)
                return None, None, {"attempted_url": url, "error_kind": "connect_timeout", "status_code": None, "message": "connect timeout"}
            except httpx.ConnectError:
                logger.warning("system-agent-fanout post_connection_refused url=%s", url)
                return None, None, {"attempted_url": url, "error_kind": "connection_refused", "status_code": None, "message": "connection refused"}
            except Exception as exc:
                logger.exception("system-agent-fanout post_transport_error url=%s", url)
                return None, None, {"attempted_url": url, "error_kind": "transport_error", "status_code": None, "message": str(exc)}
    return None, None, {"attempted_url": None, "error_kind": "no_target", "status_code": None, "message": "no target responded"}


def _failed_target_label(worker: Any) -> str:
    return str(getattr(worker, "pod_name", "") or getattr(worker, "worker_id", "") or "unknown")


def _failed_target_detail(worker: Any, urls: list[str], error_detail: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "pod_name": getattr(worker, "pod_name", None),
        "pod_ip": getattr(worker, "pod_ip", None),
        "http_port": _resolve_worker_http_port(worker),
        "attempted_urls": urls,
        "error_kind": (error_detail or {}).get("error_kind"),
        "status_code": (error_detail or {}).get("status_code"),
        "message": (error_detail or {}).get("message"),
        "attempted_url": (error_detail or {}).get("attempted_url"),
    }


async def _get_agent_observability_snapshot_impl(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    from app.service.agent_observability import get_agent_observability_service

    return get_agent_observability_service().build_snapshot(db, project_id=None)


@router.get("/agent-observability/snapshot")
async def get_agent_observability_snapshot(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    return await _get_agent_observability_snapshot_impl(db=db, user_and_token=user_and_token)


@internal_observability_router.get("/agent-observability/snapshot", response_model=dict[str, Any], include_in_schema=False)
async def get_internal_agent_observability_snapshot(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    return await _get_agent_observability_snapshot_impl(db=db, user_and_token=user_and_token)


async def _build_agent_aggregate_snapshot(token: str, db: Session) -> dict[str, Any]:
    from app.service.agent_observability import get_agent_observability_service
    from app.service.worker_slot_snapshot import build_worker_slot_cluster_snapshot

    now_ts = time.time()
    cache_key = _agent_cache_key()
    cached = _AGENT_AGGREGATE_CACHE.get(cache_key)
    if cached and (now_ts - float(cached.get("created_at") or 0.0)) <= AGGREGATE_CACHE_TTL_SECONDS:
        cache_age = now_ts - float(cached.get("created_at") or 0.0)
        meta = cached.get("meta") or {}
        _LAST_AGENT_AGGREGATE_META.update({
            "partial": bool(meta.get("partial")),
            "sources": int(meta.get("sources") or 0),
            "fanout_errors": int(meta.get("fanout_errors") or 0),
            "duration_seconds": float(meta.get("duration_seconds") or 0.0),
            "cache_hit": True,
            "cache_age_seconds": cache_age,
            "failed_targets": list(meta.get("failed_targets") or []),
            "cache_hits": int(_LAST_AGENT_AGGREGATE_META.get("cache_hits") or 0) + 1,
        })
        return cached["snapshot"]

    started = time.perf_counter()
    local = get_agent_observability_service().build_snapshot(db, project_id=None)
    cluster_snapshot = build_worker_slot_cluster_snapshot(db, project_id=None)
    workers = [worker for worker in cluster_snapshot.workers if worker.healthy and (_resolve_worker_targets(pod_ip=worker.pod_ip, pod_name=worker.pod_name))]
    total_target_pods = len(workers)
    total_healthy_pods = sum(1 for worker in workers if worker.healthy)

    merged_processes: list[dict[str, Any]] = []
    merged_tasks: list[dict[str, Any]] = []
    pod_rows: list[dict[str, Any]] = []
    sources = 0
    partial = False
    fanout_errors = 0
    failed_targets: list[str] = []
    failed_target_details: list[dict[str, Any]] = []
    seen_process_keys: set[tuple[str, int]] = set()
    seen_task_keys: set[tuple[str, str]] = set()
    seen_pod_keys: set[str] = set()

    work_items: list[tuple[Any, list[str]]] = []
    for worker in workers:
        urls = _aggregate_base_urls(worker)
        if not urls:
            partial = True
            fanout_errors += 1
            failed_targets.append(_failed_target_label(worker))
            failed_target_details.append(_failed_target_detail(worker, urls, {"error_kind": "missing_target", "status_code": None, "message": "worker has no reachable aggregate targets", "attempted_url": None}))
            continue
        work_items.append((worker, urls))

    semaphore = asyncio.Semaphore(AGGREGATE_CONCURRENCY)

    async def _fetch_worker_snapshot(worker: Any, urls: list[str]) -> tuple[Any, list[str], Any | None, dict[str, Any] | None]:
        async with semaphore:
            snapshot, _, error_detail = await _fanout_get_json(urls, path="/agent-observability/snapshot", token=token, params=_snapshot_query_params())
            return worker, urls, snapshot, error_detail

    snapshot_results = await asyncio.gather(*[_fetch_worker_snapshot(worker, urls) for worker, urls in work_items]) if work_items else []
    for worker, urls, snapshot, error_detail in snapshot_results:
        if snapshot is None:
            partial = True
            fanout_errors += 1
            failed_targets.append(_failed_target_label(worker))
            failed_target_details.append(_failed_target_detail(worker, urls, error_detail))
            continue
        sources += 1
        for item in snapshot.get("processes") or []:
            key = (str(item.get("pod_name") or ""), int(item.get("pid") or 0))
            if key in seen_process_keys:
                continue
            seen_process_keys.add(key)
            merged_processes.append(item)
        for item in snapshot.get("tasks") or []:
            key = (str(item.get("pod_name") or ""), str(item.get("task_id") or ""))
            if key in seen_task_keys:
                continue
            seen_task_keys.add(key)
            merged_tasks.append(item)
        for item in snapshot.get("pods") or []:
            pod_name = str(item.get("pod_name") or "")
            if pod_name in seen_pod_keys:
                pod_rows = [row for row in pod_rows if str(row.get("pod_name") or "") != pod_name]
            pod_rows.append(item)
            seen_pod_keys.add(pod_name)

    all_sources_failed = bool(workers) and sources == 0 and fanout_errors > 0
    if not workers:
        merged_processes = list(local.get("processes") or [])
        merged_tasks = list(local.get("tasks") or [])
        pod_rows = list(local.get("pods") or [])
        sources = 1
        partial = False
        all_sources_failed = False
        total_target_pods = len(pod_rows)
        total_healthy_pods = len([row for row in pod_rows if bool(row.get("healthy", True))])

    summary = {
        "pod_name": "system-analyse-aggregate",
        "active_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "tracked"]),
        "residual_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "residual"]),
        "unknown_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "unknown"]),
        "killable_residual_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "residual" and bool(item.get("kill_allowed"))]),
        "killable_unknown_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "unknown" and bool(item.get("kill_allowed"))]),
        "scanned_at": time.time(),
        "scan_errors": 0,
        "aggregate_mode": "fanout",
        "aggregate_partial": partial,
        "aggregate_sources": sources,
        "aggregate_fanout_errors": fanout_errors,
        "aggregate_duration_seconds": time.perf_counter() - started,
        "aggregate_cache_hit": False,
        "aggregate_cache_age_seconds": 0.0,
        "aggregate_failed_targets": failed_targets,
        "aggregate_failed_target_details": failed_target_details,
        "aggregate_all_sources_failed": all_sources_failed,
        "total_pods": total_target_pods,
        "healthy_pods": total_healthy_pods,
    }
    _LAST_AGENT_AGGREGATE_META.update({
        "partial": partial,
        "sources": sources,
        "fanout_errors": fanout_errors,
        "duration_seconds": summary["aggregate_duration_seconds"],
        "cache_hit": False,
        "cache_age_seconds": 0.0,
        "failed_targets": failed_targets,
        "failed_target_details": failed_target_details,
        "cache_misses": int(_LAST_AGENT_AGGREGATE_META.get("cache_misses") or 0) + 1,
    })
    snapshot = {
        "summary": summary,
        "processes": merged_processes,
        "tasks": merged_tasks,
        "pods": pod_rows,
    }
    _AGENT_AGGREGATE_CACHE[cache_key] = {
        "created_at": now_ts,
        "snapshot": snapshot,
        "meta": dict(_LAST_AGENT_AGGREGATE_META),
    }
    return snapshot


async def _build_agent_aggregate_summary(token: str, db: Session) -> dict[str, Any]:
    now_ts = time.time()
    cache_key = _agent_cache_key()
    cached = _AGENT_AGGREGATE_SUMMARY_CACHE.get(cache_key)
    if cached and (now_ts - float(cached.get("created_at") or 0.0)) <= AGGREGATE_CACHE_TTL_SECONDS:
        cache_age = now_ts - float(cached.get("created_at") or 0.0)
        meta = cached.get("meta") or {}
        _LAST_AGENT_AGGREGATE_META.update({
            "partial": bool(meta.get("partial")),
            "sources": int(meta.get("sources") or 0),
            "fanout_errors": int(meta.get("fanout_errors") or 0),
            "duration_seconds": float(meta.get("duration_seconds") or 0.0),
            "cache_hit": True,
            "cache_age_seconds": cache_age,
            "failed_targets": list(meta.get("failed_targets") or []),
            "failed_target_details": list(meta.get("failed_target_details") or []),
            "cache_hits": int(_LAST_AGENT_AGGREGATE_META.get("cache_hits") or 0) + 1,
        })
        return _summary_with_meta(cached.get("summary") or {}, cache_hit=True, cache_age_seconds=cache_age)

    started = time.perf_counter()
    from app.service.agent_observability import get_agent_observability_service
    from app.service.worker_slot_snapshot import build_worker_slot_cluster_snapshot

    local_summary = dict(get_agent_observability_service().build_snapshot(db, project_id=None)["summary"])
    cluster_snapshot = build_worker_slot_cluster_snapshot(db, project_id=None)
    workers = [worker for worker in cluster_snapshot.workers if worker.healthy and (_resolve_worker_targets(pod_ip=worker.pod_ip, pod_name=worker.pod_name))]

    sources = 0
    partial = False
    fanout_errors = 0
    failed_targets: list[str] = []
    failed_target_details: list[dict[str, Any]] = []
    counters = {
        "active_processes": 0,
        "residual_processes": 0,
        "unknown_processes": 0,
        "killable_residual_processes": 0,
        "killable_unknown_processes": 0,
        "scan_errors": 0,
    }

    work_items: list[tuple[Any, list[str]]] = []
    for worker in workers:
        urls = _aggregate_base_urls(worker)
        if not urls:
            partial = True
            fanout_errors += 1
            failed_targets.append(_failed_target_label(worker))
            failed_target_details.append(_failed_target_detail(worker, urls, {
                "error_kind": "missing_target",
                "status_code": None,
                "message": "worker has no reachable aggregate targets",
                "attempted_url": None,
            }))
            continue
        work_items.append((worker, urls))

    semaphore = asyncio.Semaphore(AGGREGATE_CONCURRENCY)

    async def _fetch_worker_summary(worker: Any, urls: list[str]) -> tuple[Any, list[str], Any | None, dict[str, Any] | None]:
        async with semaphore:
            worker_summary, _, error_detail = await _fanout_get_json(
                urls,
                path="/agent-observability/summary",
                token=token,
                params=_snapshot_query_params(),
            )
            return worker, urls, worker_summary, error_detail

    summary_results = await asyncio.gather(*[_fetch_worker_summary(worker, urls) for worker, urls in work_items]) if work_items else []
    for worker, urls, worker_summary, error_detail in summary_results:
        if worker_summary is None:
            partial = True
            fanout_errors += 1
            failed_targets.append(_failed_target_label(worker))
            failed_target_details.append(_failed_target_detail(worker, urls, error_detail))
            continue
        sources += 1
        for key in counters:
            counters[key] += int(worker_summary.get(key) or 0)

    all_sources_failed = bool(workers) and sources == 0 and fanout_errors > 0
    if not workers:
        summary = {
            **local_summary,
            "aggregate_mode": "local_no_workers",
            "aggregate_partial": False,
            "aggregate_sources": 1,
            "aggregate_fanout_errors": 0,
            "aggregate_duration_seconds": time.perf_counter() - started,
            "aggregate_cache_hit": False,
            "aggregate_cache_age_seconds": 0.0,
            "aggregate_failed_targets": [],
            "aggregate_failed_target_details": [],
            "aggregate_all_sources_failed": False,
        }
    else:
        summary = {
            "pod_name": "system-analyse-aggregate",
            **counters,
            "scanned_at": time.time(),
            "aggregate_mode": "all_sources_failed" if all_sources_failed else "fanout",
            "aggregate_partial": partial,
            "aggregate_sources": sources,
            "aggregate_fanout_errors": fanout_errors,
            "aggregate_duration_seconds": time.perf_counter() - started,
            "aggregate_cache_hit": False,
            "aggregate_cache_age_seconds": 0.0,
            "aggregate_failed_targets": failed_targets,
            "aggregate_failed_target_details": failed_target_details,
            "aggregate_all_sources_failed": all_sources_failed,
        }

    _LAST_AGENT_AGGREGATE_META.update({
        "partial": bool(summary.get("aggregate_partial")),
        "sources": int(summary.get("aggregate_sources") or 0),
        "fanout_errors": int(summary.get("aggregate_fanout_errors") or 0),
        "duration_seconds": float(summary.get("aggregate_duration_seconds") or 0.0),
        "cache_hit": False,
        "cache_age_seconds": 0.0,
        "failed_targets": list(summary.get("aggregate_failed_targets") or []),
        "failed_target_details": list(summary.get("aggregate_failed_target_details") or []),
        "cache_misses": int(_LAST_AGENT_AGGREGATE_META.get("cache_misses") or 0) + 1,
    })
    _AGENT_AGGREGATE_SUMMARY_CACHE[cache_key] = {
        "created_at": now_ts,
        "summary": dict(summary),
        "meta": dict(_LAST_AGENT_AGGREGATE_META),
    }
    return summary


def _build_agent_runtime_aggregate(snapshot: dict[str, Any]) -> dict[str, Any]:
    pods = list(snapshot.get("pods") or [])
    processes = list(snapshot.get("processes") or [])
    tasks = list(snapshot.get("tasks") or [])
    summary = dict(snapshot.get("summary") or {})
    return {
        "summary": {
            "total_pods": int(summary.get("total_pods") or len(pods)),
            "healthy_pods": int(summary.get("healthy_pods") or len([item for item in pods if bool(item.get("healthy", True))])),
            "total_processes": len(processes),
            "tracked_processes": len([item for item in processes if str(item.get("owner_kind") or "") == "tracked"]),
            "residual_processes": len([item for item in processes if str(item.get("owner_kind") or "") == "residual"]),
            "unknown_processes": len([item for item in processes if str(item.get("owner_kind") or "") == "unknown"]),
            "killable_residual_processes": len([item for item in processes if str(item.get("owner_kind") or "") == "residual" and bool(item.get("kill_allowed"))]),
            "killable_unknown_processes": len([item for item in processes if str(item.get("owner_kind") or "") == "unknown" and bool(item.get("kill_allowed"))]),
            "aggregate_partial": bool(summary.get("aggregate_partial")),
            "aggregate_sources": int(summary.get("aggregate_sources") or 0),
            "aggregate_fanout_errors": int(summary.get("aggregate_fanout_errors") or 0),
            "aggregate_failed_targets": list(summary.get("aggregate_failed_targets") or []),
            "aggregate_failed_target_details": list(summary.get("aggregate_failed_target_details") or []),
            "aggregate_all_sources_failed": bool(summary.get("aggregate_all_sources_failed")),
            "scanned_at": summary.get("scanned_at"),
        },
        "pods": pods,
        "processes": processes,
        "tasks": tasks,
    }


@router.post("/tasks", status_code=201)
def create_task(body: TaskCreateRequest, db: Session = Depends(get_db)):
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
def list_tasks(
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


@router.get("/tasks/stats", response_model=TaskListStatsResponse)
def get_task_stats(
    project_id: str = Query(...),
    status: Optional[str] = Query(None),
    analysis_mode: Optional[str] = Query(None),
    parent_task_id: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    return get_task_service().get_task_stats(
        db,
        project_id=project_id,
        status=status,
        analysis_mode=analysis_mode,
        parent_task_id=parent_task_id,
    )


@router.get("/workers/cluster-capacity/summary", response_model=WorkerClusterCapacitySummaryResponse)
def get_worker_cluster_capacity_summary(
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
def get_worker_cluster_capacity(
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
                pod_name=worker.pod_name,
                pod_ip=worker.pod_ip,
                http_port=worker.http_port,
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
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    from app.service.agent_observability import get_agent_observability_service

    return get_agent_observability_service().build_snapshot(db, project_id=None)["summary"]


@internal_observability_router.get("/agent-observability/summary", response_model=AgentObservabilitySummaryResponse, include_in_schema=False)
async def get_internal_agent_observability_summary(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    from app.service.agent_observability import get_agent_observability_service

    return get_agent_observability_service().build_snapshot(db, project_id=None)["summary"]


@router.get("/agent-observability/processes", response_model=list[AgentProcessSnapshotResponse])
async def list_agent_processes(
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
    del user_and_token
    from app.service.agent_observability import get_agent_observability_service

    rows = list(get_agent_observability_service().build_snapshot(db, project_id=None)["processes"])
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
        rows = [row for row in rows if str(row.get("owner_kind") or "") == "residual"]
    return rows


@internal_observability_router.get("/agent-observability/processes", response_model=list[AgentProcessSnapshotResponse], include_in_schema=False)
async def list_internal_agent_processes(
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
    return await list_agent_processes(
        pod=pod,
        task_id=task_id,
        stage_key=stage_key,
        role_kind=role_kind,
        owner_kind=owner_kind,
        kill_allowed=kill_allowed,
        orphan_only=orphan_only,
        db=db,
        user_and_token=user_and_token,
    )


@router.get("/agent-observability/sessions/content")
async def get_agent_session_content(
    task_id: str = Query(...),
    session_file: str = Query(...),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    return get_task_service().get_task_session_file(db, task_id, session_file)


@router.get("/agent-observability/tasks", response_model=list[AgentTaskOwnershipSnapshotResponse])
async def list_agent_tasks(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    from app.service.agent_observability import get_agent_observability_service

    return get_agent_observability_service().build_snapshot(db, project_id=None)["tasks"]


@internal_observability_router.get("/agent-observability/tasks", response_model=list[AgentTaskOwnershipSnapshotResponse], include_in_schema=False)
async def list_internal_agent_tasks(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    return await list_agent_tasks(db=db, user_and_token=user_and_token)


@router.get("/agent-observability/pods", response_model=list[AgentPodSnapshotResponse])
async def list_agent_pods(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    from app.service.agent_observability import get_agent_observability_service

    return get_agent_observability_service().build_snapshot(db, project_id=None)["pods"]


@internal_observability_router.get("/agent-observability/pods", response_model=list[AgentPodSnapshotResponse], include_in_schema=False)
async def list_internal_agent_pods(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    return await list_agent_pods(db=db, user_and_token=user_and_token)


@router.get("/agent-observability/aggregate/summary", response_model=AgentObservabilitySummaryResponse)
async def get_agent_aggregate_observability_summary(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    return await _build_agent_aggregate_summary(token, db)


@router.get("/agent-observability/aggregate/processes", response_model=list[AgentProcessSnapshotResponse])
async def list_agent_aggregate_processes(
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
    rows = list((await _build_agent_aggregate_snapshot(token, db))["processes"])
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
        rows = [row for row in rows if str(row.get("owner_kind") or "") == "residual"]
    return rows


@router.get("/agent-observability/aggregate/tasks", response_model=list[AgentTaskOwnershipSnapshotResponse])
async def list_agent_aggregate_tasks(
    pod: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    rows = list((await _build_agent_aggregate_snapshot(token, db))["tasks"])
    if pod:
        rows = [row for row in rows if str(row.get("pod_name") or "") == pod]
    return rows


@router.get("/agent-observability/aggregate/pods", response_model=list[AgentPodSnapshotResponse])
async def list_agent_aggregate_pods(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    return (await _build_agent_aggregate_snapshot(token, db))["pods"]


@router.get("/agent-observability/aggregate/runtime", response_model=AgentRuntimeAggregateResponse)
async def get_agent_aggregate_runtime(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    snapshot = await _build_agent_aggregate_snapshot(token, db)
    return _build_agent_runtime_aggregate(snapshot)


@router.post("/agent-observability/processes/{pid}/kill", response_model=AgentProcessKillResponse)
async def kill_agent_process(
    pid: int,
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    user, token = user_and_token
    ensure_admin_user(user)
    from app.service.agent_observability import get_agent_observability_service

    snapshot = get_agent_observability_service().build_snapshot(db, project_id=None)
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
        "system-agent-manual-kill operator=%s project_id=%s pid=%s pgid=%s task_id=%s workspace_root=%s owner_reason=%s",
        user.get("username") or user.get("name") or "unknown",
        row.get("project_id"),
        pid,
        row.get("pgid"),
        row.get("task_id"),
        row.get("workspace_root"),
        row.get("owner_reason"),
    )
    _audit_agent_kill_event(
        db=db,
        project_id=str(row.get("project_id") or ""),
        operator=user.get("username") or user.get("name") or "unknown",
        event_type="agent_process_manual_kill",
        message=f"管理员手工终止残留智能体进程 pid={pid}",
        payload={
            "pid": pid,
            "pgid": row.get("pgid"),
            "pod_name": row.get("pod_name"),
            "workspace_root": row.get("workspace_root"),
            "owner_reason": row.get("owner_reason"),
            "kill_mode": "local",
        },
        task_id=row.get("task_id"),
    )
    result = get_agent_observability_service().kill_process(pid)
    _invalidate_agent_aggregate_cache()
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
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    user, token = user_and_token
    ensure_admin_user(user)
    from app.service.agent_observability import get_agent_observability_service

    snapshot = get_agent_observability_service().build_snapshot(db, project_id=None)
    killable = [row for row in snapshot["processes"] if row.get("owner_kind") == "residual" and row.get("kill_allowed")]
    logger.warning(
        "system-agent-bulk-kill operator=%s project_id=%s count=%s pids=%s",
        user.get("username") or user.get("name") or "unknown",
        None,
        len(killable),
        [row.get("pid") for row in killable],
    )
    for row in killable:
        _audit_agent_kill_event(
            db=db,
            project_id=str(row.get("project_id") or ""),
            operator=user.get("username") or user.get("name") or "unknown",
            event_type="agent_process_bulk_manual_kill",
            message=f"管理员批量终止残留智能体进程 pid={int(row.get('pid') or 0)}",
            payload={
                "pid": int(row.get("pid") or 0),
                "pgid": row.get("pgid"),
                "pod_name": row.get("pod_name"),
                "workspace_root": row.get("workspace_root"),
                "owner_reason": row.get("owner_reason"),
                "kill_mode": "local_bulk",
            },
            task_id=row.get("task_id"),
        )
    items = [get_agent_observability_service().kill_process(int(row["pid"])) for row in killable]
    _invalidate_agent_aggregate_cache()
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


@router.post("/agent-observability/aggregate/processes/kill-all-suspected-orphans", response_model=AgentProcessKillResponse)
async def kill_all_agent_aggregate_suspected_orphans(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    user, token = user_and_token
    ensure_admin_user(user)
    snapshot = await _build_agent_aggregate_snapshot(token, db)
    killable = [row for row in snapshot["processes"] if row.get("owner_kind") == "unknown" and row.get("kill_allowed")]
    cluster_snapshot = build_worker_slot_cluster_detail(db, project_id=None)
    worker_by_pod = {str(worker.pod_name or ""): worker for worker in cluster_snapshot.workers}
    items: list[dict[str, Any]] = []

    logger.warning(
        "system-agent-aggregate-bulk-kill-suspected operator=%s project_id=%s count=%s",
        user.get("username") or user.get("name") or "unknown",
        None,
        len(killable),
    )
    for row in killable:
        _audit_agent_kill_event(
            db=db,
            project_id=str(row.get("project_id") or ""),
            operator=user.get("username") or user.get("name") or "unknown",
            event_type="agent_process_bulk_manual_kill",
            message=f"管理员跨 Pod 批量终止未归属智能体进程 pid={int(row.get('pid') or 0)}",
            payload={
                "pid": int(row.get("pid") or 0),
                "pgid": row.get("pgid"),
                "pod_name": row.get("pod_name"),
                "workspace_root": row.get("workspace_root"),
                "owner_reason": row.get("owner_reason"),
                "owner_kind": row.get("owner_kind"),
                "kill_mode": "aggregate_bulk_suspected",
            },
            task_id=row.get("task_id"),
        )
        target_worker = worker_by_pod.get(str(row.get("pod_name") or ""))
        if target_worker is None:
            items.append({"pid": int(row.get("pid") or 0), "pgid": row.get("pgid"), "status": "failed", "reason": "target pod not found in cluster snapshot"})
            continue
        result, _, error_detail = await _fanout_post_json(
            _aggregate_base_urls(target_worker),
            path=f"/agent-observability/processes/{int(row.get('pid') or 0)}/kill",
            token=token,
            params={},
        )
        if not result:
            items.append({
                "pid": int(row.get("pid") or 0),
                "pgid": row.get("pgid"),
                "status": "failed",
                "reason": (error_detail or {}).get("message") or "fanout kill request failed",
            })
            continue
        for item in result.get("items") or []:
            items.append(item)

    succeeded = sum(1 for item in items if item.get("status") in {"killed", "gone"})
    failed = sum(1 for item in items if item.get("status") == "failed")
    skipped = sum(1 for item in items if item.get("status") == "skipped")
    _invalidate_agent_aggregate_cache()
    return AgentProcessKillResponse(
        requested=len(killable),
        matched=len(killable),
        succeeded=succeeded,
        failed=failed,
        skipped=skipped,
        items=[AgentProcessKillItemResponse(**item) for item in items],
    )


@router.get("/tasks/{task_id}")
def get_task(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task(db, task_id)


@router.get("/tasks/{task_id}/timeline", response_model=TaskTimelineResponse)
def get_task_timeline(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_timeline(db, task_id)


@router.delete("/tasks/{task_id}/timeline", response_model=TaskActionResponse)
def clear_task_timeline(task_id: str, db: Session = Depends(get_db)):
    deleted_event_count = get_task_service().clear_timeline(db, task_id)
    db.commit()
    return TaskActionResponse(status="ok", task_id=task_id, message="任务时间线已清空", deleted_event_count=deleted_event_count)


@router.delete("/tasks/{task_id}/timeline/{event_id}", response_model=TaskActionResponse)
def delete_task_timeline_event(task_id: str, event_id: str, db: Session = Depends(get_db)):
    deleted_event_count = get_task_service().delete_timeline_event(db, task_id, event_id)
    db.commit()
    return TaskActionResponse(status="ok", task_id=task_id, message="事件已删除", deleted_event_count=deleted_event_count)


@router.put("/tasks/{task_id}/origin")
def repair_task_origin(task_id: str, body: TaskOriginRepairRequest, db: Session = Depends(get_db)):
    return get_task_service().repair_task_origin(db, task_id, body.analysis_mode)


@router.get("/tasks/{task_id}/result", response_model=TaskResultResponse)
def get_task_result(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task_result(db, task_id)


@router.get("/tasks/{task_id}/sessions", response_model=list[TaskSessionMetaResponse])
def list_task_sessions(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().list_task_sessions(db, task_id)


@router.get("/tasks/{task_id}/sessions/index", response_model=TaskSessionIndexResponse)
def get_task_session_index(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task_session_index(db, task_id)


@router.get("/tasks/{task_id}/sessions/file", response_model=TaskSessionFileResponse)
def get_task_session_file(task_id: str, path: str = Query(...), db: Session = Depends(get_db)):
    return get_task_service().get_task_session_file(db, task_id, path)


@router.get("/tasks/{task_id}/evaluation", response_model=TaskEvaluationResponse)
def get_task_evaluation(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task_evaluation(db, task_id)


@router.post("/tasks/{task_id}/cancel")
def cancel_task(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().cancel_task(db, task_id)


@router.post("/tasks/{task_id}/restart", status_code=201)
def restart_task(task_id: str, db: Session = Depends(get_db)):
    """Reset and restart an existing task in-place, reusing the same task ID."""
    return get_task_service().restart_task(db, task_id)


@router.post("/tasks/{task_id}/resume", status_code=201)
def resume_task(task_id: str, db: Session = Depends(get_db)):
    """Resume a task from Stage 3 (断点续跑), reusing the same task ID."""
    return get_task_service().resume_task(db, task_id)


@router.get("/tasks/{task_id}/resume-check")
def get_task_resume_check(task_id: str, db: Session = Depends(get_db)):
    """返回任务当前是否适合断点续跑，以及缺失的关键产物。"""
    return get_task_service().get_resume_check(db, task_id)


@router.delete("/tasks/{task_id}", status_code=204)
def delete_task(
    task_id: str,
    delete_files: bool = True,
    db: Session = Depends(get_db),
):
    """删除任务记录（软删除），并可选同步删除输出目录下的任务文件。"""
    get_task_service().delete_task(db, task_id, delete_files=delete_files)


@router.get("/tasks/{task_id}/reflection")
def get_task_reflection(task_id: str, db: Session = Depends(get_db)):
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
def get_task_logs(task_id: str, db: Session = Depends(get_db)):
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
def generate_prompt(body: GeneratePromptRequest):
    """Auto-generate a prompt from an input path."""
    return {"prompt": generate_prompt_from_path(body.input_path)}


@router.get("/tasks/{task_id}/checkpoint")
def get_task_checkpoint(task_id: str, db: Session = Depends(get_db)):
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
