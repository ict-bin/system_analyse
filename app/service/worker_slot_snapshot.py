from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
import time

from sqlalchemy.orm import Session

from app.db.models import AppSaTask
from app.service.runner_registry_service import get_runner_registry_service
from app.service.task_service import get_worker_runtime_settings
from app.time_utils import now_local

_TERMINAL_STATUSES = {"passed", "failed", "error", "cancelled"}


@dataclass(frozen=True)
class SaWorkerActiveJobSnapshot:
    task_id: str
    task_name: str
    status: str
    analysis_mode: str | None
    parent_task_id: str | None
    parent_task_type: str | None
    task_origin_type: str | None
    input_path: str
    started_at: datetime | None
    updated_at: datetime | None
    dispatch_started_at: datetime | None
    execution_owner_id: str | None
    execution_lease_until: datetime | None
    lease_epoch: int
    mapped: bool = True
    mapping_reason: str = "matched_dispatcher_instance_id"


@dataclass(frozen=True)
class SaWorkerSnapshot:
    worker_id: str
    host_name: str
    pod_name: str | None
    pod_ip: str | None
    healthy: bool
    max_concurrent_jobs: int
    running_jobs: int
    available_slots: int
    source: str
    last_heartbeat_at: datetime | None
    active_jobs: list[SaWorkerActiveJobSnapshot] = field(default_factory=list)
    error: str | None = None


@dataclass(frozen=True)
class SaClusterCapacitySnapshot:
    worker_count: int
    healthy_workers: int
    stale_workers: int
    total_capacity: int
    busy_slots: int
    available_slots: int
    queued_jobs: int
    updated_at: datetime | None
    workers: list[SaWorkerSnapshot] = field(default_factory=list)


_SUMMARY_CACHE_TTL_SECONDS = 5.0
_summary_cache: dict[tuple[str | None, str], tuple[float, SaClusterCapacitySnapshot]] = {}


def invalidate_worker_slot_summary_cache(*, project_id: str | None = None) -> None:
    if project_id is None:
        _summary_cache.clear()
        return
    for cache_key in [key for key in _summary_cache if key[0] in {None, project_id}]:
        _summary_cache.pop(cache_key, None)


def _normalize_worker_id(worker_id: str | None) -> str:
    return str(worker_id or "").strip()


def _parse_host_name(worker_id: str) -> str:
    separator = worker_id.find(":")
    return worker_id[:separator] if separator >= 0 else worker_id


def _lease_is_live(lease_expires_at: datetime | None, now: datetime) -> bool:
    return bool(lease_expires_at and lease_expires_at >= now)


def _job_sort_key(job: SaWorkerActiveJobSnapshot) -> tuple[int, float, str]:
    updated_ts = job.updated_at.timestamp() if job.updated_at else 0.0
    return (0 if job.status == "running" else 1, -updated_ts, job.task_id)


def _infer_analysis_mode(row: AppSaTask) -> str | None:
    value = str(getattr(row, "analysis_mode", "") or "").strip().lower()
    if value in {"binary", "source"}:
        return value
    parent_type = str(getattr(row, "parent_task_type", "") or "").strip().lower()
    if parent_type in {"binary", "source"}:
        return parent_type
    return None


def _count_queued_jobs(db: Session, *, project_id: str | None = None) -> int:
    query = db.query(AppSaTask).filter(
        AppSaTask.is_deleted.is_(False),
        AppSaTask.status == "pending",
        (AppSaTask.dispatcher_instance_id.is_(None)) | (AppSaTask.dispatcher_instance_id == ""),
    )
    if project_id:
        query = query.filter(AppSaTask.project_id == project_id)
    return int(query.count() or 0)


def _build_base_worker_snapshot(
    *,
    db: Session,
    project_id: str | None = None,
    include_active_jobs: bool,
) -> SaClusterCapacitySnapshot:
    query = db.query(AppSaTask).filter(
        AppSaTask.is_deleted.is_(False),
        AppSaTask.dispatcher_instance_id.isnot(None),
        AppSaTask.dispatcher_instance_id != "",
        AppSaTask.status.notin_(list(_TERMINAL_STATUSES)),
    )
    if project_id:
        query = query.filter(AppSaTask.project_id == project_id)
    rows = query.all()
    now = now_local()
    queued_jobs = _count_queued_jobs(db, project_id=project_id)

    active_runner_rows = get_runner_registry_service().list_active_runners(db)
    runner_map = {
        _normalize_worker_id(item.get("instance_id")): item
        for item in active_runner_rows
        if _normalize_worker_id(item.get("instance_id"))
    }

    grouped_rows: dict[str, list[AppSaTask]] = defaultdict(list)
    for row in rows:
        worker_id = _normalize_worker_id(getattr(row, "dispatcher_instance_id", None))
        if worker_id:
            grouped_rows[worker_id].append(row)

    all_worker_ids = set(runner_map) | set(grouped_rows)
    default_capacity = max(1, int(get_worker_runtime_settings().get("worker_task_concurrency") or 1))
    worker_snapshots: list[SaWorkerSnapshot] = []

    for worker_id in all_worker_ids:
        owner_rows = grouped_rows.get(worker_id, [])
        runner = runner_map.get(worker_id)
        latest_lease = max(
            (row.lease_expires_at for row in owner_rows if row.lease_expires_at is not None),
            default=None,
        )
        latest_heartbeat = runner.get("updated_at") if runner else None
        lease_live = any(_lease_is_live(row.lease_expires_at, now) for row in owner_rows)
        runner_live = bool(runner)
        healthy = runner_live or lease_live

        if not owner_rows and not runner_live and not lease_live:
            continue

        active_jobs: list[SaWorkerActiveJobSnapshot] = []
        if include_active_jobs:
            active_jobs = [
                SaWorkerActiveJobSnapshot(
                    task_id=row.task_id,
                    task_name=row.task_name,
                    status=str(getattr(row, "status", "") or ""),
                    analysis_mode=_infer_analysis_mode(row),
                    parent_task_id=getattr(row, "parent_task_id", None),
                    parent_task_type=getattr(row, "parent_task_type", None),
                    task_origin_type=getattr(row, "task_origin_type", None),
                    input_path=str(getattr(row, "input_path", "") or ""),
                    started_at=getattr(row, "started_at", None),
                    updated_at=getattr(row, "updated_at", None),
                    dispatch_started_at=getattr(row, "dispatch_started_at", None),
                    execution_owner_id=getattr(row, "dispatcher_instance_id", None),
                    execution_lease_until=getattr(row, "lease_expires_at", None),
                    lease_epoch=int(getattr(row, "lease_epoch", 0) or 0),
                )
                for row in owner_rows
            ]
            active_jobs.sort(key=_job_sort_key)

        occupied_slots = len(owner_rows)
        max_concurrent_jobs = max(1, int((runner or {}).get("capacity") or default_capacity))
        source = "runner_registry" if runner else "task_lease_fallback"
        error: str | None = None
        if not healthy:
            error = "stale lease and stale runner registry"
        elif not runner_live and latest_lease is not None:
            error = "runner registry missing; using task lease fallback"

        worker_snapshots.append(
            SaWorkerSnapshot(
                worker_id=worker_id,
                host_name=_parse_host_name(worker_id),
                pod_name=str((runner or {}).get("pod_name") or "").strip() or None,
                pod_ip=str((runner or {}).get("pod_ip") or "").strip() or None,
                healthy=healthy,
                max_concurrent_jobs=max_concurrent_jobs,
                running_jobs=occupied_slots,
                available_slots=max(0, max_concurrent_jobs - occupied_slots) if healthy else 0,
                source=source,
                last_heartbeat_at=latest_heartbeat,
                active_jobs=active_jobs,
                error=error,
            )
        )

    worker_snapshots.sort(key=lambda item: (0 if item.healthy else 1, -item.running_jobs, item.worker_id))
    return SaClusterCapacitySnapshot(
        worker_count=len(worker_snapshots),
        healthy_workers=sum(1 for worker in worker_snapshots if worker.healthy),
        stale_workers=sum(1 for worker in worker_snapshots if not worker.healthy),
        total_capacity=sum(worker.max_concurrent_jobs for worker in worker_snapshots),
        busy_slots=sum(worker.running_jobs for worker in worker_snapshots),
        available_slots=sum(worker.available_slots for worker in worker_snapshots),
        queued_jobs=queued_jobs,
        updated_at=now,
        workers=worker_snapshots,
    )


def build_worker_slot_cluster_snapshot(db: Session, *, project_id: str | None = None) -> SaClusterCapacitySnapshot:
    return build_worker_slot_cluster_detail(db, project_id=project_id)


def build_worker_slot_cluster_summary(db: Session, *, project_id: str | None = None) -> SaClusterCapacitySnapshot:
    cache_key = (project_id, "summary")
    now_ts = time.monotonic()
    cached = _summary_cache.get(cache_key)
    if cached and now_ts - cached[0] <= _SUMMARY_CACHE_TTL_SECONDS:
        return cached[1]
    snapshot = _build_base_worker_snapshot(db=db, project_id=project_id, include_active_jobs=False)
    _summary_cache[cache_key] = (now_ts, snapshot)
    return snapshot


def build_worker_slot_cluster_detail(db: Session, *, project_id: str | None = None) -> SaClusterCapacitySnapshot:
    return _build_base_worker_snapshot(db=db, project_id=project_id, include_active_jobs=True)
