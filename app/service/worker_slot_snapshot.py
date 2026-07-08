"""Worker slot snapshot — Celery inspect 模式。

v1 的 runner_registry 已废弃; 改用 Celery inspect (ping/active) 获取
活 worker + 在跑任务, 配合 DB 查 pending 队列。
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
import os
import time

from sqlalchemy.orm import Session

from app.db.models import AppSaTask
from app.time_utils import now_local

_TERMINAL_STATUSES = {"passed", "failed", "error", "cancelled"}
_SUMMARY_CACHE_TTL_SECONDS = 5.0
_summary_cache: dict[tuple[str | None, str], tuple[float, "SaClusterCapacitySnapshot"]] = {}


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
    mapping_reason: str = "celery_active"


@dataclass(frozen=True)
class SaWorkerSnapshot:
    worker_id: str
    host_name: str
    pod_name: str | None
    pod_ip: str | None
    http_port: int | None
    healthy: bool
    max_concurrent_jobs: int
    running_jobs: int
    available_slots: int
    source: str
    last_heartbeat_at: datetime | None
    pod_created_at: str | None = None
    pod_started_at: str | None = None
    pod_metrics_at: str | None = None
    pod_cpu_usage_millicores: int | None = None
    pod_memory_usage_bytes: int | None = None
    pod_cpu_request_millicores: int | None = None
    pod_memory_request_bytes: int | None = None
    pod_cpu_limit_millicores: int | None = None
    pod_memory_limit_bytes: int | None = None
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


def get_worker_runtime_settings() -> dict[str, object]:
    from app.service.task_service import get_worker_runtime_settings as _impl
    return _impl()


def invalidate_worker_slot_summary_cache(*, project_id: str | None = None) -> None:
    if project_id is None:
        _summary_cache.clear()
        return
    for cache_key in [key for key in _summary_cache if key[0] in {None, project_id}]:
        _summary_cache.pop(cache_key, None)


def _normalize_worker_id(worker_id: str | None) -> str:
    return str(worker_id or "").strip()


def _parse_host_name(worker_id: str) -> str:
    separator = worker_id.find("@")
    if separator >= 0:
        return worker_id[separator + 1:]
    return worker_id


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
    """pending + celery_task_id IS NULL = 等待 dispatcher pump 派发"""
    query = db.query(AppSaTask).filter(
        AppSaTask.is_deleted.is_(False),
        AppSaTask.status == "pending",
        AppSaTask.celery_task_id.is_(None),
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
    """用 Celery inspect 构建集群容量快照。"""
    now = now_local()
    queued_jobs = _count_queued_jobs(db, project_id=project_id)

    # 1. Celery inspect: 获取活 worker + 在跑任务
    ping: dict[str, object] = {}
    active: dict[str, list[dict]] = {}
    try:
        from app.celery_app import app as celery_app
        inspect = celery_app.control.inspect(timeout=3)
        ping = inspect.ping() or {}
        active = inspect.active() or {}
    except Exception:
        pass  # Redis 不可达时返回空

    # 2. celery_task_id → DB task 行映射 (running 任务的 active_jobs 详情)
    active_celery_ids: set[str] = set()
    for _worker, tasks in active.items():
        for t in (tasks or []):
            cid = t.get("id") if isinstance(t, dict) else None
            if cid:
                active_celery_ids.add(cid)

    task_map: dict[str, AppSaTask] = {}
    if active_celery_ids:
        rows = db.query(AppSaTask).filter(AppSaTask.celery_task_id.in_(list(active_celery_ids))).all()
        task_map = {str(r.celery_task_id): r for r in rows if r.celery_task_id}

    # 3. 按 worker 构建 snapshot
    default_capacity = max(1, int(os.environ.get("SA_CELERY_CONCURRENCY", "1")))
    worker_snapshots: list[SaWorkerSnapshot] = []

    for worker_name in sorted(set(ping.keys()) | set(active.keys())):
        pod_tasks = active.get(worker_name) or []
        running = len(pod_tasks)

        active_jobs: list[SaWorkerActiveJobSnapshot] = []
        if include_active_jobs:
            for t in pod_tasks:
                cid = t.get("id") if isinstance(t, dict) else None
                row = task_map.get(cid) if cid else None
                if row:
                    active_jobs.append(SaWorkerActiveJobSnapshot(
                        task_id=row.task_id,
                        task_name=row.task_name,
                        status=str(row.status or ""),
                        analysis_mode=_infer_analysis_mode(row),
                        parent_task_id=getattr(row, "parent_task_id", None),
                        parent_task_type=getattr(row, "parent_task_type", None),
                        task_origin_type=getattr(row, "task_origin_type", None),
                        input_path=str(row.input_path or ""),
                        started_at=getattr(row, "started_at", None),
                        updated_at=getattr(row, "updated_at", None),
                        dispatch_started_at=getattr(row, "dispatch_started_at", None),
                        execution_owner_id=getattr(row, "execution_owner_id", None),
                        execution_lease_until=getattr(row, "execution_lease_until", None),
                        lease_epoch=int(getattr(row, "execution_epoch", 0) or 0),
                    ))
            active_jobs.sort(key=_job_sort_key)

        worker_snapshots.append(SaWorkerSnapshot(
            worker_id=worker_name,
            host_name=_parse_host_name(worker_name),
            pod_name=worker_name.split("@")[1] if "@" in worker_name else worker_name,
            pod_ip=None,
            http_port=8080,
            healthy=True,
            max_concurrent_jobs=default_capacity,
            running_jobs=running,
            available_slots=max(0, default_capacity - running),
            source="celery_inspect",
            last_heartbeat_at=now,
            active_jobs=active_jobs,
        ))

    # 4. 也包含 DB running 但 celery inspect 中没出现的 worker (可能 inspect 超时)
    db_running = db.query(AppSaTask).filter(
        AppSaTask.is_deleted.is_(False),
        AppSaTask.status == "running",
        AppSaTask.execution_owner_id.isnot(None),
    )
    if project_id:
        db_running = db_running.filter(AppSaTask.project_id == project_id)
    seen_workers = {w.worker_id for w in worker_snapshots}
    for row in db_running:
        owner = _normalize_worker_id(getattr(row, "execution_owner_id", None))
        if not owner or owner in seen_workers:
            continue
        seen_workers.add(owner)
        worker_snapshots.append(SaWorkerSnapshot(
            worker_id=owner,
            host_name=_parse_host_name(owner),
            pod_name=owner,
            pod_ip=None,
            http_port=8080,
            healthy=True,
            max_concurrent_jobs=default_capacity,
            running_jobs=1,
            available_slots=max(0, default_capacity - 1),
            source="db_running_fallback",
            last_heartbeat_at=getattr(row, "execution_heartbeat_at", None),
            active_jobs=[SaWorkerActiveJobSnapshot(
                task_id=row.task_id, task_name=row.task_name, status="running",
                analysis_mode=_infer_analysis_mode(row),
                parent_task_id=getattr(row, "parent_task_id", None),
                parent_task_type=getattr(row, "parent_task_type", None),
                task_origin_type=getattr(row, "task_origin_type", None),
                input_path=str(row.input_path or ""),
                started_at=getattr(row, "started_at", None),
                updated_at=getattr(row, "updated_at", None),
                dispatch_started_at=getattr(row, "dispatch_started_at", None),
                execution_owner_id=owner,
                execution_lease_until=getattr(row, "execution_lease_until", None),
                lease_epoch=int(getattr(row, "execution_epoch", 0) or 0),
                mapping_reason="db_running_fallback",
            )] if include_active_jobs else [],
        ))

    worker_snapshots.sort(key=lambda item: (-item.running_jobs, item.worker_id))
    return SaClusterCapacitySnapshot(
        worker_count=len(worker_snapshots),
        healthy_workers=sum(1 for w in worker_snapshots if w.healthy),
        stale_workers=sum(1 for w in worker_snapshots if not w.healthy),
        total_capacity=sum(w.max_concurrent_jobs for w in worker_snapshots),
        busy_slots=sum(w.running_jobs for w in worker_snapshots),
        available_slots=sum(w.available_slots for w in worker_snapshots),
        queued_jobs=queued_jobs,
        updated_at=now,
        workers=worker_snapshots,
    )


def build_worker_slot_cluster_snapshot(db: Session, *, project_id: str | None = None) -> SaClusterCapacitySnapshot:
    return build_worker_slot_cluster_detail(db, project_id=project_id)


def build_worker_slot_cluster_detail(db: Session, *, project_id: str | None = None) -> SaClusterCapacitySnapshot:
    return _build_base_worker_snapshot(db=db, project_id=project_id, include_active_jobs=True)


def build_worker_slot_cluster_summary(db: Session, *, project_id: str | None = None) -> SaClusterCapacitySnapshot:
    cache_key = (project_id, "summary")
    now_ts = time.monotonic()
    cached = _summary_cache.get(cache_key)
    if cached and now_ts - cached[0] <= _SUMMARY_CACHE_TTL_SECONDS:
        return cached[1]
    snapshot = _build_base_worker_snapshot(db=db, project_id=project_id, include_active_jobs=False)
    _summary_cache[cache_key] = (now_ts, snapshot)
    return snapshot
