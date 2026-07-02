"""Task management service for secflow-app-system-analyse.

Bridges the FastAPI management layer with the existing Orchestrator engine.
Each task is persisted in MySQL and executed asynchronously.
"""

from __future__ import annotations

import threading
import json
import logging
import os
import re
import socket
import time as _time
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Optional

from sqlalchemy import or_
from sqlalchemy import func
from sqlalchemy.orm import Session, load_only
from sqlalchemy.orm.attributes import flag_modified

from app.copy_utils import safe_copy2
from app.config import load_service_config
from app.db.models import AppSaTask, AppSaTaskEvent
from app.logging_utils import log_event
from app.service.config_service import get_worker_task_concurrency as _get_worker_task_concurrency_from_db
from app.service.task_query_service import TaskQueryService, _agent_runtime_payload
from app.service.task_execution_lock import (
    RUNNER_BOOT_ID,
    RUNNER_MAIN_PID,
    RUNNER_PROCESS_STARTED_AT,
    RUNNER_PROCESS_TOKEN,
    TaskExecutionLockConflict,
    current_runner_lock_identity,
)
from app.service.task_runner import TaskRunner, TaskRunnerDependencies, TaskRunnerSettings
from app.service.task_repository import TaskRepository
from app.service.event_log import append_events, write_final, read_events, events_path as _events_path
from app.service.runtime_control_service import get_runtime_control_service
from app.service.runner_registry_service import (
    RUNNER_STATUS_ACTIVE,
    get_runner_registry_service,
    init_runner_registry_service,
)
from app.service.service_role import is_manager_role, is_runner_role
from app.service.worker_dispatcher import (
    GLOBAL_CLAIM_LOCK_KEY,
    GLOBAL_CLAIM_LOCK_TIMEOUT_SECONDS,
    MAX_RUNNING_TASKS_GLOBAL,
    WORKER_INSTANCE_ID,
    WORKER_IDLE_BACKOFF_MAX_SECONDS,
    WORKER_OVERLOAD_COOLDOWN_SECONDS,
    WORKER_POLL_INTERVAL_SECONDS,
    WORKER_POLL_JITTER_SECONDS,
    WORKER_STALE_SWEEP_INTERVAL_SECONDS,
    WORKER_TASK_CONCURRENCY,
    get_worker_runtime_health as _get_dispatcher_runtime_health,
    lease_deadline as _lease_deadline,
    WorkerDispatcher,
)
from app.service.scheduler import (
    SchedulerService, ExecutorAgent, TaskGuard,
    set_scheduler, INSTANCE_ID as SCHEDULER_INSTANCE_ID,
)
from app.service.scheduler_v3 import get_scheduler as get_v3_scheduler
from app.time_utils import isoformat_local, now_local

logger = logging.getLogger("sa.task_service")

SERVICE_CONFIG_PATH = os.environ.get("SERVICE_CONFIG", "/app/config.json")
TASK_CANCEL_POLL_INTERVAL_SECONDS = float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_CANCEL_POLL_INTERVAL", "2"))
TASK_LEASE_HEARTBEAT_SECONDS = max(5, int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_LEASE_HEARTBEAT_SECONDS", "15")))
TASK_STAGE_FLUSH_BATCH_SIZE = max(5, int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_STAGE_FLUSH_BATCH_SIZE", "20")))
TASK_STAGE_FLUSH_MIN_INTERVAL_SECONDS = max(
    1.0,
    float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_STAGE_FLUSH_MIN_INTERVAL", "10")),
)
RUNNER_ASSIGNMENT_POLL_INTERVAL_SECONDS = max(
    1.0,
    float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_RUNNER_ASSIGNMENT_POLL_INTERVAL_SECONDS", "3")),
)

# Running asyncio tasks keyed by task_id so we can cancel them
_running_tasks: dict[str, object] = {}
_running_task_epochs: dict[str, int] = {}
_running_tasks_guard = threading.Lock()


@dataclass
class RunnerAssignmentRuntimeState:
    last_tick_ts: float = 0.0
    last_success_ts: float = 0.0
    last_error: str | None = None
    last_rows_seen: int = 0
    last_skipped_expired_task_id: str | None = None
    last_skipped_expired_lease_epoch: int | None = None
    last_skipped_expired_lease_expires_at: str | None = None
    last_spawned_task_id: str | None = None
    last_spawned_lease_epoch: int | None = None
    loop_running: bool = False
    thread_alive: bool = False
    loop_start_count: int = 0

    def snapshot(self) -> dict[str, object]:
        now_ts = _time.time()
        max_gap = max(10.0, RUNNER_ASSIGNMENT_POLL_INTERVAL_SECONDS * 4)
        loop_fresh = (self.last_tick_ts > 0.0) and ((now_ts - self.last_tick_ts) <= max_gap)
        return {
            "runner_assignment_loop_last_tick_ts": self.last_tick_ts or None,
            "runner_assignment_loop_last_success_ts": self.last_success_ts or None,
            "runner_assignment_loop_last_error": self.last_error,
            "runner_assignment_loop_fresh": loop_fresh if self.last_tick_ts > 0.0 else self.last_error is None,
            "runner_assignment_loop_last_rows_seen": self.last_rows_seen,
            "runner_assignment_loop_last_skipped_expired_task_id": self.last_skipped_expired_task_id,
            "runner_assignment_loop_last_skipped_expired_lease_epoch": self.last_skipped_expired_lease_epoch,
            "runner_assignment_loop_last_skipped_expired_lease_expires_at": self.last_skipped_expired_lease_expires_at,
            "runner_assignment_loop_last_spawned_task_id": self.last_spawned_task_id,
            "runner_assignment_loop_last_spawned_lease_epoch": self.last_spawned_lease_epoch,
            "runner_assignment_loop_running": self.loop_running,
            "runner_assignment_thread_alive": self.thread_alive,
            "runner_assignment_loop_start_count": self.loop_start_count,
            "running_task_tracking_count": _running_tasks_count(),
            "runner_assignment_poll_interval_seconds": RUNNER_ASSIGNMENT_POLL_INTERVAL_SECONDS,
        }


_runner_assignment_runtime_state = RunnerAssignmentRuntimeState()
_RUNTIME_EVIDENCE_MODE = "cache_only"

ANALYSIS_MODE_BINARY = "binary"
ANALYSIS_MODE_SOURCE = "source"
SOURCE_MODE_DEFAULT_ANALYSE_TARGETS = ["source", "script", "config"]
_TASK_LIST_SORT_COLUMNS = {
    "created_at": AppSaTask.created_at,
    "updated_at": AppSaTask.updated_at,
    "started_at": AppSaTask.started_at,
    "finished_at": AppSaTask.finished_at,
    "status": AppSaTask.status,
    "task_name": AppSaTask.task_name,
}

_SUMMARY_PATTERNS = {
    "module_count": re.compile(r"\|\s*分析模块数\s*\|\s*(\d+)\s*\|"),
    "total_file_count": re.compile(r"\|\s*总文件数\s*\|\s*(\d+)\s*\|"),
    "high_risk_module_count": re.compile(r"\|\s*高风险模块数\s*\|\s*(\d+)\s*\|"),
    "medium_risk_module_count": re.compile(r"\|\s*中风险模块数\s*\|\s*(\d+)\s*\|"),
    "low_risk_module_count": re.compile(r"\|\s*低风险模块数\s*\|\s*(\d+)\s*\|"),
    "threat_count": re.compile(r"\|\s*威胁总数\s*\|\s*(\d+)\s*\|"),
}

_RISK_LEVEL_RE = re.compile(r"<!--\s*RISK_LEVEL:\s*([^\-][^>]*)-->")
_RISK_SCORE_RE = re.compile(r"<!--\s*RISK_SCORE:\s*(\d+)\s*-->")
_MARKDOWN_HEADING_RE = re.compile(r"^(#{1,2})\s+(.+)$", re.MULTILINE)
_SESSION_THINKING_LEVEL_MAP = {"off": "off", "minimal": "minimal", "low": "low", "medium": "medium", "high": "high", "x-high": "xhigh"}
_TIMELINE_PAYLOAD_MAX_STRING_LENGTH = 2000


def _invalidate_slot_summary_cache(project_id: str | None) -> None:
    from app.service.worker_slot_snapshot import invalidate_worker_slot_summary_cache

    invalidate_worker_slot_summary_cache(project_id=project_id)


def _clip_timeline_payload_value(value: object) -> object:
    if isinstance(value, str):
        return value if len(value) <= _TIMELINE_PAYLOAD_MAX_STRING_LENGTH else value[:_TIMELINE_PAYLOAD_MAX_STRING_LENGTH] + "..."
    if isinstance(value, list):
        return [_clip_timeline_payload_value(item) for item in value[:20]]
    if isinstance(value, dict):
        return {str(k): _clip_timeline_payload_value(v) for k, v in value.items()}
    return value


def _sanitize_timeline_payload(payload: dict | None) -> dict | None:
    if not isinstance(payload, dict):
        return None
    sanitized: dict[str, object] = {}
    for key, value in payload.items():
        if value in (None, "", [], {}):
            continue
        sanitized[str(key)] = _clip_timeline_payload_value(value)
    return sanitized or None


def _timeline_runtime_role() -> str:
    role = str(os.environ.get("SECFLOW_SYSTEM_ANALYSE_ROLE") or "").strip().lower()
    if role in {"runner", "worker", "scheduler", "api"}:
        return role
    if role == "manager":
        return "worker"
    return "api"


def _build_timeline_recorder_metadata() -> dict[str, object]:
    hostname = (os.environ.get("HOSTNAME") or socket.gethostname()).strip() or None
    pod_name = (os.environ.get("POD_NAME") or os.environ.get("HOSTNAME") or socket.gethostname()).strip() or None
    pod_ip = (
        str(os.environ.get("POD_IP") or "").strip()
        or str(os.environ.get("SA_POD_IP") or "").strip()
        or None
    )
    instance_id = (
        str(WORKER_INSTANCE_ID or "").strip()
        or str(os.environ.get("WORKER_INSTANCE_ID") or "").strip()
        or pod_name
        or hostname
    )
    return {
        "service": "system-analysis",
        "role": _timeline_runtime_role(),
        "instance_id": instance_id or None,
        "hostname": hostname,
        "pod_name": pod_name,
        "node_name": str(os.environ.get("NODE_NAME") or "").strip() or None,
        "pod_ip": pod_ip,
    }


def _merge_timeline_recorder_payload(payload: dict | None) -> dict[str, object]:
    merged = dict(payload or {})
    recorder = dict(merged.get("recorder") or {}) if isinstance(merged.get("recorder"), dict) else {}
    for key, value in _build_timeline_recorder_metadata().items():
        if value is not None:
            recorder[key] = value
    merged["recorder"] = recorder
    return merged


def _safe_isoformat(value: object) -> str | None:
    if isinstance(value, datetime):
        return isoformat_local(value)
    text = str(value or "").strip()
    return text or None


def _abnormal_evidence(key: str, label: str, value: object) -> dict | None:
    text = str(value or "").strip()
    if not text:
        return None
    return {"key": key, "label": label, "value": text}


def _task_abnormal_reason(row: AppSaTask) -> dict | None:
    status = str(row.status or "")
    if status not in {"failed", "error", "cancelled"}:
        return None
    if isinstance(row.latest_abnormal_reason_json, dict):
        return dict(row.latest_abnormal_reason_json)
    result_json = _load_task_result_json(row) or {}
    stages_payload = read_events(_events_path(row.output_path, row.task_id), row.stages_json) or {}
    events = stages_payload.get("events") if isinstance(stages_payload, dict) else []
    latest_event = next((event for event in reversed(events or []) if isinstance(event, dict) and (event.get("error") or event.get("event") in {"task_error", "stage_failed", "cancelled"})), None)
    message = str(
        row.error
        or result_json.get("error")
        or result_json.get("completion_reason")
        or (latest_event or {}).get("error")
        or (latest_event or {}).get("message")
        or ""
    ).strip()
    if status == "cancelled":
        code = "user_cancelled"
        category = "cancel"
        title = "任务已取消"
    elif "task execution lock already exists" in message.lower():
        if "lock_runner_process_token" in message.lower():
            code = "execution_lock_conflict"
            category = "runtime"
            title = "任务执行锁冲突"
        else:
            code = "execution_lock_conflict"
            category = "runtime"
            title = "任务执行锁冲突"
    elif "lease" in message.lower() or "租约" in message:
        code = "lease_lost"
        category = "runtime"
        title = "任务租约丢失"
    elif "cancel" in message.lower() or "取消" in message:
        code = "runtime_interrupted"
        category = "runtime"
        title = "运行时中断"
    elif "dispatch" in message.lower() or "调度" in message:
        code = "dispatch_failed"
        category = "runtime"
        title = "调度失败"
    elif "dependency" in message.lower() or "timeout" in message.lower() or "503" in message or "502" in message:
        code = "dependency_unavailable"
        category = "runtime"
        title = "依赖不可用"
    else:
        code = "unknown_abnormal" if status == "error" else "orchestration_failed"
        category = "orchestration"
        title = "任务异常结束"
    return {
        "is_abnormal": True,
        "category": category,
        "code": code,
        "title": title,
        "message": message or "任务以非正常状态结束。",
        "terminal": True,
        "source_layer": "task",
        "status": status,
        "service": "system-analysis",
        "stage_name": str((latest_event or {}).get("stage") or (latest_event or {}).get("stage_name") or "").strip() or None,
        "item_key": None,
        "downstream_task_id": None,
        "downstream_service": None,
        "first_seen_at": isoformat_local(row.started_at),
        "last_seen_at": isoformat_local(row.finished_at or row.updated_at),
        "evidence": [
            item for item in [
                _abnormal_evidence("status", "状态", row.status),
                _abnormal_evidence("error", "原始错误", row.error),
                _abnormal_evidence("latest_event", "最近事件", (latest_event or {}).get("event")),
            ] if item is not None
        ],
        "recommended_action": "查看任务结果、事件时间线和运行观测，确认是调度、租约还是模型执行阶段先失败。",
        "related_event_ids": [],
    }


def _lightweight_task_abnormal_reason(row: AppSaTask) -> dict | None:
    if str(row.status or "") not in {"failed", "error", "cancelled"}:
        return None
    if isinstance(row.latest_abnormal_reason_json, dict):
        return dict(row.latest_abnormal_reason_json)
    if str(row.status or "") == "cancelled":
        return {
            "is_abnormal": True,
            "category": "cancel",
            "code": "user_cancelled",
            "title": "任务已取消",
            "message": str(row.error or "任务已取消").strip() or "任务已取消",
            "terminal": True,
            "source_layer": "task",
            "status": str(row.status or ""),
            "service": "system-analysis",
        }
    return None


def _abnormal_reason_event(reason: dict, *, event_id: str | None = None) -> dict:
    timestamp = str(reason.get("last_seen_at") or isoformat_local(now_local()) or "")
    return {
        "ts": _time.time(),
        "timestamp": timestamp,
        "event": "abnormal_reason_recorded",
        "type": "abnormal_reason_recorded",
        "event_id": event_id or f"abn-{uuid.uuid4().hex[:12]}",
        "message": str(reason.get("title") or "任务异常结束"),
        "level": "warning" if str(reason.get("status") or "") == "cancelled" else "error",
        "data": {"reason": dict(reason)},
    }


def _abnormal_reason_history(row: AppSaTask) -> list[dict]:
    stages_payload = read_events(_events_path(row.output_path, row.task_id), row.stages_json) or {}
    events = stages_payload.get("events") if isinstance(stages_payload, dict) else []
    history: list[dict] = []
    for event in reversed(events or []):
        if not isinstance(event, dict):
            continue
        if event.get("event") != "abnormal_reason_recorded":
            continue
        payload = event.get("data") if isinstance(event.get("data"), dict) else {}
        reason = payload.get("reason") if isinstance(payload.get("reason"), dict) else None
        if not isinstance(reason, dict):
            continue
        history.append(
            {
                "event_id": event.get("event_id"),
                "created_at": event.get("timestamp") or event.get("ts"),
                "reason": reason,
            }
        )
        if len(history) >= 10:
            break
    return history


def _sync_task_abnormal_reason(row: AppSaTask) -> tuple[dict | None, bool]:
    reason = _task_abnormal_reason(row)
    next_payload = dict(reason) if isinstance(reason, dict) else None
    changed = row.latest_abnormal_reason_json != next_payload
    if row.latest_abnormal_reason_json != next_payload:
        row.latest_abnormal_reason_json = next_payload
        flag_modified(row, "latest_abnormal_reason_json")
    return next_payload, changed


def _record_abnormal_reason(row: AppSaTask, reason: dict | None, *, changed: bool) -> None:
    if not changed or not isinstance(reason, dict):
        return
    event = _abnormal_reason_event(reason)
    path = _events_path(row.output_path, row.task_id)
    if path is not None:
        append_events(path, [event])
        return
    payload = row.stages_json if isinstance(row.stages_json, dict) else {}
    events = list(payload.get("events") or [])
    events.append(event)
    row.stages_json = {**payload, "events": events, "final": bool(payload.get("final", False))}
    flag_modified(row, "stages_json")

def get_worker_runtime_health() -> dict:
    if is_runner_role() and not is_manager_role():
        return {
            "worker_running_tasks": _running_tasks_count(),
            "runtime_evidence_mode": _RUNTIME_EVIDENCE_MODE,
            **_runner_assignment_runtime_state.snapshot(),
        }
    health = _get_dispatcher_runtime_health(_running_tasks_count())
    if is_runner_role():
        health.update(_runner_assignment_runtime_state.snapshot())
    scheduler = get_v3_scheduler()
    if scheduler is not None:
        scheduler_health = scheduler.health()
        health.update(
            {
                "scheduler_last_tick_at": scheduler_health.get("last_tick"),
                "scheduler_last_success_at": scheduler_health.get("last_success"),
                "scheduler_stall_detected": bool(scheduler_health.get("stall_detected")),
            }
        )
    health["runtime_evidence_mode"] = _RUNTIME_EVIDENCE_MODE
    return health


def get_pending_scheduler_repair_grace_seconds() -> float:
    return max(0.0, float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_PENDING_SUBMIT_GRACE_SECONDS", "20")))


def get_runtime_tracking_snapshot() -> dict[str, int]:
    with _running_tasks_guard:
        return {
            str(task_id): int(epoch)
            for task_id, epoch in list(_running_task_epochs.items())
            if str(task_id).strip()
        }


def get_worker_runtime_settings() -> dict:
    return {
        "worker_instance_id": WORKER_INSTANCE_ID,
        "worker_task_concurrency": 1,
        "worker_poll_interval_seconds": WORKER_POLL_INTERVAL_SECONDS,
        "worker_poll_jitter_seconds": WORKER_POLL_JITTER_SECONDS,
        "worker_idle_backoff_max_seconds": WORKER_IDLE_BACKOFF_MAX_SECONDS,
        "worker_overload_cooldown_seconds": WORKER_OVERLOAD_COOLDOWN_SECONDS,
        "worker_stale_sweep_interval_seconds": WORKER_STALE_SWEEP_INTERVAL_SECONDS,
        "worker_max_running_tasks_global": MAX_RUNNING_TASKS_GLOBAL,
        "worker_global_claim_lock_key": GLOBAL_CLAIM_LOCK_KEY,
        "worker_global_claim_lock_timeout_seconds": GLOBAL_CLAIM_LOCK_TIMEOUT_SECONDS,
    }


def _running_tasks_count() -> int:
    with _running_tasks_guard:
        return len(_running_tasks)


def _task_execution_lock_path(output_path: str | None, task_id: str) -> Path | None:
    if not output_path:
        return None
    return Path(output_path) / task_id / "run" / "task.execution.lock"


def _clear_task_execution_lock(output_path: str | None, task_id: str) -> None:
    lock_path = _task_execution_lock_path(output_path, task_id)
    if not lock_path:
        return
    try:
        lock_path.unlink(missing_ok=True)
    except Exception:
        import traceback
        traceback.print_exc()
        pass


def _read_task_execution_lock_payload(lock_path: Path | None) -> dict[str, object] | None:
    if not lock_path or not lock_path.exists():
        return None
    try:
        payload = json.loads(lock_path.read_text("utf-8"))
    except Exception:
        import traceback
        traceback.print_exc()
        return None
    return payload if isinstance(payload, dict) else None


def _coerce_lock_epoch(value: object) -> int | None:
    try:
        epoch = int(value)  # type: ignore[arg-type]
    except Exception:
        import traceback
        traceback.print_exc()
        return None
    return epoch if epoch >= 0 else None


def _coerce_lock_text(value: object) -> str:
    return str(value or "").strip()


def _security_filter_log_payload(config: dict | None, *, resolved: bool = False) -> dict:
    cfg = config or {}
    return {
        "analyse_targets": cfg.get("analyse_targets"),
        "binary_arch": cfg.get("binary_arch"),
        "security_focus_categories": cfg.get("security_focus_categories"),
        "module_granularity": cfg.get("module_granularity"),
        "filter_engine": cfg.get("filter_engine"),
        "resolved": resolved,
    }


def _read_text_if_exists(path: Path) -> tuple[str | None, str | None]:
    if not path.exists() or not path.is_file():
        return None, f"文件不存在: {path.name}"
    try:
        return path.read_text("utf-8"), None
    except Exception as exc:
        return None, f"文件读取失败: {path.name} ({exc})"


def _parse_summary(final_report_markdown: str | None) -> dict:
    summary = {
        "module_count": 0,
        "high_risk_module_count": 0,
        "medium_risk_module_count": 0,
        "low_risk_module_count": 0,
        "total_file_count": 0,
        "threat_count": 0,
    }
    if not final_report_markdown:
        return summary
    for key, pattern in _SUMMARY_PATTERNS.items():
        match = pattern.search(final_report_markdown)
        if match:
            summary[key] = int(match.group(1))
    return summary


def _parse_report_sections(markdown: str | None) -> list[dict]:
    if not markdown:
        return []
    sections: list[dict] = []
    for idx, match in enumerate(_MARKDOWN_HEADING_RE.finditer(markdown)):
        sections.append({
            "level": len(match.group(1)),
            "title": match.group(2).strip(),
            "anchor": f"section-{idx + 1}",
        })
    return sections


def _infer_risk_level(markdown: str | None) -> str | None:
    if not markdown:
        return None
    level_match = _RISK_LEVEL_RE.search(markdown)
    if level_match:
        return level_match.group(1).strip()
    if "🔴高" in markdown or "风险等级 | 🔴高" in markdown:
        return "高"
    if "🟡中" in markdown or "风险等级 | 🟡中" in markdown:
        return "中"
    if "🟢低" in markdown or "风险等级 | 🟢低" in markdown:
        return "低"
    return None


def _infer_risk_score(markdown: str | None) -> int | None:
    if not markdown:
        return None
    score_match = _RISK_SCORE_RE.search(markdown)
    if score_match:
        return int(score_match.group(1))
    return None


def _task_root(row: AppSaTask) -> Path | None:
    if not row.output_path:
        return None
    return Path(row.output_path) / row.task_id


def _effective_run_root(row: AppSaTask) -> Path | None:
    """返回可读的 run 根。执行中 run/ 是指向 owner 本地的软链（API pod 跨 pod 悬空），
    此时优先用同步到 NFS 的 run_live/；finalize 后 run/ 是真实目录则用 run/。"""
    root = _task_root(row)
    if not root:
        return None
    run = root / "run"
    live = root / "run_live"
    try:
        # run 是真实可读目录（finalize 后拷回）→ 最完整
        if run.exists() and run.is_dir() and not run.is_symlink():
            return run
    except OSError:
        pass
    try:
        if live.is_dir():
            return live  # 执行中：同步副本（前端可见）
    except OSError:
        pass
    return run


def _task_sessions_root(row: AppSaTask) -> Path | None:
    run_root = _effective_run_root(row)
    return run_root / "sessions" if run_root else None


def _task_run_root(row: AppSaTask) -> Path | None:
    return _effective_run_root(row)


def _task_result_path(row: AppSaTask) -> Path | None:
    run_root = _task_run_root(row)
    return run_root / "result.json" if run_root else None


def _task_workspace_root(row: AppSaTask) -> Path | None:
    run_root = _task_run_root(row)
    return run_root / "workspace" if run_root else None


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        last_exc: OSError | None = None
        for attempt in range(20):
            try:
                tmp.replace(path)
                return
            except FileNotFoundError as exc:
                last_exc = exc
                path.parent.mkdir(parents=True, exist_ok=True)
            except PermissionError as exc:
                last_exc = exc
                _time.sleep(0.05 * (attempt + 1))
        if last_exc is not None:
            raise last_exc
        tmp.replace(path)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def _remove_task_root_for_restart(output_path: str | None, task_id: str) -> dict[str, object]:
    """Restart 前彻底删除整个任务目录（文件全清空，干净重跑）。

    事件时间线在 DB(secflow_app_sa_task_event) 中持久保留，与本文件无关，
    删除 events.jsonl 不丢历史。不再保留顶层 events.jsonl（旧“续接”设计已废弃：
    它导致重启后事件累积、阶段进度显示上一轮残留，误导诊断）。
    """
    result: dict[str, object] = {"task_root": None, "renamed_to": None, "removed": False, "existed": False}
    if not output_path:
        return result
    task_root = Path(output_path) / task_id
    result["task_root"] = str(task_root)
    if not task_root.exists():
        return result
    result["existed"] = True
    # 先重命名到墓碑（NFS 上 rmtree 慢/竞态时，canonical 路径已立刻空），再异步删墓碑
    tombstone = task_root.with_name(f".{task_root.name}.restart-delete-{uuid.uuid4().hex}")
    try:
        task_root.rename(tombstone)
        result["renamed_to"] = str(tombstone)
    except FileNotFoundError:
        return result
    except OSError:
        # rename 失败（跨文件系统等）→ 直接 rmtree 原地
        import shutil as _shutil
        _shutil.rmtree(task_root, ignore_errors=True)
        result["removed"] = True
        return result
    # 删墓碑（best-effort，失败也无所谓，下次 restart 再清）
    import shutil as _shutil
    try:
        _shutil.rmtree(tombstone)
        result["removed"] = True
    except OSError as exc:
        logger.warning("restart cleanup tombstone failed path=%s: %s", tombstone, exc)
    return result



def _load_task_result_json(row: AppSaTask) -> dict | None:
    path = _task_result_path(row)
    if path and path.is_file():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                return loaded
        except Exception as exc:
            logger.warning("failed to load task result file %s: %s", path, exc)
    return row.result_json if isinstance(row.result_json, dict) else None


def _write_task_result_json(row: AppSaTask, payload: dict) -> str | None:
    path = _task_result_path(row)
    if not path:
        return None
    _write_json_atomic(path, payload)
    return str(path)


def _read_json_file(path: Path | None) -> dict | None:
    if not path or not path.is_file():
        return None
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
        return loaded if isinstance(loaded, dict) else None
    except Exception as exc:
        logger.warning("failed to load json file %s: %s", path, exc)
        return None


def _count_input_files(input_path: str | None) -> int | None:
    if not input_path:
        return None
    try:
        path = Path(input_path)
        if not path.exists():
            return None
        if path.is_file():
            return 1
        return sum(1 for item in path.rglob("*") if item.is_file())
    except Exception as exc:
        logger.warning("failed to count input files for %s: %s", input_path, exc)
        return None


def _count_filtered_files(workspace: Path | None) -> int | None:
    if not workspace:
        return None
    filtered_path = workspace / "filtered_files.txt"
    if filtered_path.is_file():
        try:
            return sum(1 for line in filtered_path.read_text(encoding="utf-8").splitlines() if line.strip())
        except Exception as exc:
            logger.warning("failed to count filtered files from %s: %s", filtered_path, exc)
    catalog = _read_json_file(workspace / "file_catalog.json")
    if isinstance(catalog, dict):
        for key in ("filtered_count", "total"):
            value = catalog.get(key)
            if isinstance(value, int):
                return value
    return None


def _build_preprocess_summary(row: AppSaTask, payload: dict | None = None) -> dict | None:
    workspace = _task_workspace_root(row)
    summary = payload.get("preprocess_summary") if isinstance(payload, dict) and isinstance(payload.get("preprocess_summary"), dict) else {}
    filter_summary = _read_json_file(workspace / "filter_summary.json" if workspace else None) or {}

    total_input = summary.get("total_input_file_count")
    if not isinstance(total_input, int):
        total_input = filter_summary.get("total_input_file_count")
    if not isinstance(total_input, int):
        total_input = _count_input_files(getattr(row, "input_path", None))

    accepted_input = summary.get("accepted_input_file_count")
    if not isinstance(accepted_input, int):
        accepted_input = filter_summary.get("accepted_input_file_count")
    if not isinstance(accepted_input, int):
        accepted_input = _count_filtered_files(workspace)

    selected_engine = summary.get("selected_filter_engine") or filter_summary.get("selected_filter_engine")
    effective_engine = summary.get("effective_filter_engine") or filter_summary.get("effective_filter_engine")
    fallback_reason = summary.get("fallback_reason") or filter_summary.get("fallback_reason")

    if not any(
        value is not None and value != ""
        for value in (total_input, accepted_input, selected_engine, effective_engine, fallback_reason)
    ):
        return None

    return {
        "total_input_file_count": total_input if isinstance(total_input, int) else None,
        "accepted_input_file_count": accepted_input if isinstance(accepted_input, int) else None,
        "selected_filter_engine": selected_engine or None,
        "effective_filter_engine": effective_engine or None,
        "fallback_reason": fallback_reason or None,
    }


def _lightweight_result_json(row: AppSaTask, payload: dict | None, result_file: str | None = None) -> dict | None:
    if not isinstance(payload, dict):
        return None
    preprocess_summary = _build_preprocess_summary(row, payload)
    if payload.get("result_externalized"):
        return {
            **payload,
            "result_file": payload.get("result_file") or result_file or (str(_task_result_path(row)) if _task_result_path(row) else None),
            "result_externalized": True,
            "preprocess_summary": preprocess_summary,
        }
    total_tokens = payload.get("total_tokens") if isinstance(payload.get("total_tokens"), dict) else None
    modules = payload.get("modules") if isinstance(payload.get("modules"), list) else []
    rounds = payload.get("rounds") if isinstance(payload.get("rounds"), list) else []
    return {
        "result_file": result_file or (str(_task_result_path(row)) if _task_result_path(row) else None),
        "result_externalized": True,
        "status": payload.get("status") or row.status,
        "error": payload.get("error"),
        "module_count": len(modules),
        "round_count": len(rounds),
        "total_duration_ms": payload.get("total_duration_ms"),
        "total_tokens": total_tokens,
        "preprocess_summary": preprocess_summary,
    }


_DETAIL_STAGE_STEPS = [
    {"key": "preprocess", "triggers": {"filter", "explore", "prescan"}},
    {"key": "classify", "triggers": {"classify", "1"}},
    {"key": "refine", "triggers": {"2", "2-reclassify", "2-redo", "2-sub"}},
    {"key": "analyse", "triggers": {"3", "3-redo"}},
    {"key": "report", "triggers": {"4", "4a", "4b", "4b-check"}},
]


def _normalize_stage_step_key(raw_stage: object) -> str | None:
    text = str(raw_stage or "").strip()
    if not text:
        return None
    for step in _DETAIL_STAGE_STEPS:
        if text in step["triggers"]:
            return str(step["key"])
    return None


def _build_lightweight_stages_payload(row: AppSaTask) -> dict[str, object]:
    stages_payload = read_events(_events_path(row.output_path, row.task_id), row.stages_json) or {}
    events = stages_payload.get("events") if isinstance(stages_payload, dict) else []
    final = bool(stages_payload.get("final", False)) if isinstance(stages_payload, dict) else False
    summary: dict[str, dict[str, object]] = {
        str(step["key"]): {"start_ts": None, "end_ts": None, "status": "pending"}
        for step in _DETAIL_STAGE_STEPS
    }
    latest_stage_data: dict[str, dict[str, object]] = {}
    task_end_ts: float | None = None
    last_event_ts: float | None = None
    last_seen_index = -1
    for event in events if isinstance(events, list) else []:
        if not isinstance(event, dict):
            continue
        ts = event.get("ts")
        try:
            ts_value = float(ts)
        except (TypeError, ValueError):
            ts_value = None
        if ts_value is not None:
            last_event_ts = ts_value
        event_type = str(event.get("type") or "").strip()
        data = event.get("data") if isinstance(event.get("data"), dict) else {}
        step_key = _normalize_stage_step_key(data.get("stage"))
        if event_type == "task_end" and ts_value is not None:
            task_end_ts = ts_value
        if event_type == "stage" and step_key:
            step_summary = summary.get(step_key)
            if step_summary is not None and step_summary.get("start_ts") is None:
                step_summary["start_ts"] = ts_value
            for idx, step in enumerate(_DETAIL_STAGE_STEPS):
                if step["key"] == step_key and idx > last_seen_index:
                    last_seen_index = idx
                    break
        if event_type == "stage_result" and step_key:
            latest_stage_data[step_key] = dict(data)
    for idx, step in enumerate(_DETAIL_STAGE_STEPS):
        step_key = str(step["key"])
        step_summary = summary[step_key]
        if step_summary["start_ts"] is None:
            continue
        next_start: float | None = task_end_ts
        for j in range(idx + 1, len(_DETAIL_STAGE_STEPS)):
            candidate = summary[str(_DETAIL_STAGE_STEPS[j]["key"])]["start_ts"]
            if candidate is not None:
                next_start = float(candidate)
                break
        step_summary["end_ts"] = next_start
    task_status = str(row.status or "").strip()
    if task_status == "passed":
        for step_summary in summary.values():
            step_summary["status"] = "completed"
    elif task_status == "pending":
        pass
    elif last_seen_index < 0:
        if task_status == "running":
            summary["preprocess"]["status"] = "running"
        elif task_status in {"failed", "error", "cancelled"}:
            summary["preprocess"]["status"] = "failed"
    else:
        for idx, step in enumerate(_DETAIL_STAGE_STEPS):
            step_key = str(step["key"])
            if idx < last_seen_index:
                summary[step_key]["status"] = "completed"
            elif idx == last_seen_index:
                summary[step_key]["status"] = "failed" if task_status in {"failed", "error", "cancelled"} else "running"
    return {
        "events": [],
        "final": final,
        "event_count": len(events) if isinstance(events, list) else 0,
        "last_event_ts": last_event_ts,
        "step_summary": summary,
        "latest_stage_data": latest_stage_data,
    }


def _normalize_relative_session_path(path: str) -> str:
    parts = [part for part in str(path or "").replace("\\", "/").split("/") if part and part != "."]
    if not parts:
        raise ValueError("会话路径不能为空")
    if any(part == ".." for part in parts):
        raise ValueError("会话路径非法")
    return "/".join(parts)


def _resolve_session_path(sessions_root: Path, relative_path: str) -> Path:
    normalized = _normalize_relative_session_path(relative_path)
    candidate = (sessions_root / normalized).resolve()
    root_resolved = sessions_root.resolve()
    if not str(candidate).startswith(str(root_resolved)):
        raise ValueError("会话路径超出允许范围")
    if candidate.suffix.lower() != ".jsonl":
        raise ValueError("仅支持 .jsonl 会话文件")
    return candidate


def _parse_message_parts(content: object) -> list[dict]:
    parts: list[dict] = []
    if isinstance(content, str):
        parts.append({"type": "text", "text": content})
        return parts
    if not isinstance(content, list):
        return parts
    for item in content:
        if not isinstance(item, dict):
            continue
        content_type = item.get("type", "")
        if content_type == "text":
            parts.append({"type": "text", "text": item.get("text", "")})
        elif content_type == "thinking":
            parts.append({"type": "thinking", "text": item.get("thinking", "")})
        elif content_type == "toolCall":
            parts.append({
                "type": "toolCall",
                "name": item.get("name", ""),
                "id": item.get("id", ""),
                "arguments": item.get("arguments", {}),
            })
        elif content_type == "toolResult":
            parts.append({"type": "toolResult", "text": item.get("text", "")})
        else:
            parts.append({"type": "unknown", "detail": str(item)[:200]})
    return parts


def _parse_session_jsonl_lines(lines: list[str], *, start_line: int = 1) -> tuple[dict, list[dict], list[str], int]:
    events: list[dict] = []
    warnings: list[str] = []
    session_meta: dict = {}
    line_count = 0
    for index, raw_line in enumerate(lines):
        line_no = start_line + index
        line = raw_line.strip()
        if not line:
            continue
        line_count += 1
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            warnings.append(f"第 {line_no} 行 JSON 解析失败")
            events.append({"type": "raw", "line": line_no, "raw_line": line[:200], "summary": line[:200]})
            continue
        if not isinstance(obj, dict):
            events.append({"type": "raw", "line": line_no, "raw_line": line[:200], "summary": line[:200]})
            continue
        event_type = obj.get("type", "")
        if event_type == "session":
            session_meta = {
                "id": obj.get("id", ""),
                "version": obj.get("version", ""),
                "timestamp": obj.get("timestamp", ""),
                "cwd": obj.get("cwd", ""),
            }
            continue
        if event_type == "model_change":
            events.append({
                "type": "model_change",
                "line": line_no,
                "event_index": line_no,
                "timestamp": obj.get("timestamp", ""),
                "display_timestamp": obj.get("timestamp", ""),
                "provider": obj.get("provider", ""),
                "modelId": obj.get("modelId", ""),
                "raw_line": line,
            })
            continue
        if event_type == "thinking_level_change":
            level = obj.get("thinkingLevel", "")
            events.append({
                "type": "thinking_level_change",
                "line": line_no,
                "event_index": line_no,
                "timestamp": obj.get("timestamp", ""),
                "display_timestamp": obj.get("timestamp", ""),
                "thinkingLevel": level,
                "thinkingLevelClass": f"thinking-{_SESSION_THINKING_LEVEL_MAP.get(str(level).lower(), 'off')}",
                "raw_line": line,
            })
            continue
        if event_type == "message":
            msg = obj.get("message", {}) if isinstance(obj.get("message"), dict) else {}
            role = msg.get("role", "")
            event_data = {
                "type": "message",
                "line": line_no,
                "event_index": line_no,
                "timestamp": obj.get("timestamp", ""),
                "display_timestamp": obj.get("timestamp", ""),
                "role": role,
                "render_role": role,
                "parts": _parse_message_parts(msg.get("content", [])),
                "raw_line": line,
            }
            if role == "toolResult":
                event_data["toolCallId"] = msg.get("toolCallId", msg.get("tool_call_id", ""))
                event_data["toolName"] = msg.get("toolName", msg.get("tool_name", ""))
                event_data["isError"] = msg.get("isError", msg.get("is_error", False))
            events.append(event_data)
            continue
        events.append({
            "type": event_type or "unknown_event",
            "line": line_no,
            "event_index": line_no,
            "display_timestamp": obj.get("timestamp", ""),
            "summary": str(obj)[:200],
            "raw_line": line[:200],
        })
    return session_meta, events, warnings, line_count


def _parse_session_jsonl_file(path: Path) -> tuple[dict, list[dict], list[str], int]:
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        lines = handle.readlines()
    return _parse_session_jsonl_lines(lines)


def _origin_payload(row: AppSaTask) -> dict:
    task_origin_type = str(row.task_origin_type or "").strip() or "manual"
    parent_task_type = str(row.parent_task_type or "").strip() or None
    origin_label = (
        "二进制安全-源码扫描"
        if task_origin_type == "binary_security" and parent_task_type == "source"
        else "二进制安全-二进制类扫描"
        if task_origin_type == "binary_security"
        else "手动任务"
    )
    return {
        "task_origin_type": task_origin_type,
        "parent_project_id": row.parent_project_id,
        "parent_task_id": row.parent_task_id,
        "parent_task_type": parent_task_type,
        "parent_stage_name": row.parent_stage_name,
        "parent_stage_item_id": row.parent_stage_item_id,
        "parent_stage_item_key": row.parent_stage_item_key,
        "origin_label": origin_label,
        "parent_task_display": row.parent_task_id,
    }


def _load_svc_config():
    for p in [SERVICE_CONFIG_PATH, "/opt/system_analyse/config.example.json"]:
        if os.path.isfile(p):
            return load_service_config(p)
    raise RuntimeError(f"Service config not found: {SERVICE_CONFIG_PATH}")


def _load_svc_config_from_db(db: "Session", project_id: str) -> "ServiceConfig":
    """从数据库读取分析配置，构造 ServiceConfig；失败时回退到文件读取。"""
    try:
        from app.service.config_service import get_config_service
        from app.models import ServiceConfig as _ServiceConfig
        cfg_dict = get_config_service().get_config(db, project_id)
        # Strip meta/readonly fields not part of ServiceConfig schema
        for _k in ("updated_at", "project_id"):
            cfg_dict.pop(_k, None)
        return _ServiceConfig(**cfg_dict)
    except Exception as _exc:
        logger.warning("_load_svc_config_from_db failed (%s), falling back to file: %s", project_id, _exc)
        return _load_svc_config()


def _normalize_analysis_mode(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    return ANALYSIS_MODE_SOURCE if normalized == ANALYSIS_MODE_SOURCE else ANALYSIS_MODE_BINARY


def _infer_analysis_mode(row: AppSaTask, *, include_config: bool = True) -> str:
    explicit = str(getattr(row, "analysis_mode", "") or "").strip().lower()
    if explicit in (ANALYSIS_MODE_BINARY, ANALYSIS_MODE_SOURCE):
        return explicit
    if str(row.parent_task_type or "").strip().lower() == ANALYSIS_MODE_SOURCE:
        return ANALYSIS_MODE_SOURCE
    if not include_config:
        return ANALYSIS_MODE_BINARY
    targets = (row.task_config_json or {}).get("analyse_targets") if isinstance(row.task_config_json, dict) else None
    if isinstance(targets, list) and ANALYSIS_MODE_SOURCE in {str(item).strip().lower() for item in targets}:
        return ANALYSIS_MODE_SOURCE
    return ANALYSIS_MODE_BINARY


def _analysis_mode_label(mode: str) -> str:
    return "源码模式" if _normalize_analysis_mode(mode) == ANALYSIS_MODE_SOURCE else "二进制模式"


def generate_prompt_from_path(input_path: str, analysis_mode: str | None = None) -> str:
    mode = _normalize_analysis_mode(analysis_mode)
    if mode == ANALYSIS_MODE_SOURCE:
        return (
            f"对路径 `{input_path}` 下的源码项目进行系统性安全分析，"
            "重点关注：代码模块划分、入口与调用关系、危险 API、配置与脚本风险、敏感信息暴露及风险等级评估。"
        )
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


def _flush_stages(task_id: str, events: list[dict]) -> None:
    """Legacy flush path (used by _execute_task废弃路径 and as fallback).
    新代码路径通过 event_log.append_events 直接写文件，不再经过此函数。
    此函数保留以兼容尚未迁移的老代码路径。
    """
    try:
        from sqlalchemy.orm.attributes import flag_modified
        from app.db import get_db as _get_db
        _gen = _get_db()
        _db = next(_gen)
        try:
            _r = _db.query(AppSaTask).filter_by(task_id=task_id).first()
            if _r:
                _r.stages_json = {"events": [dict(e) for e in events]}
                flag_modified(_r, "stages_json")
                _db.commit()
        finally:
            try:
                next(_gen)
            except StopIteration:
                pass
    except Exception as _exc:
        logger.warning("_flush_stages failed: %s", _exc, exc_info=True)


def _write_models_json_from_db(db: "Session") -> None:  # noqa: ARG001
    """任务启动时从两个来源生成 models.json（每次任务启动拉最新）。

    来源1 = 配置中心 /service/llm/providers (模型配置中心)
    来源2 = AIGW MySQL gaiasec_llm_gateway.model_aliases (网关配置)
    sync_providers_to_pi 合并两源写入 models.json（gaiasec 的 models 用 aliases 填充）。
    参考 DVS 微服务实现。db 参数保留以兼容 deps 签名。
    """
    try:
        from app.config import get_service_yaml  # noqa: PLC0415
        from app.service.llm_provider_sync import sync_providers_to_pi  # noqa: PLC0415
        svc = get_service_yaml()
        ok = sync_providers_to_pi(
            base_url=svc.configcenter.base_url,
            token=svc.auth_service.service_machine_token,
            timeout=svc.configcenter.timeout,
        )
        if not ok:
            logger.warning("_write_models_json_from_db: 配置中心同步失败，保留现有 models.json")
    except Exception as _exc:
        logger.warning("_write_models_json_from_db failed: %s", _exc, exc_info=True)


def _merge_result_json(existing: dict | None, patch: dict | None) -> dict | None:
    base = dict(existing or {}) if isinstance(existing, dict) else {}
    if not isinstance(patch, dict):
        return base or None
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key] = _merge_result_json(base.get(key), value)
        else:
            base[key] = value
    return base or None


class TaskService:
    def __init__(self) -> None:
        from app.db import get_db

        self._task_repository = TaskRepository()
        self._runner_registry = init_runner_registry_service(
            get_db=get_db,
            get_running_tasks_count=_running_tasks_count,
            cleanup_idle_runtime=lambda: self._runner.force_cleanup_all_agents(phase="idle_reaper"),
        )
        self._runner = TaskRunner(
            deps=TaskRunnerDependencies(
                get_db=get_db,
                acquire_execution_lock=self._acquire_execution_lock,
                clear_task_execution_lock=_clear_task_execution_lock,
                flush_stages=_flush_stages,
                load_svc_config_from_db=_load_svc_config_from_db,
                infer_analysis_mode=_infer_analysis_mode,
                security_filter_log_payload_resolved=lambda payload: _security_filter_log_payload(payload, resolved=True),
                write_models_json_from_db=_write_models_json_from_db,
                write_task_result_json=_write_task_result_json,
                lightweight_result_json=_lightweight_result_json,
                remove_running_task=self._remove_running_task,
                record_timeline_event=self._record_timeline_event,
                task_repository=self._task_repository,
                merge_result_json=_merge_result_json,
            ),
            settings=TaskRunnerSettings(
                source_mode_default_analyse_targets=list(SOURCE_MODE_DEFAULT_ANALYSE_TARGETS),
                task_stage_flush_batch_size=TASK_STAGE_FLUSH_BATCH_SIZE,
                task_stage_flush_min_interval_seconds=TASK_STAGE_FLUSH_MIN_INTERVAL_SECONDS,
                task_cancel_poll_interval_seconds=TASK_CANCEL_POLL_INTERVAL_SECONDS,
                task_lease_heartbeat_seconds=TASK_LEASE_HEARTBEAT_SECONDS,
            ),
        )
        self._dispatcher = WorkerDispatcher(
            get_db=get_db,
            clear_task_execution_lock=_clear_task_execution_lock,
            cleanup_resume_files=_remove_task_root_for_restart,
            claim_task_lease=self._claim_task_lease,
            spawn_task=self._on_task_claimed,
            record_timeline_event=self._record_timeline_event,
            select_dispatch_target=self._select_dispatch_target,
            get_running_tasks_count=_running_tasks_count,
            load_runtime_control=self._load_runtime_control,
            task_repository=self._task_repository,
        )
        # 统一调度器 (单一权威派发器, 已切换为 DB-backed)。旧 WorkerDispatcher 保留类但不再启动。
        self._scheduler = SchedulerService(
            get_db=get_db,
            task_repo=self._task_repository,
            spawn_task=self._on_task_claimed,
            record_event=lambda task_id, project_id, event_type, message, level='info', payload=None: self._record_timeline_event(task_id=task_id, project_id=project_id, event_type=event_type, message=message, level=level, payload=payload),
            load_runtime_control=self._load_runtime_control,
            clear_task_lock=_clear_task_execution_lock,
            cleanup_resume=_remove_task_root_for_restart,
            select_dispatch_target=self._select_dispatch_target,
            claim_task_lease=self._claim_task_lease,
            get_running_tasks_count=_running_tasks_count,
            build_should_recover=self._build_should_recover,
        )
        set_scheduler(self._scheduler)
        self._runner_assignment_task: object | None = None
        self._runner_assignment_loop_running = False
        self._query = TaskQueryService(
            get_or_404=self._get_or_404,
            read_text_if_exists=_read_text_if_exists,
            infer_risk_level=_infer_risk_level,
            infer_risk_score=_infer_risk_score,
            parse_report_sections=_parse_report_sections,
            parse_summary=_parse_summary,
            task_sessions_root=_task_sessions_root,
            task_run_root=_task_run_root,
            resolve_session_path=_resolve_session_path,
            parse_session_jsonl_file=_parse_session_jsonl_file,
            write_json_atomic=_write_json_atomic,
        )


    def list_tasks(self, db: Session, *, project_id: str | None, page: int = 1,
                   per_page: int = 50, status: Optional[str] = None,
                   analysis_mode: Optional[str] = None,
                   parent_task_id: Optional[str] = None,
                   sort_by: str = "created_at",
                   sort_order: str = "desc") -> dict:
        query = db.query(AppSaTask).filter(AppSaTask.is_deleted.is_(False))
        if project_id:
            query = query.filter(AppSaTask.project_id == project_id)
        if status:
            query = query.filter(AppSaTask.status == status)
        normalized_parent_task_id = str(parent_task_id or "").strip()
        if normalized_parent_task_id:
            query = query.filter(AppSaTask.parent_task_id == normalized_parent_task_id)
        sort_column = _TASK_LIST_SORT_COLUMNS.get(str(sort_by or "").strip(), AppSaTask.created_at)
        order_expr = sort_column.asc() if str(sort_order or "").lower() == "asc" else sort_column.desc()
        requested_mode = _normalize_analysis_mode(analysis_mode) if analysis_mode else None
        if requested_mode:
            query = query.filter(AppSaTask.analysis_mode == requested_mode)
        total = query.count()
        rows = (query.options(*self._list_load_options())
                .order_by(order_expr, AppSaTask.id.desc())
                .offset((page - 1) * per_page).limit(per_page).all())
        return {"items": [self._row_to_list_item(r) for r in rows],
                "total": total, "page": page, "per_page": per_page}

    def get_task_stats(
        self,
        db: Session,
        *,
        project_id: str | None,
        status: Optional[str] = None,
        analysis_mode: Optional[str] = None,
        parent_task_id: Optional[str] = None,
    ) -> dict:
        query = db.query(AppSaTask.status, func.count(AppSaTask.id)).filter(AppSaTask.is_deleted.is_(False))
        if project_id:
            query = query.filter(AppSaTask.project_id == project_id)
        if status:
            query = query.filter(AppSaTask.status == status)
        normalized_parent_task_id = str(parent_task_id or "").strip()
        if normalized_parent_task_id:
            query = query.filter(AppSaTask.parent_task_id == normalized_parent_task_id)
        requested_mode = _normalize_analysis_mode(analysis_mode) if analysis_mode else None
        if requested_mode:
            query = query.filter(AppSaTask.analysis_mode == requested_mode)
        rows = query.group_by(AppSaTask.status).all()
        counts = {str(task_status or ""): int(count or 0) for task_status, count in rows}
        return {
            "total": sum(counts.values()),
            "pending": counts.get("pending", 0),
            "running": counts.get("running", 0),
            "passed": counts.get("passed", 0),
            "failed": counts.get("failed", 0),
            "error": counts.get("error", 0),
            "cancelled": counts.get("cancelled", 0),
        }

    def get_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        result = self._row_to_dict(row)
        # 计算实际生效配置：task_config_json 覆盖项目配置
        # 供前端展示具体配置项而非“使用默认”文字
        try:
            from app.service.config_service import get_config_service
            proj_cfg = get_config_service().get_config(db, row.project_id)
            tcfg = row.task_config_json or {}
            _fields = (
                "analyse_targets",
                "binary_arch",
                "security_focus_categories",
                "module_granularity",
                "filter_engine",
                "enable_final_check",
                "continue_on_module_failure",
            )
            effective: dict = {}
            source: dict = {}   # 每个字段的来源："task"或"project"
            for _f in _fields:
                if _f in tcfg and tcfg[_f] is not None:
                    effective[_f] = tcfg[_f]
                    source[_f] = "task"
                elif _f in proj_cfg and proj_cfg[_f] is not None:
                    effective[_f] = proj_cfg[_f]
                    source[_f] = "project"
            result["effective_config_json"] = effective
            result["effective_config_source"] = source
        except Exception as _exc:
            logger.warning("get_task: failed to compute effective_config for %s: %s", task_id, _exc)
            result["effective_config_json"] = row.task_config_json or {}
            result["effective_config_source"] = {}
        task_config = row.task_config_json if isinstance(row.task_config_json, dict) else {}
        result["agent_auth_json"] = task_config.get("agent_auth_json") if isinstance(task_config.get("agent_auth_json"), dict) else None
        result["role_config_snapshot"] = task_config.get("role_config_snapshot") if isinstance(task_config.get("role_config_snapshot"), dict) else None
        result["provider_runtime_summary"] = task_config.get("provider_runtime_summary") if isinstance(task_config.get("provider_runtime_summary"), dict) else None
        result["llm_binding_snapshot"] = task_config.get("llm_binding_snapshot") if isinstance(task_config.get("llm_binding_snapshot"), dict) else None
        return result

    def get_timeline(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        events = (
            db.query(AppSaTaskEvent)
            .filter(AppSaTaskEvent.task_id == row.task_id)
            .order_by(AppSaTaskEvent.created_at.asc(), AppSaTaskEvent.id.asc())
            .all()
        )
        return {
            "task_id": row.task_id,
            "events": [
                {
                    "id": event.id,
                    "task_id": event.task_id,
                    "project_id": event.project_id,
                    "stage_name": event.stage_name,
                    "level": event.level,
                    "event_type": event.event_type,
                    "message": event.message,
                    "payload": event.payload_json if isinstance(event.payload_json, dict) else None,
                    "payload_json": event.payload_json if isinstance(event.payload_json, dict) else None,
                    "recorder_instance_id": event.payload_json.get("recorder", {}).get("instance_id") if isinstance(event.payload_json, dict) else None,
                    "recorder_hostname": event.payload_json.get("recorder", {}).get("hostname") if isinstance(event.payload_json, dict) else None,
                    "recorder_pod_name": event.payload_json.get("recorder", {}).get("pod_name") if isinstance(event.payload_json, dict) else None,
                    "recorder_node_name": event.payload_json.get("recorder", {}).get("node_name") if isinstance(event.payload_json, dict) else None,
                    "recorder_pod_ip": event.payload_json.get("recorder", {}).get("pod_ip") if isinstance(event.payload_json, dict) else None,
                    "recorder_role": event.payload_json.get("recorder", {}).get("role") if isinstance(event.payload_json, dict) else None,
                    "origin_instance_id": event.payload_json.get("event_origin", {}).get("instance_id") if isinstance(event.payload_json, dict) else None,
                    "origin_hostname": event.payload_json.get("event_origin", {}).get("hostname") if isinstance(event.payload_json, dict) else None,
                    "origin_pod_name": event.payload_json.get("event_origin", {}).get("pod_name") if isinstance(event.payload_json, dict) else None,
                    "origin_node_name": event.payload_json.get("event_origin", {}).get("node_name") if isinstance(event.payload_json, dict) else None,
                    "origin_role": event.payload_json.get("event_origin", {}).get("role") if isinstance(event.payload_json, dict) else None,
                    "created_at": isoformat_local(event.created_at),
                }
                for event in events
            ],
        }

    @classmethod
    def _record_task_operation_event(
        cls,
        *,
        task_id: str,
        project_id: str | None,
        operation: str,
        event_type: str,
        message: str,
        level: str = "info",
        payload: dict | None = None,
    ) -> None:
        base_payload = {"operation": operation, "request_source": "task_api"}
        if isinstance(payload, dict):
            base_payload.update(payload)
        cls._record_timeline_event(
            task_id=task_id,
            project_id=project_id,
            event_type=event_type,
            message=message,
            level=level,
            payload=base_payload,
        )

    def clear_timeline(self, db: Session, task_id: str) -> int:
        row = self._get_or_404(db, task_id)
        deleted = (
            db.query(AppSaTaskEvent)
            .filter(AppSaTaskEvent.task_id == row.task_id)
            .delete(synchronize_session=False)
        )
        deleted_count = int(deleted or 0)
        self._record_task_operation_event(
            task_id=row.task_id,
            project_id=row.project_id,
            operation="clear_timeline",
            event_type="timeline_cleared",
            message="任务时间线已清空",
            level="warning",
            payload={
                "deleted_event_count": deleted_count,
            },
        )
        return deleted_count

    def delete_timeline_event(self, db: Session, task_id: str, event_id: str) -> int:
        row = self._get_or_404(db, task_id)
        target = (
            db.query(AppSaTaskEvent)
            .filter(AppSaTaskEvent.task_id == row.task_id, AppSaTaskEvent.id == event_id)
            .first()
        )
        if target is None:
            return 0
        deleted = (
            db.query(AppSaTaskEvent)
            .filter(AppSaTaskEvent.task_id == row.task_id, AppSaTaskEvent.id == event_id)
            .delete(synchronize_session=False)
        )
        deleted_count = int(deleted or 0)
        if deleted_count:
            self._record_task_operation_event(
                task_id=row.task_id,
                project_id=row.project_id,
                operation="delete_timeline_event",
                event_type="timeline_event_deleted",
                message="任务时间线事件已删除",
                level="warning",
                payload={
                    "deleted_event_id": target.id,
                    "deleted_event_type": target.event_type,
                    "deleted_event_stage_name": target.stage_name,
                    "deleted_event_created_at": _safe_isoformat(target.created_at),
                },
            )
        return deleted_count

    def repair_task_origin(self, db: Session, task_id: str, analysis_mode: str) -> dict:
        row = self._get_or_404(db, task_id)
        previous_status = str(row.status or "")
        previous_mode = str(row.analysis_mode or "").strip() or _infer_analysis_mode(row)
        if row.status in ("pending", "running"):
            from fastapi import HTTPException
            self._record_task_operation_event(
                task_id=row.task_id,
                project_id=row.project_id,
                operation="repair_task_origin",
                event_type="task_operation_rejected",
                message="任务来源修复被拒绝",
                level="error",
                payload={
                    "reason": "task_running",
                    "status": previous_status,
                    "before_status": previous_status,
                    "after_status": previous_status,
                    "changed": False,
                },
            )
            raise HTTPException(400, "任务处于运行态，不能修改来源信息")
        if str(row.task_origin_type or "").strip() not in ("", "manual"):
            from fastapi import HTTPException
            self._record_task_operation_event(
                task_id=row.task_id,
                project_id=row.project_id,
                operation="repair_task_origin",
                event_type="task_operation_rejected",
                message="任务来源修复被拒绝",
                level="error",
                payload={
                    "reason": "unsupported_task_origin_type",
                    "status": previous_status,
                    "before_status": previous_status,
                    "after_status": previous_status,
                    "changed": False,
                    "task_origin_type": row.task_origin_type,
                },
            )
            raise HTTPException(400, "仅手动任务支持修改来源信息")

        normalized_mode = _normalize_analysis_mode(analysis_mode)
        resolved_config_snapshot_cleared = False
        row.analysis_mode = normalized_mode
        if isinstance(row.task_config_json, dict) and "resolved_config_snapshot" in row.task_config_json:
            row.task_config_json = {
                k: v for k, v in row.task_config_json.items()
                if k != "resolved_config_snapshot"
            } or None
            if hasattr(row, "_sa_instance_state"):
                flag_modified(row, "task_config_json")
            resolved_config_snapshot_cleared = True

        db.commit()
        db.refresh(row)
        self._record_task_operation_event(
            task_id=row.task_id,
            project_id=row.project_id,
            operation="repair_task_origin",
            event_type="task_origin_repaired",
            message="任务来源信息已修复",
            payload={
                "before_status": previous_status,
                "after_status": str(row.status or ""),
                "changed": previous_mode != normalized_mode or resolved_config_snapshot_cleared,
                "previous_analysis_mode": previous_mode,
                "analysis_mode": normalized_mode,
                "task_origin_type": row.task_origin_type,
                "resolved_config_snapshot_cleared": resolved_config_snapshot_cleared,
            },
        )
        log_event(
            logger,
            logging.INFO,
            "task origin repaired",
            event="task_origin_repaired",
            task_id=task_id,
            project_id=row.project_id,
            analysis_mode=normalized_mode,
            task_origin_type=row.task_origin_type,
        )
        return self._row_to_dict(row)

    def get_task_result(self, db: Session, task_id: str) -> dict:
        return self._query.get_task_result(db, task_id)

    def list_task_sessions(self, db: Session, task_id: str) -> list[dict]:
        return self._query.list_task_sessions(db, task_id)

    def get_task_session_index(self, db: Session, task_id: str) -> dict:
        return self._query.get_task_session_index(db, task_id)

    def get_task_session_file(self, db: Session, task_id: str, relative_path: str) -> dict:
        return self._query.get_task_session_file(db, task_id, relative_path)

    def get_task_evaluation(self, db: Session, task_id: str) -> dict:
        return self._query.get_task_evaluation(db, task_id)

    def get_runtime_overview(self, db: Session) -> dict:
        status_counts = self._task_repository.get_status_counts(db)
        oldest_pending_created_at = self._task_repository.get_oldest_pending_created_at(db)
        running_rows = self._task_repository.list_running_tasks(db, limit=20)
        worker_health = get_worker_runtime_health()
        runtime_control = get_runtime_control_service().get_runtime_control(db)
        active_runners = get_runner_registry_service().list_active_runners(db)
        pending_repair_cutoff = now_local() - timedelta(seconds=get_pending_scheduler_repair_grace_seconds())
        pending_repair_candidates = self._task_repository.list_pending_tasks_for_scheduler_repair(
            db,
            created_before=pending_repair_cutoff,
            limit=500,
        )
        return {
            "queue": {
                "status_counts": status_counts,
                "pending_count": int(status_counts.get("pending", 0)),
                "running_count": int(status_counts.get("running", 0)),
                "terminal_count": sum(
                    int(status_counts.get(status, 0))
                    for status in ("passed", "failed", "error", "cancelled")
                ),
                "oldest_pending_created_at": isoformat_local(oldest_pending_created_at),
                "pending_unqueued_count": len(pending_repair_candidates),
            },
            "worker_settings": get_worker_runtime_settings(),
            "worker_health": worker_health,
            "runtime_control": runtime_control,
            "active_runners": [
                {
                    "instance_id": str(item["instance_id"]),
                    "status": str(item["status"]),
                    "capacity": int(item["capacity"]),
                    "running_tasks": int(item["running_tasks"]),
                    "age_seconds": float(item.get("age_seconds") or 0.0),
                    "updated_at": isoformat_local(item.get("updated_at")),
                }
                for item in active_runners
            ],
            "running_tasks": [
                {
                    "task_id": row.task_id,
                    "project_id": row.project_id,
                    "task_name": row.task_name,
                    "analysis_mode": _infer_analysis_mode(row),
                    "dispatcher_instance_id": row.dispatcher_instance_id,
                    "lease_epoch": int(row.lease_epoch or 0),
                    "dispatch_started_at": isoformat_local(row.dispatch_started_at),
                    "lease_expires_at": isoformat_local(row.lease_expires_at),
                    "started_at": isoformat_local(row.started_at),
                    "created_at": isoformat_local(row.created_at),
                }
                for row in running_rows
            ],
            "pending_unqueued_tasks": [
                {
                    "task_id": row.task_id,
                    "project_id": row.project_id,
                    "task_name": row.task_name,
                    "analysis_mode": _infer_analysis_mode(row),
                    "created_at": isoformat_local(row.created_at),
                }
                for row in pending_repair_candidates[:20]
            ],
        }

    def create_task(self, db: Session, *, project_id: str, task_name: str,
                    input_path: str, output_path: Optional[str] = None,
                    task_description: Optional[str] = None,
                    prompt_template_id: Optional[str] = None,
                    prompt_content: str, created_by: Optional[str] = None,
                    task_config_json: Optional[dict] = None,
                    analysis_mode: Optional[str] = None,
                    task_origin_type: Optional[str] = None,
                    parent_project_id: Optional[str] = None,
                    parent_task_id: Optional[str] = None,
                    parent_task_type: Optional[str] = None,
                    parent_stage_name: Optional[str] = None,
                    parent_stage_item_id: Optional[str] = None,
                    parent_stage_item_key: Optional[str] = None) -> dict:
        task_id = f"sat_{uuid.uuid4().hex[:16]}"
        _fs_base = os.environ.get("FILESERVER_ROOT", "/data/files")
        effective_output = output_path or f"{_fs_base}/{project_id}/app/secflow-app-system-analyse"
        mode = _normalize_analysis_mode(analysis_mode or parent_task_type)
        effective_task_config = dict(task_config_json or {})
        if mode == ANALYSIS_MODE_SOURCE and "analyse_targets" not in effective_task_config:
            effective_task_config["analyse_targets"] = list(SOURCE_MODE_DEFAULT_ANALYSE_TARGETS)
        # 快照项目配置中的关键字段，确保任务配置自包含，不依赖重跑时项目配置的当前状态
        # 未显式传入的字段（security_focus_categories / module_granularity / binary_arch）
        # 从项目配置读取并写入 task_config_json，防止重跑时项目配置变更导致运行参数隐性改变
        _snap_fields = (
            "security_focus_categories",
            "module_granularity",
            "binary_arch",
            "filter_engine",
            "enable_final_check",
            "continue_on_module_failure",
        )
        _missing_snap = [k for k in _snap_fields if k not in effective_task_config]
        if _missing_snap:
            try:
                _proj_svc = _load_svc_config_from_db(db, project_id)
                for _k in _missing_snap:
                    _v = getattr(_proj_svc, _k, None)
                    if _v is not None:
                        effective_task_config[_k] = _v
            except Exception as _snap_err:
                logger.warning("task %s: failed to snapshot project config fields %s: %s",
                               task_id, _missing_snap, _snap_err)
        row = AppSaTask(
            task_id=task_id, project_id=project_id, task_name=task_name,
            task_description=task_description, input_path=input_path,
            output_path=effective_output, prompt_template_id=prompt_template_id,
            prompt_content=prompt_content, status="pending", created_by=created_by,
            task_config_json=effective_task_config or None,
            task_origin_type=str(task_origin_type or "").strip() or "manual",
            analysis_mode=mode,
            parent_project_id=parent_project_id,
            parent_task_id=parent_task_id,
            parent_task_type=parent_task_type,
            parent_stage_name=parent_stage_name,
            parent_stage_item_id=parent_stage_item_id,
            parent_stage_item_key=parent_stage_item_key,
            lease_epoch=0,
        )
        db.add(row); db.commit(); db.refresh(row)
        self._record_task_operation_event(
            task_id=task_id,
            project_id=project_id,
            operation="create_task",
            event_type="task_created",
            message="任务已创建",
            payload={
                "before_status": None,
                "after_status": row.status,
                "changed": True,
                "analysis_mode": mode,
                "task_origin_type": row.task_origin_type,
            },
        )
        log_event(logger, logging.INFO, "task created",
                  event="task_created", task_id=task_id, project_id=project_id,
                  analysis_mode=mode,
                  **_security_filter_log_payload(effective_task_config))
        return self._row_to_dict(row)

    def restart_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        previous_status = str(row.status or "")
        if row.status in ("pending", "running"):
            from fastapi import HTTPException
            self._record_task_operation_event(
                task_id=row.task_id,
                project_id=row.project_id,
                operation="restart_task",
                event_type="task_operation_rejected",
                message="任务重启被拒绝",
                level="error",
                payload={
                    "reason": "task_active",
                    "status": previous_status,
                    "before_status": previous_status,
                    "after_status": previous_status,
                    "changed": False,
                },
            )
            raise HTTPException(400, "任务仍在运行中，请先取消后再重启")
        cleanup_result = _remove_task_root_for_restart(row.output_path, task_id)
        if row.output_path:
            # 真实失败：旧目录存在但既没删也没移走（restart 现在纯删除，不重建目录；
            # 那是预期的，不算失败）
            if cleanup_result.get("existed") and not cleanup_result.get("removed") and not cleanup_result.get("renamed_to"):
                task_root = Path(row.output_path) / task_id
                from fastapi import HTTPException
                self._record_task_operation_event(
                    task_id=row.task_id,
                    project_id=row.project_id,
                    operation="restart_task",
                    event_type="task_operation_rejected",
                    message="任务重启被拒绝：旧运行目录未清理干净",
                    level="error",
                    payload={
                        "reason": "task_root_cleanup_failed",
                        "task_root": str(task_root),
                        "cleanup": cleanup_result,
                        "before_status": previous_status,
                        "after_status": previous_status,
                        "changed": False,
                    },
                )
                raise HTTPException(500, f"重启前清理任务目录失败: {task_root}")
        _clear_task_execution_lock(row.output_path, task_id)
        row = self._task_repository.restart_task_in_place(db, row)
        row.latest_abnormal_reason_json = None
        flag_modified(row, "latest_abnormal_reason_json")
        db.commit()
        db.refresh(row)
        self._record_task_operation_event(
            task_id=task_id,
            project_id=row.project_id,
            operation="restart_task",
            event_type="task_restarted",
            message="任务已重启",
            payload={
                "before_status": previous_status,
                "after_status": str(row.status or ""),
                "changed": previous_status != str(row.status or ""),
                "analysis_mode": _infer_analysis_mode(row),
                "cleanup": cleanup_result,
            },
        )
        log_event(logger, logging.INFO, "task restarted in-place", event="task_restarted",
                  task_id=task_id, project_id=row.project_id)
        return self._row_to_dict(row)

    def cancel_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        previous_status = str(row.status or "")
        # 已完成/终态任务不能被取消
        if row.status in ("passed", "failed", "error", "cancelled"):
            self._record_task_operation_event(
                task_id=row.task_id,
                project_id=row.project_id,
                operation="cancel_task",
                event_type="task_operation_rejected",
                message="已完成的任务不能取消",
                level="warning",
                payload={
                    "before_status": previous_status,
                    "after_status": previous_status,
                    "changed": False,
                    "reason": "task_already_terminal",
                    "status": previous_status,
                },
            )
            from fastapi import HTTPException
            raise HTTPException(status_code=409, detail="已完成的任务不能取消")
        at = _running_tasks.get(task_id)
        if at and at.is_alive():
            pass  # thread cannot be cancelled
        row = self._task_repository.cancel_task_in_place(db, row)
        reason, changed = _sync_task_abnormal_reason(row)
        _record_abnormal_reason(row, reason, changed=changed)
        db.commit()
        db.refresh(row)
        _clear_task_execution_lock(row.output_path, task_id)
        self._record_task_operation_event(
            task_id=task_id,
            project_id=row.project_id,
            operation="cancel_task",
            event_type="task_cancelled",
            message="任务已取消",
            level="warning",
            payload={
                "before_status": previous_status,
                "after_status": str(row.status or ""),
                "changed": previous_status != str(row.status or ""),
                "status": row.status,
            },
        )
        return self._row_to_dict(row)

    @staticmethod
    def _claim_task_lease(db: Session, row: AppSaTask, dispatch_target: str) -> int | None:
        return TaskRepository.claim_task_lease(
            db,
            row,
            worker_instance_id=dispatch_target,
            lease_deadline=_lease_deadline,
        )

    @staticmethod
    def _load_runtime_control(db: Session) -> dict:
        return get_runtime_control_service().get_runtime_control(db)

    def _build_should_recover(self, db: Session):
        """构建纯 DB 级僵死回收判定。

        调度热路径禁止同步触发任何 /data NFS 观测或 runtime snapshot 构建。
        """
        def _should_recover(stale) -> bool:
            return True

        return _should_recover

    def start_worker_loop(self) -> None:
        if is_manager_role():
            # 确保 DB 已初始化
            try:
                from app.db import init_db
                from app.config import get_service_yaml
                svc = get_service_yaml()
                init_db(svc.database.url, pool_size=svc.database.pool_size,
                        max_overflow=svc.database.max_overflow,
                        pool_timeout=svc.database.pool_timeout,
                        pool_recycle=svc.database.pool_recycle,
                        run_migrations=False)
            except Exception:
                pass
            # 已完全切换到新调度器 (SchedulerService, DB-backed)。旧 WorkerDispatcher 不再启动。
            self._scheduler.start()
            # record_event positional→keyword args adapter
            self._scheduler._record_event = lambda tid,pid,typ,msg,lvl='info',pay=None: self._record_timeline_event(task_id=tid,project_id=pid,event_type=typ,message=msg,level=lvl,payload=pay)
        if is_runner_role():
            self._runner_registry.start()
            self._start_runner_assignment_loop()

    def stop_worker_loop(self) -> None:
        if is_manager_role():
            self._dispatcher.stop()
            self._scheduler.stop()
        if is_runner_role():
            self._stop_runner_assignment_loop()
            self._runner_registry.stop()

    # ════════════════════════ V3.0 纯 TCP 调度器启动/回调 ════════════════════════
    #
    # V3: manager 起 SchedulerV3(纯内存+TCP)，runner 起 WorkerControl(任务子进程)。
    # DB 仅用于任务记录/终态回写(供前端)，控制面无 DB 实时依赖。

    def start_v3_manager(self) -> None:
        from app.service.scheduler_v3 import SchedulerV3, set_scheduler as set_v3
        sched = SchedulerV3(
            finalize_task=self._v3_finalize,
            record_event=self._v3_record_event,
            claim_task=self._v3_claim_task,
            requeue_task=self._v3_requeue_task,
            db_running_tasks=self._v3_db_running_tasks,
        )
        set_v3(sched)
        self._v3_scheduler = sched
        sched.start()

    def stop_v3_manager(self) -> None:
        s = getattr(self, "_v3_scheduler", None)
        if s is not None:
            s.stop()
            self._v3_scheduler = None

    def start_v3_worker(self) -> None:
        from app.service.worker_control import WorkerControl
        # V3 也须启动 runner registry 心跳（供 worker_slot_snapshot 统计可执行槽位）
        try:
            self._runner_registry.start()
        except Exception:
            logger.exception("runner_registry.start failed in v3_worker")
        wc = WorkerControl(
            spawn_task_subprocess=self._v3_spawn,
            archive_task=self._v3_archive,
            output_path_getter=self._v3_get_output_path,
        )
        self._v3_worker_control = wc
        wc.start()

    def _v3_get_output_path(self, task_id: str):
        """查 DB 拿任务 output_path（控制器做 NFS↔本地桥接用）。"""
        try:
            from app.db import get_db as _get_db
            db_gen = _get_db(); db = next(db_gen)
            try:
                row = self._task_repository.get_task(db, task_id)
                return row.output_path if row else None
            finally:
                try: next(db_gen)
                except StopIteration: pass
        except Exception:
            logger.exception("v3_get_output_path failed: %s", task_id)
            return None

    def stop_v3_worker(self) -> None:
        wc = getattr(self, "_v3_worker_control", None)
        if wc is not None:
            wc.stop()
            self._v3_worker_control = None

    # —— 调度器回调：终态回写 DB 行（供前端；任务子进程正常完成时已自己写，此处幂等/仅补救）——
    def _v3_finalize(self, task_id: str, state: str, error, result) -> None:
        # 状态映射：worker_lost/failed→failed；cancelled→cancelled；finished→passed
        mapping = {
            "finished": "passed", "failed": "failed", "cancelled": "cancelled",
            "worker_lost": "failed",
        }
        db_status = mapping.get(str(state), str(state))
        try:
            db_gen = None
            from app.db import get_db as _get_db
            db_gen = _get_db()
            db = next(db_gen)
            try:
                row = self._task_repository.get_task(db, task_id)
                if row is not None and row.status not in ("passed", "failed", "error", "cancelled"):
                    row.status = db_status
                    row.finished_at = now_local()
                    if error:
                        row.error = str(error)[:65535]
                    db.commit()
            finally:
                try: next(db_gen)
                except StopIteration: pass
        except Exception:
            logger.exception("v3_finalize DB 回写失败: %s", task_id)

    def _v3_record_event(self, task_id, event_type, message, level="info", payload=None):
        # SchedulerV3 调用：(task_id, event_type, message, level, payload)
        try:
            self._record_timeline_event(task_id=task_id, event_type=event_type, message=message, level=level, payload=payload)
        except Exception:
            pass

    def _v3_requeue_task(self, task_id: str) -> bool:
        """worker 失联/瞬时拒绝 → 以 restart 语义重排（resume 断点续做已彻底移除）。
        处理：重置 DB 为 pending（清 dispatcher/finished/error/result）+ 彻底清空任务目录
        (workspace/output/events/.task_version 等)，以保证下一次派发从头运行。
        rollout/worker_lost 一律走 restart。返回是否成功。已终态(passed/cancelled)的不重排。"""
        try:
            from app.db import get_db as _get_db
            db_gen = _get_db()
            db = next(db_gen)
            output_path = None
            try:
                row = self._task_repository.get_task(db, task_id)
                if row is None:
                    return False
                if str(row.status or "") == "pending":
                    # 已 pending（如 restart 幂等重复调用），无需重置 DB
                    output_path = row.output_path
                    return True
                # restart 是显式重跑：允许 failed/cancelled/passed/running 都重置为 pending。
                # （reconcile/db_reconcile 调用方只查 status=running，不会命中 passed/cancelled）
                output_path = row.output_path
                row.status = "pending"
                row.dispatcher_instance_id = None
                row.dispatch_started_at = None
                row.finished_at = None
                row.error = None
                row.result_json = None
                try:
                    row.lease_expires_at = None
                except Exception:
                    pass
                db.commit()
            finally:
                try: next(db_gen)
                except StopIteration: pass
            # restart 语义：彻底清空任务目录(含 events.jsonl/run/output/.task_version)，
            # 确保重派后从头运行，无任何断点续做残留。
            try:
                _remove_task_root_for_restart(output_path, task_id)
                _clear_task_execution_lock(output_path, task_id)
            except Exception:
                logger.exception("v3_requeue_task restart-cleanup failed: %s", task_id)
            return True
        except Exception:
            logger.exception("v3_requeue_task failed: %s", task_id)
            return False

    def _v3_db_running_tasks(self) -> list:
        """供 SchedulerV3 DB 兜底对账：返回所有 status=running 的任务及其派发时长(秒)。"""
        out: list = []
        try:
            from app.db import get_db as _get_db
            from app.db.models import AppSaTask
            from app.time_utils import now_local
            db_gen = _get_db(); db = next(db_gen)
            try:
                now = now_local()
                rows = db.query(AppSaTask).filter(
                    AppSaTask.is_deleted.is_(False),
                    AppSaTask.status == "running",
                ).all()
                for r in rows:
                    age = None
                    ds = getattr(r, "dispatch_started_at", None)
                    if ds is not None:
                        try:
                            age = (now - ds).total_seconds()
                        except Exception:
                            age = None
                    out.append({
                        "task_id": r.task_id,
                        "dispatcher_instance_id": r.dispatcher_instance_id,
                        "dispatch_age_s": age,
                    })
            finally:
                try: next(db_gen)
                except StopIteration: pass
        except Exception:
            logger.exception("v3_db_running_tasks failed")
        return out

    # —— WorkerControl 回调：spawn 任务子进程 ——
    def _v3_claim_task(self, task_id: str, worker_id: str):
        """一次性 DB 认领：pending→running、写 dispatcher/lease_epoch/lease_expires。
        返回 lease_epoch(int)或 None。仅分发时调用一次，不是实时依赖。"""
        try:
            from app.db import get_db as _get_db
            db_gen = _get_db()
            db = next(db_gen)
            try:
                row = self._task_repository.get_task(db, task_id)
                if row is None:
                    return None
                return self._claim_task_lease(db, row, worker_id)
            finally:
                try: next(db_gen)
                except StopIteration: pass
        except Exception:
            logger.exception("v3_claim_task failed: %s", task_id)
            return None

    def _v3_spawn(self, task_id: str, lease_epoch: int):
        import subprocess, sys as _sys
        run_task_py = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "run_task.py")
        # /app/app/service/task_service.py → /app/run_task.py
        run_task_py = "/app/run_task.py"
        # 捕获任务子进程 stdout/stderr 到日志文件以便调试快速失败 (V3 任务早期异常不易定位)
        try:
            os.makedirs("/tmp/sa_v3_logs", exist_ok=True)
        except Exception:
            pass
        log_path = f"/tmp/sa_v3_logs/{task_id}.log"
        log_f = open(log_path, "wb")
        return subprocess.Popen(
            [_sys.executable, run_task_py, task_id, str(int(lease_epoch or 0))],
            stdout=log_f, stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    # —— WorkerControl 回调：任务被杀时代归档（把 workspace 产物搬到 output/）——
    def _v3_archive(self, task_id: str, normal: bool) -> None:
        if normal:
            return  # 正常完成时任务子进程自己已归档
        try:
            db_gen = None
            from app.db import get_db as _get_db
            db_gen = _get_db()
            db = next(db_gen)
            try:
                row = self._task_repository.get_task(db, task_id)
                if not row or not row.output_path:
                    return
                from pathlib import Path
                import shutil
                root = Path(row.output_path) / task_id
                ws = root / "run" / "workspace"
                out = root / "output"
                if not ws.exists():
                    return
                out.mkdir(parents=True, exist_ok=True)
                # modules
                mods_root = ws / "modules"
                if mods_root.exists() and not (out / "modules").exists():
                    out_mods = out / "modules"
                    out_mods.mkdir(parents=True, exist_ok=True)
                    for m in [d for d in mods_root.iterdir() if d.is_dir()]:
                        dst = out_mods / m.name
                        if not dst.exists():
                            shutil.copytree(str(m), str(dst))
                # final_report.md
                rep = ws / "final_report.md"
                if rep.exists() and not (out / "final_report.md").exists():
                    shutil.copy2(str(rep), str(out / "final_report.md"))
                # modules.list
                out_mods = out / "modules"
                if out_mods.exists() and not (out / "modules.list").exists():
                    from app.pipeline.helpers import generate_modules_list
                    generate_modules_list(out_mods, out / "modules.list")
            finally:
                try: next(db_gen)
                except StopIteration: pass
        except Exception:
            logger.exception("v3_archive 代归档失败: %s", task_id)


    def _on_task_claimed(self, task_id: str, lease_epoch: int, dispatch_target: str) -> None:
        if dispatch_target != WORKER_INSTANCE_ID:
            return
        self._run_task_locally(task_id, lease_epoch)

    def _on_task_claimed_scheduler(self, task_id: str, pod_id: str) -> None:
        """调度器回调: 分配任务到指定 pod 执行。"""
        if pod_id == WORKER_INSTANCE_ID or pod_id == SCHEDULER_INSTANCE_ID:
            self._run_task_locally(task_id, 0)
        # 远程 pod 通过 ExecutorAgent 接收, 此处仅记录

    @staticmethod
    def _select_dispatch_target(db: Session) -> str | None:
        if is_runner_role():
            return WORKER_INSTANCE_ID
        if not is_manager_role():
            return WORKER_INSTANCE_ID
        active_runners = get_runner_registry_service().list_active_runners(db)
        if not active_runners:
            return None
        runner_ids = [str(item["instance_id"]) for item in active_runners]
        running_counts = TaskRepository.get_running_task_counts_by_instance(db, runner_ids)
        best_runner: dict | None = None
        best_score: tuple[int, float, str] | None = None
        for item in active_runners:
            instance_id = str(item["instance_id"])
            capacity = max(1, int(item.get("capacity") or 1))
            assigned_running = int(running_counts.get(instance_id, 0))
            if assigned_running >= capacity:
                continue
            score = (assigned_running, float(item.get("age_seconds") or 0.0), instance_id)
            if best_score is None or score < best_score:
                best_score = score
                best_runner = item
        return str(best_runner["instance_id"]) if best_runner else None

    def _run_task_locally(self, task_id: str, lease_epoch: int, project_id: str | None = None) -> None:
        spawn_source = "scheduler_callback" if lease_epoch == 0 else "runner_assignment_loop"
        with _running_tasks_guard:
            existing_thread = _running_tasks.get(task_id)
            existing_epoch = _running_task_epochs.get(task_id)
            existing_alive = bool(existing_thread and existing_thread.is_alive())
            if project_id:
                self._record_timeline_event(
                    task_id=task_id,
                    project_id=project_id,
                    event_type="task_local_spawn_started",
                    message="准备在本地拉起任务执行线程",
                    payload={
                        "worker_instance_id": WORKER_INSTANCE_ID,
                        "lease_epoch": lease_epoch,
                        "thread_name": f"sa_task_{task_id}",
                        "old_epoch": existing_epoch,
                        "running_task_known": existing_thread is not None,
                        "thread_alive": existing_alive,
                        "source": spawn_source,
                    },
                )
            if existing_thread is not None:
                if existing_epoch == lease_epoch and existing_alive:
                    if project_id:
                        self._record_timeline_event(
                            task_id=task_id,
                            project_id=project_id,
                            event_type="task_local_spawn_skipped_duplicate",
                            message="检测到相同租约的本地活跃执行线程，跳过重复拉起",
                            level="warning",
                            payload={
                                "worker_instance_id": WORKER_INSTANCE_ID,
                                "lease_epoch": lease_epoch,
                                "thread_name": existing_thread.name,
                                "old_epoch": existing_epoch,
                                "running_task_known": True,
                                "thread_alive": True,
                                "source": spawn_source,
                            },
                        )
                    return
                if existing_epoch != lease_epoch:
                    logger.warning(
                        "task %s lease_epoch changed (%s -> %s), removing stale execution to allow restart",
                        task_id, existing_epoch, lease_epoch,
                    )
                    if project_id:
                        self._record_timeline_event(
                            task_id=task_id,
                            project_id=project_id,
                            event_type="task_local_spawn_replaced_stale_epoch",
                            message="检测到本地旧租约记录，已替换为新的执行租约",
                            level="warning",
                            payload={
                                "worker_instance_id": WORKER_INSTANCE_ID,
                                "lease_epoch": lease_epoch,
                                "thread_name": existing_thread.name,
                                "old_epoch": existing_epoch,
                                "running_task_known": True,
                                "thread_alive": existing_alive,
                                "source": spawn_source,
                            },
                        )
                _running_tasks.pop(task_id, None)
                _running_task_epochs.pop(task_id, None)
            task_thread = threading.Thread(
                target=self._runner.execute_task,
                args=(task_id, lease_epoch),
                name=f"sa_task_{task_id}",
                daemon=True,
            )
            _running_tasks[task_id] = task_thread
            _running_task_epochs[task_id] = lease_epoch
        task_thread.start()
        if project_id:
            self._record_timeline_event(
                task_id=task_id,
                project_id=project_id,
                event_type="task_local_spawn_registered",
                message="本地执行线程已注册并启动",
                payload={
                    "worker_instance_id": WORKER_INSTANCE_ID,
                    "lease_epoch": lease_epoch,
                    "thread_name": task_thread.name,
                    "old_epoch": existing_epoch,
                    "running_task_known": existing_thread is not None,
                    "thread_alive": bool(existing_thread and existing_thread.is_alive()),
                    "source": spawn_source,
                },
            )

    def _start_runner_assignment_loop(self) -> None:
        if self._runner_assignment_task and self._runner_assignment_task.is_alive():
            _runner_assignment_runtime_state.loop_running = True
            _runner_assignment_runtime_state.thread_alive = True
            logger.warning(
                "runner assignment loop already running: worker_instance_id=%s thread_name=%s",
                WORKER_INSTANCE_ID,
                self._runner_assignment_task.name,
            )
            return
        self._runner_assignment_loop_running = True
        self._runner_assignment_task = threading.Thread(
            target=self._runner_assignment_loop,
            name="sa_runner_assignment_loop",
            daemon=True,
        )
        self._runner_assignment_task.start()
        _runner_assignment_runtime_state.loop_running = True
        _runner_assignment_runtime_state.thread_alive = True
        _runner_assignment_runtime_state.loop_start_count += 1
        logger.info(
            "runner assignment loop started: worker_instance_id=%s thread_name=%s loop_start_count=%s",
            WORKER_INSTANCE_ID,
            self._runner_assignment_task.name,
            _runner_assignment_runtime_state.loop_start_count,
        )

    def _stop_runner_assignment_loop(self) -> None:
        self._runner_assignment_loop_running = False
        _runner_assignment_runtime_state.loop_running = False
        task = self._runner_assignment_task
        if task and task.is_alive():
            _runner_assignment_runtime_state.thread_alive = True
            task.join(timeout=1.0)
        _runner_assignment_runtime_state.thread_alive = False
        self._runner_assignment_task = None

    def _runner_assignment_loop(self) -> None:
        while self._runner_assignment_loop_running:
            try:
                _runner_assignment_runtime_state.last_tick_ts = _time.time()
                self._poll_runner_assignments_once()
                _runner_assignment_runtime_state.last_success_ts = _time.time()
                _runner_assignment_runtime_state.last_error = None
            except Exception as exc:
                _runner_assignment_runtime_state.last_error = str(exc)
                logger.warning("runner assignment loop failed: %s", exc, exc_info=True)
            time.sleep(RUNNER_ASSIGNMENT_POLL_INTERVAL_SECONDS)
        _runner_assignment_runtime_state.loop_running = False
        _runner_assignment_runtime_state.thread_alive = False

    def _poll_runner_assignments_once(self) -> None:
        db_gen = self._runner._deps.get_db()
        db: Session = next(db_gen)
        try:
            current_concurrency = 1
            available_slots = max(0, current_concurrency - _running_tasks_count())
            if available_slots <= 0:
                _runner_assignment_runtime_state.last_rows_seen = 0
                return
            rows = self._task_repository.list_tasks_assigned_to_instance(
                db,
                instance_id=WORKER_INSTANCE_ID,
                limit=available_slots,
            )
            _runner_assignment_runtime_state.last_rows_seen = len(rows)
            now = now_local()
            for row in rows:
                if row.task_id in _running_tasks:
                    old_epoch = _running_task_epochs.get(row.task_id)
                    if old_epoch == int(row.lease_epoch or 0) and _running_tasks[row.task_id].is_alive():
                        continue
                if row.lease_expires_at and row.lease_expires_at < now:
                    lease_epoch = int(row.lease_epoch or 0)
                    lease_expires_at = _safe_isoformat(row.lease_expires_at)
                    _runner_assignment_runtime_state.last_skipped_expired_task_id = row.task_id
                    _runner_assignment_runtime_state.last_skipped_expired_lease_epoch = lease_epoch
                    _runner_assignment_runtime_state.last_skipped_expired_lease_expires_at = lease_expires_at
                    logger.warning(
                        "runner assignment skipped expired lease task_id=%s lease_epoch=%s lease_expires_at=%s now=%s worker_instance_id=%s",
                        row.task_id,
                        lease_epoch,
                        lease_expires_at,
                        isoformat_local(now),
                        WORKER_INSTANCE_ID,
                    )
                    self._record_timeline_event(
                        task_id=row.task_id,
                        project_id=row.project_id,
                        event_type="runner_assignment_skipped_expired_lease",
                        message="Runner 发现任务租约已过期，跳过本地执行",
                        level="warning",
                        payload={
                            "runner_instance_id": WORKER_INSTANCE_ID,
                            "lease_epoch": lease_epoch,
                            "lease_expires_at": lease_expires_at,
                            "runner_now": isoformat_local(now),
                        },
                    )
                    continue
                lease_epoch = int(row.lease_epoch or 0)
                self._run_task_locally(row.task_id, lease_epoch, row.project_id)
                _runner_assignment_runtime_state.last_spawned_task_id = row.task_id
                _runner_assignment_runtime_state.last_spawned_lease_epoch = lease_epoch
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    @staticmethod
    def _remove_running_task(task_id: str) -> None:
        with _running_tasks_guard:
            _running_tasks.pop(task_id, None)
            _running_task_epochs.pop(task_id, None)

    @staticmethod
    def _acquire_execution_lock(
        db: Session,
        output_path: str | None,
        task_id: str,
        lease_epoch: int,
        observer: Callable[[str, dict[str, object]], None] | None = None,
    ) -> Path | None:
        lock_path = _task_execution_lock_path(output_path, task_id)
        if not lock_path:
            return None
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        runner_identity = current_runner_lock_identity()
        payload = {
            "task_id": task_id,
            "lease_epoch": lease_epoch,
            "acquired_at": isoformat_local(now_local()),
            **runner_identity,
        }
        for attempt in range(2):
            try:
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            except FileExistsError:
                row = db.query(AppSaTask).filter_by(task_id=task_id).first()
                lock_payload = _read_task_execution_lock_payload(lock_path)
                lock_worker = _coerce_lock_text((lock_payload or {}).get("worker_instance_id"))
                lock_epoch = _coerce_lock_epoch((lock_payload or {}).get("lease_epoch"))
                lock_boot_id = _coerce_lock_text((lock_payload or {}).get("runner_boot_id"))
                lock_process_token = _coerce_lock_text((lock_payload or {}).get("runner_process_token"))
                lock_process_started_at = _coerce_lock_text((lock_payload or {}).get("runner_process_started_at"))
                lock_main_pid = (lock_payload or {}).get("runner_main_pid")
                row_worker = str(getattr(row, "dispatcher_instance_id", "") or "").strip()
                row_epoch = int(getattr(row, "lease_epoch", 0) or 0)
                row_status = str(getattr(row, "status", "") or "").strip()
                row_lease_expires_at = getattr(row, "lease_expires_at", None)
                requested_epoch = int(lease_epoch or 0)
                current_worker = str(runner_identity["worker_instance_id"])
                current_boot_id = str(runner_identity["runner_boot_id"])
                current_process_token = str(runner_identity["runner_process_token"])
                same_process_instance = (
                    lock_worker == current_worker
                    and bool(lock_process_token)
                    and lock_process_token == current_process_token
                )
                stale_reasons: list[str] = []
                if row is None:
                    stale_reasons.append("task_row_missing")
                if row_status != "running":
                    stale_reasons.append("row_not_running")
                if row_lease_expires_at is not None and row_lease_expires_at < now_local():
                    stale_reasons.append("lease_expired")
                if lock_epoch is None:
                    stale_reasons.append("lock_epoch_missing")
                elif lock_epoch != row_epoch:
                    stale_reasons.append("lock_epoch_mismatch_row")
                if row_epoch != requested_epoch:
                    stale_reasons.append("requested_epoch_mismatch_row")
                if row_worker != current_worker:
                    stale_reasons.append("row_worker_mismatch_current")
                if lock_worker and row_worker and lock_worker != row_worker:
                    stale_reasons.append("lock_worker_mismatch_row")
                if not lock_process_token:
                    stale_reasons.append("legacy_lock_missing_process_token")
                elif lock_process_token != current_process_token:
                    stale_reasons.append("runner_process_token_mismatch")
                if not lock_boot_id:
                    stale_reasons.append("legacy_lock_missing_boot_id")
                elif lock_boot_id != current_boot_id:
                    stale_reasons.append("runner_boot_id_mismatch")
                lock_is_stale = (
                    row is None
                    or bool(stale_reasons)
                )
                debug_payload = {
                    "task_id": task_id,
                    "lock_path": str(lock_path),
                    "requested_worker_instance_id": current_worker,
                    "requested_lease_epoch": requested_epoch,
                    "requested_runner_boot_id": current_boot_id,
                    "requested_runner_process_token": current_process_token,
                    "lock_worker_instance_id": lock_worker or None,
                    "lock_lease_epoch": lock_epoch,
                    "lock_runner_boot_id": lock_boot_id or None,
                    "lock_runner_process_token": lock_process_token or None,
                    "lock_runner_process_started_at": lock_process_started_at or None,
                    "lock_runner_main_pid": lock_main_pid,
                    "row_status": row_status or None,
                    "row_worker_instance_id": row_worker or None,
                    "row_lease_epoch": row_epoch,
                    "row_lease_expires_at": isoformat_local(row_lease_expires_at) if row_lease_expires_at else None,
                }
                if lock_is_stale and attempt == 0:
                    if callable(observer):
                        observer(
                            "task_execution_lock_stale_detected",
                            {
                                **debug_payload,
                                "decision": "stale_detected",
                                "stale_reasons": list(stale_reasons),
                            },
                        )
                    logger.warning(
                        "detected stale task execution lock; clearing and retrying: task_id=%s lock_path=%s "
                        "lock_worker_instance_id=%s lock_lease_epoch=%s row_status=%s row_worker_instance_id=%s "
                        "row_lease_epoch=%s row_lease_expires_at=%s requested_lease_epoch=%s requested_worker_instance_id=%s "
                        "lock_runner_boot_id=%s lock_runner_process_token=%s requested_runner_boot_id=%s "
                        "requested_runner_process_token=%s stale_reasons=%s",
                        task_id,
                        lock_path,
                        lock_worker or "-",
                        lock_epoch if lock_epoch is not None else "-",
                        row_status or "-",
                        row_worker or "-",
                        row_epoch,
                        isoformat_local(row_lease_expires_at) if row_lease_expires_at else "-",
                        lease_epoch,
                        current_worker,
                        lock_boot_id or "-",
                        lock_process_token or "-",
                        current_boot_id,
                        current_process_token,
                        ",".join(stale_reasons) or "-",
                    )
                    _clear_task_execution_lock(output_path, task_id)
                    if callable(observer):
                        observer(
                            "task_execution_lock_cleared",
                            {
                                **debug_payload,
                                "decision": "stale_cleared",
                                "stale_reasons": list(stale_reasons),
                            },
                        )
                    continue
                conflict_kind = "execution_lock_reentry" if same_process_instance else "execution_lock_conflict"
                conflict_payload = {
                    **debug_payload,
                    "decision": "reentry" if same_process_instance else "active_conflict",
                }
                if callable(observer):
                    observer(
                        "task_execution_lock_reentry_blocked" if same_process_instance else "task_execution_lock_conflict",
                        conflict_payload,
                    )
                raise TaskExecutionLockConflict(
                    "task execution lock already exists: "
                    f"{lock_path} "
                    f"(lock_worker_instance_id={lock_worker or '-'}, "
                    f"lock_lease_epoch={lock_epoch if lock_epoch is not None else '-'}, "
                    f"lock_runner_boot_id={lock_boot_id or '-'}, "
                    f"lock_runner_process_token={lock_process_token or '-'}, "
                    f"row_status={row_status or '-'}, "
                    f"row_worker_instance_id={row_worker or '-'}, "
                    f"row_lease_epoch={row_epoch}, "
                    f"row_lease_expires_at={isoformat_local(row_lease_expires_at) if row_lease_expires_at else '-'})",
                    conflict_kind=conflict_kind,
                    payload=conflict_payload,
                )
            try:
                os.write(fd, json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))
            finally:
                os.close(fd)
            if callable(observer):
                observer(
                    "task_execution_lock_acquired" if attempt == 0 else "task_execution_lock_reacquired",
                    {
                        "task_id": task_id,
                        "lock_path": str(lock_path),
                        "requested_worker_instance_id": str(runner_identity["worker_instance_id"]),
                        "requested_lease_epoch": int(lease_epoch or 0),
                        "requested_runner_boot_id": str(runner_identity["runner_boot_id"]),
                        "requested_runner_process_token": str(runner_identity["runner_process_token"]),
                        "decision": "acquired" if attempt == 0 else "continued",
                    },
                )
            return lock_path
        raise RuntimeError(f"failed to acquire task execution lock after stale-lock cleanup retry: {lock_path}")

    def delete_task(self, db: Session, task_id: str, *, delete_files: bool = True) -> dict:
        """无条件删除任务（软删除）。运行中/排队中的任务先自动取消再删除。

        返回 {cancelled_first: bool, files_deleted: bool}，供调用方（API 层）
        在 cancelled_first=True 时通知调度器 kill 实际执行进程。
        """
        import shutil as _shutil
        row = self._get_or_404(db, task_id)
        previous_status = str(row.status or "")
        task_dir = os.path.join(row.output_path, task_id) if row.output_path else ""
        files_deleted = False
        cancelled_first = False
        # 运行中/排队中：先自动取消（DB 置 cancelled + 清 lease），再删除。
        # 实际执行进程的 kill 由 API 层收到 cancelled_first 后通知调度器完成。
        if row.status in ("running", "pending"):
            try:
                self._task_repository.cancel_task_in_place(db, row)
                cancelled_first = True
                self._record_task_operation_event(
                    task_id=row.task_id,
                    project_id=row.project_id,
                    operation="delete_task",
                    event_type="task_auto_cancelled_before_delete",
                    message=f"删除触发自动取消（原状态 {previous_status}）",
                    level="warning",
                    payload={
                        "before_status": previous_status,
                        "after_status": "cancelled",
                        "changed": True,
                        "reason": "auto_cancel_on_delete",
                    },
                )
            except Exception:
                logger.exception("delete_task: auto-cancel failed for %s", task_id)
        # 删除输出文件
        if delete_files and row.output_path:
            if os.path.isdir(task_dir):
                try:
                    _shutil.rmtree(task_dir)
                    files_deleted = True
                    logger.info("delete_task: removed task dir %s", task_dir)
                except Exception as _e:
                    logger.warning("delete_task: failed to remove %s: %s", task_dir, _e)
        # 软删除
        row.is_deleted = True
        db.commit()
        _invalidate_slot_summary_cache(row.project_id)
        self._record_task_operation_event(
            task_id=row.task_id,
            project_id=row.project_id,
            operation="delete_task",
            event_type="task_deleted",
            message="任务已删除",
            level="warning",
            payload={
                "before_status": previous_status,
                "after_status": previous_status,
                "changed": True,
                "delete_files": delete_files,
                "task_dir": task_dir or None,
                "files_deleted": files_deleted,
                "status_before_delete": previous_status,
                "cancelled_first": cancelled_first,
            },
        )
        return {"cancelled_first": cancelled_first, "files_deleted": files_deleted}

    def _execute_task(self, task_id: str) -> None:
        from app.db import get_db
        db_gen = get_db()
        db: Session = next(db_gen)
        event_buffer: list[dict] = []

        def on_event(event: SwarmEvent) -> None:
            event_buffer.append({"ts": _time.time(), "type": event.type,
                                  "data": dict(event.data)})
            n = len(event_buffer)
            if n == 1 or n % 3 == 0:
                _flush_stages(task_id, event_buffer)

        try:
            row = db.query(AppSaTask).filter_by(task_id=task_id).first()
            if not row or row.status == "cancelled":
                return
            row.status = "running"
            # 续跑时保留原始 started_at，首次运行才设置
            if row.started_at is None:
                row.started_at = now_local()
            db.commit()
            _invalidate_slot_summary_cache(row.project_id)
            svc = _load_svc_config_from_db(db, row.project_id)
            # Apply per-task config overrides (analyse_targets, binary_arch, etc.)
            tcfg = row.task_config_json or {}
            if tcfg.get("analyse_targets"):
                svc.analyse_targets = tcfg["analyse_targets"]
            elif _infer_analysis_mode(row) == ANALYSIS_MODE_SOURCE:
                svc.analyse_targets = list(SOURCE_MODE_DEFAULT_ANALYSE_TARGETS)
            if tcfg.get("binary_arch"):
                svc.binary_arch = tcfg["binary_arch"]
            # [修复] security_focus_categories 和 module_granularity 需要同样从 task_config_json 覆盖到 svc，
            # 原先遗漏导致这两个配置项始终无法生效。
            # 注意：security_focus_categories 用 is not None 而非 bool，因为 ["all"] 也是有效配置。
            if tcfg.get("security_focus_categories") is not None:
                svc.security_focus_categories = tcfg["security_focus_categories"]
            if tcfg.get("module_granularity"):
                svc.module_granularity = tcfg["module_granularity"]
            if tcfg.get("filter_engine"):
                svc.filter_engine = tcfg["filter_engine"]
            if "enable_final_check" in tcfg:
                svc.enable_final_check = bool(tcfg["enable_final_check"])
            if "continue_on_module_failure" in tcfg:
                svc.continue_on_module_failure = bool(tcfg["continue_on_module_failure"])
            # 断点续跑由文件系统 .checkpoint/ 目录驱动，
            # 不再从 task_config_json 读取 start_stage/resume_workspace。
            # Use row.output_path as the working root so the Orchestrator writes to
            # the user-specified location ({output_path}/{task_id}/workspace/) rather
            # than the global /data/output directory from config.json.
            if row.output_path:
                svc.output_dir = row.output_path
                svc.archive_dir = row.output_path
                svc.result_dir = row.output_path
            cfg = build_task_config(svc, row.prompt_content, cwd=row.input_path)
            orch = Orchestrator(config=cfg, on_event=on_event)
            result = orch.execute(task_id)
            _flush_stages(task_id, event_buffer)
            db.expire(row); db.refresh(row)
            if row.status == "cancelled":
                reason, changed = _sync_task_abnormal_reason(row)
                _record_abnormal_reason(row, reason, changed=changed)
                db.commit()
                _invalidate_slot_summary_cache(row.project_id)
                return
            row.status = result.status.value if result else "error"
            row.finished_at = now_local()
            # 合并历史事件（续跑场景保留前序阶段记录）
            _prev = row.stages_json
            _prev_events = _prev["events"] if isinstance(_prev, dict) and isinstance(_prev.get("events"), list) else []
            row.stages_json = {"events": _prev_events + event_buffer, "final": True}
            if result:
                result_payload = result.model_dump(mode="json")
                result_file = _write_task_result_json(row, result_payload)
                row.result_json = _lightweight_result_json(row, result_payload, result_file)
                if result.error:
                    row.error = result.error
            reason, changed = _sync_task_abnormal_reason(row)
            _record_abnormal_reason(row, reason, changed=changed)
            db.commit()
            _invalidate_slot_summary_cache(row.project_id)
            # —— 自省分析（异步后台，不阻塞任务完成） ——
            try:
                from app.pipeline.self_reflection import get_self_reflection_service
                _sr_run_dir = Path(row.output_path or "") / row.task_id / "run" if row.output_path else None
                _sr_out_dir = Path(row.output_path or "") / row.task_id / "output" if row.output_path else None
                _sr_status = result.status.value if result else "error"
                if _sr_run_dir and _sr_out_dir:
                    get_self_reflection_service().trigger_async(
                        task_id=task_id,
                        run_dir=_sr_run_dir,
                        output_dir=_sr_out_dir,
                        cfg=cfg,
                        task_status=_sr_status,
                    )
            except Exception as _sr_exc:
                logger.warning("self-reflection trigger failed: %s", _sr_exc)
        except Exception:
            import traceback
            traceback.print_exc()
            pass
        except Exception as exc:
            log_event(logger, logging.ERROR, "task execution failed",
                      event="task_error", task_id=task_id, error=str(exc))
            try:
                db.rollback()
                r = db.query(AppSaTask).filter_by(task_id=task_id).first()
                if r and r.status == "running":
                    r.status = "error"
                    r.error = str(exc)
                    r.finished_at = now_local()
                    _prev2 = r.stages_json
                    _prev_events2 = _prev2["events"] if isinstance(_prev2, dict) and isinstance(_prev2.get("events"), list) else []
                    r.stages_json = {"events": _prev_events2 + event_buffer, "final": True}
                    reason, changed = _sync_task_abnormal_reason(r)
                    _record_abnormal_reason(r, reason, changed=changed)
                    db.commit()
                    _invalidate_slot_summary_cache(r.project_id)
            except Exception:
                import traceback
                traceback.print_exc()
                pass
        finally:
            _running_tasks.pop(task_id, None)
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _get_or_404(self, db: Session, task_id: str) -> AppSaTask:
        row = self._task_repository.get_task_not_deleted(db, task_id)
        if not row:
            from fastapi import HTTPException
            raise HTTPException(404, f"任务不存在: {task_id}")
        return row

    @staticmethod
    def _record_timeline_event(
        *,
        task_id: str,
        project_id: str | None,
        event_type: str,
        message: str,
        level: str = "info",
        stage_name: str | None = None,
        payload: dict | None = None,
        created_at: datetime | None = None,
    ) -> None:
        if not task_id or not project_id or not event_type or not message:
            return
        sanitized_payload = _sanitize_timeline_payload(payload)
        sanitized_payload = _merge_timeline_recorder_payload(sanitized_payload)
        db_gen = None
        db: Session | None = None
        try:
            from app.db import get_db as _get_db

            db_gen = _get_db()
            db = next(db_gen)
            event = AppSaTaskEvent(
                id=f"sae_{uuid.uuid4().hex[:24]}",
                task_id=task_id,
                project_id=project_id,
                stage_name=str(stage_name).strip() or None if stage_name is not None else None,
                level=str(level or "info").strip() or "info",
                event_type=str(event_type).strip(),
                message=str(message).strip(),
                payload_json=sanitized_payload,
                created_at=created_at or now_local(),
            )
            db.add(event)
            db.commit()
        except Exception:
            import traceback
            traceback.print_exc()
            try:
                if db is not None:
                    db.rollback()
            except Exception:
                import traceback
                traceback.print_exc()
                pass
        finally:
            if db_gen is not None:
                try:
                    next(db_gen)
                except StopIteration:
                    pass

    @staticmethod
    @staticmethod
    def _list_load_options():
        return (
            load_only(
                AppSaTask.id,
                AppSaTask.task_id,
                AppSaTask.project_id,
                AppSaTask.task_origin_type,
                AppSaTask.analysis_mode,
                AppSaTask.parent_project_id,
                AppSaTask.parent_task_id,
                AppSaTask.parent_task_type,
                AppSaTask.parent_stage_name,
                AppSaTask.parent_stage_item_id,
                AppSaTask.parent_stage_item_key,
                AppSaTask.task_name,
                AppSaTask.task_description,
                AppSaTask.input_path,
                AppSaTask.output_path,
                AppSaTask.prompt_template_id,
                AppSaTask.status,
                AppSaTask.error,
                AppSaTask.created_by,
                AppSaTask.created_at,
                AppSaTask.updated_at,
                AppSaTask.started_at,
                AppSaTask.finished_at,
                AppSaTask.dispatcher_instance_id,
                AppSaTask.dispatch_started_at,
                AppSaTask.lease_epoch,
                AppSaTask.lease_expires_at,
                AppSaTask.latest_abnormal_reason_json,
            ),
        )

    @staticmethod
    def _row_to_list_item(row: AppSaTask) -> dict:
        def fmt(dt: datetime | None) -> str | None:
            return isoformat_local(dt)

        analysis_mode = _normalize_analysis_mode(row.analysis_mode)
        abnormal_reason = _lightweight_task_abnormal_reason(row)
        return {
            "task_id": row.task_id,
            "project_id": row.project_id,
            **_origin_payload(row),
            "analysis_mode": analysis_mode,
            "analysis_mode_label": _analysis_mode_label(analysis_mode),
            "task_name": row.task_name,
            "status": row.status,
            "created_at": fmt(row.created_at),
            "updated_at": fmt(row.updated_at),
            "started_at": fmt(row.started_at),
            "finished_at": fmt(row.finished_at),
            "dispatcher_instance_id": row.dispatcher_instance_id,
            "dispatch_started_at": fmt(row.dispatch_started_at),
            "lease_epoch": int(row.lease_epoch or 0),
            "lease_expires_at": fmt(row.lease_expires_at),
            "abnormal_reason": abnormal_reason,
            "abnormal_reason_title": (abnormal_reason or {}).get("title"),
            "abnormal_reason_code": (abnormal_reason or {}).get("code"),
            "abnormal_reason_category": (abnormal_reason or {}).get("category"),
        }

    @staticmethod
    def _row_to_dict(row: AppSaTask, *, include_heavy: bool = True) -> dict:
        def fmt(dt: datetime | None) -> str | None:
            return isoformat_local(dt)
        analysis_mode = _infer_analysis_mode(row, include_config=include_heavy)
        abnormal_reason = _task_abnormal_reason(row) if include_heavy else _lightweight_task_abnormal_reason(row)
        task_root = str(Path(row.output_path) / row.task_id) if row.output_path else None
        run_root = str(Path(task_root) / "run") if task_root else None
        workspace_root = str(Path(run_root) / "workspace") if run_root else None
        output_root = str(Path(task_root) / "output") if task_root else None
        return {
            "task_id": row.task_id, "project_id": row.project_id,
            **_origin_payload(row),
            "analysis_mode": analysis_mode,
            "analysis_mode_label": _analysis_mode_label(analysis_mode),
            "task_name": row.task_name, "task_description": row.task_description,
            "input_path": row.input_path, "output_path": row.output_path,
            "task_root": task_root,
            "run_root": run_root,
            "workspace_root": workspace_root,
            "output_root": output_root,
            "prompt_template_id": row.prompt_template_id,
            "prompt_content": row.prompt_content if include_heavy else None, "status": row.status,
            "error": row.error,
            "result_json": _lightweight_result_json(row, row.result_json) if include_heavy else None,
            "stages_json": _build_lightweight_stages_payload(row) if include_heavy else None,
            "task_config_json": row.task_config_json if include_heavy else None,
            "created_by": row.created_by,
            "created_at": fmt(row.created_at), "updated_at": fmt(row.updated_at),
            "started_at": fmt(row.started_at), "finished_at": fmt(row.finished_at),
            "dispatcher_instance_id": row.dispatcher_instance_id,
            "dispatch_started_at": fmt(row.dispatch_started_at),
            "lease_epoch": int(row.lease_epoch or 0),
            "lease_expires_at": fmt(row.lease_expires_at),
            "abnormal_reason": abnormal_reason,
            "abnormal_reason_history": _abnormal_reason_history(row) if include_heavy else [],
            "abnormal_reason_title": (abnormal_reason or {}).get("title"),
            "abnormal_reason_code": (abnormal_reason or {}).get("code"),
            "abnormal_reason_category": (abnormal_reason or {}).get("category"),
            **_agent_runtime_payload(row),
        }


_task_service: TaskService | None = None


def get_task_service() -> TaskService:
    global _task_service
    if _task_service is None:
        _task_service = TaskService()
    return _task_service
