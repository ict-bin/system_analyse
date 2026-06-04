from __future__ import annotations

import contextlib
from datetime import datetime
from datetime import timedelta
import json
import os
import pathlib
import shlex
import signal
import time
from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from app.db.models import AppSaTask
from app.service.agent_runtime_registry import build_runtime_registration_key, get_agent_runtime_snapshot
from app.service.worker_slot_snapshot import build_worker_slot_cluster_snapshot
from app.service.event_log import events_path, read_events
from app.service.session_index import build_session_catalog

POD_NAME = (
    os.environ.get("SA_POD_NAME")
    or os.environ.get("POD_NAME")
    or os.environ.get("HOSTNAME")
    or "system-analyse-pod"
)
_RUNTIME_ACTIVITY_STALE_SECONDS = max(
    30,
    int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_RUNTIME_ACTIVITY_STALE_SECONDS", "120")),
)
_ORPHAN_PROTECTION_SECONDS = max(
    60,
    int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_ORPHAN_PROTECTION_SECONDS", "120")),
)
_RUNNING_TASK_STATUSES = {"running", "pending", "queued", "dispatching"}

_SESSION_ARG_KEYS = {
    "--session",
    "--session-file",
    "--session_path",
    "--session-path",
    "--resume",
}
_AGENT_TOKENS: tuple[tuple[str, str], ...] = (
    ("claude-code", "claude-code"),
    ("claude", "claude"),
    ("opencode", "opencode"),
    ("codex", "codex"),
    ("npx pi", "pi"),
    (" pi ", "pi"),
    ("/pi", "pi"),
)
_WRAPPER_NAMES = {"node", "npm", "npx", "pnpm", "yarn", "python", "python3", "uv"}


@dataclass
class SaAgentProcessSnapshot:
    pod_name: str
    pid: int
    pgid: int | None
    ppid: int | None
    command: str
    cwd: str | None
    exe: str | None
    rss_bytes: int | None
    runtime_kind: str | None
    match_source: str | None
    match_confidence: str | None
    workspace_root: str | None
    task_id: str | None
    task_name: str | None
    task_status: str | None
    stage_key: str | None
    stage_group: str | None
    family_key: str | None
    parallel_group: str | None
    role_kind: str | None
    owner_kind: str
    owner_reason: str
    runtime_evidence: dict[str, Any]
    lease_state: str | None
    last_runtime_activity_at: str | None
    kill_allowed: bool
    kill_block_reason: str | None
    kill_eligibility_reason: str | None
    termination_state: str


def _read_text(path: pathlib.Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip()
    except Exception:
        return ""


def _normalize_path(raw: str | None) -> str | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        return str(pathlib.Path(value).resolve(strict=False))
    except Exception:
        return value


def _infer_runtime_kind(command: str, exe: str | None) -> str | None:
    normalized = f" {command.lower()} "
    for token, runtime_kind in _AGENT_TOKENS:
        if token in normalized:
            return runtime_kind
    exe_name = pathlib.Path(exe or "").name.lower()
    if exe_name in {"pi", "claude", "claude-code", "codex", "opencode"}:
        return exe_name
    if exe_name in _WRAPPER_NAMES:
        for runtime_name in ("claude-code", "claude", "codex", "opencode", "pi"):
            if runtime_name in normalized:
                return runtime_name
    return None


def _extract_role_kind(command: str) -> str | None:
    lowered = f" {command.lower()} "
    for token in ("judge", "review", "reviewer", "worker", "coder", "analysis", "planner", "critic"):
        if f" {token} " in lowered or f"/{token}" in lowered or f"--{token}" in lowered:
            return token
    return None


def _extract_session_arg_path(command: str) -> str | None:
    with contextlib.suppress(Exception):
        tokens = shlex.split(command)
        for index, token in enumerate(tokens):
            if token in _SESSION_ARG_KEYS and index + 1 < len(tokens):
                return _normalize_path(tokens[index + 1])
            for key in _SESSION_ARG_KEYS:
                prefix = f"{key}="
                if token.startswith(prefix):
                    return _normalize_path(token[len(prefix):])
    return None


def _collect_open_paths(proc_dir: pathlib.Path) -> list[str]:
    rows: list[str] = []
    fd_dir = proc_dir / "fd"
    if not fd_dir.exists():
        return rows
    with contextlib.suppress(Exception):
        for fd_entry in fd_dir.iterdir():
            with contextlib.suppress(Exception):
                normalized = _normalize_path(os.readlink(fd_entry))
                if normalized:
                    rows.append(normalized)
    deduped: list[str] = []
    seen: set[str] = set()
    for item in rows:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _iter_agent_processes() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for proc_dir in pathlib.Path("/proc").iterdir():
        if not proc_dir.name.isdigit():
            continue
        pid = int(proc_dir.name)
        try:
            command = (proc_dir / "cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
        except Exception:
            continue
        exe = None
        with contextlib.suppress(Exception):
            exe = os.readlink(proc_dir / "exe")
        runtime_kind = _infer_runtime_kind(command, exe)
        if runtime_kind is None:
            continue
        ppid = pgid = None
        rss_bytes = None
        cwd = None
        with contextlib.suppress(Exception):
            cwd = os.readlink(proc_dir / "cwd")
        stat_raw = _read_text(proc_dir / "stat").split()
        if len(stat_raw) > 4:
            ppid = int(stat_raw[3])
            pgid = int(stat_raw[4])
        status_raw = _read_text(proc_dir / "status")
        for line in status_raw.splitlines():
            if line.startswith("VmRSS:"):
                parts = line.split()
                if len(parts) >= 2:
                    rss_bytes = int(parts[1]) * 1024
                break
        rows.append({
            "pid": pid,
            "ppid": ppid,
            "pgid": pgid,
            "command": command,
            "cwd": cwd,
            "exe": exe,
            "rss_bytes": rss_bytes,
            "runtime_kind": runtime_kind,
            "session_arg_path": _extract_session_arg_path(command),
            "open_paths": _collect_open_paths(proc_dir),
            "started_at_ts": getattr(proc_dir.stat(), "st_ctime", None),
        })
    return rows


def _task_roots(row: AppSaTask) -> list[str]:
    roots: list[str] = []
    output_root = _normalize_path(getattr(row, "output_path", None))
    task_id = str(getattr(row, "task_id", "") or "").strip()
    if output_root and task_id:
        roots.extend(
            [
                os.path.join(output_root, task_id),
                os.path.join(output_root, task_id, "run"),
                os.path.join(output_root, task_id, "output"),
            ]
        )
    for item in [getattr(row, "input_path", None), output_root]:
        normalized = _normalize_path(item)
        if normalized:
            roots.append(normalized)
    deduped: list[str] = []
    seen: set[str] = set()
    for item in roots:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _path_belongs_to_root(path_value: str | None, root: str | None) -> bool:
    if not path_value or not root:
        return False
    try:
        pathlib.Path(path_value).relative_to(pathlib.Path(root))
        return True
    except Exception:
        return False


def _belongs_to_any_root(proc: dict[str, Any], root: str) -> bool:
    candidates = [
        _normalize_path(proc.get("cwd")),
        _normalize_path(proc.get("exe")),
        _normalize_path(proc.get("session_arg_path")),
    ]
    candidates.extend(_normalize_path(item) for item in (proc.get("open_paths") or []))
    command = str(proc.get("command") or "")
    for candidate in candidates:
        if _path_belongs_to_root(candidate, root):
            return True
    return bool(root and root in command)


def _task_sort_key(row: AppSaTask) -> tuple[int, float]:
    status = str(getattr(row, "status", "") or "").strip().lower()
    status_rank = 2 if status == "running" else 1 if status else 0
    updated_at = getattr(row, "updated_at", None)
    updated_ts = updated_at.timestamp() if isinstance(updated_at, datetime) else 0.0
    return status_rank, updated_ts


def _path_mtime(path_value: str | None) -> datetime | None:
    normalized = _normalize_path(path_value)
    if not normalized:
        return None
    try:
        return datetime.fromtimestamp(pathlib.Path(normalized).stat().st_mtime)
    except Exception:
        return None


def _read_execution_lock_payload(task_root: str | None) -> dict[str, Any] | None:
    if not task_root:
        return None
    lock_path = pathlib.Path(task_root) / ".task_execution.lock"
    if not lock_path.exists():
        return None
    try:
        payload = json.loads(lock_path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _safe_iso(value: datetime | None) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


def _session_descriptor_map(task_row: AppSaTask) -> dict[str, dict[str, Any]]:
    if not task_row.output_path or not task_row.task_id:
        return {}
    sessions_root = pathlib.Path(task_row.output_path) / task_row.task_id / "run" / "sessions"
    run_root = pathlib.Path(task_row.output_path) / task_row.task_id / "run"
    if not sessions_root.is_dir() or not run_root.is_dir():
        return {}
    try:
        catalog = build_session_catalog(
            task_id=task_row.task_id,
            row_status=str(task_row.status or ""),
            sessions_root=sessions_root,
            run_root=run_root,
            parse_session_jsonl_file=lambda path: _parse_session_jsonl_file(path),
            write_json_atomic=None,
        )
    except Exception:
        return {}
    result: dict[str, dict[str, Any]] = {}
    for node in (catalog.get("index") or {}).get("nodes") or []:
        if not isinstance(node, dict):
            continue
        relative_path = str(node.get("relative_path") or "").strip()
        if not relative_path:
            continue
        result[str((sessions_root / relative_path).resolve(strict=False))] = node
    return result


def _parse_session_jsonl_file(path: pathlib.Path) -> tuple[dict, list[dict], list[str], int]:
    warnings: list[str] = []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception as exc:
        return {}, [], [str(exc)], 0
    session_meta: dict[str, Any] = {}
    events: list[dict[str, Any]] = []
    for line in lines:
        raw = str(line or "").strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        if payload.get("type") == "session" and not session_meta:
            session_meta = dict(payload)
        events.append(payload)
    return session_meta, events, warnings, len(lines)


def build_agent_runtime_evidence(
    *,
    proc: dict[str, Any] | None,
    task_row: AppSaTask | None,
    workspace_root: str | None,
    now: datetime | None = None,
    running_task_registered: bool = False,
    running_task_epoch: int | None = None,
) -> dict[str, Any]:
    checked_at = now or datetime.now()
    process_alive = bool(proc)
    runtime_kind = str((proc or {}).get("runtime_kind") or "").strip() or None
    session_path = _normalize_path((proc or {}).get("session_arg_path"))
    runtime_registry = get_agent_runtime_snapshot()
    runtime_key = build_runtime_registration_key(
        session_file=session_path,
        cwd=str((proc or {}).get("cwd") or ""),
        command=str((proc or {}).get("command") or ""),
    )
    runtime_registration = runtime_registry.get(runtime_key or "")
    session_activity_at = _path_mtime(session_path)
    events_activity_at = None
    execution_lock_present = False
    execution_lock_matches = False
    task_root = None
    if task_row is not None and task_row.output_path and task_row.task_id:
        task_root = str(pathlib.Path(task_row.output_path) / task_row.task_id)
        events_file = events_path(task_row.output_path, task_row.task_id)
        if events_file is not None and events_file.exists():
            events_activity_at = _path_mtime(str(events_file))
        lock_payload = _read_execution_lock_payload(task_root)
        if lock_payload:
            execution_lock_present = True
            execution_lock_matches = (
                str(lock_payload.get("worker_instance_id") or "").strip() == str(getattr(task_row, "dispatcher_instance_id", "") or "").strip()
                and int(lock_payload.get("lease_epoch") or 0) == int(getattr(task_row, "lease_epoch", 0) or 0)
            )
    activity_candidates = [item for item in [session_activity_at, events_activity_at] if item is not None]
    last_runtime_activity_at = max(activity_candidates) if activity_candidates else None
    recent_runtime_activity = bool(
        last_runtime_activity_at
        and (checked_at - last_runtime_activity_at) <= timedelta(seconds=_RUNTIME_ACTIVITY_STALE_SECONDS)
    )
    live_runtime_evidence = bool(
        process_alive
        and (
            recent_runtime_activity
            or execution_lock_matches
            or running_task_registered
            or runtime_registration is not None
        )
    )
    no_runtime_evidence = not any(
        (
            process_alive,
            recent_runtime_activity,
            execution_lock_present,
            running_task_registered,
            runtime_registration is not None,
        )
    )
    return {
        "checked_at": _safe_iso(checked_at),
        "process_alive": process_alive,
        "runtime_kind": runtime_kind,
        "session_activity_at": _safe_iso(session_activity_at),
        "events_activity_at": _safe_iso(events_activity_at),
        "last_runtime_activity_at": _safe_iso(last_runtime_activity_at),
        "recent_runtime_activity": recent_runtime_activity,
        "execution_lock_present": execution_lock_present,
        "execution_lock_matches": execution_lock_matches,
        "running_task_registered": running_task_registered,
        "running_task_epoch": running_task_epoch,
        "runtime_registration": dict(runtime_registration) if isinstance(runtime_registration, dict) else None,
        "workspace_root": workspace_root,
        "task_root": task_root,
        "live_runtime_evidence": live_runtime_evidence,
        "no_runtime_evidence": no_runtime_evidence,
        "orphan_protection_seconds": _ORPHAN_PROTECTION_SECONDS,
    }


def _match_task(proc: dict[str, Any], task_rows: list[AppSaTask], task_roots_by_id: dict[str, list[str]]) -> tuple[str | None, str | None, str | None]:
    matches: list[tuple[tuple[int, int, int, float], str, str, str]] = []

    def _record_matches(path_value: str | None, source: str, source_rank: int) -> None:
        normalized_path = _normalize_path(path_value)
        if not normalized_path:
            return
        for row in task_rows:
            task_id = str(row.task_id or "")
            status_rank, updated_ts = _task_sort_key(row)
            for root in task_roots_by_id.get(task_id, []):
                if _path_belongs_to_root(normalized_path, root):
                    matches.append(((source_rank, len(root), status_rank, updated_ts), task_id, source, root))

    session_arg_path = _normalize_path(proc.get("session_arg_path"))
    _record_matches(session_arg_path, "session_arg_path", 3)
    cwd = _normalize_path(proc.get("cwd"))
    _record_matches(cwd, "cwd", 2)
    for row in task_rows:
        task_id = str(row.task_id or "")
        status_rank, updated_ts = _task_sort_key(row)
        for root in task_roots_by_id.get(task_id, []):
            if _belongs_to_any_root(proc, root):
                matches.append(((1, len(root), status_rank, updated_ts), task_id, "task_root", root))
    if matches:
        _, task_id, match_source, workspace_root = max(matches, key=lambda item: item[0])
        return task_id, match_source, workspace_root
    return None, None, None


class AgentObservabilityService:
    def build_snapshot_for_processes(self, processes_source: list[dict[str, Any]]) -> dict[str, Any]:
        from app.db import get_db

        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            return self._build_snapshot_from_processes(db, processes_source)
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def build_snapshot(self, db: Session, *, project_id: str | None = None) -> dict[str, Any]:
        return self._build_snapshot_from_processes(db, list(_iter_agent_processes()), project_id=project_id)

    def _build_snapshot_from_processes(
        self,
        db: Session,
        processes_source: list[dict[str, Any]],
        *,
        project_id: str | None = None,
    ) -> dict[str, Any]:
        from app.service.task_service import get_runtime_tracking_snapshot

        query = db.query(AppSaTask).filter(AppSaTask.is_deleted.is_(False))
        if project_id:
            query = query.filter(AppSaTask.project_id == project_id)
        task_rows = query.all()
        task_by_id = {row.task_id: row for row in task_rows}
        task_roots_by_id = {row.task_id: _task_roots(row) for row in task_rows}
        session_descriptors_by_task_id = {
            row.task_id: _session_descriptor_map(row)
            for row in task_rows
        }
        runtime_tracking = get_runtime_tracking_snapshot()
        try:
            build_worker_slot_cluster_snapshot(db, project_id=project_id)
        except Exception:
            pass

        processes: list[dict[str, Any]] = []
        for proc in processes_source:
            task_id, match_source, workspace_root = _match_task(proc, task_rows, task_roots_by_id)
            task_row = task_by_id.get(task_id or "")
            task_name = task_row.task_name if task_row is not None else None
            task_status = str(task_row.status or "") if task_row is not None else None
            role_kind = _extract_role_kind(str(proc.get("command") or ""))
            runtime_registered = task_id in runtime_tracking
            runtime_epoch = runtime_tracking.get(task_id)
            runtime_evidence = build_agent_runtime_evidence(
                proc=proc,
                task_row=task_row,
                workspace_root=workspace_root,
                now=datetime.now(),
                running_task_registered=runtime_registered,
                running_task_epoch=runtime_epoch,
            )
            session_descriptor = session_descriptors_by_task_id.get(task_id or "", {}).get(
                _normalize_path(proc.get("session_arg_path")) or ""
            )
            lease_state = None
            if task_row is not None:
                if str(task_row.status or "").strip() == "running":
                    lease_state = "running"
                elif task_row.lease_expires_at is not None:
                    lease_state = "expired"
                else:
                    lease_state = "released"
            started_at_ts = proc.get("started_at_ts")
            process_age_seconds: int | None = None
            try:
                if started_at_ts is not None:
                    process_age_seconds = max(0, int(time.time() - float(started_at_ts)))
            except Exception:
                process_age_seconds = None
            if task_row is not None and str(task_status or "").strip() in _RUNNING_TASK_STATUSES and runtime_evidence["live_runtime_evidence"]:
                owner_kind = "tracked"
                owner_reason = "active_task_with_runtime_evidence"
                kill_allowed = False
                kill_block_reason = "进程归属于活动任务"
                kill_eligibility_reason = "runtime_evidence_present"
            elif task_row is not None and runtime_evidence["live_runtime_evidence"]:
                owner_kind = "lease_drifted_active"
                owner_reason = "lease_drift_but_runtime_evidence_present"
                kill_allowed = False
                kill_block_reason = "任务租约漂移但进程仍有真实运行证据"
                kill_eligibility_reason = "runtime_evidence_present"
            elif task_row is not None:
                owner_kind = "residual"
                owner_reason = "matched_task_without_runtime_evidence"
                kill_allowed = bool(
                    process_age_seconds is not None
                    and process_age_seconds >= _ORPHAN_PROTECTION_SECONDS
                )
                kill_block_reason = None if kill_allowed else "残留进程仍在保护窗口内"
                kill_eligibility_reason = "no_runtime_evidence_confirmed" if kill_allowed else "orphan_protection_window"
            else:
                owner_kind = "unknown"
                owner_reason = "unmatched_process_without_runtime_evidence"
                kill_allowed = False
                kill_block_reason = "未匹配任务归属，默认不自动终止"
                kill_eligibility_reason = "unknown_owner"
            processes.append(
                SaAgentProcessSnapshot(
                    pod_name=POD_NAME,
                    pid=int(proc["pid"]),
                    pgid=proc.get("pgid"),
                    ppid=proc.get("ppid"),
                    command=str(proc.get("command") or ""),
                    cwd=proc.get("cwd"),
                    exe=proc.get("exe"),
                    rss_bytes=proc.get("rss_bytes"),
                    runtime_kind=proc.get("runtime_kind"),
                    match_source=match_source,
                    match_confidence="high" if match_source in {"session_arg_path", "cwd"} else ("medium" if match_source == "task_root" else None),
                    workspace_root=workspace_root,
                    task_id=task_id,
                    task_name=task_name,
                    task_status=task_status,
                    stage_key=str((session_descriptor or {}).get("stage_key") or "") or None,
                    stage_group=str((session_descriptor or {}).get("stage_group") or "") or None,
                    family_key=str((session_descriptor or {}).get("family_key") or "") or None,
                    parallel_group=str((session_descriptor or {}).get("parallel_group") or "") or None,
                    role_kind=str((session_descriptor or {}).get("role") or role_kind or "") or None,
                    owner_kind=owner_kind,
                    owner_reason=owner_reason,
                    runtime_evidence=runtime_evidence,
                    lease_state=lease_state,
                    last_runtime_activity_at=runtime_evidence.get("last_runtime_activity_at"),
                    kill_allowed=kill_allowed,
                    kill_block_reason=kill_block_reason,
                    kill_eligibility_reason=kill_eligibility_reason,
                    termination_state="running",
                ).__dict__
            )

        tasks: list[dict[str, Any]] = []
        for row in task_rows:
            linked_processes = [item for item in processes if item.get("task_id") == row.task_id]
            if not linked_processes:
                continue
            tasks.append({
                "task_id": row.task_id,
                "task_name": row.task_name,
                "task_status": row.status,
                "stage_key": "",
                "pod_name": POD_NAME,
                "process_count": len(linked_processes),
                "agent_roles": sorted({str(item.get("role_kind") or "") for item in linked_processes if item.get("role_kind")}),
                "process_pids": [int(item["pid"]) for item in linked_processes],
                "ownership_status": (
                    "tracked"
                    if any(str(item.get("owner_kind") or "") == "tracked" for item in linked_processes)
                    else "lease_drifted_active"
                    if any(str(item.get("owner_kind") or "") == "lease_drifted_active" for item in linked_processes)
                    else "residual"
                ),
            })

        tracked_process_count = len([item for item in processes if item.get("owner_kind") == "tracked"])
        residual_process_count = len([item for item in processes if item.get("owner_kind") == "residual"])
        unknown_process_count = len([item for item in processes if item.get("owner_kind") == "unknown"])
        scanned_at = time.time()
        return {
            "summary": {
                "pod_name": POD_NAME,
                "active_processes": tracked_process_count,
                "residual_processes": residual_process_count,
                "unknown_processes": unknown_process_count,
                "killable_residual_processes": len([item for item in processes if item.get("owner_kind") == "residual" and item.get("kill_allowed")]),
                "killable_unknown_processes": len([item for item in processes if item.get("owner_kind") == "unknown" and item.get("kill_allowed")]),
                "scanned_at": scanned_at,
                "scan_errors": 0,
            },
            "processes": processes,
            "tasks": tasks,
            "pods": [{
                "pod_name": POD_NAME,
                "worker_id": POD_NAME,
                "healthy": True,
                "process_count": len(processes),
                "tracked_process_count": tracked_process_count,
                "residual_process_count": residual_process_count,
                "unknown_process_count": unknown_process_count,
                "task_count": len(tasks),
                "running_task_count": len([item for item in tasks if str(item.get("ownership_status") or "") == "tracked"]),
                "residual_task_count": len([item for item in tasks if str(item.get("ownership_status") or "") == "residual"]),
                "last_scanned_at": scanned_at,
                "scan_errors": 0,
                "processes": processes,
                "tasks": tasks,
            }],
        }

    def kill_process(self, pid: int) -> dict[str, Any]:
        proc_dir = pathlib.Path("/proc") / str(pid)
        stat = _read_text(proc_dir / "stat").split()
        pgid = int(stat[4]) if len(stat) > 4 else None
        try:
            if pgid is not None:
                os.killpg(pgid, signal.SIGTERM)
            else:
                os.kill(pid, signal.SIGTERM)
            time.sleep(0.2)
            with contextlib.suppress(ProcessLookupError):
                if pgid is not None:
                    os.killpg(pgid, signal.SIGKILL)
                else:
                    os.kill(pid, signal.SIGKILL)
            return {"pid": pid, "pgid": pgid, "status": "killed"}
        except ProcessLookupError:
            return {"pid": pid, "pgid": pgid, "status": "gone"}
        except Exception as exc:
            return {"pid": pid, "pgid": pgid, "status": "failed", "reason": str(exc)}


_service: AgentObservabilityService | None = None


def get_agent_observability_service() -> AgentObservabilityService:
    global _service
    if _service is None:
        _service = AgentObservabilityService()
    return _service
