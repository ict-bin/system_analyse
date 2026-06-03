from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any


_lock = threading.Lock()
_entries: dict[str, dict[str, Any]] = {}


def _normalize_path(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return str(Path(raw).resolve(strict=False))
    except Exception:
        return raw


def build_runtime_registration_key(*, session_file: str | None, cwd: str | None, command: str | None = None) -> str | None:
    session_path = _normalize_path(session_file)
    if session_path:
        return session_path
    normalized_cwd = _normalize_path(cwd)
    normalized_command = str(command or "").strip()
    if not normalized_cwd and not normalized_command:
        return None
    return f"nosession::{normalized_cwd or '-'}::{normalized_command or '-'}"


def register_agent_runtime(
    *,
    session_file: str | None,
    cwd: str | None,
    pid: int,
    command: str | None = None,
    runtime_kind: str | None = None,
) -> None:
    runtime_key = build_runtime_registration_key(session_file=session_file, cwd=cwd, command=command)
    if not runtime_key:
        return
    now = time.time()
    with _lock:
        _entries[runtime_key] = {
            "session_path": _normalize_path(session_file),
            "runtime_key": runtime_key,
            "cwd": _normalize_path(cwd),
            "pid": int(pid),
            "command": str(command or "").strip() or None,
            "runtime_kind": str(runtime_kind or "").strip() or None,
            "registered_at_ts": now,
            "last_activity_at_ts": now,
        }


def touch_agent_runtime(*, session_file: str | None, cwd: str | None = None, command: str | None = None) -> None:
    runtime_key = build_runtime_registration_key(session_file=session_file, cwd=cwd, command=command)
    if not runtime_key:
        return
    with _lock:
        entry = _entries.get(runtime_key)
        if not entry:
            return
        entry["last_activity_at_ts"] = time.time()


def unregister_agent_runtime(*, session_file: str | None, cwd: str | None = None, command: str | None = None) -> None:
    runtime_key = build_runtime_registration_key(session_file=session_file, cwd=cwd, command=command)
    if not runtime_key:
        return
    with _lock:
        _entries.pop(runtime_key, None)


def get_agent_runtime_snapshot() -> dict[str, dict[str, Any]]:
    with _lock:
        return {
            session_path: dict(payload)
            for session_path, payload in _entries.items()
        }
