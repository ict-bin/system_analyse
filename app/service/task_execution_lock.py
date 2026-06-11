from __future__ import annotations

import os
import uuid
from datetime import datetime
from pathlib import Path

from app.service.worker_dispatcher import WORKER_INSTANCE_ID
from app.time_utils import isoformat_local, now_local

_RUNNER_BOOT_ID_PATH = Path("/proc/sys/kernel/random/boot_id")
RUNNER_PROCESS_STARTED_AT = now_local()
RUNNER_PROCESS_TOKEN = uuid.uuid4().hex
RUNNER_MAIN_PID = os.getpid()


def _read_runner_boot_id() -> str:
    try:
        value = _RUNNER_BOOT_ID_PATH.read_text("utf-8").strip()
    except Exception:
        value = ""
    return value or f"pid-{RUNNER_MAIN_PID}"


RUNNER_BOOT_ID = _read_runner_boot_id()


def current_runner_lock_identity() -> dict[str, object]:
    return {
        "worker_instance_id": WORKER_INSTANCE_ID,
        "runner_boot_id": RUNNER_BOOT_ID,
        "runner_process_started_at": isoformat_local(RUNNER_PROCESS_STARTED_AT),
        "runner_process_token": RUNNER_PROCESS_TOKEN,
        "runner_main_pid": RUNNER_MAIN_PID,
    }


class TaskExecutionLockConflict(RuntimeError):
    def __init__(self, message: str, *, conflict_kind: str, payload: dict[str, object] | None = None):
        super().__init__(message)
        self.conflict_kind = str(conflict_kind or "execution_lock_conflict")
        self.payload = dict(payload or {})
