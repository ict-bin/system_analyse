from __future__ import annotations

import os


def service_role() -> str:
    raw_role = os.environ.get("SECFLOW_SYSTEM_ANALYSE_ROLE") or ""
    normalized = str(raw_role).strip().lower()
    if normalized == "worker":
        return "manager"
    return normalized if normalized in {"api", "manager", "runner", "all"} else "all"


def is_api_role() -> bool:
    return service_role() in {"api", "all"}


def is_manager_role() -> bool:
    return service_role() in {"manager", "all"}


def is_runner_role() -> bool:
    return service_role() in {"runner", "all"}


def is_dispatcher_role() -> bool:
    return is_manager_role()
