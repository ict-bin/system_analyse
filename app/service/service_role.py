from __future__ import annotations

import os


def service_role() -> str:
    """规范化服务角色。

    新命名（与 deployment 名对齐）:
      api        — REST API 服务 (secflow-app-system-analyse)
      scheduler  — 调度器 (secflow-app-system-analyse-scheduler)，原 manager
      worker     — 任务执行 (secflow-app-system-analyse-worker)，原 runner
      all        — 单 pod 开发模式
    兼容旧值: manager→scheduler, runner→worker。
    """
    raw_role = os.environ.get("SECFLOW_SYSTEM_ANALYSE_ROLE") or ""
    normalized = str(raw_role).strip().lower()
    # 兼容旧角色名
    legacy = {"manager": "scheduler", "runner": "worker"}
    if normalized in legacy:
        return legacy[normalized]
    return normalized if normalized in {"api", "scheduler", "worker", "debugger", "all"} else "all"


def is_api_role() -> bool:
    return service_role() in {"api", "all"}


def is_scheduler_role() -> bool:
    return service_role() in {"scheduler", "all"}


def is_worker_role() -> bool:
    return service_role() in {"worker", "all"}


def is_debugger_role() -> bool:
    return service_role() in {"debugger", "all"}


# 兼容旧函数名（调用方未迁移时保留）
def is_manager_role() -> bool:
    return is_scheduler_role()


def is_runner_role() -> bool:
    return is_worker_role()


def is_dispatcher_role() -> bool:
    return is_scheduler_role()
