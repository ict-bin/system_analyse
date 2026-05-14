"""Task management service for secflow-app-system-analyse.

Bridges the FastAPI management layer with the existing Orchestrator engine.
Each task is persisted in MySQL and executed asynchronously.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time as _time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import or_
from sqlalchemy.orm import Session, load_only
from sqlalchemy.orm.attributes import flag_modified

from app.config import load_service_config
from app.db.models import AppSaTask
from app.logging_utils import log_event
from app.service.config_service import get_worker_task_concurrency as _get_worker_task_concurrency_from_db
from app.service.task_query_service import TaskQueryService
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
_running_tasks: dict[str, asyncio.Task] = {}
_running_task_epochs: dict[str, int] = {}

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

def get_worker_runtime_health() -> dict:
    return _get_dispatcher_runtime_health(len(_running_tasks))


def get_worker_runtime_settings() -> dict:
    configured_concurrency = WORKER_TASK_CONCURRENCY
    try:
        from app.db import get_db as _get_db

        _db_gen = _get_db()
        _db = next(_db_gen)
        try:
            configured_concurrency = max(1, int(_get_worker_task_concurrency_from_db(_db)))
        finally:
            try:
                next(_db_gen)
            except StopIteration:
                pass
    except Exception:
        configured_concurrency = WORKER_TASK_CONCURRENCY
    return {
        "worker_instance_id": WORKER_INSTANCE_ID,
        "worker_task_concurrency": configured_concurrency,
        "worker_poll_interval_seconds": WORKER_POLL_INTERVAL_SECONDS,
        "worker_poll_jitter_seconds": WORKER_POLL_JITTER_SECONDS,
        "worker_idle_backoff_max_seconds": WORKER_IDLE_BACKOFF_MAX_SECONDS,
        "worker_overload_cooldown_seconds": WORKER_OVERLOAD_COOLDOWN_SECONDS,
        "worker_stale_sweep_interval_seconds": WORKER_STALE_SWEEP_INTERVAL_SECONDS,
        "worker_max_running_tasks_global": MAX_RUNNING_TASKS_GLOBAL,
        "worker_global_claim_lock_key": GLOBAL_CLAIM_LOCK_KEY,
        "worker_global_claim_lock_timeout_seconds": GLOBAL_CLAIM_LOCK_TIMEOUT_SECONDS,
    }


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
        pass


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


def _task_sessions_root(row: AppSaTask) -> Path | None:
    root = _task_root(row)
    if not root:
        return None
    return root / "run" / "sessions"


def _task_run_root(row: AppSaTask) -> Path | None:
    root = _task_root(row)
    if not root:
        return None
    return root / "run"


def _task_result_path(row: AppSaTask) -> Path | None:
    run_root = _task_run_root(row)
    return run_root / "result.json" if run_root else None


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


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


def _lightweight_result_json(row: AppSaTask, payload: dict | None, result_file: str | None = None) -> dict | None:
    if not isinstance(payload, dict):
        return None
    if payload.get("result_externalized"):
        return {
            **payload,
            "result_file": payload.get("result_file") or result_file or (str(_task_result_path(row)) if _task_result_path(row) else None),
            "result_externalized": True,
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


def _write_models_json_from_db(db: "Session") -> None:
    """从数据库读取 models 配置并写入 pi 的配置目录，使 pi 能识别模型。"""
    try:
        from app.service.config_service import get_model_config_service  # noqa: PLC0415
        import json as _json
        pi_dir = os.environ.get("PI_CODING_AGENT_DIR", "/root/.pi/agent")
        os.makedirs(pi_dir, exist_ok=True)
        models_cfg = get_model_config_service().get_models_config(db)
        blob = {k: v for k, v in models_cfg.items() if k != "updated_at"}
        dest = os.path.join(pi_dir, "models.json")
        with open(dest, "w", encoding="utf-8") as _f:
            _json.dump(blob, _f, ensure_ascii=False, indent=2)
        logger.info("models.json written from DB → %s", dest)
    except Exception as _exc:
        logger.warning("_write_models_json_from_db failed: %s", _exc, exc_info=True)


class TaskService:
    def __init__(self) -> None:
        from app.db import get_db

        self._task_repository = TaskRepository()
        self._runner_registry = init_runner_registry_service(
            get_db=get_db,
            get_running_tasks_count=lambda: len(_running_tasks),
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
                task_repository=self._task_repository,
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
            claim_task_lease=self._claim_task_lease,
            spawn_task=self._on_task_claimed,
            select_dispatch_target=self._select_dispatch_target,
            get_running_tasks_count=lambda: len(_running_tasks),
            load_runtime_control=self._load_runtime_control,
            task_repository=self._task_repository,
        )
        self._runner_assignment_task: asyncio.Task | None = None
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


    def list_tasks(self, db: Session, *, project_id: str, page: int = 1,
                   per_page: int = 100, status: Optional[str] = None,
                   analysis_mode: Optional[str] = None,
                   sort_by: str = "created_at",
                   sort_order: str = "desc") -> dict:
        query = db.query(AppSaTask).filter(
            AppSaTask.project_id == project_id,
            AppSaTask.is_deleted.is_(False),
        )
        if status:
            query = query.filter(AppSaTask.status == status)
        sort_column = _TASK_LIST_SORT_COLUMNS.get(str(sort_by or "").strip(), AppSaTask.created_at)
        order_expr = sort_column.asc() if str(sort_order or "").lower() == "asc" else sort_column.desc()
        requested_mode = _normalize_analysis_mode(analysis_mode) if analysis_mode else None
        if requested_mode:
            all_rows = (
                query.options(*self._list_load_options())
                .filter(or_(AppSaTask.analysis_mode == requested_mode, AppSaTask.parent_task_type == requested_mode))
                .order_by(order_expr, AppSaTask.id.desc())
                .all()
            )
            filtered = [row for row in all_rows if _infer_analysis_mode(row, include_config=False) == requested_mode]
            total = len(filtered)
            rows = filtered[(page - 1) * per_page:page * per_page]
        else:
            total = query.count()
            rows = (query.options(*self._list_load_options())
                    .order_by(order_expr, AppSaTask.id.desc())
                    .offset((page - 1) * per_page).limit(per_page).all())
        return {"items": [self._row_to_dict(r, include_heavy=False) for r in rows],
                "total": total, "page": page, "per_page": per_page}

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
        return result

    def repair_task_origin(self, db: Session, task_id: str, analysis_mode: str) -> dict:
        row = self._get_or_404(db, task_id)
        if row.status in ("pending", "running"):
            from fastapi import HTTPException
            raise HTTPException(400, "任务处于运行态，不能修改来源信息")
        if str(row.task_origin_type or "").strip() not in ("", "manual"):
            from fastapi import HTTPException
            raise HTTPException(400, "仅手动任务支持修改来源信息")

        normalized_mode = _normalize_analysis_mode(analysis_mode)
        row.analysis_mode = normalized_mode
        if isinstance(row.task_config_json, dict) and "resolved_config_snapshot" in row.task_config_json:
            row.task_config_json = {
                k: v for k, v in row.task_config_json.items()
                if k != "resolved_config_snapshot"
            } or None
            flag_modified(row, "task_config_json")

        db.commit()
        db.refresh(row)
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
        log_event(logger, logging.INFO, "task created",
                  event="task_created", task_id=task_id, project_id=project_id,
                  analysis_mode=mode,
                  **_security_filter_log_payload(effective_task_config))
        return self._row_to_dict(row)

    def restart_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        if row.status in ("pending", "running"):
            from fastapi import HTTPException
            raise HTTPException(400, "任务仍在运行中，请先取消后再重启")
        row = self._task_repository.restart_task_in_place(db, row)
        # 清除上次运行目录（含 .checkpoint/），当次从零开始
        if row.output_path:
            import shutil as _shutil
            task_root = os.path.join(row.output_path, task_id)
            if os.path.isdir(task_root):
                try:
                    _shutil.rmtree(task_root)
                except Exception as _e:
                    logger.warning("Failed to clean task dir %s: %s", task_root, _e)
        _clear_task_execution_lock(row.output_path, task_id)
        log_event(logger, logging.INFO, "task restarted in-place", event="task_restarted",
                  task_id=task_id, project_id=row.project_id)
        return self._row_to_dict(row)

    def resume_task(self, db: Session, task_id: str) -> dict:
        """断点续跑：保留已有 workspace 和 .checkpoint/ 目录，系统自动从中断处继续。"""
        from fastapi import HTTPException
        from pathlib import Path as _Path
        row = self._get_or_404(db, task_id)
        if row.status in ("pending", "running"):
            raise HTTPException(400, "任务仍在运行中，请先取消后再续跑")
        # 检查 .checkpoint/ 目录是否存在（无断点就无法续跑）
        if row.output_path:
            checkpoint_dir = _Path(row.output_path) / task_id / "run" / "workspace" / ".checkpoint"
            if not checkpoint_dir.exists():
                raise HTTPException(
                    400,
                    f"没有找到断点信息（{checkpoint_dir}），"
                    f"请使用重启（restart）代替续跑。"
                )
        row = self._task_repository.resume_task_in_place(db, row)
        _clear_task_execution_lock(row.output_path, task_id)
        log_event(logger, logging.INFO, "task resumed in-place", event="task_resumed",
                  task_id=task_id, project_id=row.project_id)
        return self._row_to_dict(row)

    def cancel_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        if row.status in ("passed", "failed", "error", "cancelled"):
            return self._row_to_dict(row)
        at = _running_tasks.get(task_id)
        if at and not at.done():
            at.cancel()
        row = self._task_repository.cancel_task_in_place(db, row)
        _clear_task_execution_lock(row.output_path, task_id)
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

    async def start_worker_loop(self) -> None:
        if is_manager_role():
            await self._dispatcher.start()
        if is_runner_role():
            await self._runner_registry.start()
            await self._start_runner_assignment_loop()

    async def stop_worker_loop(self) -> None:
        if is_manager_role():
            await self._dispatcher.stop()
        if is_runner_role():
            await self._stop_runner_assignment_loop()
            await self._runner_registry.stop()

    def _on_task_claimed(self, task_id: str, lease_epoch: int, dispatch_target: str) -> None:
        if dispatch_target != WORKER_INSTANCE_ID:
            return
        self._run_task_locally(task_id, lease_epoch)

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

    def _run_task_locally(self, task_id: str, lease_epoch: int) -> None:
        if task_id in _running_tasks and not _running_tasks[task_id].done():
            return
        asyncio_task = asyncio.create_task(self._runner.execute_task(task_id, lease_epoch), name=f"sa_task_{task_id}")
        _running_tasks[task_id] = asyncio_task
        _running_task_epochs[task_id] = lease_epoch

    async def _start_runner_assignment_loop(self) -> None:
        if self._runner_assignment_task and not self._runner_assignment_task.done():
            return
        self._runner_assignment_loop_running = True
        self._runner_assignment_task = asyncio.create_task(
            self._runner_assignment_loop(),
            name="sa_runner_assignment_loop",
        )

    async def _stop_runner_assignment_loop(self) -> None:
        self._runner_assignment_loop_running = False
        task = self._runner_assignment_task
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._runner_assignment_task = None

    async def _runner_assignment_loop(self) -> None:
        while self._runner_assignment_loop_running:
            try:
                self._poll_runner_assignments_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("runner assignment loop failed: %s", exc, exc_info=True)
            await asyncio.sleep(RUNNER_ASSIGNMENT_POLL_INTERVAL_SECONDS)

    def _poll_runner_assignments_once(self) -> None:
        db_gen = self._runner._deps.get_db()
        db: Session = next(db_gen)
        try:
            current_concurrency = max(1, int(_get_worker_task_concurrency_from_db(db)))
            available_slots = max(0, current_concurrency - len(_running_tasks))
            if available_slots <= 0:
                return
            rows = self._task_repository.list_tasks_assigned_to_instance(
                db,
                instance_id=WORKER_INSTANCE_ID,
                limit=available_slots,
            )
            now = now_local()
            for row in rows:
                if row.task_id in _running_tasks:
                    continue
                if row.lease_expires_at and row.lease_expires_at < now:
                    continue
                self._run_task_locally(row.task_id, int(row.lease_epoch or 0))
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    @staticmethod
    def _remove_running_task(task_id: str) -> None:
        _running_tasks.pop(task_id, None)
        _running_task_epochs.pop(task_id, None)

    @staticmethod
    def _acquire_execution_lock(output_path: str | None, task_id: str, lease_epoch: int) -> Path | None:
        lock_path = _task_execution_lock_path(output_path, task_id)
        if not lock_path:
            return None
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "task_id": task_id,
            "worker_instance_id": WORKER_INSTANCE_ID,
            "lease_epoch": lease_epoch,
            "acquired_at": isoformat_local(now_local()),
        }
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            raise RuntimeError(f"task execution lock already exists: {lock_path}")
        try:
            os.write(fd, json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))
        finally:
            os.close(fd)
        return lock_path

    def delete_task(self, db: Session, task_id: str, *, delete_files: bool = True) -> None:
        """软删除任务记录，并可选删除输出目录下的任务文件。运行中任务不允许删除。"""
        import shutil as _shutil
        from fastapi import HTTPException
        row = self._get_or_404(db, task_id)
        # 运行中的任务必须先取消，不允许直接删除
        if row.status == "running":
            raise HTTPException(status_code=409, detail="任务正在运行，请先取消后再删除")
        # 删除输出文件
        if delete_files and row.output_path:
            task_dir = os.path.join(row.output_path, task_id)
            if os.path.isdir(task_dir):
                try:
                    _shutil.rmtree(task_dir)
                    logger.info("delete_task: removed task dir %s", task_dir)
                except Exception as _e:
                    logger.warning("delete_task: failed to remove %s: %s", task_dir, _e)
        # 软删除
        row.is_deleted = True
        db.commit()

    async def _execute_task(self, task_id: str) -> None:
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
            result = await orch.execute(task_id)
            _flush_stages(task_id, event_buffer)
            db.expire(row); db.refresh(row)
            if row.status == "cancelled":
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
            db.commit()
            # —— 自省分析（异步后台，不阻塞任务完成） ——
            try:
                from app.pipeline.self_reflection import get_self_reflection_service
                _sr_run_dir = Path(row.output_path or "") / row.task_id / "run" if row.output_path else None
                _sr_out_dir = Path(row.output_path or "") / row.task_id / "output" if row.output_path else None
                _sr_status = result.status.value if result else "error"
                if _sr_run_dir and _sr_out_dir:
                    await get_self_reflection_service().trigger_async(
                        task_id=task_id,
                        run_dir=_sr_run_dir,
                        output_dir=_sr_out_dir,
                        cfg=cfg,
                        task_status=_sr_status,
                    )
            except Exception as _sr_exc:
                logger.warning("self-reflection trigger failed: %s", _sr_exc)
        except asyncio.CancelledError:
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
                    db.commit()
            except Exception:
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
            ),
        )

    @staticmethod
    def _row_to_dict(row: AppSaTask, *, include_heavy: bool = True) -> dict:
        def fmt(dt: datetime | None) -> str | None:
            return isoformat_local(dt)
        analysis_mode = _infer_analysis_mode(row, include_config=include_heavy)
        return {
            "task_id": row.task_id, "project_id": row.project_id,
            **_origin_payload(row),
            "analysis_mode": analysis_mode,
            "analysis_mode_label": _analysis_mode_label(analysis_mode),
            "task_name": row.task_name, "task_description": row.task_description,
            "input_path": row.input_path, "output_path": row.output_path,
            "prompt_template_id": row.prompt_template_id,
            "prompt_content": row.prompt_content if include_heavy else None, "status": row.status,
            "error": row.error,
            "result_json": _lightweight_result_json(row, row.result_json) if include_heavy else None,
            "stages_json": read_events(
                _events_path(row.output_path, row.task_id),
                row.stages_json,
            ) if include_heavy else None,
            "task_config_json": row.task_config_json if include_heavy else None,
            "created_by": row.created_by,
            "created_at": fmt(row.created_at), "updated_at": fmt(row.updated_at),
            "started_at": fmt(row.started_at), "finished_at": fmt(row.finished_at),
        }


_task_service: TaskService | None = None


def get_task_service() -> TaskService:
    global _task_service
    if _task_service is None:
        _task_service = TaskService()
    return _task_service
