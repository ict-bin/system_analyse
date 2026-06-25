from __future__ import annotations

import threading
import inspect
import json
import logging
import os
import shutil
import time as _time
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

from app.copy_utils import safe_copy2
from sqlalchemy.orm import Session

from app.config import build_task_config
from app.db.models import AppSaTask
from app.logging_utils import log_event
from app.models import SwarmEvent
from app.orchestrator import Orchestrator
from app.service.event_log import append_events, write_final, events_path
from app.service.agent_cleanup import AgentCleanupService
from app.service.task_execution_lock import TaskExecutionLockConflict, RUNNER_PROCESS_TOKEN
from app.service.task_repository import TaskRepository
from app.service.worker_dispatcher import WORKER_INSTANCE_ID, lease_deadline
from app.service.scheduler import TaskGuard
from app.time_utils import isoformat_local, now_local

logger = logging.getLogger("sa.task_runner")
LEASE_HEARTBEAT_FAILURE_TOLERANCE = 3


_PI_COMPACTION_SETTINGS = {
    "defaultThinkingLevel": "off",
    "compaction": {
        "enabled": True,
        "reserveTokens": 8192,
        "keepRecentTokens": 50000,
    },
}


def _task_agent_key(task_config_json: dict | None) -> dict | None:
    if not isinstance(task_config_json, dict):
        return None
    payload = task_config_json.get("agent_task_key")
    return payload if isinstance(payload, dict) else None


# Worker 阶段中属于 Reader(sub_read) 的阶段名；其余归 Worker。
_READER_STAGES = {"sub_read"}


def _apply_selected_models(cfg: Any, task_config_json: dict | None) -> None:
    """按 task_config.selected_models 覆盖三类角色模型。

    selected_models = {worker, reader, judge}
      worker → workers 的 explore/classify/refine/analyse/report 阶段 + agents[0]
      reader → workers 的 sub_read 阶段
      judge  → judges 的所有阶段 + agents[0]
    未提供的角色保持原配置不动。"""
    if not isinstance(task_config_json, dict):
        return
    selected = task_config_json.get("selected_models")
    if not isinstance(selected, dict) or not selected:
        return
    worker_model = str(selected.get("worker") or "").strip()
    reader_model = str(selected.get("reader") or "").strip()
    judge_model = str(selected.get("judge") or "").strip()
    # Worker 角色（除 sub_read 外的阶段）
    if worker_model:
        if getattr(cfg, "workers", None) is not None:
            cfg.workers.default_model = worker_model
            if cfg.workers.agents:
                for a in cfg.workers.agents:
                    if not a.model or a.model == "gaiasec/auto":
                        a.model = worker_model
            sm = dict(cfg.workers.stage_models or {})
            for stage in ("explore", "classify", "refine", "analyse", "report"):
                sm[stage] = worker_model
            cfg.workers.stage_models = sm
    # Reader 角色（sub_read 阶段）
    if reader_model and getattr(cfg, "workers", None) is not None:
        sm = dict(cfg.workers.stage_models or {})
        sm["sub_read"] = reader_model
        cfg.workers.stage_models = sm
    # Judge 角色
    if judge_model and getattr(cfg, "judges", None) is not None:
        cfg.judges.default_model = judge_model
        if cfg.judges.agents:
            for a in cfg.judges.agents:
                if not a.model or a.model == "gaiasec/auto":
                    a.model = judge_model
        sm = dict(cfg.judges.stage_models or {})
        for stage in ("classify", "refine", "analyse", "completeness", "report"):
            sm[stage] = judge_model
        cfg.judges.stage_models = sm


def _merge_pi_settings(base_settings: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(base_settings or {})
    merged["defaultThinkingLevel"] = _PI_COMPACTION_SETTINGS["defaultThinkingLevel"]
    compaction = merged.get("compaction") if isinstance(merged.get("compaction"), dict) else {}
    compaction.update(_PI_COMPACTION_SETTINGS["compaction"])
    merged["compaction"] = compaction
    return merged


def _build_role_models_json(
    role_name: str,
    role_config: Any,
    *,
    global_models_json: dict[str, Any] | None,
) -> dict[str, Any]:
    providers = (global_models_json or {}).get("providers")
    provider_map = providers if isinstance(providers, dict) else {}
    requested_models: set[str] = set()
    default_model = str(getattr(role_config, "default_model", "") or "").strip()
    if default_model:
        requested_models.add(default_model)
    for agent in getattr(role_config, "agents", []) or []:
        model = str(getattr(agent, "model", "") or "").strip()
        if model:
            requested_models.add(model)
    stage_models = getattr(role_config, "stage_models", {}) or {}
    if isinstance(stage_models, dict):
        for model in stage_models.values():
            text = str(model or "").strip()
            if text:
                requested_models.add(text)

    filtered: dict[str, Any] = {}
    for provider_key, provider_cfg in provider_map.items():
        if not isinstance(provider_cfg, dict):
            continue
        provider_copy = dict(provider_cfg)
        models = provider_cfg.get("models")
        raw_models = models if isinstance(models, list) else []
        kept_models: list[dict[str, Any]] = []
        for item in raw_models:
            if not isinstance(item, dict):
                continue
            model_id = str(item.get("id") or "").strip()
            qualified = f"{provider_key}/{model_id}" if provider_key and model_id else model_id
            if not requested_models or qualified in requested_models or model_id in requested_models:
                kept_models.append(dict(item))
        if kept_models:
            provider_copy["models"] = kept_models
            filtered[str(provider_key)] = provider_copy

    if filtered:
        return {"providers": filtered}
    return global_models_json if isinstance(global_models_json, dict) else {"providers": {}}


def _materialize_task_pi_runtime(*, task_root: str, agent_task_key: dict | None, cfg: Any) -> tuple[dict[str, str], str]:
    role_dirs: dict[str, str] = {}
    # 废弃 auth.json：wsk 不再写 auth.json（pi 的 auth.json 覆盖机制不可靠）。
    # 调度下发场景的 wsk 由 _substitute_wsk_into_models_json 直接替换进 models.json 的 apiKey。
    # pi 只读 models.json，key 确定无歧义。
    if not task_root:
        return role_dirs, "global"
    global_pi_dir = Path(os.environ.get("PI_CODING_AGENT_DIR", "/root/.pi/agent"))
    # 清理可能残留的旧 auth.json，避免 pi 误用
    try:
        auth_path = global_pi_dir / "auth.json"
        if auth_path.exists():
            auth_path.unlink()
    except OSError:
        pass
    return role_dirs, "global"


def _substitute_wsk_into_models_json(
    agent_task_key: dict | None,
    selected_models: dict | None,
) -> bool:
    """调度下发场景：把 wsk secret 直接替换进 models.json 的 apiKey。

    废弃 auth.json 后，pi 只读 models.json。本函数按 selected_models 解析出
    用到的 provider（形如 "gaiasec/auto" → provider=gaiasec），把这些 provider 的
    apiKey 替换为 wsk secret。这样 pi 发出的就是 wsk，确定无歧义。
    返回是否替换成功。
    """
    secret = str((agent_task_key or {}).get("secret") or "").strip()
    if not secret:
        return False
    if not isinstance(selected_models, dict) or not selected_models:
        return False
    # 收集 selected_models 引用到的 provider
    providers_used: set[str] = set()
    for role in ("worker", "reader", "judge"):
        val = str((selected_models or {}).get(role) or "").strip()
        if "/" in val:
            providers_used.add(val.split("/", 1)[0])
    if not providers_used:
        return False
    try:
        pi_dir = Path(os.environ.get("PI_CODING_AGENT_DIR", "/root/.pi/agent"))
        models_path = pi_dir / "models.json"
        if not models_path.is_file():
            return False
        data = json.loads(models_path.read_text(encoding="utf-8"))
        providers = data.get("providers") if isinstance(data, dict) else None
        if not isinstance(providers, dict):
            return False
        changed = False
        for pkey in providers_used:
            pcfg = providers.get(pkey)
            if isinstance(pcfg, dict):
                pcfg["apiKey"] = secret
                changed = True
        if not changed:
            return False
        models_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(
            "wsk 已直接替换进 models.json apiKey（providers=%s），废弃 auth.json",
            sorted(providers_used),
        )
        return True
    except Exception:
        logger.exception("_substitute_wsk_into_models_json failed")
        return False


def _read_json_file(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _normalize_agent_auth_snapshot(agent_task_key: dict | None) -> dict[str, Any] | None:
    if not isinstance(agent_task_key, dict):
        return None
    payload = {
        "agent_task_key_id": str(agent_task_key.get("id") or "").strip() or None,
        "agent_task_key_name": str(agent_task_key.get("name") or "").strip() or None,
        "agent_task_key_prefix": str(agent_task_key.get("prefix") or "").strip() or None,
        "agent_task_key_secret": str(agent_task_key.get("secret") or "").strip() or None,
        "agent_task_key_source": str(agent_task_key.get("source") or "").strip() or None,
    }
    return payload if any(payload.values()) else None


def _build_role_runtime_summary(
    role_name: str,
    role_config: Any,
    *,
    runtime_dir: str | None,
    models_json: dict[str, Any] | None,
    settings_json: dict[str, Any] | None,
    auth_json: dict[str, Any] | None,
) -> dict[str, Any]:
    agents = []
    for index, agent in enumerate(getattr(role_config, "agents", []) or []):
        if hasattr(agent, "model_dump"):
            payload = agent.model_dump(mode="json")
        elif isinstance(agent, dict):
            payload = dict(agent)
        else:
            payload = {"model": str(getattr(agent, "model", "") or "").strip() or None}
        payload.setdefault("index", index)
        agents.append(payload)
    stage_models = getattr(role_config, "stage_models", {}) or {}
    return {
        "role_name": role_name,
        "config_file_key": None,
        "provider_key": None,
        "provider_type": None,
        "model": str(getattr(role_config, "default_model", "") or "").strip() or None,
        "model_selector": None,
        "default_model": str(getattr(role_config, "default_model", "") or "").strip() or None,
        "default_tools": list(getattr(role_config, "default_tools", []) or []),
        "default_thinking_level": str(getattr(role_config, "default_thinking_level", "") or "").strip() or None,
        "system_prompt_dir": str(getattr(role_config, "system_prompt_dir", "") or "").strip() or None,
        "runtime_dir": str(runtime_dir or "").strip() or None,
        "agent_count": len(agents),
        "agents": agents,
        "stage_models": dict(stage_models) if isinstance(stage_models, dict) else {},
        "models_json": models_json,
        "settings_json": settings_json,
        "auth_json": auth_json,
    }


def _build_runtime_config_snapshots(
    *,
    cfg: Any,
    agent_task_key: dict | None,
    task_pi_dirs: dict[str, str] | None,
    agent_runtime_mode: str,
) -> tuple[dict[str, Any] | None, dict[str, Any], dict[str, Any], dict[str, Any]]:
    frozen_at = isoformat_local(now_local()) or datetime.utcnow().isoformat()
    agent_auth_json = _normalize_agent_auth_snapshot(agent_task_key)
    role_dirs = task_pi_dirs if isinstance(task_pi_dirs, dict) else {}

    role_config_snapshot: dict[str, Any] = {}
    role_runtime_files: dict[str, Any] = {}
    provider_runtime_summary = {
        "workers": None,
        "judges": None,
    }
    for role_name, role_config in (("workers", cfg.workers), ("judges", cfg.judges)):
        runtime_dir = role_dirs.get(role_name)
        runtime_path = Path(runtime_dir) if runtime_dir else None
        models_json = _read_json_file(runtime_path / "models.json" if runtime_path else None)
        settings_json = _read_json_file(runtime_path / "settings.json" if runtime_path else None)
        auth_json = _read_json_file(runtime_path / "auth.json" if runtime_path else None)
        role_runtime_files[role_name] = {
            "runtime_dir": runtime_dir,
            "models_json": models_json,
            "settings_json": settings_json,
            "auth_json": auth_json,
        }
        role_config_snapshot[role_name] = {
            "config": role_config.model_dump(mode="json") if hasattr(role_config, "model_dump") else {},
            "runtime_dir": runtime_dir,
            "runtime_files": {
                "models_json": models_json,
                "settings_json": settings_json,
                "auth_json": auth_json,
            },
        }
        provider_runtime_summary[role_name] = _build_role_runtime_summary(
            role_name,
            role_config,
            runtime_dir=runtime_dir,
            models_json=models_json,
            settings_json=settings_json,
            auth_json=auth_json,
        )
    llm_binding_snapshot = {
        "version": 1,
        "frozen_at": frozen_at,
        "agent_runtime_mode": agent_runtime_mode,
        "agent_task_key": {
            "id": str((agent_task_key or {}).get("id") or "").strip() or None,
            "name": str((agent_task_key or {}).get("name") or "").strip() or None,
            "prefix": str((agent_task_key or {}).get("prefix") or "").strip() or None,
            "secret": str((agent_task_key or {}).get("secret") or "").strip() or None,
            "source": str((agent_task_key or {}).get("source") or "").strip() or None,
        } if isinstance(agent_task_key, dict) else None,
        "runtime_files": role_runtime_files,
        "roles": role_config_snapshot,
    }
    return agent_auth_json, role_config_snapshot, provider_runtime_summary, llm_binding_snapshot


@dataclass
class TaskRunnerDependencies:
    get_db: Callable[[], object]
    acquire_execution_lock: Callable[[Session, str | None, str, int], object | None]
    clear_task_execution_lock: Callable[[str | None, str], None]
    flush_stages: Callable[[str, list[dict]], None]  # kept for legacy _execute_task path
    load_svc_config_from_db: Callable[[Session, str], object]
    infer_analysis_mode: Callable[[AppSaTask], str]
    security_filter_log_payload_resolved: Callable[[dict | None], dict]
    write_models_json_from_db: Callable[[Session], None]
    write_models_json_from_gateway: Callable[[], None]
    write_task_result_json: Callable[[object, dict], str | None]
    lightweight_result_json: Callable[[object, dict | None, str | None], dict | None]
    remove_running_task: Callable[[str], None]
    record_timeline_event: Callable[..., None]
    task_repository: TaskRepository
    merge_result_json: Callable[[dict | None, dict | None], dict | None]


@dataclass
class TaskRunnerSettings:
    source_mode_default_analyse_targets: list[str]
    task_stage_flush_batch_size: int
    task_stage_flush_min_interval_seconds: float
    task_cancel_poll_interval_seconds: float
    task_lease_heartbeat_seconds: float


class TaskRunner:
    def __init__(self, *, deps: TaskRunnerDependencies, settings: TaskRunnerSettings) -> None:
        self._deps = deps
        self._settings = settings
        self._agent_cleanup = AgentCleanupService()

    def execute_task(self, task_id: str, lease_epoch: int) -> None:
        event_buffer: list[dict] = []
        output_path_for_lock: str | None = None
        last_stage_flush_ts = 0.0
        last_stage_flush_count = 0
        emitted_stage_states: set[tuple[str, str]] = set()
        task_snapshot = None
        pre_cleanup_report: dict | None = None
        # events_file 在 _prepare_task_execution 返回后才可知，先用 None
        events_file: Path | None = None

        def _normalize_stage_name(raw: object) -> str | None:
            text = str(raw or "").strip()
            return text or None

        def _build_stage_payload(data: dict) -> dict | None:
            payload: dict[str, object] = {}
            for src_key, dst_key in (
                ("module", "module"),
                ("module_name", "module_name"),
                ("attempt", "attempt"),
                ("modules", "modules"),
                ("split", "split"),
                ("new_modules", "new_modules"),
                ("duration_ms", "duration_ms"),
                ("duration_seconds", "duration_seconds"),
                ("elapsed_ms", "elapsed_ms"),
                ("elapsed_seconds", "elapsed_seconds"),
                ("file_count", "file_count"),
                ("module_count", "module_count"),
                ("lease_epoch", "lease_epoch"),
            ):
                value = data.get(src_key)
                if value not in (None, "", [], {}):
                    payload[dst_key] = value
            if events_file is not None:
                payload["events_file"] = str(events_file)
            return payload or None

        def _message_for_stage(event_type: str, stage_name: str, data: dict) -> str:
            module_name = str(data.get("module") or data.get("module_name") or "").strip()
            suffix = f"（模块: {module_name}）" if module_name else ""
            mapping = {
                "stage_started": f"阶段开始: {stage_name}{suffix}",
                "stage_finished": f"阶段完成: {stage_name}{suffix}",
                "stage_failed": f"阶段失败: {stage_name}{suffix}",
                "stage_timeout": f"阶段超时: {stage_name}{suffix}",
                "stage_skipped": f"阶段跳过: {stage_name}{suffix}",
            }
            return mapping.get(event_type, f"阶段事件: {stage_name}{suffix}")

        def _record_stage_timeline_if_needed(event: SwarmEvent) -> None:
            stage_name = _normalize_stage_name(event.data.get("stage"))
            if str(event.type or "").strip().lower() == "task_rate_limited_retrying":
                payload = dict(event.data or {})
                self._deps.record_timeline_event(
                    task_id=task_id,
                    project_id=getattr(task_snapshot, "project_id", None),
                    stage_name=stage_name,
                    event_type="task_rate_limited_retrying",
                    message="智能体请求被 429 限流，30 秒后自动重试",
                    level="warning",
                    payload=payload,
                )
                return
            if str(event.type or "").strip().lower() == "task_api_retrying":
                payload = dict(event.data or {})
                self._deps.record_timeline_event(
                    task_id=task_id,
                    project_id=getattr(task_snapshot, "project_id", None),
                    stage_name=stage_name,
                    event_type="task_api_retrying",
                    message="智能体 API 错误，已进入无限重试",
                    level="warning",
                    payload=payload,
                )
                return
            if str(event.type or "").strip().lower() == "task_fatal_retrying":
                payload = dict(event.data or {})
                self._deps.record_timeline_event(
                    task_id=task_id,
                    project_id=getattr(task_snapshot, "project_id", None),
                    stage_name=stage_name,
                    event_type="task_fatal_retrying",
                    message="智能体基础设施异常，已进入 30 秒固定间隔重试",
                    level="warning",
                    payload=payload,
                )
                return
            if str(event.type or "").strip().lower() == "task_context_compaction_requested":
                payload = dict(event.data or {})
                self._deps.record_timeline_event(
                    task_id=task_id,
                    project_id=getattr(task_snapshot, "project_id", None),
                    stage_name=stage_name,
                    event_type="task_context_compaction_requested",
                    message="智能体上下文超限，已请求会话压缩",
                    level="warning",
                    payload=payload,
                )
                return
            if str(event.type or "").strip().lower() == "task_context_compaction_completed":
                payload = dict(event.data or {})
                self._deps.record_timeline_event(
                    task_id=task_id,
                    project_id=getattr(task_snapshot, "project_id", None),
                    stage_name=stage_name,
                    event_type="task_context_compaction_completed",
                    message="智能体会话压缩已完成",
                    level="info",
                    payload=payload,
                )
                return
            if str(event.type or "").strip().lower() == "task_context_budget_exceeded_preflight":
                payload = dict(event.data or {})
                self._deps.record_timeline_event(
                    task_id=task_id,
                    project_id=getattr(task_snapshot, "project_id", None),
                    stage_name=stage_name,
                    event_type="task_context_budget_exceeded_preflight",
                    message="智能体请求在发送前已判定超出上下文预算",
                    level="error",
                    payload=payload,
                )
                return
            if str(event.type or "").strip().lower() == "task_context_overflow_retrying":
                payload = dict(event.data or {})
                self._deps.record_timeline_event(
                    task_id=task_id,
                    project_id=getattr(task_snapshot, "project_id", None),
                    stage_name=stage_name,
                    event_type="task_context_overflow_retrying",
                    message="智能体上下文持续超限，已进入无限压缩重试",
                    level="warning",
                    payload=payload,
                )
                return
            if str(event.type or "").strip().lower() == "task_context_overflow_failed_after_compaction":
                payload = dict(event.data or {})
                self._deps.record_timeline_event(
                    task_id=task_id,
                    project_id=getattr(task_snapshot, "project_id", None),
                    stage_name=stage_name,
                    event_type="task_context_overflow_failed_after_compaction",
                    message="智能体上下文压缩后仍超出预算，请求已终止",
                    level="error",
                    payload=payload,
                )
                return
            if not stage_name:
                return
            raw_type = str(event.type or "").strip().lower()
            data = dict(event.data or {})
            timeline_type: str | None = None
            if raw_type == "stage":
                timeline_type = "stage_started"
            elif raw_type == "stage_result":
                if bool(data.get("skipped")):
                    timeline_type = "stage_skipped"
                elif bool(data.get("timeout")):
                    timeline_type = "stage_timeout"
                elif data.get("error") or str(data.get("status") or "").strip().lower() in {"failed", "error"}:
                    timeline_type = "stage_failed"
                else:
                    timeline_type = "stage_finished"
            elif raw_type in {"stage_timeout", "timeout"}:
                timeline_type = "stage_timeout"
            elif raw_type in {"stage_failed", "stage_error"}:
                timeline_type = "stage_failed"
            elif raw_type == "stage_skipped":
                timeline_type = "stage_skipped"
            if not timeline_type:
                return
            dedupe_key = (timeline_type, stage_name)
            if dedupe_key in emitted_stage_states:
                return
            emitted_stage_states.add(dedupe_key)
            payload = _build_stage_payload(data)
            if data.get("error"):
                payload = {**(payload or {}), "error": str(data.get("error"))}
            self._deps.record_timeline_event(
                task_id=task_id,
                project_id=getattr(task_snapshot, "project_id", None),
                stage_name=stage_name,
                event_type=timeline_type,
                message=_message_for_stage(timeline_type, stage_name, data),
                level="error" if timeline_type in {"stage_failed", "stage_timeout"} else "info",
                payload=payload,
            )

        def on_event(event: SwarmEvent) -> None:
            nonlocal last_stage_flush_ts, last_stage_flush_count
            # 心跳事件仅用于实时监控，不写入 events.jsonl
            # （它们是 "Worker 还在运行" 信号，不是业务状态变化）
            if event.type == "heartbeat":
                return
            event_buffer.append({"ts": _time.time(), "type": event.type, "data": dict(event.data)})
            _record_stage_timeline_if_needed(event)
            now_ts = _time.time()
            buffered_count = len(event_buffer) - last_stage_flush_count
            if (
                buffered_count >= self._settings.task_stage_flush_batch_size
                or (last_stage_flush_ts > 0.0 and (now_ts - last_stage_flush_ts) >= self._settings.task_stage_flush_min_interval_seconds)
            ):
                # 增量写文件（存在 events_file），否则降级写 DB
                if events_file is not None:
                    persisted = append_events(events_file, event_buffer[last_stage_flush_count:])
                    if not persisted:
                        self._deps.flush_stages(task_id, event_buffer)
                else:
                    self._deps.flush_stages(task_id, event_buffer)
                last_stage_flush_ts = now_ts
                last_stage_flush_count = len(event_buffer)

        try:
            pre_cleanup_report = self._run_agent_cleanup(task_id=task_id, project_id=None, phase="pre_task")
            task_snapshot, cfg = self._prepare_task_execution(task_id, lease_epoch, on_event)
            if pre_cleanup_report is not None:
                pre_cleanup_report["project_id"] = task_snapshot.project_id
            output_path_for_lock = task_snapshot.output_path
            self._deps.record_timeline_event(
                task_id=task_id,
                project_id=task_snapshot.project_id,
                event_type="task_started",
                message="任务开始执行",
                payload={
                    "runner_instance_id": WORKER_INSTANCE_ID,
                    "lease_epoch": lease_epoch,
                },
            )
            # 现在可以确定 events_file 路径
            events_file = events_path(task_snapshot.output_path, task_id)
            # 如果在 _prepare 期间 on_event 已经被触发过（极少发生），补刷一次
            if event_buffer:
                persisted = append_events(events_file, event_buffer)
                if not persisted:
                    self._deps.flush_stages(task_id, event_buffer)
                last_stage_flush_count = len(event_buffer)
            orch = Orchestrator(config=cfg, on_event=on_event, skip_provider_sync=True)
            task_supervisor = threading.Thread(
                target=self._supervise_running_task,
                args=(task_id, lease_epoch, orch),
                name=f"sa_supervise_{task_id}",
                daemon=True,
            )
            task_supervisor.start()
            # 调度器任务守卫: 心跳 + 结束通知
            guard = TaskGuard(task_id, scheduler_url="", pod_id=WORKER_INSTANCE_ID)
            guard.start()
            try:
                result = orch.execute(task_id)
                guard.done("completed")
            except Exception:
                guard.done("failed")
                raise
            finally:
                pass  # supervisor thread is daemon
            # 最终增量刷新剩余 events
            if events_file is not None:
                persisted = append_events(events_file, event_buffer[last_stage_flush_count:])
                if not persisted:
                    self._deps.flush_stages(task_id, event_buffer)
            else:
                self._deps.flush_stages(task_id, event_buffer)
            self._persist_task_result(task_id, lease_epoch, task_snapshot, result, event_buffer, events_file)
        except Exception as exc:
            log_event(
                logger,
                logging.ERROR,
                "task execution failed",
                event="task_error",
                task_id=task_id,
                error=str(exc),
            )
            import traceback
            traceback.print_exc()
            self._persist_task_error(task_id, lease_epoch, event_buffer, exc, events_file, pre_cleanup_report=pre_cleanup_report)
        finally:
            post_project_id = getattr(task_snapshot, "project_id", None) if task_snapshot is not None else None
            post_cleanup_report = self._run_agent_cleanup(task_id=task_id, project_id=post_project_id, phase="post_task")
            self._attach_cleanup_report(task_id=task_id, pre_cleanup_report=pre_cleanup_report, post_cleanup_report=post_cleanup_report)
            self._deps.remove_running_task(task_id)
            self._deps.clear_task_execution_lock(output_path_for_lock, task_id)

    def _run_agent_cleanup(self, *, task_id: str, project_id: str | None, phase: str) -> dict[str, object]:
        self._deps.record_timeline_event(
            task_id=task_id,
            project_id=project_id,
            event_type="agent_cleanup_started",
            message="任务启动前智能体清理开始" if phase == "pre_task" else "任务结束后智能体清理开始",
            level="info",
            payload={"cleanup_phase": phase, "runner_instance_id": WORKER_INSTANCE_ID},
        )
        try:
            report = self._agent_cleanup.run_cleanup(phase=phase)
        except Exception as exc:
            report = {
                "cleanup_phase": phase,
                "runner_instance_id": WORKER_INSTANCE_ID,
                "scanned_process_count": 0,
                "killed_process_count": 0,
                "failed_process_count": 0,
                "surviving_process_count": 0,
                "cleanup_failed": True,
                "level": "warning" if phase == "pre_task" else "error",
                "task_continued": phase == "pre_task",
                "error": str(exc),
                "items": [],
            }
        event_type = "agent_cleanup_failed" if bool(report.get("cleanup_failed")) else "agent_cleanup_completed"
        self._deps.record_timeline_event(
            task_id=task_id,
            project_id=project_id,
            event_type=event_type,
            message=(
                "任务启动前智能体清理失败，任务继续执行"
                if phase == "pre_task" and bool(report.get("cleanup_failed"))
                else "任务启动前智能体清理完成"
                if phase == "pre_task"
                else "任务结束后智能体清理失败"
                if bool(report.get("cleanup_failed"))
                else "任务结束后智能体清理完成"
            ),
            level=str(report.get("level") or "info"),
            payload=report,
        )
        return report

    def force_cleanup_all_agents(self, *, phase: str) -> dict[str, object]:
        return self._agent_cleanup.run_cleanup(phase=phase)

    def _attach_cleanup_report(
        self,
        *,
        task_id: str,
        pre_cleanup_report: dict | None,
        post_cleanup_report: dict | None,
    ) -> None:
        db_gen = self._deps.get_db()
        db: Session = next(db_gen)
        try:
            row = self._deps.task_repository.get_task(db, task_id)
            if row is None:
                return
            cleanup_payload = {
                "pre": pre_cleanup_report,
                "post": post_cleanup_report,
            }
            row.result_json = self._deps.merge_result_json(row.result_json, {"agent_cleanup": cleanup_payload})
            if hasattr(db, "commit"):
                db.commit()
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _prepare_task_execution(self, task_id: str, lease_epoch: int, on_event: Callable[[SwarmEvent], None]) -> tuple[SimpleNamespace, object]:
        db_gen = self._deps.get_db()
        db: Session = next(db_gen)
        try:
            row = self._deps.task_repository.get_task(db, task_id)
            if not row or row.status == "cancelled":
                raise RuntimeError("task no longer runnable")
            if row.dispatcher_instance_id != WORKER_INSTANCE_ID or int(row.lease_epoch or 0) != lease_epoch:
                logger.warning(
                    "skip task %s because active lease belongs to another worker or epoch changed: owner=%s epoch=%s",
                    task_id,
                    row.dispatcher_instance_id,
                    row.lease_epoch,
                )
                raise RuntimeError("task lease lost before execute")

            def _lock_observer(event_type: str, payload: dict[str, object]) -> None:
                message_map = {
                    "task_execution_lock_stale_detected": "检测到旧执行锁，准备自动清理",
                    "task_execution_lock_cleared": "旧执行锁已清理，准备重试获取",
                    "task_execution_lock_reacquired": "任务执行锁已重新获取",
                    "task_execution_lock_conflict": "任务执行锁冲突，当前已有活跃执行实例",
                    "task_execution_lock_reentry_blocked": "检测到同实例重复进入执行路径",
                    "task_execution_lock_acquired": "任务执行锁已获取",
                }
                level = (
                    "warning"
                    if event_type in {"task_execution_lock_stale_detected", "task_execution_lock_cleared"}
                    else "error"
                    if event_type in {"task_execution_lock_conflict", "task_execution_lock_reentry_blocked"}
                    else "info"
                )
                self._deps.record_timeline_event(
                    task_id=task_id,
                    project_id=getattr(row, "project_id", None),
                    event_type=event_type,
                    message=message_map.get(event_type, "任务执行锁状态更新"),
                    level=level,
                    payload=payload,
                )

            acquire_params = inspect.signature(self._deps.acquire_execution_lock).parameters
            if "observer" in acquire_params:
                self._deps.acquire_execution_lock(db, row.output_path, task_id, lease_epoch, observer=_lock_observer)
            else:
                self._deps.acquire_execution_lock(db, row.output_path, task_id, lease_epoch)
            svc = self._deps.load_svc_config_from_db(db, row.project_id)
            tcfg = dict(row.task_config_json or {})
            if tcfg.get("analyse_targets"):
                svc.analyse_targets = tcfg["analyse_targets"]
            elif self._deps.infer_analysis_mode(row) == "source":
                svc.analyse_targets = list(self._settings.source_mode_default_analyse_targets)
            if tcfg.get("binary_arch"):
                svc.binary_arch = tcfg["binary_arch"]
            if tcfg.get("security_focus_categories"):
                svc.security_focus_categories = tcfg["security_focus_categories"]
            if tcfg.get("module_granularity"):
                svc.module_granularity = tcfg["module_granularity"]
            # resume(断点续做)已移除：不读取 start_stage/resume_workspace，任务始终从头运行
            if tcfg.get("filter_engine"):
                svc.filter_engine = tcfg["filter_engine"]
            if "enable_final_check" in tcfg:
                svc.enable_final_check = bool(tcfg["enable_final_check"])
            if "continue_on_module_failure" in tcfg:
                svc.continue_on_module_failure = bool(tcfg["continue_on_module_failure"])
            if "super_fast_mode" in tcfg:
                svc.super_fast_mode = bool(tcfg["super_fast_mode"])
            # resume(断点续做)已移除：不读取 start_stage/resume_workspace，任务始终从头运行
            if row.output_path:
                svc.output_dir = row.output_path
                svc.archive_dir = row.output_path
                svc.result_dir = row.output_path
            task_snapshot = SimpleNamespace(
                task_id=row.task_id,
                project_id=row.project_id,
                prompt_content=row.prompt_content,
                input_path=row.input_path,
                output_path=row.output_path,
                analysis_mode=row.analysis_mode,
                task_origin_type=row.task_origin_type,
                task_config_json=tcfg,
                result_json=row.result_json,
                stages_json=row.stages_json,
            )
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

        cfg = build_task_config(svc, task_snapshot.prompt_content, cwd=task_snapshot.input_path)
        _tcfg = task_snapshot.task_config_json if isinstance(task_snapshot.task_config_json, dict) else {}
        agent_task_key = _task_agent_key(_tcfg)
        has_secret = bool((agent_task_key or {}).get("secret"))
        # 模型选择：有传入模型用该模型；无传入模型但有 secret(wsk) 默认 auto；
        # 无 secret 且无模型 → 不覆盖，用参数配置界面的服务默认模型。
        selected_models = _tcfg.get("selected_models") if isinstance(_tcfg.get("selected_models"), dict) else None
        if has_secret and not selected_models:
            selected_models = {"worker": "gaiasec/auto", "reader": "gaiasec/auto", "judge": "gaiasec/auto"}
        if selected_models:
            _apply_selected_models(cfg, {"selected_models": selected_models})
        task_root = str(Path(task_snapshot.output_path or "") / task_id) if task_snapshot.output_path else ""
        task_pi_dirs, agent_runtime_mode = _materialize_task_pi_runtime(
            task_root=task_root,
            agent_task_key=agent_task_key,
            cfg=cfg,
        )
        cfg.task_pi_dirs = dict(task_pi_dirs)
        cfg.task_pi_dir = cfg.role_pi_dir("workers")
        self._deps.record_timeline_event(
            task_id=task_id,
            project_id=task_snapshot.project_id,
            event_type="task_agent_runtime_materialized",
            message="已生成任务级角色 PI runtime",
            payload={
                "agent_task_key_id": str((agent_task_key or {}).get("id") or "").strip() or None,
                "agent_task_key_prefix": str((agent_task_key or {}).get("prefix") or "").strip() or None,
                "agent_task_key_source": str((agent_task_key or {}).get("source") or "").strip() or None,
                "agent_runtime_mode": agent_runtime_mode,
                "role_runtime_dirs": dict(task_pi_dirs),
            },
        )
        resolved_snapshot = cfg.model_dump(mode="json")
        (
            agent_auth_json,
            role_config_snapshot,
            provider_runtime_summary,
            llm_binding_snapshot,
        ) = _build_runtime_config_snapshots(
            cfg=cfg,
            agent_task_key=agent_task_key,
            task_pi_dirs=task_pi_dirs,
            agent_runtime_mode=agent_runtime_mode,
        )
        log_event(
            logger,
            logging.INFO,
            "security filter resolved",
            event="security_filter_resolved",
            task_id=task_id,
            project_id=task_snapshot.project_id,
            analysis_mode=task_snapshot.analysis_mode,
            task_origin_type=task_snapshot.task_origin_type,
            **self._deps.security_filter_log_payload_resolved(resolved_snapshot),
        )

        db_gen = self._deps.get_db()
        db = next(db_gen)
        try:
            updated = self._deps.task_repository.save_resolved_config_snapshot(
                db,
                task_id=task_id,
                lease_epoch=lease_epoch,
                worker_instance_id=WORKER_INSTANCE_ID,
                task_config_json=task_snapshot.task_config_json,
                resolved_snapshot=resolved_snapshot,
                agent_auth_json=agent_auth_json,
                role_config_snapshot=role_config_snapshot,
                provider_runtime_summary=provider_runtime_summary,
                llm_binding_snapshot=llm_binding_snapshot,
                lease_deadline=lease_deadline,
            )
            if not updated:
                raise RuntimeError("task lease lost when persisting resolved config")
            # models.json 来源路由（按 secret 是否传入，不按 task_origin/model_source）：
            #   有 secret(wsk) → 网关配置 models.json + 把 secret 直接替换进 apiKey
            #   无 secret      → 模型配置中心 models.json (sk，手动模式默认 key)
            if has_secret:
                self._deps.write_models_json_from_gateway()
                _substitute_wsk_into_models_json(agent_task_key, selected_models)
            else:
                self._deps.write_models_json_from_db(db)
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
        return task_snapshot, cfg

    def _persist_task_result(
        self,
        task_id: str,
        lease_epoch: int,
        task_snapshot: object,
        result: object,
        event_buffer: list[dict],
        events_file: Path | None = None,
    ) -> None:
        # 先写文件（包含 __final__ 标记）
        if not write_final(events_file, event_buffer):
            self._deps.flush_stages(task_id, event_buffer)
        db_gen = self._deps.get_db()
        db = next(db_gen)
        try:
            row = self._deps.task_repository.get_task(db, task_id)
            if not row or row.status == "cancelled":
                if row and row.status == "cancelled":
                    self._deps.record_timeline_event(
                        task_id=task_id,
                        project_id=row.project_id,
                        event_type="task_failed",
                        message="任务已取消",
                        level="warning",
                        payload={
                            "runner_instance_id": WORKER_INSTANCE_ID,
                            "lease_epoch": lease_epoch,
                            "status": row.status,
                        },
                    )
                return
            result_json = None
            result_error = None
            program_errors: list[dict] = []
            soft_failed_modules: list[dict] = []
            if result:
                result_payload = result.model_dump(mode="json")
                result_file = self._deps.write_task_result_json(task_snapshot, result_payload)
                result_json = self._deps.lightweight_result_json(task_snapshot, result_payload, result_file)
                if result.error:
                    result_error = result.error
            program_errors = list(getattr(task_snapshot, "program_error_modules", []) or [])
            soft_failed_modules = list(getattr(task_snapshot, "soft_failed_modules", []) or [])
            if program_errors:
                if not result_error:
                    first = program_errors[0]
                    result_error = (
                        f"程序性错误 ({first.get('error_type')}) in {first.get('stage')} "
                        f"{first.get('module_name')}: {first.get('error_message')}"
                    )
            updated = self._deps.task_repository.finalize_task_result(
                db,
                task_id=task_id,
                lease_epoch=lease_epoch,
                worker_instance_id=WORKER_INSTANCE_ID,
                result_status=result.status.value if result else "error",
                result_json=result_json,
                result_error=result_error,
            )
            if not updated:
                return
            final_status = result.status.value if result else "error"
            event_type = "task_finished" if final_status == "passed" else "task_failed"
            level = "info" if event_type == "task_finished" else "error"
            payload: dict[str, object] = {
                "runner_instance_id": WORKER_INSTANCE_ID,
                "lease_epoch": lease_epoch,
                "status": final_status,
            }
            duration_ms = getattr(result, "total_duration_ms", None) if result is not None else None
            if duration_ms not in (None, ""):
                payload["duration_ms"] = duration_ms
            if result_error:
                payload["error"] = str(result_error)
            self._deps.record_timeline_event(
                task_id=task_id,
                project_id=row.project_id,
                event_type=event_type,
                message="任务执行完成" if event_type == "task_finished" else "任务执行失败",
                level=level,
                payload=payload,
            )
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _persist_task_error(
        self, task_id: str, lease_epoch: int,
        event_buffer: list[dict], exc: Exception,
        events_file: Path | None = None,
        pre_cleanup_report: dict | None = None,
    ) -> None:
        # 先写文件（包含 __final__ 标记）
        if not write_final(events_file, event_buffer):
            self._deps.flush_stages(task_id, event_buffer)
        try:
            db_gen = self._deps.get_db()
            db = next(db_gen)
            try:
                row = self._deps.task_repository.get_task(db, task_id)
                project_id = getattr(row, "project_id", None)
                self._deps.task_repository.finalize_task_error(
                    db,
                    task_id=task_id,
                    lease_epoch=lease_epoch,
                    error=str(exc),
                )
                self._deps.record_timeline_event(
                    task_id=task_id,
                    project_id=project_id,
                    event_type="task_execution_lock_conflict" if isinstance(exc, TaskExecutionLockConflict) else "task_error",
                    message="任务执行锁冲突，任务执行异常结束" if isinstance(exc, TaskExecutionLockConflict) else "任务执行异常结束",
                    level="error",
                    payload={
                        "runner_instance_id": WORKER_INSTANCE_ID,
                        "runner_process_token": RUNNER_PROCESS_TOKEN,
                        "lease_epoch": lease_epoch,
                        "error": str(exc),
                        "error_kind": getattr(exc, "conflict_kind", None),
                        "error_payload": getattr(exc, "payload", None),
                        "events_file": str(events_file) if events_file is not None else None,
                    },
                )
                if row is not None:
                    row.result_json = self._deps.merge_result_json(row.result_json, {"agent_cleanup": {"pre": pre_cleanup_report}})
                    db.commit()
            finally:
                try:
                    next(db_gen)
                except StopIteration:
                    pass
        except Exception:
            import traceback
            traceback.print_exc()
            pass

    def _supervise_running_task(self, task_id: str, lease_epoch: int, orch: Orchestrator) -> None:
        loop_interval = max(1.0, min(self._settings.task_cancel_poll_interval_seconds, self._settings.task_lease_heartbeat_seconds))
        last_heartbeat_ts = 0.0
        heartbeat_failures = 0
        while True:
            time.sleep(loop_interval)
            db = None
            try:
                db_gen = self._deps.get_db()
                db = next(db_gen)
                row = self._deps.task_repository.get_task(db, task_id)
                if not row or row.status == "cancelled":
                    orch.stop()
                    return
                if row.dispatcher_instance_id != WORKER_INSTANCE_ID or int(row.lease_epoch or 0) != lease_epoch:
                    orch.stop()
                    return
                now_ts = _time.time()
                if now_ts - last_heartbeat_ts >= self._settings.task_lease_heartbeat_seconds:
                    updated = self._deps.task_repository.heartbeat_task_lease(
                        db,
                        task_id=task_id,
                        lease_epoch=lease_epoch,
                        worker_instance_id=WORKER_INSTANCE_ID,
                        lease_deadline=lease_deadline,
                    )
                    if not updated:
                        heartbeat_failures += 1
                        self._deps.record_timeline_event(
                            task_id=task_id,
                            project_id=getattr(row, "project_id", None),
                            event_type="task_lease_heartbeat_degraded",
                            message="任务租约续租失败，进入降级观察",
                            level="warning",
                            payload={
                                "lease_epoch": lease_epoch,
                                "dispatcher_instance_id": WORKER_INSTANCE_ID,
                                "heartbeat_failures": heartbeat_failures,
                            },
                        )
                        if heartbeat_failures >= LEASE_HEARTBEAT_FAILURE_TOLERANCE:
                            self._deps.record_timeline_event(
                                task_id=task_id,
                                project_id=getattr(row, "project_id", None),
                                event_type="task_lease_lost_stop_requested",
                                message="任务租约连续续租失败，停止当前执行",
                                level="warning",
                                payload={
                                    "lease_epoch": lease_epoch,
                                    "dispatcher_instance_id": WORKER_INSTANCE_ID,
                                    "heartbeat_failures": heartbeat_failures,
                                },
                            )
                            orch.stop()
                            return
                        continue
                    heartbeat_failures = 0
                    last_heartbeat_ts = now_ts
            except Exception:
                logger.exception(
                    "supervisor loop exception (task_id=%s lease_epoch=%s), will retry",
                    task_id, lease_epoch,
                )
            finally:
                if db is not None:
                    try:
                        next(db_gen)
                    except StopIteration:
                        pass
