"""Per-project analysis config service."""

from __future__ import annotations

import logging
from typing import Any, Dict

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.config import build_prompt_override_config, load_prompt_defaults
from app.db.models import AppSaModelsConfig, AppSaProjectConfig
from app.models import normalize_max_rounds_exceeded_action

logger = logging.getLogger("sa.config_service")
_GLOBAL_CONFIG_PROJECT_ID = "__global__"

# Fields in workers/judges that must NOT be stored in DB — always use fixed defaults
_ROLE_READONLY_FIELDS = {"system_prompt_dir"}
_RUNTIME_SETTINGS_CONFIG_KEY = "runtime_settings"
_DEFAULT_RUNTIME_SETTINGS: Dict[str, Any] = {
    "worker_task_concurrency": 4,
    "agent_timeout_seconds": 1800.0,
}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge *override* into *base*, returning a new dict.

    Rules:
    - Nested dicts are merged recursively.
    - A ``None`` (or non-dict) value in *override* for a key whose base value is
      a dict is **ignored** — the base dict is kept intact.  This prevents a
      corrupted / partially-migrated stored config from wiping out default
      nested structures (e.g. ``stages``, ``workers``, ``judges``).
    - All other scalar / list values in *override* replace those in *base*.
    """
    result = dict(base)
    for key, val in override.items():
        base_val = result.get(key)
        if isinstance(base_val, dict) and not isinstance(val, dict):
            # Never overwrite a default dict with None / scalar / list
            continue
        if isinstance(base_val, dict) and isinstance(val, dict):
            result[key] = _deep_merge(base_val, val)
        else:
            result[key] = val
    return result

_DEFAULT_CONFIG: Dict[str, Any] = {
    "max_rounds_exceeded_action": "treat_as_passed",
    "continue_on_module_failure": True,
    "enable_final_check": False,
    "analyse_targets": ["binary", "source"],
    "binary_arch": ["all"],
    "security_focus_categories": ["all"],
    "module_granularity": "coarse",
    "filter_engine": "script",
    "worker_task_concurrency": 4,
    "parallel_modules": 4,
    "parallel_sub_workers": 4,
    "agent_max_retries": -1,
    "agent_retry_delay": 3,
    "pi_max_retries": -1,
    "agent_timeout_seconds": 1800.0,
    "pi_retry_delay": 5,
    "model_stuck_timeout": 1800,
    "model_stuck_max_activations": 5,
    "stages": {
        "classify":    {"min_rounds": 1, "max_rounds": -1, "pass_mode": "all"},
        "refine":      {"min_rounds": 1, "max_rounds": -1, "pass_mode": "all"},
        "analyse":     {"min_rounds": 1, "max_rounds": -1, "pass_mode": "all"},
        "final_check": {"min_rounds": 1, "max_rounds": -1, "pass_mode": "all"},
    },
    "workers": {
        "default_model": "",
        "default_tools": ["read", "bash", "edit", "write", "grep", "find"],
        "system_prompt_dir": "/app/prompts/workers",
        "default_thinking_level": "off",
        "agents": [
            {"model": "gaiasec/auto", "tools": None, "system_prompt": None, "thinking_level": None},
        ],
        "stage_models": {
            "explore": "gaiasec/auto",
            "classify": "gaiasec/auto",
            "sub_read": "gaiasec/auto",
            "refine": "gaiasec/auto",
            "analyse": "gaiasec/auto",
            "report": "gaiasec/auto",
        },
    },
    "judges": {
        "default_model": "",
        "default_tools": ["read", "bash", "grep", "find"],
        "system_prompt_dir": "/app/prompts/judges",
        "default_thinking_level": "off",
        "agents": [
            {"model": "gaiasec/auto", "tools": None, "system_prompt": None, "thinking_level": None},
        ],
        "stage_models": {
            "classify": "gaiasec/auto",
            "refine": "gaiasec/auto",
            "analyse": "gaiasec/auto",
            "completeness": "gaiasec/auto",
            "report": "gaiasec/auto",
        },
    },
    "prompt_overrides": {},
    "output_dir": "/data/output",
    "archive_dir": "/data/output",
    "result_dir": "/data/output",
    "start_stage": 0,
    "resume_workspace": "",
    "self_reflection": {
        "enabled": False,
        "model": "",
        "output_dir": "",  # 空 = 自动使用 /data/files/{project_id}/app/secflow-app-system-analyse/self-reflection
        "max_session_lines": 1000,
    },
}


class ConfigService:
    def _latest_legacy_project_row(self, db: Session) -> AppSaProjectConfig | None:
        return (
            db.query(AppSaProjectConfig)
            .filter(AppSaProjectConfig.project_id != _GLOBAL_CONFIG_PROJECT_ID)
            .order_by(AppSaProjectConfig.updated_at.desc())
            .first()
        )

    def _ensure_global_config_row(self, db: Session) -> AppSaProjectConfig | None:
        row = db.query(AppSaProjectConfig).filter_by(project_id=_GLOBAL_CONFIG_PROJECT_ID).first()
        if row is not None:
            return row
        legacy_row = self._latest_legacy_project_row(db)
        if legacy_row is None:
            return None
        migrated = AppSaProjectConfig(
            project_id=_GLOBAL_CONFIG_PROJECT_ID,
            config_json=dict(legacy_row.config_json or {}),
        )
        db.add(migrated)
        db.commit()
        db.refresh(migrated)
        logger.info(
            "migrated system-analysis project config to global config from project %s",
            legacy_row.project_id,
        )
        return migrated

    @staticmethod
    def _sanitize_runtime_settings(raw: Any, *, updated_at: str | None = None) -> dict:
        payload = dict(_DEFAULT_RUNTIME_SETTINGS)
        if isinstance(raw, dict):
            payload.update(raw)
        try:
            worker_task_concurrency = int(payload.get("worker_task_concurrency") or 4)
        except (TypeError, ValueError):
            worker_task_concurrency = 4
        try:
            agent_timeout_seconds = float(payload.get("agent_timeout_seconds") or 1800.0)
        except (TypeError, ValueError):
            agent_timeout_seconds = 1800.0
        return {
            "worker_task_concurrency": max(1, worker_task_concurrency),
            "agent_timeout_seconds": max(60.0, agent_timeout_seconds),
            "updated_at": updated_at,
        }

    def get_runtime_settings(self, db: Session) -> dict:
        row = db.query(AppSaModelsConfig).filter_by(config_key=_RUNTIME_SETTINGS_CONFIG_KEY).first()
        payload = dict(row.config_json) if row and isinstance(row.config_json, dict) else None
        return self._sanitize_runtime_settings(
            payload,
            updated_at=row.updated_at.isoformat() if row and row.updated_at else None,
        )

    def save_runtime_settings(self, db: Session, config_data: dict | None) -> dict:
        sanitized = self._sanitize_runtime_settings(config_data)
        blob = {k: v for k, v in sanitized.items() if k != "updated_at"}
        row = db.query(AppSaModelsConfig).filter_by(config_key=_RUNTIME_SETTINGS_CONFIG_KEY).first()
        if row:
            row.config_json = blob
            flag_modified(row, "config_json")
        else:
            row = AppSaModelsConfig(config_key=_RUNTIME_SETTINGS_CONFIG_KEY, config_json=blob)
            db.add(row)
        db.commit()
        db.refresh(row)
        return self._sanitize_runtime_settings(
            blob,
            updated_at=row.updated_at.isoformat() if row.updated_at else None,
        )

    @staticmethod
    def _extract_stored_prompt_overrides(raw: Any) -> Dict[str, Dict[str, str]]:
        result: Dict[str, Dict[str, str]] = {"workers": {}, "judges": {}}
        if not isinstance(raw, dict):
            return result
        for role in ("workers", "judges"):
            group = raw.get(role)
            if not isinstance(group, dict):
                continue
            for key, value in group.items():
                if isinstance(value, dict):
                    content = str(value.get("content") or "").strip()
                else:
                    content = str(value or "").strip()
                if content:
                    result[role][str(key)] = content
        return result

    @staticmethod
    def _project_prompt_override_blob(raw: Any) -> Dict[str, Dict[str, str]]:
        incoming = ConfigService._extract_stored_prompt_overrides(raw)
        defaults = {
            "workers": load_prompt_defaults("workers"),
            "judges": load_prompt_defaults("judges"),
        }
        cleaned: Dict[str, Dict[str, str]] = {}
        for role in ("workers", "judges"):
            for key, content in incoming.get(role, {}).items():
                if content and content != defaults[role].get(key, ""):
                    cleaned.setdefault(role, {})[key] = content
        return cleaned

    def get_config(self, db: Session, project_id: str | None = None) -> dict:
        row = self._ensure_global_config_row(db)
        if row and row.config_json:
            data = _deep_merge(_DEFAULT_CONFIG, row.config_json)
        else:
            data = dict(_DEFAULT_CONFIG)
        runtime_settings = self.get_runtime_settings(db)
        stored_prompt_overrides = (
            row.config_json.get("prompt_overrides")
            if row and isinstance(row.config_json, dict)
            else None
        )
        data["max_rounds_exceeded_action"] = normalize_max_rounds_exceeded_action(
            data.get("max_rounds_exceeded_action")
        )
        data["prompt_overrides"] = build_prompt_override_config(
            stored_prompt_overrides,
            worker_prompt_dir=data.get("workers", {}).get("system_prompt_dir"),
            judge_prompt_dir=data.get("judges", {}).get("system_prompt_dir"),
        ).model_dump(mode="json")
        data["worker_task_concurrency"] = int(runtime_settings["worker_task_concurrency"])
        data["agent_timeout_seconds"] = float(runtime_settings["agent_timeout_seconds"])
        data["updated_at"] = row.updated_at.isoformat() if (row and row.updated_at) else None
        return data

    def save_config(self, db: Session, config_data: dict, project_id: str | None = None) -> dict:
        # Strip meta-fields and task-execution-only overrides from the stored blob
        # start_stage / resume_workspace are ephemeral per-run values set by
        # resume_task / restart_task; they must never be persisted in project config.
        _STRIP = {"project_id", "updated_at", "start_stage", "resume_workspace", "worker_task_concurrency", "agent_timeout_seconds"}
        blob = {k: v for k, v in config_data.items() if k not in _STRIP}
        current_runtime_settings = self.get_runtime_settings(db)
        runtime_settings_payload = {
            "worker_task_concurrency": config_data.get(
                "worker_task_concurrency",
                current_runtime_settings["worker_task_concurrency"],
            ),
            "agent_timeout_seconds": config_data.get(
                "agent_timeout_seconds",
                current_runtime_settings["agent_timeout_seconds"],
            ),
        }
        blob["max_rounds_exceeded_action"] = normalize_max_rounds_exceeded_action(
            blob.get("max_rounds_exceeded_action")
        )
        prompt_overrides_blob = self._project_prompt_override_blob(blob.get("prompt_overrides"))
        if prompt_overrides_blob:
            blob["prompt_overrides"] = prompt_overrides_blob
        else:
            blob.pop("prompt_overrides", None)
        # Strip read-only role fields so they always fall back to defaults
        for role_key in ("workers", "judges"):
            if isinstance(blob.get(role_key), dict):
                blob[role_key] = {k: v for k, v in blob[role_key].items() if k not in _ROLE_READONLY_FIELDS}
        row = self._ensure_global_config_row(db)
        if row:
            row.config_json = blob
            flag_modified(row, "config_json")
        else:
            row = AppSaProjectConfig(project_id=_GLOBAL_CONFIG_PROJECT_ID, config_json=blob)
            db.add(row)
        db.commit()
        db.refresh(row)
        runtime_settings = self.save_runtime_settings(db, runtime_settings_payload)
        # Return fully-merged config (same shape as get_config)
        result = _deep_merge(_DEFAULT_CONFIG, blob)
        result["max_rounds_exceeded_action"] = normalize_max_rounds_exceeded_action(
            result.get("max_rounds_exceeded_action")
        )
        result["prompt_overrides"] = build_prompt_override_config(
            prompt_overrides_blob,
            worker_prompt_dir=result.get("workers", {}).get("system_prompt_dir"),
            judge_prompt_dir=result.get("judges", {}).get("system_prompt_dir"),
        ).model_dump(mode="json")
        result["worker_task_concurrency"] = int(runtime_settings["worker_task_concurrency"])
        result["agent_timeout_seconds"] = float(runtime_settings["agent_timeout_seconds"])
        result["updated_at"] = row.updated_at.isoformat() if row.updated_at else None
        return result


_config_service: ConfigService | None = None


def get_config_service() -> ConfigService:
    global _config_service
    if _config_service is None:
        _config_service = ConfigService()
    return _config_service


def get_worker_task_concurrency(db: Session) -> int:
    return 1


_DEFAULT_MODELS_CONFIG: Dict[str, Any] = {
    "providers": {
        "icsl_vllm_1": {
            "baseUrl": "http://172.31.29.10:8000/v1/",
            "api": "openai-completions",
            "apiKey": "1234",
            "models": [
                {"id": "zai-org/GLM-5", "reasoning": True},
            ],
        },
        "icsl_vllm_2": {
            "baseUrl": "http://172.31.29.10:8003/v1/",
            "api": "openai-completions",
            "apiKey": "12345",
            "models": [
                {"id": "MiniMax/MiniMax-M2.5", "reasoning": True},
            ],
        },
        "gptplus_glm": {
            "baseUrl": "https://az.gptplus5.com/v1",
            "api": "openai-completions",
            "apiKey": "sk-8zyyvaRQ6QlQzwONikzreTNlRqbLBokuUFH70Akk0AMTcF6y",
            "models": [
                {"id": "glm-5.1", "reasoning": True},
            ],
        },
        "gptplus_minimax": {
            "baseUrl": "https://az.gptplus5.com/v1",
            "api": "openai-completions",
            "apiKey": "sk-8zyyvaRQ6QlQzwONikzreTNlRqbLBokuUFH70Akk0AMTcF6y",
            "models": [
                {"id": "MiniMax-M2.7", "reasoning": True},
            ],
        },
        "gptplus_openai": {
            "baseUrl": "https://az.gptplus5.com/v1",
            "api": "openai-completions",
            "apiKey": "sk-8zyyvaRQ6QlQzwONikzreTNlRqbLBokuUFH70Akk0AMTcF6y",
            "models": [
                {"id": "gpt-5.4", "reasoning": False},
            ],
        },
    }
}


class ModelConfigService:
    """Global models.json configuration stored in the database."""

    def get_models_config(self, db: Session) -> dict:
        try:
            row = db.query(AppSaModelsConfig).filter_by(config_key="global").first()
        except SQLAlchemyError as exc:
            logger.error("Failed to query models config: %s", exc)
            return dict(_DEFAULT_MODELS_CONFIG)
        if row and row.config_json:
            data = dict(row.config_json)
        else:
            data = dict(_DEFAULT_MODELS_CONFIG)
        data["updated_at"] = row.updated_at.isoformat() if (row and row.updated_at) else None
        return data

    def save_models_config(self, db: Session, config_data: dict) -> dict:
        blob = {k: v for k, v in config_data.items() if k != "updated_at"}
        try:
            row = db.query(AppSaModelsConfig).filter_by(config_key="global").first()
            if row:
                row.config_json = blob
            else:
                row = AppSaModelsConfig(config_key="global", config_json=blob)
                db.add(row)
            db.commit()
            db.refresh(row)
        except SQLAlchemyError as exc:
            logger.error("Failed to save models config: %s", exc)
            db.rollback()
            raise
        result = dict(blob)
        result["updated_at"] = row.updated_at.isoformat() if row.updated_at else None
        return result


_model_config_service: ModelConfigService | None = None


def get_model_config_service() -> ModelConfigService:
    global _model_config_service
    if _model_config_service is None:
        _model_config_service = ModelConfigService()
    return _model_config_service
