"""Per-project analysis config service."""

from __future__ import annotations

import logging
from typing import Any, Dict

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.config import build_prompt_override_config, load_prompt_defaults
from app.db.models import AppSaModelsConfig, AppSaProjectConfig
from app.models import normalize_max_rounds_exceeded_action

logger = logging.getLogger("sa.config_service")

# Fields in workers/judges that must NOT be stored in DB — always use fixed defaults
_ROLE_READONLY_FIELDS = {"system_prompt_dir"}


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
    "analyse_targets": ["binary", "source"],
    "binary_arch": ["arm", "aarch64"],
    "security_focus_categories": ["all"],
    "module_granularity": "fine",
    "parallel_modules": 20,
    "parallel_sub_workers": 4,
    "agent_max_retries": 5,
    "agent_retry_delay": 60,
    "agent_run_timeout_seconds": 3600,
    "agent_timeout_retry_enabled": True,
    "agent_timeout_max_retries": 3,
    "pi_max_retries": -1,
    "pi_retry_delay": 5,
    "stages": {
        "classify":    {"min_rounds": 1, "max_rounds": 8,  "pass_mode": "all"},
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
            {"model": "gptplus_openai/gpt-5.4", "tools": None, "system_prompt": None, "thinking_level": None},
        ],
        "stage_models": {
            "explore": "gptplus_minimax/MiniMax-M2.7",
        },
    },
    "judges": {
        "default_model": "",
        "default_tools": ["read", "bash", "grep", "find"],
        "system_prompt_dir": "/app/prompts/judges",
        "default_thinking_level": "off",
        "agents": [
            {"model": "gptplus_openai/gpt-5.4", "tools": None, "system_prompt": None, "thinking_level": None},
        ],
        "stage_models": {},
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

    def get_config(self, db: Session, project_id: str) -> dict:
        row = db.query(AppSaProjectConfig).filter_by(project_id=project_id).first()
        if row and row.config_json:
            data = _deep_merge(_DEFAULT_CONFIG, row.config_json)
        else:
            data = dict(_DEFAULT_CONFIG)
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
        data["project_id"] = project_id
        data["updated_at"] = row.updated_at.isoformat() if (row and row.updated_at) else None
        # self_reflection.output_dir 空时自动填充项目级路径
        sr = data.setdefault("self_reflection", {})
        if not sr.get("output_dir"):
            sr["output_dir"] = (
                f"/data/files/{project_id}/app/secflow-app-system-analyse/self-reflection"
            )
        return data

    def save_config(self, db: Session, project_id: str, config_data: dict) -> dict:
        # Strip meta-fields and task-execution-only overrides from the stored blob
        # start_stage / resume_workspace are ephemeral per-run values set by
        # resume_task / restart_task; they must never be persisted in project config.
        _STRIP = {"project_id", "updated_at", "start_stage", "resume_workspace"}
        blob = {k: v for k, v in config_data.items() if k not in _STRIP}
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
        from sqlalchemy.orm.attributes import flag_modified
        row = db.query(AppSaProjectConfig).filter_by(project_id=project_id).first()
        if row:
            row.config_json = blob
            flag_modified(row, "config_json")
        else:
            row = AppSaProjectConfig(project_id=project_id, config_json=blob)
            db.add(row)
        db.commit()
        db.refresh(row)
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
        result["project_id"] = project_id
        result["updated_at"] = row.updated_at.isoformat() if row.updated_at else None
        return result


_config_service: ConfigService | None = None


def get_config_service() -> ConfigService:
    global _config_service
    if _config_service is None:
        _config_service = ConfigService()
    return _config_service


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
