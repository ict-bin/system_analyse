"""
llm_provider_sync.py — 从两个来源同步 LLM 模型信息，生成 pi 的 models.json

来源 1: 配置中心 /service/llm/providers (模型配置中心)
  - 直连 Provider (local_minimax, local_codex 等)
  - gaiasec Provider (网关入口, model=auto)

来源 2: AIGW MySQL gaiasec_llm_gateway.model_aliases (网关配置)
  - 网关可路由的模型别名 (auto, max, pro 等)
  - 合并到 gaiasec provider 的 models 列表
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Iterable

import httpx

from app.models import (
    DEFAULT_PI_CHAT_TEMPLATE_KWARGS,
    normalize_bool,
    normalize_pi_chat_template_kwargs,
    normalize_pi_thinking_format,
)

logger = logging.getLogger("sa.llm_sync")

_PI_DIR = os.environ.get("PI_CODING_AGENT_DIR", "/root/.pi/agent")
_DEFAULT_CONTEXT_WINDOW = 128000
_MIN_CONTEXT_WINDOW = 131072        # 128K 最小值, 低于此值的提供商会被提升到此值
_MIN_MAX_OUTPUT_TOKENS = 32768      # 32K 最小 max output tokens, 防止网关默认极小值导致模型只输出1个token
_REQUIRED_MODEL_FIELDS = ("contextWindow", "contextLength")


def _provider_api(provider_type: str) -> str:
    normalized = str(provider_type or "").strip().lower()
    if normalized == "anthropic":
        return "anthropic-messages"
    return "openai-completions"


def _as_positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
        return parsed if parsed > 0 else default
    except (TypeError, ValueError):
        return default


def _normalize_thinking_config(thinking_config: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = thinking_config if isinstance(thinking_config, dict) else {}
    return {
        "pi_thinking_format": normalize_pi_thinking_format(raw.get("pi_thinking_format")),
        "pi_chat_template_kwargs": normalize_pi_chat_template_kwargs(
            raw.get("pi_chat_template_kwargs", DEFAULT_PI_CHAT_TEMPLATE_KWARGS)
        ),
        "pi_supports_reasoning_effort": normalize_bool(
            raw.get("pi_supports_reasoning_effort"),
            default=False,
        ),
    }


def _load_runtime_thinking_config() -> dict[str, Any]:
    try:
        from app import db as _dbmod

        if _dbmod._SessionLocal is None:
            return _normalize_thinking_config()
        with _dbmod._SessionLocal() as db:
            from app.service.config_service import get_config_service

            return _normalize_thinking_config(get_config_service().get_config(db))
    except Exception as exc:
        logger.warning("failed to load SA thinking config from DB, using defaults: %s", exc)
        return _normalize_thinking_config()


def _apply_reasoning_compat(entry: dict[str, Any], thinking_config: dict[str, Any] | None = None) -> None:
    normalized = _normalize_thinking_config(thinking_config)
    entry["reasoning"] = True
    compat = dict(entry.get("compat") or {})
    compat["thinkingFormat"] = normalized["pi_thinking_format"]
    compat["supportsDeveloperRole"] = False
    if normalized["pi_thinking_format"] == "together":
        compat["supportsReasoningEffort"] = normalized["pi_supports_reasoning_effort"]
    else:
        compat.pop("supportsReasoningEffort", None)
    if normalized["pi_thinking_format"] == "chat-template":
        compat["chatTemplateKwargs"] = normalized["pi_chat_template_kwargs"]
    else:
        compat.pop("chatTemplateKwargs", None)
    entry["compat"] = compat


def _model_entries(provider: dict[str, Any], thinking_config: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    model_id = str(provider.get("model") or "").strip()
    extra_config = provider.get("extra_config") if isinstance(provider.get("extra_config"), dict) else {}
    context_window = _as_positive_int(
        provider.get("model_context_window")
        or provider.get("context_window")
        or provider.get("contextWindow")
        or provider.get("context_length")
        or provider.get("contextLength")
        or extra_config.get("model_context_window")
        or extra_config.get("contextWindow")
        or extra_config.get("context_length")
        or extra_config.get("contextLength"),
        _DEFAULT_CONTEXT_WINDOW,
    )
    pi_models = extra_config.get("pi_models")
    raw_models = pi_models if isinstance(pi_models, list) else (
        [{"id": model_id, "reasoning": False}] if model_id else []
    )
    models: list[dict[str, Any]] = []
    for raw in raw_models:
        if not isinstance(raw, dict):
            continue
        entry = dict(raw)
        # Strip any maxTokens restriction to allow unlimited LLM output
        # 但设置最小 max_output_tokens = 32K, 防止网关默认极小值导致 stopReason="length" 死循环
        for _k in list(entry.keys()):
            if _k.lower() in ("maxtokens", "max_tokens", "max_output_tokens", "maxoutputtokens"):
                entry.pop(_k, None)
        entry["maxTokens"] = _MIN_MAX_OUTPUT_TOKENS
        entry.setdefault("id", model_id)
        entry.setdefault("name", entry.get("id") or model_id)
        thinking_level_map = entry.get("thinkingLevelMap")
        if not isinstance(thinking_level_map, dict):
            thinking_level_map = {}
        thinking_level_map.setdefault("disabled", "disabled")
        entry["thinkingLevelMap"] = thinking_level_map
        entry.setdefault("input", ["text"])
        entry.setdefault("contextWindow", context_window)
        # 强制 contextWindow 不低于 128K (gaiasec 报 8192 会被提升到 131072)
        if int(entry.get("contextWindow") or 0) < _MIN_CONTEXT_WINDOW:
            entry["contextWindow"] = _MIN_CONTEXT_WINDOW
        entry.setdefault("contextLength", entry["contextWindow"])
        entry.setdefault("cost", {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0})
        _apply_reasoning_compat(entry, thinking_config=thinking_config)
        models.append(entry)
    return models


def build_models_json(
    providers: list[dict[str, Any]],
    gateway_model_aliases: list[dict[str, Any]] | None = None,
    thinking_config: dict[str, Any] | None = None,
) -> dict:
    """
    将配置中心的 LlmProviderSummary 列表 + AIGW model_aliases 转换为 pi 的 models.json。
    
    gateway_model_aliases 中的别名会合并到 gaiasec provider 的 models 列表。
    """
    result: dict[str, Any] = {"providers": {}}
    normalized_thinking_config = _normalize_thinking_config(thinking_config)
    for p in providers:
        if not p.get("enabled"):
            continue
        key = p.get("provider_key", "").strip()
        if not key:
            continue
        models = _model_entries(p, thinking_config=normalized_thinking_config)
        # Source 2: AIGW model aliases → 合并到 gaiasec provider
        if key == "gaiasec" and gateway_model_aliases:
            alias_models: list[dict[str, Any]] = []
            for a in gateway_model_aliases:
                if not a.get("enabled"):
                    continue
                alias_name = str(a.get("alias_name") or "").strip()
                if not alias_name:
                    continue
                _alias_cw = _as_positive_int(a.get("max_tokens_default"), _DEFAULT_CONTEXT_WINDOW)
                if _alias_cw < _MIN_CONTEXT_WINDOW:
                    _alias_cw = _MIN_CONTEXT_WINDOW
                alias_models.append({
                    "id": alias_name,
                    "name": alias_name,
                    "input": ["text"],
                    "contextWindow": _alias_cw,
                    "contextLength": _alias_cw,
                    "maxTokens": _MIN_MAX_OUTPUT_TOKENS,
                    "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0},
                })
                _apply_reasoning_compat(alias_models[-1], thinking_config=normalized_thinking_config)
            if alias_models:
                models = alias_models
        result["providers"][key] = {
            "baseUrl": p.get("api_base", ""),
            "api": _provider_api(str(p.get("provider_type") or "")),
            "apiKey": p.get("api_key", ""),
            "models": models,
        }
    return result


def _fetch_gateway_model_aliases() -> list[dict[str, Any]]:
    """从 AIGW MySQL 数据库拉取网关配置的 model aliases (来源 2)。
    
    环境变量:
      AIGW_DB_HOST (默认 gaiasec-llm-gateway-mysql)
      AIGW_DB_PORT (默认 3306)
      AIGW_DB_USER (默认 sa)
      AIGW_DB_PASSWORD
      AIGW_DB_NAME (默认 gaiasec_llm_gateway)
    """
    host = os.environ.get("AIGW_DB_HOST", "gaiasec-llm-gateway-mysql")
    port = int(os.environ.get("AIGW_DB_PORT", "3306"))
    user = os.environ.get("AIGW_DB_USER", "sa")
    password = os.environ.get("AIGW_DB_PASSWORD", "")
    db_name = os.environ.get("AIGW_DB_NAME", "gaiasec_llm_gateway")
    if not password:
        logger.warning("AIGW_DB_PASSWORD 未设置，跳过网关模型别名同步")
        return []
    try:
        import pymysql
        conn = pymysql.connect(host=host, port=port, user=user, password=password, database=db_name, connect_timeout=5)
        c = conn.cursor(pymysql.cursors.DictCursor)
        c.execute("SELECT alias_name, max_tokens_default, temperature_default, enabled, description FROM model_aliases")
        rows = c.fetchall()
        conn.close()
        logger.info("从 AIGW DB 同步 %d 个 model aliases", len(rows))
        return rows
    except Exception as e:
        logger.warning("AIGW DB model aliases 同步失败: %s", e)
        return []


def sync_providers_to_pi(
    base_url: str,
    token: str = "",
    timeout: int = 30,
) -> bool:
    """
    从配置中心拉取所有 LLM Provider，写入 pi 的 models.json。

    - 如果 models.json 原来是符号链接，先删除再写入真实文件。
    - 失败时保留现有 models.json，返回 False。
    """
    url = f"{base_url.rstrip('/')}/service/llm/providers"
    headers: dict[str, str] = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        resp = httpx.get(url, headers=headers, timeout=timeout)
        if resp.status_code != 200:
            logger.warning("配置中心返回 HTTP %s，跳过 Provider 同步", resp.status_code)
            return False

        data = resp.json()
        items: list[dict] = data.get("items", [])
        if not items:
            logger.warning("配置中心返回空 Provider 列表，跳过同步")
            return False

        models_json = build_models_json(
            items,
            gateway_model_aliases=_fetch_gateway_model_aliases(),
            thinking_config=_load_runtime_thinking_config(),
        )
        enabled_count = len(models_json["providers"])

        pi_dir = Path(_PI_DIR)
        pi_dir.mkdir(parents=True, exist_ok=True)
        models_path = pi_dir / "models.json"

        # 若原来是 symlink（entrypoint.sh 创建），先移除
        if models_path.is_symlink():
            models_path.unlink()

        models_path.write_text(
            json.dumps(models_json, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info(
            "已从配置中心同步 %d 个 Provider 到 %s", enabled_count, models_path
        )
        for provider_key, provider_cfg in models_json["providers"].items():
            for model in provider_cfg.get("models", []):
                logger.info(
                    "LLM Provider %s/%s contextWindow=%s maxTokens=%s",
                    provider_key,
                    model.get("id"),
                    model.get("contextWindow"),
                    model.get("maxTokens"),
                )
        return True

    except httpx.RequestError as e:
        logger.error("连接配置中心失败，跳过同步: %s", e)
    except Exception as e:
        logger.exception("同步 LLM Provider 时发生未知错误: %s", e)
    return False


def get_pi_models_path() -> Path:
    return Path(_PI_DIR) / "models.json"


def write_pi_models_file(models_json: dict[str, Any], *, source: str) -> dict[str, Any]:
    pi_dir = Path(_PI_DIR)
    pi_dir.mkdir(parents=True, exist_ok=True)
    models_path = get_pi_models_path()
    if models_path.is_symlink():
        models_path.unlink()
    models_path.write_text(json.dumps(models_json, ensure_ascii=False, indent=2), encoding="utf-8")
    validation = validate_pi_models_file(models_path)
    logger.info(
        "已写入 pi runtime models.json: source=%s path=%s providers=%s models=%s",
        source, validation["path"], validation["provider_count"], validation["model_count"],
    )
    return validation


def validate_pi_models_file(
    path: Path | None = None,
    *,
    required_fields: Iterable[str] = _REQUIRED_MODEL_FIELDS,
) -> dict[str, Any]:
    models_path = path or get_pi_models_path()
    if not models_path.exists():
        raise RuntimeError(f"pi models.json 不存在: {models_path}")
    if models_path.is_symlink():
        raise RuntimeError(f"pi models.json 仍是符号链接，未切换到运行时文件: {models_path}")
    try:
        payload = json.loads(models_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"pi models.json 读取失败: {models_path}: {exc}") from exc
    providers = payload.get("providers")
    if not isinstance(providers, dict) or not providers:
        raise RuntimeError(f"pi models.json provider 配置为空: {models_path}")
    required = tuple(required_fields)
    provider_count = 0
    model_count = 0
    summaries: list[dict[str, Any]] = []
    for provider_key, provider_cfg in providers.items():
        if not isinstance(provider_cfg, dict):
            raise RuntimeError(f"provider 配置格式非法: {provider_key}")
        models = provider_cfg.get("models")
        if not isinstance(models, list) or not models:
            raise RuntimeError(f"provider 未配置 models: {provider_key}")
        provider_count += 1
        for model in models:
            if not isinstance(model, dict):
                raise RuntimeError(f"model 配置格式非法: {provider_key}")
            missing = [field for field in required if not model.get(field)]
            if missing:
                raise RuntimeError(
                    f"provider {provider_key}/{model.get('id') or '<unknown>'} 缺少字段: {', '.join(missing)}"
                )
            summaries.append({
                "provider_key": provider_key,
                "model_id": model.get("id"),
                "contextWindow": model.get("contextWindow"),
                "contextLength": model.get("contextLength"),
            })
            model_count += 1
    return {"path": str(models_path), "provider_count": provider_count, "model_count": model_count, "models": summaries}


def apply_models_config_to_pi(models_json: dict[str, Any], *, source: str = "api") -> dict[str, Any]:
    return write_pi_models_file(models_json, source=source)
