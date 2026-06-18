"""
llm_provider_sync.py — 从平台配置中心同步 LLM Provider，生成 pi 的 models.json
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Iterable

import httpx

logger = logging.getLogger("sa.llm_sync")

# pi 的 models.json 写入目录（与 Dockerfile 中 PI_CODING_AGENT_DIR 一致）
_PI_DIR = os.environ.get("PI_CODING_AGENT_DIR", "/root/.pi/agent")
_DEFAULT_CONTEXT_WINDOW = 128000
_REQUIRED_MODEL_FIELDS = ("contextWindow", "contextLength")
_DEFAULT_THINKING_LEVEL_MAP = {"disabled": "disabled"}


def _env_var_name(provider_key: str) -> str:
    """将 provider_key 转换为安全的环境变量名。"""
    safe = provider_key.upper().replace("-", "_").replace(".", "_").replace("/", "_")
    return f"SA_LLM_{safe}_KEY"


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


def _model_entries(provider: dict[str, Any]) -> list[dict[str, Any]]:
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
    max_tokens = _as_positive_int(
        provider.get("max_tokens") or extra_config.get("max_tokens"),
        0,
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
        entry.setdefault("id", model_id)
        entry.setdefault("name", entry.get("id") or model_id)
        entry.setdefault("reasoning", False)
        thinking_level_map = entry.get("thinkingLevelMap")
        if not isinstance(thinking_level_map, dict):
            thinking_level_map = {}
        thinking_level_map.setdefault("disabled", "disabled")
        entry["thinkingLevelMap"] = thinking_level_map
        entry.setdefault("input", ["text"])
        entry.setdefault("contextWindow", context_window)
        entry.setdefault("cost", {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0})
        # Keep contextLength for compatibility, but pi examples use contextWindow.
        entry.setdefault("contextLength", entry["contextWindow"])
        models.append(entry)
    return models


def build_models_json(providers: list[dict[str, Any]]) -> dict:
    """
    将配置中心的 LlmProviderSummary 列表转换为 pi 的 models.json 格式。

    pi models.json 格式：
    {
        "providers": {
            "<provider_key>": {
                "baseUrl": "...",
                "api": "openai-completions",
                "apiKey": "<ENV_VAR_NAME>",
                "models": [{"id": "<model_id>", "contextWindow": 128000}]
            }
        }
    }
    apiKey 字段是环境变量名，pi 运行时会从 os.environ 中读取实际密钥。
    contextWindow 控制 pi 的上下文自动压缩阈值；若配置中心未提供则默认 128000。
    """
    result: dict[str, Any] = {"providers": {}}
    for p in providers:
        if not p.get("enabled"):
            continue
        key = p.get("provider_key", "").strip()
        if not key:
            continue
        api_key_raw = p.get("api_key", "").strip()

        result["providers"][key] = {
            "baseUrl": p.get("api_base", ""),
            "api": _provider_api(str(p.get("provider_type") or "")),
            "apiKey": api_key_raw,
            "models": _model_entries(p),
        }
    return result


def get_pi_models_path() -> Path:
    return Path(_PI_DIR) / "models.json"


def write_pi_models_file(models_json: dict[str, Any], *, source: str) -> dict[str, Any]:
    pi_dir = Path(_PI_DIR)
    pi_dir.mkdir(parents=True, exist_ok=True)
    models_path = get_pi_models_path()

    # 若原来是 symlink（entrypoint.sh 创建），先移除，确保后续写入真实运行时文件
    if models_path.is_symlink():
        models_path.unlink()

    models_path.write_text(
        json.dumps(models_json, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    validation = validate_pi_models_file(models_path)
    logger.info(
        "已写入 pi runtime models.json: source=%s path=%s providers=%s models=%s",
        source,
        validation["path"],
        validation["provider_count"],
        validation["model_count"],
    )
    for model_summary in validation["models"]:
        logger.info(
            "LLM Provider %s/%s contextWindow=%s contextLength=%s",
            model_summary["provider_key"],
            model_summary["model_id"],
            model_summary["contextWindow"],
            model_summary["contextLength"],
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
            summaries.append(
                {
                    "provider_key": provider_key,
                    "model_id": model.get("id"),
                    "contextWindow": model.get("contextWindow"),
                    "contextLength": model.get("contextLength"),
                }
            )
            model_count += 1
    return {
        "path": str(models_path),
        "provider_count": provider_count,
        "model_count": model_count,
        "models": summaries,
    }


def sync_providers_to_pi(
    base_url: str,
    token: str = "",
    timeout: int = 30,
) -> bool:
    """
    从配置中心拉取所有 LLM Provider，写入 pi 的 models.json。

    - 如果 models.json 原来是一个符号链接（指向 /data/config/models.json），
      先删除符号链接再写入真实文件，避免覆盖 ConfigMap 挂载文件。
    - 失败时保留现有 models.json，返回 False。
    """
    url = f"{base_url.rstrip('/')}/service/llm/providers"
    headers: dict[str, str] = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        with httpx.Client(timeout=httpx.Timeout(timeout)) as client:
            resp = client.get(
                url,
                headers=headers,
            )
            if resp.status_code != 200:
                logger.warning("配置中心返回 HTTP %s，跳过 Provider 同步", resp.status_code)
                return False
            data = resp.json()

        items: list[dict] = data.get("items", [])
        if not items:
            logger.warning("配置中心返回空 Provider 列表，跳过同步")
            return False

        models_json = build_models_json(items)
        write_pi_models_file(models_json, source="configcenter")
        return True

    except httpx.HTTPError as e:
        logger.error("连接配置中心失败，跳过同步: %s", e)
    except Exception as e:
        logger.exception("同步 LLM Provider 时发生未知错误: %s", e)
    return False


def apply_models_config_to_pi(models_json: dict[str, Any], *, source: str = "api") -> dict[str, Any]:
    return write_pi_models_file(models_json, source=source)
