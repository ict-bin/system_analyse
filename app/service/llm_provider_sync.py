"""
app/service/llm_provider_sync.py — LLM Provider 同步服务（已适配 LangChain）

原版行为：从配置中心拉取 Provider 列表 → 写入 pi 的 models.json
新版行为：从配置中心拉取 Provider 列表 → 更新 app.model_factory 的内存缓存
         同时保留写入 /data/config/models.json 文件的逻辑，
         以便 model_factory 重启后可从文件恢复（向后兼容）。

build_models_json() 函数保持原有签名，供 REST API 或脚本调用。
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import aiohttp

logger = logging.getLogger("sa.llm_sync")

# 与原版保持相同的环境变量（兼容旧配置），但不再写入 pi 目录
_DATA_CONFIG_DIR = os.environ.get("CONFIG_DIR", "/data/config")


def _env_var_name(provider_key: str) -> str:
    """将 provider_key 转换为安全的环境变量名（保留原版逻辑）。"""
    safe = provider_key.upper().replace("-", "_").replace(".", "_").replace("/", "_")
    return f"SA_LLM_{safe}_KEY"


def build_models_json(providers: list[dict[str, Any]]) -> dict:
    """
    将配置中心的 LlmProviderSummary 列表转换为 models.json 格式。

    与原版完全兼容，供测试和外部调用。
    """
    _DEFAULT_CONTEXT_LENGTH = 131072

    result: dict[str, Any] = {"providers": {}}
    for p in providers:
        if not p.get("enabled"):
            continue
        key = p.get("provider_key", "").strip()
        if not key:
            continue
        model_id      = p.get("model", "").strip()
        api_key_raw   = p.get("api_key", "").strip()
        context_length = int(p.get("context_length") or 0) or _DEFAULT_CONTEXT_LENGTH

        model_entry: dict[str, Any] = {
            "id": model_id,
            "reasoning": False,
            "contextLength": context_length,
        }
        result["providers"][key] = {
            "baseUrl":  p.get("api_base", ""),
            "api":      "openai-completions",
            "apiKey":   api_key_raw,
            "models":   [model_entry] if model_id else [],
        }
    return result


async def sync_providers_to_pi(
    base_url: str,
    token: str = "",
    timeout: int = 30,
) -> bool:
    """
    从配置中心同步 LLM Provider。

    新版行为（pi 已移除）：
      1. 拉取配置中心数据
      2. 更新 app.model_factory 的内存 Provider 缓存（立即生效）
      3. 同时写入 /data/config/models.json（持久化，重启后可恢复）

    失败时保留现有配置，返回 False。
    """
    url = f"{base_url.rstrip('/')}/service/llm/providers"
    headers: dict[str, str] = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                if resp.status != 200:
                    logger.warning("Config center returned HTTP %s, skipping sync", resp.status)
                    return False
                data = await resp.json()

        items: list[dict] = data.get("items", [])
        if not items:
            logger.warning("Config center returned empty provider list, skipping sync")
            return False

        models_json = build_models_json(items)
        enabled_count = len(models_json["providers"])

        # ── 更新 model_factory 内存缓存（核心：立即生效）────────────
        try:
            from app.model_factory import update_providers
            update_providers(models_json["providers"])
        except Exception as exc:
            logger.warning("Failed to update model_factory cache: %s", exc)

        # ── 持久化到 /data/config/models.json（重启恢复）────────────
        try:
            config_dir = Path(_DATA_CONFIG_DIR)
            config_dir.mkdir(parents=True, exist_ok=True)
            models_path = config_dir / "models.json"
            # 若是符号链接先移除（与原版行为一致）
            if models_path.is_symlink():
                models_path.unlink()
            models_path.write_text(
                json.dumps(models_json, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            logger.info(
                "Synced %d provider(s) from config center → %s",
                enabled_count, models_path,
            )
        except Exception as exc:
            logger.warning("Failed to persist models.json: %s", exc)

        return True

    except aiohttp.ClientError as exc:
        logger.error("Cannot reach config center, skipping sync: %s", exc)
    except Exception as exc:
        logger.exception("Unexpected error during provider sync: %s", exc)
    return False
