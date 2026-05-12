"""
app/model_factory.py — LangChain 模型工厂

从 models.json 加载 Provider 配置，将形如 "provider/model_id" 的字符串
解析为可用的 LangChain ChatOpenAI 实例。

支持的配置文件搜索路径（优先级从高到低）：
  1. 环境变量 MODELS_JSON_PATH
  2. /data/config/models.json（容器默认）
  3. ./config/models.json（本地开发）
  4. /opt/system_analyse/config/models.json

apiKey 字段处理规则：
  - 全大写且含下划线 → 当作环境变量名，读取 os.environ
  - 否则直接作为 key 值使用
"""
from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import Optional

from langchain_openai import ChatOpenAI  # 模块级导入，便于测试 patch

logger = logging.getLogger("sa.model_factory")

_DEFAULT_SEARCH_PATHS = [
    os.environ.get("MODELS_JSON_PATH", ""),
    "/data/config/models.json",
    "./config/models.json",
    "/opt/system_analyse/config/models.json",
]

# ── 全局 Provider 缓存 ────────────────────────────────────────────────
_providers: dict = {}
_providers_lock = threading.Lock()
_providers_loaded = False


def _load_providers_once() -> dict:
    global _providers, _providers_loaded
    with _providers_lock:
        if _providers_loaded:
            return _providers
        for path_str in _DEFAULT_SEARCH_PATHS:
            if not path_str:
                continue
            p = Path(path_str)
            if p.is_file():
                try:
                    data = json.loads(p.read_text(encoding="utf-8"))
                    _providers = data.get("providers", {})
                    _providers_loaded = True
                    logger.info(
                        "Loaded model config from %s (%d providers)",
                        p, len(_providers),
                    )
                    return _providers
                except Exception as exc:
                    logger.warning("Failed to load model config %s: %s", p, exc)
        logger.warning(
            "No models.json found in search paths. "
            "Models will fall back to OPENAI_API_KEY / OPENAI_API_BASE env vars."
        )
        _providers_loaded = True
        return _providers


def update_providers(new_providers: dict) -> None:
    """运行时热更新 Provider 配置（由 llm_provider_sync 或测试调用）。"""
    global _providers, _providers_loaded
    with _providers_lock:
        _providers = new_providers
        _providers_loaded = True
    logger.info("Model providers updated (%d providers)", len(new_providers))


def _resolve_api_key(raw: str) -> str:
    """
    解析 apiKey 字段：
      - 形如 $VAR 或全大写+下划线 → 当作环境变量名读取
      - 否则直接返回原值
    """
    if not raw:
        return "none"
    key = raw.lstrip("$")
    # 判断是否像环境变量名（全大写字母、数字、下划线，无空格）
    if key and key.replace("_", "").replace("-", "").isupper() and " " not in key:
        env_val = os.environ.get(key)
        if env_val:
            return env_val
        env_val2 = os.environ.get(raw)
        if env_val2:
            return env_val2
    return raw


def _parse_model_string(model_string: str) -> tuple[str | None, str]:
    """
    从 "provider/model_id" 格式中提取 (provider_name, model_id)。

    规则：
      1. 遍历已知 provider key，以 "<key>/" 为前缀精确匹配
      2. 未匹配时，取第一个 "/" 前的部分作为 provider 候选
      3. 候选不在 providers 中 → 整个字符串视为 model_id（无 provider）
    """
    providers = _load_providers_once()

    if "/" not in model_string:
        return None, model_string

    # 精确前缀匹配（处理 model_id 本身含 "/" 的情况，如 "zai-org/GLM-5"）
    for pname in providers:
        prefix = pname + "/"
        if model_string.startswith(prefix):
            return pname, model_string[len(prefix):]

    # 退化：取第一段作为 provider 候选
    parts = model_string.split("/", 1)
    if parts[0] in providers:
        return parts[0], parts[1]

    # 完全无法识别 provider
    return None, model_string


def create_model(model_string: str, thinking_level: str = "off"):
    """
    根据模型字符串创建 LangChain BaseChatModel 实例。

    Args:
        model_string: "provider/model_id" 格式，例如 "vllm/zai-org/GLM-5"
        thinking_level: "off" / "low" / "medium" / "high"（暂仅日志记录）

    Returns:
        ChatOpenAI 实例，失败时抛出异常
    """
    providers = _load_providers_once()
    provider_name, model_id = _parse_model_string(model_string)

    kwargs: dict = {
        "timeout": 600.0,
        "max_retries": 0,  # 重试由 runner.py 外层循环负责
    }

    if provider_name and provider_name in providers:
        pconfig = providers[provider_name]
        base_url = pconfig.get("baseUrl", "").rstrip("/")
        raw_key  = pconfig.get("apiKey") or pconfig.get("api_key") or "none"
        api_key  = _resolve_api_key(raw_key)

        if base_url:
            kwargs["base_url"] = base_url
        kwargs["api_key"] = api_key

        # 检查 reasoning 标志（后续可扩展为 extra_body 参数）
        model_cfg = next(
            (m for m in pconfig.get("models", []) if m.get("id") == model_id),
            {},
        )
        if model_cfg.get("reasoning") and thinking_level not in ("off", ""):
            logger.debug(
                "Model %s supports reasoning; thinking_level=%s (reserved for future use)",
                model_id, thinking_level,
            )
    else:
        # 无 provider 配置，回退到环境变量
        api_key  = (
            os.environ.get("OPENAI_API_KEY")
            or os.environ.get("API_KEY")
            or "none"
        )
        base_url = os.environ.get("OPENAI_API_BASE") or os.environ.get("OPENAI_BASE_URL")
        kwargs["api_key"] = api_key
        if base_url:
            kwargs["base_url"] = base_url
        if provider_name:
            logger.warning(
                "Provider %r not found in models.json; using env vars for model %r",
                provider_name, model_id,
            )

    logger.debug("Creating model: %s (provider=%s)", model_id, provider_name)
    return ChatOpenAI(model=model_id, **kwargs)
