import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from app.service import llm_provider_sync
from app.service.llm_provider_sync import build_models_json


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def json(self) -> dict:
        return self._payload


def _provider_payload() -> dict:
    return {
        "items": [
            {
                "enabled": True,
                "provider_key": "icsl_litellm",
                "provider_type": "openai",
                "api_base": "http://api.ai.icsl.huawei.com/v1",
                "api_key": "sk-test",
                "model": "zai-org/GLM-5.1-180K",
            }
        ]
    }


def _thinking_config(mode: str) -> dict:
    payload = {
        "pi_thinking_format": mode,
        "pi_chat_template_kwargs": {
            "thinking": {
                "$var": "thinking.enabled",
            }
        },
        "pi_supports_reasoning_effort": False,
    }
    if mode == "together":
        payload["pi_supports_reasoning_effort"] = True
    return payload


def test_build_models_json_adds_glm_compatible_thinking_level_map():
    payload = build_models_json(
        [
            {
                "enabled": True,
                "provider_key": "icsl_litellm",
                "provider_type": "openai",
                "api_base": "http://api.ai.icsl.huawei.com/v1",
                "api_key": "sk-test",
                "model": "zai-org/GLM-5.1-180K",
            }
        ]
    )

    model = payload["providers"]["icsl_litellm"]["models"][0]
    assert model["reasoning"] is True
    assert model["thinkingLevelMap"]["disabled"] == "disabled"
    assert model["compat"]["thinkingFormat"] == "qwen-chat-template"
    assert model["compat"]["supportsDeveloperRole"] is False


def test_sync_providers_to_pi_loads_thinking_config_from_db():
    provider_payload = _provider_payload()

    with tempfile.TemporaryDirectory() as pi_dir:
        models_path = Path(pi_dir) / "models.json"
        with patch.dict(llm_provider_sync.os.environ, {"PI_CODING_AGENT_DIR": pi_dir}, clear=False):
            with patch.object(llm_provider_sync, "_PI_DIR", pi_dir):
                with patch.object(llm_provider_sync.httpx, "get", return_value=_FakeResponse(provider_payload)):
                    with patch.object(llm_provider_sync, "_fetch_gateway_model_aliases", return_value=[]):
                        with patch.object(
                            llm_provider_sync,
                            "_load_runtime_thinking_config",
                            return_value=_thinking_config("openrouter"),
                        ):
                            ok = llm_provider_sync.sync_providers_to_pi("http://config-center")

        assert ok is True
        written = json.loads(models_path.read_text(encoding="utf-8"))
        assert written["providers"]["icsl_litellm"]["models"][0]["compat"]["thinkingFormat"] == "openrouter"


def test_sync_providers_to_pi_writes_expected_compat_for_each_thinking_format():
    provider_payload = _provider_payload()
    expectations = {
        "reasoning_effort": {"thinkingFormat": "reasoning_effort"},
        "openrouter": {"thinkingFormat": "openrouter"},
        "deepseek": {"thinkingFormat": "deepseek"},
        "together": {"thinkingFormat": "together", "supportsReasoningEffort": True},
        "zai": {"thinkingFormat": "zai"},
        "qwen": {"thinkingFormat": "qwen"},
        "chat-template": {
            "thinkingFormat": "chat-template",
            "chatTemplateKwargs": {"thinking": {"$var": "thinking.enabled"}},
        },
        "qwen-chat-template": {"thinkingFormat": "qwen-chat-template"},
    }

    for mode, expected in expectations.items():
        with tempfile.TemporaryDirectory() as pi_dir:
            models_path = Path(pi_dir) / "models.json"
            with patch.dict(llm_provider_sync.os.environ, {"PI_CODING_AGENT_DIR": pi_dir}, clear=False):
                with patch.object(llm_provider_sync, "_PI_DIR", pi_dir):
                    with patch.object(llm_provider_sync.httpx, "get", return_value=_FakeResponse(provider_payload)):
                        with patch.object(llm_provider_sync, "_fetch_gateway_model_aliases", return_value=[]):
                            with patch.object(
                                llm_provider_sync,
                                "_load_runtime_thinking_config",
                                return_value=_thinking_config(mode),
                            ):
                                ok = llm_provider_sync.sync_providers_to_pi("http://config-center")
            assert ok is True
            written = json.loads(models_path.read_text(encoding="utf-8"))
            model = written["providers"]["icsl_litellm"]["models"][0]
            compat = model["compat"]
            assert model["reasoning"] is True
            assert compat["thinkingFormat"] == expected["thinkingFormat"]
            assert compat["supportsDeveloperRole"] is False
            if "supportsReasoningEffort" in expected:
                assert compat["supportsReasoningEffort"] == expected["supportsReasoningEffort"]
            else:
                assert "supportsReasoningEffort" not in compat
            if "chatTemplateKwargs" in expected:
                assert compat["chatTemplateKwargs"] == expected["chatTemplateKwargs"]
            else:
                assert "chatTemplateKwargs" not in compat
