import tempfile
from pathlib import Path
from unittest.mock import patch

from app.models import (
    AgentInstanceConfig,
    RoleConfig,
    StageLoopConfig,
    StagesConfig,
    TaskConfig,
    TaskStatus,
)
from app.orchestrator import Orchestrator, StageError, _check_voting, _parse_eval_md


def _make_config(tmp: str) -> TaskConfig:
    return TaskConfig(
        task="test",
        target_dir=str(Path(tmp) / "target"),
        analyse_targets=["binary"],
        binary_arch=["all"],
        agent_max_retries=1,
        agent_retry_delay=0,
        pi_max_retries=1,
        pi_retry_delay=0,
        stages=StagesConfig(
            classify=StageLoopConfig(min_rounds=1, max_rounds=2, pass_mode="all"),
            refine=StageLoopConfig(min_rounds=1, max_rounds=2, pass_mode="all"),
            analyse=StageLoopConfig(min_rounds=1, max_rounds=2, pass_mode="all"),
            final_check=StageLoopConfig(min_rounds=1, max_rounds=2, pass_mode="all"),
        ),
        workers=RoleConfig(
            default_tools=["read"],
            system_prompt_dir=str(Path(tmp) / "prompts" / "workers"),
            agents=[AgentInstanceConfig(model="vllm/glm5")],
        ),
        judges=RoleConfig(
            default_tools=["read"],
            system_prompt_dir=str(Path(tmp) / "prompts" / "judges"),
            agents=[AgentInstanceConfig(model="vllm/glm5")],
        ),
        output_dir=str(Path(tmp) / "output"),
        archive_dir=str(Path(tmp) / "output"),
        result_dir=str(Path(tmp) / "result"),
    )


def test_orchestrator_compat_helper_exports_delegate_to_current_helpers():
    assert _parse_eval_md("## 评分: 80")["pass"] is True
    assert _check_voting([{"pass": True}, {"pass": False}], "any", 2) is True


def test_orchestrator_empty_source_short_circuits_to_passed():
    with tempfile.TemporaryDirectory() as tmp:
        Path(tmp, "output").mkdir(parents=True, exist_ok=True)
        Path(tmp, "result").mkdir(parents=True, exist_ok=True)
        cfg = _make_config(tmp)

        with patch("app.orchestrator.sync_providers_to_pi", return_value=True), patch(
            "app.orchestrator.validate_pi_models_file",
            return_value={"path": "/tmp/models.json", "provider_count": 1, "model_count": 1, "models": []},
        ):
            result = Orchestrator(cfg, skip_provider_sync=True).execute("task-1")

        assert result.status == TaskStatus.PASSED
        assert (Path(tmp) / "output" / "task-1" / "output" / "final_report.md").exists()


def test_orchestrator_marks_stage_error_as_failed():
    with tempfile.TemporaryDirectory() as tmp:
        Path(tmp, "target").mkdir(parents=True, exist_ok=True)
        Path(tmp, "target", "app.bin").write_text("bin", encoding="utf-8")
        Path(tmp, "output").mkdir(parents=True, exist_ok=True)
        Path(tmp, "result").mkdir(parents=True, exist_ok=True)
        cfg = _make_config(tmp)

        with patch("app.orchestrator.validate_pi_models_file", return_value={"path": "/tmp/models.json", "provider_count": 1, "model_count": 1, "models": []}), patch(
            "app.orchestrator.Pipeline.run",
            side_effect=StageError("broken-stage"),
        ):
            result = Orchestrator(cfg, skip_provider_sync=True).execute("task-2")

        assert result.status == TaskStatus.FAILED
        assert result.error == "broken-stage"


def test_orchestrator_stop_sets_cancel_event_when_present():
    orch = Orchestrator(TaskConfig(task="x", target_dir=".", analyse_targets=["binary"]))
    from threading import Event

    evt = Event()
    orch._cancel_event = evt
    orch.stop()
    assert evt.is_set() is True
