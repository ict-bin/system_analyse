import asyncio
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.agent_process import AgentProcessHandle
from app import agent_process
from app import runner
from app.service import task_runner


def _overflow_result() -> runner.AgentResult:
    result = runner.AgentResult()
    result.exit_code = 1
    result.error = (
        "400 litellm.BadRequestError: Hosted_vllmException - "
        '{"error":{"message":"You passed 147421 input tokens and requested 16384 output tokens. '
        "However, the model's context length is only 163804 tokens, resulting in a maximum input "
        'length of 147420 tokens. Please reduce the length of the input prompt."}}'
    )
    return result


class RunAgentTests(unittest.TestCase):
    def test_materialize_task_pi_runtime_creates_role_scoped_dirs(self):
        cfg = SimpleNamespace(
            workers=SimpleNamespace(
                default_model="glm-5.1-180k",
                agents=[SimpleNamespace(model="glm-5.1-180k")],
                stage_models={},
                default_tools=["read"],
                default_thinking_level="off",
                system_prompt_dir="/tmp/workers",
            ),
            judges=SimpleNamespace(
                default_model="gpt-5.4",
                agents=[SimpleNamespace(model="gpt-5.4")],
                stage_models={"judge": "gpt-5.4"},
                default_tools=["read"],
                default_thinking_level="off",
                system_prompt_dir="/tmp/judges",
            ),
        )
        with tempfile.TemporaryDirectory() as task_root, tempfile.TemporaryDirectory() as pi_root:
            (Path(pi_root) / "settings.json").write_text('{"theme":"light"}', encoding="utf-8")
            (Path(pi_root) / "models.json").write_text(
                '{"providers":{"lite":{"models":[{"id":"glm-5.1-180k","contextWindow":128000,"contextLength":128000},{"id":"gpt-5.4","contextWindow":128000,"contextLength":128000}]}}}',
                encoding="utf-8",
            )
            with patch.dict(runner.os.environ, {"PI_CODING_AGENT_DIR": pi_root, "PI_MODELS_JSON": str(Path(pi_root) / "models.json")}, clear=False):
                role_dirs, mode = task_runner._materialize_task_pi_runtime(
                    task_root=task_root,
                    agent_task_key={"id": "key-1", "secret": "secret-1"},
                    cfg=cfg,
                )
            self.assertEqual(mode, "task_scoped")
            self.assertIn("workers", role_dirs)
            self.assertIn("judges", role_dirs)
            workers_dir = Path(role_dirs["workers"])
            judges_dir = Path(role_dirs["judges"])
            self.assertTrue((workers_dir / "models.json").is_file())
            self.assertTrue((workers_dir / "settings.json").is_file())
            self.assertTrue((workers_dir / "auth.json").is_file())
            self.assertTrue((judges_dir / "models.json").is_file())
            workers_models = __import__("json").loads((workers_dir / "models.json").read_text(encoding="utf-8"))
            judges_models = __import__("json").loads((judges_dir / "models.json").read_text(encoding="utf-8"))
            workers_settings = __import__("json").loads((workers_dir / "settings.json").read_text(encoding="utf-8"))
            self.assertIn("glm-5.1-180k", (workers_dir / "models.json").read_text(encoding="utf-8"))
            self.assertNotEqual(workers_models, judges_models)
            self.assertTrue(workers_settings["compaction"]["enabled"])
            self.assertEqual(workers_settings["compaction"]["reserveTokens"], 8192)
            self.assertEqual(workers_settings["compaction"]["keepRecentTokens"], 50000)

    def test_runtime_snapshot_includes_role_runtime_files(self):
        cfg = SimpleNamespace(
            workers=SimpleNamespace(
                model_dump=lambda mode="json": {"default_model": "glm-5.1-180k"},
                default_model="glm-5.1-180k",
                default_tools=[],
                default_thinking_level="off",
                system_prompt_dir="/tmp/workers",
                agents=[],
                stage_models={},
            ),
            judges=SimpleNamespace(
                model_dump=lambda mode="json": {"default_model": "gpt-5.4"},
                default_model="gpt-5.4",
                default_tools=[],
                default_thinking_level="off",
                system_prompt_dir="/tmp/judges",
                agents=[],
                stage_models={},
            ),
        )
        with tempfile.TemporaryDirectory() as task_root:
            workers_dir = Path(task_root) / ".pi" / "agents" / "workers"
            judges_dir = Path(task_root) / ".pi" / "agents" / "judges"
            workers_dir.mkdir(parents=True)
            judges_dir.mkdir(parents=True)
            for directory, model in ((workers_dir, "glm-5.1-180k"), (judges_dir, "gpt-5.4")):
                (directory / "models.json").write_text(f'{{"model":"{model}"}}', encoding="utf-8")
                (directory / "settings.json").write_text('{"compaction":{"enabled":true}}', encoding="utf-8")
                (directory / "auth.json").write_text('{"agent_task_key_secret":"masked"}', encoding="utf-8")
            _, role_snapshot, provider_summary, llm_snapshot = task_runner._build_runtime_config_snapshots(
                cfg=cfg,
                agent_task_key={"id": "key-1", "secret": "secret-1"},
                task_pi_dirs={"workers": str(workers_dir), "judges": str(judges_dir)},
                agent_runtime_mode="task_scoped",
            )
            self.assertEqual(llm_snapshot["agent_runtime_mode"], "task_scoped")
            self.assertEqual(role_snapshot["workers"]["runtime_dir"], str(workers_dir))
            self.assertIn("runtime_files", role_snapshot["workers"])
            self.assertEqual(
                llm_snapshot["roles"]["judges"]["runtime_files"]["models_json"]["model"],
                "gpt-5.4",
            )
            self.assertEqual(provider_summary["workers"]["runtime_dir"], str(workers_dir))

    def test_cleanup_orphan_pi_processes_skips_business_pid1_container(self):
        with patch.object(agent_process, "_pid1_is_reaper_process", return_value=False):
            killed = agent_process.cleanup_orphan_pi_processes(lambda _: None, label="test")
        self.assertEqual(killed, 0)

    def test_pid1_reaper_detection_rejects_python_main(self):
        with patch.object(agent_process, "_read_proc_name", return_value="python3"):
            with patch("app.agent_process.os.readlink", return_value="/usr/bin/python3"):
                self.assertFalse(agent_process._pid1_is_reaper_process())

    def test_pid1_reaper_detection_accepts_tini(self):
        with patch.object(agent_process, "_read_proc_name", return_value="tini"):
            self.assertTrue(agent_process._pid1_is_reaper_process())

    def test_agent_process_terminate_tree_force_cleans_group_after_exit(self):
        logs: list[str] = []

        class FakeProc:
            pid = 123
            returncode = 0

            def wait(self):
                return 0

        with patch("app.agent_process.process_group_exists", return_value=True):
            with patch("app.agent_process.os.killpg") as killpg:
                handle = AgentProcessHandle(
                    proc=FakeProc(),
                    label="test",
                    logger=logs.append,
                    pgid=456,
                )
                handle.terminate_tree(reason="cleanup")
                killpg.assert_called_once()
        self.assertTrue(any("cleaning leaked pi process group" in msg for msg in logs))

    def test_run_agent_passes_prompt_via_rpc_payload(self):
        captured = {}

        def fake_run_with_pi_retry(**kwargs):
            captured["args"] = kwargs["args"]
            captured["prompt_text"] = kwargs["prompt"]
            result = runner.AgentResult()
            result.output = "ok"
            result.exit_code = 0
            return result

        long_prompt = "# Task\n\n" + "\n".join(
            f"{idx}. /very/long/path/to/file_{idx}.c" for idx in range(5000)
        )

        with tempfile.TemporaryDirectory() as cwd:
            with patch.object(runner, "_find_pi_command", return_value=["/usr/bin/pi"]):
                with patch.object(runner, "_run_with_pi_retry", side_effect=fake_run_with_pi_retry):
                    result = runner.run_agent(
                        long_prompt,
                        model="test-model",
                        tools=["read"],
                        cwd=cwd,
                    )

        self.assertEqual(result.output, "ok")
        self.assertEqual(captured["prompt_text"], long_prompt)
        self.assertNotIn(long_prompt, captured["args"])

    def test_run_agent_triggers_compaction_then_retries_on_context_overflow(self):
        prompts: list[str] = []

        def fake_run_with_pi_retry(**kwargs):
            prompts.append(kwargs["prompt"])
            if len(prompts) == 1:
                return _overflow_result()
            result = runner.AgentResult()
            result.output = "ok"
            result.exit_code = 0
            return result

        with tempfile.TemporaryDirectory() as cwd:
            with patch.object(runner, "_find_pi_command", return_value=["/usr/bin/pi"]):
                with patch.object(runner, "_run_with_pi_retry", side_effect=fake_run_with_pi_retry):
                    result = runner.run_agent(
                        "analyse module",
                        model="MiniMax/MiniMax-M2.5",
                        tools=["read"],
                        cwd=cwd,
                        session_file="/tmp/test-session.jsonl",
                        max_retries=0,
                        pi_max_retries=0,
                    )

        self.assertEqual(result.output, "ok")
        self.assertEqual(len(prompts), 3)
        self.assertEqual(prompts[0], "analyse module")
        self.assertIn("compaction", prompts[1].lower())
        self.assertEqual(prompts[2], "analyse module")

    def test_run_agent_stops_when_single_input_exceeds_seventy_five_percent(self):
        prompts: list[str] = []

        def fake_run_with_pi_retry(**kwargs):
            prompts.append(kwargs["prompt"])
            if len(prompts) == 1:
                return _overflow_result()
            result = runner.AgentResult()
            result.output = "COMPACTION_OK"
            result.exit_code = 0
            return result

        oversized_prompt = "中" * 130000
        with tempfile.TemporaryDirectory() as cwd:
            with patch.object(runner, "_find_pi_command", return_value=["/usr/bin/pi"]):
                with patch.object(runner, "_run_with_pi_retry", side_effect=fake_run_with_pi_retry):
                    result = runner.run_agent(
                        oversized_prompt,
                        model="MiniMax/MiniMax-M2.5",
                        tools=["read"],
                        cwd=cwd,
                        session_file="/tmp/test-session.jsonl",
                        max_retries=0,
                        pi_max_retries=0,
                    )

        self.assertEqual(len(prompts), 1)
        self.assertIn("compaction", prompts[0].lower())
        self.assertIn("75%", result.error or "")
        self.assertIn("不再继续重试", result.error or "")
        self.assertTrue(result.context_budget_exceeded_preflight)
        self.assertTrue(result.context_overflow_failed_after_compaction)

    def test_run_agent_preflight_without_session_fails_fast(self):
        oversized_prompt = "中" * 130000
        with tempfile.TemporaryDirectory() as cwd:
            with patch.object(runner, "_find_pi_command", return_value=["/usr/bin/pi"]):
                with patch.object(runner, "_run_with_pi_retry") as fake_retry:
                    result = runner.run_agent(
                        oversized_prompt,
                        model="glm-5.1-180k",
                        tools=["read"],
                        cwd=cwd,
                        session_file=None,
                        max_retries=0,
                        pi_max_retries=0,
                    )
        fake_retry.assert_not_called()
        self.assertTrue(result.context_budget_exceeded_preflight)
        self.assertTrue(result.context_overflow_failed_after_compaction)
        self.assertIn("75%", result.error or "")

    def test_run_agent_retries_after_timeout(self):
        attempts = {"count": 0}
        ok_result = runner.AgentResult()
        ok_result.output = "ok"

        def fake_run_with_context_overflow_recovery(**kwargs):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise TimeoutError("timed out")
            return ok_result

        with patch.object(runner, "_find_pi_command", return_value=["/usr/bin/pi"]):
            with patch.object(runner, "_run_with_context_overflow_recovery", side_effect=fake_run_with_context_overflow_recovery):
                result = runner.run_agent(
                    "hello",
                    model="test-model",
                    tools=["read"],
                    cwd=".",
                    run_timeout_seconds=0.01,
                    timeout_retry_enabled=True,
                    timeout_max_retries=1,
                    retry_delay=0,
                )

        self.assertEqual(attempts["count"], 2)
        self.assertEqual(result.output, "ok")

if __name__ == "__main__":
    unittest.main()
