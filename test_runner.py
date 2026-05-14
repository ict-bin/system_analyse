import asyncio
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app import runner


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
    def test_run_agent_passes_prompt_via_rpc_payload(self):
        captured = {}

        async def fake_run_with_pi_retry(**kwargs):
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
                    result = asyncio.run(
                        runner.run_agent(
                            long_prompt,
                            model="test-model",
                            tools=["read"],
                            cwd=cwd,
                        )
                    )

        self.assertEqual(result.output, "ok")
        self.assertEqual(captured["prompt_text"], long_prompt)
        self.assertNotIn(long_prompt, captured["args"])

    def test_run_agent_triggers_compaction_then_retries_on_context_overflow(self):
        prompts: list[str] = []

        async def fake_run_with_pi_retry(**kwargs):
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
                    result = asyncio.run(
                        runner.run_agent(
                            "analyse module",
                            model="MiniMax/MiniMax-M2.5",
                            tools=["read"],
                            cwd=cwd,
                            session_file="/tmp/test-session.jsonl",
                            max_retries=0,
                            pi_max_retries=0,
                        )
                    )

        self.assertEqual(result.output, "ok")
        self.assertEqual(len(prompts), 3)
        self.assertEqual(prompts[0], "analyse module")
        self.assertIn("compaction", prompts[1].lower())
        self.assertEqual(prompts[2], "analyse module")

    def test_run_agent_stops_when_single_input_exceeds_seventy_five_percent(self):
        prompts: list[str] = []

        async def fake_run_with_pi_retry(**kwargs):
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
                    result = asyncio.run(
                        runner.run_agent(
                            oversized_prompt,
                            model="MiniMax/MiniMax-M2.5",
                            tools=["read"],
                            cwd=cwd,
                            session_file="/tmp/test-session.jsonl",
                            max_retries=0,
                            pi_max_retries=0,
                        )
                    )

        self.assertEqual(len(prompts), 2)
        self.assertIn("75%", result.error or "")
        self.assertIn("不再继续重试", result.error or "")

    def test_run_agent_retries_after_timeout(self):
        attempts = {"count": 0}

        async def fake_run_with_pi_retry(**kwargs):
            attempts["count"] += 1
            await asyncio.sleep(0.02)
            result = runner.AgentResult()
            result.output = "ok"
            return result

        with patch.object(runner, "_find_pi_command", return_value=["/usr/bin/pi"]):
            with patch.object(runner, "_run_with_pi_retry", side_effect=fake_run_with_pi_retry):
                result = asyncio.run(
                    runner.run_agent(
                        "hello",
                        model="test-model",
                        tools=["read"],
                        cwd=".",
                        run_timeout_seconds=0.01,
                        timeout_retry_enabled=True,
                        timeout_max_retries=1,
                        retry_delay=0,
                    )
                )

        self.assertEqual(attempts["count"], 2)
        self.assertIn("timed out", result.error or "")


if __name__ == "__main__":
    unittest.main()
