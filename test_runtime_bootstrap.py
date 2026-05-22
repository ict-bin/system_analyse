import asyncio
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.service.runtime_bootstrap import RuntimeBootstrap


class RuntimeBootstrapTests(unittest.IsolatedAsyncioTestCase):
    async def test_retries_db_init_until_success(self):
        bootstrap = RuntimeBootstrap(
            pool_overrides=lambda svc: (1, 1, 1, 1),
            should_run_migrations=lambda: False,
        )
        app = SimpleNamespace(include_router=lambda router: None)
        init_attempts = []
        worker_loop_starts = []

        def fake_init_db(*args, **kwargs):
            init_attempts.append(1)
            if len(init_attempts) == 1:
                raise RuntimeError("mysql not ready")

        async def fake_start_worker_loop():
            worker_loop_starts.append(1)

        with patch("app.service.runtime_bootstrap.get_service_yaml", return_value=SimpleNamespace(
            database=SimpleNamespace(url="mysql://", host="db", port=3306, name="sa"),
            configcenter=SimpleNamespace(base_url="http://cc", timeout=1),
            auth_service=SimpleNamespace(service_machine_token="token"),
        )), patch("app.service.runtime_bootstrap.DB_INIT_RETRY_SECONDS", 0.01), patch(
            "app.service.runtime_bootstrap.is_api_role",
            return_value=True,
        ), patch(
            "app.service.runtime_bootstrap.is_dispatcher_role",
            return_value=True,
        ), patch(
            "app.service.runtime_bootstrap.is_runner_role",
            return_value=False,
        ), patch(
            "app.service.runtime_bootstrap.service_role",
            return_value="all",
        ), patch(
            "app.service.runtime_bootstrap.sync_providers_to_pi",
            return_value=True,
        ), patch(
            "app.service.runtime_bootstrap.validate_pi_models_file",
            return_value={"path": "/tmp/models.json", "provider_count": 1, "model_count": 1},
        ), patch(
            "app.db.init_db",
            side_effect=fake_init_db,
        ), patch(
            "app.service.registry_service.get_registry_service",
            return_value=SimpleNamespace(register=lambda: asyncio.sleep(0), start=lambda: None, stop=lambda: None),
        ), patch(
            "app.service.task_service.get_task_service",
            return_value=SimpleNamespace(start_worker_loop=fake_start_worker_loop, stop_worker_loop=lambda: asyncio.sleep(0)),
        ):
            await bootstrap.start(app)
            for _ in range(80):
                if bootstrap.status()["ready"]:
                    break
                await asyncio.sleep(0.01)
            await bootstrap.stop()

        status = bootstrap.status()
        self.assertTrue(status["ready"])
        self.assertTrue(status["db_ready"])
        self.assertTrue(status["management_api_ready"])
        self.assertTrue(status["registry_ready"])
        self.assertTrue(status["worker_loop_ready"])
        self.assertTrue(status["provider_sync_done"])
        self.assertEqual(2, status["attempts"])
        self.assertEqual(2, len(init_attempts))
        self.assertEqual(1, len(worker_loop_starts))


if __name__ == "__main__":
    unittest.main()
