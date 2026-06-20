import asyncio
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.service.runtime_bootstrap import RuntimeBootstrap
from app.service.scheduler import SchedulerService
from app.service import scheduler as scheduler_module
from unittest.mock import MagicMock


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

        def fake_start_worker_loop():
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
        ), patch.object(
            bootstrap,
            "_install_management_router",
            side_effect=lambda _app: setattr(bootstrap._status, "management_api_ready", True),
        ), patch.object(
            bootstrap,
            "_start_registry",
            side_effect=lambda: setattr(bootstrap._status, "registry_ready", True),
        ), patch.object(
            bootstrap,
            "_start_worker_loop",
            side_effect=lambda: (worker_loop_starts.append(1), setattr(bootstrap._status, "worker_loop_ready", True)),
        ):
            bootstrap.start(app)
            for _ in range(80):
                if bootstrap.status()["ready"]:
                    break
                await asyncio.sleep(0.01)
            bootstrap.stop()

        status = bootstrap.status()
        self.assertTrue(status["db_ready"])
        self.assertTrue(status["management_api_ready"])
        self.assertTrue(status["registry_ready"])
        self.assertTrue(status["worker_loop_ready"])
        self.assertTrue(status["provider_sync_done"])
        self.assertEqual(2, status["attempts"])
        self.assertEqual(2, len(init_attempts))
        self.assertEqual(1, len(worker_loop_starts))

    async def test_worker_loop_starts_only_once_when_runner_role_is_enabled(self):
        bootstrap = RuntimeBootstrap(
            pool_overrides=lambda svc: (1, 1, 1, 1),
            should_run_migrations=lambda: False,
        )
        app = SimpleNamespace(include_router=lambda router: None)
        worker_loop_starts = []

        with patch("app.service.runtime_bootstrap.get_service_yaml", return_value=SimpleNamespace(
            database=SimpleNamespace(url="mysql://", host="db", port=3306, name="sa"),
            configcenter=SimpleNamespace(base_url="http://cc", timeout=1),
            auth_service=SimpleNamespace(service_machine_token="token"),
        )), patch(
            "app.service.runtime_bootstrap.is_api_role",
            return_value=False,
        ), patch(
            "app.service.runtime_bootstrap.is_dispatcher_role",
            return_value=False,
        ), patch(
            "app.service.runtime_bootstrap.is_runner_role",
            return_value=True,
        ), patch(
            "app.service.runtime_bootstrap.service_role",
            return_value="runner",
        ), patch(
            "app.service.runtime_bootstrap.sync_providers_to_pi",
            return_value=True,
        ), patch(
            "app.service.runtime_bootstrap.validate_pi_models_file",
            return_value={"path": "/tmp/models.json", "provider_count": 1, "model_count": 1},
        ), patch.object(
            bootstrap,
            "_init_db",
            return_value=False,
        ), patch.object(
            bootstrap,
            "_start_worker_loop",
            side_effect=lambda: (worker_loop_starts.append(1), setattr(bootstrap._status, "worker_loop_ready", True)),
        ):
            bootstrap.start(app)
            for _ in range(80):
                if bootstrap.status()["ready"]:
                    break
                await asyncio.sleep(0.01)
            bootstrap.stop()

        self.assertEqual(1, len(worker_loop_starts))
        self.assertTrue(bootstrap.status()["worker_loop_ready"])


if __name__ == "__main__":
    unittest.main()


class SchedulerServiceTests(unittest.TestCase):
    def test_watchdog_marks_scheduler_stalled(self):
        svc = SchedulerService(
            get_db=lambda: iter(()),
            task_repo=SimpleNamespace(),
            spawn_task=lambda *args, **kwargs: None,
            record_event=lambda *args, **kwargs: None,
        )
        with patch.object(scheduler_module, "STALL_WARN_SECONDS", 1.0), patch.object(
            scheduler_module, "STALL_EXIT_ENABLED", False
        ):
            svc._running = True
            svc._last_tick = 100.0
            with patch.object(scheduler_module._time, "time", return_value=102.5):
                svc._watchdog_once()
        self.assertTrue(svc.health()["stall_detected"])


class TaskServiceRecoverPredicateTests(unittest.TestCase):
    def test_build_should_recover_avoids_observability_snapshot(self):
        import app.service.task_service as task_service_module

        task_service = object.__new__(task_service_module.TaskService)
        predicate = task_service._build_should_recover(db=SimpleNamespace())
        self.assertTrue(callable(predicate))
        stale_row = SimpleNamespace(task_id="sat_1")
        self.assertTrue(predicate(stale_row))
