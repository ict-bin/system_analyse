import asyncio
import sys
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.service.runtime_bootstrap import RuntimeBootstrap
from app.service.scheduler import SchedulerService
from app.service import scheduler as scheduler_module
from app.service.scheduler_v3 import SchedulerV3
from app.service.task_service import TaskService
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


class TaskServiceRuntimeHealthTests(unittest.TestCase):
    def test_get_worker_runtime_health_without_scheduler_instance(self):
        import app.service.task_service as task_service_module

        with patch.object(task_service_module, "is_runner_role", return_value=False), patch.object(
            task_service_module, "is_manager_role", return_value=True
        ), patch.object(
            task_service_module, "_get_dispatcher_runtime_health", return_value={"worker_loop_fresh": True}
        ), patch.object(
            task_service_module, "get_v3_scheduler", return_value=None
        ):
            health = task_service_module.get_worker_runtime_health()

        self.assertEqual(True, health["worker_loop_fresh"])
        self.assertEqual(task_service_module._RUNTIME_EVIDENCE_MODE, health["runtime_evidence_mode"])
        self.assertNotIn("scheduler_last_tick_at", health)

    def test_get_worker_runtime_health_includes_v3_scheduler_fields(self):
        import app.service.task_service as task_service_module

        scheduler = MagicMock()
        scheduler.health.return_value = {
            "last_tick": 123.0,
            "last_success": 122.5,
            "stall_detected": True,
        }
        with patch.object(task_service_module, "is_runner_role", return_value=False), patch.object(
            task_service_module, "is_manager_role", return_value=True
        ), patch.object(
            task_service_module, "_get_dispatcher_runtime_health", return_value={"worker_loop_fresh": True}
        ), patch.object(
            task_service_module, "get_v3_scheduler", return_value=scheduler
        ):
            health = task_service_module.get_worker_runtime_health()

        self.assertEqual(123.0, health["scheduler_last_tick_at"])
        self.assertEqual(122.5, health["scheduler_last_success_at"])
        self.assertTrue(health["scheduler_stall_detected"])

    def test_v3_requeue_task_skips_parent_orchestrated_binary_security_restart(self):
        import app.service.task_service as task_service_module

        row = SimpleNamespace(
            task_id="sat_parent_1",
            project_id="p1",
            status="running",
            output_path="/tmp/sa-out",
            task_origin_type="binary_security",
            parent_task_id="parent-1",
            parent_stage_item_id="item-1",
            parent_project_id="p1",
            parent_task_type="source",
            parent_stage_name="system_analysis",
            parent_stage_item_key="item-key-1",
            dispatcher_instance_id="worker-a",
            dispatch_started_at="started",
            lease_expires_at="lease",
            error=None,
            result_json={"status": "running"},
        )

        events = []
        service = object.__new__(task_service_module.TaskService)
        service._task_repository = SimpleNamespace(get_task=lambda db, task_id: row if task_id == "sat_parent_1" else None)
        service._record_timeline_event = lambda **kwargs: events.append(kwargs)

        class _Db:
            def commit(self):
                return None

        def _fake_get_db():
            yield _Db()

        with patch("app.db.get_db", _fake_get_db), patch.object(
            task_service_module,
            "_remove_task_root_for_restart",
            side_effect=AssertionError("should not clean restart parent-orchestrated task"),
        ), patch.object(
            task_service_module,
            "_clear_task_execution_lock",
            side_effect=AssertionError("should not clear execution lock for restart"),
        ):
            self.assertTrue(service._v3_requeue_task("sat_parent_1"))

        self.assertEqual("pending", row.status)
        self.assertIsNone(row.dispatcher_instance_id)
        self.assertIsNone(row.dispatch_started_at)
        self.assertIsNone(row.lease_expires_at)


class SchedulerV3PendingRepairTests(unittest.TestCase):
    def test_db_reconcile_requeues_pending_task_missing_from_scheduler_queue(self):
        events = []
        scheduler = SchedulerV3(
            finalize_task=lambda *args, **kwargs: None,
            record_event=lambda task_id, event_type, message, level="info", payload=None: events.append(
                {
                    "task_id": task_id,
                    "event_type": event_type,
                    "message": message,
                    "level": level,
                    "payload": payload or {},
                }
            ),
        )
        scheduler._pending_tasks_missing_from_scheduler = lambda: [
            {"task_id": "sat_pending_1", "project_id": "p1", "status": "pending"}
        ]

        scheduler._repair_pending_unqueued_tasks()

        self.assertEqual(["sat_pending_1"], list(scheduler._queue))
        self.assertTrue(scheduler._dirty)
        self.assertEqual("task_requeued", events[-1]["event_type"])
        self.assertEqual(
            "pending_task_missing_from_scheduler_queue",
            events[-1]["payload"]["reason"],
        )
        self.assertEqual(
            "scheduler_db_reconcile",
            events[-1]["payload"]["repair_source"],
        )

    def test_task_status_marks_pending_row_missing_from_queue(self):
        scheduler = SchedulerV3(finalize_task=lambda *args, **kwargs: None)

        class _Row:
            task_id = "sat_pending_2"
            status = "pending"
            started_at = None
            finished_at = None
            dispatcher_instance_id = None
            dispatch_started_at = None
            error = None
            result_json = None

        class _Query:
            def filter_by(self, **kwargs):
                return self

            def first(self):
                return _Row()

        class _Db:
            def query(self, model):
                del model
                return _Query()

        def _fake_get_db():
            yield _Db()

        with patch("app.db.get_db", _fake_get_db):
            status = scheduler.task_status("sat_pending_2")

        self.assertEqual("pending", status["status"])
        self.assertEqual("missing_from_queue", status["scheduler_state"])

    def test_dispatch_once_removes_ghost_queue_entry(self):
        events = []
        scheduler = SchedulerV3(
            finalize_task=lambda *args, **kwargs: None,
            record_event=lambda task_id, event_type, message, level="info", payload=None: events.append(
                {
                    "task_id": task_id,
                    "event_type": event_type,
                    "message": message,
                    "level": level,
                    "payload": payload or {},
                }
            ),
        )
        scheduler._task_exists_for_dispatch = lambda task_id: False if task_id == "sat_ghost_1" else True
        scheduler._queue.append("sat_ghost_1")
        scheduler._workers["worker-1"] = SimpleNamespace(
            worker_id="worker-1",
            online=True,
            current_task=None,
            reported_task=None,
            last_heartbeat=0.0,
            sock=None,
            is_idle=True,
        )

        scheduler._dispatch_once()

        self.assertEqual([], list(scheduler._queue))
        self.assertEqual({}, scheduler._running)
        self.assertEqual("task_queue_entry_removed", events[-1]["event_type"])
        self.assertEqual("task_row_missing_for_dispatch", events[-1]["payload"]["reason"])

    def test_dispatch_once_skips_ghost_and_preserves_following_real_task(self):
        events = []
        claims = []
        scheduler = SchedulerV3(
            finalize_task=lambda *args, **kwargs: None,
            record_event=lambda task_id, event_type, message, level="info", payload=None: events.append(
                {
                    "task_id": task_id,
                    "event_type": event_type,
                    "message": message,
                    "level": level,
                    "payload": payload or {},
                }
            ),
            claim_task=lambda task_id, worker_id: claims.append((task_id, worker_id)) or 1,
        )
        scheduler._task_exists_for_dispatch = lambda task_id: task_id != "sat_ghost_2"
        scheduler._send = lambda conn, msg: True
        scheduler._queue.extend(["sat_ghost_2", "sat_real_1"])
        scheduler._workers["worker-1"] = SimpleNamespace(
            worker_id="worker-1",
            online=True,
            current_task=None,
            reported_task=None,
            last_heartbeat=0.0,
            sock=object(),
            is_idle=True,
        )

        scheduler._dispatch_once()
        scheduler._dispatch_once()

        self.assertEqual([("sat_real_1", "worker-1")], claims)
        self.assertNotIn("sat_ghost_2", list(scheduler._queue))
        self.assertIn("sat_real_1", scheduler._running)
        self.assertEqual("task_queue_entry_removed", events[0]["event_type"])
        self.assertEqual("task_dispatched", events[-1]["event_type"])


class TaskServiceRuntimeOverviewTests(unittest.TestCase):
    def test_runtime_overview_includes_pending_unqueued_count(self):
        service = object.__new__(TaskService)
        stale_pending = SimpleNamespace(
            task_id="sat_pending_3",
            project_id="p1",
            task_name="demo",
            analysis_mode="source",
            created_at=datetime(2026, 6, 21, 10, 0, 0),
        )
        service._task_repository = SimpleNamespace(
            get_status_counts=lambda db: {"pending": 2, "running": 1},
            get_oldest_pending_created_at=lambda db: datetime(2026, 6, 21, 9, 0, 0),
            list_running_tasks=lambda db, limit=20: [],
            list_pending_tasks_for_scheduler_repair=lambda db, created_before=None, limit=500: [stale_pending],
        )

        with patch("app.service.task_service.get_worker_runtime_health", return_value={"worker_ok": True}), patch(
            "app.service.task_service.get_runtime_control_service",
            return_value=SimpleNamespace(get_runtime_control=lambda db: {"enabled": True}),
        ), patch(
            "app.service.task_service.get_runner_registry_service",
            return_value=SimpleNamespace(list_active_runners=lambda db: []),
        ), patch(
            "app.service.task_service.get_pending_scheduler_repair_grace_seconds",
            return_value=20.0,
        ), patch(
            "app.service.task_service.now_local",
            return_value=datetime(2026, 6, 21, 10, 1, 0),
        ):
            payload = TaskService.get_runtime_overview(service, object())

        self.assertEqual(1, payload["queue"]["pending_unqueued_count"])
        self.assertEqual("sat_pending_3", payload["pending_unqueued_tasks"][0]["task_id"])
