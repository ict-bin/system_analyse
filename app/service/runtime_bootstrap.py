"""Bootstrap DB-dependent runtime components with retry."""

from __future__ import annotations

import threading
import logging
import os
from dataclasses import asdict, dataclass
from typing import Optional

from fastapi import FastAPI

from app.config import get_service_yaml
from app.service.llm_provider_sync import sync_providers_to_pi, validate_pi_models_file
from app.service.service_role import is_api_role, is_dispatcher_role, is_runner_role, service_role

logger = logging.getLogger("sa.bootstrap")

DB_INIT_RETRY_SECONDS = int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_DB_INIT_RETRY_SECONDS", "5"))


@dataclass
class RuntimeBootstrapStatus:
    db_ready: bool = False
    management_api_ready: bool = False
    registry_ready: bool = False
    worker_loop_ready: bool = False
    ready: bool = False
    phase: str = "booting"
    error: str | None = None
    attempts: int = 0
    provider_sync_done: bool = False


class RuntimeBootstrap:
    def __init__(self, pool_overrides, should_run_migrations) -> None:
        self._task: Optional[object] = None
        self._stop_event = threading.Event()
        self._status = RuntimeBootstrapStatus()
        self._router_installed = False
        self._pool_overrides = pool_overrides
        self._should_run_migrations = should_run_migrations

    def start(self, app: FastAPI) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event = threading.Event()
        self._status = RuntimeBootstrapStatus()
        self._task = threading.Thread(target=self._bootstrap_loop, args=(app,), name="sa_runtime_bootstrap", daemon=True)
        self._task.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._task and self._task.is_alive():
            self._task.join(timeout=5.0)
        self._task = None
        try:
            if is_dispatcher_role() or is_runner_role():
                from app.service.task_service import get_task_service

                get_task_service().stop_worker_loop()
            if is_api_role():
                from app.service.registry_service import get_registry_service

                get_registry_service().stop()
        except Exception:
            import traceback
            traceback.print_exc()
            pass

    def status(self) -> dict:
        return asdict(self._status)

    def ready(self) -> bool:
        return self._status.ready

    def _bootstrap_loop(self, app: FastAPI) -> None:
        svc_yaml = get_service_yaml()
        self._sync_providers_once(svc_yaml)

        while not self._stop_event.is_set():
            made_progress = False

            if not self._status.db_ready:
                made_progress = self._init_db(svc_yaml)

            if self._status.db_ready:
                if is_api_role() and not self._router_installed:
                    made_progress = self._attempt_component_start(
                        "router_init",
                        lambda: self._install_management_router(app),
                    ) or made_progress

                if is_api_role() and not self._status.registry_ready:
                    made_progress = self._attempt_async_component_start(
                        "registry_register",
                        self._start_registry,
                    ) or made_progress

                if (is_dispatcher_role() or is_runner_role()) and not self._status.worker_loop_ready:
                    made_progress = self._attempt_async_component_start(
                        "worker_loop_start",
                        self._start_worker_loop,
                    ) or made_progress

                if self._all_required_components_ready():
                    self._status.phase = "ready"
                    self._status.ready = True
                    self._status.error = None
                    return

            if made_progress:
                continue

            try:
                self._stop_event.wait(timeout=DB_INIT_RETRY_SECONDS)
            except Exception:
                import traceback
                traceback.print_exc()
                pass

    def _sync_providers_once(self, svc_yaml) -> None:
        if self._status.provider_sync_done:
            return
        self._status.phase = "provider_sync"
        try:
            sync_ok = sync_providers_to_pi(
                base_url=svc_yaml.configcenter.base_url,
                token=svc_yaml.auth_service.service_machine_token,
                timeout=svc_yaml.configcenter.timeout,
            )
            if not sync_ok:
                logger.warning("Startup LLM Provider sync failed, runtime models.json may be stale")
            else:
                validation = validate_pi_models_file()
                logger.info(
                    "Startup runtime models ready: path=%s providers=%s models=%s",
                    validation["path"],
                    validation["provider_count"],
                    validation["model_count"],
                )
        except Exception as exc:
            logger.warning("Startup LLM Provider sync/validation failed: %s", exc, exc_info=True)
        finally:
            self._status.provider_sync_done = True

    def _init_db(self, svc_yaml) -> bool:
        from app.db import init_db

        self._status.phase = "db_init"
        self._status.attempts += 1
        try:
            pool_size, max_overflow, pool_timeout, pool_recycle = self._pool_overrides(svc_yaml)
            init_db(
                svc_yaml.database.url,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_timeout=pool_timeout,
                pool_recycle=pool_recycle,
                run_migrations=self._should_run_migrations(),
            )
            self._status.db_ready = True
            self._status.error = None
            logger.info(
                "DB initialized: %s:%s/%s (role=%s pool_size=%s max_overflow=%s pool_timeout=%s pool_recycle=%s run_migrations=%s)",
                svc_yaml.database.host,
                svc_yaml.database.port,
                svc_yaml.database.name,
                service_role(),
                pool_size,
                max_overflow,
                pool_timeout,
                pool_recycle,
                self._should_run_migrations(),
            )
            return True
        except Exception as exc:
            self._status.error = f"db_init: {exc}"
            logger.warning(
                "DB init failed on role=%s (attempt %s, retry in %ss): %s",
                service_role(),
                self._status.attempts,
                DB_INIT_RETRY_SECONDS,
                exc,
            )
            return False

    def _attempt_component_start(self, phase: str, starter) -> bool:
        self._status.phase = phase
        try:
            starter()
            self._status.error = None
            return True
        except Exception as exc:
            self._status.error = f"{phase}: {exc}"
            logger.warning("%s failed on role=%s (retry in %ss): %s", phase, service_role(), DB_INIT_RETRY_SECONDS, exc, exc_info=True)
            return False

    def _attempt_async_component_start(self, phase: str, starter) -> bool:
        self._status.phase = phase
        try:
            starter()
            self._status.error = None
            return True
        except Exception as exc:
            self._status.error = f"{phase}: {exc}"
            logger.warning("%s failed on role=%s (retry in %ss): %s", phase, service_role(), DB_INIT_RETRY_SECONDS, exc, exc_info=True)
            return False

    def _install_management_router(self, app: FastAPI) -> None:
        from app.api import router as mgmt_router

        app.include_router(mgmt_router)
        self._router_installed = True
        self._status.management_api_ready = True

    def _start_registry(self) -> None:
        from app.service.registry_service import get_registry_service

        registry = get_registry_service()
        registry.register()
        registry.start()
        self._status.registry_ready = True

    def _start_worker_loop(self) -> None:
        from app.service.task_service import get_task_service

        get_task_service().start_worker_loop()
        self._status.worker_loop_ready = True

    def _all_required_components_ready(self) -> bool:
        if not self._status.db_ready:
            return False
        if is_api_role() and not self._status.management_api_ready:
            return False
        if is_api_role() and not self._status.registry_ready:
            return False
        if (is_dispatcher_role() or is_runner_role()) and not self._status.worker_loop_ready:
            return False
        return True


_runtime_bootstrap: RuntimeBootstrap | None = None


def get_runtime_bootstrap(pool_overrides, should_run_migrations) -> RuntimeBootstrap:
    global _runtime_bootstrap
    if _runtime_bootstrap is None:
        _runtime_bootstrap = RuntimeBootstrap(pool_overrides, should_run_migrations)
    return _runtime_bootstrap
