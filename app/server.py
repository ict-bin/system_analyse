"""
system_analyse — REST API 服务器

  Management layer (persistent, project-scoped):
    POST /api/app/system-analyse/tasks          创建任务（input_path, prompt 自动生成）
    GET  /api/app/system-analyse/tasks          任务列表（project_id 过滤）
    GET  /api/app/system-analyse/tasks/{id}     任务详情
    POST /api/app/system-analyse/tasks/{id}/cancel   取消任务
    POST /api/app/system-analyse/tasks/{id}/restart  以当前配置重新运行任务
    POST /api/app/system-analyse/generate-prompt    根据路径生成 prompt
    CRUD /api/app/system-analyse/prompts/*      Prompt 模板
    GET/PUT /api/app/system-analyse/config      项目配置
    GET  /api/app/system-analyse/health         健康检查

  Legacy engine routes (in-memory, backward compat):
    POST /analyse           直接提交分析（CLI 兼容）
    GET  /task/{id}         查询结果
    GET  /task/{id}/stream  SSE 实时事件流
    POST /task/{id}/stop    中止
    GET  /tasks             列出内存任务
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time as _time
from contextlib import asynccontextmanager

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from .config import (
    CONFIG_DIR, TARGET_DIR,
    build_task_config, get_service_yaml, load_service_config,
)
from .logging_utils import configure_container_logging, log_event
from .metrics import observe_request as observe_metrics_request, render_metrics
from .models import SwarmEvent, TaskResult, TaskStatus, make_id
from .orchestrator import Orchestrator
from .service.service_role import is_api_role as _is_api_service_role
from .service.service_role import is_dispatcher_role as _is_dispatcher_service_role
from .service.service_role import is_runner_role as _is_runner_service_role
from .service.service_role import service_role as _normalized_service_role
from .service.llm_provider_sync import sync_providers_to_pi, validate_pi_models_file

load_dotenv()
configure_container_logging("01-system_analyse")
logger = logging.getLogger("sa.server")

SERVICE_CONFIG_PATH = os.environ.get("SERVICE_CONFIG", f"{CONFIG_DIR}/config.json")
CLEANUP_DELAY = int(os.environ.get("CLEANUP_DELAY", "300"))

def _is_api_role() -> bool:
    return _is_api_service_role()


def _is_manager_role() -> bool:
    return _is_dispatcher_service_role()


def _is_runner_role() -> bool:
    return _is_runner_service_role()


def _service_role() -> str:
    return _normalized_service_role()


def _require_api_role() -> None:
    if not _is_api_role():
        raise HTTPException(status_code=503, detail="当前实例不提供 API 服务")


def _db_pool_overrides(svc_yaml) -> tuple[int, int, int, int]:
    role = _service_role()
    default_pool = int(svc_yaml.database.pool_size)
    default_overflow = int(svc_yaml.database.max_overflow)
    default_timeout = int(svc_yaml.database.pool_timeout)
    default_recycle = int(svc_yaml.database.pool_recycle)

    def _env_with_fallback(primary: str, fallback: str) -> str:
        return os.environ.get(primary, os.environ.get(fallback, ""))

    if role == "api":
        pool_size = int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_DB_POOL_SIZE_API", str(default_pool)))
        max_overflow = int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_DB_MAX_OVERFLOW_API", str(default_overflow)))
        pool_timeout = int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_DB_POOL_TIMEOUT_API", str(default_timeout)))
        pool_recycle = int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_DB_POOL_RECYCLE_API", str(default_recycle)))
    elif role == "runner":
        pool_size = int(_env_with_fallback("SECFLOW_SYSTEM_ANALYSE_DB_POOL_SIZE_RUNNER", "SECFLOW_SYSTEM_ANALYSE_DB_POOL_SIZE_WORKER") or str(default_pool))
        max_overflow = int(_env_with_fallback("SECFLOW_SYSTEM_ANALYSE_DB_MAX_OVERFLOW_RUNNER", "SECFLOW_SYSTEM_ANALYSE_DB_MAX_OVERFLOW_WORKER") or str(default_overflow))
        pool_timeout = int(_env_with_fallback("SECFLOW_SYSTEM_ANALYSE_DB_POOL_TIMEOUT_RUNNER", "SECFLOW_SYSTEM_ANALYSE_DB_POOL_TIMEOUT_WORKER") or str(default_timeout))
        pool_recycle = int(_env_with_fallback("SECFLOW_SYSTEM_ANALYSE_DB_POOL_RECYCLE_RUNNER", "SECFLOW_SYSTEM_ANALYSE_DB_POOL_RECYCLE_WORKER") or str(default_recycle))
    else:
        pool_size = int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_DB_POOL_SIZE_WORKER", str(default_pool)))
        max_overflow = int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_DB_MAX_OVERFLOW_WORKER", str(default_overflow)))
        pool_timeout = int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_DB_POOL_TIMEOUT_WORKER", str(default_timeout)))
        pool_recycle = int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_DB_POOL_RECYCLE_WORKER", str(default_recycle)))
    return max(1, pool_size), max(0, max_overflow), max(1, pool_timeout), max(60, pool_recycle)


def _should_run_db_migrations() -> bool:
    raw = os.environ.get("SECFLOW_SYSTEM_ANALYSE_DB_AUTO_MIGRATE")
    if raw is None:
        return _service_role() == "all"
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


# ─── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- startup ---
    svc_yaml = get_service_yaml()
    db_url = svc_yaml.database.url

    try:
        sync_ok = await sync_providers_to_pi(
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

    try:
        from .db import init_db
        pool_size, max_overflow, pool_timeout, pool_recycle = _db_pool_overrides(svc_yaml)
        init_db(
            db_url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
            run_migrations=_should_run_db_migrations(),
        )
        logger.info(
            "DB initialized: %s:%s/%s (role=%s pool_size=%s max_overflow=%s pool_timeout=%s pool_recycle=%s run_migrations=%s)",
            svc_yaml.database.host, svc_yaml.database.port, svc_yaml.database.name,
            _service_role(), pool_size, max_overflow, pool_timeout, pool_recycle, _should_run_db_migrations(),
        )
    except Exception as exc:
        logger.warning("DB init failed (management APIs will be unavailable): %s", exc)

    if _is_api_role():
        try:
            from .service.registry_service import get_registry_service
            registry = get_registry_service()
            await registry.register()
            registry.start()
        except Exception as exc:
            logger.warning("Registry startup failed: %s", exc)

        from .api import router as mgmt_router
        app.include_router(mgmt_router)

    if _is_manager_role() or _is_runner_role():
        from .service.task_service import get_task_service
        await get_task_service().start_worker_loop()

    yield

    # --- shutdown ---
    try:
        if _is_manager_role() or _is_runner_role():
            from .service.task_service import get_task_service
            await get_task_service().stop_worker_loop()
        if _is_api_role():
            from .service.registry_service import get_registry_service
            get_registry_service().stop()
    except Exception:
        pass


# ─── Application ──────────────────────────────────────────────────────────────

class TaskEntry:
    def __init__(self, orch: Orchestrator, task_id: str, prompt: str):
        self.orch = orch
        self.task_id = task_id
        self.prompt = prompt
        self.result: TaskResult | None = None
        self.events: list[dict] = []
        self.queues: list[asyncio.Queue] = []
        self.done = asyncio.Event()
        self.callback_url: str | None = None


_tasks: dict[str, TaskEntry] = {}

app = FastAPI(title="system_analyse", version="2.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.middleware("http")
async def collect_request_metrics(request, call_next):
    started = _time.perf_counter()
    response = await call_next(request)
    observe_metrics_request(request.method, request.url.path, response.status_code, _time.perf_counter() - started)
    return response

_svc_config = None


def _get_svc_config():
    global _svc_config
    if _svc_config is None:
        for p in [SERVICE_CONFIG_PATH, "/opt/system_analyse/config.example.json"]:
            if os.path.isfile(p):
                _svc_config = load_service_config(p)
                break
        if _svc_config is None:
            raise RuntimeError(f"服务配置文件不存在: {SERVICE_CONFIG_PATH}")
    return _svc_config


def _health_status() -> dict:
    from .db import ping_db
    from .service.task_service import get_worker_runtime_health

    role = _service_role()
    worker_health = get_worker_runtime_health()
    db_ok = ping_db()
    worker_claim_paused = bool(
        worker_health.get("worker_pause_claim_until_ts")
        and float(worker_health["worker_pause_claim_until_ts"]) > _time.time()
    )
    worker_ok = worker_health.get("worker_loop_fresh", True) and not worker_claim_paused
    if role == "api":
        ready = db_ok
    elif role == "manager":
        ready = db_ok and worker_ok
    elif role == "runner":
        ready = db_ok
    else:
        ready = db_ok and worker_ok
    return {
        "status": "ok" if ready else "degraded",
        "role": role,
        "db_ok": db_ok,
        "worker_ok": worker_ok,
        "worker_claim_paused": worker_claim_paused,
        **worker_health,
    }


# ─── Health ───────────────────────────────────────────────────────────────────

class AnalyseRequest(BaseModel):
    prompt: str = Field(..., description="一句话任务描述，如：对解包后的所有文件进行威胁分析与模块分析")
    cwd: str = Field(default="", description="待分析文件目录，默认 /data/target")
    callback_url: str = Field(default="", description="任务完成后 POST 通知的 URL")


@app.get("/health")
@app.get("/api/app/system-analyse/health")
async def health():
    base = _health_status()
    return {
        **base,
        "active": sum(1 for t in _tasks.values() if t.result is None),
        "completed": sum(1 for t in _tasks.values() if t.result is not None),
    }


@app.get("/metrics")
@app.get("/api/app/system-analyse/metrics", include_in_schema=False)
async def metrics():
    return PlainTextResponse(render_metrics(), media_type="text/plain; version=0.0.4; charset=utf-8")


@app.get("/livez")
@app.get("/api/app/system-analyse/livez")
async def livez():
    return {
        "status": "ok",
        "role": _service_role(),
    }


@app.get("/readyz")
@app.get("/api/app/system-analyse/readyz")
async def readyz():
    from fastapi import HTTPException

    payload = _health_status()
    if payload["status"] != "ok":
        raise HTTPException(status_code=503, detail=payload)
    return payload


@app.post("/analyse", status_code=202)
async def submit_analyse(body: AnalyseRequest):
    """直接提交分析任务（CLI 兼容路由）。"""
    _require_api_role()
    svc = _get_svc_config()
    cwd = body.cwd or TARGET_DIR
    cfg = build_task_config(svc, body.prompt, cwd=cwd)
    task_id = make_id()

    def on_event(event: SwarmEvent):
        entry = _tasks.get(task_id)
        if not entry:
            return
        d = event.model_dump()
        entry.events.append(d)
        for q in entry.queues:
            try:
                q.put_nowait(d)
            except asyncio.QueueFull:
                pass

    orch = Orchestrator(config=cfg, on_event=on_event)
    entry = TaskEntry(orch, task_id, body.prompt)
    entry.callback_url = body.callback_url or None
    _tasks[task_id] = entry
    log_event(logger, logging.INFO, "analysis task accepted",
              event="task_submitted", task_id=task_id, cwd=cwd,
              callback_url=entry.callback_url or "")

    async def _run():
        try:
            entry.result = await orch.execute(task_id)
        except Exception as e:
            log_event(logger, logging.ERROR, "analysis task failed",
                      event="task_failed", task_id=task_id, error=str(e))
            entry.result = TaskResult(
                task_id=task_id, status=TaskStatus.ERROR,
                task=body.prompt, error=str(e))
        finally:
            done_data = {
                "type": "done", "task_id": task_id,
                "status": entry.result.status.value if entry.result else "error",
            }
            for q in entry.queues:
                try:
                    q.put_nowait(done_data)
                except asyncio.QueueFull:
                    pass
            entry.done.set()
            if entry.result:
                log_event(logger, logging.INFO, "analysis task finished",
                          event="task_finished", task_id=task_id, status=entry.result.status.value)
            if entry.callback_url and entry.result:
                await _notify(entry)
            await asyncio.sleep(CLEANUP_DELAY)
            _tasks.pop(task_id, None)

    asyncio.create_task(_run())
    return {
        "task_id": task_id,
        "source_file": cfg.source_file,
        "function_name": cfg.function_name,
        "status": "accepted",
        "stream": f"/task/{task_id}/stream",
        "result": f"/task/{task_id}",
    }


async def _notify(entry: TaskEntry):
    if not entry.callback_url or not entry.result:
        return
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            await client.post(entry.callback_url, json={
                "task_id": entry.task_id,
                "status": entry.result.status.value,
                "duration_ms": entry.result.total_duration_ms,
                "cost": entry.result.total_tokens.cost,
            })
    except Exception:
        log_event(logger, logging.WARNING, "callback notification failed",
                  event="callback_failed", task_id=entry.task_id,
                  callback_url=entry.callback_url or "")


@app.get("/task/{task_id}")
async def get_task(task_id: str):
    _require_api_role()
    entry = _tasks.get(task_id)
    if not entry:
        raise HTTPException(404, "Task not found")
    if entry.result:
        return entry.result.model_dump()
    return {"task_id": task_id, "status": "running", "events_count": len(entry.events)}


@app.get("/task/{task_id}/stream")
async def stream_task(task_id: str):
    _require_api_role()
    entry = _tasks.get(task_id)
    if not entry:
        raise HTTPException(404, "Task not found")
    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    entry.queues.append(queue)

    async def gen():
        for evt in entry.events:
            yield {"data": json.dumps(evt, ensure_ascii=False)}
        if entry.result:
            yield {"data": json.dumps({"type": "done", "task_id": task_id})}
            return
        try:
            while True:
                try:
                    evt = await asyncio.wait_for(queue.get(), timeout=30)
                    yield {"data": json.dumps(evt, ensure_ascii=False)}
                    if evt.get("type") == "done":
                        return
                except asyncio.TimeoutError:
                    yield {"comment": "keepalive"}
        finally:
            if queue in entry.queues:
                entry.queues.remove(queue)

    return EventSourceResponse(gen())


@app.post("/task/{task_id}/stop")
@app.post("/task/{task_id}/abort")  # alias kept for compatibility
async def stop_task(task_id: str):
    _require_api_role()
    entry = _tasks.get(task_id)
    if not entry:
        raise HTTPException(404)
    if entry.result:
        return {"message": "Already completed", "status": entry.result.status.value}
    entry.orch.stop()
    return {"message": "Stop sent", "task_id": task_id}


@app.get("/tasks")
async def list_engine_tasks():
    _require_api_role()
    return {"tasks": [
        {"task_id": tid, "prompt": e.prompt[:100],
         "status": e.result.status.value if e.result else "running"}
        for tid, e in _tasks.items()
    ]}
