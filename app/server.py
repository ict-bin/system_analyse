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

import threading
import queue
import json
import logging
import os
import time as _time
import time
from contextlib import asynccontextmanager
from threading import Lock
from typing import Any, Callable

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel, Field

from .build_info import build_service_meta
from .config import (
    CONFIG_DIR, TARGET_DIR,
    build_task_config, get_service_yaml, load_service_config,
)
from .logging_utils import configure_container_logging, log_event
from .metrics import normalize_http_route, observe_http_request as observe_metrics_request, observe_http_request_inflight, render_metrics, render_summary_metrics
from .metrics_summary import build_ai_summary, build_generic_observability_summary, build_rest_api_summary, parse_prometheus_metrics
from .models import SwarmEvent, TaskResult, TaskStatus, make_id
from .orchestrator import Orchestrator
from .probe_server import ThreadedProbeServer
from .service.service_role import is_api_role as _is_api_service_role
from .service.service_role import is_dispatcher_role as _is_dispatcher_service_role
from .service.service_role import is_runner_role as _is_runner_service_role
from .service.runtime_bootstrap import get_runtime_bootstrap
from .service.service_role import service_role as _normalized_service_role

load_dotenv()
configure_container_logging("01-system_analyse")
logger = logging.getLogger("sa.server")

SERVICE_CONFIG_PATH = os.environ.get("SERVICE_CONFIG", f"{CONFIG_DIR}/config.json")
CLEANUP_DELAY = int(os.environ.get("CLEANUP_DELAY", "300"))
_SUMMARY_CACHE_TTL_SECONDS = 5.0
_summary_cache: dict[str, tuple[float, Any]] = {}
_summary_cache_lock = Lock()
_probe_server: ThreadedProbeServer | None = None
_probe_shutdown = False
_probe_started_at = 0.0


def _external_probe_process_enabled() -> bool:
    return str(os.environ.get("SECFLOW_EXTERNAL_PROBE_PROCESS", "")).strip().lower() in {"1", "true", "yes", "on"}


def _cached_summary(key: str, builder: Callable[[], Any]) -> Any:
    now = _time.monotonic()
    with _summary_cache_lock:
        cached = _summary_cache.get(key)
        if cached and now - cached[0] <= _SUMMARY_CACHE_TTL_SECONDS:
            return cached[1]
    value = builder()
    with _summary_cache_lock:
        _summary_cache[key] = (_time.monotonic(), value)
    return value


def _metrics_rows():
    return parse_prometheus_metrics(render_summary_metrics())


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
        # "all" = 单 pod 开发模式；"api" = 生产模式下由 API pod 统一执行 DDL 迁移
        # runner/worker 多副本不跑迁移，避免并发 ALTER（迁移幂等但不必要竞争）
        return _service_role() in {"all", "api"}
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


# ─── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- startup ---
    global _probe_shutdown, _probe_started_at
    _probe_shutdown = False
    _probe_started_at = _time.time()
    if not _external_probe_process_enabled():
        _ensure_probe_server_started()
    get_runtime_bootstrap(_db_pool_overrides, _should_run_db_migrations).start(app)
    # 挂载调度器内部 API
    from .service.scheduler import create_scheduler_router
    app.include_router(create_scheduler_router())

    yield

    # --- shutdown ---
    _probe_shutdown = True
    get_runtime_bootstrap(_db_pool_overrides, _should_run_db_migrations).stop()
    if not _external_probe_process_enabled():
        _stop_probe_server()


# ─── Application ──────────────────────────────────────────────────────────────

class TaskEntry:
    def __init__(self, orch: Orchestrator, task_id: str, prompt: str):
        self.orch = orch
        self.task_id = task_id
        self.prompt = prompt
        self.result: TaskResult | None = None
        self.events: list[dict] = []
        self.queues: list[queue.Queue] = []
        self.done = threading.Event()
        self.callback_url: str | None = None


_tasks: dict[str, TaskEntry] = {}

app = FastAPI(title="system_analyse", version="2.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

try:
    if _is_runner_role():
        from .api.tasks import internal_observability_router as runner_internal_router

        app.include_router(runner_internal_router)
except Exception:
    import traceback
    traceback.print_exc()
    logger.exception("failed to include runner internal observability router")


@app.middleware("http")
async def collect_request_metrics(request, call_next):
    started = _time.perf_counter()
    response = None
    route = request.scope.get("route")
    path = getattr(route, "path", None) or request.url.path
    normalized_route = normalize_http_route(str(path))
    observe_http_request_inflight(request.method, normalized_route, 1)
    try:
        response = await call_next(request)
        return response
    finally:
        status_code = response.status_code if response is not None else 500
        observe_metrics_request(request.method, str(path), status_code, _time.perf_counter() - started)
        observe_http_request_inflight(request.method, normalized_route, -1)

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
    from .service.task_service import get_worker_runtime_health

    role = _service_role()
    bootstrap = get_runtime_bootstrap(_db_pool_overrides, _should_run_db_migrations).status()
    worker_health = get_worker_runtime_health()
    db_ok = bool(bootstrap.get("db_ready"))
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
        "bootstrap_db_ready": bootstrap["db_ready"],
        "bootstrap_ready": bootstrap["ready"],
        "bootstrap_phase": bootstrap["phase"],
        "bootstrap_error": bootstrap["error"],
        "bootstrap_attempts": bootstrap["attempts"],
        "worker_ok": worker_ok,
        "worker_claim_paused": worker_claim_paused,
        **worker_health,
    }


def _probe_payload() -> dict[str, object]:
    base = _health_status()
    role = str(base.get("role") or _service_role())
    ready_ok = bool(base.get("status") == "ok")
    db_ok = bool(base.get("db_ok"))
    payload = {
        **base,
        **build_service_meta(),
        "service": "secflow-app-system-analyse",
        "role": role,
        "started_at": _probe_started_at or None,
        "updated_at": _time.time(),
        "shutting_down": _probe_shutdown,
        "startup_phase": base.get("bootstrap_phase") or "booting",
        "last_error": base.get("bootstrap_error"),
        "reason": None if ready_ok else (base.get("bootstrap_error") or ("worker loop stale" if not bool(base.get("worker_ok")) else "bootstrap not ready")),
        "liveness_ok": not _probe_shutdown,
        "readiness_ok": ready_ok and not _probe_shutdown,
        "checks": {
            "bootstrap": {
                "db_ready": bool(base.get("bootstrap_db_ready")),
                "ready": bool(base.get("bootstrap_ready")),
                "attempts": int(base.get("bootstrap_attempts") or 0),
            },
            "database": {
                "ok": db_ok,
            },
            "worker_loop": {
                "ok": bool(base.get("worker_ok")),
                "claim_paused": bool(base.get("worker_claim_paused")),
                "fresh": bool(base.get("worker_loop_fresh", True)),
            },
        },
    }
    if role == "api":
        payload["checks"]["worker_loop"]["ok"] = True
    return payload


def _ensure_probe_server_started() -> None:
    # 独立 sidecar 进程 probe_sidecar.py 替代线程内探针
    # 环境变量 SECFLOW_SYSTEM_ANALYSE_PROBE_STANDALONE=1 启用旧探针（兼容）
    if os.environ.get("SECFLOW_SYSTEM_ANALYSE_PROBE_STANDALONE") != "1":
        return
    global _probe_server
    if _probe_server is not None:
        _probe_server.start()
        return
    port = int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_PROBE_PORT", "18080"))
    _probe_server = ThreadedProbeServer(
        host="0.0.0.0",
        port=port,
        payload_provider=_probe_payload,
        health_paths=("/health", "/livez", "/api/app/system-analyse/health", "/api/app/system-analyse/livez"),
        ready_paths=("/ready", "/readyz", "/api/app/system-analyse/ready", "/api/app/system-analyse/readyz"),
    )
    _probe_server.start()


def _stop_probe_server() -> None:
    global _probe_server
    if _probe_server is not None:
        _probe_server.stop()
        _probe_server = None


# ─── Health ───────────────────────────────────────────────────────────────────

class AnalyseRequest(BaseModel):
    prompt: str = Field(..., description="一句话任务描述，如：对解包后的所有文件进行威胁分析与模块分析")
    cwd: str = Field(default="", description="待分析文件目录，默认 /data/target")
    callback_url: str = Field(default="", description="任务完成后 POST 通知的 URL")


@app.get("/health")
@app.get("/api/app/system-analyse/health")
def health():
    payload = _probe_payload()
    payload["active"] = sum(1 for t in _tasks.values() if t.result is None)
    payload["completed"] = sum(1 for t in _tasks.values() if t.result is not None)
    return payload


@app.get("/metrics")
@app.get("/api/app/system-analyse/metrics", include_in_schema=False)
def metrics():
    return PlainTextResponse(render_metrics(), media_type="text/plain; version=0.0.4; charset=utf-8")


@app.get("/api/app/system-analyse/metrics/summary", include_in_schema=False)
def metrics_summary():
    return run_in_threadpool(
        _cached_summary,
        "summary",
        lambda: build_generic_observability_summary(_metrics_rows(), title="系统分析"),
    )


@app.get("/api/app/system-analyse/metrics/rest-api-summary", include_in_schema=False)
def metrics_rest_api_summary():
    return run_in_threadpool(
        _cached_summary,
        "rest-api-summary",
        lambda: build_rest_api_summary(_metrics_rows()),
    )


@app.get("/api/app/system-analyse/metrics/ai-summary", include_in_schema=False)
def metrics_ai_summary():
    return run_in_threadpool(
        _cached_summary,
        "ai-summary",
        lambda: build_ai_summary(_metrics_rows(), coverage_text="系统分析 AI 指标覆盖 worker / judge / review 等调用。"),
    )


@app.get("/livez")
@app.get("/api/app/system-analyse/livez")
def livez():
    payload = _probe_payload()
    payload["status"] = "ok" if payload["liveness_ok"] else "degraded"
    return payload


@app.get("/readyz")
@app.get("/api/app/system-analyse/readyz")
@app.get("/ready")
@app.get("/api/app/system-analyse/ready")
def readyz():
    from fastapi import HTTPException

    payload = _probe_payload()
    if not payload["readiness_ok"]:
        raise HTTPException(status_code=503, detail=payload)
    return payload


@app.post("/analyse", status_code=202)
def submit_analyse(body: AnalyseRequest):
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
            except queue.QueueFull:
                pass

    orch = Orchestrator(config=cfg, on_event=on_event)
    entry = TaskEntry(orch, task_id, body.prompt)
    entry.callback_url = body.callback_url or None
    _tasks[task_id] = entry
    log_event(logger, logging.INFO, "analysis task accepted",
              event="task_submitted", task_id=task_id, cwd=cwd,
              callback_url=entry.callback_url or "")

    def _run():
        try:
            entry.result = orch.execute(task_id)
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
                except queue.QueueFull:
                    pass
            entry.done.set()
            if entry.result:
                log_event(logger, logging.INFO, "analysis task finished",
                          event="task_finished", task_id=task_id, status=entry.result.status.value)
            if entry.callback_url and entry.result:
                _notify(entry)
            time.sleep(CLEANUP_DELAY)
            _tasks.pop(task_id, None)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return {
        "task_id": task_id,
        "source_file": cfg.source_file,
        "function_name": cfg.function_name,
        "status": "accepted",
        "stream": f"/task/{task_id}/stream",
        "result": f"/task/{task_id}",
    }


def _notify(entry: TaskEntry):
    if not entry.callback_url or not entry.result:
        return
    try:
        with httpx.Client(timeout=30) as client:
            client.post(entry.callback_url, json={
                "task_id": entry.task_id,
                "status": entry.result.status.value,
                "duration_ms": entry.result.total_duration_ms,
                "cost": entry.result.total_tokens.cost,
            })
    except Exception:
        import traceback
        traceback.print_exc()
        log_event(logger, logging.WARNING, "callback notification failed",
                  event="callback_failed", task_id=entry.task_id,
                  callback_url=entry.callback_url or "")


@app.get("/task/{task_id}")
def get_task(task_id: str):
    _require_api_role()
    entry = _tasks.get(task_id)
    if not entry:
        raise HTTPException(404, "Task not found")
    if entry.result:
        return entry.result.model_dump()
    return {"task_id": task_id, "status": "running", "events_count": len(entry.events)}


@app.get("/task/{task_id}/stream")
def stream_task(task_id: str):
    _require_api_role()
    entry = _tasks.get(task_id)
    if not entry:
        raise HTTPException(404, "Task not found")
    queue: queue.Queue = queue.Queue(maxsize=1000)
    entry.queues.append(queue)

    def gen():
        for evt in entry.events:
            yield {"data": json.dumps(evt, ensure_ascii=False)}
        if entry.result:
            yield {"data": json.dumps({"type": "done", "task_id": task_id})}
            return
        try:
            while True:
                try:
                    evt = queue.get(timeout=30)
                    yield {"data": json.dumps(evt, ensure_ascii=False)}
                    if evt.get("type") == "done":
                        return
                except TimeoutError:
                    yield {"comment": "keepalive"}
        finally:
            if queue in entry.queues:
                entry.queues.remove(queue)

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.post("/task/{task_id}/stop")
@app.post("/task/{task_id}/abort")  # alias kept for compatibility
def stop_task(task_id: str):
    _require_api_role()
    entry = _tasks.get(task_id)
    if not entry:
        raise HTTPException(404)
    if entry.result:
        return {"message": "Already completed", "status": entry.result.status.value}
    entry.orch.stop()
    return {"message": "Stop sent", "task_id": task_id}


@app.get("/tasks")
def list_engine_tasks():
    _require_api_role()
    return {"tasks": [
        {"task_id": tid, "prompt": e.prompt[:100],
         "status": e.result.status.value if e.result else "running"}
        for tid, e in _tasks.items()
    ]}
