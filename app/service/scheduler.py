"""
service/scheduler.py — 统一调度器

架构: SchedulerService (manager pod) 统一分配任务到 ExecutorAgent (runner pod).
通信: HTTP API (内部, 不走 kubectl).
心跳: 独立子进程上报.

核心功能 (从 worker_dispatcher 完整迁移):
  1. 任务认领与租约 (lease_epoch + lease_expires_at + heartbeat 续租)
  2. 僵死任务回收 (stale recovery + runtime evidence 检测)
  3. 运行时控制 (pause/drain/unpause via DB)
  4. 全局并发限制 (MAX_RUNNING_TASKS_GLOBAL)
  5. 单 Pod 并发限制 (worker_task_concurrency)
  6. 进程清理 (任务结束/取消耗时 kill pi/python 子进程)
  7. pod 注册/心跳/健康检查
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import threading
import time
import time as _time
import urllib.request
import uuid
from dataclasses import dataclass, field
from typing import Callable

from sqlalchemy.orm import Session

from app.time_utils import now_local

logger = logging.getLogger("sa.scheduler")

# ─── 环境变量 ──────────────────────────────────────────────────────────────────

POLL_INTERVAL = float(os.environ.get("SA_SCHEDULER_POLL_INTERVAL", "3"))
HEARTBEAT_INTERVAL = float(os.environ.get("SA_SCHEDULER_HEARTBEAT_INTERVAL", "15"))
LEASE_TIMEOUT = max(30, int(os.environ.get("SA_SCHEDULER_LEASE_TIMEOUT", "300")))
POD_STALE_TIMEOUT = max(60, int(os.environ.get("SA_SCHEDULER_POD_STALE_TIMEOUT", "120")))
TASK_CONCURRENCY = int(os.environ.get("SA_SCHEDULER_TASK_CONCURRENCY", "1"))
MAX_GLOBAL_TASKS = max(0, int(os.environ.get("SA_SCHEDULER_MAX_GLOBAL_TASKS", "0")))
STALE_SWEEP_INTERVAL = float(os.environ.get("SA_SCHEDULER_STALE_SWEEP_INTERVAL", "30"))
OVERLOAD_COOLDOWN = float(os.environ.get("SA_SCHEDULER_OVERLOAD_COOLDOWN", "30"))

INSTANCE_ID = str(os.environ.get("POD_NAME") or f"sa-{uuid.uuid4().hex[:8]}")


# ═══════════════════════════════════════════════════════════════════════════════
# 数据结构
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class PodInfo:
    pod_id: str
    pod_ip: str = ""
    role: str = "runner"
    max_tasks: int = 1
    registered_at: float = 0.0
    last_heartbeat: float = 0.0
    task_ids: set[str] = field(default_factory=set)
    status: str = "online"

    @property
    def available_slots(self) -> int:
        return max(0, self.max_tasks - len(self.task_ids))

    @property
    def is_healthy(self) -> bool:
        return self.status == "online" and _time.time() - self.last_heartbeat < POD_STALE_TIMEOUT


@dataclass
class TaskAssignment:
    task_id: str
    pod_id: str
    lease_epoch: int = 0
    assigned_at: float = 0.0
    lease_expires: float = 0.0
    last_heartbeat: float = 0.0

    def is_expired(self) -> bool:
        return _time.time() > self.lease_expires


@dataclass
class RuntimeControl:
    claim_enabled: bool = True
    drain_mode: bool = False
    pause_until: float = 0.0
    reason: str | None = None
    updated_at: str | None = None


# ═══════════════════════════════════════════════════════════════════════════════
# SchedulerService
# ═══════════════════════════════════════════════════════════════════════════════

class SchedulerService:
    """统一调度器."""

    def __init__(
        self,
        get_db: Callable,
        task_repo: object,
        spawn_task: Callable,
        record_event: Callable,
        load_runtime_control: Callable | None = None,
        clear_task_lock: Callable | None = None,
        cleanup_resume: Callable | None = None,
    ):
        self._get_db = get_db
        self._task_repo = task_repo
        self._spawn_task = spawn_task
        self._record_event = record_event
        self._load_control = load_runtime_control or (lambda db: {})
        self._clear_lock = clear_task_lock or (lambda *a, **kw: None)
        self._cleanup_resume = cleanup_resume or (lambda *a, **kw: None)

        self._pods: dict[str, PodInfo] = {}
        self._tasks: dict[str, TaskAssignment] = {}
        self._control = RuntimeControl()
        self._lock = threading.Lock()

        self._running = False
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

        # 运行时状态
        self._last_tick = 0.0
        self._last_stale_recovery = 0.0
        self._last_error: str | None = None
        self._recovered_task_ids: set[str] = set()

    # ── 生命周期 ──────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._running = True
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="sa_scheduler", daemon=True)
        self._thread.start()
        logger.info("scheduler started (poll=%ss heartbeat=%ss lease=%ss max_global=%s)",
                     POLL_INTERVAL, HEARTBEAT_INTERVAL, LEASE_TIMEOUT,
                     MAX_GLOBAL_TASKS or "unlimited")

    def stop(self) -> None:
        self._running = False
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    # ── 主循环 ────────────────────────────────────────────────────────────

    def _run(self) -> None:
        while self._running and not self._stop.is_set():
            try:
                self._tick()
            except Exception as exc:
                self._last_error = str(exc)
                logger.exception("scheduler tick: %s", exc)
            self._stop.wait(timeout=POLL_INTERVAL)

    def _tick(self) -> None:
        now = _time.time()
        self._last_tick = now

        db_gen = self._get_db()
        db: Session = next(db_gen)
        try:
            # 加载运行时控制
            self._apply_control(self._load_control(db), now)

            # 1. 回收僵死 pod
            self._reap_stale_pods(now)

            # 2. 回收过期任务 (含 runtime evidence 检测)
            if now - self._last_stale_recovery >= STALE_SWEEP_INTERVAL:
                self._reap_stale_tasks(db, now)
                self._last_stale_recovery = now

            # 3. 分配 pending 任务
            if self._can_claim(now):
                self._dispatch(db, now)
        finally:
            try: next(db_gen)
            except StopIteration: pass

    # ── 运行时控制 ────────────────────────────────────────────────────────

    def _apply_control(self, payload: dict, now: float) -> None:
        self._control.claim_enabled = bool(payload.get("claim_enabled", True))
        self._control.drain_mode = bool(payload.get("drain_mode", False))
        try:
            self._control.pause_until = max(0.0, float(payload.get("pause_claim_until_ts", 0)))
        except (TypeError, ValueError):
            self._control.pause_until = 0.0
        if self._control.pause_until <= now:
            self._control.pause_until = 0.0
        self._control.reason = str(payload.get("reason", "")).strip() or None
        self._control.updated_at = str(payload.get("updated_at", "")).strip() or None

    def _can_claim(self, now: float) -> bool:
        if not self._control.claim_enabled:
            return False
        if self._control.drain_mode:
            return False
        if self._control.pause_until > now:
            return False
        return True

    # ── Pod 管理 ───────────────────────────────────────────────────────────

    def _reap_stale_pods(self, now: float) -> None:
        with self._lock:
            stale = [pid for pid, p in self._pods.items() if not p.is_healthy]
        for pid in stale:
            stale_tasks = list(self._pods[pid].task_ids)
            # 调度器主动通知 executor 清理
            self._notify_cleanup(pid, stale_tasks)
            with self._lock:
                for tid in stale_tasks:
                    self._tasks.pop(tid, None)
                    self._record_event(tid, None, "task_pod_stale",
                                       f"pod {pid} 失联, 任务回收", "warning",
                                       {"pod_id": pid})
                self._pods[pid].task_ids.clear()
                self._pods[pid].status = "stale"
            logger.warning("pod %s marked stale, %d tasks recycled", pid, len(stale_tasks))

    def _reap_stale_tasks(self, db: Session, now: float) -> None:
        """回收过期任务, 使用 TaskRepository 现有方法."""
        from datetime import datetime as _dt
        now_local = __import__('app.time_utils', fromlist=['now_local']).now_local()
        rows = self._task_repo.recover_stale_running_tasks(
            db, now=now_local, lease_timeout_seconds=LEASE_TIMEOUT,
            clear_task_execution_lock=self._clear_lock,
            cleanup_resume_files=self._cleanup_resume,
            should_recover=None,
        )
        for row in rows:
            tid = row.task_id
            self._recovered_task_ids.add(tid)
            self._record_event(tid, getattr(row, "project_id", None),
                               "task_lease_recovered",
                               "任务租约过期，已回收并重新排队",
                               "warning",
                               {"lease_epoch": getattr(row, "lease_epoch", 0)})

    # ── 任务分配 ──────────────────────────────────────────────────────────

    def _dispatch(self, db: Session, now: float) -> None:
        if MAX_GLOBAL_TASKS > 0:
            running = self._task_repo.count_running_tasks(db)
            if running >= MAX_GLOBAL_TASKS:
                return

        with self._lock:
            available = [p for p in self._pods.values()
                         if p.is_healthy and p.available_slots > 0]
        if not available:
            return

        pending_rows = self._task_repo.list_pending_tasks(db, len(available))
        for i, row in enumerate(pending_rows):
            if i >= len(available):
                break
            pod = available[i]
            from app.time_utils import now_local, datetime as _dt
            deadline = now_local() + __import__('datetime').timedelta(seconds=LEASE_TIMEOUT)
            lease_epoch = self._task_repo.claim_task_lease(
                db, row, worker_instance_id=pod.pod_id,
                lease_deadline=lambda: deadline,
            )
            if not lease_epoch:
                continue

            with self._lock:
                pod.task_ids.add(row.task_id)
                self._tasks[row.task_id] = TaskAssignment(
                    task_id=row.task_id, pod_id=pod.pod_id,
                    lease_epoch=lease_epoch, assigned_at=now,
                    lease_expires=now + LEASE_TIMEOUT, last_heartbeat=now,
                )

            self._spawn_task(row.task_id, pod.pod_id)
            self._record_event(row.task_id, getattr(row, "project_id", None),
                               "task_assigned",
                               f"任务已分配给 {pod.pod_id}", None,
                               {"pod_id": pod.pod_id, "lease_epoch": lease_epoch})
            if row.task_id in self._recovered_task_ids:
                self._recovered_task_ids.discard(row.task_id)
                self._record_event(row.task_id, getattr(row, "project_id", None),
                                   "task_auto_recovered",
                                   "任务已由系统自动恢复并重新调度", None,
                                   {"pod_id": pod.pod_id, "lease_epoch": lease_epoch,
                                    "reason": "lease_recovered_and_reclaimed"})

    def _notify_cleanup(self, pod_id: str, task_ids: list[str]) -> None:
        """通知 executor pod 清理指定任务的所有残留进程."""
        pod = self._pods.get(pod_id)
        if not pod or not pod.pod_ip:
            return
        for tid in task_ids:
            try:
                url = f"http://{pod.pod_ip}:8080/api/internal/scheduler/cleanup"
                body = json.dumps({"task_id": tid}).encode()
                req = urllib.request.Request(url, data=body, method="POST")
                req.add_header("Content-Type", "application/json")
                urllib.request.urlopen(req, timeout=5)
            except Exception:
                pass

    # ── Pod API ────────────────────────────────────────────────────────────

    def register_pod(self, pod_id: str, pod_ip: str = "", role: str = "runner",
                     max_tasks: int = 1) -> dict:
        with self._lock:
            if pod_id in self._pods:
                self._pods[pod_id].pod_ip = pod_ip
                self._pods[pod_id].last_heartbeat = _time.time()
                self._pods[pod_id].status = "online"
            else:
                self._pods[pod_id] = PodInfo(
                    pod_id=pod_id, pod_ip=pod_ip, role=role,
                    max_tasks=max_tasks, registered_at=_time.time(),
                    last_heartbeat=_time.time(),
                )
            return {"status": "ok", "capacity": self._pods[pod_id].available_slots}

    def pod_heartbeat(self, pod_id: str, task_ids: list[str] | None = None) -> dict:
        with self._lock:
            if pod_id not in self._pods:
                return {"status": "unknown_pod"}
            p = self._pods[pod_id]
            p.last_heartbeat = _time.time()
            if task_ids is not None:
                p.task_ids = set(task_ids)
            for tid in p.task_ids:
                if tid in self._tasks:
                    self._tasks[tid].last_heartbeat = _time.time()
                    self._tasks[tid].lease_expires = _time.time() + LEASE_TIMEOUT
            return {"status": "ok"}

    def task_heartbeat(self, task_id: str) -> dict:
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id].last_heartbeat = _time.time()
                self._tasks[task_id].lease_expires = _time.time() + LEASE_TIMEOUT
                return {"status": "ok"}
            return {"status": "unknown_task"}

    def task_completed(self, task_id: str, status: str = "completed") -> dict:
        """任务完成通知。调度器触发清理。"""
        pod_id = ""
        with self._lock:
            if task_id in self._tasks:
                a = self._tasks.pop(task_id)
                pod_id = a.pod_id
                if a.pod_id in self._pods:
                    self._pods[a.pod_id].task_ids.discard(task_id)
        # 通知 executor 清理
        if pod_id and pod_id in self._pods:
            self._notify_cleanup(pod_id, [task_id])
        return {"status": "ok"}

    def health(self) -> dict:
        return {
            "status": "ok" if self._running else "stopped",
            "pods": len(self._pods),
            "tasks": len(self._tasks),
            "last_tick": self._last_tick,
            "last_error": self._last_error,
            "control": {
                "claim_enabled": self._control.claim_enabled,
                "drain_mode": self._control.drain_mode,
                "pause_until": self._control.pause_until,
            },
        }

    def status(self) -> dict:
        with self._lock:
            return {
                "pods": {pid: {"role": p.role, "status": p.status,
                                "tasks": len(p.task_ids), "slots": p.available_slots}
                         for pid, p in self._pods.items()},
                "tasks": {tid: {"pod": t.pod_id, "lease_epoch": t.lease_epoch,
                                "age": _time.time() - t.assigned_at}
                          for tid, t in self._tasks.items()},
            }


# ═══════════════════════════════════════════════════════════════════════════════
# ExecutorAgent
# ═══════════════════════════════════════════════════════════════════════════════

class ExecutorAgent:
    """Runner pod 端: 注册/心跳/接收清理指令."""

    def __init__(self, scheduler_url: str, pod_id: str = INSTANCE_ID,
                 pod_ip: str = ""):
        self._url = scheduler_url.rstrip("/")
        self._pod_id = pod_id
        self._pod_ip = pod_ip or os.environ.get("SA_POD_IP", "")
        self._running = False
        self._hb_thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._tasks: set[str] = set()

    def start(self) -> None:
        self._running = True
        self._stop.clear()
        self._call("register", {"pod_id": self._pod_id, "pod_ip": self._pod_ip,
                                 "role": "runner", "max_tasks": TASK_CONCURRENCY})
        self._hb_thread = threading.Thread(target=self._hb_loop,
                                            name="sa_exec_hb", daemon=True)
        self._hb_thread.start()
        logger.info("executor agent started (scheduler=%s)", self._url)

    def stop(self) -> None:
        self._running = False
        self._stop.set()
        if self._hb_thread:
            self._hb_thread.join(timeout=5)

    def add_task(self, task_id: str) -> None:
        self._tasks.add(task_id)

    def remove_task(self, task_id: str) -> None:
        self._tasks.discard(task_id)
        self._call("task_completed", {"task_id": task_id})

    def _hb_loop(self) -> None:
        while self._running and not self._stop.is_set():
            try:
                self._call("heartbeat", {
                    "pod_id": self._pod_id,
                    "task_ids": list(self._tasks),
                })
            except Exception:
                pass
            self._stop.wait(timeout=HEARTBEAT_INTERVAL)

    def _call(self, ep: str, data: dict) -> dict:
        url = f"{self._url}/api/internal/scheduler/{ep}"
        body = json.dumps(data).encode()
        req = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return json.loads(r.read())
        except Exception:
            return {"status": "error"}


# ═══════════════════════════════════════════════════════════════════════════════
# 独立心跳子进程
# ═══════════════════════════════════════════════════════════════════════════════

def run_heartbeat_process(scheduler_url: str, pod_id: str, task_id: str,
                          interval: float = HEARTBEAT_INTERVAL,
                          timeout: float = LEASE_TIMEOUT) -> None:
    """独立子进程: 定期向调度器上报任务心跳."""
    deadline = _time.time() + timeout
    while _time.time() < deadline:
        time.sleep(interval)
        try:
            url = f"{scheduler_url}/api/internal/scheduler/task_heartbeat"
            body = json.dumps({"task_id": task_id}).encode()
            req = urllib.request.Request(url, data=body, method="POST")
            req.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req, timeout=5) as r:
                if json.loads(r.read()).get("status") != "ok":
                    break
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════════
# 进程清理 — 任务结束后（任何状态）清理所有关联进程
# ═══════════════════════════════════════════════════════════════════════════════

def cleanup_task_processes(task_id: str, protected_pids: set[int] | None = None) -> int:
    """清理指定任务的所有关联进程。

    白名单机制: 保护系统进程 (main.py, uvicorn, probe, cleanup自身),
    杀除白名单外的所有 task_id 关联进程。

    清理流程: SIGTERM → 等 2s → SIGKILL (ESRCH 视为已退出)
    返回清理的进程数。
    """
    import glob as _glob

    if protected_pids is None:
        protected_pids = _build_protected_set()
    protected_pids.add(os.getpid())  # 保护清理进程自身

    task_pids: set[int] = set()

    for proc_dir in _glob.glob("/proc/[0-9]*"):
        try:
            pid = int(os.path.basename(proc_dir))
            if pid in protected_pids:
                continue
            cmdline_path = os.path.join(proc_dir, "cmdline")
            with open(cmdline_path, "rb") as f:
                cmdline = f.read().decode("utf-8", errors="replace")
            if task_id not in cmdline:
                continue
            # 白名单: 只保护 infrastructure 进程
            # 其他含 task_id 的进程一律清理
            task_pids.add(pid)
        except (OSError, ValueError):
            continue

    if not task_pids:
        return 0

    logger.info("cleaning up %d processes for task %s (protected=%d): %s",
                len(task_pids), task_id, len(protected_pids), sorted(task_pids))

    killed = 0
    for pid in task_pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass

    time.sleep(2)

    for pid in list(task_pids):
        try:
            os.kill(pid, signal.SIGKILL)
            killed += 1
        except OSError:
            killed += 1

    logger.info("cleaned up %d processes for task %s", killed, task_id)
    return killed


def _build_protected_set() -> set[int]:
    """构建受保护进程白名单: main.py, uvicorn, probe 及其一级子进程。"""
    protected: set[int] = {1, os.getpid()}  # init + cleanup自身
    my_pid = os.getpid()
    try:
        # 扫描所有进程, 保护 infrastructure
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            pid = int(entry)
            if pid == my_pid:
                continue
            try:
                with open(f"/proc/{pid}/cmdline", "rb") as f:
                    cmdline = f.read().decode("utf-8", errors="replace")
                # 保护 infrastructure 进程
                cmd = cmdline.replace("\x00", " ").strip()
                is_infra = any(kw in cmd for kw in [
                    "main.py",        # executor 主进程
                    "uvicorn",        # HTTP server
                    "probe_process",  # health probe
                    "probe_sidecar",  # health probe
                    "gunicorn",       # WSGI server
                    "entrypoint.sh",  # container entry
                    "start-with-probe.sh",
                ])
                if is_infra:
                    protected.add(pid)
            except (OSError, ValueError):
                pass
    except (OSError, FileNotFoundError):
        pass
    return protected


class TaskGuard:
    """任务生命周期守卫: 无论成功/失败/取消/异常, 确保清理。

    用法:
      guard = TaskGuard(task_id, scheduler_url, pod_id)
      guard.start()  # 记录开始, 启动心跳子进程
      try:
          orch.execute(task_id)
          guard.complete()  # 正常完成
      except:
          guard.fail()  # 失败
      finally:
          guard.cleanup()  # 无论如何都清理
    """

    def __init__(self, task_id: str, scheduler_url: str = "", pod_id: str = ""):
        self.task_id = task_id
        self._scheduler = scheduler_url
        self._pod_id = pod_id
        self._hb_proc: subprocess.Popen | None = None
        self._started = False

    def start(self) -> None:
        self._started = True
        if self._scheduler and self._pod_id:
            self._hb_proc = subprocess.Popen(
                ["python3", "-c", f"""
import json, time, urllib.request, sys
url = "{self._scheduler}/api/internal/scheduler/task_heartbeat"
tid = "{self.task_id}"
interval = {HEARTBEAT_INTERVAL}
timeout = {LEASE_TIMEOUT}
deadline = __import__('time').time() + timeout
while __import__('time').time() < deadline:
    __import__('time').sleep(interval)
    try:
        body = json.dumps({{"task_id": tid}}).encode()
        req = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=5) as r:
            if json.loads(r.read()).get("status") != "ok":
                break
    except:
        pass
"""],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True,
            )

    def complete(self) -> None:
        self._notify("completed")

    def fail(self) -> None:
        self._notify("failed")

    def cleanup(self) -> None:
        if self._hb_proc and self._hb_proc.poll() is None:
            try:
                self._hb_proc.terminate()
                self._hb_proc.wait(timeout=3)
            except Exception:
                try:
                    self._hb_proc.kill()
                except Exception:
                    pass
        if self._started:
            cleanup_task_processes(self.task_id)

    def _notify(self, status: str) -> None:
        if not self._scheduler:
            return
        try:
            url = f"{self._scheduler}/api/internal/scheduler/task_completed"
            body = json.dumps({"task_id": self.task_id, "status": status}).encode()
            req = urllib.request.Request(url, data=body, method="POST")
            req.add_header("Content-Type", "application/json")
            urllib.request.urlopen(req, timeout=5)
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════════
# 内部 API Router
# ═══════════════════════════════════════════════════════════════════════════════

_scheduler_instance: SchedulerService | None = None


def set_scheduler(s: SchedulerService) -> None:
    global _scheduler_instance
    _scheduler_instance = s


def get_scheduler() -> SchedulerService | None:
    return _scheduler_instance


def create_scheduler_router():
    """创建调度器内部通信 API."""
    from fastapi import APIRouter
    from pydantic import BaseModel

    router = APIRouter(prefix="/api/internal/scheduler")

    class RegisterReq(BaseModel):
        pod_id: str; pod_ip: str = ""; role: str = "runner"; max_tasks: int = 1

    class HeartbeatReq(BaseModel):
        pod_id: str; task_ids: list[str] = []

    class TaskHbReq(BaseModel):
        task_id: str

    class TaskDoneReq(BaseModel):
        task_id: str; status: str = "completed"

    class CleanupReq(BaseModel):
        task_id: str

    @router.post("/register")
    def register(req: RegisterReq):
        s = get_scheduler()
        return s.register_pod(req.pod_id, req.pod_ip, req.role, req.max_tasks) if s else {"status": "no_scheduler"}

    @router.post("/heartbeat")
    def heartbeat(req: HeartbeatReq):
        s = get_scheduler()
        return s.pod_heartbeat(req.pod_id, req.task_ids) if s else {"status": "no_scheduler"}

    @router.post("/task_heartbeat")
    def task_heartbeat(req: TaskHbReq):
        s = get_scheduler()
        return s.task_heartbeat(req.task_id) if s else {"status": "no_scheduler"}

    @router.post("/task_completed")
    def task_completed(req: TaskDoneReq):
        s = get_scheduler()
        return s.task_completed(req.task_id, req.status) if s else {"status": "no_scheduler"}

    @router.post("/cleanup")
    def cleanup(req: CleanupReq):
        killed = cleanup_task_processes(req.task_id)
        return {"status": "ok", "killed": killed}

    @router.get("/health")
    def health():
        s = get_scheduler()
        return s.health() if s else {"status": "no_scheduler"}

    @router.get("/status")
    def status():
        s = get_scheduler()
        return s.status() if s else {"status": "no_scheduler"}

    return router
