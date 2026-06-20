"""scheduler_v3.py — V3.0 调度器（manager 上的纯 TCP/内存 控制面，无 DB 实时依赖）。

设计见 docs/scheduler_v3_design.md。按"纯 TCP、去掉 DB 实时依赖"实现：

  控制面（实时，纯内存 + TCP，不碰 DB）:
    - 任务队列 deque + 运行中 dict{task_id: RunRecord} + worker 连接表
    - 顺序 push 派发（非抢占，FIFO）；一个 worker 同时最多 1 个任务
    - worker 心跳/状态全走 TCP；TCP 断联/超时 = worker 死
    - cancel/restart 命令 push 给 owner worker
    - 状态持久化到 JSON 文件（NFS，跨 manager 重启），不写 DB
  持久化边界:
    - DB 仅用于「任务记录/前端展示与终态落库」(创建时写一行，完成时回写终态)，控制面不读/不写它
    - 实时派发/心跳/回收/状态 全在内存 + 文件，无 DB 实时依赖
    - rollout: manager 重启→加载文件队列+运行中→worker 重连对账→未恢复任务重排

内部 HTTP (供 API pod / 前端 调用，非 DB):
    POST /api/internal/sched/submit   {task_id}            API 创建任务后通知调度器入队
    POST /api/internal/sched/cancel    {task_id}            取消
    POST /api/internal/sched/restart   {task_id}            重启
    GET  /api/internal/sched/task_status?task_id=           前端实时状态
    GET  /api/internal/sched/cluster_status                集群 worker/队列总览
"""
from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time as _time
from collections import deque
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Callable

from app.service import sched_proto as proto
from app.time_utils import now_local

logger = logging.getLogger("sa.scheduler_v3")

# ── 配置 ───────────────────────────────────────────────────────────────────
SCHED_BIND = os.environ.get("SECFLOW_SYSTEM_ANALYSE_SCHED_BIND", "0.0.0.0")
SCHED_PORT = int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_SCHED_PORT", "8090"))
DISPATCH_POLL_INTERVAL = float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_SCHED_POLL", "2"))
WORKER_HEARTBEAT_TIMEOUT = max(10.0, float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_WORKER_HB_TIMEOUT", "30")))
RECONCILE_INTERVAL = float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_SCHED_RECONCILE", "5"))
STATE_FILE = os.environ.get("SECFLOW_SYSTEM_ANALYSE_SCHED_STATE_FILE", "/data/sa_scheduler_state.json")
PERSIST_INTERVAL = float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_SCHED_PERSIST", "3"))


@dataclass
class RunRecord:
    task_id: str
    worker_id: str
    started_ts: float
    state: str = proto.STATE_RUNNING
    error: str | None = None
    finished_ts: float | None = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class WorkerConn:
    worker_id: str
    ip: str = ""
    max_tasks: int = 1
    current_task: str | None = None
    last_heartbeat: float = 0.0
    online: bool = True
    sock: socket.socket | None = None

    @property
    def is_idle(self) -> bool:
        return self.online and self.current_task is None


class SchedulerV3:
    def __init__(
        self,
        *,
        finalize_task: Callable[[str, str, "str | None", Any], None] | None = None,
        record_event: Callable[..., None] | None = None,
        bind: str = SCHED_BIND,
        port: int = SCHED_PORT,
        state_file: str = STATE_FILE,
    ) -> None:
        self._finalize = finalize_task or (lambda tid, state, err, result: None)
        self._record = record_event or (lambda *a, **kw: None)
        self._bind = bind
        self._port = port
        self._state_file = Path(state_file)

        self._queue: deque[str] = deque()
        self._running: dict[str, RunRecord] = {}
        self._workers: dict[str, WorkerConn] = {}
        self._lock = threading.RLock()

        self._running_flag = False
        self._stop = threading.Event()
        self._server_sock: socket.socket | None = None
        self._threads: list[threading.Thread] = []
        self._dirty = False

    # ── 生命周期 ────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running_flag:
            return
        self._running_flag = True
        self._stop.clear()
        self._load_state()
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self._bind, self._port))
        self._server_sock.listen(64)
        for name, tgt in (("accept", self._accept_loop), ("dispatch", self._dispatch_loop),
                          ("reconcile", self._reconcile_loop), ("persist", self._persist_loop)):
            t = threading.Thread(target=tgt, name=f"sa_v3_{name}", daemon=True)
            t.start()
            self._threads.append(t)
        logger.info("SchedulerV3(pure-tcp) started: %s:%s queue=%d running=%d",
                    self._bind, self._port, len(self._queue), len(self._running))

    def stop(self) -> None:
        self._running_flag = False
        self._stop.set()
        s, self._server_sock = self._server_sock, None
        if s is not None:
            try: s.close()
            except OSError: pass
        with self._lock:
            for w in list(self._workers.values()):
                self._close_worker(w)
            self._workers.clear()
        self._save_state()
        for t in self._threads:
            t.join(timeout=3)

    # ── 状态持久化（文件，非 DB）─────────────────────────────────────────────

    def _load_state(self) -> None:
        try:
            if not self._state_file.exists():
                return
            data = json.loads(self._state_file.read_text("utf-8"))
            with self._lock:
                for tid in data.get("queue", []):
                    if tid not in self._queue and tid not in self._running:
                        self._queue.append(tid)
                for rec in data.get("running", []):
                    r = RunRecord(task_id=rec["task_id"], worker_id=rec["worker_id"],
                                  started_ts=rec.get("started_ts", _time.time()),
                                  state=rec.get("state", proto.STATE_RUNNING),
                                  error=rec.get("error"), finished_ts=rec.get("finished_ts"))
                    self._running[r.task_id] = r
            logger.info("SchedulerV3 loaded state: queue=%d running=%d", len(self._queue), len(self._running))
        except Exception as exc:
            logger.warning("load scheduler state failed: %s", exc)

    def _save_state(self) -> None:
        with self._lock:
            payload = {"queue": list(self._queue),
                       "running": [r.to_dict() for r in self._running.values()],
                       "saved_at": now_local().isoformat()}
        try:
            tmp = self._state_file.with_suffix(self._state_file.suffix + ".tmp")
            self._state_file.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(json.dumps(payload, ensure_ascii=False), "utf-8")
            tmp.replace(self._state_file)
            self._dirty = False
        except Exception as exc:
            logger.warning("save scheduler state failed: %s", exc)

    def _persist_loop(self) -> None:
        while self._running_flag and not self._stop.wait(timeout=PERSIST_INTERVAL):
            if self._dirty:
                self._save_state()

    # ── TCP accept + worker 消息循环 ─────────────────────────────────────────

    def _accept_loop(self) -> None:
        while self._running_flag and self._server_sock is not None:
            try:
                conn, _addr = self._server_sock.accept()
            except OSError:
                break
            threading.Thread(target=self._serve_worker, args=(conn,), name="sa_v3_worker", daemon=True).start()

    def _serve_worker(self, conn: socket.socket) -> None:
        worker_id: str | None = None
        try:
            reader = conn.makefile("rb")
            while self._running_flag:
                msg = proto.read_frame(reader)
                if msg is None:
                    break
                try:
                    wid = self._handle_worker_msg(conn, msg)
                    if wid:
                        worker_id = wid
                except Exception:
                    logger.exception("handle worker msg failed: %s", msg)
                    break
        except OSError:
            pass
        finally:
            if worker_id:
                self._on_worker_disconnect(worker_id)
            try: conn.close()
            except OSError: pass

    def _handle_worker_msg(self, conn: socket.socket, msg: dict) -> str | None:
        mtype = msg.get("type")
        if mtype == proto.MSG_HELLO:
            wid = str(msg.get("worker_id") or "").strip()
            if not wid:
                self._send(conn, proto.msg_error("hello missing worker_id"))
                return None
            self._register_worker(wid, conn, ip=str(msg.get("ip") or ""), max_tasks=int(msg.get("max_tasks") or 1))
            self._send(conn, proto.msg_ok(worker_id=wid))
            self._record("worker_connected", f"worker {wid} 已连接", "info", {"worker_id": wid})
            return wid
        wid = str(msg.get("worker_id") or "").strip()
        if not wid:
            return None
        if mtype == proto.MSG_HEARTBEAT:
            self._on_heartbeat(wid, msg)
        elif mtype == proto.MSG_TASK_STATE:
            self._on_task_state(wid, msg)
        return wid

    def _register_worker(self, wid: str, conn: socket.socket, ip: str, max_tasks: int) -> None:
        with self._lock:
            old = self._workers.get(wid)
            if old is not None and old.sock is not None and old.sock is not conn:
                self._close_worker(old)
            self._workers[wid] = WorkerConn(worker_id=wid, ip=ip, max_tasks=max(1, max_tasks),
                                            last_heartbeat=_time.time(), online=True, sock=conn)
            self._dirty = True

    def _on_heartbeat(self, wid: str, msg: dict) -> None:
        with self._lock:
            w = self._workers.get(wid)
            if w is None:
                return
            w.last_heartbeat = _time.time()
            w.online = True
            w.current_task = msg.get("task_id") or None

    def _on_task_state(self, wid: str, msg: dict) -> None:
        task_id = str(msg.get("task_id") or "")
        state = str(msg.get("state") or "")
        err = msg.get("error")
        result = msg.get("result")
        finished = False
        with self._lock:
            w = self._workers.get(wid)
            if w is not None and w.current_task == task_id and state in (
                    proto.STATE_FINISHED, proto.STATE_FAILED, proto.STATE_CANCELLED):
                w.current_task = None
            rec = self._running.get(task_id)
            if rec is not None:
                rec.state = state
                if err:
                    rec.error = str(err)
                if state in (proto.STATE_FINISHED, proto.STATE_FAILED, proto.STATE_CANCELLED):
                    rec.finished_ts = _time.time()
                    finished = True
            self._dirty = True
        self._record_event_for(task_id, "worker_task_state",
                               f"worker {wid} 任务 {task_id} 状态={state}", "info",
                               {"worker_id": wid, "task_id": task_id, "state": state, "error": err})
        if finished:
            try:
                self._finalize(task_id, state, err, result)
            except Exception:
                logger.exception("finalize task failed: %s", task_id)
            with self._lock:
                self._running.pop(task_id, None)
                self._dirty = True

    def _on_worker_disconnect(self, wid: str) -> None:
        with self._lock:
            w = self._workers.get(wid)
            if w is None:
                return
            w.online = False
            w.sock = None
            self._dirty = True
        self._record("worker_disconnected", f"worker {wid} 断联", "warning", {"worker_id": wid})

    def _close_worker(self, w: WorkerConn) -> None:
        w.online = False
        s, w.sock = w.sock, None
        if s is not None:
            try: s.close()
            except OSError: pass

    def _send(self, conn: socket.socket, msg: dict) -> bool:
        try:
            conn.sendall(proto.encode(msg))
            return True
        except OSError:
            return False

    # ── 派发主循环（顺序 push，非抢占，纯内存）──────────────────────────────

    def _dispatch_loop(self) -> None:
        while self._running_flag and not self._stop.wait(timeout=DISPATCH_POLL_INTERVAL):
            try:
                self._dispatch_once()
            except Exception as exc:
                logger.exception("dispatch loop: %s", exc)

    def _dispatch_once(self) -> None:
        with self._lock:
            if not self._queue:
                return
            idle = [w for w in self._workers.values() if w.is_idle]
            if not idle:
                return
            idle.sort(key=lambda w: (w.last_heartbeat, w.worker_id))
            w = idle[0]
            task_id = self._queue.popleft()
            w.current_task = task_id
            w.last_heartbeat = _time.time()
            self._running[task_id] = RunRecord(task_id=task_id, worker_id=w.worker_id, started_ts=_time.time())
            conn = w.sock
            self._dirty = True
        sent = self._send(conn, proto.msg_run(task_id, lease_epoch=0)) if conn is not None else False
        self._record_event_for(task_id, "task_dispatched", f"任务已下发 worker={w.worker_id}", "info",
                               {"worker_id": w.worker_id})
        if not sent:
            with self._lock:
                self._running.pop(task_id, None)
                if task_id not in self._queue:
                    self._queue.appendleft(task_id)
                ww = self._workers.get(w.worker_id)
                if ww is not None and ww.current_task == task_id:
                    ww.current_task = None
                self._dirty = True

    # ── 对账循环（worker 死亡 → 任务重排；清理失联 worker）──────────────────

    def _reconcile_loop(self) -> None:
        while self._running_flag and not self._stop.wait(timeout=RECONCILE_INTERVAL):
            try:
                self._reconcile_once()
            except Exception as exc:
                logger.exception("reconcile loop: %s", exc)

    def _reconcile_once(self) -> None:
        now = _time.time()
        requeue: list[str] = []
        with self._lock:
            for w in self._workers.values():
                if w.online and now - w.last_heartbeat > WORKER_HEARTBEAT_TIMEOUT:
                    w.online = False
                    self._record("worker_heartbeat_timeout", f"worker {w.worker_id} 心跳超时", "warning", {"worker_id": w.worker_id})
            for tid, rec in list(self._running.items()):
                w = self._workers.get(rec.worker_id)
                owner_alive = (w is not None and w.online and w.current_task == tid)
                if not owner_alive:
                    rec.state = proto.STATE_FAILED
                    rec.error = "worker_lost"
                    requeue.append(tid)
            for tid in requeue:
                self._running.pop(tid, None)
                if tid not in self._queue:
                    self._queue.append(tid)
            dead = [wid for wid, w in self._workers.items()
                    if not w.online and w.current_task is None and now - w.last_heartbeat > WORKER_HEARTBEAT_TIMEOUT * 4]
            for wid in dead:
                self._workers.pop(wid, None)
            if requeue or dead:
                self._dirty = True
        for tid in requeue:
            self._record_event_for(tid, "task_requeued", "worker 失联，任务重新排队", "warning", {"task_id": tid})
            try:
                self._finalize(tid, proto.STATE_FAILED, "worker_lost", None)
            except Exception:
                logger.exception("finalize(worker_lost) failed: %s", tid)

    # ── 内部 API（HTTP → 内存控制面；无 DB）──────────────────────────────────

    def submit(self, task_id: str) -> dict:
        with self._lock:
            if task_id in self._queue or task_id in self._running:
                return {"status": "already_queued", "task_id": task_id}
            self._queue.append(task_id)
            self._dirty = True
        self._record_event_for(task_id, "task_submitted", "任务已入调度队列", "info", {"task_id": task_id})
        return {"status": "queued", "task_id": task_id, "queue_len": len(self._queue)}

    def cancel(self, task_id: str) -> dict:
        with self._lock:
            if task_id in self._queue:
                try: self._queue.remove(task_id)
                except ValueError: pass
                self._dirty = True
                self._record_event_for(task_id, "task_cancelled", "任务从队列取消", "warning", {"task_id": task_id})
                try: self._finalize(task_id, proto.STATE_CANCELLED, "cancelled", None)
                except Exception: logger.exception("finalize cancel(queued) failed: %s", task_id)
                return {"status": "cancelled_from_queue", "task_id": task_id}
            rec = self._running.get(task_id)
            if rec is None:
                return {"status": "not_found", "task_id": task_id}
            w = self._workers.get(rec.worker_id)
            conn = w.sock if (w is not None and w.online) else None
            worker_id = rec.worker_id
        sent = self._send(conn, proto.msg_cancel(task_id)) if conn is not None else False
        self._record_event_for(task_id, "task_cancel_commanded", f"已向 worker {worker_id} 发送取消命令", "info",
                               {"worker_id": worker_id})
        return {"status": "cancel_commanded" if sent else "worker_offline", "task_id": task_id, "worker_id": worker_id}

    def restart(self, task_id: str) -> dict:
        with self._lock:
            rec = self._running.pop(task_id, None)
            conn = None
            if rec is not None:
                w = self._workers.get(rec.worker_id)
                conn = w.sock if (w is not None and w.online) else None
            try: self._queue.remove(task_id)
            except ValueError: pass
            if task_id not in self._queue:
                self._queue.append(task_id)
            self._dirty = True
        if conn is not None:
            self._send(conn, proto.msg_cancel(task_id))
        self._record_event_for(task_id, "task_restart_queued", "任务重新入队", "info", {"task_id": task_id})
        return {"status": "requeued", "task_id": task_id, "queue_len": len(self._queue)}

    def task_status(self, task_id: str) -> dict:
        with self._lock:
            if task_id in self._queue:
                return {"task_id": task_id, "status": "pending", "queue_pos": list(self._queue).index(task_id)}
            rec = self._running.get(task_id)
            if rec is not None:
                return {"task_id": task_id, "status": rec.state, "worker_id": rec.worker_id,
                        "started_ts": rec.started_ts, "error": rec.error}
        return {"task_id": task_id, "status": "unknown"}

    def cluster_status(self) -> dict:
        with self._lock:
            workers = [{"worker_id": w.worker_id, "online": w.online, "task": w.current_task,
                        "hb_age": round(_time.time() - w.last_heartbeat, 1)} for w in self._workers.values()]
            return {"queue_len": len(self._queue), "running": len(self._running),
                    "idle_workers": sum(1 for w in self._workers.values() if w.is_idle),
                    "workers": workers,
                    "running_tasks": [r.to_dict() for r in self._running.values()]}

    def health(self) -> dict:
        with self._lock:
            return {"status": "ok" if self._running_flag else "stopped", "version": "v3-pure-tcp",
                    "tcp": f"{self._bind}:{self._port}",
                    "queue": len(self._queue), "running": len(self._running), "workers": len(self._workers)}

    # record_event 适配：允许 (task_id, event_type, msg, level, payload) 或 (event_type, msg, level, payload)
    def _record_event_for(self, task_id: str | None, event_type: str, msg: str, level: str = "info", payload: dict | None = None) -> None:
        try:
            self._record(task_id, event_type, msg, level, payload)
        except TypeError:
            self._record(event_type=event_type, message=msg, level=level, payload=payload, task_id=task_id)


# ═══════════════════════════════════════════════════════════════════════════════
# 单例 + 内部 HTTP Router（manager pod 暴露给 API pod / 前端）
# ═══════════════════════════════════════════════════════════════════════════════

_scheduler_instance: SchedulerV3 | None = None


def set_scheduler(s: SchedulerV3) -> None:
    global _scheduler_instance
    _scheduler_instance = s


def get_scheduler() -> SchedulerV3 | None:
    return _scheduler_instance


def create_sched_router():
    from fastapi import APIRouter
    from pydantic import BaseModel

    router = APIRouter(prefix="/api/internal/sched")

    class TaskReq(BaseModel):
        task_id: str

    @router.post("/submit")
    def submit(req: TaskReq):
        s = get_scheduler()
        return s.submit(req.task_id) if s else {"status": "no_scheduler"}

    @router.post("/cancel")
    def cancel(req: TaskReq):
        s = get_scheduler()
        return s.cancel(req.task_id) if s else {"status": "no_scheduler"}

    @router.post("/restart")
    def restart(req: TaskReq):
        s = get_scheduler()
        return s.restart(req.task_id) if s else {"status": "no_scheduler"}

    @router.get("/task_status")
    def task_status(task_id: str):
        s = get_scheduler()
        return s.task_status(task_id) if s else {"status": "no_scheduler", "task_id": task_id}

    @router.get("/cluster_status")
    def cluster_status():
        s = get_scheduler()
        return s.cluster_status() if s else {"status": "no_scheduler"}

    @router.get("/health")
    def health():
        s = get_scheduler()
        return s.health() if s else {"status": "no_scheduler"}

    return router
