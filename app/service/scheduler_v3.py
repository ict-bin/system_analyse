"""scheduler_v3.py — V3.0 调度器（manager 上的 TCP server，单一权威派发）。

设计见 docs/scheduler_v3_design.md。核心：
  - TCP server 接受 worker 控制进程的持久连接；内存维护 worker 实时表（能力/在跑任务/心跳）。
  - 顺序 push 派发：从 DB 取 pending(FIFO) → 选空闲 worker → 写 DB lease → 发 RUN 命令。
  - 一个 worker 同一时刻最多 1 个任务（worker 端也强制单任务）。
  - TCP 断联 / 心跳超时 = worker 死 → 其在跑任务进入"待回收"。
  - 对账回收：DB 仍为任务账本；running 任务 owner 失联超宽限 → 置 pending 重排（rollout 安全）。
  - cancel/restart：API 调调度器 → 向 owner worker 发 CANCEL/RESTART 命令。

DB 仅用于任务记录/终态持久化，不再承担实时派发/心跳（实时控制面在 TCP/内存）。
"""
from __future__ import annotations

import logging
import os
import socket
import threading
import time
import time as _time
from dataclasses import dataclass, field
from typing import Any, Callable

from sqlalchemy.orm import Session

from app.service import sched_proto as proto
from app.time_utils import now_local

logger = logging.getLogger("sa.scheduler_v3")

# ── 配置（环境变量）──────────────────────────────────────────────────────────
SCHED_BIND = os.environ.get("SECFLOW_SYSTEM_ANALYSE_SCHED_BIND", "0.0.0.0")
SCHED_PORT = int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_SCHED_PORT", "8090"))
DISPATCH_POLL_INTERVAL = float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_SCHED_POLL", "3"))
WORKER_HEARTBEAT_TIMEOUT = max(10.0, float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_WORKER_HB_TIMEOUT", "30")))
WORKER_RECOVER_GRACE = max(WORKER_HEARTBEAT_TIMEOUT, float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_WORKER_RECOVER_GRACE", "60")))
RECOVER_SWEEP_INTERVAL = float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_SCHED_RECOVER_SWEEP", "15"))


# ═══════════════════════════════════════════════════════════════════════════════
# Worker 连接表（内存实时状态）
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class WorkerConn:
    worker_id: str
    ip: str = ""
    max_tasks: int = 1
    current_task: str | None = None          # 该 worker 正在跑的 task_id（仅 1 个）
    last_heartbeat: float = 0.0
    online: bool = True
    sock: socket.socket | None = None
    writer_lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def is_idle(self) -> bool:
        return self.online and self.current_task is None


# ═══════════════════════════════════════════════════════════════════════════════
# SchedulerV3
# ═══════════════════════════════════════════════════════════════════════════════

class SchedulerV3:
    """V3.0 调度器：TCP server + 顺序派发 + worker 监督 + 对账回收。"""

    def __init__(
        self,
        *,
        get_db: Callable[[], Any],
        task_repo: object,
        record_event: Callable[..., None],
        claim_task_lease: Callable[[Session, Any, str], int | None],
        load_runtime_control: Callable[[Session], dict],
        build_should_recover: Callable[[Session], Callable] | None = None,
        clear_task_lock: Callable | None = None,
        cleanup_resume: Callable | None = None,
        bind: str = SCHED_BIND,
        port: int = SCHED_PORT,
    ) -> None:
        self._get_db = get_db
        self._task_repo = task_repo
        self._record_event = record_event
        self._claim_task_lease = claim_task_lease
        self._load_control = load_runtime_control
        self._build_should_recover = build_should_recover
        self._clear_lock = clear_task_lock or (lambda *a, **kw: None)
        self._cleanup_resume = cleanup_resume or (lambda *a, **kw: None)
        self._bind = bind
        self._port = port

        self._workers: dict[str, WorkerConn] = {}
        self._lock = threading.Lock()              # 保护 _workers 表

        self._running = False
        self._stop = threading.Event()
        self._server_sock: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None
        self._dispatch_thread: threading.Thread | None = None
        self._recovery_thread: threading.Thread | None = None

        self._last_tick = 0.0
        self._last_recover = 0.0
        self._last_error: str | None = None
        # claim_enabled/drain/pause 运行时总闸
        self._claim_enabled = True
        self._drain_mode = False
        self._pause_until = 0.0

    # ── 生命周期 ────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._stop.clear()
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self._bind, self._port))
        self._server_sock.listen(64)
        self._accept_thread = threading.Thread(target=self._accept_loop, name="sa_v3_accept", daemon=True)
        self._accept_thread.start()
        self._dispatch_thread = threading.Thread(target=self._dispatch_loop, name="sa_v3_dispatch", daemon=True)
        self._dispatch_thread.start()
        self._recovery_thread = threading.Thread(target=self._recovery_loop, name="sa_v3_recover", daemon=True)
        self._recovery_thread.start()
        logger.info("SchedulerV3 started: tcp=%s:%s poll=%ss hb_timeout=%ss", self._bind, self._port, DISPATCH_POLL_INTERVAL, WORKER_HEARTBEAT_TIMEOUT)

    def stop(self) -> None:
        self._running = False
        self._stop.set()
        s, self._server_sock = self._server_sock, None
        if s is not None:
            try:
                s.close()
            except OSError:
                pass
        with self._lock:
            for w in self._workers.values():
                self._close_worker(w)
            self._workers.clear()
        for t in (self._accept_thread, self._dispatch_thread, self._recovery_thread):
            if t:
                t.join(timeout=3)

    # ── TCP accept + worker 消息循环 ─────────────────────────────────────────

    def _accept_loop(self) -> None:
        while self._running and self._server_sock is not None:
            try:
                conn, addr = self._server_sock.accept()
            except OSError:
                if self._running:
                    logger.exception("accept failed")
                break
            t = threading.Thread(target=self._serve_worker, args=(conn, addr), name="sa_v3_worker", daemon=True)
            t.start()

    def _serve_worker(self, conn: socket.socket, addr) -> None:
        conn.settimeout(None)
        worker_id: str | None = None
        try:
            reader = conn.makefile("rb")
            while self._running:
                msg = proto.read_frame(reader)
                if msg is None:
                    break  # 连接关闭
                try:
                    handled_id = self._handle_worker_msg(conn, msg)
                    if handled_id:
                        worker_id = handled_id
                except Exception:
                    logger.exception("handle worker msg failed: %s", msg)
                    break
        except OSError:
            pass
        finally:
            if worker_id:
                self._on_worker_disconnect(worker_id)
            try:
                conn.close()
            except OSError:
                pass

    def _handle_worker_msg(self, conn: socket.socket, msg: dict) -> str | None:
        mtype = msg.get("type")
        if mtype == proto.MSG_HELLO:
            wid = str(msg.get("worker_id") or "").strip()
            if not wid:
                self._send(conn, proto.msg_error("hello missing worker_id"))
                return None
            w = self._register_worker(wid, conn, ip=str(msg.get("ip") or ""), max_tasks=int(msg.get("max_tasks") or 1))
            self._send(conn, proto.msg_ok(worker_id=wid))
            self._record_event(None, None, "worker_connected", f"worker {wid} 已连接", "info", {"worker_id": wid})
            return wid
        wid = str(msg.get("worker_id") or "").strip()
        if not wid:
            return None
        if mtype == proto.MSG_HEARTBEAT:
            self._on_heartbeat(wid, msg)
        elif mtype == proto.MSG_TASK_STATE:
            self._on_task_state(wid, msg)
        return wid

    def _register_worker(self, wid: str, conn: socket.socket, ip: str, max_tasks: int) -> WorkerConn:
        with self._lock:
            old = self._workers.get(wid)
            if old is not None and old.sock is not None and old.sock is not conn:
                # 同一 worker 重连：关闭旧连接
                self._close_worker(old)
            w = WorkerConn(worker_id=wid, ip=ip, max_tasks=max(1, max_tasks),
                           last_heartbeat=_time.time(), online=True, sock=conn)
            self._workers[wid] = w
            return w

    def _on_heartbeat(self, wid: str, msg: dict) -> None:
        with self._lock:
            w = self._workers.get(wid)
            if w is None:
                return
            w.last_heartbeat = _time.time()
            w.online = True
            w.current_task = msg.get("task_id") or None

    def _on_task_state(self, wid: str, msg: dict) -> None:
        """worker 上报任务状态变更：更新内存表 + 记事件。终态(DB)由 worker 侧执行 finalize 时写。"""
        task_id = str(msg.get("task_id") or "")
        state = str(msg.get("state") or "")
        with self._lock:
            w = self._workers.get(wid)
            if w is not None:
                if state in (proto.STATE_FINISHED, proto.STATE_FAILED, proto.STATE_CANCELLED):
                    if w.current_task == task_id:
                        w.current_task = None
        self._record_event(task_id, None, "worker_task_state",
                           f"worker {wid} 报告任务 {task_id} 状态={state}", "info",
                           {"worker_id": wid, "task_id": task_id, "state": state,
                            "error": msg.get("error"), "result": msg.get("result")})

    def _on_worker_disconnect(self, wid: str) -> None:
        with self._lock:
            w = self._workers.get(wid)
            if w is None:
                return
            w.online = False
            w.sock = None
            # 注意：不立即清 current_task —— 留给 recovery 用 DB lease 对账；
            # 若 worker 只是短暂断联并重连，current_task 会被 hello/heartbeat 更正。
        self._record_event(None, None, "worker_disconnected", f"worker {wid} 断联", "warning", {"worker_id": wid})

    def _close_worker(self, w: WorkerConn) -> None:
        w.online = False
        s, w.sock = w.sock, None
        if s is not None:
            try:
                s.close()
            except OSError:
                pass

    def _send(self, conn: socket.socket, msg: dict) -> bool:
        try:
            data = proto.encode(msg)
            with _lock_for_conn(conn):
                conn.sendall(data)
            return True
        except OSError:
            return False

    # ── 派发主循环（顺序 push，非抢占）──────────────────────────────────────

    def _dispatch_loop(self) -> None:
        while self._running and not self._stop.wait(timeout=DISPATCH_POLL_INTERVAL):
            try:
                self._dispatch_once()
                self._last_tick = _time.time()
            except Exception as exc:
                self._last_error = str(exc)
                logger.exception("dispatch loop: %s", exc)

    def _dispatch_once(self) -> None:
        db_gen = self._get_db()
        db: Session = next(db_gen)
        try:
            # 运行时控制总闸
            self._apply_control(self._load_control(db))
            if not self._claim_enabled or self._drain_mode or self._pause_until > _time.time():
                return
            # 选一个空闲 worker
            wid = self._pick_idle_worker()
            if not wid:
                return
            # 取一个 pending 任务（FIFO）
            rows = self._task_repo.list_pending_tasks(db, 1)
            if not rows:
                return
            row = rows[0]
            lease_epoch = self._claim_task_lease(db, row, wid)
            if not lease_epoch:
                return  # 被别人抢了（多 manager 安全）/状态已变
            # 标记该 worker 正在跑该任务（占用槽位）
            with self._lock:
                w = self._workers.get(wid)
                if w is not None:
                    w.current_task = row.task_id
                    w.last_heartbeat = _time.time()
            # 下发 RUN 命令
            conn = self._get_conn(wid)
            sent = self._send(conn, proto.msg_run(row.task_id, lease_epoch)) if conn is not None else False
            self._record_event(row.task_id, getattr(row, "project_id", None),
                               "task_dispatched", f"任务已下发 worker={wid}", "info",
                               {"worker_id": wid, "lease_epoch": lease_epoch})
            if not sent:
                # 下发失败（worker 刚断联）→ 释放占用，靠 recovery 把它回收重排
                with self._lock:
                    w = self._workers.get(wid)
                    if w is not None and w.current_task == row.task_id:
                        w.current_task = None
                logger.warning("RUN 下发失败 task=%s worker=%s，留给 recovery 回收", row.task_id, wid)
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _pick_idle_worker(self) -> str | None:
        with self._lock:
            idle = [w for w in self._workers.values() if w.is_idle]
        if not idle:
            return None
        # 选最久未派发（last_heartbeat 最早）的空闲 worker，简单轮询均衡
        idle.sort(key=lambda w: (w.last_heartbeat, w.worker_id))
        return idle[0].worker_id

    def _get_conn(self, wid: str) -> socket.socket | None:
        with self._lock:
            w = self._workers.get(wid)
            return w.sock if (w is not None and w.online) else None

    # ── 回收 / 对账循环（rollout 安全）──────────────────────────────────────

    def _recovery_loop(self) -> None:
        while self._running and not self._stop.wait(timeout=RECOVER_SWEEP_INTERVAL):
            try:
                self._recovery_once()
                self._last_recover = _time.time()
            except Exception as exc:
                logger.exception("recovery loop: %s", exc)

    def _recovery_once(self) -> None:
        """对账：把"已死 worker 仍占着的 running 任务"回收重排；清理内存中失联 worker。"""
        now = _time.time()
        # 1. 内存表：心跳超时的 worker 标 offline
        with self._lock:
            for w in self._workers.values():
                if w.online and now - w.last_heartbeat > WORKER_HEARTBEAT_TIMEOUT:
                    w.online = False
                    self._record_event(None, None, "worker_heartbeat_timeout",
                                       f"worker {w.worker_id} 心跳超时", "warning", {"worker_id": w.worker_id})
            # 移除长期失联且无任务的 worker 条目
            dead = [wid for wid, w in self._workers.items()
                    if not w.online and w.current_task is None and now - w.last_heartbeat > WORKER_HEARTBEAT_TIMEOUT * 4]
            for wid in dead:
                self._workers.pop(wid, None)
        # 2. DB 对账：running 任务的 owner 若失联超宽限 → 回收重排
        db_gen = self._get_db()
        db: Session = next(db_gen)
        try:
            should_recover = self._build_should_recover(db) if self._build_should_recover else None

            def _stale_owner_recover(stale_row) -> bool:
                owner = str(getattr(stale_row, "dispatcher_instance_id", "") or "")
                # owner worker 仍在线且有该任务在跑 → 不回收
                with self._lock:
                    w = self._workers.get(owner)
                    if w is not None and w.online and w.current_task == stale_row.task_id:
                        return False
                # owner 离线，但宽限期内 → 不回收（给重连机会）
                lease_exp = getattr(stale_row, "lease_expires_at", None)
                if lease_exp is not None and lease_exp > now_local():
                    return False
                return True

            rows = self._task_repo.recover_stale_running_tasks(
                db, now=now_local(), lease_timeout_seconds=int(WORKER_RECOVER_GRACE),
                clear_task_execution_lock=self._clear_lock,
                cleanup_resume_files=self._cleanup_resume,
                should_recover=should_recover if should_recover is not None else _stale_owner_recover,
            )
            for row in rows:
                self._record_event(row.task_id, getattr(row, "project_id", None),
                                   "task_lease_recovered", "任务租约过期/worker 失联，已回收重排",
                                   "warning", {"lease_epoch": getattr(row, "lease_epoch", 0)})
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    # ── 控制命令（API → 调度器 → worker）─────────────────────────────────────

    def cancel_task(self, task_id: str, owner_worker_id: str) -> dict:
        """API cancel：向 owner worker 发 CANCEL 命令（worker 杀任务进程+代归档+清理pod）。"""
        conn = self._get_conn(owner_worker_id)
        if conn is None:
            return {"status": "worker_offline", "task_id": task_id, "worker_id": owner_worker_id}
        ok = self._send(conn, proto.msg_cancel(task_id))
        self._record_event(task_id, None, "task_cancel_commanded", f"已向 worker {owner_worker_id} 发送取消命令", "info",
                           {"worker_id": owner_worker_id})
        return {"status": "ok" if ok else "send_failed", "task_id": task_id, "worker_id": owner_worker_id}

    def restart_task(self, task_id: str, owner_worker_id: str, lease_epoch: int) -> dict:
        conn = self._get_conn(owner_worker_id)
        if conn is None:
            return {"status": "worker_offline"}
        ok = self._send(conn, proto.msg_restart(task_id, lease_epoch))
        return {"status": "ok" if ok else "send_failed"}

    # ── 辅助 ────────────────────────────────────────────────────────────────

    def _apply_control(self, payload: dict) -> None:
        p = payload or {}
        self._claim_enabled = bool(p.get("claim_enabled", True))
        self._drain_mode = bool(p.get("drain_mode", False))
        try:
            self._pause_until = max(0.0, float(p.get("pause_claim_until_ts", 0) or 0))
        except (TypeError, ValueError):
            self._pause_until = 0.0

    def health(self) -> dict:
        with self._lock:
            workers = [
                {"worker_id": w.worker_id, "online": w.online, "task": w.current_task,
                 "hb_age": round(_time.time() - w.last_heartbeat, 1)}
                for w in self._workers.values()
            ]
            idle = sum(1 for w in self._workers.values() if w.is_idle)
        return {
            "status": "ok" if self._running else "stopped",
            "version": "v3",
            "tcp": f"{self._bind}:{self._port}",
            "workers": workers,
            "idle_workers": idle,
            "last_tick": self._last_tick,
            "last_recover": self._last_recover,
            "last_error": self._last_error,
            "control": {"claim_enabled": self._claim_enabled, "drain_mode": self._drain_mode, "pause_until": self._pause_until},
        }


# 一个 per-socket 的写锁注册表（避免多线程向同一 socket 并发 sendall 帧交错）
_CONN_LOCKS: "dict[int, threading.Lock]" = {}
_CONN_LOCKS_GUARD = threading.Lock()


def _lock_for_conn(conn: socket.socket) -> threading.Lock:
    key = id(conn)
    with _CONN_LOCKS_GUARD:
        lk = _CONN_LOCKS.get(key)
        if lk is None:
            lk = threading.Lock()
            _CONN_LOCKS[key] = lk
        return lk
