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
# worker 失联(非任务失败)时重排上限：rollout/pod 重启/网络抖动 → 重派重跑；
# 超过上限才判 failed，避免 worker 反复死亡时无限重排。
MAX_WORKER_LOST_REQUEUE = int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_MAX_REQUEUE", "3"))
STATE_FILE = os.environ.get("SECFLOW_SYSTEM_ANALYSE_SCHED_STATE_FILE", "/data/sa_scheduler_state.json")
PERSIST_INTERVAL = float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_SCHED_PERSIST", "3"))
# DB 兜底对账：周期扫描 DB status=running 但调度器内存(_running/_queue)无的"孤儿"任务
# （上一任 manager 派发、rollout/重启丢失内存态）。有存活 worker 上报则收编，否则按 restart 重排。
DB_RECONCILE_INTERVAL = float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_SCHED_DB_RECONCILE", "30"))
ORPHAN_GRACE_SECONDS = float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_ORPHAN_GRACE", "60"))
PENDING_SUBMIT_GRACE_SECONDS = max(
    0.0,
    float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_PENDING_SUBMIT_GRACE_SECONDS", "20")),
)


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
    current_task: str | None = None          # 调度器派发权威：派发时设，task_state 终态时清
    reported_task: str | None = None         # worker 心跳上报的实际在跑任务（用于重连对账）
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
        # 分发时一次性 DB 认领（设 status=running/dispatcher/lease_epoch），
        # 桥接 execute_task 的预期 + 让前端 DB 状态实时；非实时心跳/回收依赖。
        # 返回认领到的 lease_epoch（int）或 None（失败）。调度器把该 epoch 放入 RUN 命令。
        claim_task: Callable[[str, str], "int | None"] | None = None,
        # worker 失联时重置任务 DB 为 pending（以便重新认领重派）。返回是否成功。
        requeue_task: Callable[[str], bool] | None = None,
        # DB 兜底对账：返回所有 status=running 的任务 [{task_id, dispatcher_instance_id, dispatch_age_s}]。
        db_running_tasks: Callable[[], list] | None = None,
        bind: str = SCHED_BIND,
        port: int = SCHED_PORT,
        state_file: str = STATE_FILE,
    ) -> None:
        self._finalize = finalize_task or (lambda tid, state, err, result: None)
        self._record = record_event or (lambda *a, **kw: None)
        self._claim_task = claim_task or (lambda tid, wid: 1)
        self._requeue_task = requeue_task or (lambda tid: True)
        self._db_running_tasks = db_running_tasks or (lambda: [])
        self._bind = bind
        self._port = port
        self._state_file = Path(state_file)

        self._queue: deque[str] = deque()
        self._running: dict[str, RunRecord] = {}
        self._workers: dict[str, WorkerConn] = {}
        self._requeue_counts: dict[str, int] = {}
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
                          ("reconcile", self._reconcile_loop), ("db_reconcile", self._db_reconcile_loop),
                          ("persist", self._persist_loop)):
            t = threading.Thread(target=tgt, name=f"sa_v3_{name}", daemon=True)
            t.start()
            self._threads.append(t)
        self._load_pending_tasks_from_db(reason="startup_pending_recovery")
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
            w = WorkerConn(worker_id=wid, ip=ip, max_tasks=max(1, max_tasks),
                           last_heartbeat=_time.time(), online=True, sock=conn)
            # 重连对账：若 _running 中有归属该 worker 的任务（manager 重启后从文件加载），
            # 恢复其 current_task，避免被误判空闲而重复派发。
            for tid, rec in self._running.items():
                if rec.worker_id == wid:
                    w.current_task = tid
                    break
            self._workers[wid] = w
            self._dirty = True

    def _on_heartbeat(self, wid: str, msg: dict) -> None:
        with self._lock:
            w = self._workers.get(wid)
            if w is None:
                return
            w.last_heartbeat = _time.time()
            w.online = True
            # 心跳只更新存活 + worker 上报的实际任务；不覆盖调度器派发权威的 current_task
            w.reported_task = msg.get("task_id") or None

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
            if state in (proto.STATE_FINISHED, proto.STATE_FAILED, proto.STATE_CANCELLED):
                # 任务到达终态 → 清除重排计数
                self._requeue_counts.pop(task_id, None)
            self._dirty = True
        self._record_event_for(task_id, "worker_task_state",
                               f"worker {wid} 任务 {task_id} 状态={state}", "info",
                               {"worker_id": wid, "task_id": task_id, "state": state, "error": err})
        if finished:
            # 瞬时派发拒绝(worker_busy/spawn_failed/no_spawner) → 重排重派(非真失败)，带重试上限。
            # rollout/worker 重连期调度器视图与 worker 实际态短暂不一致会导致派给忙 worker。
            transient = (state == proto.STATE_FAILED and isinstance(err, str)
                         and any(k in err for k in ("worker_busy", "spawn_failed", "no_spawner")))
            requeued = False
            if transient:
                cnt = self._requeue_counts.get(task_id, 0) + 1
                if cnt <= MAX_WORKER_LOST_REQUEUE:
                    self._requeue_counts[task_id] = cnt
                    ok = False
                    try:
                        ok = bool(self._requeue_task(task_id))
                    except Exception:
                        logger.exception("requeue(transient) failed: %s", task_id)
                    if ok:
                        with self._lock:
                            self._running.pop(task_id, None)
                            if task_id not in self._queue:
                                self._queue.append(task_id)
                            self._dirty = True
                        self._record_event_for(task_id, "task_requeued",
                                               f"worker 瞬时拒绝({err})，任务重排", "warning", {"task_id": task_id})
                        requeued = True
            if not requeued:
                try:
                    self._finalize(task_id, state, err, result)
                except Exception:
                    logger.exception("finalize task failed: %s", task_id)
                # 任务终态（非取消）→ 查 DB 实际状态，失败/错误才给 debugger 下发
                if state in (proto.STATE_FINISHED, proto.STATE_FAILED):
                    threading.Thread(
                        target=self._dispatch_failure_debug,
                        args=(task_id,),
                        name=f"sa_debug_dispatch_{task_id}",
                        daemon=True,
                    ).start()
                with self._lock:
                    self._running.pop(task_id, None)
                    self._requeue_counts.pop(task_id, None)
                    self._dirty = True

    # ── 失败调试下发 ──────────────────────────────────────────────────────
    _DEBUGGER_HOST = os.environ.get("SA_DEBUGGER_HOST", "secflow-app-system-analyse-debugger")
    _DEBUGGER_PORT = int(os.environ.get("SA_DEBUGGER_PORT", "8080"))

    def _dispatch_failure_debug(self, task_id: str) -> None:
        """任务终态后，查 DB 实际状态；仅 failed/error 才通知 debugger 调试。

        worker 对子进程 rc=0 的失败（orchestrator 内部捕获）报 STATE_FINISHED，
        故不能只凭协议 state 判断，须以 DB 终态为准。debugger 不主动轮询任务表。
        """
        import urllib.request
        import urllib.error
        # 先查 DB：只对真实失败/错误下发
        try:
            from app.db import _SessionLocal
            from app.db.models import AppSaTask
            if _SessionLocal is not None:
                db = _SessionLocal()
                try:
                    t = db.query(AppSaTask).filter(AppSaTask.task_id == task_id).first()
                    if t is None or t.status not in ("failed", "error"):
                        return  # passed/cancelled/running → 不调试
                finally:
                    db.close()
        except Exception:
            logger.exception("failure-debug DB status check failed for %s", task_id)
            return
        url = f"http://{self._DEBUGGER_HOST}:{self._DEBUGGER_PORT}/api/app/system-analyse/internal/failure-debug"
        payload = json.dumps({"task_id": task_id}).encode()
        for attempt in range(1, 4):
            try:
                req = urllib.request.Request(
                    url, data=payload, method="POST",
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    if 200 <= resp.status < 300:
                        logger.info("dispatched failure-debug for task %s (attempt %d)", task_id, attempt)
                        return
            except Exception as exc:
                logger.warning("failure-debug dispatch attempt %d for %s failed: %s", attempt, task_id, exc)
            _time.sleep(5)
        logger.error("failure-debug dispatch exhausted for task %s (debugger unreachable)", task_id)

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
        sent = False
        lease_epoch = self._claim_task(task_id, w.worker_id)
        if not lease_epoch:
            # 认领失败（状态已变/并发）→ 不下发
            with self._lock:
                self._running.pop(task_id, None)
                ww = self._workers.get(w.worker_id)
                if ww is not None and ww.current_task == task_id:
                    ww.current_task = None
                self._dirty = True
            return
        sent = self._send(conn, proto.msg_run(task_id, lease_epoch=lease_epoch)) if conn is not None else False
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
        lost: list[tuple[str, str]] = []
        with self._lock:
            for w in self._workers.values():
                if w.online and now - w.last_heartbeat > WORKER_HEARTBEAT_TIMEOUT:
                    w.online = False
                    self._record("worker_heartbeat_timeout", f"worker {w.worker_id} 心跳超时", "warning", {"worker_id": w.worker_id})
            for tid, rec in list(self._running.items()):
                # 派发→运行宽限期：刚派发的任务给 worker 拉起/上报的时间，不误判
                if now - rec.started_ts < WORKER_HEARTBEAT_TIMEOUT:
                    continue
                w = self._workers.get(rec.worker_id)
                if w is None or not w.online:
                    owner_alive = False
                else:
                    # worker 在线：以其心跳上报的 reported_task 为准判断是否真在跑该任务
                    owner_alive = (w.reported_task == tid)
                if not owner_alive:
                    rec.error = "worker_lost"
                    lost.append((tid, rec.worker_id))
            to_requeue: list[str] = []
            to_fail: list[str] = []
            for tid, wid in lost:
                self._running.pop(tid, None)
                ww = self._workers.get(wid)
                if ww is not None and ww.current_task == tid:
                    ww.current_task = None
                cnt = self._requeue_counts.get(tid, 0) + 1
                self._requeue_counts[tid] = cnt
                if cnt <= MAX_WORKER_LOST_REQUEUE:
                    to_requeue.append(tid)
                else:
                    to_fail.append(tid)
            dead = [wid for wid, w in self._workers.items()
                    if not w.online and w.current_task is None and now - w.last_heartbeat > WORKER_HEARTBEAT_TIMEOUT * 4]
            for wid in dead:
                self._workers.pop(wid, None)
            if lost or dead:
                self._dirty = True
        # 锁外：worker 失联(非任务失败) → 重置 DB 为 pending + 重排重跑；超重试上限才 failed。
        for tid in to_requeue:
            ok = False
            try:
                ok = bool(self._requeue_task(tid))
            except Exception:
                logger.exception("requeue_task(reset pending) failed: %s", tid)
            if ok:
                with self._lock:
                    if tid not in self._queue and tid not in self._running:
                        self._queue.append(tid)
                    self._dirty = True
                self._record_event_for(tid, "task_requeued",
                                       f"worker 失联，任务重排重跑(第{self._requeue_counts.get(tid, 0)}次)",
                                       "warning", {"task_id": tid})
            else:
                to_fail.append(tid)
        for tid in to_fail:
            self._requeue_counts.pop(tid, None)
            self._record_event_for(tid, "task_failed", "worker 多次失联，任务失败", "error", {"task_id": tid})
            try:
                self._finalize(tid, proto.STATE_FAILED, "worker_lost", None)
            except Exception:
                logger.exception("finalize(worker_lost) failed: %s", tid)

    # ── DB 兜底对账（孤儿 running 回收/收编；V3 实时 reconcile 只看内存，此处补 DB）──

    def _db_reconcile_loop(self) -> None:
        # 启动后先等一个心跳超时周期，让幸存 worker 重连并上报 reported_task，
        # 避免把"内存暂无记录但 worker 仍在跑"的任务误判为孤儿。
        if self._stop.wait(timeout=WORKER_HEARTBEAT_TIMEOUT):
            return
        while self._running_flag and not self._stop.wait(timeout=DB_RECONCILE_INTERVAL):
            try:
                self._db_reconcile_once()
            except Exception as exc:
                logger.exception("db_reconcile loop: %s", exc)

    def _db_reconcile_once(self) -> None:
        """对账 DB：status=running 但不在 _running/_queue 的任务。
        有存活 worker 上报(reported_task) → 收编(adopt)进内存正常跟踪；
        否则(owner 死/失联且派发已久) → 孤儿，按 restart 语义重排(带重试上限)。"""
        try:
            rows = self._db_running_tasks() or []
        except Exception:
            logger.exception("db_reconcile: query db_running_tasks failed")
            return
        now = _time.time()
        adopt: list[tuple[str, str]] = []
        orphan: list[str] = []
        with self._lock:
            tracked = set(self._running.keys()) | set(self._queue)
            reported = {w.reported_task: w.worker_id
                        for w in self._workers.values() if w.online and w.reported_task}
            for row in rows:
                tid = str(row.get("task_id") or "")
                if not tid or tid in tracked:
                    continue
                wid = reported.get(tid)
                if wid:
                    adopt.append((tid, wid))
                else:
                    age = row.get("dispatch_age_s")
                    if age is None or age >= ORPHAN_GRACE_SECONDS:
                        orphan.append(tid)
            for tid, wid in adopt:
                self._running[tid] = RunRecord(task_id=tid, worker_id=wid, started_ts=now)
                w = self._workers.get(wid)
                if w is not None and w.current_task is None:
                    w.current_task = tid
            if adopt:
                self._dirty = True
        for tid, wid in adopt:
            self._record_event_for(tid, "task_adopted",
                                   f"DB 对账：收编存活 worker {wid} 上的运行任务（内存态缺失）",
                                   "info", {"task_id": tid, "worker_id": wid})
        # 锁外：孤儿 → 按 restart 语义重排（_requeue_task 已清空任务目录），带重试上限。
        for tid in orphan:
            cnt = self._requeue_counts.get(tid, 0) + 1
            self._requeue_counts[tid] = cnt
            if cnt <= MAX_WORKER_LOST_REQUEUE:
                ok = False
                try:
                    ok = bool(self._requeue_task(tid))
                except Exception:
                    logger.exception("db_reconcile requeue failed: %s", tid)
                if ok:
                    with self._lock:
                        if tid not in self._queue and tid not in self._running:
                            self._queue.append(tid)
                        self._dirty = True
                    self._record_event_for(tid, "task_requeued",
                                           f"DB 对账：孤儿运行任务(无内存态/无存活 worker)按 restart 重排(第{cnt}次)",
                                           "warning", {"task_id": tid})
            else:
                self._requeue_counts.pop(tid, None)
                self._record_event_for(tid, "task_failed",
                                       "DB 对账：孤儿任务多次重排仍失败", "error", {"task_id": tid})
                try:
                    self._finalize(tid, proto.STATE_FAILED, "orphan_requeue_exhausted", None)
                except Exception:
                    logger.exception("db_reconcile finalize failed: %s", tid)
        self._repair_pending_unqueued_tasks()

    def _pending_tasks_missing_from_scheduler(self) -> list[dict]:
        from datetime import timedelta

        from app.db import get_db
        from app.service.task_repository import TaskRepository
        from app.time_utils import now_local

        db_gen = get_db()
        db = next(db_gen)
        try:
            cutoff = now_local() - timedelta(seconds=PENDING_SUBMIT_GRACE_SECONDS)
            rows = TaskRepository.list_pending_tasks_for_scheduler_repair(
                db,
                created_before=cutoff,
                limit=500,
            )
            with self._lock:
                tracked = set(self._queue) | set(self._running)
            return [
                {
                    "task_id": row.task_id,
                    "project_id": row.project_id,
                    "status": row.status,
                }
                for row in rows
                if row.task_id not in tracked
            ]
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _enqueue_pending_repair_rows(self, rows: list[dict], *, reason: str) -> None:
        if not rows:
            return
        enqueued: list[dict] = []
        with self._lock:
            tracked = set(self._queue) | set(self._running)
            for row in rows:
                task_id = str(row.get("task_id") or "").strip()
                if not task_id or task_id in tracked:
                    continue
                self._queue.append(task_id)
                tracked.add(task_id)
                enqueued.append(row)
            if enqueued:
                self._dirty = True
        for row in enqueued:
            task_id = str(row.get("task_id") or "").strip()
            self._record_event_for(
                task_id,
                "task_requeued",
                "DB 对账：检测到 pending 任务脱离调度队列，已重新入队",
                "warning",
                {
                    "task_id": task_id,
                    "project_id": str(row.get("project_id") or "").strip() or None,
                    "task_status": row.get("status") or "pending",
                    "reason": "pending_task_missing_from_scheduler_queue",
                    "repair_source": reason,
                },
            )

    def _load_pending_tasks_from_db(self, *, reason: str) -> None:
        try:
            repaired = self._pending_tasks_missing_from_scheduler()
        except Exception:
            logger.exception("load pending tasks from db failed")
            return
        self._enqueue_pending_repair_rows(repaired, reason=reason)

    def _repair_pending_unqueued_tasks(self) -> None:
        try:
            repaired = self._pending_tasks_missing_from_scheduler()
        except Exception:
            logger.exception("repair pending unqueued tasks failed")
            return
        self._enqueue_pending_repair_rows(repaired, reason="scheduler_db_reconcile")

    # ── 内部 API（HTTP → 内存控制面；无 DB）───────────────────────────────────

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
        # 先重置 DB 为 pending + 清空任务目录（restart 语义：从头重跑）。
        # 否则 _claim_task 要求 status=pending 会拒领，任务卡在队列不派发。
        requeued_ok = False
        try:
            requeued_ok = bool(self._requeue_task(task_id))
        except Exception:
            logger.exception("restart: requeue_task failed: %s", task_id)
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
        self._record_event_for(task_id, "task_restart_queued", "任务重新入队", "info",
                               {"task_id": task_id, "db_requeued": requeued_ok})
        return {"status": "requeued", "task_id": task_id, "queue_len": len(self._queue),
                "db_requeued": requeued_ok}

    def task_status(self, task_id: str) -> dict:
        with self._lock:
            if task_id in self._queue:
                return {
                    "task_id": task_id,
                    "status": "pending",
                    "queue_pos": list(self._queue).index(task_id),
                    "scheduler_state": "pending_in_queue",
                }
            rec = self._running.get(task_id)
            if rec is not None:
                return {"task_id": task_id, "status": rec.state, "worker_id": rec.worker_id,
                        "started_ts": rec.started_ts, "error": rec.error, "scheduler_state": "running_in_memory"}
        # 内存中未找到（已完成/已 cancelled/未知）→ DB 回退，保障前端显示
        return self._db_task_status(task_id)

    def _db_task_status(self, task_id: str) -> dict:
        try:
            from app.db import get_db
            from app.db.models import AppSaTask
            from app.time_utils import now_local as _now
            db_gen = get_db()
            db = next(db_gen)
            try:
                row = db.query(AppSaTask).filter_by(task_id=task_id).first()
                if row is None:
                    return {"task_id": task_id, "status": "unknown"}
                d = {"task_id": task_id, "status": row.status,
                     "started_at": row.started_at.isoformat() if row.started_at else None,
                     "finished_at": row.finished_at.isoformat() if row.finished_at else None}
                if str(row.status or "") == "pending" and not row.dispatcher_instance_id and not row.dispatch_started_at:
                    d["scheduler_state"] = "missing_from_queue"
                if row.error: d["error"] = row.error
                if row.result_json and isinstance(row.result_json, dict):
                    d["module_count"] = row.result_json.get("module_count")
                return d
            finally:
                try: next(db_gen)
                except StopIteration: pass
        except Exception as exc:
            logger.warning("db_task_status fallback failed: %s", exc)
            return {"task_id": task_id, "status": "unknown"}

    def cluster_status(self) -> dict:
        pending_unqueued_count = 0
        try:
            pending_unqueued_count = len(self._pending_tasks_missing_from_scheduler())
        except Exception:
            logger.exception("cluster_status pending_unqueued_count failed")
        with self._lock:
            workers = [{"worker_id": w.worker_id, "online": w.online, "task": w.current_task,
                        "hb_age": round(_time.time() - w.last_heartbeat, 1)} for w in self._workers.values()]
            return {"queue_len": len(self._queue), "running": len(self._running),
                    "idle_workers": sum(1 for w in self._workers.values() if w.is_idle),
                    "workers": workers,
                    "running_tasks": [r.to_dict() for r in self._running.values()],
                    "pending_unqueued_count": pending_unqueued_count}

    def health(self) -> dict:
        pending_unqueued_count = 0
        try:
            pending_unqueued_count = len(self._pending_tasks_missing_from_scheduler())
        except Exception:
            logger.exception("health pending_unqueued_count failed")
        with self._lock:
            return {"status": "ok" if self._running_flag else "stopped", "version": "v3-pure-tcp",
                    "tcp": f"{self._bind}:{self._port}",
                    "queue": len(self._queue), "running": len(self._running), "workers": len(self._workers),
                    "pending_unqueued_count": pending_unqueued_count}

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
    from fastapi import APIRouter, Query

    router = APIRouter(prefix="/api/internal/sched")

    @router.post("/submit")
    def submit(task_id: str = Query(...)):
        s = get_scheduler()
        return s.submit(task_id) if s else {"status": "no_scheduler"}

    @router.post("/cancel")
    def cancel(task_id: str = Query(...)):
        s = get_scheduler()
        return s.cancel(task_id) if s else {"status": "no_scheduler"}

    @router.post("/restart")
    def restart(task_id: str = Query(...)):
        s = get_scheduler()
        return s.restart(task_id) if s else {"status": "no_scheduler"}

    @router.get("/task_status")
    def task_status(task_id: str = Query(...)):
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
