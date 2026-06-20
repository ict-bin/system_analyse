"""worker_control.py — V3.0 Worker 控制进程（runner pod 上的常驻主进程）。

设计见 docs/scheduler_v3_design.md。它是 runner 的"控制主进程"，不是任务进程：
  - 作为 TCP client 连接调度器 (req 3)；TCP 断联说明自己/调度器出问题，控制进程保持存活并重连。
  - 收 RUN 命令 → spawn 任务子进程 (TaskRunner/Orchestrator)；同一时刻仅 1 个任务 (req 7)。
  - 周期上报 task 心跳/状态 (req 3)。
  - 收 CANCEL → 杀任务子进程 + 清理 pod 进程(白名单:探针+控制主进程) + 代归档 (req 5)。
  - 任务前后清理 pod 进程 (req 6)。
  - 任务子进程异常退出(被杀)时由控制进程代为归档产物 (req 2)。

任务子进程：独立 python 进程执行 TaskRunner.execute_task；通过约定路径写 result/flag，
控制进程读其退出码 + 输出目录判断成功/失败并代归档。
"""
from __future__ import annotations

import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import time as _time
from dataclasses import dataclass
from typing import Any, Callable

from app.service import sched_proto as proto
from app.service.scheduler import cleanup_task_processes, _build_protected_set

logger = logging.getLogger("sa.worker_control")

SCHEDULER_HOST = os.environ.get("SECFLOW_SYSTEM_ANALYSE_SCHED_HOST", "secflow-app-system-analyse-worker")
SCHEDULER_PORT = int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_SCHED_PORT", "8090"))
RECONNECT_INTERVAL = float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_WC_RECONNECT", "5"))
HEARTBEAT_INTERVAL = float(os.environ.get("SECFLOW_SYSTEM_ANALYSE_WC_HEARTBEAT", "10"))
TASK_BIN = os.environ.get("SECFLOW_SYSTEM_ANALYSE_TASK_PYTHON", sys.executable)  # 跑任务子进程的解释器
WORKER_ID = os.environ.get("POD_NAME") or os.environ.get("HOSTNAME") or f"sa-runner-{os.getpid()}"
WORKER_IP = os.environ.get("SA_POD_IP") or os.environ.get("POD_IP") or ""


@dataclass
class RunningTask:
    task_id: str
    lease_epoch: int
    proc: subprocess.Popen
    output_path: str | None = None


class WorkerControl:
    """Worker 控制进程核心。常驻，由 server.py 在 runner 角色下拉起。"""

    def __init__(
        self,
        *,
        spawn_task_subprocess: Callable[[str, int], subprocess.Popen] | None = None,
        archive_task: Callable[[str, bool], None] | None = None,
        scheduler_host: str = SCHEDULER_HOST,
        scheduler_port: int = SCHEDULER_PORT,
        worker_id: str = WORKER_ID,
        worker_ip: str = WORKER_IP,
    ) -> None:
        # 默认 spawn 实现由 wiring 注入（避免本模块直接依赖 TaskRunner/DB）
        self._spawn = spawn_task_subprocess
        self._archive = archive_task or (lambda task_id, normal: None)
        self._host = scheduler_host
        self._port = scheduler_port
        self._worker_id = worker_id
        self._worker_ip = worker_ip

        self._sock: socket.socket | None = None
        self._reader = None
        self._write_lock = threading.Lock()
        self._running = False
        self._stop = threading.Event()
        self._current: RunningTask | None = None
        self._task_lock = threading.Lock()   # 保护 _current

        self._net_thread: threading.Thread | None = None      # 收命令
        self._hb_thread: threading.Thread | None = None       # 上报心跳
        self._reaper_thread: threading.Thread | None = None   # 回收任务子进程

    # ── 生命周期 ────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._stop.clear()
        self._net_thread = threading.Thread(target=self._net_loop, name="sa_wc_net", daemon=True)
        self._net_thread.start()
        self._hb_thread = threading.Thread(target=self._heartbeat_loop, name="sa_wc_hb", daemon=True)
        self._hb_thread.start()
        self._reaper_thread = threading.Thread(target=self._reaper_loop, name="sa_wc_reaper", daemon=True)
        self._reaper_thread.start()
        logger.info("WorkerControl started: worker_id=%s scheduler=%s:%s", self._worker_id, self._host, self._port)

    def stop(self) -> None:
        self._running = False
        self._stop.set()
        self._kill_current_task(reason="worker_control_stop")
        self._close_sock()
        for t in (self._net_thread, self._hb_thread, self._reaper_thread):
            if t:
                t.join(timeout=3)

    # ── 网络循环（连调度器 + 收命令 + 断联重连）────────────────────────────

    def _net_loop(self) -> None:
        while self._running and not self._stop.is_set():
            connected = self._connect()
            if not connected:
                self._stop.wait(timeout=RECONNECT_INTERVAL)
                continue
            try:
                assert self._reader is not None
                while self._running:
                    msg = proto.read_frame(self._reader)
                    if msg is None:
                        break
                    try:
                        self._handle_command(msg)
                    except Exception:
                        logger.exception("handle command failed: %s", msg)
            except OSError:
                pass
            finally:
                self._close_sock()
            if self._running:
                self._stop.wait(timeout=RECONNECT_INTERVAL)

    def _connect(self) -> bool:
        try:
            sock = socket.create_connection((self._host, self._port), timeout=10)
            sock.settimeout(None)
            self._sock = sock
            self._reader = sock.makefile("rb")
            # 发 HELLO
            self._send(proto.msg_hello(self._worker_id, ip=self._worker_ip, max_tasks=1))
            logger.info("connected to scheduler %s:%s (worker_id=%s)", self._host, self._port, self._worker_id)
            return True
        except OSError as exc:
            logger.warning("connect scheduler %s:%s failed: %s", self._host, self._port, exc)
            self._close_sock()
            return False

    def _handle_command(self, msg: dict) -> None:
        mtype = msg.get("type")
        if mtype == proto.MSG_RUN:
            self._on_run(str(msg.get("task_id") or ""), int(msg.get("lease_epoch") or 0))
        elif mtype == proto.MSG_CANCEL:
            self._on_cancel(str(msg.get("task_id") or ""))
        elif mtype == proto.MSG_RESTART:
            self._on_restart(str(msg.get("task_id") or ""), int(msg.get("lease_epoch") or 0))
        elif mtype == proto.MSG_OK:
            pass
        elif mtype == proto.MSG_ERROR:
            logger.warning("scheduler error: %s", msg.get("error"))

    # ── 任务生命周期（单任务约束 + spawn + 清理 + 归档）──────────────────────

    def _on_run(self, task_id: str, lease_epoch: int) -> None:
        with self._task_lock:
            if self._current is not None:
                # 单任务约束：已有任务在跑，拒绝新任务（理论上调度器不会给忙 worker 派活）
                self._send(proto.msg_task_state(self._worker_id, task_id, proto.STATE_FAILED, error="worker_busy"))
                return
            if self._spawn is None:
                self._send(proto.msg_task_state(self._worker_id, task_id, proto.STATE_FAILED, error="no_spawner_configured"))
                return
            # 任务前清理 pod 进程（白名单：探针 + 控制主进程）(req 6)
            self._cleanup_pod(f"pre_task:{task_id}")
            try:
                proc = self._spawn(task_id, lease_epoch)
            except Exception as exc:
                logger.exception("spawn task subprocess failed: %s", task_id)
                self._send(proto.msg_task_state(self._worker_id, task_id, proto.STATE_FAILED, error=f"spawn_failed: {exc}"))
                return
            self._current = RunningTask(task_id=task_id, lease_epoch=lease_epoch, proc=proc)
        self._send(proto.msg_task_state(self._worker_id, task_id, proto.STATE_STARTING))

    def _on_cancel(self, task_id: str) -> None:
        """req 5: 杀任务进程 + 清理 pod(白名单) + 代归档（任务进程已死所以控制进程代归档）。"""
        with self._task_lock:
            rt = self._current
            if rt is None or rt.task_id != task_id:
                self._send(proto.msg_task_state(self._worker_id, task_id, proto.STATE_CANCELLED, error="not_running_here"))
                return
            self._current = None
        killed_normal = False
        try:
            self._terminate_proc(rt.proc)
            # 任务被杀 → 控制进程代归档 (req 2)
            self._archive(task_id, normal=False)
        finally:
            self._cleanup_pod(f"cancel:{task_id}")
        self._send(proto.msg_task_state(self._worker_id, task_id, proto.STATE_CANCELLED))

    def _on_restart(self, task_id: str, lease_epoch: int) -> None:
        # 重启 = 杀旧 + 清理 + 重新 spawn（同一 task_id 新 lease_epoch）
        self._kill_current_task(reason=f"restart:{task_id}")
        self._on_run(task_id, lease_epoch)

    def _kill_current_task(self, reason: str) -> None:
        with self._task_lock:
            rt = self._current
            self._current = None
        if rt is None:
            return
        try:
            self._terminate_proc(rt.proc)
        except Exception:
            logger.exception("kill current task failed: %s", rt.task_id)
        finally:
            self._cleanup_pod(reason)

    # ── 任务子进程回收（检测结束 + 归档 + 状态上报 + 任务后清理）────────────

    def _reaper_loop(self) -> None:
        while self._running and not self._stop.wait(timeout=2.0):
            try:
                rt = self._peek_current()
                if rt is None:
                    continue
                rc = rt.proc.poll()
                if rc is None:
                    continue  # 仍在跑
                # 任务子进程已退出
                with self._task_lock:
                    if self._current is rt:
                        self._current = None
                normal = (rc == 0)
                # 正常退出 → 任务自己应已归档；非正常(被杀/崩溃) → 控制进程代归档 (req 2)
                try:
                    if not normal:
                        self._archive(rt.task_id, normal=False)
                except Exception:
                    logger.exception("archive(代) failed: %s", rt.task_id)
                # 任务后清理 pod 进程 (req 6)
                self._cleanup_pod(f"post_task:{rt.task_id}")
                state = proto.STATE_FINISHED if normal else proto.STATE_FAILED
                err = None if normal else f"task_subprocess_exit={rc}"
                self._send(proto.msg_task_state(self._worker_id, rt.task_id, state, error=err))
            except Exception:
                logger.exception("reaper loop failed")

    def _peek_current(self) -> RunningTask | None:
        with self._task_lock:
            return self._current

    # ── 心跳上报 (req 3) ────────────────────────────────────────────────────

    def _heartbeat_loop(self) -> None:
        while self._running and not self._stop.wait(timeout=HEARTBEAT_INTERVAL):
            with self._task_lock:
                tid = self._current.task_id if self._current else None
            self._send(proto.msg_heartbeat(self._worker_id, task_id=tid, state=proto.STATE_RUNNING if tid else None))

    # ── 进程清理 / 归档辅助 ──────────────────────────────────────────────────

    def _cleanup_pod(self, reason: str) -> None:
        """req 5/6: 清理 pod 内进程，白名单保留 探针 sidecar + 控制主进程。"""
        try:
            killed = cleanup_task_processes(self._worker_id, protected_pids=_build_protected_set())
            if killed:
                logger.info("pod cleanup (%s): killed %d procs", reason, killed)
        except Exception:
            logger.exception("pod cleanup failed: %s", reason)

    def _terminate_proc(self, proc: subprocess.Popen) -> None:
        # 先 SIGTERM 整个进程组，等 5s，再 SIGKILL
        try:
            pgid = os.getpgid(proc.pid)
            os.killpg(pgid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError, OSError):
            try:
                proc.terminate()
            except OSError:
                pass
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                pgid = os.getpgid(proc.pid)
                os.killpg(pgid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError, OSError):
                try:
                    proc.kill()
                except OSError:
                    pass
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass

    # ── 网络 IO ─────────────────────────────────────────────────────────────

    def _send(self, msg: dict) -> bool:
        sock = self._sock
        if sock is None:
            return False
        try:
            data = proto.encode(msg)
            with self._write_lock:
                sock.sendall(data)
            return True
        except OSError:
            self._close_sock()
            return False

    def _close_sock(self) -> None:
        sock, self._sock = self._sock, None
        reader, self._reader = self._reader, None
        if reader is not None:
            try:
                reader.close()
            except OSError:
                pass
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    # ── 自检（调度器侧可查询当前任务）────────────────────────────────────────

    def status(self) -> dict:
        with self._task_lock:
            cur = self._current
        return {
            "worker_id": self._worker_id,
            "current_task": cur.task_id if cur else None,
            "connected": self._sock is not None,
            "scheduler": f"{self._host}:{self._port}",
        }
