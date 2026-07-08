"""系统分析 Celery 任务定义。

run_sa_task(task_id): Celery worker prefork 子进程执行单个系统分析任务。
  - os.setsid() 新进程组, 便于 revoke 时 killpg 杀 pi 全树
  - claim_specific_task 设 owner/epoch (防 acks_late 重投双跑)
  - _clean_task_artifacts 清旧产物 (restart 语义)
  - setup_local_workspace 建本地 workspace + NFS symlink
  - sync_loop 线程: 每 10s sync_for_frontend (前端实时日志)
  - 复用 TaskRunner.execute_task 跑 pipeline
  - commit_terminal_state_if_owner 提交终态
  - finalize_workspace 归档 NFS + 删本地
  - task_revoked 信号 → killpg 兜底
"""
from __future__ import annotations

import logging
import os
import signal
import threading
import time
import json

from celery import current_task
from celery.signals import task_revoked

from app.celery_app import app

logger = logging.getLogger("sa.celery_tasks")

WORKER_ID = str(os.environ.get("SA_POD_NAME") or os.environ.get("HOSTNAME") or "local").strip() or "local"

_PGID_LOCK = threading.Lock()
_PGID: dict[str, int] = {}

SYNC_INTERVAL = float(os.environ.get("SA_SYNC_INTERVAL", "10"))


@app.task(bind=True, name="app.celery_tasks.run_sa_task", acks_late=True)
def run_sa_task(self, task_id: str) -> dict:
    """执行一个系统分析任务 (Celery prefork 子进程)。"""
    celery_id = self.request.id
    try:
        os.setsid()
    except OSError:
        pass
    try:
        pgid = os.getpgid(0)
    except OSError:
        pgid = os.getpid()
    with _PGID_LOCK:
        _PGID[celery_id] = pgid
    logger.info("run_sa_task start task=%s celery_id=%s pgid=%s pod=%s", task_id, celery_id, pgid, WORKER_ID)

    from app.db import get_db
    from app.celery_app import _ensure_db
    _ensure_db()  # 确保 DB 初始化 (celery worker 进程不经 runtime_bootstrap)
    from app.service.execution_coordinator import (
        claim_specific_task,
        begin_execution_if_owner,
        commit_terminal_state_if_owner,
        still_owner,
        recover_running_task_for_cleanup,
        load_execution_snapshot,
        renew_lease,
        LEASE_TTL_SECONDS,
        HEARTBEAT_INTERVAL_SECONDS,
    )
    from app.time_utils import now_local

    db_gen = get_db()
    db = next(db_gen)
    claimed = None
    try:
        claimed = claim_specific_task(db, WORKER_ID, task_id)
    finally:
        try:
            next(db_gen)
        except StopIteration:
            pass

    if claimed is None:
        logger.info("run_sa_task skip (not claimable) task=%s", task_id)
        with _PGID_LOCK:
            _PGID.pop(celery_id, None)
        return {"task_id": task_id, "status": "skipped"}

    # restart 语义: 清空上一轮产物 (run/output), 保留 input + DB 事件时间线
    _clean_task_artifacts(task_id)

    # setup local workspace
    output_path = claimed.output_path
    ws_ok = False
    if output_path:
        try:
            from app.service.task_workspace import setup_local_workspace
            ws_result = setup_local_workspace(output_path, task_id)
            ws_ok = ws_result.get("ok", False)
        except Exception:
            logger.exception("setup_local_workspace failed for %s", task_id)

    # begin execution (pending→running)
    begin_ts = now_local()
    begin_execution_if_owner(db, task_id, WORKER_ID, claimed.epoch, claimed.control_version, started_at=begin_ts)

    # lease heartbeat thread
    cancel_stop = threading.Event()
    lease_stop = threading.Event()

    def _lease_heartbeat():
        while not cancel_stop.wait(timeout=HEARTBEAT_INTERVAL_SECONDS):
            if lease_stop.is_set():
                return
            try:
                hb_gen = get_db()
                hb_db = next(hb_gen)
                try:
                    ok = renew_lease(hb_db, task_id, WORKER_ID, claimed.epoch)
                    if not ok or not still_owner(hb_db, task_id, WORKER_ID, claimed.epoch, claimed.control_version):
                        logger.warning("lease lost for task=%s", task_id)
                        lease_stop.set()
                        cancel_stop.set()
                        return
                finally:
                    try:
                        next(hb_gen)
                    except StopIteration:
                        pass
            except Exception:
                logger.warning("lease heartbeat error for %s", task_id, exc_info=True)

    hb_thread = threading.Thread(target=_lease_heartbeat, name=f"sa_hb_{task_id[:12]}", daemon=True)
    hb_thread.start()

    # sync loop thread
    sync_stop = threading.Event()

    def _sync_loop():
        while not sync_stop.wait(timeout=SYNC_INTERVAL):
            if cancel_stop.is_set():
                return
            try:
                if output_path:
                    from app.service.task_workspace import sync_for_frontend
                    sync_for_frontend(output_path, task_id)
            except Exception:
                logger.debug("sync_loop error for %s", task_id, exc_info=True)

    sync_thread = threading.Thread(target=_sync_loop, name=f"sa_sync_{task_id[:12]}", daemon=True)
    sync_thread.start()

    # execute pipeline
    result_status = "error"
    result_error = None
    result_stages = None
    result_json = None
    try:
        from app.service.task_service import get_task_service
        svc = get_task_service()
        svc._runner.execute_task(task_id, claimed.epoch)
        # success
        result_status = "passed"
    except Exception as exc:
        result_status = "error"
        result_error = str(exc)[:2000]
        logger.exception("run_sa_task failed task=%s: %s", task_id, exc)
    finally:
        # stop sync + lease threads
        sync_stop.set()
        lease_stop.set()
        cancel_stop.set()
        sync_thread.join(timeout=15)
        hb_thread.join(timeout=5)

        # cleanup pi processes
        try:
            _cleanup_pi_processes()
        except Exception:
            logger.debug("pi cleanup failed", exc_info=True)

        # finalize workspace (NFS 归档)
        normal = (result_status == "passed")
        if output_path:
            try:
                from app.service.task_workspace import finalize_workspace
                finalize_workspace(output_path, task_id, normal=normal)
            except Exception:
                logger.exception("finalize_workspace failed for %s", task_id)

        # commit terminal state (CAS)
        try:
            db2_gen = get_db()
            db2 = next(db2_gen)
            try:
                finished_at = now_local()
                stages_json = result_stages or {"events": [], "final": True}
                committed = commit_terminal_state_if_owner(
                    db2, task_id, WORKER_ID, claimed.epoch, claimed.control_version,
                    status=result_status, finished_at=finished_at,
                    stages_json=stages_json, result_json=result_json, error=result_error,
                )
                if not committed:
                    # CAS failed (cancel/restart took over) → try recover
                    snap = load_execution_snapshot(db2, task_id)
                    if snap and snap.status == "running" and snap.execution_owner_id == WORKER_ID:
                        recover_running_task_for_cleanup(
                            db2, task_id, WORKER_ID, claimed.epoch, claimed.control_version,
                            reason="worker_finally_without_terminal_state",
                        )
            finally:
                try:
                    next(db2_gen)
                except StopIteration:
                    pass
        except Exception:
            logger.exception("commit/recover failed for %s", task_id)

        # ── 失败调试派发: 任务 error/failed → POST 给 debugger ──
        # 旧 scheduler_v3._dispatch_failure_debug 的等价实现。
        # debugger 不轮询任务表, 由这里主动通知。
        if result_status in ("error", "failed"):
            threading.Thread(
                target=_dispatch_failure_debug,
                args=(task_id,),
                name=f"sa_debug_dispatch_{task_id[:12]}",
                daemon=True,
            ).start()

        with _PGID_LOCK:
            _PGID.pop(celery_id, None)

    return {"task_id": task_id, "status": result_status}


def _cleanup_pi_processes() -> None:
    """任务结束后 best-effort 清理残留 pi/node 进程 (本进程组内)。"""
    try:
        import subprocess
        subprocess.run(["pkill", "-9", "-f", "pi-coding-agent"], capture_output=True, timeout=5)
    except Exception:
        pass
    try:
        import subprocess
        subprocess.run(["pkill", "-9", "-f", "node.*pi"], capture_output=True, timeout=5)
    except Exception:
        pass


def _dispatch_failure_debug(task_id: str) -> None:
    """任务终态后, 查 DB 实际状态; 仅 error/failed 才通知 debugger 调试。

    debugger 不主动轮询任务表, 由本函数在任务结束后触发。
    外部/基础设施错误 (源文件丢失/模型错误/key错误/超时) 不调试, 标记 skipped。
    """
    import urllib.request
    import urllib.error
    try:
        from app.db import get_db
        from app.db.models import AppSaTask
        db_gen = get_db()
        db = next(db_gen)
        try:
            t = db.query(AppSaTask).filter(AppSaTask.task_id == task_id).first()
            if t is None or t.status not in ("failed", "error"):
                return  # passed/cancelled/running → 不调试
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
    except Exception:
        logger.exception("failure-debug DB status check failed for %s", task_id)
        return
    host = os.environ.get("SA_DEBUGGER_HOST", "secflow-app-system-analyse-debugger")
    port = int(os.environ.get("SA_DEBUGGER_PORT", "8080"))
    url = f"http://{host}:{port}/api/app/system-analyse/internal/failure-debug"
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
        time.sleep(5)
    logger.error("failure-debug dispatch exhausted for task %s (debugger unreachable)", task_id)


def _clean_task_artifacts(task_id: str) -> None:
    """restart 语义: 清空任务 run/output 产物 + DB stages_json。

    安全检查: 如果任务正在被别的 worker 执行 (lease 有效), 跳过清理,
    避免删掉正在执行的 workspace (Redis 重连重投场景)。
    """
    import shutil
    from pathlib import Path
    from app.db import get_db
    from app.db.models import AppSaTask
    from app.time_utils import now_local
    try:
        db_gen = get_db()
        db = next(db_gen)
        try:
            row = db.query(AppSaTask).filter_by(task_id=task_id).first()
            if row is None:
                return
            # 安全检查: 如果有活 worker 持有有效 lease, 不清理 (防双执行竞态)
            if (row.status == "running"
                and row.execution_owner_id
                and row.execution_owner_id != WORKER_ID
                and row.execution_lease_until
                and row.execution_lease_until > now_local()):
                logger.warning("skip _clean_task_artifacts: task %s has active lease by %s",
                               task_id, row.execution_owner_id)
                return
            row.stages_json = None
            row.result_json = None
            row.latest_abnormal_reason_json = None
            db.commit()
            task_root = Path(row.output_path or "") / task_id
            if task_root.is_dir():
                for child_name in ("run", "output"):
                    child = task_root / child_name
                    if child.exists() or child.is_symlink():
                        try:
                            if child.is_symlink():
                                child.unlink()
                            else:
                                shutil.rmtree(str(child))
                            logger.info("cleaned task artifacts: %s/%s", task_id, child_name)
                        except Exception as exc:
                            logger.warning("clean task artifact %s failed: %s", child_name, exc)
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
    except Exception:
        logger.warning("_clean_task_artifacts failed task=%s", task_id, exc_info=True)


@task_revoked.connect
def _on_revoked(sender, request, **kwargs):
    """cancel/revoke 时杀整组 pi/node (等价 killpg)。"""
    celery_id = getattr(request, "id", None) if request else None
    if not celery_id:
        return
    with _PGID_LOCK:
        pgid = _PGID.pop(celery_id, None)
    if pgid is None:
        return
    logger.info("task_revoked celery_id=%s pgid=%s → killpg SIGKILL", celery_id, pgid)
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            return
        except OSError:
            return
        if sig == signal.SIGTERM:
            time.sleep(0.5)
