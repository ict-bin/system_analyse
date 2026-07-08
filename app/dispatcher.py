"""系统分析调度器侧车: DB→Celery 泵 + 启动重置 + stale 扫描。

跑在 scheduler pod (与 Redis 同 pod)。纯 threading, 无 asyncio。
DB 是任务真相, Redis 是临时队列; Redis 丢/重启 → _startup_reset 全 running→pending + 重新发布。
worker 死亡 → _stale_loop 用 inspect.active() 找孤儿 running → 重置重排。

入口: python -m app.dispatcher
"""
from __future__ import annotations

import logging
import os
import threading
import time

logger = logging.getLogger("sa.dispatcher")

PUMP_INTERVAL = float(os.environ.get("SA_DISPATCHER_PUMP_INTERVAL", "3"))
STALE_INTERVAL = float(os.environ.get("SA_DISPATCHER_STALE_INTERVAL", "30"))
PUMP_BATCH = int(os.environ.get("SA_DISPATCHER_PUMP_BATCH", "20"))
STALE_HEARTBEAT_SECONDS = int(os.environ.get("SA_DISPATCHER_STALE_HEARTBEAT_SECONDS", "120"))
INSPECT_TIMEOUT = float(os.environ.get("SA_DISPATCHER_INSPECT_TIMEOUT", "3"))


class Dispatcher:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        self._stop.clear()
        self._startup_reset()
        t = threading.Thread(target=self._pump_loop, name="sa_disp_pump", daemon=True)
        t.start(); self._threads.append(t)
        t = threading.Thread(target=self._stale_loop, name="sa_disp_stale", daemon=True)
        t.start(); self._threads.append(t)
        logger.info("Dispatcher started: pump=%ss stale=%ss", PUMP_INTERVAL, STALE_INTERVAL)

    def stop(self) -> None:
        self._stop.set()

    def _startup_reset(self) -> None:
        from app.db import get_db
        from app.db.models import AppSaTask
        db_gen = get_db()
        db = next(db_gen)
        try:
            n_running = db.query(AppSaTask).filter(
                AppSaTask.status == "running",
                AppSaTask.is_deleted.is_(False),
            ).update({
                AppSaTask.status: "pending",
                AppSaTask.celery_task_id: None,
                AppSaTask.execution_owner_id: None,
                AppSaTask.execution_lease_until: None,
                AppSaTask.dispatch_status: None,
            }, synchronize_session=False)
            n_pending = db.query(AppSaTask).filter(
                AppSaTask.status == "pending",
                AppSaTask.is_deleted.is_(False),
                AppSaTask.celery_task_id.is_not(None),
            ).update({
                AppSaTask.celery_task_id: None,
                AppSaTask.execution_owner_id: None,
                AppSaTask.execution_lease_until: None,
                AppSaTask.dispatch_status: None,
            }, synchronize_session=False)
            db.commit()
            if n_running or n_pending:
                logger.warning("startup_reset: %d running→pending, %d pending stale celery_id cleared",
                               n_running, n_pending)
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _pump_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._pump_once()
            except Exception as exc:
                logger.warning("pump loop error: %s", exc, exc_info=True)
            self._stop.wait(PUMP_INTERVAL)

    def _pump_once(self) -> int:
        from app.db import get_db
        from app.db.models import AppSaTask
        from app.celery_tasks import run_sa_task
        db_gen = get_db()
        db = next(db_gen)
        published = 0
        try:
            rows = (
                db.query(AppSaTask)
                .filter(
                    AppSaTask.status == "pending",
                    AppSaTask.is_deleted.is_(False),
                    AppSaTask.celery_task_id.is_(None),
                )
                .order_by(AppSaTask.created_at.asc())
                .limit(PUMP_BATCH)
                .all()
            )
            for row in rows:
                try:
                    ar = run_sa_task.delay(row.task_id)
                    row.celery_task_id = ar.id
                    db.commit()
                    published += 1
                    logger.info("published task=%s celery_id=%s", row.task_id, ar.id)
                except Exception as exc:
                    logger.warning("publish failed task=%s: %s (retry next loop)", row.task_id, exc)
                    db.rollback()
                    break
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
        return published

    def _stale_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._stale_once()
            except Exception as exc:
                logger.warning("stale loop error: %s", exc, exc_info=True)
            self._stop.wait(STALE_INTERVAL)

    def _stale_once(self) -> int:
        from app.db import get_db
        from app.db.models import AppSaTask
        from app.time_utils import now_local
        from app.celery_app import app as celery_app
        active_ids: set[str] = set()
        try:
            inspect = celery_app.control.inspect(timeout=INSPECT_TIMEOUT)
            active = inspect.active() or {}
            for _pod, tasks in active.items():
                for t in (tasks or []):
                    cid = t.get("id") if isinstance(t, dict) else None
                    if cid:
                        active_ids.add(cid)
        except Exception as exc:
            logger.warning("inspect.active failed: %s (skip this round)", exc)
            return 0
        db_gen = get_db()
        db = next(db_gen)
        reset = 0
        try:
            now = now_local()
            rows = db.query(AppSaTask).filter(
                AppSaTask.status == "running",
                AppSaTask.is_deleted.is_(False),
            ).all()
            for row in rows:
                cid = row.celery_task_id
                in_active = cid is not None and cid in active_ids
                heartbeat_stale = (
                    row.execution_heartbeat_at is None
                    or (now - row.execution_heartbeat_at).total_seconds() > STALE_HEARTBEAT_SECONDS
                )
                if in_active and not heartbeat_stale:
                    continue
                if cid:
                    try:
                        celery_app.control.revoke(cid, terminate=True, signal="SIGKILL")
                    except Exception:
                        pass
                row.status = "pending"
                row.celery_task_id = None
                row.execution_owner_id = None
                row.execution_lease_until = None
                row.dispatch_status = None
                reset += 1
                logger.warning("stale reset task=%s celery_id=%s in_active=%s hb_stale=%s",
                               row.task_id, cid, in_active, heartbeat_stale)
            if reset:
                db.commit()
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
        return reset


_dispatcher: Dispatcher | None = None


def get_dispatcher() -> Dispatcher:
    global _dispatcher
    if _dispatcher is None:
        _dispatcher = Dispatcher()
    return _dispatcher


def main() -> None:
    import signal as _sig
    from app.logging_utils import configure_container_logging
    configure_container_logging("sa-dispatcher")
    from app.celery_app import _ensure_db
    _ensure_db()
    disp = get_dispatcher()
    disp.start()
    def _handle(signum, frame):
        disp.stop()
    _sig.signal(_sig.SIGTERM, _handle)
    _sig.signal(_sig.SIGINT, _handle)
    while not disp._stop.is_set():
        time.sleep(5)


if __name__ == "__main__":
    main()
