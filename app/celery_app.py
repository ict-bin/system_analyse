"""Celery 实例 + 配置（系统分析 v2 调度）。

- broker / result backend = scheduler pod 内的 Redis（非持久, DB 为真相）。
- worker pod: `celery -A app.celery_app worker -P prefork -c 1 ...`
- 本模块 import 时即初始化 DB（celery worker / dispatcher 进程不经 runtime_bootstrap）。
"""
from __future__ import annotations

import logging
import os

from celery import Celery

logger = logging.getLogger("sa.celery")

REDIS_HOST = os.environ.get("SA_SCHEDULER_HOST", "secflow-app-system-analyse-scheduler")
REDIS_PORT = int(os.environ.get("SA_SCHEDULER_REDIS_PORT", "6379"))
BROKER_DB = int(os.environ.get("SA_CELERY_BROKER_DB", "0"))
BACKEND_DB = int(os.environ.get("SA_CELERY_BACKEND_DB", "1"))

broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/{BROKER_DB}"
result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/{BACKEND_DB}"

app = Celery("sa", broker_url=broker_url, result_backend=result_backend, include=["app.celery_tasks"])

# 长任务 + 无限 LLM: 不靠 visibility_timeout 回收, 靠 dispatcher stale 扫描
_VIS_TIMEOUT = int(os.environ.get("SA_CELERY_VISIBILITY_TIMEOUT", str(86400 * 7)))
app.conf.update(
    task_acks_late=True,                 # worker 死/rollout → 未 ack 消息回队列重投
    task_reject_on_worker_lost=True,     # worker 进程丢失 → 消息重投
    task_track_started=True,
    broker_transport_options={"visibility_timeout": _VIS_TIMEOUT},
    result_backend_transport_options={"visibility_timeout": _VIS_TIMEOUT},
    worker_prefetch_multiplier=1,        # 长任务, 不预取多余
    worker_max_tasks_per_child=int(os.environ.get("SA_CELERY_MAX_TASKS_PER_CHILD", "10")),
    worker_send_task_events=True,
    task_send_sent_events=True,
    broker_connection_retry_on_startup=True,
    result_expires=86400,
    task_default_queue="sa",
    task_routes={"app.celery_tasks.run_sa_task": {"queue": "sa"}},
)


def _ensure_db() -> None:
    """celery worker / dispatcher 进程无 runtime_bootstrap, 需自己 init DB。"""
    try:
        import app.db as _dbmod
        if _dbmod._engine is not None:
            return
        from app.db import init_db
        from app.config import get_service_yaml
        svc = get_service_yaml()
        init_db(
            svc.database.url,
            pool_size=svc.database.pool_size,
            max_overflow=svc.database.max_overflow,
            pool_timeout=svc.database.pool_timeout,
            pool_recycle=svc.database.pool_recycle,
        )
        logger.info("DB initialized for celery/dispatcher process: %s:%s", svc.database.host, svc.database.port)
    except Exception:
        logger.warning("celery_app: DB init failed (will retry on first DB use)", exc_info=True)


# 不在模块级调 _ensure_db(): 避免导入时 DB migration 异常导致 celery app 加载失败。
# DB 初始化在 run_sa_task / dispatcher.main() 中显式调用。
