#!/usr/bin/env python3
"""run_task.py — V3.0 任务子进程入口。

由 Worker 控制进程 (worker_control.WorkerControl) 作为子进程拉起：
    python run_task.py <task_id> [lease_epoch]

在独立进程中执行 TaskRunner.execute_task（复用 TaskService 的全部依赖装配）。
任务进程自身负责正常归档（orchestrator finalize 写 output/ + DB 终态）。
被杀/异常退出时由控制进程代为归档。退出码：0=成功，非0=失败。
"""
from __future__ import annotations

import logging
import sys


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: run_task.py <task_id> [lease_epoch]", file=sys.stderr)
        return 2
    task_id = sys.argv[1]
    lease_epoch = int(sys.argv[2]) if len(sys.argv) > 2 else 0

    from app.logging_utils import configure_container_logging
    configure_container_logging("01-system_analyse")
    logger = logging.getLogger("sa.run_task")

    # 诊断：对卡死子进程 `kill -USR1 <pid>` 可把当前 Python 栈转储到 stderr（无需 ptrace）。
    # 用于定位 NFS 卡死等挂起点。
    try:
        import faulthandler, signal
        faulthandler.enable()
        if hasattr(signal, "SIGUSR1"):
            faulthandler.register(signal.SIGUSR1, all_threads=True, chain=False)
    except Exception:
        pass

    try:
        # 任务子进程是全新进程，必须先初始化 DB engine（server 进程才在启动时 init_db）
        from app.config import get_service_yaml
        from app.db import init_db
        svc = get_service_yaml()
        init_db(svc.database.url, pool_size=svc.database.pool_size,
                max_overflow=svc.database.max_overflow,
                pool_timeout=svc.database.pool_timeout,
                pool_recycle=svc.database.pool_recycle,
                run_migrations=False)
        # 复用 TaskService 的完整依赖装配（DB/锁/配置/事件等）
        from app.service.task_service import get_task_service
        ts = get_task_service()
        runner = ts._runner  # TaskRunner 已在 TaskService.__init__ 装配好
        runner.execute_task(task_id, lease_epoch)
        return 0
    except SystemExit as e:   # 任务内部主动退出
        return int(getattr(e, "code", 0) or 0)
    except Exception as exc:
        logger.exception("task subprocess failed: %s", task_id)
        print(f"task failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
