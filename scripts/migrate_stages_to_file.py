#!/usr/bin/env python3
"""migrate_stages_to_file.py — 将存量 stages_json 从 DB 迁移到 events.jsonl 文件

运行条件：
- 新代码已部署（event_log.py 存在）
- 在 secflow-app-system-analyse pod 内运行（有 DB 连接和文件系统访问权）

操作：
1. 查找所有 is_deleted=0 且 stages_json IS NOT NULL 的任务
2. 若 events.jsonl 不存在 → 将 stages_json.events 写入 events.jsonl
3. 写入成功 → 将 DB 中 stages_json 置为 NULL（释放行空间）
4. 分批处理（每批 10 条），打印进度

用法（在 pod 内）：
  python3 /app/scripts/migrate_stages_to_file.py [--dry-run] [--batch-size 10]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, "/app")


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate stages_json from DB to events.jsonl")
    parser.add_argument("--dry-run", action="store_true", help="不写入，只打印统计")
    parser.add_argument("--batch-size", type=int, default=10)
    args = parser.parse_args()

    from app.server import _load_service_config_once  # noqa: PLC0415
    from app.db import init_db, get_db  # noqa: PLC0415
    from app.db.models import AppSaTask  # noqa: PLC0415
    from app.service.event_log import events_path, write_final  # noqa: PLC0415

    svc = _load_service_config_once()
    init_db(svc.db.url, pool_size=2, max_overflow=2, run_migrations=False)

    db_gen = get_db()
    db = next(db_gen)

    total = db.query(AppSaTask).filter(
        AppSaTask.is_deleted.is_(False),
        AppSaTask.stages_json.isnot(None),
    ).count()
    print(f"待迁移任务数: {total}")

    migrated = 0
    skipped = 0
    errors = 0
    offset = 0

    while True:
        rows = db.query(AppSaTask).filter(
            AppSaTask.is_deleted.is_(False),
            AppSaTask.stages_json.isnot(None),
        ).order_by(AppSaTask.id.asc()).limit(args.batch_size).offset(offset).all()

        if not rows:
            break

        for row in rows:
            epath = events_path(row.output_path, row.task_id)
            sj = row.stages_json

            if epath is None:
                print(f"  SKIP {row.task_id}: output_path 为 None，无法写文件")
                skipped += 1
                continue

            if epath.exists():
                print(f"  SKIP {row.task_id}: events.jsonl 已存在，跳过（可能已迁移）")
                skipped += 1
                continue

            events = []
            if isinstance(sj, dict):
                events = sj.get("events") or []
            elif isinstance(sj, str):
                try:
                    parsed = json.loads(sj)
                    events = parsed.get("events") or []
                except Exception:
                    pass

            print(f"  MIGR {row.task_id}: {len(events)} events → {epath}")
            if not args.dry_run:
                try:
                    write_final(epath, events)
                    row.stages_json = None
                    db.commit()
                    migrated += 1
                except Exception as exc:
                    db.rollback()
                    print(f"  ERROR {row.task_id}: {exc}")
                    errors += 1
            else:
                migrated += 1

        offset += args.batch_size

    print(f"\n完成: 迁移={migrated} 跳过={skipped} 错误={errors}")
    if args.dry_run:
        print("(--dry-run 模式，未实际写入)")

    try:
        next(db_gen)
    except StopIteration:
        pass


if __name__ == "__main__":
    main()
