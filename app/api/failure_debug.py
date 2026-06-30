"""failure_debug.py — 失败调试报告 API。

GET  /failure-debug-reports            列表（project_id 过滤、分页）
GET  /failure-debug-reports/{id}        详情
GET  /failure-debug-reports/{id}/download  下载 Markdown 报告
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import Depends, HTTPException, Query, Response
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import OUTPUT_DIR
from app.db import get_db
from app.db.models import AppSaFailureDebug

from . import router


def _row_to_dict(row: AppSaFailureDebug, *, detail: bool = False) -> dict[str, Any]:
    d = {
        "id": row.id,
        "task_id": row.task_id,
        "project_id": row.project_id,
        "task_name": row.task_name,
        "status": row.status,
        "error_kind": row.error_kind,
        "failing_stage": row.failing_stage,
        "summary": row.summary,
        "report_path": row.report_path,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }
    if detail:
        d["report_json"] = row.report_json
        d["debug_error"] = row.debug_error
    return d


@router.get("/failure-debug-reports")
def list_failure_debug_reports(
    project_id: str | None = Query(None),
    status: str | None = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    q = select(AppSaFailureDebug)
    if project_id:
        q = q.where(AppSaFailureDebug.project_id == project_id)
    if status:
        q = q.where(AppSaFailureDebug.status == status)
    from sqlalchemy import func

    count_q = select(func.count(AppSaFailureDebug.id))
    if project_id:
        count_q = count_q.where(AppSaFailureDebug.project_id == project_id)
    if status:
        count_q = count_q.where(AppSaFailureDebug.status == status)
    total = int(db.execute(count_q).scalar() or 0)

    rows = (
        db.execute(
            q.order_by(AppSaFailureDebug.created_at.desc())
            .offset((page - 1) * per_page)
            .limit(per_page)
        )
        .scalars()
        .all()
    )
    return {
        "items": [_row_to_dict(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
    }


@router.get("/failure-debug-reports/{report_id}")
def get_failure_debug_report(report_id: int, db: Session = Depends(get_db)):
    row = db.get(AppSaFailureDebug, report_id)
    if not row:
        raise HTTPException(status_code=404, detail="report not found")
    return _row_to_dict(row, detail=True)


@router.get("/failure-debug-reports/{report_id}/download")
def download_failure_debug_report(report_id: int, db: Session = Depends(get_db)):
    row = db.get(AppSaFailureDebug, report_id)
    if not row:
        raise HTTPException(status_code=404, detail="report not found")
    # 优先用 report_path，回退到标准位置
    md_path = Path(row.report_path) if row.report_path else None
    if not md_path or not md_path.is_file():
        md_path = Path(OUTPUT_DIR) / row.task_id / "output" / "failure_debug_report.md"
    if not md_path.is_file():
        raise HTTPException(status_code=404, detail="report file not found on disk")
    content = md_path.read_bytes()
    filename = f"failure_debug_{row.task_id}.md"
    return Response(
        content=content,
        media_type="text/markdown; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
