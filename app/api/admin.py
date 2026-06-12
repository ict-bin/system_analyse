"""Administrative runtime API routes."""

from __future__ import annotations

from fastapi import Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db import get_db
from app.service.runtime_control_service import get_runtime_control_service
from app.service.task_service import get_task_service

from . import router


class RuntimeControlSaveRequest(BaseModel):
    claim_enabled: bool = True
    drain_mode: bool = False
    pause_claim_until_ts: float | None = None
    reason: str = ""
    updated_by: str = ""


class RuntimePauseClaimRequest(BaseModel):
    seconds: int = Field(..., ge=1, le=86400)
    reason: str = ""
    updated_by: str = ""


class RuntimeOperatorRequest(BaseModel):
    reason: str = ""
    updated_by: str = ""


@router.get("/admin/runtime")
def get_runtime_overview(db: Session = Depends(get_db)):
    return get_task_service().get_runtime_overview(db)


@router.get("/admin/runtime-control")
def get_runtime_control(db: Session = Depends(get_db)):
    return get_runtime_control_service().get_runtime_control(db)


@router.put("/admin/runtime-control")
def save_runtime_control(body: RuntimeControlSaveRequest, db: Session = Depends(get_db)):
    return get_runtime_control_service().save_runtime_control(db, body.model_dump())


@router.post("/admin/runtime-control/pause-claim")
def pause_runtime_claim(body: RuntimePauseClaimRequest, db: Session = Depends(get_db)):
    return get_runtime_control_service().pause_claim(
        db,
        seconds=body.seconds,
        reason=body.reason,
        updated_by=body.updated_by,
    )


@router.post("/admin/runtime-control/resume-claim")
def resume_runtime_claim(body: RuntimeOperatorRequest, db: Session = Depends(get_db)):
    return get_runtime_control_service().resume_claim(
        db,
        reason=body.reason,
        updated_by=body.updated_by,
    )


@router.post("/admin/runtime-control/drain")
def enable_runtime_drain(body: RuntimeOperatorRequest, db: Session = Depends(get_db)):
    return get_runtime_control_service().set_drain_mode(
        db,
        enabled=True,
        reason=body.reason,
        updated_by=body.updated_by,
    )


@router.post("/admin/runtime-control/activate")
def disable_runtime_drain(body: RuntimeOperatorRequest, db: Session = Depends(get_db)):
    return get_runtime_control_service().set_drain_mode(
        db,
        enabled=False,
        reason=body.reason,
        updated_by=body.updated_by,
    )
