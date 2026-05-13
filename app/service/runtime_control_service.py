from __future__ import annotations

import logging
import time as _time
from typing import Any

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db.models import AppSaModelsConfig

logger = logging.getLogger("sa.runtime_control")

RUNTIME_CONTROL_CONFIG_KEY = "runtime_control"
_DEFAULT_RUNTIME_CONTROL = {
    "claim_enabled": True,
    "drain_mode": False,
    "pause_claim_until_ts": None,
    "reason": "",
    "updated_by": "",
}


def _sanitize_runtime_control(raw: dict[str, Any] | None, updated_at: str | None = None) -> dict:
    data = dict(_DEFAULT_RUNTIME_CONTROL)
    data.update(raw or {})
    try:
        pause_claim_until_ts = float(data.get("pause_claim_until_ts") or 0.0)
    except (TypeError, ValueError):
        pause_claim_until_ts = 0.0
    if pause_claim_until_ts <= 0:
        pause_claim_until_ts = None
    return {
        "claim_enabled": bool(data.get("claim_enabled", True)),
        "drain_mode": bool(data.get("drain_mode", False)),
        "pause_claim_until_ts": pause_claim_until_ts,
        "reason": str(data.get("reason") or "").strip(),
        "updated_by": str(data.get("updated_by") or "").strip(),
        "updated_at": updated_at,
    }


class RuntimeControlService:
    def get_runtime_control(self, db: Session) -> dict:
        try:
            row = db.query(AppSaModelsConfig).filter_by(config_key=RUNTIME_CONTROL_CONFIG_KEY).first()
        except SQLAlchemyError as exc:
            logger.error("Failed to query runtime control: %s", exc)
            return _sanitize_runtime_control(None)
        payload = dict(row.config_json) if row and isinstance(row.config_json, dict) else None
        return _sanitize_runtime_control(payload, row.updated_at.isoformat() if row and row.updated_at else None)

    def save_runtime_control(self, db: Session, payload: dict[str, Any]) -> dict:
        current = self.get_runtime_control(db)
        merged = {
            "claim_enabled": payload.get("claim_enabled", current["claim_enabled"]),
            "drain_mode": payload.get("drain_mode", current["drain_mode"]),
            "pause_claim_until_ts": payload.get("pause_claim_until_ts", current["pause_claim_until_ts"]),
            "reason": payload.get("reason", current["reason"]),
            "updated_by": payload.get("updated_by", current["updated_by"]),
        }
        sanitized = _sanitize_runtime_control(merged)
        blob = {k: v for k, v in sanitized.items() if k != "updated_at"}
        try:
            row = db.query(AppSaModelsConfig).filter_by(config_key=RUNTIME_CONTROL_CONFIG_KEY).first()
            if row:
                row.config_json = blob
            else:
                row = AppSaModelsConfig(config_key=RUNTIME_CONTROL_CONFIG_KEY, config_json=blob)
                db.add(row)
            db.commit()
            db.refresh(row)
        except SQLAlchemyError as exc:
            db.rollback()
            logger.error("Failed to save runtime control: %s", exc)
            raise
        return _sanitize_runtime_control(blob, row.updated_at.isoformat() if row.updated_at else None)

    def pause_claim(self, db: Session, *, seconds: int, reason: str = "", updated_by: str = "") -> dict:
        pause_until_ts = _time.time() + max(1, int(seconds))
        return self.save_runtime_control(
            db,
            {
                "claim_enabled": True,
                "drain_mode": False,
                "pause_claim_until_ts": pause_until_ts,
                "reason": reason,
                "updated_by": updated_by,
            },
        )

    def resume_claim(self, db: Session, *, reason: str = "", updated_by: str = "") -> dict:
        return self.save_runtime_control(
            db,
            {
                "claim_enabled": True,
                "drain_mode": False,
                "pause_claim_until_ts": None,
                "reason": reason,
                "updated_by": updated_by,
            },
        )

    def set_drain_mode(self, db: Session, *, enabled: bool, reason: str = "", updated_by: str = "") -> dict:
        current = self.get_runtime_control(db)
        return self.save_runtime_control(
            db,
            {
                "claim_enabled": current["claim_enabled"],
                "drain_mode": bool(enabled),
                "pause_claim_until_ts": current["pause_claim_until_ts"],
                "reason": reason,
                "updated_by": updated_by,
            },
        )


_runtime_control_service: RuntimeControlService | None = None


def get_runtime_control_service() -> RuntimeControlService:
    global _runtime_control_service
    if _runtime_control_service is None:
        _runtime_control_service = RuntimeControlService()
    return _runtime_control_service
