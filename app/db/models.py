"""SQLAlchemy ORM models for secflow-app-system-analyse."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import Boolean, DateTime, Integer, String, Text
from sqlalchemy.dialects.mysql import JSON
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.time_utils import now_local


class Base(DeclarativeBase):
    pass


class AppSaTask(Base):
    """File-path based analysis task, scoped to a project."""
    __tablename__ = "secflow_app_sa_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    project_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    task_origin_type: Mapped[Optional[str]] = mapped_column(String(32), nullable=True, index=True)
    analysis_mode: Mapped[Optional[str]] = mapped_column(String(32), nullable=True, index=True)
    parent_project_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    parent_task_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    parent_task_type: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    parent_stage_name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    parent_stage_item_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    parent_stage_item_key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    task_name: Mapped[str] = mapped_column(String(255), nullable=False)
    task_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    input_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    output_path: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)

    prompt_template_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    prompt_content: Mapped[str] = mapped_column(Text, nullable=False)

    # Status: pending | running | passed | failed | error | cancelled
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending", index=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    result_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    # DEPRECATED: events moved to {output_path}/{task_id}/run/events.jsonl
    # Kept for backward-compat read of old tasks that have no events.jsonl.
    # New tasks no longer write to this field. Do NOT remove until all rows
    # have been migrated by scripts/migrate_stages_to_file.py.
    stages_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    latest_abnormal_reason_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)

    created_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, onupdate=now_local)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    dispatcher_instance_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)
    dispatch_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, index=True)
    lease_epoch: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, index=True)

    task_config_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)

    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class AppSaTaskEvent(Base):
    """Structured task/stage timeline events for execution trace analysis."""

    __tablename__ = "secflow_app_sa_task_event"

    id: Mapped[str] = mapped_column(String(48), primary_key=True)
    task_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    project_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    stage_name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    level: Mapped[str] = mapped_column(String(16), nullable=False, default="info")
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, index=True)


class AppSaPromptTemplate(Base):
    """Reusable prompt templates for secflow-app-system-analyse."""
    __tablename__ = "secflow_app_sa_prompt_templates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    prompt_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[str] = mapped_column(String(64), nullable=False, default="general")
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    variables_json: Mapped[Optional[List[str]]] = mapped_column(JSON, nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    is_default: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    created_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    updated_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, onupdate=now_local)

    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class AppSaProjectConfig(Base):
    """Per-project analysis configuration blob."""
    __tablename__ = "secflow_app_sa_project_configs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    project_id: Mapped[str] = mapped_column(String(100), nullable=False, unique=True, index=True)
    config_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, onupdate=now_local)


class AppSaModelsConfig(Base):
    """Global models.json configuration (LLM provider/model registry)."""
    __tablename__ = "secflow_app_sa_models_config"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # Singleton row keyed by a fixed label so we can have only one global config.
    config_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True, default="global")
    config_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, onupdate=now_local)
