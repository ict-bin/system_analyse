"""Database engine and session management."""

from __future__ import annotations

import logging
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from .models import Base

logger = logging.getLogger("sa.db")

_engine = None
_SessionLocal = None

_MIGRATIONS = [
    # Add stages_json column for real-time stage tracking (added 2026-05)
    "ALTER TABLE secflow_app_sa_tasks ADD COLUMN stages_json JSON NULL",
    "ALTER TABLE secflow_app_sa_tasks ADD COLUMN task_origin_type VARCHAR(32) NULL",
    "ALTER TABLE secflow_app_sa_tasks ADD COLUMN analysis_mode VARCHAR(32) NULL",
    "ALTER TABLE secflow_app_sa_tasks ADD COLUMN parent_project_id VARCHAR(100) NULL",
    "ALTER TABLE secflow_app_sa_tasks ADD COLUMN parent_task_id VARCHAR(64) NULL",
    "ALTER TABLE secflow_app_sa_tasks ADD COLUMN parent_task_type VARCHAR(32) NULL",
    "ALTER TABLE secflow_app_sa_tasks ADD COLUMN parent_stage_name VARCHAR(64) NULL",
    "ALTER TABLE secflow_app_sa_tasks ADD COLUMN parent_stage_item_id VARCHAR(64) NULL",
    "ALTER TABLE secflow_app_sa_tasks ADD COLUMN parent_stage_item_key VARCHAR(255) NULL",
    "ALTER TABLE secflow_app_sa_tasks ADD COLUMN dispatcher_instance_id VARCHAR(128) NULL",
    "ALTER TABLE secflow_app_sa_tasks ADD COLUMN dispatch_started_at DATETIME NULL",
    "CREATE INDEX ix_sa_tasks_dispatcher_instance_id ON secflow_app_sa_tasks (dispatcher_instance_id)",
    "CREATE INDEX ix_sa_tasks_dispatch_started_at ON secflow_app_sa_tasks (dispatch_started_at)",
    "ALTER TABLE secflow_app_sa_tasks ADD COLUMN lease_epoch INT NOT NULL DEFAULT 0",
    "ALTER TABLE secflow_app_sa_tasks ADD COLUMN lease_expires_at DATETIME NULL",
    "CREATE INDEX ix_sa_tasks_lease_expires_at ON secflow_app_sa_tasks (lease_expires_at)",
    # Add task_config_json column for per-task analysis scope overrides (added 2026-06)
    "ALTER TABLE secflow_app_sa_tasks ADD COLUMN task_config_json JSON NULL",
    # Fallback: create per-project config table if create_all failed (added 2026-06)
    (
        "CREATE TABLE IF NOT EXISTS secflow_app_sa_project_configs ("
        "  id INT NOT NULL AUTO_INCREMENT,"
        "  project_id VARCHAR(100) NOT NULL,"
        "  config_json JSON NULL,"
        "  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
        "  PRIMARY KEY (id),"
        "  UNIQUE KEY uix_sa_project_cfg_pid (project_id)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
    ),
    # Fallback: create global models config table if create_all failed (added 2026-06)
    (
        "CREATE TABLE IF NOT EXISTS secflow_app_sa_models_config ("
        "  id INT NOT NULL AUTO_INCREMENT,"
        "  config_key VARCHAR(64) NOT NULL DEFAULT 'global',"
        "  config_json JSON NULL,"
        "  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
        "  PRIMARY KEY (id),"
        "  UNIQUE KEY uix_sa_models_cfg_key (config_key)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
    ),
    "CREATE INDEX ix_sa_tasks_project_deleted_created_id ON secflow_app_sa_tasks (project_id, is_deleted, created_at, id)",
    "CREATE INDEX ix_sa_tasks_project_created_id ON secflow_app_sa_tasks (project_id, created_at, id)",
    "CREATE INDEX ix_sa_tasks_project_deleted_status_created_id ON secflow_app_sa_tasks (project_id, is_deleted, status, created_at, id)",
    "CREATE INDEX ix_sa_tasks_project_deleted_mode_created_id ON secflow_app_sa_tasks (project_id, is_deleted, analysis_mode, created_at, id)",
]


def _run_migrations(engine) -> None:
    """Apply additive schema migrations; silently skips already-applied ones."""
    with engine.connect() as conn:
        for stmt in _MIGRATIONS:
            try:
                conn.execute(text(stmt))
                conn.commit()
                logger.info("Migration applied: %s", stmt[:60])
            except Exception:
                conn.rollback()


def init_db(
    db_url: str,
    pool_size: int = 5,
    max_overflow: int = 10,
    pool_timeout: int = 30,
    pool_recycle: int = 3600,
    run_migrations: bool = True,
) -> None:
    """Initialize the database engine and create tables."""
    global _engine, _SessionLocal
    _engine = create_engine(
        db_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_pre_ping=True,
        pool_recycle=pool_recycle,
        pool_use_lifo=True,
    )
    _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)
    if run_migrations:
        Base.metadata.create_all(bind=_engine)
        _run_migrations(_engine)
    logger.info("Database initialized")


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency that provides a DB session."""
    if _SessionLocal is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    db: Session = _SessionLocal()
    try:
        yield db
    finally:
        db.close()


def ping_db() -> bool:
    """Return True when the configured DB is reachable and can serve a trivial query."""
    if _engine is None:
        return False
    try:
        with _engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
