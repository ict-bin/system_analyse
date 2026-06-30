"""API router package for secflow-app-system-analyse."""

from fastapi import APIRouter

router = APIRouter(prefix="/api/app/system-analyse")

from . import admin, tasks, prompts, config, failure_debug  # noqa: E402, F401
