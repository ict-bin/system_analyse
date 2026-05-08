"""Task management service for secflow-app-system-analyse.

Bridges the FastAPI management layer with the existing Orchestrator engine.
Each task is persisted in MySQL and executed asynchronously.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time as _time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session

from app.config import build_task_config, load_service_config
from app.db.models import AppSaTask
from app.logging_utils import log_event
from app.models import SwarmEvent, TaskStatus
from app.orchestrator import Orchestrator

logger = logging.getLogger("sa.task_service")

SERVICE_CONFIG_PATH = os.environ.get("SERVICE_CONFIG", "/app/config.json")

# Running asyncio tasks keyed by task_id so we can cancel them
_running_tasks: dict[str, asyncio.Task] = {}

_SUMMARY_PATTERNS = {
    "module_count": re.compile(r"\|\s*分析模块数\s*\|\s*(\d+)\s*\|"),
    "total_file_count": re.compile(r"\|\s*总文件数\s*\|\s*(\d+)\s*\|"),
    "high_risk_module_count": re.compile(r"\|\s*高风险模块数\s*\|\s*(\d+)\s*\|"),
    "medium_risk_module_count": re.compile(r"\|\s*中风险模块数\s*\|\s*(\d+)\s*\|"),
    "low_risk_module_count": re.compile(r"\|\s*低风险模块数\s*\|\s*(\d+)\s*\|"),
    "threat_count": re.compile(r"\|\s*威胁总数\s*\|\s*(\d+)\s*\|"),
}

_RISK_LEVEL_RE = re.compile(r"<!--\s*RISK_LEVEL:\s*([^\-][^>]*)-->")
_RISK_SCORE_RE = re.compile(r"<!--\s*RISK_SCORE:\s*(\d+)\s*-->")
_MARKDOWN_HEADING_RE = re.compile(r"^(#{1,2})\s+(.+)$", re.MULTILINE)


def _read_text_if_exists(path: Path) -> tuple[str | None, str | None]:
    if not path.exists() or not path.is_file():
        return None, f"文件不存在: {path.name}"
    try:
        return path.read_text("utf-8"), None
    except Exception as exc:
        return None, f"文件读取失败: {path.name} ({exc})"


def _parse_summary(final_report_markdown: str | None) -> dict:
    summary = {
        "module_count": 0,
        "high_risk_module_count": 0,
        "medium_risk_module_count": 0,
        "low_risk_module_count": 0,
        "total_file_count": 0,
        "threat_count": 0,
    }
    if not final_report_markdown:
        return summary
    for key, pattern in _SUMMARY_PATTERNS.items():
        match = pattern.search(final_report_markdown)
        if match:
            summary[key] = int(match.group(1))
    return summary


def _parse_report_sections(markdown: str | None) -> list[dict]:
    if not markdown:
        return []
    sections: list[dict] = []
    for idx, match in enumerate(_MARKDOWN_HEADING_RE.finditer(markdown)):
        sections.append({
            "level": len(match.group(1)),
            "title": match.group(2).strip(),
            "anchor": f"section-{idx + 1}",
        })
    return sections


def _infer_risk_level(markdown: str | None) -> str | None:
    if not markdown:
        return None
    level_match = _RISK_LEVEL_RE.search(markdown)
    if level_match:
        return level_match.group(1).strip()
    if "🔴高" in markdown or "风险等级 | 🔴高" in markdown:
        return "高"
    if "🟡中" in markdown or "风险等级 | 🟡中" in markdown:
        return "中"
    if "🟢低" in markdown or "风险等级 | 🟢低" in markdown:
        return "低"
    return None


def _infer_risk_score(markdown: str | None) -> int | None:
    if not markdown:
        return None
    score_match = _RISK_SCORE_RE.search(markdown)
    if score_match:
        return int(score_match.group(1))
    return None


def _origin_payload(row: AppSaTask) -> dict:
    task_origin_type = str(row.task_origin_type or "").strip() or "manual"
    parent_task_type = str(row.parent_task_type or "").strip() or None
    origin_label = (
        "二进制安全-源码扫描"
        if task_origin_type == "binary_security" and parent_task_type == "source"
        else "二进制安全-二进制类扫描"
        if task_origin_type == "binary_security"
        else "手动任务"
    )
    return {
        "task_origin_type": task_origin_type,
        "parent_project_id": row.parent_project_id,
        "parent_task_id": row.parent_task_id,
        "parent_task_type": parent_task_type,
        "parent_stage_name": row.parent_stage_name,
        "parent_stage_item_id": row.parent_stage_item_id,
        "parent_stage_item_key": row.parent_stage_item_key,
        "origin_label": origin_label,
        "parent_task_display": row.parent_task_id,
    }


def _load_svc_config():
    for p in [SERVICE_CONFIG_PATH, "/opt/system_analyse/config.example.json"]:
        if os.path.isfile(p):
            return load_service_config(p)
    raise RuntimeError(f"Service config not found: {SERVICE_CONFIG_PATH}")


def _load_svc_config_from_db(db: "Session", project_id: str) -> "ServiceConfig":
    """从数据库读取分析配置，构造 ServiceConfig；失败时回退到文件读取。"""
    try:
        from app.service.config_service import get_config_service
        from app.models import ServiceConfig as _ServiceConfig
        cfg_dict = get_config_service().get_config(db, project_id)
        # Strip meta/readonly fields not part of ServiceConfig schema
        for _k in ("updated_at", "project_id"):
            cfg_dict.pop(_k, None)
        return _ServiceConfig(**cfg_dict)
    except Exception as _exc:
        logger.warning("_load_svc_config_from_db failed (%s), falling back to file: %s", project_id, _exc)
        return _load_svc_config()


def generate_prompt_from_path(input_path: str) -> str:
    path_lower = input_path.lower()
    if any(kw in path_lower for kw in ("firmware", "unpacked", "squashfs", "rootfs")):
        subject = "固件解包后的所有文件"
    elif any(kw in path_lower for kw in ("binary", "bin", "elf")):
        subject = "二进制可执行文件"
    elif any(kw in path_lower for kw in ("script", "sh", "py", "lua")):
        subject = "脚本文件"
    elif any(kw in path_lower for kw in ("config", "conf", "cfg", "etc")):
        subject = "配置文件"
    elif any(kw in path_lower for kw in ("source", "src", ".c", ".cpp", ".h")):
        subject = "源代码文件"
    else:
        subject = "目标文件"
    return (
        f"对路径 `{input_path}` 下的{subject}进行系统性安全分析，"
        "重点关注：威胁识别、模块功能分类、安全漏洞、敏感信息暴露及风险等级评估。"
    )


def _flush_stages(task_id: str, events: list[dict]) -> None:
    try:
        from sqlalchemy.orm.attributes import flag_modified
        from app.db import get_db as _get_db
        _gen = _get_db()
        _db = next(_gen)
        try:
            _r = _db.query(AppSaTask).filter_by(task_id=task_id).first()
            if _r:
                _r.stages_json = {"events": [dict(e) for e in events]}
                flag_modified(_r, "stages_json")
                _db.commit()
        finally:
            try:
                next(_gen)
            except StopIteration:
                pass
    except Exception as _exc:
        logger.warning("_flush_stages failed: %s", _exc, exc_info=True)


def _write_models_json_from_db(db: Session) -> None:
    """从数据库读取 models 配置并写入 pi 的配置目录，使 pi 能识别模型。"""
    try:
        from app.service.config_service import get_model_config_service
        import json as _json
        pi_dir = os.environ.get("PI_CODING_AGENT_DIR", "/root/.pi/agent")
        os.makedirs(pi_dir, exist_ok=True)
        models_cfg = get_model_config_service().get_models_config(db)
        # strip meta field before writing
        blob = {k: v for k, v in models_cfg.items() if k != "updated_at"}
        dest = os.path.join(pi_dir, "models.json")
        with open(dest, "w", encoding="utf-8") as _f:
            _json.dump(blob, _f, ensure_ascii=False, indent=2)
        logger.info("models.json written from DB → %s", dest)
    except Exception as _exc:
        logger.warning("_write_models_json_from_db failed: %s", _exc, exc_info=True)


class TaskService:

    def list_tasks(self, db: Session, *, project_id: str, page: int = 1,
                   per_page: int = 20, status: Optional[str] = None) -> dict:
        query = db.query(AppSaTask).filter(
            AppSaTask.project_id == project_id,
            AppSaTask.is_deleted.is_(False),
        )
        if status:
            query = query.filter(AppSaTask.status == status)
        total = query.count()
        rows = (query.order_by(AppSaTask.created_at.desc())
                .offset((page - 1) * per_page).limit(per_page).all())
        return {"items": [self._row_to_dict(r) for r in rows],
                "total": total, "page": page, "per_page": per_page}

    def get_task(self, db: Session, task_id: str) -> dict:
        return self._row_to_dict(self._get_or_404(db, task_id))

    def get_task_result(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        output_root = Path(row.output_path or "") / row.task_id / "output" if row.output_path else None
        final_report_path = output_root / "final_report.md" if output_root else None
        modules_list_path = output_root / "modules.list" if output_root else None
        modules_root = output_root / "modules" if output_root else None
        warnings: list[str] = []

        final_report_markdown: str | None = None
        if final_report_path:
            final_report_markdown, err = _read_text_if_exists(final_report_path)
            if err:
                warnings.append(err)

        modules_order: list[str] = []
        if modules_list_path:
            modules_list_markdown, err = _read_text_if_exists(modules_list_path)
            if err:
                warnings.append(err)
            elif modules_list_markdown:
                modules_order = [line.strip() for line in modules_list_markdown.splitlines() if line.strip()]

        available = bool(final_report_markdown or (modules_root and modules_root.exists()))
        if row.status not in ("passed", "failed", "error", "cancelled"):
            available = False

        modules: list[dict] = []
        total_files_counted = 0
        high_risk_modules_counted = 0
        if modules_root and modules_root.exists():
            discovered = {
                path.name
                for path in modules_root.iterdir()
                if path.is_dir() and not path.name.startswith(".")
            }
            ordered_names = modules_order + sorted(discovered - set(modules_order))
            for rank, module_name in enumerate(ordered_names, start=1):
                module_dir = modules_root / module_name
                if not module_dir.exists() or not module_dir.is_dir():
                    warnings.append(f"模块目录不存在: {module_name}")
                    continue
                files_list_path = module_dir / "files.list"
                module_report_path = module_dir / "module_report.md"
                if not module_report_path.exists():
                    fallback_report_path = module_dir / "modules_report.md"
                    if fallback_report_path.exists():
                        module_report_path = fallback_report_path

                files_list_content, files_err = _read_text_if_exists(files_list_path)
                if files_err:
                    warnings.append(f"{module_name}: {files_err}")
                module_report_markdown, report_err = _read_text_if_exists(module_report_path)
                if report_err:
                    warnings.append(f"{module_name}: {report_err}")

                files = [line.strip() for line in (files_list_content or "").splitlines() if line.strip()]
                file_count = len(files)
                total_files_counted += file_count
                risk_level = _infer_risk_level(module_report_markdown)
                risk_score = _infer_risk_score(module_report_markdown)
                if risk_level == "高":
                    high_risk_modules_counted += 1
                report_lines = [line for line in (module_report_markdown or "").splitlines() if line.strip()]
                modules.append({
                    "module_name": module_name,
                    "rank": rank,
                    "module_dir_path": str(module_dir),
                    "files_list_path": str(files_list_path),
                    "module_report_path": str(module_report_path),
                    "module_report_markdown": module_report_markdown,
                    "files": files,
                    "file_count": file_count,
                    "risk_level": risk_level,
                    "risk_score": risk_score,
                    "report_sections": _parse_report_sections(module_report_markdown),
                    "report_preview": "\n".join(report_lines[:12]) if report_lines else None,
                })

        summary = _parse_summary(final_report_markdown)
        if summary["module_count"] == 0 and modules:
            summary["module_count"] = len(modules)
        if summary["high_risk_module_count"] == 0 and high_risk_modules_counted:
            summary["high_risk_module_count"] = high_risk_modules_counted
        if summary["total_file_count"] == 0 and total_files_counted:
            summary["total_file_count"] = total_files_counted

        return {
            "task_id": row.task_id,
            "available": available,
            "status": row.status,
            "output_root": str(output_root) if output_root else None,
            "final_report_path": str(final_report_path) if final_report_path else None,
            "modules_list_path": str(modules_list_path) if modules_list_path else None,
            "final_report_markdown": final_report_markdown,
            "modules": modules,
            "summary": summary,
            "warnings": warnings,
        }

    def create_task(self, db: Session, *, project_id: str, task_name: str,
                    input_path: str, output_path: Optional[str] = None,
                    task_description: Optional[str] = None,
                    prompt_template_id: Optional[str] = None,
                    prompt_content: str, created_by: Optional[str] = None,
                    task_config_json: Optional[dict] = None,
                    task_origin_type: Optional[str] = None,
                    parent_project_id: Optional[str] = None,
                    parent_task_id: Optional[str] = None,
                    parent_task_type: Optional[str] = None,
                    parent_stage_name: Optional[str] = None,
                    parent_stage_item_id: Optional[str] = None,
                    parent_stage_item_key: Optional[str] = None) -> dict:
        task_id = f"sat_{uuid.uuid4().hex[:16]}"
        _fs_base = os.environ.get("FILESERVER_ROOT", "/data/files")
        effective_output = output_path or f"{_fs_base}/{project_id}/app/secflow-app-system-analyse"
        row = AppSaTask(
            task_id=task_id, project_id=project_id, task_name=task_name,
            task_description=task_description, input_path=input_path,
            output_path=effective_output, prompt_template_id=prompt_template_id,
            prompt_content=prompt_content, status="pending", created_by=created_by,
            task_config_json=task_config_json,
            task_origin_type=str(task_origin_type or "").strip() or "manual",
            parent_project_id=parent_project_id,
            parent_task_id=parent_task_id,
            parent_task_type=parent_task_type,
            parent_stage_name=parent_stage_name,
            parent_stage_item_id=parent_stage_item_id,
            parent_stage_item_key=parent_stage_item_key,
        )
        db.add(row); db.commit(); db.refresh(row)
        asyncio_task = asyncio.create_task(self._execute_task(task_id),
                                            name=f"sa_task_{task_id}")
        _running_tasks[task_id] = asyncio_task
        log_event(logger, logging.INFO, "task created",
                  event="task_created", task_id=task_id, project_id=project_id)
        return self._row_to_dict(row)

    def restart_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        if row.status in ("pending", "running"):
            from fastapi import HTTPException
            raise HTTPException(400, "任务仍在运行中，请先取消后再重启")
        # Reset in-place — strip any resume overrides from previous resume_task call
        from sqlalchemy.orm.attributes import flag_modified
        clean_config = {k: v for k, v in (row.task_config_json or {}).items()
                        if k not in ("start_stage", "resume_workspace")} or None
        row.task_config_json = clean_config
        row.status = "pending"
        row.started_at = None
        row.finished_at = None
        row.stages_json = None
        row.result_json = None
        row.error = None
        flag_modified(row, "task_config_json")
        db.commit(); db.refresh(row)
        # Clean up previous run directory so fresh execution starts from scratch
        if row.output_path:
            import shutil as _shutil
            task_root = os.path.join(row.output_path, task_id)
            if os.path.isdir(task_root):
                try:
                    _shutil.rmtree(task_root)
                except Exception as _e:
                    logger.warning("Failed to clean task dir %s: %s", task_root, _e)
        asyncio_task = asyncio.create_task(self._execute_task(task_id),
                                            name=f"sa_task_{task_id}")
        _running_tasks[task_id] = asyncio_task
        log_event(logger, logging.INFO, "task restarted in-place", event="task_restarted",
                  task_id=task_id, project_id=row.project_id)
        return self._row_to_dict(row)

    def resume_task(self, db: Session, task_id: str) -> dict:
        """从断点续跑：保留同一任务ID，跳过 Stage 1/2 直接从 Stage 3 开始。"""
        row = self._get_or_404(db, task_id)
        if row.status in ("pending", "running"):
            from fastapi import HTTPException
            raise HTTPException(400, "任务仍在运行中，请先取消后再续跑")
        from sqlalchemy.orm.attributes import flag_modified
        svc = _load_svc_config_from_db(db, row.project_id)
        effective_output = row.output_path or svc.output_dir
        resume_workspace = os.path.join(effective_output, task_id, "run", "workspace")
        tcfg = dict(row.task_config_json or {})
        tcfg["start_stage"] = 3
        tcfg["resume_workspace"] = resume_workspace
        row.task_config_json = tcfg
        row.status = "pending"
        # 保留 started_at 和 stages_json，使续跑后仍能看到前序阶段的时间与事件
        row.finished_at = None
        row.result_json = None
        row.error = None
        flag_modified(row, "task_config_json")
        db.commit(); db.refresh(row)
        asyncio_task = asyncio.create_task(self._execute_task(task_id),
                                            name=f"sa_task_{task_id}")
        _running_tasks[task_id] = asyncio_task
        log_event(logger, logging.INFO, "task resumed in-place", event="task_resumed",
                  task_id=task_id, project_id=row.project_id, resume_workspace=resume_workspace)
        return self._row_to_dict(row)

    def cancel_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        if row.status in ("passed", "failed", "error", "cancelled"):
            return self._row_to_dict(row)
        at = _running_tasks.get(task_id)
        if at and not at.done():
            at.cancel()
        row.status = "cancelled"
        row.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
        db.commit(); db.refresh(row)
        return self._row_to_dict(row)

    def delete_task(self, db: Session, task_id: str, *, delete_files: bool = True) -> None:
        """软删除任务记录，并可选删除输出目录下的任务文件。运行中任务不允许删除。"""
        import shutil as _shutil
        from fastapi import HTTPException
        row = self._get_or_404(db, task_id)
        # 运行中的任务必须先取消，不允许直接删除
        if row.status == "running":
            raise HTTPException(status_code=409, detail="任务正在运行，请先取消后再删除")
        # 删除输出文件
        if delete_files and row.output_path:
            task_dir = os.path.join(row.output_path, task_id)
            if os.path.isdir(task_dir):
                try:
                    _shutil.rmtree(task_dir)
                    logger.info("delete_task: removed task dir %s", task_dir)
                except Exception as _e:
                    logger.warning("delete_task: failed to remove %s: %s", task_dir, _e)
        # 软删除
        row.is_deleted = True
        db.commit()

    async def _execute_task(self, task_id: str) -> None:
        from app.db import get_db
        db_gen = get_db()
        db: Session = next(db_gen)
        event_buffer: list[dict] = []

        def on_event(event: SwarmEvent) -> None:
            event_buffer.append({"ts": _time.time(), "type": event.type,
                                  "data": dict(event.data)})
            n = len(event_buffer)
            if n == 1 or n % 3 == 0:
                _flush_stages(task_id, event_buffer)

        try:
            row = db.query(AppSaTask).filter_by(task_id=task_id).first()
            if not row or row.status == "cancelled":
                return
            row.status = "running"
            # 续跑时保留原始 started_at，首次运行才设置
            if row.started_at is None:
                row.started_at = datetime.now(timezone.utc).replace(tzinfo=None)
            db.commit()
            _write_models_json_from_db(db)
            svc = _load_svc_config_from_db(db, row.project_id)
            # Apply per-task config overrides (analyse_targets, binary_arch, etc.)
            tcfg = row.task_config_json or {}
            if tcfg.get("analyse_targets"):
                svc.analyse_targets = tcfg["analyse_targets"]
            if tcfg.get("binary_arch"):
                svc.binary_arch = tcfg["binary_arch"]
            # start_stage / resume_workspace come ONLY from task_config_json
            # (set by resume_task).  Never inherit from project config so that
            # fresh runs and restarts always start from Stage 0.
            svc.start_stage = tcfg["start_stage"] if tcfg.get("start_stage") else 0
            svc.resume_workspace = tcfg.get("resume_workspace") or ""
            # Use row.output_path as the working root so the Orchestrator writes to
            # the user-specified location ({output_path}/{task_id}/workspace/) rather
            # than the global /data/output directory from config.json.
            if row.output_path:
                svc.output_dir = row.output_path
                svc.archive_dir = row.output_path
                svc.result_dir = row.output_path
            cfg = build_task_config(svc, row.prompt_content, cwd=row.input_path)
            orch = Orchestrator(config=cfg, on_event=on_event)
            result = await orch.execute(task_id)
            _flush_stages(task_id, event_buffer)
            db.expire(row); db.refresh(row)
            if row.status == "cancelled":
                return
            row.status = result.status.value if result else "error"
            row.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
            # 合并历史事件（续跑场景保留前序阶段记录）
            _prev = row.stages_json
            _prev_events = _prev["events"] if isinstance(_prev, dict) and isinstance(_prev.get("events"), list) else []
            row.stages_json = {"events": _prev_events + event_buffer, "final": True}
            if result:
                row.result_json = result.model_dump(mode="json")
                if result.error:
                    row.error = result.error
            db.commit()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log_event(logger, logging.ERROR, "task execution failed",
                      event="task_error", task_id=task_id, error=str(exc))
            try:
                db.rollback()
                r = db.query(AppSaTask).filter_by(task_id=task_id).first()
                if r and r.status == "running":
                    r.status = "error"
                    r.error = str(exc)
                    r.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
                    _prev2 = r.stages_json
                    _prev_events2 = _prev2["events"] if isinstance(_prev2, dict) and isinstance(_prev2.get("events"), list) else []
                    r.stages_json = {"events": _prev_events2 + event_buffer, "final": True}
                    db.commit()
            except Exception:
                pass
        finally:
            _running_tasks.pop(task_id, None)
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _get_or_404(self, db: Session, task_id: str) -> AppSaTask:
        row = db.query(AppSaTask).filter(
            AppSaTask.task_id == task_id,
            AppSaTask.is_deleted.is_(False),
        ).first()
        if not row:
            from fastapi import HTTPException
            raise HTTPException(404, f"任务不存在: {task_id}")
        return row

    @staticmethod
    def _row_to_dict(row: AppSaTask) -> dict:
        def fmt(dt: datetime | None) -> str | None:
            return dt.isoformat() + "Z" if dt else None
        return {
            "task_id": row.task_id, "project_id": row.project_id,
            **_origin_payload(row),
            "task_name": row.task_name, "task_description": row.task_description,
            "input_path": row.input_path, "output_path": row.output_path,
            "prompt_template_id": row.prompt_template_id,
            "prompt_content": row.prompt_content, "status": row.status,
            "error": row.error, "result_json": row.result_json,
            "stages_json": row.stages_json,
            "task_config_json": row.task_config_json,
            "created_by": row.created_by,
            "created_at": fmt(row.created_at), "updated_at": fmt(row.updated_at),
            "started_at": fmt(row.started_at), "finished_at": fmt(row.finished_at),
        }


_task_service: TaskService | None = None


def get_task_service() -> TaskService:
    global _task_service
    if _task_service is None:
        _task_service = TaskService()
    return _task_service
