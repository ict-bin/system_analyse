from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from sqlalchemy.orm import Session

from app.db.models import AppSaTask
from app.service.session_index import build_session_catalog


def _normalize_evaluation_round_status(payload: dict) -> dict:
    normalized = dict(payload)
    raw_status = str(normalized.get("status") or "").strip()
    ended_at = normalized.get("ended_at")
    completion_reason = str(normalized.get("completion_reason") or "").strip()
    module_completed = bool(normalized.get("module_completed"))
    metrics = normalized.get("metrics") if isinstance(normalized.get("metrics"), dict) else {}
    passed_by_vote = bool(metrics.get("passed_by_vote"))

    effective_status = raw_status
    if raw_status == "running" and ended_at:
        if completion_reason == "reclassify_required":
            effective_status = "reclassify_required"
        elif module_completed or completion_reason in {"passed", "max_rounds_exceeded_treated_as_passed"}:
            effective_status = "passed"
        elif passed_by_vote:
            effective_status = "needs_reflection"
        else:
            effective_status = "needs_retry"

    normalized["raw_status"] = raw_status
    normalized["status"] = effective_status
    return normalized


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


_DISCOVERED_MODULE_COUNT_RE = re.compile(r"已发现\s*(\d+)\s*个模块")


def _resolve_enable_final_check(row: AppSaTask) -> bool | None:
    raw_task_config = getattr(row, "task_config_json", None)
    task_config = raw_task_config if isinstance(raw_task_config, dict) else {}
    snapshot = task_config.get("resolved_config_snapshot") if isinstance(task_config.get("resolved_config_snapshot"), dict) else None
    if snapshot and "enable_final_check" in snapshot:
        return bool(snapshot.get("enable_final_check"))
    if "enable_final_check" in task_config:
        return bool(task_config.get("enable_final_check"))
    return None


def _compute_missing_files(workspace_root: Path) -> tuple[list[str], list[str]]:
    warnings: list[str] = []
    filtered_files_path = workspace_root / "filtered_files.txt"
    modules_root = workspace_root / "modules"

    if not filtered_files_path.is_file():
        warnings.append("filtered_files.txt 缺失，无法计算遗漏文件")
        return [], warnings

    try:
        all_target = {
            line.strip()
            for line in filtered_files_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
    except Exception as exc:
        warnings.append(f"filtered_files.txt 读取失败: {exc}")
        return [], warnings

    if not modules_root.exists() or not modules_root.is_dir():
        warnings.append("modules 目录缺失，无法计算遗漏文件")
        return [], warnings

    classified_files: set[str] = set()
    module_dirs = sorted(path for path in modules_root.iterdir() if path.is_dir() and not path.name.startswith("."))
    if not module_dirs:
        warnings.append("modules 目录为空，无法计算遗漏文件")
        return [], warnings

    files_list_found = False
    for module_dir in module_dirs:
        files_list_path = module_dir / "files.list"
        if not files_list_path.is_file():
            warnings.append(f"模块 {module_dir.name} 缺失 files.list")
            continue
        files_list_found = True
        try:
            lines = [line.strip() for line in files_list_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            if not lines:
                warnings.append(f"模块 {module_dir.name} 的 files.list 为空")
                continue
            for normalized in lines:
                classified_files.add(normalized)
        except Exception as exc:
            warnings.append(f"{files_list_path.relative_to(workspace_root)} 读取失败: {exc}")

    if not files_list_found:
        warnings.append("未发现任何模块 files.list，无法计算遗漏文件")
        return [], warnings

    return sorted(all_target - classified_files), warnings


class TaskQueryService:
    def __init__(
        self,
        *,
        get_or_404: Callable[[Session, str], AppSaTask],
        read_text_if_exists: Callable[[Path], tuple[str | None, str | None]],
        infer_risk_level: Callable[[str | None], str | None],
        infer_risk_score: Callable[[str | None], int | None],
        parse_report_sections: Callable[[str | None], list[dict]],
        parse_summary: Callable[[str | None], dict],
        task_sessions_root: Callable[[AppSaTask], Path | None],
        task_run_root: Callable[[AppSaTask], Path | None],
        resolve_session_path: Callable[[Path, str], Path],
        parse_session_jsonl_file: Callable[[Path], tuple[dict, list[dict], list[str], int]],
        write_json_atomic: Callable[[Path, dict], None],
    ) -> None:
        self._get_or_404 = get_or_404
        self._read_text_if_exists = read_text_if_exists
        self._infer_risk_level = infer_risk_level
        self._infer_risk_score = infer_risk_score
        self._parse_report_sections = parse_report_sections
        self._parse_summary = parse_summary
        self._task_sessions_root = task_sessions_root
        self._task_run_root = task_run_root
        self._resolve_session_path = resolve_session_path
        self._parse_session_jsonl_file = parse_session_jsonl_file
        self._write_json_atomic = write_json_atomic

    @staticmethod
    def _parse_discovered_module_count(final_report_markdown: str | None) -> int | None:
        if not final_report_markdown:
            return None
        match = _DISCOVERED_MODULE_COUNT_RE.search(final_report_markdown)
        if not match:
            return None
        try:
            return int(match.group(1))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _load_evaluation_summary_module_count(run_root: Path | None, warnings: list[str]) -> int | None:
        if not run_root or not run_root.is_dir():
            return None
        summary_path = run_root / "evaluation_summary.json"
        if not summary_path.exists():
            return None
        try:
            loaded = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception as exc:
            warnings.append(f"evaluation_summary.json 读取失败: {exc}")
            return None
        if not isinstance(loaded, dict):
            warnings.append("evaluation_summary.json 格式不是对象")
            return None
        try:
            module_count = int(loaded.get("module_count") or 0)
        except (TypeError, ValueError):
            return None
        return module_count if module_count > 0 else None

    @staticmethod
    def _count_workspace_modules(run_root: Path | None, warnings: list[str]) -> int | None:
        if not run_root or not run_root.is_dir():
            return None
        modules_root = run_root / "workspace" / "modules"
        if not modules_root.exists() or not modules_root.is_dir():
            return None
        try:
            count = sum(
                1
                for path in modules_root.iterdir()
                if path.is_dir() and not path.name.startswith(".")
            )
        except Exception as exc:
            warnings.append(f"workspace/modules 统计失败: {exc}")
            return None
        return count if count > 0 else None

    def get_task_result(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        output_root = Path(row.output_path or "") / row.task_id / "output" if row.output_path else None
        run_root = self._task_run_root(row)
        final_report_path = output_root / "final_report.md" if output_root else None
        modules_list_path = output_root / "modules.list" if output_root else None
        modules_root = output_root / "modules" if output_root else None
        warnings: list[str] = []

        final_report_markdown: str | None = None
        if final_report_path:
            final_report_markdown, err = self._read_text_if_exists(final_report_path)
            if err:
                warnings.append(err)

        modules_order: list[str] = []
        if modules_list_path:
            modules_list_markdown, err = self._read_text_if_exists(modules_list_path)
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

                files_list_content, files_err = self._read_text_if_exists(files_list_path)
                if files_err:
                    warnings.append(f"{module_name}: {files_err}")
                module_report_markdown, report_err = self._read_text_if_exists(module_report_path)
                if report_err:
                    warnings.append(f"{module_name}: {report_err}")

                files = [line.strip() for line in (files_list_content or "").splitlines() if line.strip()]
                file_count = len(files)
                if file_count == 0:
                    warnings.append(f"{module_name}: files.list 为空，跳过该无效模块")
                    continue
                total_files_counted += file_count
                risk_level = self._infer_risk_level(module_report_markdown)
                risk_score = self._infer_risk_score(module_report_markdown)
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
                    "report_sections": self._parse_report_sections(module_report_markdown),
                    "report_preview": "\n".join(report_lines[:12]) if report_lines else None,
                })

        summary = self._parse_summary(final_report_markdown)
        if summary["module_count"] == 0:
            report_module_count = self._parse_discovered_module_count(final_report_markdown)
            if report_module_count:
                summary["module_count"] = report_module_count
        if summary["module_count"] == 0 and modules:
            summary["module_count"] = len(modules)
        if summary["module_count"] == 0:
            evaluation_module_count = self._load_evaluation_summary_module_count(run_root, warnings)
            if evaluation_module_count:
                summary["module_count"] = evaluation_module_count
        if summary["module_count"] == 0:
            workspace_module_count = self._count_workspace_modules(run_root, warnings)
            if workspace_module_count:
                summary["module_count"] = workspace_module_count
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

    def _build_session_catalog(self, row: AppSaTask) -> dict:
        sessions_root = self._task_sessions_root(row)
        run_root = self._task_run_root(row)
        if not sessions_root or not sessions_root.is_dir() or not run_root or not run_root.is_dir():
            return {
                "task_id": row.task_id,
                "status": row.status,
                "sessions_root": str(sessions_root) if sessions_root else None,
                "index_path": str((sessions_root / "index.json")) if sessions_root else None,
                "generated_at": None,
                "items": [],
                "index": None,
                "warnings": [],
            }
        return build_session_catalog(
            task_id=row.task_id,
            row_status=row.status,
            sessions_root=sessions_root,
            run_root=run_root,
            parse_session_jsonl_file=self._parse_session_jsonl_file,
            write_json_atomic=self._write_json_atomic,
        )

    def list_task_sessions(self, db: Session, task_id: str) -> list[dict]:
        row = self._get_or_404(db, task_id)
        return self._build_session_catalog(row).get("items") or []

    def get_task_session_index(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        catalog = self._build_session_catalog(row)
        return {
            "task_id": catalog.get("task_id"),
            "status": catalog.get("status"),
            "sessions_root": catalog.get("sessions_root"),
            "index_path": catalog.get("index_path"),
            "generated_at": catalog.get("generated_at"),
            "summary": (catalog.get("index") or {}).get("summary") or {},
            "nodes": (catalog.get("index") or {}).get("nodes") or [],
            "edges": (catalog.get("index") or {}).get("edges") or [],
            "groups": (catalog.get("index") or {}).get("groups") or [],
            "warnings": list(dict.fromkeys((catalog.get("warnings") or []) + (((catalog.get("index") or {}).get("warnings")) or []))),
        }

    def get_task_session_file(self, db: Session, task_id: str, relative_path: str) -> dict:
        row = self._get_or_404(db, task_id)
        sessions_root = self._task_sessions_root(row)
        if not sessions_root or not sessions_root.is_dir():
            from fastapi import HTTPException
            raise HTTPException(404, "会话目录不存在")
        try:
            target = self._resolve_session_path(sessions_root, relative_path)
        except ValueError as exc:
            from fastapi import HTTPException
            raise HTTPException(400, str(exc))
        if not target.is_file():
            from fastapi import HTTPException
            raise HTTPException(404, f"会话文件不存在: {relative_path}")
        session_meta, events, warnings, line_count = self._parse_session_jsonl_file(target)
        return {
            "path": str(target.relative_to(sessions_root)).replace("\\", "/"),
            "session_meta": session_meta,
            "events": events,
            "warnings": warnings,
            "line_count": line_count,
        }

    def get_task_evaluation(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        run_root = self._task_run_root(row)
        warnings: list[str] = []
        if not run_root or not run_root.is_dir():
            return {
                "task_id": row.task_id,
                "status": row.status,
                "available": False,
                "summary": None,
                "rounds": [],
                "warnings": warnings,
            }

        summary: dict | None = None
        summary_path = run_root / "evaluation_summary.json"
        if summary_path.exists():
            try:
                loaded = json.loads(summary_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    summary = loaded
                else:
                    warnings.append("evaluation_summary.json 格式不是对象")
            except Exception as exc:
                warnings.append(f"evaluation_summary.json 读取失败: {exc}")

        final_check_enabled = _resolve_enable_final_check(row)
        if final_check_enabled is False:
            if summary is None:
                summary = {}
            workspace_root = run_root / "workspace"
            if not workspace_root.exists() or not workspace_root.is_dir():
                warnings.append("workspace 目录缺失，无法计算遗漏文件")
                missing_files: list[str] = []
            else:
                missing_files, missing_warnings = _compute_missing_files(workspace_root)
                warnings.extend(missing_warnings)
            summary.update({
                "final_check_disabled": True,
                "missing_file_count": len(missing_files),
                "missing_files": missing_files,
                "missing_files_preview": missing_files[:20],
                "missing_files_computed_at": _utc_now_iso(),
            })

        rounds: list[dict] = []
        for round_dir in sorted(run_root.glob("round_*")):
            if not round_dir.is_dir():
                continue
            for path in sorted(round_dir.glob("*.json")):
                if path.name.endswith(".tmp"):
                    continue
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                except Exception as exc:
                    warnings.append(f"{path.relative_to(run_root)} 读取失败: {exc}")
                    continue
                if not isinstance(payload, dict):
                    warnings.append(f"{path.relative_to(run_root)} 格式不是对象")
                    continue
                payload = _normalize_evaluation_round_status(payload)
                payload.setdefault("source_path", str(path))
                rounds.append(payload)

        rounds.sort(key=lambda item: (
            int(item.get("round") or 0),
            str(item.get("module_name") or ""),
            str(item.get("stage") or ""),
        ))
        return {
            "task_id": row.task_id,
            "status": row.status,
            "available": bool(summary or rounds),
            "summary": summary,
            "rounds": rounds,
            "warnings": warnings,
        }
