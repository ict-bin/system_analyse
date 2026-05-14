from __future__ import annotations

import json
import threading
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from .db.models import AppSaTask
from .service.task_service import get_worker_runtime_health, get_worker_runtime_settings

_REQUEST_LOCK = threading.Lock()
_REQUEST_TOTAL = defaultdict(int)
_REQUEST_DURATION = defaultdict(lambda: {"count": 0, "sum": 0.0})
_TERMINAL_STATUSES = {"passed", "failed", "error", "cancelled"}


def observe_request(method: str, path: str, status_code: int, duration_seconds: float) -> None:
    key = (method.upper(), path or "/", str(int(status_code)))
    with _REQUEST_LOCK:
        _REQUEST_TOTAL[key] += 1
        bucket = _REQUEST_DURATION[key]
        bucket["count"] += 1
        bucket["sum"] += max(0.0, float(duration_seconds))


def render_metrics() -> str:
    lines = ["# HELP secflow_sa_up Service metrics scrape succeeded.", "# TYPE secflow_sa_up gauge"]
    try:
        lines.append("secflow_sa_up 1")
        lines.extend(_render_request_metrics())
        lines.extend(_render_task_metrics())
    except Exception:
        lines.append("secflow_sa_up 0")
    return "\n".join(lines) + "\n"


def _render_request_metrics() -> list[str]:
    lines = [
        "# HELP secflow_sa_api_requests_total Total API requests observed by this process.",
        "# TYPE secflow_sa_api_requests_total counter",
        "# HELP secflow_sa_api_request_duration_seconds API request duration in seconds.",
        "# TYPE secflow_sa_api_request_duration_seconds summary",
    ]
    with _REQUEST_LOCK:
        totals = dict(_REQUEST_TOTAL)
        durations = {key: dict(value) for key, value in _REQUEST_DURATION.items()}
    for key in sorted(set(totals) | set(durations)):
        method, path, status = key
        labels = _labels(method=method, path=path, status=status)
        lines.append(f"secflow_sa_api_requests_total{labels} {totals.get(key, 0)}")
        duration = durations.get(key, {"count": 0, "sum": 0.0})
        lines.append(f"secflow_sa_api_request_duration_seconds_count{labels} {int(duration['count'])}")
        lines.append(f"secflow_sa_api_request_duration_seconds_sum{labels} {_fmt(duration['sum'])}")
    return lines


def _render_task_metrics() -> list[str]:
    from .db import get_db

    db_up = 0
    rows: list[AppSaTask] = []
    try:
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            rows = db.query(AppSaTask).filter(AppSaTask.is_deleted.is_(False)).all()
            db_up = 1
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
    except Exception:
        rows = []

    status_counts: dict[str, int] = defaultdict(int)
    queue_count = turnaround_count = execution_count = 0
    queue_sum = turnaround_sum = execution_sum = 0.0
    token_input_total = token_output_total = token_cache_read_total = token_cache_write_total = 0
    token_cost_total = 0.0
    token_input_running = token_output_running = 0
    token_cost_running = 0.0
    retry_total = timeout_total = cancel_total = 0
    failure_category_counts: dict[str, int] = defaultdict(int)
    worker_session_gauge = judge_session_gauge = total_session_gauge = 0
    stage_duration: dict[tuple[str, str], float] = defaultdict(float)
    stage_rounds: dict[tuple[str, str], int] = defaultdict(int)
    stage_tokens: dict[tuple[str, str], int] = defaultdict(int)
    stage_cost: dict[tuple[str, str], float] = defaultdict(float)

    for row in rows:
        status = str(row.status or "unknown")
        status_counts[status] += 1
        if row.started_at and row.created_at:
            queue_sum += _seconds_between(row.created_at, row.started_at)
            queue_count += 1
        if row.finished_at and row.created_at:
            turnaround_sum += _seconds_between(row.created_at, row.finished_at)
            turnaround_count += 1
        result_json = row.result_json if isinstance(row.result_json, dict) else {}
        total_tokens = result_json.get("total_tokens") if isinstance(result_json.get("total_tokens"), dict) else {}
        usage = _token_usage(total_tokens)
        token_input_total += usage["input"]
        token_output_total += usage["output"]
        token_cache_read_total += usage["cache_read"]
        token_cache_write_total += usage["cache_write"]
        token_cost_total += usage["cost"]
        if status == "running":
            token_input_running += usage["input"]
            token_output_running += usage["output"]
            token_cost_running += usage["cost"]
        execution_seconds = 0.0
        if row.started_at and row.finished_at:
            execution_seconds = _seconds_between(row.started_at, row.finished_at)
        elif result_json.get("total_duration_ms") is not None:
            execution_seconds = max(0.0, float(result_json.get("total_duration_ms") or 0.0) / 1000.0)
        if execution_seconds > 0:
            execution_sum += execution_seconds
            execution_count += 1

        eval_summary = _load_json(_task_run_root(row) / "evaluation_summary.json")
        eval_records = _load_stage_records(_task_run_root(row))
        if isinstance(eval_summary, dict):
            retry_total += max(0, int(eval_summary.get("round_count") or 0) - int(eval_summary.get("module_count") or 0))
        for record in eval_records:
            stage = str(record.get("stage") or "unknown")
            stage_status = str(record.get("status") or "unknown")
            key = (stage, stage_status)
            stage_duration[key] += max(0.0, float(record.get("duration_ms") or 0.0) / 1000.0)
            stage_rounds[key] += 1
            metrics = record.get("metrics") if isinstance(record.get("metrics"), dict) else {}
            stage_tokens[key] += int(metrics.get("token_total") or 0)
            stage_cost[key] += float(metrics.get("cost") or 0.0)
            worker = record.get("worker") if isinstance(record.get("worker"), dict) else {}
            if worker.get("session_file"):
                worker_session_gauge += 1
                total_session_gauge += 1
            judges = record.get("judges") if isinstance(record.get("judges"), list) else []
            judge_session_gauge += len(judges)
            total_session_gauge += sum(1 for judge in judges if isinstance(judge, dict) and judge.get("session_file"))
        total_session_gauge += _count_session_files(_task_run_root(row) / "sessions")

        classification = _classify_failure(row.error, result_json)
        if classification == "timeout":
            timeout_total += 1
        if classification == "cancel":
            cancel_total += 1
        if classification != "none":
            failure_category_counts[classification] += 1

    worker_health = get_worker_runtime_health()
    worker_settings = get_worker_runtime_settings()
    worker_gauge = max(
        int(worker_settings.get("worker_task_concurrency") or 0),
        int(worker_health.get("running_task_count") or 0),
        1 if rows else 0,
    )

    lines = [
        "# HELP secflow_sa_db_up Database query path for metrics is available.",
        "# TYPE secflow_sa_db_up gauge",
        f"secflow_sa_db_up {db_up}",
        "# HELP secflow_sa_tasks_status Number of tasks by status.",
        "# TYPE secflow_sa_tasks_status gauge",
    ]
    for status in sorted(status_counts):
        lines.append(f"secflow_sa_tasks_status{_labels(status=status)} {status_counts[status]}")
    finished_count = sum(count for status, count in status_counts.items() if status in _TERMINAL_STATUSES)
    lines.extend([
        "# HELP secflow_sa_tasks_pending Pending tasks.",
        "# TYPE secflow_sa_tasks_pending gauge",
        f"secflow_sa_tasks_pending {status_counts.get('pending', 0)}",
        "# HELP secflow_sa_tasks_running Running tasks.",
        "# TYPE secflow_sa_tasks_running gauge",
        f"secflow_sa_tasks_running {status_counts.get('running', 0)}",
        "# HELP secflow_sa_tasks_finished Finished tasks.",
        "# TYPE secflow_sa_tasks_finished gauge",
        f"secflow_sa_tasks_finished {finished_count}",
        "# HELP secflow_sa_queue_wait_seconds Queue wait duration aggregated over tasks.",
        "# TYPE secflow_sa_queue_wait_seconds summary",
        f"secflow_sa_queue_wait_seconds_count {queue_count}",
        f"secflow_sa_queue_wait_seconds_sum {_fmt(queue_sum)}",
        "# HELP secflow_sa_execution_seconds Execution duration aggregated over tasks.",
        "# TYPE secflow_sa_execution_seconds summary",
        f"secflow_sa_execution_seconds_count {execution_count}",
        f"secflow_sa_execution_seconds_sum {_fmt(execution_sum)}",
        "# HELP secflow_sa_turnaround_seconds End-to-end turnaround duration aggregated over tasks.",
        "# TYPE secflow_sa_turnaround_seconds summary",
        f"secflow_sa_turnaround_seconds_count {turnaround_count}",
        f"secflow_sa_turnaround_seconds_sum {_fmt(turnaround_sum)}",
        "# HELP secflow_sa_workers Number of worker slots or active workers.",
        "# TYPE secflow_sa_workers gauge",
        f"secflow_sa_workers {worker_gauge}",
        "# HELP secflow_sa_judges Aggregated judge session count.",
        "# TYPE secflow_sa_judges gauge",
        f"secflow_sa_judges {judge_session_gauge}",
        "# HELP secflow_sa_sessions Aggregated session file count.",
        "# TYPE secflow_sa_sessions gauge",
        f"secflow_sa_sessions {total_session_gauge}",
        "# HELP secflow_sa_retry_total Aggregated retry or reflection count.",
        "# TYPE secflow_sa_retry_total counter",
        f"secflow_sa_retry_total {retry_total}",
        "# HELP secflow_sa_timeout_total Timeout-classified terminal tasks.",
        "# TYPE secflow_sa_timeout_total counter",
        f"secflow_sa_timeout_total {timeout_total}",
        "# HELP secflow_sa_cancel_total Cancelled tasks.",
        "# TYPE secflow_sa_cancel_total counter",
        f"secflow_sa_cancel_total {cancel_total}",
        "# HELP secflow_sa_failure_category_total Terminal tasks classified by failure category.",
        "# TYPE secflow_sa_failure_category_total counter",
    ])
    for category in sorted(failure_category_counts):
        lines.append(f"secflow_sa_failure_category_total{_labels(category=category)} {failure_category_counts[category]}")
    lines.extend([
        "# HELP secflow_sa_token_input_total Aggregated input tokens.",
        "# TYPE secflow_sa_token_input_total counter",
        f"secflow_sa_token_input_total {token_input_total}",
        "# HELP secflow_sa_token_output_total Aggregated output tokens.",
        "# TYPE secflow_sa_token_output_total counter",
        f"secflow_sa_token_output_total {token_output_total}",
        "# HELP secflow_sa_token_cost_total Aggregated token cost.",
        "# TYPE secflow_sa_token_cost_total counter",
        f"secflow_sa_token_cost_total {_fmt(token_cost_total)}",
        "# HELP secflow_sa_token_input_running Current running-task input tokens snapshot.",
        "# TYPE secflow_sa_token_input_running gauge",
        f"secflow_sa_token_input_running {token_input_running}",
        "# HELP secflow_sa_token_output_running Current running-task output tokens snapshot.",
        "# TYPE secflow_sa_token_output_running gauge",
        f"secflow_sa_token_output_running {token_output_running}",
        "# HELP secflow_sa_token_cost_running Current running-task token cost snapshot.",
        "# TYPE secflow_sa_token_cost_running gauge",
        f"secflow_sa_token_cost_running {_fmt(token_cost_running)}",
        "# HELP secflow_sa_stage_duration_seconds Aggregated stage duration by stage and status.",
        "# TYPE secflow_sa_stage_duration_seconds gauge",
        "# HELP secflow_sa_stage_rounds Aggregated stage round count by stage and status.",
        "# TYPE secflow_sa_stage_rounds gauge",
        "# HELP secflow_sa_stage_token_total Aggregated stage tokens by stage and status.",
        "# TYPE secflow_sa_stage_token_total gauge",
        "# HELP secflow_sa_stage_cost_total Aggregated stage cost by stage and status.",
        "# TYPE secflow_sa_stage_cost_total gauge",
    ])
    for key in sorted(set(stage_rounds) | set(stage_duration) | set(stage_tokens) | set(stage_cost)):
        stage, stage_status = key
        labels = _labels(stage=stage, status=stage_status)
        lines.append(f"secflow_sa_stage_duration_seconds{labels} {_fmt(stage_duration.get(key, 0.0))}")
        lines.append(f"secflow_sa_stage_rounds{labels} {stage_rounds.get(key, 0)}")
        lines.append(f"secflow_sa_stage_token_total{labels} {stage_tokens.get(key, 0)}")
        lines.append(f"secflow_sa_stage_cost_total{labels} {_fmt(stage_cost.get(key, 0.0))}")
    _append_ai_alias_metrics(
        lines,
        prefix="secflow_sa",
        worker_count=worker_gauge,
        judge_count=judge_session_gauge,
        session_total=total_session_gauge,
        round_total=sum(stage_rounds.values()),
        retry_total=retry_total,
        timeout_total=timeout_total,
        cancel_total=cancel_total,
        failure_category_counts=failure_category_counts,
        token_input_total=token_input_total,
        token_output_total=token_output_total,
        token_cache_read_total=token_cache_read_total,
        token_cache_write_total=token_cache_write_total,
        token_cost_total=token_cost_total,
        review_pass_total=finished_count - sum(failure_category_counts.values()),
        review_fail_total=sum(failure_category_counts.values()),
        worker_duration_seconds=execution_sum,
        judge_duration_seconds=sum(stage_cost.values()) * 0.0,
    )
    return lines


def _task_run_root(row: AppSaTask) -> Path | None:
    if not row.output_path:
        return None
    return Path(row.output_path) / row.task_id / "run"


def _load_json(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _load_stage_records(run_root: Path | None) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if run_root is None or not run_root.is_dir():
        return records
    for round_dir in sorted(run_root.glob("round_*")):
        if not round_dir.is_dir():
            continue
        for path in sorted(round_dir.glob("*.json")):
            if path.name.endswith(".tmp"):
                continue
            payload = _load_json(path)
            if isinstance(payload, dict):
                records.append(payload)
    return records


def _count_session_files(path: Path | None) -> int:
    if path is None or not path.is_dir():
        return 0
    return sum(1 for item in path.rglob("*.jsonl") if item.is_file())


def _classify_failure(error: Any, result_json: dict[str, Any]) -> str:
    status = str(result_json.get("status") or result_json.get("analysis_status") or "").lower()
    reason = str(result_json.get("completion_reason") or error or "").lower()
    text = f"{status} {reason}"
    if "cancel" in text:
        return "cancel"
    if "timeout" in text or "timed out" in text or "deadline" in text:
        return "timeout"
    if "lease" in text:
        return "lease_lost"
    if "validation" in text or "invalid" in text:
        return "validation"
    if "error" in text:
        return "error"
    if "failed" in text:
        return "failed"
    return "none"


def _token_usage(value: dict[str, Any] | None) -> dict[str, int | float]:
    usage = value if isinstance(value, dict) else {}
    return {
        "input": int(usage.get("input", 0) or usage.get("prompt_tokens", 0) or 0),
        "output": int(usage.get("output", 0) or usage.get("completion_tokens", 0) or 0),
        "cache_read": int(usage.get("cache_read", 0) or 0),
        "cache_write": int(usage.get("cache_write", 0) or 0),
        "cost": float(usage.get("cost", 0.0) or 0.0),
    }


def _seconds_between(start: datetime | None, end: datetime | None) -> float:
    if not start or not end:
        return 0.0
    return max(0.0, (end - start).total_seconds())


def _labels(**labels: Any) -> str:
    parts = []
    for key, value in labels.items():
        safe = str(value).replace("\\", "\\\\").replace("\n", "\\n").replace("\"", "\\\"")
        parts.append(f'{key}="{safe}"')
    return "{" + ",".join(parts) + "}" if parts else ""


def _fmt(value: float) -> str:
    return f"{float(value):.6f}"


def _append_ai_alias_metrics(
    lines: list[str],
    *,
    prefix: str,
    worker_count: int,
    judge_count: int,
    session_total: int,
    round_total: int,
    retry_total: int,
    timeout_total: int,
    cancel_total: int,
    failure_category_counts: dict[str, int],
    token_input_total: int,
    token_output_total: int,
    token_cache_read_total: int,
    token_cache_write_total: int,
    token_cost_total: float,
    review_pass_total: int,
    review_fail_total: int,
    worker_duration_seconds: float,
    judge_duration_seconds: float,
) -> None:
    lines.extend([
        f"# HELP {prefix}_ai_role_count Aggregated AI role counts for this service.",
        f"# TYPE {prefix}_ai_role_count gauge",
        f"# HELP {prefix}_ai_role_duration_seconds Aggregated AI role duration in seconds.",
        f"# TYPE {prefix}_ai_role_duration_seconds gauge",
        f"# HELP {prefix}_ai_session_total Aggregated AI session count by role.",
        f"# TYPE {prefix}_ai_session_total counter",
        f"# HELP {prefix}_ai_round_total Aggregated AI round counts by kind.",
        f"# TYPE {prefix}_ai_round_total counter",
        f"# HELP {prefix}_ai_retry_total Aggregated AI retry counts by reason.",
        f"# TYPE {prefix}_ai_retry_total counter",
        f"# HELP {prefix}_ai_timeout_total Aggregated AI timeout counts by scope.",
        f"# TYPE {prefix}_ai_timeout_total counter",
        f"# HELP {prefix}_ai_failure_total Aggregated AI failures by category.",
        f"# TYPE {prefix}_ai_failure_total counter",
        f"# HELP {prefix}_ai_token_usage_total Aggregated AI token usage by type.",
        f"# TYPE {prefix}_ai_token_usage_total counter",
        f"# HELP {prefix}_ai_token_cost_total Aggregated AI token cost.",
        f"# TYPE {prefix}_ai_token_cost_total counter",
        f"# HELP {prefix}_ai_review_total Aggregated AI review outcomes.",
        f"# TYPE {prefix}_ai_review_total counter",
    ])
    lines.append(f'{prefix}_ai_role_count{{role="worker"}} {max(0, int(worker_count))}')
    lines.append(f'{prefix}_ai_role_count{{role="judge"}} {max(0, int(judge_count))}')
    lines.append(f'{prefix}_ai_role_duration_seconds{{role="worker"}} {_fmt(worker_duration_seconds)}')
    lines.append(f'{prefix}_ai_role_duration_seconds{{role="judge"}} {_fmt(judge_duration_seconds)}')
    lines.append(f'{prefix}_ai_session_total{{role="worker"}} {max(0, int(worker_count))}')
    lines.append(f'{prefix}_ai_session_total{{role="judge"}} {max(0, int(judge_count))}')
    lines.append(f'{prefix}_ai_session_total{{role="agent"}} {max(0, int(session_total))}')
    lines.append(f'{prefix}_ai_round_total{{kind="round"}} {max(0, int(round_total))}')
    lines.append(f'{prefix}_ai_retry_total{{reason="reflection"}} {max(0, int(retry_total))}')
    lines.append(f'{prefix}_ai_timeout_total{{scope="task"}} {max(0, int(timeout_total))}')
    lines.append(f'{prefix}_ai_failure_total{{category="cancel"}} {max(0, int(cancel_total))}')
    for category in sorted(failure_category_counts):
        lines.append(f'{prefix}_ai_failure_total{{category="{category}"}} {max(0, int(failure_category_counts[category]))}')
    total_tokens = token_input_total + token_output_total + token_cache_read_total + token_cache_write_total
    lines.append(f'{prefix}_ai_token_usage_total{{type="input"}} {max(0, int(token_input_total))}')
    lines.append(f'{prefix}_ai_token_usage_total{{type="output"}} {max(0, int(token_output_total))}')
    lines.append(f'{prefix}_ai_token_usage_total{{type="cache_read"}} {max(0, int(token_cache_read_total))}')
    lines.append(f'{prefix}_ai_token_usage_total{{type="cache_write"}} {max(0, int(token_cache_write_total))}')
    lines.append(f'{prefix}_ai_token_usage_total{{type="total"}} {max(0, int(total_tokens))}')
    lines.append(f"{prefix}_ai_token_cost_total {_fmt(token_cost_total)}")
    lines.append(f'{prefix}_ai_review_total{{result="pass"}} {max(0, int(review_pass_total))}')
    lines.append(f'{prefix}_ai_review_total{{result="fail"}} {max(0, int(review_fail_total))}')
