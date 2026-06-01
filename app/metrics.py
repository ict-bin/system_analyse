from __future__ import annotations

import json
import re
import threading
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from .db.models import AppSaTask
from .service.task_service import get_worker_runtime_health, get_worker_runtime_settings
from .service.worker_slot_snapshot import build_worker_slot_cluster_snapshot

_REQUEST_LOCK = threading.Lock()
_HTTP_REQUEST_TOTAL = defaultdict(int)
_HTTP_REQUEST_DURATION = defaultdict(lambda: {"count": 0, "sum": 0.0, "buckets": [0] * 13})
_HTTP_REQUEST_INFLIGHT = defaultdict(int)
_TERMINAL_STATUSES = {"passed", "failed", "error", "cancelled"}
_HTTP_DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)
_PATH_ID_SEGMENT_RE = re.compile(r"/(?:\d+|[0-9a-f]{8,}|[0-9a-f]{8}-[0-9a-f-]{27,})(?=/|$)", re.IGNORECASE)


def normalize_http_route(path: str | None) -> str:
    raw = str(path or "/").strip() or "/"
    return _PATH_ID_SEGMENT_RE.sub("/{id}", raw)


def http_status_class(status_code: int | str | None) -> str:
    try:
        code = int(status_code or 500)
    except (TypeError, ValueError):
        code = 500
    if code < 0:
        return "cancelled"
    return f"{code // 100}xx"


def observe_http_request_inflight(method: str, route: str, delta: int) -> None:
    key = (str(method or "GET").upper(), normalize_http_route(route))
    with _REQUEST_LOCK:
        _HTTP_REQUEST_INFLIGHT[key] += int(delta)
        if _HTTP_REQUEST_INFLIGHT[key] < 0:
            _HTTP_REQUEST_INFLIGHT[key] = 0


def observe_http_request(method: str, path: str, status_code: int, duration_seconds: float) -> None:
    normalized_route = normalize_http_route(path)
    http_key = (method.upper(), normalized_route, http_status_class(status_code), str(int(status_code)))
    duration_key = (method.upper(), normalized_route)
    with _REQUEST_LOCK:
        _HTTP_REQUEST_TOTAL[http_key] += 1
        duration_bucket = _HTTP_REQUEST_DURATION[duration_key]
        duration_bucket["count"] += 1
        duration_bucket["sum"] += max(0.0, float(duration_seconds))
        for index, upper_bound in enumerate(_HTTP_DURATION_BUCKETS):
            if duration_seconds <= upper_bound:
                duration_bucket["buckets"][index] += 1


def render_metrics() -> str:
    lines = ["# HELP secflow_sa_up Service metrics scrape succeeded.", "# TYPE secflow_sa_up gauge"]
    try:
        lines.append("secflow_sa_up 1")
        lines.extend(_render_request_metrics())
        lines.extend(_render_task_metrics())
        lines.extend(_render_agent_observability_metrics())
    except Exception:
        lines.append("secflow_sa_up 0")
    return "\n".join(lines) + "\n"


def render_summary_metrics() -> str:
    lines = ["# HELP secflow_sa_up Service metrics scrape succeeded.", "# TYPE secflow_sa_up gauge"]
    try:
        lines.append("secflow_sa_up 1")
        lines.extend(_render_request_metrics())
        lines.extend(_render_agent_observability_metrics())
    except Exception:
        lines.append("secflow_sa_up 0")
    return "\n".join(lines) + "\n"


def _render_request_metrics() -> list[str]:
    lines = [
        "# HELP secflow_system_analyse_http_requests_total Total normalized HTTP requests observed by this process.",
        "# TYPE secflow_system_analyse_http_requests_total counter",
        "# HELP secflow_system_analyse_http_request_duration_seconds Normalized HTTP request duration in seconds.",
        "# TYPE secflow_system_analyse_http_request_duration_seconds histogram",
        "# HELP secflow_system_analyse_http_request_inflight Current inflight HTTP requests.",
        "# TYPE secflow_system_analyse_http_request_inflight gauge",
    ]
    with _REQUEST_LOCK:
        http_totals = dict(_HTTP_REQUEST_TOTAL)
        http_durations = {
            key: {"count": value["count"], "sum": value["sum"], "buckets": list(value["buckets"])}
            for key, value in _HTTP_REQUEST_DURATION.items()
        }
        http_inflight = dict(_HTTP_REQUEST_INFLIGHT)
    for key in sorted(http_totals):
        method, route, status_class, status_code = key
        labels = _labels(method=method, route=route, status_class=status_class, status_code=status_code)
        lines.append(f"secflow_system_analyse_http_requests_total{labels} {http_totals[key]}")
    for key in sorted(http_durations):
        method, route = key
        labels = _labels(method=method, route=route)
        cumulative = 0
        for index, upper_bound in enumerate(_HTTP_DURATION_BUCKETS):
            cumulative += int(http_durations[key]["buckets"][index])
            lines.append(
                f"secflow_system_analyse_http_request_duration_seconds_bucket"
                f"{_labels(method=method, route=route, le=_fmt(upper_bound))} {cumulative}"
            )
        lines.append(f"secflow_system_analyse_http_request_duration_seconds_sum{labels} {_fmt(http_durations[key]['sum'])}")
        lines.append(f"secflow_system_analyse_http_request_duration_seconds_count{labels} {int(http_durations[key]['count'])}")
    for key in sorted(http_inflight):
        method, route = key
        lines.append(
            f"secflow_system_analyse_http_request_inflight{_labels(method=method, route=route)} {int(http_inflight[key])}"
        )
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
    stage_records_total: dict[tuple[str, str], int] = defaultdict(int)
    stage_tokens: dict[tuple[str, str], int] = defaultdict(int)
    stage_cost: dict[tuple[str, str], float] = defaultdict(float)
    stage_vote_pass_total: dict[tuple[str, str], int] = defaultdict(int)
    stage_vote_fail_total: dict[tuple[str, str], int] = defaultdict(int)
    stage_judge_score_sum: dict[tuple[str, str], float] = defaultdict(float)
    stage_judge_score_count: dict[tuple[str, str], int] = defaultdict(int)
    stage_review_pass_rate_sum: dict[tuple[str, str], float] = defaultdict(float)
    stage_review_pass_rate_count: dict[tuple[str, str], int] = defaultdict(int)
    stage_round_index_sum: dict[tuple[str, str], int] = defaultdict(int)
    stage_round_index_count: dict[tuple[str, str], int] = defaultdict(int)
    module_total = module_completed_total = module_failed_total = 0
    effectiveness_first_round_pass_rate_sum = 0.0
    effectiveness_first_round_pass_rate_count = 0
    effectiveness_final_module_pass_rate_sum = 0.0
    effectiveness_final_module_pass_rate_count = 0
    effectiveness_multi_round_pass_rate_sum = 0.0
    effectiveness_multi_round_pass_rate_count = 0
    effectiveness_reflection_round_total = 0
    effectiveness_reclassify_total = 0
    checkpoint_any_tasks = checkpoint_partial_tasks = checkpoint_overall_done_tasks = 0
    checkpoint_stage_done_total: dict[str, int] = defaultdict(int)
    checkpoint_module_done_total: dict[str, int] = defaultdict(int)

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
            module_total += int(eval_summary.get("module_count") or 0)
            module_completed_total += int(eval_summary.get("completed_module_count") or 0)
            module_failed_total += int(eval_summary.get("failed_module_count") or 0)
            effectiveness = eval_summary.get("effectiveness") if isinstance(eval_summary.get("effectiveness"), dict) else {}
            if effectiveness:
                effectiveness_first_round_pass_rate_sum += float(effectiveness.get("first_round_pass_rate") or 0.0)
                effectiveness_first_round_pass_rate_count += 1
                effectiveness_final_module_pass_rate_sum += float(effectiveness.get("final_module_pass_rate") or 0.0)
                effectiveness_final_module_pass_rate_count += 1
                effectiveness_multi_round_pass_rate_sum += float(effectiveness.get("multi_round_pass_rate") or 0.0)
                effectiveness_multi_round_pass_rate_count += 1
                effectiveness_reflection_round_total += int(effectiveness.get("reflection_round_count") or 0)
                effectiveness_reclassify_total += int(effectiveness.get("reclassify_count") or 0)
        for record in eval_records:
            stage = str(record.get("stage") or "unknown")
            stage_status = str(record.get("status") or "unknown")
            key = (stage, stage_status)
            stage_duration[key] += max(0.0, float(record.get("duration_ms") or 0.0) / 1000.0)
            stage_rounds[key] += 1
            stage_records_total[key] += 1
            metrics = record.get("metrics") if isinstance(record.get("metrics"), dict) else {}
            stage_tokens[key] += int(metrics.get("token_total") or 0)
            stage_cost[key] += float(metrics.get("cost") or 0.0)
            if "avg_judge_score" in metrics:
                stage_judge_score_sum[key] += float(metrics.get("avg_judge_score") or 0.0)
                stage_judge_score_count[key] += 1
            if "review_pass_rate" in metrics:
                stage_review_pass_rate_sum[key] += float(metrics.get("review_pass_rate") or 0.0)
                stage_review_pass_rate_count[key] += 1
            if bool(metrics.get("passed_by_vote")):
                stage_vote_pass_total[key] += 1
            else:
                stage_vote_fail_total[key] += 1
            if record.get("stage_round") is not None:
                stage_round_index_sum[key] += int(record.get("stage_round") or 0)
                stage_round_index_count[key] += 1
            worker = record.get("worker") if isinstance(record.get("worker"), dict) else {}
            if worker.get("session_file"):
                worker_session_gauge += 1
                total_session_gauge += 1
            judges = record.get("judges") if isinstance(record.get("judges"), list) else []
            judge_session_gauge += len(judges)
            total_session_gauge += sum(1 for judge in judges if isinstance(judge, dict) and judge.get("session_file"))
        total_session_gauge += _count_session_files(_task_run_root(row) / "sessions")
        checkpoint_summary = _load_checkpoint_summary(row)
        if checkpoint_summary is not None:
            checkpoint_any_tasks += 1
            if bool(checkpoint_summary.get("overall_done")):
                checkpoint_overall_done_tasks += 1
            else:
                checkpoint_partial_tasks += 1
            for stage_name, stage_payload in (checkpoint_summary.get("stages") or {}).items():
                if isinstance(stage_payload, dict) and stage_payload.get("done"):
                    checkpoint_stage_done_total[str(stage_name)] += 1
            checkpoint_module_done_total["s2"] += int(checkpoint_summary.get("s2_done_count") or 0)
            checkpoint_module_done_total["s3"] += int(checkpoint_summary.get("s3_done_count") or 0)

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
    worker_running_tasks = max(0, int(worker_health.get("worker_running_tasks") or worker_health.get("running_task_count") or 0))
    worker_capacity = max(0, int(worker_health.get("worker_task_concurrency") or worker_settings.get("worker_task_concurrency") or worker_gauge))
    worker_available_slots = max(0, worker_capacity - worker_running_tasks)
    worker_utilization_ratio = (worker_running_tasks / worker_capacity) if worker_capacity > 0 else 0.0
    worker_global_remaining = worker_health.get("worker_last_global_capacity_remaining")
    try:
        worker_global_remaining_value = int(worker_global_remaining) if worker_global_remaining is not None else -1
    except (TypeError, ValueError):
        worker_global_remaining_value = -1
    worker_global_limit_reached = 1 if bool(worker_health.get("worker_global_limit_reached")) else 0
    worker_loop_fresh = 1 if bool(worker_health.get("worker_loop_fresh")) else 0
    worker_claim_enabled = 1 if bool(worker_health.get("worker_control_claim_enabled", True)) else 0
    worker_drain_mode = 1 if bool(worker_health.get("worker_control_drain_mode")) else 0
    cluster_snapshot = build_worker_slot_cluster_snapshot(db) if db_up else None

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
        "# HELP secflow_sa_worker_runtime Worker runtime snapshot by kind.",
        "# TYPE secflow_sa_worker_runtime gauge",
        f'secflow_sa_worker_runtime{{kind="capacity"}} {worker_capacity}',
        f'secflow_sa_worker_runtime{{kind="running"}} {worker_running_tasks}',
        f'secflow_sa_worker_runtime{{kind="available_slots"}} {worker_available_slots}',
        f'secflow_sa_worker_runtime{{kind="global_capacity_remaining"}} {worker_global_remaining_value}',
        f'secflow_sa_worker_runtime{{kind="global_limit_reached"}} {worker_global_limit_reached}',
        f'secflow_sa_worker_runtime{{kind="loop_fresh"}} {worker_loop_fresh}',
        f'secflow_sa_worker_runtime{{kind="claim_enabled"}} {worker_claim_enabled}',
        f'secflow_sa_worker_runtime{{kind="drain_mode"}} {worker_drain_mode}',
        "# HELP secflow_sa_worker_utilization_ratio Current worker slot utilization ratio.",
        "# TYPE secflow_sa_worker_utilization_ratio gauge",
        f"secflow_sa_worker_utilization_ratio {_fmt(worker_utilization_ratio)}",
        "# HELP secflow_sa_cluster_worker_runtime Worker cluster runtime snapshot by worker and kind.",
        "# TYPE secflow_sa_cluster_worker_runtime gauge",
        "# HELP secflow_sa_cluster_worker_active_jobs Worker cluster active jobs snapshot by worker and status.",
        "# TYPE secflow_sa_cluster_worker_active_jobs gauge",
        "# HELP secflow_sa_cluster_worker_last_heartbeat_timestamp_seconds Worker cluster last heartbeat timestamp in unix seconds.",
        "# TYPE secflow_sa_cluster_worker_last_heartbeat_timestamp_seconds gauge",
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
        "# HELP secflow_sa_stage_records_total Aggregated stage record count by stage and status.",
        "# TYPE secflow_sa_stage_records_total gauge",
        "# HELP secflow_sa_stage_token_total Aggregated stage tokens by stage and status.",
        "# TYPE secflow_sa_stage_token_total gauge",
        "# HELP secflow_sa_stage_cost_total Aggregated stage cost by stage and status.",
        "# TYPE secflow_sa_stage_cost_total gauge",
        "# HELP secflow_sa_stage_vote_pass_total Aggregated vote-passed records by stage and status.",
        "# TYPE secflow_sa_stage_vote_pass_total gauge",
        "# HELP secflow_sa_stage_vote_fail_total Aggregated vote-failed records by stage and status.",
        "# TYPE secflow_sa_stage_vote_fail_total gauge",
        "# HELP secflow_sa_stage_judge_score Aggregated stage judge score summary by stage and status.",
        "# TYPE secflow_sa_stage_judge_score summary",
        "# HELP secflow_sa_stage_review_pass_rate Aggregated stage review pass rate summary by stage and status.",
        "# TYPE secflow_sa_stage_review_pass_rate summary",
        "# HELP secflow_sa_stage_round_index Aggregated stage_round index summary by stage and status.",
        "# TYPE secflow_sa_stage_round_index summary",
        "# HELP secflow_sa_module_total Aggregated module count from evaluation summaries.",
        "# TYPE secflow_sa_module_total gauge",
        f"secflow_sa_module_total {module_total}",
        "# HELP secflow_sa_module_completed_total Aggregated completed module count from evaluation summaries.",
        "# TYPE secflow_sa_module_completed_total gauge",
        f"secflow_sa_module_completed_total {module_completed_total}",
        "# HELP secflow_sa_module_failed_total Aggregated failed module count from evaluation summaries.",
        "# TYPE secflow_sa_module_failed_total gauge",
        f"secflow_sa_module_failed_total {module_failed_total}",
        "# HELP secflow_sa_effectiveness_first_round_pass_rate Aggregated first round pass rate summary.",
        "# TYPE secflow_sa_effectiveness_first_round_pass_rate summary",
        f"secflow_sa_effectiveness_first_round_pass_rate_count {effectiveness_first_round_pass_rate_count}",
        f"secflow_sa_effectiveness_first_round_pass_rate_sum {_fmt(effectiveness_first_round_pass_rate_sum)}",
        "# HELP secflow_sa_effectiveness_final_module_pass_rate Aggregated final module pass rate summary.",
        "# TYPE secflow_sa_effectiveness_final_module_pass_rate summary",
        f"secflow_sa_effectiveness_final_module_pass_rate_count {effectiveness_final_module_pass_rate_count}",
        f"secflow_sa_effectiveness_final_module_pass_rate_sum {_fmt(effectiveness_final_module_pass_rate_sum)}",
        "# HELP secflow_sa_effectiveness_multi_round_pass_rate Aggregated multi-round final pass rate summary.",
        "# TYPE secflow_sa_effectiveness_multi_round_pass_rate summary",
        f"secflow_sa_effectiveness_multi_round_pass_rate_count {effectiveness_multi_round_pass_rate_count}",
        f"secflow_sa_effectiveness_multi_round_pass_rate_sum {_fmt(effectiveness_multi_round_pass_rate_sum)}",
        "# HELP secflow_sa_effectiveness_reflection_round_total Aggregated reflection rounds from evaluation summaries.",
        "# TYPE secflow_sa_effectiveness_reflection_round_total counter",
        f"secflow_sa_effectiveness_reflection_round_total {effectiveness_reflection_round_total}",
        "# HELP secflow_sa_effectiveness_reclassify_total Aggregated reclassify count from evaluation summaries.",
        "# TYPE secflow_sa_effectiveness_reclassify_total counter",
        f"secflow_sa_effectiveness_reclassify_total {effectiveness_reclassify_total}",
        "# HELP secflow_sa_checkpoint_tasks Aggregated checkpoint task coverage by state.",
        "# TYPE secflow_sa_checkpoint_tasks gauge",
        f'secflow_sa_checkpoint_tasks{{state="any"}} {checkpoint_any_tasks}',
        f'secflow_sa_checkpoint_tasks{{state="partial"}} {checkpoint_partial_tasks}',
        f'secflow_sa_checkpoint_tasks{{state="overall_done"}} {checkpoint_overall_done_tasks}',
        "# HELP secflow_sa_checkpoint_stage_done_total Aggregated stage-level completed checkpoint count.",
        "# TYPE secflow_sa_checkpoint_stage_done_total gauge",
        "# HELP secflow_sa_checkpoint_module_done_total Aggregated module-level completed checkpoint count.",
        "# TYPE secflow_sa_checkpoint_module_done_total gauge",
    ])
    for key in sorted(set(stage_rounds) | set(stage_duration) | set(stage_tokens) | set(stage_cost) | set(stage_records_total) | set(stage_vote_pass_total) | set(stage_vote_fail_total) | set(stage_judge_score_sum) | set(stage_review_pass_rate_sum) | set(stage_round_index_sum)):
        stage, stage_status = key
        labels = _labels(stage=stage, status=stage_status)
        lines.append(f"secflow_sa_stage_duration_seconds{labels} {_fmt(stage_duration.get(key, 0.0))}")
        lines.append(f"secflow_sa_stage_rounds{labels} {stage_rounds.get(key, 0)}")
        lines.append(f"secflow_sa_stage_records_total{labels} {stage_records_total.get(key, 0)}")
        lines.append(f"secflow_sa_stage_token_total{labels} {stage_tokens.get(key, 0)}")
        lines.append(f"secflow_sa_stage_cost_total{labels} {_fmt(stage_cost.get(key, 0.0))}")
        lines.append(f"secflow_sa_stage_vote_pass_total{labels} {stage_vote_pass_total.get(key, 0)}")
        lines.append(f"secflow_sa_stage_vote_fail_total{labels} {stage_vote_fail_total.get(key, 0)}")
        lines.append(f"secflow_sa_stage_judge_score_count{labels} {stage_judge_score_count.get(key, 0)}")
        lines.append(f"secflow_sa_stage_judge_score_sum{labels} {_fmt(stage_judge_score_sum.get(key, 0.0))}")
        lines.append(f"secflow_sa_stage_review_pass_rate_count{labels} {stage_review_pass_rate_count.get(key, 0)}")
        lines.append(f"secflow_sa_stage_review_pass_rate_sum{labels} {_fmt(stage_review_pass_rate_sum.get(key, 0.0))}")
        lines.append(f"secflow_sa_stage_round_index_count{labels} {stage_round_index_count.get(key, 0)}")
        lines.append(f"secflow_sa_stage_round_index_sum{labels} {_fmt(float(stage_round_index_sum.get(key, 0)))}")
    if cluster_snapshot is not None:
        for worker in cluster_snapshot.workers:
            base_labels = {
                "worker_id": worker.worker_id,
                "host_name": worker.host_name,
                "healthy": "true" if worker.healthy else "false",
                "source": worker.source,
            }
            lines.append(f"secflow_sa_cluster_worker_runtime{_labels(**base_labels, kind='capacity')} {worker.max_concurrent_jobs}")
            lines.append(f"secflow_sa_cluster_worker_runtime{_labels(**base_labels, kind='running_jobs')} {worker.running_jobs}")
            lines.append(f"secflow_sa_cluster_worker_runtime{_labels(**base_labels, kind='available_slots')} {worker.available_slots}")
            heartbeat_ts = worker.last_heartbeat_at.timestamp() if worker.last_heartbeat_at else 0.0
            lines.append(f"secflow_sa_cluster_worker_last_heartbeat_timestamp_seconds{_labels(worker_id=worker.worker_id, host_name=worker.host_name)} {_fmt(heartbeat_ts)}")
            status_counts_by_worker: dict[str, int] = defaultdict(int)
            for job in worker.active_jobs:
                status_counts_by_worker[str(job.status or "unknown")] += 1
            for status, count in sorted(status_counts_by_worker.items()):
                lines.append(f"secflow_sa_cluster_worker_active_jobs{_labels(worker_id=worker.worker_id, host_name=worker.host_name, status=status)} {count}")
    for stage_name in sorted(checkpoint_stage_done_total):
        lines.append(f"secflow_sa_checkpoint_stage_done_total{_labels(stage=stage_name)} {checkpoint_stage_done_total[stage_name]}")
    for stage_name in sorted(checkpoint_module_done_total):
        lines.append(f"secflow_sa_checkpoint_module_done_total{_labels(stage=stage_name)} {checkpoint_module_done_total[stage_name]}")
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


def _task_workspace(row: AppSaTask) -> Path | None:
    run_root = _task_run_root(row)
    if run_root is None:
        return None
    return run_root / "workspace"


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


def _load_checkpoint_summary(row: AppSaTask) -> dict[str, Any] | None:
    workspace = _task_workspace(row)
    if workspace is None:
        return None
    checkpoint_dir = workspace / ".checkpoint"
    if not checkpoint_dir.is_dir():
        return None
    try:
        from .pipeline.checkpoint import CheckpointManager

        return CheckpointManager(workspace).load_summary()
    except Exception:
        return None


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


def _render_agent_observability_metrics() -> list[str]:
    from .db import get_db
    from .service.agent_observability import get_agent_observability_service

    try:
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            snapshot = get_agent_observability_service().build_snapshot(db)
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
    except Exception:
        return []

    processes = list(snapshot.get("processes") or [])
    sessions = list(snapshot.get("sessions") or [])
    tasks = list(snapshot.get("tasks") or [])
    lines = [
        "# HELP secflow_sa_agent_process_total Agent process total grouped by owner state, pod and role.",
        "# TYPE secflow_sa_agent_process_total gauge",
        "# HELP secflow_sa_agent_orphan_process_total Confirmed orphan agent process total by pod.",
        "# TYPE secflow_sa_agent_orphan_process_total gauge",
        "# HELP secflow_sa_agent_suspected_orphan_process_total Suspected orphan agent process total by pod.",
        "# TYPE secflow_sa_agent_suspected_orphan_process_total gauge",
        "# HELP secflow_sa_agent_killable_orphan_process_total Killable orphan agent process total by pod.",
        "# TYPE secflow_sa_agent_killable_orphan_process_total gauge",
        "# HELP secflow_sa_agent_killable_suspected_orphan_process_total Killable suspected orphan agent process total by pod.",
        "# TYPE secflow_sa_agent_killable_suspected_orphan_process_total gauge",
        "# HELP secflow_sa_agent_session_total Agent session total grouped by state, pod and role.",
        "# TYPE secflow_sa_agent_session_total gauge",
        "# HELP secflow_sa_agent_orphan_session_total Orphan agent session total by pod.",
        "# TYPE secflow_sa_agent_orphan_session_total gauge",
        "# HELP secflow_sa_agent_task_ownership_total Agent task ownership total by status.",
        "# TYPE secflow_sa_agent_task_ownership_total gauge",
    ]
    process_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    session_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    orphan_by_pod: dict[str, int] = defaultdict(int)
    suspected_by_pod: dict[str, int] = defaultdict(int)
    killable_by_pod: dict[str, int] = defaultdict(int)
    killable_suspected_by_pod: dict[str, int] = defaultdict(int)
    orphan_sessions_by_pod: dict[str, int] = defaultdict(int)
    ownership_counts: dict[str, int] = defaultdict(int)
    for item in processes:
        key = (str(item.get("owner_kind") or "unknown"), str(item.get("pod_name") or "unknown"), str(item.get("role_kind") or "unknown"))
        process_counts[key] += 1
        if str(item.get("owner_kind") or "") == "orphan":
            orphan_by_pod[str(item.get("pod_name") or "unknown")] += 1
            if bool(item.get("kill_allowed")):
                killable_by_pod[str(item.get("pod_name") or "unknown")] += 1
        if str(item.get("owner_kind") or "") == "unknown":
            suspected_by_pod[str(item.get("pod_name") or "unknown")] += 1
            if bool(item.get("kill_allowed")):
                killable_suspected_by_pod[str(item.get("pod_name") or "unknown")] += 1
    for item in sessions:
        session_state = "orphan" if bool(item.get("orphan_session")) else ("live" if bool(item.get("live")) else "history")
        key = (session_state, str(item.get("pod_name") or "unknown"), str(item.get("role_kind") or "unknown"))
        session_counts[key] += 1
        if bool(item.get("orphan_session")):
            orphan_sessions_by_pod[str(item.get("pod_name") or "unknown")] += 1
    for item in tasks:
        ownership_counts[str(item.get("ownership_status") or "unknown")] += 1
    for (state, pod, role_kind), value in sorted(process_counts.items()):
        lines.append(f"secflow_sa_agent_process_total{_labels(state=state, pod=pod, role_kind=role_kind)} {value}")
    for pod, value in sorted(orphan_by_pod.items()):
        lines.append(f"secflow_sa_agent_orphan_process_total{_labels(pod=pod)} {value}")
    for pod, value in sorted(suspected_by_pod.items()):
        lines.append(f"secflow_sa_agent_suspected_orphan_process_total{_labels(pod=pod)} {value}")
    for pod, value in sorted(killable_by_pod.items()):
        lines.append(f"secflow_sa_agent_killable_orphan_process_total{_labels(pod=pod)} {value}")
    for pod, value in sorted(killable_suspected_by_pod.items()):
        lines.append(f"secflow_sa_agent_killable_suspected_orphan_process_total{_labels(pod=pod)} {value}")
    for (state, pod, role_kind), value in sorted(session_counts.items()):
        lines.append(f"secflow_sa_agent_session_total{_labels(state=state, pod=pod, role_kind=role_kind)} {value}")
    for pod, value in sorted(orphan_sessions_by_pod.items()):
        lines.append(f"secflow_sa_agent_orphan_session_total{_labels(pod=pod)} {value}")
    for ownership_status, value in sorted(ownership_counts.items()):
        lines.append(f"secflow_sa_agent_task_ownership_total{_labels(ownership_status=ownership_status)} {value}")
    return lines


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
