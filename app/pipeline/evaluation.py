"""Round-level evaluation metrics for system analysis tasks."""

from __future__ import annotations

import json
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_module_key(value: str | None) -> str:
    raw = (value or "__task__").strip() or "__task__"
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", raw).strip("._") or "__task__"


def token_usage_to_dict(value: Any) -> dict[str, float | int]:
    return {
        "input": int(getattr(value, "input", 0) or getattr(value, "prompt_tokens", 0) or 0),
        "output": int(getattr(value, "output", 0) or getattr(value, "completion_tokens", 0) or 0),
        "cache_read": int(getattr(value, "cache_read", 0) or 0),
        "cache_write": int(getattr(value, "cache_write", 0) or 0),
        "cost": float(getattr(value, "cost", 0.0) or 0.0),
    }


def merge_token_usage(items: list[Any]) -> dict[str, float | int]:
    total: dict[str, float | int] = {
        "input": 0,
        "output": 0,
        "cache_read": 0,
        "cache_write": 0,
        "cost": 0.0,
    }
    for item in items:
        usage = token_usage_to_dict(item)
        total["input"] = int(total["input"]) + int(usage["input"])
        total["output"] = int(total["output"]) + int(usage["output"])
        total["cache_read"] = int(total["cache_read"]) + int(usage["cache_read"])
        total["cache_write"] = int(total["cache_write"]) + int(usage["cache_write"])
        total["cost"] = float(total["cost"]) + float(usage["cost"])
    return total


def token_count(usage: dict[str, float | int]) -> int:
    return int(usage.get("input", 0)) + int(usage.get("output", 0)) + int(usage.get("cache_read", 0)) + int(usage.get("cache_write", 0))


class EvaluationRecorder:
    """Writes per-round evaluation JSON files and a task-level summary."""

    def __init__(self, task_id: str, run_dir: str | Path):
        self.task_id = task_id
        self.run_dir = Path(run_dir)
        self._lock = threading.Lock()
        self._round = 0
        self._records: list[dict[str, Any]] = []
        self._previous: dict[tuple[str, str], dict[str, float]] = {}
        self._load_existing_records()

    def record_round(
        self,
        *,
        module_name: str | None,
        stage: str,
        stage_round: int,
        status: str,
        started_at: str,
        ended_at: str,
        duration_ms: float,
        worker: dict[str, Any],
        judges: list[dict[str, Any]],
        passed_by_vote: bool,
        module_completed: bool = False,
        completion_reason: str = "",
        needed_reflection: bool = False,
        triggered_reclassify: bool = False,
        artifact_paths: list[str] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        module_key = safe_module_key(module_name)
        judge_count = len(judges)
        passed_judges = sum(1 for item in judges if item.get("passed"))
        scores = [int(item.get("score") or 0) for item in judges]
        avg_score = sum(scores) / judge_count if judge_count else 0.0
        review_pass_rate = passed_judges / judge_count if judge_count else 0.0

        worker_usage = token_usage_to_dict(worker.get("token_usage"))
        judge_usages = [item.get("token_usage") for item in judges]
        total_usage = merge_token_usage([worker.get("token_usage")] + judge_usages)
        total_tokens = token_count(total_usage)

        prev_key = (module_key, stage)
        prev = self._previous.get(prev_key)
        score_delta = None if prev is None else avg_score - prev["avg_judge_score"]
        pass_rate_delta = None if prev is None else review_pass_rate - prev["review_pass_rate"]

        with self._lock:
            self._round += 1
            round_no = self._round
            round_dir = self.run_dir / f"round_{round_no:03d}"
            round_dir.mkdir(parents=True, exist_ok=True)

            normalized_judges = []
            for item in judges:
                usage = token_usage_to_dict(item.get("token_usage"))
                normalized_judges.append({
                    "judge_id": item.get("judge_id", ""),
                    "model": item.get("model", ""),
                    "session_file": item.get("session_file", ""),
                    "score": int(item.get("score") or 0),
                    "passed": bool(item.get("passed")),
                    "feedback_excerpt": str(item.get("feedback") or "")[:1000],
                    "token_usage": usage,
                })

            record = {
                "task_id": self.task_id,
                "module_name": module_name or "__task__",
                "stage": stage,
                "round": round_no,
                "stage_round": stage_round,
                "status": status,
                "started_at": started_at,
                "ended_at": ended_at,
                "duration_ms": duration_ms,
                "worker": {
                    "model": worker.get("model", ""),
                    "session_file": worker.get("session_file", ""),
                    "token_usage": worker_usage,
                    "error": worker.get("error"),
                    "artifact_paths": artifact_paths or worker.get("artifact_paths", []) or [],
                },
                "judges": normalized_judges,
                "metrics": {
                    "review_pass_rate": review_pass_rate,
                    "avg_judge_score": avg_score,
                    "accuracy_proxy": avg_score / 100.0,
                    "token_usage": total_usage,
                    "token_total": total_tokens,
                    "cost": float(total_usage["cost"]),
                    "tokens_per_score_point": (total_tokens / avg_score) if avg_score > 0 else None,
                    "passed_by_vote": passed_by_vote,
                },
                "effectiveness": {
                    "score_delta_from_previous_round": score_delta,
                    "pass_rate_delta_from_previous_round": pass_rate_delta,
                    "needed_reflection": needed_reflection,
                    "triggered_reclassify": triggered_reclassify,
                },
                "module_completed": module_completed,
                "completion_reason": completion_reason,
            }
            if extra:
                record["extra"] = extra

            dest = round_dir / f"{module_key}.{safe_module_key(stage)}.json"
            self._atomic_write_json(dest, record)
            self._records.append(record)
            self._previous[prev_key] = {
                "avg_judge_score": avg_score,
                "review_pass_rate": review_pass_rate,
            }
            return record

    def write_summary(self, *, task_status: str = "", error: str | None = None) -> dict[str, Any]:
        records = list(self._records)
        module_records = [r for r in records if r.get("module_name") != "__task__"]
        module_names = sorted({r["module_name"] for r in module_records})
        completed_modules = sorted({
            r["module_name"] for r in module_records if r.get("module_completed")
        })
        failed_modules = sorted({
            r["module_name"] for r in module_records
            if r.get("status") == "failed" or r.get("completion_reason") in {"max_rounds_exceeded", "error"}
        } - set(completed_modules))

        total_usage = merge_token_usage([
            _DictTokenUsage(r.get("metrics", {}).get("token_usage", {})) for r in records
        ])
        total_duration = sum(float(r.get("duration_ms") or 0.0) for r in records)
        rounds_by_module: dict[str, int] = {}
        for record in module_records:
            rounds_by_module[record["module_name"]] = rounds_by_module.get(record["module_name"], 0) + 1

        stage_summary: dict[str, dict[str, Any]] = {}
        for stage in sorted({str(r.get("stage")) for r in records}):
            stage_records = [r for r in records if str(r.get("stage")) == stage]
            avg_score = _avg([r["metrics"]["avg_judge_score"] for r in stage_records])
            avg_pass_rate = _avg([r["metrics"]["review_pass_rate"] for r in stage_records])
            stage_summary[stage] = {
                "round_count": len(stage_records),
                "avg_judge_score": avg_score,
                "avg_review_pass_rate": avg_pass_rate,
                "passed_round_count": sum(1 for r in stage_records if r["metrics"]["passed_by_vote"]),
            }

        score_deltas = [
            r["effectiveness"]["score_delta_from_previous_round"]
            for r in records
            if r.get("effectiveness", {}).get("score_delta_from_previous_round") is not None
        ]
        positive_score_gain = sum(delta for delta in score_deltas if delta > 0)
        summary = {
            "task_id": self.task_id,
            "task_status": task_status,
            "error": error,
            "generated_at": utc_now_iso(),
            "module_count": len(module_names),
            "completed_module_count": len(completed_modules),
            "failed_module_count": len(failed_modules),
            "completed_modules": completed_modules,
            "failed_modules": failed_modules,
            "round_count": len(records),
            "avg_rounds_per_module": _avg(list(rounds_by_module.values())),
            "total_duration_ms": total_duration,
            "avg_duration_ms": (total_duration / len(records)) if records else 0.0,
            "total_token_usage": total_usage,
            "total_tokens": token_count(total_usage),
            "total_cost": float(total_usage["cost"]),
            "stage_summary": stage_summary,
            "effectiveness": {
                "final_module_pass_rate": (len(completed_modules) / len(module_names)) if module_names else 0.0,
                "avg_score_improvement": _avg(score_deltas),
                "tokens_per_score_improvement": (token_count(total_usage) / positive_score_gain) if positive_score_gain > 0 else None,
                "first_round_pass_rate": _pass_rate_for_stage_round(records, 1),
                "multi_round_pass_rate": _multi_round_final_pass_rate(records),
                "reflection_round_count": sum(1 for r in records if r.get("effectiveness", {}).get("needed_reflection")),
                "reclassify_count": sum(1 for r in records if r.get("effectiveness", {}).get("triggered_reclassify")),
            },
        }
        self._atomic_write_json(self.run_dir / "evaluation_summary.json", summary)
        return summary

    def _atomic_write_json(self, dest: Path, data: dict[str, Any]) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, dest)

    def _load_existing_records(self) -> None:
        """Resume-friendly initialization from existing round JSON files."""
        if not self.run_dir.exists():
            return
        records: list[dict[str, Any]] = []
        for round_dir in sorted(self.run_dir.glob("round_*")):
            if not round_dir.is_dir():
                continue
            match = re.fullmatch(r"round_(\d+)", round_dir.name)
            if match:
                self._round = max(self._round, int(match.group(1)))
            for path in sorted(round_dir.glob("*.json")):
                if path.name.endswith(".tmp"):
                    continue
                try:
                    record = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                if record.get("task_id") not in (None, self.task_id):
                    continue
                records.append(record)

        records.sort(key=lambda item: int(item.get("round") or 0))
        self._records = records
        for record in records:
            self._round = max(self._round, int(record.get("round") or 0))
            module_key = safe_module_key(record.get("module_name"))
            stage = str(record.get("stage") or "")
            metrics = record.get("metrics") or {}
            self._previous[(module_key, stage)] = {
                "avg_judge_score": float(metrics.get("avg_judge_score") or 0.0),
                "review_pass_rate": float(metrics.get("review_pass_rate") or 0.0),
            }


class _DictTokenUsage:
    def __init__(self, data: dict[str, Any]):
        self.input = data.get("input", 0)
        self.output = data.get("output", 0)
        self.cache_read = data.get("cache_read", 0)
        self.cache_write = data.get("cache_write", 0)
        self.cost = data.get("cost", 0.0)


def _avg(values: list[float | int]) -> float:
    return float(sum(values) / len(values)) if values else 0.0


def _pass_rate_for_stage_round(records: list[dict[str, Any]], stage_round: int) -> float:
    selected = [r for r in records if r.get("stage_round") == stage_round]
    if not selected:
        return 0.0
    return sum(1 for r in selected if r.get("metrics", {}).get("passed_by_vote")) / len(selected)


def _multi_round_final_pass_rate(records: list[dict[str, Any]]) -> float:
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for record in records:
        key = (str(record.get("module_name")), str(record.get("stage")))
        if int(record.get("stage_round") or 0) > 1:
            latest[key] = record
    if not latest:
        return 0.0
    return sum(1 for r in latest.values() if r.get("metrics", {}).get("passed_by_vote")) / len(latest)
