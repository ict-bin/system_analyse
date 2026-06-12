from __future__ import annotations

import json
import re
import time as _time
from pathlib import Path
from typing import Callable


_STAGE_ORDER = {
    "filter-engine": 1,
    "filter-tree-batch": 2,
    "filter-merge": 3,
    "filter-fallback": 4,
    "classify": 10,
    "2-sub": 20,
    "refine": 30,
    "2-reclassify": 35,
    "refine-redo": 40,
    "analyse": 50,
    "analyse-redo": 60,
    "4a-completeness": 70,
    "analyse-s4": 80,
    "final_report": 90,
}

_STAGE_LABEL = {
    "filter-engine": "智能体过滤",
    "filter-tree-batch": "文件树批次过滤",
    "filter-merge": "全局模块归并",
    "filter-fallback": "过滤引擎回退",
    "classify": "全局分类",
    "2-sub": "细分类预读",
    "refine": "细分类",
    "2-reclassify": "补归类",
    "refine-redo": "重细分",
    "analyse": "安全分析",
    "analyse-redo": "重分析",
    "4a-completeness": "完整性检查",
    "analyse-s4": "缺失模块补做",
    "final_report": "最终报告",
}


def _safe_int(value: object) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        import traceback
        traceback.print_exc()
        return None


def _normalize_relative_path(path: str) -> str:
    return str(path or "").replace("\\", "/").strip().lstrip("/")


def _parse_iso_timestamp(value: object) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        from datetime import datetime

        return datetime.fromisoformat(raw).timestamp()
    except Exception:
        import traceback
        traceback.print_exc()
        return None


def _extract_session_timestamps(session_meta: dict, events: list[dict], stat_mtime: float) -> tuple[float | None, float | None]:
    started_ts = _parse_iso_timestamp(session_meta.get("timestamp"))
    event_timestamps = [
        ts
        for ts in (_parse_iso_timestamp(evt.get("timestamp") or evt.get("display_timestamp")) for evt in events)
        if ts is not None
    ]
    if started_ts is None and event_timestamps:
        started_ts = event_timestamps[0]
    last_ts = event_timestamps[-1] if event_timestamps else started_ts
    if started_ts is None:
        started_ts = stat_mtime
    if last_ts is None:
        last_ts = stat_mtime
    return started_ts, last_ts


def _round_status_to_session_status(status: str, is_active: bool) -> str:
    if is_active:
        return "running"
    normalized = str(status or "").strip().lower()
    if normalized in {"passed", "skipped"}:
        return "completed"
    if normalized in {"failed", "needs_retry", "reclassify_required"}:
        return "blocked"
    if normalized in {"needs_reflection"}:
        return "waiting"
    return "completed"


def _infer_path_descriptor(relative_path: str) -> dict:
    normalized = _normalize_relative_path(relative_path)
    parts = normalized.split("/")
    stem = Path(normalized).stem
    desc = {
        "role": "worker",
        "role_label": "Worker",
        "stage_key": "unknown",
        "stage_label": "未知阶段",
        "stage_order": 999,
        "module_name": None,
        "attempt": None,
        "judge_index": None,
        "batch_index": None,
        "parent_relative_path": None,
        "parallel_group": None,
        "family_key": None,
        "flow_kind": "worker",
    }
    if normalized == "classify.jsonl":
        desc.update({
            "stage_key": "classify",
            "stage_label": _STAGE_LABEL["classify"],
            "stage_order": _STAGE_ORDER["classify"],
            "family_key": "classify",
        })
        return desc
    if parts[0] == "filter-engine" and len(parts) >= 2:
        if stem == "merge":
            desc.update({
                "stage_key": "filter-merge",
                "stage_label": _STAGE_LABEL["filter-merge"],
                "stage_order": _STAGE_ORDER["filter-merge"],
                "family_key": "filter-merge",
            })
            return desc
        match = re.fullmatch(r"batch-(\d+)", stem)
        batch_index = _safe_int(match.group(1)) if match else None
        desc.update({
            "stage_key": "filter-tree-batch",
            "stage_label": _STAGE_LABEL["filter-tree-batch"],
            "stage_order": _STAGE_ORDER["filter-tree-batch"],
            "batch_index": batch_index,
            "parallel_group": "filter-tree-batch",
            "family_key": "filter-tree-batch",
            "flow_kind": "parallel",
        })
        return desc
    if normalized == "final_report.jsonl":
        desc.update({
            "stage_key": "final_report",
            "stage_label": _STAGE_LABEL["final_report"],
            "stage_order": _STAGE_ORDER["final_report"],
            "family_key": "final_report",
        })
        return desc
    if parts[0] == "reclassify":
        match = re.fullmatch(r"reclassify-a(\d+)", stem)
        attempt = _safe_int(match.group(1)) if match else None
        desc.update({
            "stage_key": "2-reclassify",
            "stage_label": _STAGE_LABEL["2-reclassify"],
            "stage_order": _STAGE_ORDER["2-reclassify"],
            "attempt": attempt,
            "family_key": "reclassify",
        })
        return desc
    if parts[0] == "sub_read" and len(parts) >= 3:
        module_name = parts[1]
        match = re.fullmatch(r"batch(\d+)", stem)
        batch_index = _safe_int(match.group(1)) if match else None
        desc.update({
            "role": "sub_worker",
            "role_label": "Sub Worker",
            "stage_key": "2-sub",
            "stage_label": _STAGE_LABEL["2-sub"],
            "stage_order": _STAGE_ORDER["2-sub"],
            "module_name": module_name,
            "batch_index": batch_index,
            "parent_relative_path": f"refine/{module_name}.jsonl",
            "parallel_group": f"sub_read::{module_name}",
            "family_key": f"sub_read::{module_name}",
            "flow_kind": "parallel",
        })
        return desc
    if parts[0] in {"refine", "refine-redo", "refine-s4", "analyse", "analyse-redo", "analyse-s4"} and len(parts) >= 2:
        module_name = stem
        stage_map = {
            "refine": "refine",
            "refine-redo": "refine-redo",
            "refine-s4": "refine-redo",
            "analyse": "analyse",
            "analyse-redo": "analyse-redo",
            "analyse-s4": "analyse-s4",
        }
        stage_key = stage_map[parts[0]]
        desc.update({
            "stage_key": stage_key,
            "stage_label": _STAGE_LABEL.get(stage_key, stage_key),
            "stage_order": _STAGE_ORDER.get(stage_key, 999),
            "module_name": module_name,
            "family_key": f"{stage_key}::{module_name}",
        })
        return desc
    if parts[0] == "judges" and len(parts) >= 3:
        judge_kind = parts[1]
        desc.update({
            "role": "judge",
            "role_label": "Judge",
            "flow_kind": "parallel",
        })
        if judge_kind == "classify":
            match = re.fullmatch(r"classify-a(\d+)-j(\d+)", stem)
            attempt = _safe_int(match.group(1)) if match else None
            judge_index = _safe_int(match.group(2)) if match else None
            desc.update({
                "stage_key": "classify",
                "stage_label": _STAGE_LABEL["classify"],
                "stage_order": _STAGE_ORDER["classify"],
                "attempt": attempt,
                "judge_index": judge_index,
                "parent_relative_path": "classify.jsonl",
                "parallel_group": f"judge::classify::a{attempt or 0}",
                "family_key": f"judge::classify::a{attempt or 0}",
            })
            return desc
        if judge_kind == "report-completeness":
            match = re.fullmatch(r"s4a-j(\d+)", stem)
            judge_index = _safe_int(match.group(1)) if match else None
            desc.update({
                "stage_key": "4a-completeness",
                "stage_label": _STAGE_LABEL["4a-completeness"],
                "stage_order": _STAGE_ORDER["4a-completeness"],
                "judge_index": judge_index,
                "parallel_group": "judge::4a-completeness",
                "family_key": "judge::4a-completeness",
            })
            return desc
        if judge_kind == "final_report":
            match = re.fullmatch(r"final-report-a(\d+)-j(\d+)", stem)
            attempt = _safe_int(match.group(1)) if match else None
            judge_index = _safe_int(match.group(2)) if match else None
            desc.update({
                "stage_key": "final_report",
                "stage_label": _STAGE_LABEL["final_report"],
                "stage_order": _STAGE_ORDER["final_report"],
                "attempt": attempt,
                "judge_index": judge_index,
                "parent_relative_path": "final_report.jsonl",
                "parallel_group": f"judge::final_report::a{attempt or 0}",
                "family_key": f"judge::final_report::a{attempt or 0}",
            })
            return desc
        if len(parts) >= 4:
            module_name = parts[2]
            stage_key = judge_kind
            pattern_by_kind = {
                "refine": rf"refine-a(\d+)-j(\d+)",
                "analyse": rf"analyse-a(\d+)-j(\d+)",
                "analyse-redo": rf"analyse-a(\d+)-j(\d+)",
                "refine-redo": rf"refine-redo-a(\d+)-j(\d+)",
                "analyse-s4": rf"analyse-s4-a(\d+)-j(\d+)",
            }
            match = re.fullmatch(pattern_by_kind.get(judge_kind, r".^"), stem)
            attempt = _safe_int(match.group(1)) if match else None
            judge_index = _safe_int(match.group(2)) if match else None
            parent_prefix = {
                "refine": "refine",
                "analyse": "analyse",
                "analyse-redo": "analyse-redo",
                "refine-redo": "refine-redo",
                "analyse-s4": "analyse-s4",
            }.get(judge_kind, judge_kind)
            desc.update({
                "stage_key": stage_key,
                "stage_label": _STAGE_LABEL.get(stage_key, stage_key),
                "stage_order": _STAGE_ORDER.get(stage_key, 999),
                "module_name": module_name,
                "attempt": attempt,
                "judge_index": judge_index,
                "parent_relative_path": f"{parent_prefix}/{module_name}.jsonl",
                "parallel_group": f"judge::{judge_kind}::{module_name}::a{attempt or 0}",
                "family_key": f"judge::{judge_kind}::{module_name}::a{attempt or 0}",
            })
            return desc
    return desc


def _round_ref_path(value: object) -> str:
    normalized = _normalize_relative_path(str(value or ""))
    return normalized.replace("run/sessions/", "", 1) if normalized.startswith("run/sessions/") else normalized


def _load_round_refs(run_root: Path) -> tuple[dict[str, list[dict]], list[str]]:
    refs: dict[str, list[dict]] = {}
    warnings: list[str] = []
    for round_dir in sorted(run_root.glob("round_*")):
        if not round_dir.is_dir():
            continue
        for path in sorted(round_dir.glob("*.json")):
            if path.name.endswith(".tmp"):
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:
                warnings.append(f"{path.name} 读取失败: {exc}")
                continue
            if not isinstance(payload, dict):
                continue
            base_ref = {
                "round": payload.get("round"),
                "stage_round": payload.get("stage_round"),
                "stage": payload.get("stage"),
                "module_name": payload.get("module_name"),
                "status": payload.get("status"),
                "started_at": payload.get("started_at"),
                "ended_at": payload.get("ended_at"),
                "completion_reason": payload.get("completion_reason"),
            }
            worker = payload.get("worker") if isinstance(payload.get("worker"), dict) else {}
            worker_path = _round_ref_path(worker.get("session_file"))
            if worker_path:
                refs.setdefault(worker_path, []).append({
                    **base_ref,
                    "kind": "worker",
                    "model": worker.get("model"),
                })
            for judge in payload.get("judges") or []:
                if not isinstance(judge, dict):
                    continue
                judge_path = _round_ref_path(judge.get("session_file"))
                if judge_path:
                    refs.setdefault(judge_path, []).append({
                        **base_ref,
                        "kind": "judge",
                        "judge_id": judge.get("judge_id"),
                        "model": judge.get("model"),
                        "score": judge.get("score"),
                        "passed": judge.get("passed"),
                    })
    for items in refs.values():
        items.sort(key=lambda item: (
            _safe_int(item.get("round")) or 0,
            _safe_int(item.get("stage_round")) or 0,
        ))
    return refs, warnings


def build_session_catalog(
    *,
    task_id: str,
    row_status: str,
    sessions_root: Path,
    run_root: Path,
    parse_session_jsonl_file: Callable[[Path], tuple[dict, list[dict], list[str], int]],
    write_json_atomic: Callable[[Path, dict], None] | None = None,
) -> dict:
    now_ts = _time.time()
    refs_by_path, ref_warnings = _load_round_refs(run_root)
    items: list[dict] = []
    nodes: list[dict] = []
    node_map: dict[str, dict] = {}
    warnings = list(ref_warnings)

    for session_file in sorted(sessions_root.rglob("*.jsonl")):
        try:
            relative_path = _normalize_relative_path(str(session_file.relative_to(sessions_root)))
            stage_group = relative_path.split("/")[0] if "/" in relative_path else "root"
            session_name = session_file.stem
            session_meta, events, session_warnings, line_count = parse_session_jsonl_file(session_file)
            stat = session_file.stat()
            is_active = row_status in ("pending", "running") and (now_ts - stat.st_mtime) <= 120
            display_name = session_name if stage_group == "root" else f"{stage_group} / {session_name}"
            desc = _infer_path_descriptor(relative_path)
            round_refs = refs_by_path.get(relative_path, [])
            latest_ref = round_refs[-1] if round_refs else {}
            started_ts, last_event_ts = _extract_session_timestamps(session_meta, events, stat.st_mtime)
            status = _round_status_to_session_status(str(latest_ref.get("status") or ""), is_active)
            started_at = latest_ref.get("started_at") or session_meta.get("timestamp")
            ended_at = latest_ref.get("ended_at")
            model_id = None
            for event in reversed(events):
                model_id = event.get("modelId")
                if model_id:
                    break
            role_name = desc["role"]
            item = {
                "session_id": session_name,
                "session_name": session_name,
                "relative_path": relative_path,
                "stage_group": stage_group,
                "role_name": role_name,
                "size": stat.st_size,
                "mtime": stat.st_mtime,
                "event_count": len(events),
                "line_count": line_count,
                "is_active": is_active,
                "display_name": display_name,
                "warnings": session_warnings,
            }
            items.append(item)
            node = {
                "node_id": relative_path,
                "relative_path": relative_path,
                "session_name": session_name,
                "display_name": display_name,
                "role": role_name,
                "role_label": desc["role_label"],
                "status": status,
                "is_active": is_active,
                "stage_key": desc["stage_key"],
                "stage_label": desc["stage_label"],
                "stage_order": desc["stage_order"],
                "stage_group": stage_group,
                "module_name": desc["module_name"],
                "attempt": desc["attempt"],
                "judge_index": desc["judge_index"],
                "batch_index": desc["batch_index"],
                "parent_relative_path": desc["parent_relative_path"],
                "parallel_group": desc["parallel_group"],
                "family_key": desc["family_key"],
                "flow_kind": desc["flow_kind"],
                "started_at": started_at,
                "ended_at": ended_at,
                "started_ts": started_ts,
                "last_event_at": latest_ref.get("ended_at") or latest_ref.get("started_at") or session_meta.get("timestamp"),
                "last_event_ts": last_event_ts,
                "mtime": stat.st_mtime,
                "size": stat.st_size,
                "event_count": len(events),
                "line_count": line_count,
                "warnings": session_warnings,
                "session_header": session_meta,
                "cwd": session_meta.get("cwd"),
                "model": latest_ref.get("model") or model_id,
                "latest_round_ref": latest_ref or None,
                "round_refs": round_refs,
                "attempts_seen": sorted({
                    attempt for attempt in (
                        _safe_int(ref.get("stage_round")) for ref in round_refs
                    ) if attempt is not None
                }),
            }
            nodes.append(node)
            node_map[relative_path] = node
        except Exception as exc:
            warnings.append(f"{session_file.name} 解析失败: {exc}")

    edges: list[dict] = []
    edge_seen: set[tuple[str, str, str]] = set()

    def add_edge(source: str | None, target: str | None, kind: str, label: str) -> None:
        if not source or not target or source == target:
            return
        if source not in node_map or target not in node_map:
            return
        key = (source, target, kind)
        if key in edge_seen:
            return
        edge_seen.add(key)
        edges.append({
            "edge_id": f"{kind}:{source}->{target}",
            "source_node_id": source,
            "target_node_id": target,
            "kind": kind,
            "label": label,
        })

    root_nodes = [node for node in nodes if node["role"] != "judge"]
    by_module: dict[str, list[dict]] = {}
    for node in root_nodes:
        module_name = str(node.get("module_name") or "").strip()
        if module_name:
            by_module.setdefault(module_name, []).append(node)
    for module_nodes in by_module.values():
        module_nodes.sort(key=lambda item: (
            int(item.get("stage_order") or 999),
            float(item.get("started_ts") or item.get("mtime") or 0.0),
            str(item.get("relative_path") or ""),
        ))
        for current, nxt in zip(module_nodes, module_nodes[1:]):
            add_edge(current["relative_path"], nxt["relative_path"], "progress", "模块推进")

    classify_node = node_map.get("classify.jsonl")
    if classify_node:
        for node in root_nodes:
            if node["stage_key"] in {"refine", "2-sub"}:
                add_edge(classify_node["relative_path"], node["relative_path"], "dispatch", "进入细分类")

    filter_merge_node = node_map.get("filter-engine/merge.jsonl")
    if filter_merge_node:
        for node in root_nodes:
            if node["stage_key"] == "filter-tree-batch":
                add_edge(node["relative_path"], filter_merge_node["relative_path"], "progress", "汇总到全局归并")

    final_report_node = node_map.get("final_report.jsonl")
    if final_report_node:
        for node in root_nodes:
            if node["stage_key"] in {"analyse", "analyse-redo", "analyse-s4", "4a-completeness"}:
                add_edge(node["relative_path"], final_report_node["relative_path"], "progress", "汇总到最终报告")

    for node in nodes:
        add_edge(node.get("parent_relative_path"), node["relative_path"], "spawn", "派生")

    groups: list[dict] = []
    groups_by_key: dict[str, list[str]] = {}
    for node in nodes:
        group_key = str(node.get("parallel_group") or "").strip()
        if group_key:
            groups_by_key.setdefault(group_key, []).append(node["node_id"])
    for group_key, node_ids in sorted(groups_by_key.items()):
        node_ids.sort(key=lambda value: (
            float(node_map[value].get("started_ts") or node_map[value].get("mtime") or 0.0),
            value,
        ))
        if len(node_ids) >= 2:
            for left, right in zip(node_ids, node_ids[1:]):
                add_edge(left, right, "parallel", "并列")
        sample = node_map[node_ids[0]]
        groups.append({
            "group_id": group_key,
            "kind": "parallel",
            "label": (
                "并行 Judge"
                if str(sample.get("role")) == "judge"
                else "并行子 Worker"
                if str(sample.get("role")) == "sub_worker"
                else "并行会话"
            ),
            "stage_key": sample.get("stage_key"),
            "module_name": sample.get("module_name"),
            "node_ids": node_ids,
        })

    nodes.sort(key=lambda item: (
        int(item.get("stage_order") or 999),
        float(item.get("started_ts") or item.get("mtime") or 0.0),
        str(item.get("relative_path") or ""),
    ))
    items.sort(key=lambda item: (item["stage_group"], -item["mtime"], item["relative_path"]))

    index_payload = {
        "version": 1,
        "generated_at": _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime(now_ts)),
        "task_id": task_id,
        "task_status": row_status,
        "sessions_root": str(sessions_root),
        "summary": {
            "session_count": len(nodes),
            "active_session_count": sum(1 for node in nodes if node.get("is_active")),
            "worker_count": sum(1 for node in nodes if node.get("role") == "worker"),
            "judge_count": sum(1 for node in nodes if node.get("role") == "judge"),
            "sub_worker_count": sum(1 for node in nodes if node.get("role") == "sub_worker"),
            "edge_count": len(edges),
            "parallel_group_count": len(groups),
            "stage_count": len({str(node.get("stage_key") or "") for node in nodes}),
        },
        "nodes": nodes,
        "edges": edges,
        "groups": groups,
        "warnings": warnings,
    }
    if write_json_atomic:
        write_json_atomic(sessions_root / "index.json", index_payload)
    return {
        "task_id": task_id,
        "status": row_status,
        "sessions_root": str(sessions_root),
        "index_path": str(sessions_root / "index.json"),
        "generated_at": index_payload["generated_at"],
        "items": items,
        "index": index_payload,
        "warnings": warnings,
    }
