"""event_log.py — 任务事件流文件持久化

events.jsonl 格式：
  每行一个 JSON event：{"ts":..., "type":..., "data":{...}}
  最后一行完成标记：{"__final__": true}

文件路径：{output_path}/{task_id}/run/events.jsonl

设计原则：
- append_events 只追加新增行（增量写），不重写全文件
- write_final 原子写（tmp → rename）全量 + 末行标记
- read_events 有 DB 回退：output_path 为 None 或文件不存在时降级读 stages_json
- 所有写操作静默处理异常（不让事件写失败影响任务流程）
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger("sa.event_log")

_FINAL_MARKER = {"__final__": True}
_EVENTS_FILENAME = "events.jsonl"


# ─── 路径辅助 ────────────────────────────────────────────────────────────────

def events_path(output_path: Optional[str], task_id: str) -> Optional[Path]:
    """返回 events.jsonl 的绝对路径；output_path 为 None 时返回 None。"""
    if not output_path or not task_id:
        return None
    return Path(output_path) / task_id / "run" / _EVENTS_FILENAME


# ─── 写操作 ──────────────────────────────────────────────────────────────────

def append_events(path: Optional[Path], new_events: list[dict]) -> None:
    """增量追加 new_events 到 events.jsonl（每次只写本批新增行）。

    幂等安全：若文件中已有内容，只追加 new_events 对应的行。
    在运行期间由 on_event 触发，替代原来的全量 DB flush。
    """
    if path is None or not new_events:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = "".join(
            json.dumps(e, ensure_ascii=False, separators=(",", ":")) + "\n"
            for e in new_events
        )
        with open(path, "a", encoding="utf-8") as f:
            f.write(lines)
    except Exception as exc:
        logger.warning("append_events failed path=%s: %s", path, exc)


def write_final(path: Optional[Path], all_events: list[dict]) -> None:
    """原子写：全量 events + 末行 __final__ 标记（tmp → rename）。

    在任务完成/失败时调用，保证 events.jsonl 是完整快照。
    """
    if path is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = Path(str(path) + ".tmp")
        lines = "".join(
            json.dumps(e, ensure_ascii=False, separators=(",", ":")) + "\n"
            for e in all_events
        )
        lines += json.dumps(_FINAL_MARKER, separators=(",", ":")) + "\n"
        tmp.write_text(lines, encoding="utf-8")
        tmp.replace(path)
    except Exception as exc:
        logger.warning("write_final failed path=%s: %s", path, exc)


def clear_events(path: Optional[Path]) -> None:
    """删除 events.jsonl（restart 时调用，对应旧的 stages_json = None）。"""
    if path is None:
        return
    try:
        if path.exists():
            path.unlink()
    except Exception as exc:
        logger.warning("clear_events failed path=%s: %s", path, exc)


# ─── 读操作 ──────────────────────────────────────────────────────────────────

def read_events(
    path: Optional[Path],
    fallback_stages_json: Optional[dict] = None,
) -> dict:
    """读取 events.jsonl，返回 {events: [...], final: bool}。

    回退优先级：
    1. path 存在且可读 → 解析 jsonl
    2. fallback_stages_json 非 None → 直接返回（DB 兼容旧数据）
    3. 两者都无 → 返回空结构
    """
    if path is not None and path.is_file():
        return _parse_jsonl(path)

    # DB 回退：兼容尚未迁移的旧任务 or output_path 为 None 的任务
    if isinstance(fallback_stages_json, dict):
        return {
            "events": fallback_stages_json.get("events") or [],
            "final": bool(fallback_stages_json.get("final", False)),
        }

    return {"events": [], "final": False}


def _parse_jsonl(path: Path) -> dict:
    """解析 events.jsonl，提取 events 列表和 final 标记。"""
    events: list[dict] = []
    final = False
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict) and obj.get("__final__"):
                    final = True
                elif isinstance(obj, dict):
                    events.append(obj)
    except Exception as exc:
        logger.warning("_parse_jsonl failed path=%s: %s", path, exc)
    return {"events": events, "final": final}
