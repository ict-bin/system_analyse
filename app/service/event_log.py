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

def append_events(path: Optional[Path], new_events: list[dict]) -> bool:
    """增量追加 new_events 到 events.jsonl（每次只写本批新增行）。

    幂等安全：若文件中已有内容，只追加 new_events 对应的行。
    在运行期间由 on_event 触发，替代原来的全量 DB flush。
    """
    if path is None or not new_events:
        return True
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = "".join(
            json.dumps(e, ensure_ascii=False, separators=(",", ":")) + "\n"
            for e in new_events
        )
        with open(path, "a", encoding="utf-8") as f:
            f.write(lines)
        return True
    except Exception as exc:
        logger.warning("append_events failed path=%s: %s", path, exc)
        return False


def write_final(path: Optional[Path], all_events: list[dict]) -> bool:  # noqa: ARG001
    """追加 __final__ 标记到 events.jsonl。

    events 已由 append_events 增量写入，此函数只负责追加结束标记。
    all_events 参数保留以兼容调用方签名，但不再用于写入（避免 resume 场景
    下用当前轮 event_buffer 覆盖历史事件）。
    """
    if path is None:
        return False
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(_FINAL_MARKER, ensure_ascii=False, separators=(",", ":")) + "\n")
        return True
    except Exception as exc:
        logger.warning("write_final failed path=%s: %s", path, exc)
        return False


def clear_events(path: Optional[Path]) -> None:
    """删除 events.jsonl（restart 时调用，对应旧的 stages_json = None）。"""
    if path is None:
        return
    try:
        if path.exists():
            path.unlink()
    except Exception as exc:
        logger.warning("clear_events failed path=%s: %s", path, exc)


def strip_final_marker(path: Optional[Path]) -> None:
    """删除 events.jsonl 末尾的 __final__ 行（resume 时调用）。

    resume 续跑保留历史事件，但上一次运行结束时写入的 {"__final__": true}
    必须删除，否则 read_events 会返回 final=True，误报任务已完成。
    只删除尾部的 __final__ 行（中间的由 _parse_jsonl 读取时自动忽略）。
    """
    if path is None or not path.exists():
        return
    try:
        content = path.read_bytes()
        if not content:
            return
        # 读取所有行，删除最后连续的 __final__ 行
        lines = content.splitlines(keepends=True)
        while lines:
            stripped = lines[-1].strip()
            if not stripped:
                lines.pop()
                continue
            try:
                obj = json.loads(stripped)
                if isinstance(obj, dict) and obj.get("__final__"):
                    lines.pop()
                    continue
            except Exception:
                pass
            break
        if len(lines) != len(content.splitlines(keepends=True)):
            tmp = Path(str(path) + ".tmp")
            tmp.write_bytes(b"".join(lines))
            tmp.replace(path)
    except Exception as exc:
        logger.warning("strip_final_marker failed path=%s: %s", path, exc)


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
    """解析 events.jsonl，提取 events 列表和 final 标记。

    __final__ 只有在文件最后一行才被视为「任务已完成」标记。
    resume 场景下旧 __final__ 夹在中间，不影响 final 判断。
    """
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
                    # 临时设 final=True；若后面还有 event 行则重置为 False
                    final = True
                elif isinstance(obj, dict):
                    events.append(obj)
                    # 新 event 出现在 __final__ 之后 → 任务已续跑，重置 final
                    final = False
    except Exception as exc:
        logger.warning("_parse_jsonl failed path=%s: %s", path, exc)
    return {"events": events, "final": final}
