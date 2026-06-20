"""sched_proto.py — V3.0 调度器 TCP 控制面协议。

帧格式: 4 字节大端 length 前缀 + UTF-8 JSON。
消息为单层 dict，约定字段:
    type:     消息类型 (见 MSG_*)
    worker_id, task_id, lease_epoch, state, error, result, ip, max_tasks, ...

Worker → Scheduler: hello / heartbeat / task_state
Scheduler → Worker: run / cancel / restart / ok / error
"""
from __future__ import annotations

import json
import struct
from typing import Any

# Worker → Scheduler
MSG_HELLO = "hello"
MSG_HEARTBEAT = "heartbeat"
MSG_TASK_STATE = "task_state"

# Scheduler → Worker
MSG_RUN = "run"
MSG_CANCEL = "cancel"
MSG_RESTART = "restart"
MSG_OK = "ok"
MSG_ERROR = "error"

# task_state 取值
STATE_STARTING = "starting"
STATE_RUNNING = "running"
STATE_FINISHED = "finished"
STATE_FAILED = "failed"
STATE_CANCELLED = "cancelled"

_HEADER = struct.Struct(">I")
_MAX_FRAME = 16 * 1024 * 1024  # 16MB 单帧上限，防止恶意/异常巨型帧


# ── 帧编解码 ──────────────────────────────────────────────────────────────────

def encode(msg: dict[str, Any]) -> bytes:
    payload = json.dumps(msg, ensure_ascii=False).encode("utf-8")
    if len(payload) > _MAX_FRAME:
        raise ValueError(f"frame too large: {len(payload)} > {_MAX_FRAME}")
    return _HEADER.pack(len(payload)) + payload


def read_frame(reader) -> dict[str, Any] | None:
    """从可 read(n) 的对象（socket.makefile('rb')）读一帧；连接关闭返回 None。"""
    header = _reader_read_exact(reader, _HEADER.size)
    if header is None:
        return None
    (length,) = _HEADER.unpack(header)
    if length <= 0 or length > _MAX_FRAME:
        raise ValueError(f"invalid frame length: {length}")
    payload = _reader_read_exact(reader, length)
    if payload is None:
        return None
    return json.loads(payload.decode("utf-8"))


def _reader_read_exact(reader, n: int) -> bytes | None:
    # reader 为 socket.makefile('rb') 返回的 BufferedReader
    chunks: list[bytes] = []
    remaining = n
    while remaining > 0:
        chunk = reader.read(remaining)
        if not chunk:  # EOF / 连接关闭
            return None
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def msg_run(task_id: str, lease_epoch: int) -> dict[str, Any]:
    return {"type": MSG_RUN, "task_id": task_id, "lease_epoch": int(lease_epoch)}


def msg_cancel(task_id: str) -> dict[str, Any]:
    return {"type": MSG_CANCEL, "task_id": task_id}


def msg_restart(task_id: str, lease_epoch: int) -> dict[str, Any]:
    return {"type": MSG_RESTART, "task_id": task_id, "lease_epoch": int(lease_epoch)}


def msg_hello(worker_id: str, ip: str = "", max_tasks: int = 1) -> dict[str, Any]:
    return {"type": MSG_HELLO, "worker_id": worker_id, "ip": ip, "max_tasks": int(max_tasks)}


def msg_heartbeat(worker_id: str, task_id: str | None = None, state: str | None = None) -> dict[str, Any]:
    return {"type": MSG_HEARTBEAT, "worker_id": worker_id, "task_id": task_id, "state": state}


def msg_task_state(worker_id: str, task_id: str, state: str, error: str | None = None, result: Any = None) -> dict[str, Any]:
    m: dict[str, Any] = {"type": MSG_TASK_STATE, "worker_id": worker_id, "task_id": task_id, "state": state}
    if error:
        m["error"] = error
    if result is not None:
        m["result"] = result
    return m


def msg_ok(**kw: Any) -> dict[str, Any]:
    m = {"type": MSG_OK}
    m.update(kw)
    return m


def msg_error(reason: str, **kw: Any) -> dict[str, Any]:
    m = {"type": MSG_ERROR, "error": reason}
    m.update(kw)
    return m
