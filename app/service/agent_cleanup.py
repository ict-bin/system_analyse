from __future__ import annotations

import contextlib
import os
import pathlib
import signal
import subprocess
import time
from typing import Any

from app.service.agent_observability import AgentObservabilityService
from app.service.worker_dispatcher import WORKER_INSTANCE_ID

_CRITICAL_SURVIVOR_THRESHOLD = max(
    2,
    int(os.environ.get("SECFLOW_SYSTEM_ANALYSE_AGENT_CLEANUP_CRITICAL_SURVIVOR_THRESHOLD", "3")),
)


def _read_status_value(proc_dir: pathlib.Path, key: str) -> int | None:
    try:
        status = (proc_dir / "status").read_text(encoding="utf-8", errors="replace")
    except Exception:
        import traceback
        traceback.print_exc()
        return None
    prefix = f"{key}:"
    for line in status.splitlines():
        if line.startswith(prefix):
            try:
                return int(line.split(":", 1)[1].strip())
            except ValueError:
                return None
    return None


def _iter_agent_processes_for_cleanup() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    proc_root = pathlib.Path("/proc")
    current_pid = os.getpid()
    current_pgid = os.getpgrp()
    for proc_dir in proc_root.iterdir():
        if not proc_dir.name.isdigit():
            continue
        try:
            pid = int(proc_dir.name)
        except ValueError:
            continue
        if pid in {1, current_pid}:
            continue
        try:
            comm = (proc_dir / "comm").read_text(encoding="utf-8", errors="replace").strip()
            exe = os.path.basename(os.readlink(proc_dir / "exe"))
            cmdline = (proc_dir / "cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
            cwd = os.readlink(proc_dir / "cwd")
            stat = subprocess.check_output(
                ["sh", "-lc", f"awk '{{print $5}}' /proc/{pid}/stat"],
                text=True,
            ).strip()
            pgid = int(stat) if stat else None
        except Exception:
            import traceback
            traceback.print_exc()
            continue
        is_pi_runtime = comm == "pi" or exe == "node"
        is_python_runtime = comm.lower() in {"python", "python3"} or exe.lower().startswith("python")
        if not is_pi_runtime and not is_python_runtime:
            continue
        # Never signal the service's own process group. Python helpers sharing that
        # group are killed by PID; pi-created sessions retain group cleanup.
        safe_pgid = pgid if pgid is not None and pgid != current_pgid else None
        session_arg_path = None
        if "--session" in cmdline:
            try:
                tokens = cmdline.split()
                idx = tokens.index("--session")
                if idx + 1 < len(tokens):
                    session_arg_path = tokens[idx + 1]
            except Exception:
                import traceback
                traceback.print_exc()
                session_arg_path = None
        items.append(
            {
                "pid": pid,
                "pgid": safe_pgid,
                "ppid": _read_status_value(proc_dir, "PPid"),
                "command": cmdline,
                "cwd": cwd,
                "exe": exe,
                "session_arg_path": session_arg_path,
            }
        )
    return items


class AgentCleanupService:
    def __init__(self) -> None:
        self._observability = AgentObservabilityService()

    def _scan(self) -> list[dict[str, Any]]:
        snapshot = self._observability.build_snapshot_for_processes(
            _iter_agent_processes_for_cleanup(),
            include_session_descriptors=False,
        )
        rows: list[dict[str, Any]] = []
        for item in snapshot.get("processes", []):
            rows.append(
                {
                    "pid": int(item.get("pid") or 0),
                    "pgid": item.get("pgid"),
                    "ppid": item.get("ppid"),
                    "command": item.get("command"),
                    "cwd": item.get("cwd"),
                    "rss_bytes": item.get("rss_bytes"),
                    "session_arg_path": item.get("session_arg_path"),
                    "matched_task_id": item.get("task_id"),
                    "owner_kind": item.get("owner_kind"),
                    "owner_reason": item.get("owner_reason"),
                    "termination_status": "pending",
                    "termination_signal": None,
                    "termination_error": None,
                }
            )
        return rows

    @staticmethod
    def _terminate_process_group(pid: int, pgid: int | None) -> tuple[str, str | None, str | None]:
        try:
            if pgid is not None:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(int(pgid), signal.SIGTERM)
            else:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(int(pid), signal.SIGTERM)
            time.sleep(0.2)
            try:
                if pgid is not None:
                    os.killpg(int(pgid), 0)
                else:
                    os.kill(int(pid), 0)
            except ProcessLookupError:
                return "killed", "SIGTERM", None
            if pgid is not None:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(int(pgid), signal.SIGKILL)
            else:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(int(pid), signal.SIGKILL)
            time.sleep(0.1)
            try:
                if pgid is not None:
                    os.killpg(int(pgid), 0)
                else:
                    os.kill(int(pid), 0)
            except ProcessLookupError:
                return "killed", "SIGKILL", None
            return "failed", "SIGKILL", "process_still_alive_after_sigkill"
        except ProcessLookupError:
            return "gone", None, None
        except Exception as exc:
            return "failed", None, str(exc)

    def run_cleanup(self, *, phase: str) -> dict[str, Any]:
        items = self._scan()
        terminated_groups: set[int] = set()
        for item in items:
            pgid = item.get("pgid")
            if pgid is not None and int(pgid) in terminated_groups:
                item["termination_status"] = "gone"
                item["termination_signal"] = "group_already_terminated"
                continue
            status, term_signal, error = self._terminate_process_group(
                int(item.get("pid") or 0),
                pgid,
            )
            if pgid is not None:
                terminated_groups.add(int(pgid))
            item["termination_status"] = status
            item["termination_signal"] = term_signal
            item["termination_error"] = error
        scanned_process_count = len(items)
        killed_process_count = sum(1 for item in items if item.get("termination_status") in {"killed", "gone"})
        failed_process_count = sum(1 for item in items if item.get("termination_status") == "failed")
        surviving_process_count = failed_process_count
        cleanup_failed = surviving_process_count > 0
        level = "info"
        if cleanup_failed:
            level = "critical" if surviving_process_count >= _CRITICAL_SURVIVOR_THRESHOLD else "error"
        return {
            "cleanup_phase": phase,
            "cleanup_scope": "pod_all_pi",
            "runner_instance_id": WORKER_INSTANCE_ID,
            "scanned_process_count": scanned_process_count,
            "killed_process_count": killed_process_count,
            "killed_pgid_count": len(terminated_groups),
            "failed_process_count": failed_process_count,
            "surviving_process_count": surviving_process_count,
            "cleanup_failed": cleanup_failed,
            "level": level,
            "task_continued": phase == "pre_task",
            "items": items,
        }
