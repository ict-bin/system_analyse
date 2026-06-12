from __future__ import annotations

import contextlib
import os
import pathlib
import shutil
import signal
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Callable


def find_pi_command() -> list[str]:
    pi_bin = os.environ.get("PI_BIN")
    if pi_bin and os.path.isfile(pi_bin):
        return [pi_bin]
    pi_path = shutil.which("pi")
    if pi_path:
        return [pi_path]
    npx = shutil.which("npx")
    if npx:
        return [npx, "pi"]
    raise FileNotFoundError(
        "找不到 'pi'。请安装: npm install -g @mariozechner/pi-coding-agent"
    )


def process_group_id(proc: subprocess.Popen) -> int | None:
    try:
        return os.getpgid(proc.pid)
    except ProcessLookupError:
        return None
    except Exception:
        import traceback
        traceback.print_exc()
        return None


def process_group_exists(pgid: int | None) -> bool:
    if pgid is None:
        return False
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        import traceback
        traceback.print_exc()
        return False
    return True


_PID1_REAPER_NAMES = {
    "init",
    "systemd",
    "tini",
    "dumb-init",
    "busybox",
}


def _read_proc_name(pid: int, field: str) -> str:
    try:
        return (pathlib.Path("/proc") / str(pid) / field).read_text(
            encoding="utf-8",
            errors="replace",
        ).strip()
    except Exception:
        import traceback
        traceback.print_exc()
        return ""


def _pid1_is_reaper_process() -> bool:
    """Only treat PPID=1 as orphan when PID 1 looks like a real init/reaper."""
    pid1_comm = _read_proc_name(1, "comm").lower()
    if pid1_comm in _PID1_REAPER_NAMES:
        return True
    try:
        pid1_exe = os.path.basename(os.readlink("/proc/1/exe")).lower()
    except Exception:
        import traceback
        traceback.print_exc()
        pid1_exe = ""
    return pid1_exe in _PID1_REAPER_NAMES


def cleanup_orphan_pi_processes(
    logger: Callable[[str], None],
    *,
    label: str,
    orphan_verifier: Callable[[int, int | None, int | None], tuple[bool, str | None]] | None = None,
) -> int:
    if not _pid1_is_reaper_process():
        return 0
    killed = 0
    proc_root = pathlib.Path("/proc")
    for proc_dir in proc_root.iterdir():
        if not proc_dir.name.isdigit():
            continue
        try:
            status = (proc_dir / "status").read_text(encoding="utf-8", errors="replace")
            comm = (proc_dir / "comm").read_text(encoding="utf-8", errors="replace").strip()
            exe = os.path.basename(os.readlink(proc_dir / "exe"))
        except Exception:
            import traceback
            traceback.print_exc()
            continue
        if comm != "pi" and exe != "node":
            continue
        ppid = None
        pid = int(proc_dir.name)
        for line in status.splitlines():
            if line.startswith("PPid:"):
                try:
                    ppid = int(line.split(":", 1)[1].strip())
                except ValueError:
                    ppid = None
                break
        if ppid != 1:
            continue
        try:
            pgid = int(
                subprocess.check_output(
                    ["sh", "-lc", f"awk '{{print $5}}' /proc/{pid}/stat"],
                    text=True,
                ).strip()
            )
        except Exception:
            import traceback
            traceback.print_exc()
            pgid = None
        if orphan_verifier is not None:
            try:
                kill_allowed, reason = orphan_verifier(pid, ppid, pgid)
            except Exception:
                import traceback
                traceback.print_exc()
                kill_allowed, reason = False, "orphan_verifier_failed"
            if not kill_allowed:
                logger(
                    f"skip suspected orphan pi process [{label}] pid={pid} "
                    f"pgid={pgid if pgid is not None else 'unknown'} reason={reason or 'runtime_evidence_present'}"
                )
                continue
        logger(
            f"cleaning orphan pi process [{label}] pid={pid} pgid={pgid if pgid is not None else 'unknown'}"
        )
        try:
            if pgid is not None:
                os.killpg(pgid, signal.SIGKILL)
            else:
                os.kill(pid, signal.SIGKILL)
            killed += 1
        except ProcessLookupError:
            continue
        except Exception:
            import traceback
            traceback.print_exc()
            continue
    return killed


def _wait_with_timeout(proc: subprocess.Popen, timeout: float) -> None:
    """Wait for process with timeout using a thread."""
    result = [None]

    def _waiter():
        try:
            proc.wait()
            result[0] = True
        except Exception:
            import traceback
            traceback.print_exc()
            result[0] = False

    t = threading.Thread(target=_waiter, daemon=True)
    t.start()
    t.join(timeout=timeout)
    if t.is_alive():
        return  # timeout
    return  # completed


@dataclass
class AgentProcessHandle:
    proc: subprocess.Popen
    label: str
    logger: Callable[[str], None]
    pgid: int | None

    @classmethod
    def spawn(
        cls,
        *args: str,
        cwd: str,
        env: dict[str, str] | None,
        stdout,
        stderr,
        stdin,
        logger: Callable[[str], None],
        label: str,
    ) -> "AgentProcessHandle":
        proc = subprocess.Popen(
            list(args),
            cwd=cwd,
            env=env,
            stdout=stdout,
            stderr=stderr,
            stdin=stdin,
            start_new_session=True,
        )
        return cls(proc=proc, label=label, logger=logger, pgid=process_group_id(proc))

    def terminate_tree(
        self,
        *,
        reason: str,
        term_timeout: float = 5.0,
        kill_timeout: float = 5.0,
        force_if_group_still_exists: bool = True,
    ) -> None:
        if self.proc.returncode is not None:
            if force_if_group_still_exists and process_group_exists(self.pgid):
                self.logger(
                    f"cleaning leaked pi process group [{self.label}] "
                    f"reason={reason} pid={self.proc.pid} pgid={self.pgid}"
                )
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(self.pgid, signal.SIGKILL)
            with contextlib.suppress(Exception):
                _wait_with_timeout(self.proc, timeout=1.0)
            return

        if self.pgid is not None:
            self.logger(
                f"terminating pi process group [{self.label}] "
                f"reason={reason} pid={self.proc.pid} pgid={self.pgid}"
            )
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.pgid, signal.SIGTERM)
        else:
            self.logger(
                f"terminating pi process [{self.label}] "
                f"reason={reason} pid={self.proc.pid} pgid=unavailable"
            )
            with contextlib.suppress(ProcessLookupError):
                self.proc.terminate()

        try:
            _wait_with_timeout(self.proc, timeout=term_timeout)
        except ProcessLookupError:
            return
        else:
            if proc_alive := (self.proc.returncode is None):
                pass
            else:
                if not force_if_group_still_exists or not process_group_exists(self.pgid):
                    return

        # Force kill if still alive
        if self.proc.returncode is not None and not (force_if_group_still_exists and process_group_exists(self.pgid)):
            return

        if self.pgid is not None:
            self.logger(
                f"force killing pi process group [{self.label}] "
                f"reason={reason} pid={self.proc.pid} pgid={self.pgid}"
            )
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.pgid, signal.SIGKILL)
        else:
            self.logger(
                f"force killing pi process [{self.label}] "
                f"reason={reason} pid={self.proc.pid} pgid=unavailable"
            )
            with contextlib.suppress(ProcessLookupError):
                self.proc.kill()

        with contextlib.suppress(Exception):
            _wait_with_timeout(self.proc, timeout=kill_timeout)
