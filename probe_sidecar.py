#!/usr/bin/env python3
"""独立探针进程 — 与主服务完全隔离，不受 Python GIL / pi agent CPU 影响。

通过 HTTP 调用主服务 (127.0.0.1:APP_PORT) 的健康端点来判断存活度和就绪度。
K8s liveness/readiness/startup probe 指向本进程的 18080 端口。
"""

import json
import os
import signal
import sys
import time
import urllib.error
import urllib.request
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

APP_PORT = int(os.environ.get("PROBE_APP_PORT", os.environ.get("PORT", "8080")))
PROBE_PORT = int(os.environ.get("PROBE_PORT", "18080"))
PROBE_BIND = os.environ.get("PROBE_BIND", "0.0.0.0")
PROBE_TIMEOUT = max(1.0, float(os.environ.get("PROBE_HTTP_TIMEOUT", "3.0")))
LIVEZ_PATH = os.environ.get("PROBE_LIVEZ_PATH", "/api/app/system-analyse/livez")
READYZ_PATH = os.environ.get("PROBE_READYZ_PATH", "/api/app/system-analyse/readyz")

_shutting_down = False
_last_health: dict | None = None
_last_health_at = 0.0
_last_health_error: str | None = None
_request_total = 0
_request_fail_total = 0
_started_at = time.time()


def _fetch_health() -> dict:
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{APP_PORT}/api/app/system-analyse/health",
            headers={"User-Agent": "sa-probe-sidecar/1.0"},
        )
        with urllib.request.urlopen(req, timeout=PROBE_TIMEOUT) as rsp:
            return json.loads(rsp.read().decode("utf-8", errors="replace"))
    except Exception:
        return {}


def _background_refresh() -> None:
    global _last_health, _last_health_at, _last_health_error
    while not _shutting_down:
        try:
            _last_health = _fetch_health()
            _last_health_at = time.time()
            _last_health_error = None
        except Exception as exc:
            _last_health = None
            _last_health_error = str(exc)
        time.sleep(2.0)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        global _request_total, _request_fail_total
        try:
            payload = _last_health or {}
            is_livez = self.path in ("/livez", "/health", LIVEZ_PATH)
            is_readyz = self.path in ("/readyz", "/ready", READYZ_PATH)

            if is_livez:
                live = bool(payload.get("liveness_ok")) and not _shutting_down
                code = HTTPStatus.OK if live else HTTPStatus.SERVICE_UNAVAILABLE
            elif is_readyz:
                ready = bool(payload.get("readiness_ok")) and not _shutting_down
                code = HTTPStatus.OK if ready else HTTPStatus.SERVICE_UNAVAILABLE
            else:
                code = HTTPStatus.NOT_FOUND
                payload = {"status": "not_found"}

            body = json.dumps(
                {
                    **payload,
                    "probe_sidecar": {
                        "started_at": _started_at,
                        "last_health_at": _last_health_at or None,
                        "last_health_error": _last_health_error,
                    },
                },
                ensure_ascii=False,
            ).encode("utf-8")
            self.send_response(int(code))
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            _request_total += 1
        except Exception:
            _request_fail_total += 1
            try:
                self.send_response(int(HTTPStatus.SERVICE_UNAVAILABLE))
                self.end_headers()
            except Exception:
                pass

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> None:
    global _shutting_down

    def _signal_handler(signum, frame):
        global _shutting_down
        _shutting_down = True

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    refresher = Thread(target=_background_refresh, name="sa_probe_refresher", daemon=True)
    refresher.start()

    httpd = ThreadingHTTPServer((PROBE_BIND, PROBE_PORT), Handler)
    httpd.daemon_threads = True
    httpd.timeout = 1
    print(f"[sa-probe-sidecar] listening on {PROBE_BIND}:{PROBE_PORT} (app port={APP_PORT})", flush=True)

    try:
        while not _shutting_down:
            httpd.handle_request()
    finally:
        try:
            httpd.server_close()
        except Exception:
            pass
        print("[sa-probe-sidecar] stopped", flush=True)


if __name__ == "__main__":
    main()
