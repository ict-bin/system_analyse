#!/usr/bin/env python3
"""
system_analyse 服务器启动入口

  python main.py               启动 REST API
  python main.py --port 8000   指定端口
"""

import os
import sys

import uvicorn
from dotenv import load_dotenv

from app.logging_utils import configure_container_logging, log_event

load_dotenv()
logger = configure_container_logging("01-system_analyse")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "3000"))
    if "--port" in sys.argv:
        idx = sys.argv.index("--port")
        if idx + 1 < len(sys.argv):
            port = int(sys.argv[idx + 1])

    log_event(
        logger,
        20,
        "starting system analyse api server",
        event="service_start",
        port=port,
        host="0.0.0.0",
    )

    uvicorn.run(
        "app.server:app",
        host="0.0.0.0",
        port=port,
        reload=os.environ.get("DEV", "") == "1",
        log_config=None,
    )
