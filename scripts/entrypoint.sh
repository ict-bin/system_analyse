#!/bin/bash
# 容器入口脚本

set -e

PI_DIR="${PI_CODING_AGENT_DIR:-/root/.pi/agent}"
mkdir -p "$PI_DIR"

# 链接 models.json
if [ -f /data/config/models.json ]; then
    ln -sf /data/config/models.json "$PI_DIR/models.json"
    echo "[entrypoint] models.json linked → $PI_DIR/models.json"
fi

# 生成/合并 settings.json，并显式关闭默认思考
python3 - <<'PY'
import json
import os
from pathlib import Path
p = Path(os.environ.get("PI_CODING_AGENT_DIR", "/root/.pi/agent")) / "settings.json"
try:
    data = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
    if not isinstance(data, dict):
        data = {}
except Exception:
    data = {}
data.setdefault("theme", "dark")
data["defaultThinkingLevel"] = "off"
p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
print(f"[entrypoint] settings.json updated → {p}")
PY

if [ -d /data/config/prompts ]; then
    echo "[entrypoint] custom prompts found at /data/config/prompts/"
fi

exec "$@"
