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

# 生成 settings.json
if [ ! -f "$PI_DIR/settings.json" ]; then
    echo '{"theme":"dark"}' > "$PI_DIR/settings.json"
    echo "[entrypoint] settings.json generated → $PI_DIR/settings.json"
fi

if [ -d /data/config/prompts ]; then
    echo "[entrypoint] custom prompts found at /data/config/prompts/"
fi

exec "$@"
