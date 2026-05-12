#!/bin/bash
# 容器入口脚本（已移除 pi 相关设置）

set -e

# ── models.json 检查 ────────────────────────────────────────────────
if [ -f /data/config/models.json ]; then
    echo "[entrypoint] models.json found at /data/config/models.json"
    PROVIDER_COUNT=$(python3 -c "
import json, sys
try:
    d = json.load(open('/data/config/models.json'))
    print(len(d.get('providers', {})))
except Exception as e:
    print(0)
" 2>/dev/null || echo 0)
    echo "[entrypoint] providers loaded: ${PROVIDER_COUNT}"
else
    echo "[entrypoint] WARNING: /data/config/models.json not found"
    echo "[entrypoint] Models will fall back to OPENAI_API_KEY / OPENAI_BASE_URL env vars"
fi

# ── 自定义 prompts 检查 ─────────────────────────────────────────────
if [ -d /data/config/prompts ]; then
    echo "[entrypoint] custom prompts found at /data/config/prompts/"
fi

# ── 输出目录准备 ────────────────────────────────────────────────────
mkdir -p /data/output /data/workspace /data/sessions
echo "[entrypoint] data directories ready"

exec "$@"
