FROM m.daocloud.io/docker.io/library/ubuntu:24.04

ARG SECFLOW_BUILD_VERSION=""

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# ══�?系统工具 ════════════════════════════════════════════════════════════════�?
RUN apt-get update && apt-get install -y \
    curl wget gnupg ca-certificates git zip \
    python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

# ══�?Node.js 22 ══════════════════════════════════════════════════════════════�?
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# ══�?pi-coding-agent ══════════════════════════════════════════════════════════
RUN npm install -g @mariozechner/pi-coding-agent

# ══�?项目代码 ════════════════════════════════════════════════════════════════�?
WORKDIR /app
COPY app/               ./app/
COPY cli.py main.py     ./
COPY prompts/           ./prompts/
COPY scripts/           ./scripts/
COPY config.example.json .env.example ./
COPY requirements.txt ./
RUN printf '{"build_version":"%s"}\n' "$SECFLOW_BUILD_VERSION" > /app/build_meta.json
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt -q
# 修复 Windows CRLF + 添加执行权限
RUN find . -name '*.sh' -exec sed -i 's/\r$//' {} + && chmod +x scripts/*.sh 2>/dev/null || true

# ══�?pi 配置目录 ══════════════════════════════════════════════════════════════
# pi 的全局配置目录，models.json 放这里才能被 pi 识别
# 容器启动脚本会将 /data/config/models.json 链接到此�?
ENV PI_CODING_AGENT_DIR=/root/.pi/agent
RUN mkdir -p /root/.pi/agent

# ══�?挂载�?══════════════════════════════════════════════════════════════════�?
#
# /data/target  �?待分析文件（只读�?
# /data/config  �?config.json + models.json + prompts/（只读）
# /data/output  �?输出目录
#
RUN mkdir -p /data/target /data/config /data/output /data/workspace /data/sessions
# 不声�?VOLUME（避免匿名卷遮盖 bind mount�?

ENV PORT=3000
ENV OUTPUT_DIR=/data/output
ENV ARCHIVE_DIR=/data/output
ENV RESULT_DIR=/data/output
ENV SESSION_DIR=/data/sessions

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# ══�?入口脚本 ════════════════════════════════════════════════════════════════�?
# 启动前自动链�?models.json（如果挂载了的话�?
COPY scripts/entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# 默认 REST API，覆�? python3 cli.py /data/config/config.json
CMD ["python3", "main.py"]
