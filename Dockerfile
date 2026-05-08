FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# ═══ 系统工具 ═════════════════════════════════════════════════════════════════
RUN apt-get update && apt-get install -y \
    curl wget gnupg ca-certificates git zip \
    python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

# ═══ Node.js 22 ═══════════════════════════════════════════════════════════════
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# ═══ pi-coding-agent ══════════════════════════════════════════════════════════
RUN npm install -g @mariozechner/pi-coding-agent

# ═══ 项目代码 ═════════════════════════════════════════════════════════════════
WORKDIR /app
COPY app/               ./app/
COPY cli.py main.py     ./
COPY prompts/           ./prompts/
COPY scripts/           ./scripts/
COPY config.example.json .env.example ./
COPY requirements.txt ./
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt -q
# 修复 Windows CRLF + 添加执行权限
RUN find . -name '*.sh' -exec sed -i 's/\r$//' {} + && chmod +x scripts/*.sh 2>/dev/null || true

# ═══ pi 配置目录 ══════════════════════════════════════════════════════════════
# pi 的全局配置目录，models.json 放这里才能被 pi 识别
# 使用 /app/.pi/agent 而非 /root/.pi/agent，使 pi-worker 也能读取
ENV PI_CODING_AGENT_DIR=/app/.pi/agent
RUN mkdir -p /app/.pi/agent

# ═══ pi-worker 用户（agent 子进程以非 root 身份运行）══════════════════════════
# uid=2001 无 home 无 shell，仅用于权限隔离
RUN useradd -u 2001 -M -s /sbin/nologin pi-worker

# ═══ 挂载点 ═══════════════════════════════════════════════════════════════════
#
# /data/target  — 待分析文件（只读）
# /data/config  — config.json + models.json + prompts/（只读）
# /data/output  — 输出目录
#
RUN mkdir -p /data/target /data/config /data/output /data/workspace /data/sessions
# 不声明 VOLUME（避免匿名卷遮盖 bind mount）

ENV PORT=3000
ENV OUTPUT_DIR=/data/output
ENV ARCHIVE_DIR=/data/output
ENV RESULT_DIR=/data/output
ENV SESSION_DIR=/data/sessions

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# ═══ 入口脚本 ═════════════════════════════════════════════════════════════════
# 启动前自动链接 models.json（如果挂载了的话）
COPY scripts/entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# 默认 REST API，覆盖: python3 cli.py /data/config/config.json
CMD ["python3", "main.py"]
