FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# ══ 系统工具（保留 nm/readelf/strings/binutils 供 Stage3 binary 预读）════
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl wget ca-certificates git zip \
    binutils file \
    && rm -rf /var/lib/apt/lists/*

# ══ 项目代码 ════════════════════════════════════════════════════════════
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt -q

COPY app/               ./app/
COPY cli.py main.py     ./
COPY prompts/           ./prompts/
COPY scripts/           ./scripts/
COPY config.example.json .env.example ./

# 修复 Windows CRLF + 添加执行权限
RUN find . -name '*.sh' -exec sed -i 's/\r$//' {} + \
    && chmod +x scripts/*.sh 2>/dev/null || true

# ══ 挂载点 ══════════════════════════════════════════════════════════════
#
# /data/target  — 待分析固件目录（只读）
# /data/config  — config.json + models.json（只读）
# /data/output  — 分析结果输出目录（读写）
# /data/sessions — （可选）会话数据持久化目录
#
RUN mkdir -p /data/target /data/config /data/output /data/workspace /data/sessions
# 不声明 VOLUME（避免匿名卷遮盖 bind mount）

ENV PORT=3000
ENV OUTPUT_DIR=/data/output
ENV ARCHIVE_DIR=/data/output
ENV RESULT_DIR=/data/output
ENV SESSION_DIR=/data/sessions

# model_factory 的 models.json 搜索路径（与容器挂载路径匹配）
ENV MODELS_JSON_PATH=/data/config/models.json

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# ══ 入口脚本 ════════════════════════════════════════════════════════════
COPY scripts/entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# 默认 REST API；命令行模式覆盖为 python3 cli.py "..."
CMD ["python3", "main.py"]
