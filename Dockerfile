ARG SECFLOW_PI_AGENT_RUNTIME_IMAGE=ghcr.io/runshine/secflow-base-pi-agent-runtime:20260602
FROM ${SECFLOW_PI_AGENT_RUNTIME_IMAGE}

ARG SECFLOW_BUILD_VERSION=""

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt -q

COPY app/ ./app/
COPY cli.py main.py probe_sidecar.py ./
COPY prompts/ ./prompts/
COPY scripts/ ./scripts/
COPY config.example.json .env.example ./
RUN printf '{"build_version":"%s"}\n' "$SECFLOW_BUILD_VERSION" > /app/build_meta.json

# Normalize shell scripts copied from mixed Windows/Linux environments.
RUN find . -name '*.sh' -exec sed -i 's/\r$//' {} + \
    && chmod +x scripts/*.sh 2>/dev/null || true

ENV PI_CODING_AGENT_DIR=/root/.pi/agent
RUN mkdir -p "${PI_CODING_AGENT_DIR}"

RUN mkdir -p /data/target /data/config /data/output /data/workspace /data/sessions

ENV PORT=3000
ENV OUTPUT_DIR=/data/output
ENV ARCHIVE_DIR=/data/output
ENV RESULT_DIR=/data/output
ENV SESSION_DIR=/data/sessions

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:18080/healthz || exit 1

COPY scripts/entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

CMD ["python3", "main.py"]
