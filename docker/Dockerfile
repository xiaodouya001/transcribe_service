# Multi-stage build for ECS Fargate
FROM python:3.12-slim AS builder

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

FROM python:3.12-slim

# Non-root user
RUN adduser --disabled-password --gecos "" appuser
WORKDIR /app

COPY --from=builder /root/.local /home/appuser/.local
COPY src/ ./src/

ENV PATH=/home/appuser/.local/bin:$PATH
ENV PYTHONPATH=/app:/app/src
ENV PYTHONUNBUFFERED=1
# APP_ENV/REDIS_URL/KAFKA_BOOTSTRAP_SERVERS are intentionally not defaulted here.
# Deployed runtimes must inject them explicitly to keep fail-fast startup semantics.

USER appuser

CMD ["python", "-m", "realtime_transcribe_service.main"]

