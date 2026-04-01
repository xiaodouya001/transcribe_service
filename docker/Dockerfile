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
# No app secrets in the image. For APP_ENV=deployed, inject bootstrap env (APP_ENV,
# AWS_SECRETS_MANAGER_SECRET_ID, optional AWS_REGION); JSON config is loaded from Secrets Manager at startup.

USER appuser

CMD ["python", "-m", "realtime_transcribe_service.main"]

