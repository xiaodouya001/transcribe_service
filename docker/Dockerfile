# Multi-stage build for ECS Fargate
FROM python:3.12-slim as builder

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

FROM python:3.12-slim

# Non-root user
RUN adduser --disabled-password --gecos "" appuser
WORKDIR /app

COPY --from=builder /root/.local /home/appuser/.local
COPY config/ ./config/
COPY src/ ./src/
COPY pyproject.toml .

ENV PATH=/home/appuser/.local/bin:$PATH
ENV PYTHONPATH=/app:/app/src
ENV PYTHONUNBUFFERED=1

USER appuser

CMD ["python", "-m", "transcription_ingest.main"]
