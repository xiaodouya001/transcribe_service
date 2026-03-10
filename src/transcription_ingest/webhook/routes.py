"""Webhook routes - POST /webhook/session receives Vendor session notifications."""

import re
from urllib.parse import urlparse

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

WEBHOOK_PATH = "/webhook/session"
URL_MAX_LENGTH = 2048
ALLOWED_SSE_SCHEMES = ("https", "http")  # http 仅用于本地 Demo
ALLOWED_WS_SCHEMES = ("wss", "ws")
BLOCKED_HOSTS = re.compile(
    r"^(localhost|127\.|10\.|172\.(1[6-9]|2\d|3[01])\.|192\.168\.|169\.254\.|::1)"
)


def _validate_stt_url(
    url: str, schemes: tuple[str, ...], label: str, allow_localhost: bool = False
) -> str:
    """Validate STT URL: scheme, length, no SSRF (block private/local hosts)."""
    if not url or len(url) > URL_MAX_LENGTH:
        raise ValueError(f"{label} length must be 1-{URL_MAX_LENGTH}")
    parsed = urlparse(url)
    if parsed.scheme not in schemes:
        raise ValueError(f"{label} scheme must be one of {schemes}")
    if not allow_localhost:
        host = (parsed.hostname or "").lower()
        if BLOCKED_HOSTS.match(host):
            raise ValueError(f"{label} must not target private/local host")
    return url


class WebhookPayload(BaseModel):
    """Vendor Webhook payload: metadata + ws_url + sse_url."""

    metadata: dict = {}
    ws_url: str = ""
    sse_url: str = ""


def create_app(connector_manager, *, redis_url: str = "", producer=None) -> FastAPI:
    """Create FastAPI app with Webhook route, connector_manager in app.state."""
    app = FastAPI(
        title="Transcribe Service",
        description="Webhook 接收 Vendor 会话通知，ConnectorManager 建连 STT",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    @app.get("/health")
    async def health():
        """Liveness: process alive."""
        return {"status": "ok"}

    @app.get("/ready")
    async def ready():
        """Readiness: Redis and Kafka reachable."""
        errors = []
        if redis_url:
            try:
                from redis.asyncio import Redis
                client = Redis.from_url(redis_url, decode_responses=True)
                await client.ping()
                await client.aclose()
            except Exception as e:
                errors.append(f"redis:{e}")
        if producer and hasattr(producer, "ensure_ready"):
            try:
                await producer.ensure_ready()
            except Exception as e:
                errors.append(f"kafka:{e}")
        if errors:
            return JSONResponse(status_code=503, content={"status": "not_ready", "errors": errors})
        return {"status": "ready"}

    router = APIRouter()
    allow_localhost = getattr(
        getattr(connector_manager, "_settings", None),
        "transcribe_service_ssrf_allow_localhost",
        False,
    )

    @router.post(WEBHOOK_PATH)
    async def webhook_session(request: Request, payload: WebhookPayload) -> JSONResponse:
        """Receive Vendor session notification, add to ConnectorManager."""
        manager = request.app.state.connector_manager
        session_id = (payload.metadata or {}).get("session_id", "")
        if not session_id:
            return JSONResponse(
                status_code=400,
                content={"error": "metadata.session_id required"},
            )
        use_sse = (manager._settings.transcribe_service_protocol or "sse").lower() == "sse"
        url = (payload.sse_url or "").strip() if use_sse else (payload.ws_url or "").strip()
        if url:
            try:
                schemes = ALLOWED_SSE_SCHEMES if use_sse else ALLOWED_WS_SCHEMES
                _validate_stt_url(url, schemes, "sse_url" if use_sse else "ws_url", allow_localhost)
            except ValueError as e:
                return JSONResponse(status_code=400, content={"error": str(e)})
        added = manager.add_session(
            metadata=payload.metadata,
            ws_url=payload.ws_url or "",
            sse_url=payload.sse_url or "",
        )
        if not added:
            return JSONResponse(
                status_code=503,
                content={"error": "session limit reached", "session_id": session_id},
            )
        return JSONResponse(status_code=202, content={"session_id": session_id})

    app.include_router(router)
    app.state.connector_manager = connector_manager
    app.state.redis_url = redis_url
    app.state.producer = producer
    return app
