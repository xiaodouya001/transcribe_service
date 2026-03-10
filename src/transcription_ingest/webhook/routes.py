"""Webhook routes - POST /webhook/session receives Vendor session notifications."""

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

WEBHOOK_PATH = "/webhook/session"


class WebhookPayload(BaseModel):
    """Vendor Webhook payload: metadata + ws_url + sse_url."""

    metadata: dict = {}
    ws_url: str = ""
    sse_url: str = ""


def create_app(connector_manager) -> FastAPI:
    """Create FastAPI app with Webhook route, connector_manager in app.state."""
    app = FastAPI(
        title="Transcribe Service",
        description="Webhook 接收 Vendor 会话通知，ConnectorManager 建连 STT",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    @app.get("/")
    async def root():
        return {"service": "Transcribe Service", "docs": "/docs", "redoc": "/redoc"}

    router = APIRouter()

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
        manager.add_session(
            metadata=payload.metadata,
            ws_url=payload.ws_url or "",
            sse_url=payload.sse_url or "",
        )
        return JSONResponse(status_code=202, content={"session_id": session_id})

    app.include_router(router)
    app.state.connector_manager = connector_manager
    return app
