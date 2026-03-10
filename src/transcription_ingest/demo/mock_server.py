"""Minimal mock STT server for local demo - inject queue + SSE + frontend. Not used by main."""

import asyncio
import json
from pathlib import Path

import structlog
from aiohttp import web

log = structlog.get_logger(__name__)

_DEMO_ROOT = Path(__file__).resolve().parent
QUEUE_WAIT_TIMEOUT = 120.0
KEEPALIVE_INTERVAL = 30.0


def _get_session_queue(app: web.Application, session_id: str) -> asyncio.Queue:
    """Get or create queue for session_id."""
    queues: dict[str, asyncio.Queue] = app["session_queues"]
    if session_id not in queues:
        queues[session_id] = asyncio.Queue()
    return queues[session_id]


def _validate_payload(data: dict) -> None:
    """Validate payload for TranscriptionEvent.from_vendor_payload."""
    if not data.get("success") or "result" not in data:
        raise ValueError("Payload must have success and result")
    r = data["result"]
    if "callStatus" not in r or "sessionId" not in r.get("callStatus", {}):
        raise ValueError("result.callStatus.sessionId required")
    if "transcripts" not in r or not isinstance(r["transcripts"], list):
        raise ValueError("result.transcripts must be an array")


async def inject_handler(request: web.Request) -> web.Response:
    """POST /inject: accept JSON, put into session-specific queue."""
    try:
        data = await request.json()
    except json.JSONDecodeError as e:
        raise web.HTTPBadRequest(text=f"Invalid JSON: {e}")
    try:
        _validate_payload(data)
    except ValueError as e:
        raise web.HTTPBadRequest(text=str(e))
    sid = data.get("result", {}).get("callStatus", {}).get("sessionId", "")
    if not sid:
        raise web.HTTPBadRequest(text="result.callStatus.sessionId required")
    queue = _get_session_queue(request.app, sid)
    await queue.put(data)
    log.info("Mock: 收到前端注入的 JSON payload", session_id=sid)
    return web.json_response({"ok": True})


async def sse_handler(request: web.Request) -> web.StreamResponse:
    """GET /sse?session_id=xxx: stream from session-specific queue with keepalive."""
    import time

    session_id = request.query.get("session_id", "")
    if not session_id:
        raise web.HTTPBadRequest(text="session_id query param required")
    queue = _get_session_queue(request.app, session_id)
    response = web.StreamResponse()
    response.headers["Content-Type"] = "text/event-stream"
    response.headers["Cache-Control"] = "no-cache"
    response.headers["Connection"] = "keep-alive"
    await response.prepare(request)

    event_id = 0
    last_payload_time = time.monotonic()
    try:
        while True:
            try:
                data = await asyncio.wait_for(queue.get(), timeout=KEEPALIVE_INTERVAL)
                last_payload_time = time.monotonic()
                payload = json.dumps(data, ensure_ascii=False)
                await response.write(f"id: {event_id}\ndata: {payload}\n\n".encode("utf-8"))
                event_id += 1
            except asyncio.TimeoutError:
                if time.monotonic() - last_payload_time >= QUEUE_WAIT_TIMEOUT:
                    break
                await response.write(b": keepalive\n\n")
    except asyncio.CancelledError:
        raise
    finally:
        await response.write(b"data: [DONE]\n\n")
        await response.write_eof()
    return response


async def index_handler(request: web.Request) -> web.Response:
    """GET /: serve frontend."""
    path = _DEMO_ROOT / "static" / "index.html"
    if not path.exists():
        raise web.HTTPNotFound(text="index.html not found")
    return web.FileResponse(path)


def create_app() -> web.Application:
    """Create app with per-session queues, POST /inject, GET /sse?session_id=xxx, GET /."""
    app = web.Application()
    app["session_queues"] = {}  # session_id -> asyncio.Queue
    app.router.add_post("/inject", inject_handler)
    app.router.add_get("/sse", sse_handler)
    app.router.add_get("/", index_handler)
    return app


async def run(host: str = "127.0.0.1", port: int = 8765) -> None:
    """Run mock server until cancelled."""
    app = create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    log.info(
        "Mock: 本地 Demo 服务已启动（多会话：/sse?session_id=xxx）",
        frontend=f"http://{host}:{port}/",
        sse=f"http://{host}:{port}/sse?session_id=<session_id>",
    )
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await runner.cleanup()
