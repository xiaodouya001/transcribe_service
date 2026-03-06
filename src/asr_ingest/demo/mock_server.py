"""Mock Vendor SSE server - streams transcripts.json or directory of JSONs."""

import asyncio
import json
from pathlib import Path

import structlog
from aiohttp import web

log = structlog.get_logger()

# Default path: scenarios > project root > example
# __file__ = demo/mock_server.py, .parent = demo/
_DEMO_ROOT = Path(__file__).resolve().parent
_PROJECT_ROOT = _DEMO_ROOT.parents[2]  # demo -> asr_ingest -> src -> project_root
_SCENARIOS_SINGLE = _DEMO_ROOT / "scenarios" / "single_response_multi_transcriptions" / "transcripts.json"
_EXAMPLE = _DEMO_ROOT / "example" / "transcripts.json"
DEFAULT_TRANSCRIPTS_PATH = (
    _SCENARIOS_SINGLE
    if _SCENARIOS_SINGLE.exists()
    else (_PROJECT_ROOT / "transcripts.json" if (_PROJECT_ROOT / "transcripts.json").exists() else _EXAMPLE)
)
# Legacy: directory for multi-JSON streaming
TRANSCRIPTS_DIR = _DEMO_ROOT / "transcripts"
STREAM_DELAY_SEC = 0.15


def _load_payloads(path: Path) -> list[dict]:
    """Load payload(s): single file or directory of JSONs (sorted by name)."""
    if path.is_dir():
        files = sorted(path.glob("*.json"))
        if not files:
            raise web.HTTPBadRequest(text=f"No .json files in {path}")
        return [json.loads(f.read_text(encoding="utf-8")) for f in files]
    data = json.loads(path.read_text(encoding="utf-8"))
    if not data.get("success") or "result" not in data:
        raise web.HTTPBadRequest(text="Invalid transcripts.json")
    return [data]


def _shuffle_payloads(payloads: list[dict], shuffle: bool) -> list[dict]:
    """Optionally shuffle payload order to simulate out-of-order arrival."""
    if not shuffle or len(payloads) <= 1:
        return payloads
    import random
    out = payloads.copy()
    random.shuffle(out)
    return out


async def sse_handler(request: web.Request) -> web.StreamResponse:
    """Stream SSE events from transcripts.json or transcripts/ directory."""
    path = request.app.get("transcripts_path", DEFAULT_TRANSCRIPTS_PATH)
    payloads = _load_payloads(path)
    inject_duplicates = request.query.get("inject_duplicates", "").lower() in ("1", "true", "yes")
    shuffle_order = request.query.get("shuffle", "").lower() in ("1", "true", "yes")
    payloads = _shuffle_payloads(payloads, shuffle_order)

    response = web.StreamResponse()
    response.headers["Content-Type"] = "text/event-stream"
    response.headers["Cache-Control"] = "no-cache"
    response.headers["Connection"] = "keep-alive"
    await response.prepare(request)

    event_id = 0
    for data in payloads:
        payload = json.dumps(data, ensure_ascii=False)
        await response.write(f"id: {event_id}\ndata: {payload}\n\n".encode("utf-8"))
        event_id += 1
        if len(payloads) > 1:
            await asyncio.sleep(STREAM_DELAY_SEC)

    if inject_duplicates:
        for data in payloads:
            payload = json.dumps(data, ensure_ascii=False)
            await response.write(f"id: {event_id}\ndata: {payload}\n\n".encode("utf-8"))
            event_id += 1
            if len(payloads) > 1:
                await asyncio.sleep(STREAM_DELAY_SEC)

    await response.write(f"data: [DONE]\n\n".encode("utf-8"))
    await response.write_eof()
    return response


async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    """WebSocket: send JSON payloads one by one (same logic as SSE)."""
    path = request.app.get("transcripts_path", DEFAULT_TRANSCRIPTS_PATH)
    payloads = _load_payloads(path)
    inject_duplicates = request.query.get("inject_duplicates", "").lower() in ("1", "true", "yes")
    shuffle_order = request.query.get("shuffle", "").lower() in ("1", "true", "yes")
    payloads = _shuffle_payloads(payloads, shuffle_order)

    ws = web.WebSocketResponse()
    await ws.prepare(request)

    try:
        for data in payloads:
            await ws.send_str(json.dumps(data, ensure_ascii=False))
            if len(payloads) > 1:
                await asyncio.sleep(STREAM_DELAY_SEC)
        if inject_duplicates:
            for data in payloads:
                await ws.send_str(json.dumps(data, ensure_ascii=False))
                if len(payloads) > 1:
                    await asyncio.sleep(STREAM_DELAY_SEC)
    finally:
        await ws.close()
    return ws


def create_app(transcripts_path: Path | None = None) -> web.Application:
    """Create aiohttp app with SSE and WebSocket endpoints."""
    app = web.Application()
    if transcripts_path:
        app["transcripts_path"] = transcripts_path
    app.router.add_get("/sse", sse_handler)
    app.router.add_get("/ws", websocket_handler)
    return app


async def run_server(host: str = "127.0.0.1", port: int = 8765, transcripts_path: Path | None = None) -> None:
    """Run mock server with SSE and WebSocket endpoints."""
    app = create_app(transcripts_path)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    log.info("Mock server started", sse_url=f"http://{host}:{port}/sse", ws_url=f"ws://{host}:{port}/ws")
    try:
        await asyncio.Event().wait()  # Run until cancelled
    except asyncio.CancelledError:
        pass
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(run_server())
