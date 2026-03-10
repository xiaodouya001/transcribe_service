"""Connector layer - SSE and WebSocket clients for STT provider."""

from typing import TYPE_CHECKING, Any

from transcribe_service.connector.base import TranscriptionEvent
from transcribe_service.connector.sse import SseConnector
from transcribe_service.connector.websocket import WebSocketConnector

if TYPE_CHECKING:
    import httpx

__all__ = ["TranscriptionEvent", "SseConnector", "WebSocketConnector", "get_connector_for_url"]


def get_connector_for_url(
    url: str,
    *,
    use_sse: bool,
    last_event_id: str | None = None,
    read_timeout: float | None = None,
    ping_interval: float | None = 20.0,
    ping_timeout: float | None = 20.0,
    http_client: "httpx.AsyncClient | None" = None,
):
    """Factory: return SseConnector or WebSocketConnector based on url and use_sse."""
    if use_sse:
        return SseConnector(url, last_event_id, read_timeout=read_timeout, http_client=http_client)
    return WebSocketConnector(
        url,
        ping_interval=ping_interval,
        ping_timeout=ping_timeout,
    )
