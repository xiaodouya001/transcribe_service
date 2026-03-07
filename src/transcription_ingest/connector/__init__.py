"""Connector layer - SSE and WebSocket clients for STT provider."""

from typing import Any

from transcription_ingest.connector.base import TranscriptionEvent
from transcription_ingest.connector.sse import SseConnector
from transcription_ingest.connector.websocket import WebSocketConnector

__all__ = ["TranscriptionEvent", "SseConnector", "WebSocketConnector", "get_connector"]


def get_connector(settings: Any, last_event_id: str | None = None):
    """Factory: return SseConnector or WebSocketConnector based on settings.mode."""
    if settings.mode == "sse":
        return SseConnector(
            settings.stt_provider_url,
            last_event_id,
            read_timeout=getattr(settings, "sse_read_timeout", None),
        )
    return WebSocketConnector(
        settings.stt_provider_url,
        ping_interval=getattr(settings, "ws_ping_interval", 20.0),
        ping_timeout=getattr(settings, "ws_ping_timeout", 20.0),
    )
