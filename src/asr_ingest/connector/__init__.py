"""Connector layer - SSE and WebSocket clients for Fanolab ASR."""

from asr_ingest.connector.base import TranscriptionEvent
from asr_ingest.connector.sse import SSEConnector
from asr_ingest.connector.websocket import WebSocketConnector

__all__ = ["TranscriptionEvent", "SSEConnector", "WebSocketConnector"]
