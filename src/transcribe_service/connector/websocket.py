"""WebSocket connector - stream from STT provider via WebSocket."""

import time

import orjson
from typing import AsyncIterator

import structlog
import websockets

from transcribe_service.connector.base import TranscriptionEvent
from transcribe_service.connector.sse import _log_payload

log = structlog.get_logger(__name__)


class WebSocketConnector:
    """Connect to Vendor WebSocket, parse JSON messages, yield TranscriptionEvents."""

    def __init__(
        self,
        url: str,
        *,
        ping_interval: float | None = 20.0,
        ping_timeout: float | None = 20.0,
    ) -> None:
        self._url = url
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._normal_end = False  # True when vendor sends EOF (call ended)

    async def connect(self) -> AsyncIterator[tuple[TranscriptionEvent, dict]]:
        """Connect and stream messages, expand result.transcripts. Yields (event, raw_payload)."""
        kwargs: dict = {}
        if self._ping_interval is not None:
            kwargs["ping_interval"] = self._ping_interval
        if self._ping_timeout is not None:
            kwargs["ping_timeout"] = self._ping_timeout
        async with websockets.connect(self._url, **kwargs) as ws:
            async for message in ws:
                try:
                    payload = orjson.loads(message)
                except orjson.JSONDecodeError:
                    continue
                if payload.get("event") == "request" and payload.get("data") == "EOF":
                    log.info("Connector: 收到 EOF，通话结束，正常断开")
                    self._normal_end = True
                    return
                payload["_ingest_received_at"] = time.monotonic()
                _log_payload(payload, "connect")
                for event in TranscriptionEvent.from_vendor_payload(payload):
                    yield event, payload
