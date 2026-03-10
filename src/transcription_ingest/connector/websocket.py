"""WebSocket connector - stream from STT provider via WebSocket."""

import json
import time
from typing import AsyncIterator

import structlog
import websockets

from transcription_ingest.connector.base import TranscriptionEvent
from transcription_ingest.connector.sse import _log_payload

log = structlog.get_logger()


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
                    payload = json.loads(message)
                except json.JSONDecodeError:
                    continue
                payload["_ingest_received_at"] = time.monotonic()
                _log_payload(payload, "connect")
                for event in TranscriptionEvent.from_vendor_payload(payload):
                    yield event, payload
