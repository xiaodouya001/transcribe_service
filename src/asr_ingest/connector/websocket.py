"""WebSocket connector - stream from Fanolab via WebSocket."""

import json
from typing import AsyncIterator

import websockets

from asr_ingest.connector.base import TranscriptionEvent
from asr_ingest.connector.sse import BufferBackend


class WebSocketConnector:
    """Connect to Vendor WebSocket, parse JSON messages, yield TranscriptionEvents."""

    def __init__(self, url: str) -> None:
        self._url = url

    async def connect(self) -> AsyncIterator[tuple[TranscriptionEvent, dict]]:
        """Connect and stream messages, expand result.transcripts. Yields (event, raw_payload)."""
        async with websockets.connect(self._url) as ws:
            async for message in ws:
                try:
                    payload = json.loads(message)
                except json.JSONDecodeError:
                    continue
                for event in TranscriptionEvent.from_vendor_payload(payload):
                    yield event, payload

    async def connect_and_push(self, buffer: BufferBackend) -> None:
        """Connect and stream messages, push raw payload to buffer (no yield)."""
        async with websockets.connect(self._url) as ws:
            async for message in ws:
                try:
                    payload = json.loads(message)
                except json.JSONDecodeError:
                    continue
                await buffer.push(payload)
