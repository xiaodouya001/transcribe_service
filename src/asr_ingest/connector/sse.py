"""SSE connector - stream from Fanolab via Server-Sent Events."""

import json
from typing import AsyncIterator, Protocol

import httpx

from asr_ingest.connector.base import TranscriptionEvent


class BufferBackend(Protocol):
    """Protocol for buffer backends (e.g. RedisBuffer)."""

    async def push(self, payload: dict) -> str: ...


class SSEConnector:
    """Connect to Vendor SSE endpoint, parse and yield TranscriptionEvents."""

    def __init__(self, url: str, last_event_id: str | None = None) -> None:
        self._url = url
        self._last_event_id = last_event_id

    async def connect(self) -> AsyncIterator[tuple[TranscriptionEvent, dict]]:
        """Stream SSE, parse data lines, expand result.transcripts. Yields (event, raw_payload)."""
        headers = {}
        if self._last_event_id:
            headers["Last-Event-ID"] = self._last_event_id

        async with httpx.AsyncClient(timeout=60.0) as client:
            async with client.stream("GET", self._url, headers=headers) as resp:
                resp.raise_for_status()
                buffer = ""
                async for chunk in resp.aiter_text():
                    buffer += chunk
                    while "\n" in buffer or "\r\n" in buffer:
                        line, _, buffer = buffer.partition("\n")
                        line = line.strip()
                        if line.startswith("data:"):
                            data_str = line[5:].strip()
                            if data_str == "[DONE]" or not data_str:
                                continue
                            try:
                                payload = json.loads(data_str)
                            except json.JSONDecodeError:
                                continue
                            for event in TranscriptionEvent.from_vendor_payload(payload):
                                yield event, payload

    async def connect_and_push(self, buffer: BufferBackend) -> None:
        """Stream SSE, parse data lines, push raw payload to buffer (no yield)."""
        async with httpx.AsyncClient(timeout=60.0) as client:
            async with client.stream("GET", self._url) as resp:
                resp.raise_for_status()
                buf = ""
                async for chunk in resp.aiter_text():
                    buf += chunk
                    while "\n" in buf or "\r\n" in buf:
                        line, _, buf = buf.partition("\n")
                        line = line.strip()
                        if line.startswith("data:"):
                            data_str = line[5:].strip()
                            if data_str == "[DONE]" or not data_str:
                                continue
                            try:
                                payload = json.loads(data_str)
                            except json.JSONDecodeError:
                                continue
                            await buffer.push(payload)
