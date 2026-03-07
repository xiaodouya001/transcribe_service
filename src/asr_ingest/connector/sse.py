"""SSE connector - stream from Fanolab via Server-Sent Events."""

import json
from typing import AsyncIterator, Protocol

import httpx
import structlog

from asr_ingest.connector.base import TranscriptionEvent

log = structlog.get_logger(__name__)


def _log_payload(payload: dict, stage: str) -> None:
    """Log payload summary for full-chain tracing."""
    r = payload.get("result") or {}
    cs = r.get("callStatus") or {}
    session_id = cs.get("sessionId", "")
    processing_id = r.get("processingId", "")
    n = len(r.get("transcripts") or [])
    log.info(
        "Connector: 从 ASR 收到 payload",
        stage=stage,
        session_id=session_id,
        processing_id=processing_id,
        transcript_count=n,
    )


class BufferBackend(Protocol):
    """Protocol for buffer backends (e.g. RedisBuffer)."""

    async def push(self, payload: dict) -> str: ...


class SseConnector:
    """Connect to Vendor SSE endpoint, parse and yield TranscriptionEvents."""

    def __init__(
        self,
        url: str,
        last_event_id: str | None = None,
        *,
        read_timeout: float | None = None,
    ) -> None:
        self._url = url
        self._last_event_id = last_event_id
        self._read_timeout = read_timeout
        self.last_event_id: str | None = last_event_id  # Updated during stream

    async def connect(self) -> AsyncIterator[tuple[TranscriptionEvent, dict]]:
        """Stream SSE, parse data lines, expand result.transcripts. Yields (event, raw_payload)."""
        headers = {}
        if self._last_event_id:
            headers["Last-Event-ID"] = self._last_event_id

        if self._read_timeout is not None:
            timeout = httpx.Timeout(connect=10.0, read=self._read_timeout, write=60.0, pool=60.0)
        else:
            timeout = 60.0
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("GET", self._url, headers=headers) as resp:
                resp.raise_for_status()
                buffer = ""
                async for chunk in resp.aiter_text():
                    buffer += chunk
                    while "\n" in buffer or "\r\n" in buffer:
                        line, _, buffer = buffer.partition("\n")
                        line = line.strip()
                        if line.startswith("id:"):
                            self.last_event_id = line[3:].strip() or None
                            continue
                        if line.startswith("data:"):
                            data_str = line[5:].strip()
                            if data_str == "[DONE]" or not data_str:
                                continue
                            try:
                                payload = json.loads(data_str)
                            except json.JSONDecodeError:
                                continue
                            _log_payload(payload, "connect")
                            for event in TranscriptionEvent.from_vendor_payload(payload):
                                yield event, payload

    async def connect_and_push(self, buffer: BufferBackend) -> None:
        """Stream SSE, parse data lines, push raw payload to buffer (no yield)."""
        if self._read_timeout is not None:
            timeout = httpx.Timeout(connect=10.0, read=self._read_timeout, write=60.0, pool=60.0)
        else:
            timeout = 60.0
        log.info("Connector: 正在连接 SSE", url=self._url)
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("GET", self._url) as resp:
                resp.raise_for_status()
                log.info("Connector: 已连接 SSE", url=self._url, status=resp.status_code)
                buf = ""
                async for chunk in resp.aiter_text():
                    buf += chunk
                    while "\n" in buf or "\r\n" in buf:
                        line, _, buf = buf.partition("\n")
                        line = line.strip()
                        if line.startswith("id:"):
                            self.last_event_id = line[3:].strip() or None
                            continue
                        if line.startswith("data:"):
                            data_str = line[5:].strip()
                            if data_str == "[DONE]" or not data_str:
                                continue
                            try:
                                payload = json.loads(data_str)
                            except json.JSONDecodeError:
                                continue
                            _log_payload(payload, "connect_and_push")
                            await buffer.push(payload)
