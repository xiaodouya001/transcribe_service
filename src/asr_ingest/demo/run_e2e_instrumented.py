"""Instrumented E2E Demo - runs pipeline with DemoCollector, no main.py changes."""

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import AsyncIterator, NamedTuple

import httpx
import websockets

# Ensure project root in path
_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

# Force demo mode; FANOLAB_URL and MODE set in run_instrumented
os.environ["DEMO_MODE"] = "true"

import structlog

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ]
)

from asr_ingest.connector.base import TranscriptionEvent


async def _connect_sse_with_payload(url: str) -> AsyncIterator[tuple[TranscriptionEvent, dict]]:
    """SSE connect yielding (event, source_payload) for raw_payload recording."""
    async with httpx.AsyncClient(timeout=60.0) as client:
        async with client.stream("GET", url) as resp:
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


async def _connect_ws_with_payload(url: str) -> AsyncIterator[tuple[TranscriptionEvent, dict]]:
    """WebSocket connect yielding (event, source_payload) for raw_payload recording."""
    async with websockets.connect(url) as ws:
        async for message in ws:
            try:
                payload = json.loads(message)
            except json.JSONDecodeError:
                continue
            for event in TranscriptionEvent.from_vendor_payload(payload):
                yield event, payload


class InstrumentedResult(NamedTuple):
    """Result of instrumented pipeline run."""

    collector: "DemoCollector"
    output_path: str
    error: str | None = None


def _dedup_key(session_id: str, seq_no: int, processing_id: str = "") -> str:
    return f"dedup:{session_id}:{processing_id or ''}:{seq_no}"


async def run_instrumented(
    inject_duplicates: bool = False,
    shuffle_order: bool = False,
    max_retries: int = 3,
    transcripts_path: Path | str | None = None,
    mode: str = "sse",
) -> InstrumentedResult:
    """Run mock server + instrumented pipeline, return collector and output path.

    Does not modify main.py or business code. Uses composition: wraps
    MemoryDedup and EchoProducer with instrumentation.

    transcripts_path: single JSON file or directory of JSONs (stream mode).
    shuffle_order: mock sends payloads out of order; pipeline reorders before Kafka.
    mode: "sse" or "websocket" - transport protocol for connector.
    """
    from asr_ingest.demo.collector import DemoCollector
    from asr_ingest.demo.mock_server import run_server
    from asr_ingest.dedup import MemoryDedup
    from asr_ingest.producer import EchoProducer

    collector = DemoCollector()
    arrival_idx = [0]  # mutable for closure
    current_payload: list[dict | None] = [None]  # pipeline sets before should_emit

    # Instrumented dedup: delegate to MemoryDedup, record to collector with raw_payload
    class InstrumentedMemoryDedup(MemoryDedup):
        async def should_emit(
            self,
            session_id: str,
            seq_no: int,
            *,
            processing_id: str = "",
            created_at: str = "",
            **kwargs: object,
        ) -> bool:
            result = await super().should_emit(
                session_id,
                seq_no,
                processing_id=processing_id,
                created_at=created_at,
                **kwargs,
            )
            arrival_idx[0] += 1
            key = _dedup_key(session_id, seq_no, processing_id)
            collector.record_dedup(
                key=key,
                result="pass" if result else "filtered",
                session_id=session_id,
                seq_no=seq_no,
                arrival_order=arrival_idx[0],
                source_json=current_payload[0],
            )
            return result

    # Instrumented producer: record to collector with raw_payload, then delegate
    class InstrumentedEchoProducer(EchoProducer):
        async def send(
            self,
            session_id: str,
            seq_no: int,
            transcript: str,
            role: str = "",
            created_at: str = "",
            processing_status: str = "",
            source_json: dict | None = None,
            **kwargs: object,
        ) -> None:
            collector.record_kafka(
                session_id=session_id,
                seq_no=seq_no,
                transcript=transcript,
                role=role,
                created_at=created_at,
                processing_status=processing_status,
                source_json=source_json,
                **kwargs,
            )
            await super().send(
                session_id=session_id,
                seq_no=seq_no,
                transcript=transcript,
                role=role,
                created_at=created_at,
                processing_status=processing_status,
                **kwargs,
            )

    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

    base_url = "http://127.0.0.1:8765/sse" if mode == "sse" else "ws://127.0.0.1:8765/ws"
    url = base_url
    if inject_duplicates or shuffle_order:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        if inject_duplicates:
            qs["inject_duplicates"] = ["1"]
        if shuffle_order:
            qs["shuffle"] = ["1"]
        url = urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))

    os.environ["FANOLAB_URL"] = url
    os.environ["MODE"] = mode

    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
        output_path = f.name
    os.environ["DEMO_OUTPUT_FILE"] = output_path

    # Clear settings cache so new env is picked up
    from config.settings import get_settings
    get_settings.cache_clear()

    connect_fn = _connect_sse_with_payload if mode == "sse" else _connect_ws_with_payload
    dedup = InstrumentedMemoryDedup()
    producer = InstrumentedEchoProducer(output_file=output_path)

    async def _run_pipeline() -> None:
        """Pipeline loop. When shuffle_order: collect passed events, reorder by seq_no, then send."""
        passed_events: list[tuple] = []  # (event, payload)
        try:
            async for event, payload in connect_fn(url):
                current_payload[0] = payload
                if await dedup.should_emit(
                    event.session_id,
                    event.seq_no,
                    processing_id=event.processing_id,
                    created_at=event.created_at,
                ):
                    if shuffle_order:
                        passed_events.append((event, payload))
                    else:
                        await producer.send(
                            session_id=event.session_id,
                            seq_no=event.seq_no,
                            transcript=event.transcript,
                            role=event.role,
                            created_at=event.created_at,
                            processing_status=event.processing_status,
                            processing_id=event.processing_id,
                            source_json=payload,
                        )
            if shuffle_order and passed_events:
                for ev, pl in sorted(passed_events, key=lambda x: (x[0].session_id, x[0].seq_no)):
                    await producer.send(
                        session_id=ev.session_id,
                        seq_no=ev.seq_no,
                        transcript=ev.transcript,
                        role=ev.role,
                        created_at=ev.created_at,
                        processing_status=ev.processing_status,
                        processing_id=ev.processing_id,
                        source_json=pl,
                    )
        finally:
            await producer.flush()
            if hasattr(producer, "close"):
                producer.close()
            if hasattr(dedup, "close"):
                await dedup.close()

    from asr_ingest.demo.mock_server import DEFAULT_TRANSCRIPTS_PATH

    path = transcripts_path
    if path is not None:
        path = Path(path) if isinstance(path, str) else path
    else:
        path = DEFAULT_TRANSCRIPTS_PATH

    server_task = asyncio.create_task(run_server(port=8765, transcripts_path=path))
    await asyncio.sleep(0.5)

    last_error: str | None = None
    for attempt in range(max_retries):
        try:
            await _run_pipeline()
            break
        except Exception as e:
            last_error = str(e)
            if attempt < max_retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1))
    else:
        # All retries failed - cancel server, cleanup, return
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass
        Path(output_path).unlink(missing_ok=True)
        return InstrumentedResult(
            collector=collector,
            output_path=output_path,
            error=last_error,
        )

    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        pass

    # Self-healing: cleanup temp file to avoid accumulation
    Path(output_path).unlink(missing_ok=True)

    return InstrumentedResult(
        collector=collector,
        output_path=output_path,
        error=None,
    )
