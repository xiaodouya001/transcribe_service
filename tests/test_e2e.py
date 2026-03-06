"""E2E integration tests - pipeline with dedup verification."""

import asyncio
import json
import tempfile
from pathlib import Path

import pytest
from asr_ingest.connector.base import TranscriptionEvent
from asr_ingest.dedup import MemoryDedup
from asr_ingest.producer import EchoProducer


@pytest.mark.asyncio
async def test_pipeline_dedup_filters_duplicates() -> None:
    """Pipeline should filter duplicate (session_id, seq_no) via dedup."""
    dedup = MemoryDedup()
    received: list[dict] = []

    class CaptureProducer:
        async def send(self, **kwargs):
            received.append(kwargs)

        async def flush(self):
            pass

    # Simulate events: e0, e0 (dup), e1, e0 again (dup, after TTL might pass - but within same run)
    events = [
        TranscriptionEvent("s1", 0, "hello", "Agent"),
        TranscriptionEvent("s1", 0, "hello", "Agent"),  # duplicate
        TranscriptionEvent("s1", 1, "hi", "Customer"),
    ]

    producer = CaptureProducer()
    for event in events:
        if await dedup.should_emit(event.session_id, event.seq_no):
            await producer.send(
                session_id=event.session_id,
                seq_no=event.seq_no,
                transcript=event.transcript,
                role=event.role,
            )

    assert len(received) == 2  # Only first e0 and e1
    assert received[0]["seq_no"] == 0
    assert received[1]["seq_no"] == 1


@pytest.mark.asyncio
async def test_e2e_mock_server_integration() -> None:
    """Run mock server + pipeline, verify output file."""
    import os
    import sys

    _root = Path(__file__).resolve().parents[1]
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

    os.environ["DEMO_MODE"] = "true"
    os.environ["FANOLAB_URL"] = "http://127.0.0.1:8766/sse"
    os.environ["MODE"] = "sse"

    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
        out_path = f.name
    os.environ["DEMO_OUTPUT_FILE"] = out_path

    # Clear settings cache so new env is picked up
    from config.settings import get_settings
    get_settings.cache_clear()

    from asr_ingest.demo.mock_server import run_server
    from asr_ingest.main import run_pipeline

    transcripts_path = _root / "src" / "asr_ingest" / "demo" / "example" / "transcripts.json"
    if not transcripts_path.exists():
        pytest.skip("demo/example/transcripts.json not found")

    server_task = asyncio.create_task(run_server(port=8766, transcripts_path=transcripts_path))
    await asyncio.sleep(0.5)
    try:
        await run_pipeline()
    finally:
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass

    content = Path(out_path).read_text(encoding="utf-8")
    Path(out_path).unlink(missing_ok=True)
    lines = [l for l in content.strip().split("\n") if l]
    if not lines:
        pytest.fail("No output in demo_output.jsonl")
    data = json.loads(lines[0])
    payload = data.get("cleaned", data)
    assert "session_id" in payload
    assert "seq_no" in payload
    assert "transcript" in payload
