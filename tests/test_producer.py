"""Tests for producer layer."""

import json
import tempfile
from pathlib import Path

import pytest
from asr_ingest.producer import EchoProducer, get_producer_backend


@pytest.mark.asyncio
async def test_echo_producer_send() -> None:
    """EchoProducer should write to file and print."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        path = f.name
    try:
        producer = EchoProducer(output_file=path)
        await producer.send(
            session_id="s1",
            seq_no=0,
            transcript="hello",
            role="Agent",
        )
        await producer.flush()
        producer.close()
        content = Path(path).read_text(encoding="utf-8")
        data = json.loads(content.strip())
        assert "raw" in data and "cleaned" in data
        c = data["cleaned"]
        assert c["session_id"] == "s1"
        assert c["seq_no"] == 0
        assert c["transcript"] == "hello"
        assert c["role"] == "Agent"
    finally:
        Path(path).unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_get_producer_backend_demo() -> None:
    """Demo mode returns EchoProducer."""
    backend = get_producer_backend(demo_mode=True)
    assert isinstance(backend, EchoProducer)


@pytest.mark.asyncio
async def test_get_producer_backend_prod() -> None:
    """Non-demo returns KafkaProducer."""
    from asr_ingest.producer import KafkaProducer

    backend = get_producer_backend(
        demo_mode=False,
        kafka_bootstrap="localhost:9092",
        kafka_topic="asr_realtime_text",
    )
    assert isinstance(backend, KafkaProducer)
