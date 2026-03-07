"""Tests for producer layer."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from asr_ingest.producer import KafkaProducer, get_producer_backend


@pytest.mark.asyncio
async def test_get_producer_backend() -> None:
    """get_producer_backend returns KafkaProducer."""
    backend = get_producer_backend(
        kafka_bootstrap="localhost:9092",
        kafka_topic="asr_realtime_text",
    )
    assert isinstance(backend, KafkaProducer)


@pytest.mark.asyncio
async def test_kafka_producer_send_payload_structure() -> None:
    """KafkaProducer.send builds correct payload with raw and cleaned."""
    mock_producer = AsyncMock()
    mock_producer.send_and_wait = AsyncMock(return_value=MagicMock())

    with patch.object(KafkaProducer, "_get_producer", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_producer
        producer = KafkaProducer(bootstrap_servers="localhost:9092", topic="test_topic")

        await producer.send(
            session_id="s1",
            seq_no=0,
            transcript="hello",
            role="Agent",
            raw_payload={"foo": "bar"},
            cleaned={"session_id": "s1", "seq_no": 0, "transcript": "hello"},
        )

        mock_producer.send_and_wait.assert_called_once()
        call_kwargs = mock_producer.send_and_wait.call_args[1]
        assert call_kwargs["key"] == b"s1"
        import json
        value = json.loads(call_kwargs["value"].decode("utf-8"))
        assert value["raw"] == {"foo": "bar"}
        assert value["cleaned"]["transcript"] == "hello"
