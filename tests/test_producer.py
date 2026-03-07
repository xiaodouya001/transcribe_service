"""Tests for producer layer. Uses unittest.mock for Kafka (no real Kafka required)."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from transcription_ingest.producer import KafkaProducer, get_producer_backend


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
        value = json.loads(call_kwargs["value"].decode("utf-8"))
        assert value["raw"] == {"foo": "bar"}
        assert value["cleaned"]["transcript"] == "hello"


@pytest.mark.asyncio
async def test_kafka_producer_send_without_raw_cleaned() -> None:
    """KafkaProducer.send uses cleaned_dict when raw/cleaned not provided."""
    mock_producer = AsyncMock()
    mock_producer.send_and_wait = AsyncMock(return_value=MagicMock())

    with patch.object(KafkaProducer, "_get_producer", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_producer
        producer = KafkaProducer(bootstrap_servers="localhost:9092", topic="test_topic")

        await producer.send(
            session_id="s1",
            seq_no=1,
            transcript="hi",
            role="Customer",
            created_at="2025-01-01T00:00:00Z",
            processing_status="DONE",
        )

        call_kwargs = mock_producer.send_and_wait.call_args[1]
        value = json.loads(call_kwargs["value"].decode("utf-8"))
        assert value["raw"] is None
        assert value["cleaned"]["session_id"] == "s1"
        assert value["cleaned"]["seq_no"] == 1
        assert value["cleaned"]["transcript"] == "hi"
        assert value["cleaned"]["role"] == "Customer"
        assert value["cleaned"]["created_at"] == "2025-01-01T00:00:00Z"
        assert value["cleaned"]["processing_status"] == "DONE"


@pytest.mark.asyncio
async def test_kafka_producer_ensure_ready() -> None:
    """ensure_ready calls _get_producer (validates Kafka reachable)."""
    with patch.object(KafkaProducer, "_get_producer", new_callable=AsyncMock) as mock_get:
        producer = KafkaProducer(bootstrap_servers="localhost:9092", topic="test_topic")
        await producer.ensure_ready()
        mock_get.assert_called_once()


@pytest.mark.asyncio
async def test_kafka_producer_flush() -> None:
    """flush calls producer.flush when producer exists."""
    mock_producer = AsyncMock()
    mock_producer.flush = AsyncMock()

    with patch.object(KafkaProducer, "_get_producer", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_producer
        producer = KafkaProducer(bootstrap_servers="localhost:9092", topic="test_topic")
        producer._producer = mock_producer
        await producer.flush()
        mock_producer.flush.assert_called_once()


@pytest.mark.asyncio
async def test_kafka_producer_flush_no_producer() -> None:
    """flush does nothing when producer is None."""
    producer = KafkaProducer(bootstrap_servers="localhost:9092", topic="test_topic")
    await producer.flush()  # Should not raise


@pytest.mark.asyncio
async def test_kafka_producer_close() -> None:
    """close stops producer and sets to None."""
    mock_producer = AsyncMock()
    mock_producer.stop = AsyncMock()

    producer = KafkaProducer(bootstrap_servers="localhost:9092", topic="test_topic")
    producer._producer = mock_producer
    await producer.close()
    mock_producer.stop.assert_called_once()
    assert producer._producer is None


@pytest.mark.asyncio
async def test_kafka_producer_close_no_producer() -> None:
    """close does nothing when producer is None."""
    producer = KafkaProducer(bootstrap_servers="localhost:9092", topic="test_topic")
    await producer.close()  # Should not raise


@pytest.mark.asyncio
async def test_kafka_producer_get_producer_creates_topic_and_starts() -> None:
    """_get_producer ensures topic exists and starts producer (mocked Kafka)."""
    mock_admin = AsyncMock()
    mock_admin.start = AsyncMock()
    mock_admin.create_topics = AsyncMock()
    mock_admin.close = AsyncMock()

    mock_producer = AsyncMock()
    mock_producer.start = AsyncMock()
    mock_producer.send_and_wait = AsyncMock(return_value=MagicMock())

    with (
        patch("transcription_ingest.producer.kafka_producer.AIOKafkaAdminClient", return_value=mock_admin),
        patch("transcription_ingest.producer.kafka_producer.AIOKafkaProducer", return_value=mock_producer),
    ):
        producer = KafkaProducer(bootstrap_servers="localhost:9092", topic="test_topic")
        await producer.send(session_id="s1", seq_no=0, transcript="x", role="Agent")
        mock_admin.create_topics.assert_called_once()
        mock_producer.start.assert_called_once()


@pytest.mark.asyncio
async def test_kafka_producer_ensure_topic_ignores_existing() -> None:
    """_ensure_topic ignores when topic already exists."""
    mock_admin = AsyncMock()
    mock_admin.start = AsyncMock()
    mock_admin.create_topics = AsyncMock(side_effect=Exception("TopicExists"))
    mock_admin.close = AsyncMock()

    mock_producer = AsyncMock()
    mock_producer.start = AsyncMock()
    mock_producer.send_and_wait = AsyncMock(return_value=MagicMock())

    with (
        patch("transcription_ingest.producer.kafka_producer.AIOKafkaAdminClient", return_value=mock_admin),
        patch("transcription_ingest.producer.kafka_producer.AIOKafkaProducer", return_value=mock_producer),
    ):
        producer = KafkaProducer(bootstrap_servers="localhost:9092", topic="test_topic")
        await producer.send(session_id="s1", seq_no=0, transcript="x", role="Agent")
        mock_admin.close.assert_called_once()
