"""coverage: mock_client.kafka_viewer"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from mock_client import kafka_viewer as kv
from mock_client.kafka_viewer import KafkaViewer


class _BoomConsumer:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_consume_loop_logs_context_on_error(monkeypatch):
    viewer = KafkaViewer(
        bootstrap_servers="127.0.0.1:9092",
        topic="topic-a",
        on_error=MagicMock(),
    )
    viewer._consumer = _BoomConsumer()
    viewer._subscribers = {"sid1": AsyncMock()}

    log_exc = MagicMock()
    monkeypatch.setattr(kv.log, "exception", log_exc)

    await viewer._consume_loop()

    log_exc.assert_called_once()
    args, kwargs = log_exc.call_args
    assert args[0] == "Kafka consume loop error"
    assert kwargs["bootstrap_servers"] == "127.0.0.1:9092"
    assert kwargs["topic"] == "topic-a"
    assert kwargs["subscriber_count"] == 1
    assert kwargs["exc_type"] == "RuntimeError"
    assert kwargs["error"] == "boom"
    viewer._on_error.assert_called_once_with("boom")
