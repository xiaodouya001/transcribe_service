"""Tests for ConnectorManager."""

from unittest.mock import AsyncMock, MagicMock

import pytest

pytestmark = pytest.mark.asyncio

from transcription_ingest.connector.manager import ConnectorManager


@pytest.fixture
def mock_dedup():
    d = MagicMock()
    d.should_emit = AsyncMock(return_value=True)
    return d


@pytest.fixture
def mock_cleaner():
    c = MagicMock()
    c.clean = MagicMock(return_value={"raw": {}, "cleaned": {}})
    return c


@pytest.fixture
def mock_producer():
    p = MagicMock()
    p.send = AsyncMock()
    p.flush = AsyncMock()
    return p


@pytest.fixture
def mock_settings():
    s = MagicMock()
    s.transcribe_service_protocol = "sse"
    s.transcribe_service_max_sessions_per_pod = 100
    s.reconnect_enabled = False
    s.sse_read_timeout = None
    s.ws_ping_interval = 20.0
    s.ws_ping_timeout = 20.0
    return s


async def test_add_session_creates_task(mock_dedup, mock_cleaner, mock_producer, mock_settings) -> None:
    """add_session creates a task for the session."""
    manager = ConnectorManager(
        dedup=mock_dedup,
        cleaner=mock_cleaner,
        producer=mock_producer,
        settings=mock_settings,
    )
    manager.add_session(
        metadata={"session_id": "s1"},
        ws_url="wss://x/ws",
        sse_url="http://x/sse",
    )
    assert "s1" in manager._sessions
    assert manager._sessions["s1"].done() is False
    manager.remove_session("s1")  # cleanup to avoid warning


async def test_add_session_ignores_empty_session_id(mock_dedup, mock_cleaner, mock_producer, mock_settings) -> None:
    """add_session ignores payload without session_id."""
    manager = ConnectorManager(
        dedup=mock_dedup,
        cleaner=mock_cleaner,
        producer=mock_producer,
        settings=mock_settings,
    )
    manager.add_session(metadata={}, ws_url="", sse_url="http://x/sse")
    assert len(manager._sessions) == 0


async def test_remove_session_cancels_task(mock_dedup, mock_cleaner, mock_producer, mock_settings) -> None:
    """remove_session cancels and removes the task."""
    manager = ConnectorManager(
        dedup=mock_dedup,
        cleaner=mock_cleaner,
        producer=mock_producer,
        settings=mock_settings,
    )
    manager.add_session(
        metadata={"session_id": "s1"},
        ws_url="",
        sse_url="http://x/sse",
    )
    manager.remove_session("s1")
    assert "s1" not in manager._sessions
