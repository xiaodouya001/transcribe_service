"""Tests for reconnect logic."""

import asyncio
from types import SimpleNamespace

import pytest

from asr_ingest.connector.reconnect import run_with_reconnect


@pytest.mark.asyncio
async def test_run_with_reconnect_disabled() -> None:
    """When reconnect_enabled=False, connect_fn runs once and exits."""
    call_count = 0

    async def connect_fn(last_event_id):
        nonlocal call_count
        call_count += 1
        return None  # WebSocket-style: no last_event_id

    settings = SimpleNamespace(reconnect_enabled=False)
    await run_with_reconnect(connect_fn, settings)
    assert call_count == 1


@pytest.mark.asyncio
async def test_run_with_reconnect_passes_last_event_id() -> None:
    """SSE: last_event_id is passed to connect_fn on retry."""
    calls: list[str | None] = []

    async def connect_fn(last_event_id):
        calls.append(last_event_id)
        if len(calls) == 1:
            return "evt-123"  # Simulate connection end, return last id
        return None  # Second call exits (break loop)

    settings = SimpleNamespace(
        reconnect_enabled=True,
        reconnect_max_retries=3,
        reconnect_initial_delay=0.01,
        reconnect_max_delay=1.0,
        reconnect_backoff_factor=2.0,
    )
    await run_with_reconnect(connect_fn, settings)
    assert len(calls) == 2
    assert calls[0] is None
    assert calls[1] == "evt-123"


@pytest.mark.asyncio
async def test_run_with_reconnect_respects_draining() -> None:
    """When shutdown.draining=True, loop exits without retry."""
    call_count = 0
    shutdown = SimpleNamespace(draining=False)

    async def connect_fn(last_event_id):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            shutdown.draining = True
            raise RuntimeError("simulated error")
        return None

    settings = SimpleNamespace(
        reconnect_enabled=True,
        reconnect_max_retries=5,
        reconnect_initial_delay=0.01,
        reconnect_max_delay=1.0,
        reconnect_backoff_factor=2.0,
    )
    await run_with_reconnect(connect_fn, settings, shutdown=shutdown)
    assert call_count == 1  # Exits on draining, no retry


@pytest.mark.asyncio
async def test_run_with_reconnect_max_retries() -> None:
    """When max_retries reached, raises last exception."""
    call_count = 0

    async def connect_fn(last_event_id):
        nonlocal call_count
        call_count += 1
        raise RuntimeError("connection failed")

    settings = SimpleNamespace(
        reconnect_enabled=True,
        reconnect_max_retries=2,
        reconnect_initial_delay=0.01,
        reconnect_max_delay=0.1,
        reconnect_backoff_factor=2.0,
    )
    with pytest.raises(RuntimeError, match="connection failed"):
        await run_with_reconnect(connect_fn, settings)
    assert call_count == 2
