"""Tests for connector layer. SSE/WebSocket use mocked HTTP/WS (no real network)."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from transcription_ingest.connector.base import TranscriptionEvent
from transcription_ingest.connector.sse import SseConnector, _log_payload
from transcription_ingest.connector import get_connector


def test_get_connector_sse() -> None:
    """get_connector returns SseConnector when settings.mode is 'sse'."""
    settings = MagicMock()
    settings.mode = "sse"
    settings.stt_provider_url = "https://stt.example/sse"
    settings.sse_read_timeout = 30.0
    conn = get_connector(settings, last_event_id="ev-1")
    assert isinstance(conn, SseConnector)
    assert conn._url == "https://stt.example/sse"
    assert conn._last_event_id == "ev-1"
    assert conn._read_timeout == 30.0


def test_get_connector_websocket() -> None:
    """get_connector returns WebSocketConnector when settings.mode is not 'sse'."""
    from transcription_ingest.connector.websocket import WebSocketConnector

    settings = MagicMock()
    settings.mode = "websocket"
    settings.stt_provider_url = "wss://stt.example/ws"
    settings.ws_ping_interval = 15.0
    settings.ws_ping_timeout = 10.0
    conn = get_connector(settings, last_event_id=None)
    assert isinstance(conn, WebSocketConnector)
    assert conn._url == "wss://stt.example/ws"
    assert conn._ping_interval == 15.0
    assert conn._ping_timeout == 10.0


def test_from_vendor_payload() -> None:
    """Parse Vendor payload into TranscriptionEvents."""
    payload = {
        "success": True,
        "result": {
            "processingId": "proc-123",
            "processingStatus": "IN_PROGRESS",
            "callStatus": {"sessionId": "s1"},
            "transcripts": [
                {"seqNo": 0, "transcript": "hello", "role": "Agent", "createdAt": "2025-01-01T00:00:00Z"},
                {"seqNo": 1, "transcript": "hi", "role": "Customer", "createdAt": "2025-01-01T00:00:01Z"},
            ],
        },
    }
    events = TranscriptionEvent.from_vendor_payload(payload)
    assert len(events) == 2
    assert events[0].session_id == "s1"
    assert events[0].processing_id == "proc-123"
    assert events[0].seq_no == 0
    assert events[0].transcript == "hello"
    assert events[0].role == "Agent"
    assert events[1].seq_no == 1
    assert events[1].transcript == "hi"


def test_from_vendor_payload_empty() -> None:
    """Empty or missing result yields empty list."""
    assert TranscriptionEvent.from_vendor_payload({}) == []
    assert TranscriptionEvent.from_vendor_payload({"result": {}}) == []
    assert TranscriptionEvent.from_vendor_payload({"result": {"transcripts": []}}) == []


def test_from_vendor_payload_missing_optional_fields() -> None:
    """Missing optional fields use defaults."""
    payload = {
        "result": {
            "callStatus": {"sessionId": "s1"},
            "transcripts": [
                {"seqNo": 0, "transcript": "hi"},
                {"seqNo": 1},
            ],
        },
    }
    events = TranscriptionEvent.from_vendor_payload(payload)
    assert len(events) == 2
    assert events[0].session_id == "s1"
    assert events[0].seq_no == 0
    assert events[0].transcript == "hi"
    assert events[0].role == ""
    assert events[1].seq_no == 1
    assert events[1].transcript == ""


def test_from_vendor_payload_camel_case_fields() -> None:
    """Vendor payload uses camelCase (seqNo, sessionId, etc)."""
    payload = {
        "success": True,
        "result": {
            "processingId": "proc-1",
            "processingStatus": "DONE",
            "callStatus": {"sessionId": "sid-123"},
            "transcripts": [
                {
                    "seqNo": 5,
                    "transcript": "test",
                    "role": "Customer",
                    "createdAt": "2025-01-01T12:00:00Z",
                },
            ],
        },
    }
    events = TranscriptionEvent.from_vendor_payload(payload)
    assert len(events) == 1
    assert events[0].processing_id == "proc-1"
    assert events[0].processing_status == "DONE"
    assert events[0].session_id == "sid-123"
    assert events[0].seq_no == 5
    assert events[0].created_at == "2025-01-01T12:00:00Z"


def test_log_payload_no_raise() -> None:
    """_log_payload does not raise on malformed payload."""
    _log_payload({}, "test")
    _log_payload({"result": {}}, "test")


@pytest.mark.asyncio
async def test_sse_connector_connect_parses_sse_stream() -> None:
    """SseConnector.connect parses SSE data lines and yields events."""
    payload = {
        "success": True,
        "result": {
            "callStatus": {"sessionId": "s1"},
            "transcripts": [{"seqNo": 0, "transcript": "hello", "role": "Agent"}],
        },
    }
    sse_body = f"id: evt-1\ndata: {json.dumps(payload)}\n\n"

    async def fake_aiter_text():
        yield sse_body

    fake_resp = MagicMock()
    fake_resp.aiter_text = fake_aiter_text
    fake_resp.raise_for_status = MagicMock()
    fake_resp.__aenter__ = AsyncMock(return_value=fake_resp)
    fake_resp.__aexit__ = AsyncMock(return_value=None)

    fake_stream_ctx = MagicMock()
    fake_stream_ctx.__aenter__ = AsyncMock(return_value=fake_resp)
    fake_stream_ctx.__aexit__ = AsyncMock(return_value=None)

    with patch("transcription_ingest.connector.sse.httpx.AsyncClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.stream = MagicMock(return_value=fake_stream_ctx)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_cls.return_value = mock_client

        connector = SseConnector("http://fake.local/sse")
        events = []
        async for event, raw in connector.connect():
            events.append((event, raw))
        assert len(events) == 1
        assert events[0][0].session_id == "s1"
        assert events[0][0].transcript == "hello"


@pytest.mark.asyncio
async def test_sse_connector_connect_with_last_event_id() -> None:
    """SseConnector sends Last-Event-ID header when provided."""
    async def fake_aiter_text():
        yield ""

    fake_resp = MagicMock()
    fake_resp.aiter_text = fake_aiter_text
    fake_resp.raise_for_status = MagicMock()
    fake_resp.__aenter__ = AsyncMock(return_value=fake_resp)
    fake_resp.__aexit__ = AsyncMock(return_value=None)

    fake_stream_ctx = MagicMock()
    fake_stream_ctx.__aenter__ = AsyncMock(return_value=fake_resp)
    fake_stream_ctx.__aexit__ = AsyncMock(return_value=None)

    with patch("transcription_ingest.connector.sse.httpx.AsyncClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.stream = MagicMock(return_value=fake_stream_ctx)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_cls.return_value = mock_client

        connector = SseConnector("http://fake.local/sse", last_event_id="evt-99")
        async for _ in connector.connect():
            break
        call_kwargs = mock_client.stream.call_args[1]
        assert call_kwargs["headers"]["Last-Event-ID"] == "evt-99"


@pytest.mark.asyncio
async def test_sse_connector_connect_and_push() -> None:
    """SseConnector.connect_and_push pushes payloads to buffer."""
    payload = {
        "success": True,
        "result": {
            "callStatus": {"sessionId": "s1"},
            "transcripts": [{"seqNo": 0, "transcript": "hi", "role": "Agent"}],
        },
    }
    sse_body = f"data: {json.dumps(payload)}\n\n"

    async def fake_aiter_text():
        yield sse_body

    fake_resp = MagicMock()
    fake_resp.aiter_text = fake_aiter_text
    fake_resp.raise_for_status = MagicMock()
    fake_resp.__aenter__ = AsyncMock(return_value=fake_resp)
    fake_resp.__aexit__ = AsyncMock(return_value=None)

    fake_stream_ctx = MagicMock()
    fake_stream_ctx.__aenter__ = AsyncMock(return_value=fake_resp)
    fake_stream_ctx.__aexit__ = AsyncMock(return_value=None)

    pushed: list[dict] = []

    class FakeBuffer:
        async def push(self, p: dict) -> str:
            pushed.append(p)
            return "0-1"

    with patch("transcription_ingest.connector.sse.httpx.AsyncClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.stream = MagicMock(return_value=fake_stream_ctx)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_cls.return_value = mock_client

        connector = SseConnector("http://fake.local/sse")
        await connector.connect_and_push(FakeBuffer())
        assert len(pushed) == 1
        assert pushed[0]["result"]["transcripts"][0]["transcript"] == "hi"


@pytest.mark.asyncio
async def test_sse_connector_skips_done_and_invalid_json() -> None:
    """SseConnector skips [DONE], empty data, and invalid JSON."""
    sse_body = "data: [DONE]\n\ndata: \n\ndata: {invalid}\n\n"
    async def fake_aiter_text():
        yield sse_body

    fake_resp = MagicMock()
    fake_resp.aiter_text = fake_aiter_text
    fake_resp.raise_for_status = MagicMock()
    fake_resp.__aenter__ = AsyncMock(return_value=fake_resp)
    fake_resp.__aexit__ = AsyncMock(return_value=None)

    fake_stream_ctx = MagicMock()
    fake_stream_ctx.__aenter__ = AsyncMock(return_value=fake_resp)
    fake_stream_ctx.__aexit__ = AsyncMock(return_value=None)

    with patch("transcription_ingest.connector.sse.httpx.AsyncClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.stream = MagicMock(return_value=fake_stream_ctx)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_cls.return_value = mock_client

        connector = SseConnector("http://fake.local/sse")
        events = list()
        async for e, _ in connector.connect():
            events.append(e)
        assert len(events) == 0


@pytest.mark.asyncio
async def test_websocket_connector_connect() -> None:
    """WebSocketConnector.connect parses JSON and yields events."""
    from transcription_ingest.connector.websocket import WebSocketConnector

    payload = {
        "success": True,
        "result": {
            "callStatus": {"sessionId": "s1"},
            "transcripts": [{"seqNo": 0, "transcript": "ws-msg", "role": "Agent"}],
        },
    }

    count = 0
    class FakeWS:
        def __aiter__(self):
            return self
        async def __anext__(self):
            nonlocal count
            count += 1
            if count > 1:
                raise StopAsyncIteration
            return json.dumps(payload)

    with patch("transcription_ingest.connector.websocket.websockets.connect") as mock_connect:
        fake_ctx = MagicMock()
        fake_ctx.__aenter__ = AsyncMock(return_value=FakeWS())
        fake_ctx.__aexit__ = AsyncMock(return_value=None)
        mock_connect.return_value = fake_ctx

        connector = WebSocketConnector("ws://fake.local/ws")
        events = []
        async for event, _ in connector.connect():
            events.append(event)
        assert len(events) == 1
        assert events[0].transcript == "ws-msg"


@pytest.mark.asyncio
async def test_websocket_connector_connect_and_push() -> None:
    """WebSocketConnector.connect_and_push pushes to buffer."""
    from transcription_ingest.connector.websocket import WebSocketConnector

    payload = {
        "success": True,
        "result": {
            "callStatus": {"sessionId": "s1"},
            "transcripts": [{"seqNo": 0, "transcript": "pushed", "role": "Agent"}],
        },
    }

    pushed: list[dict] = []

    class FakeBuffer:
        async def push(self, p: dict) -> str:
            pushed.append(p)
            return "0-1"

    count = 0
    class FakeWS:
        def __aiter__(self):
            return self
        async def __anext__(self):
            nonlocal count
            count += 1
            if count > 1:
                raise StopAsyncIteration
            return json.dumps(payload)

    with patch("transcription_ingest.connector.websocket.websockets.connect") as mock_connect:
        fake_ctx = MagicMock()
        fake_ctx.__aenter__ = AsyncMock(return_value=FakeWS())
        fake_ctx.__aexit__ = AsyncMock(return_value=None)
        mock_connect.return_value = fake_ctx

        connector = WebSocketConnector("ws://fake.local/ws")
        await connector.connect_and_push(FakeBuffer())
        assert len(pushed) == 1
        assert pushed[0]["result"]["transcripts"][0]["transcript"] == "pushed"
