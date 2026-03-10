"""Tests for Webhook endpoint."""

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from transcription_ingest.webhook import create_app, WebhookPayload
from transcription_ingest.connector.manager import ConnectorManager


@pytest.fixture
def mock_manager():
    return MagicMock(spec=ConnectorManager)


@pytest.fixture
def client(mock_manager):
    app = create_app(mock_manager)
    return TestClient(app)


def test_webhook_post_valid(client, mock_manager) -> None:
    """POST valid payload returns 202."""
    resp = client.post(
        "/webhook/session",
        json={
            "metadata": {"session_id": "s1"},
            "ws_url": "wss://vendor/ws",
            "sse_url": "https://vendor/sse",
        },
    )
    assert resp.status_code == 202
    assert resp.json() == {"session_id": "s1"}
    mock_manager.add_session.assert_called_once()
    call_kw = mock_manager.add_session.call_args[1]
    assert call_kw["metadata"] == {"session_id": "s1"}
    assert call_kw["ws_url"] == "wss://vendor/ws"
    assert call_kw["sse_url"] == "https://vendor/sse"


def test_webhook_post_missing_session_id(client, mock_manager) -> None:
    """POST without session_id returns 400."""
    resp = client.post(
        "/webhook/session",
        json={
            "metadata": {},
            "ws_url": "",
            "sse_url": "https://vendor/sse",
        },
    )
    assert resp.status_code == 400
    assert "session_id" in resp.json().get("error", "").lower()
    mock_manager.add_session.assert_not_called()


def test_webhook_payload_model() -> None:
    """WebhookPayload accepts metadata, ws_url, sse_url."""
    p = WebhookPayload(
        metadata={"session_id": "x"},
        ws_url="wss://a",
        sse_url="https://b",
    )
    assert p.metadata["session_id"] == "x"
    assert p.ws_url == "wss://a"
    assert p.sse_url == "https://b"
