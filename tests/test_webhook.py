"""Tests for Webhook endpoint."""

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from transcribe_service.webhook import create_app, WebhookPayload
from transcribe_service.connector.manager import ConnectorManager


@pytest.fixture
def mock_manager():
    m = MagicMock(spec=ConnectorManager)
    m._settings = MagicMock()
    m._settings.transcribe_service_protocol = "sse"
    m._settings.transcribe_service_ssrf_allow_localhost = False
    m._shutdown = MagicMock()
    m._shutdown.draining = False
    return m


@pytest.fixture
def client(mock_manager):
    app = create_app(mock_manager)
    return TestClient(app)


def test_webhook_post_valid(client, mock_manager) -> None:
    """POST valid payload returns 202."""
    mock_manager.add_session.return_value = True
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


def test_webhook_post_session_limit(client, mock_manager) -> None:
    """POST when session limit reached returns 503."""
    mock_manager.add_session.return_value = False
    resp = client.post(
        "/webhook/session",
        json={
            "metadata": {"session_id": "s1"},
            "ws_url": "wss://vendor/ws",
            "sse_url": "https://vendor/sse",
        },
    )
    assert resp.status_code == 503
    assert "session limit" in resp.json().get("error", "").lower()


def test_webhook_post_draining_returns_503(client, mock_manager) -> None:
    """POST when draining returns 503."""
    mock_manager._shutdown.draining = True
    resp = client.post(
        "/webhook/session",
        json={
            "metadata": {"session_id": "s1"},
            "ws_url": "wss://vendor/ws",
            "sse_url": "https://vendor/sse",
        },
    )
    assert resp.status_code == 503
    assert "draining" in resp.json().get("error", "").lower()
    mock_manager.add_session.assert_not_called()


def test_webhook_post_ssrf_rejected(client, mock_manager) -> None:
    """POST with private URL (SSRF) returns 400."""
    mock_manager._settings.transcribe_service_ssrf_allow_localhost = False
    resp = client.post(
        "/webhook/session",
        json={
            "metadata": {"session_id": "s1"},
            "ws_url": "",
            "sse_url": "http://127.0.0.1/internal",
        },
    )
    assert resp.status_code == 400
    assert "private" in resp.json().get("error", "").lower() or "local" in resp.json().get("error", "").lower()


def test_health_ready(client) -> None:
    """GET /health returns 200."""
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


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
