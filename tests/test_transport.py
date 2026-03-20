"""Tests for transport 接入层 — WebSocket 端到端。"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import orjson
import pytest
from httpx import ASGITransport, AsyncClient
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect, WebSocketState

from transcribe_service.orchestrator.base import OrchestratorResult
from transcribe_service.schemas.response import build_ack, build_error
from transcribe_service.shutdown.graceful import GracefulShutdown
from transcribe_service.transport.websocket_handler import ConnectionRegistry, create_app


@pytest.fixture
def mock_orchestrator():
    orch = AsyncMock()
    orch.handle_message = AsyncMock(
        return_value=OrchestratorResult(
            response=build_ack("conv-1", 0),
            disconnect=False,
        )
    )
    return orch


@pytest.fixture
def shutdown():
    return GracefulShutdown()


@pytest.fixture
def registry():
    return ConnectionRegistry()


@pytest.fixture
def app(mock_orchestrator, shutdown, registry):
    return create_app(mock_orchestrator, shutdown, registry)


class TestHealthEndpoints:
    """HTTP 健康检查端点。"""

    async def test_health(self, app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/health")
            assert resp.status_code == 200
            assert resp.json()["status"] == "ok"

    async def test_metrics(self, app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/metrics")
            assert resp.status_code == 200
            assert "active_connections" in resp.json()

    async def test_ready_without_checks_when_empty(self, mock_orchestrator, shutdown, registry):
        app = create_app(
            mock_orchestrator, shutdown, registry, redis_url="", producer=None
        )
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/ready")
            assert resp.status_code == 200
            assert resp.json()["status"] == "ready"

    async def test_ready_redis_and_kafka_ok(self, mock_orchestrator, shutdown, registry):
        from unittest.mock import patch

        fake_r = MagicMock()
        fake_r.ping = AsyncMock()
        fake_r.aclose = AsyncMock()
        prod = MagicMock()
        prod.ensure_ready = AsyncMock()
        with patch(
            "redis.asyncio.Redis.from_url",
            return_value=fake_r,
        ):
            app = create_app(
                mock_orchestrator,
                shutdown,
                registry,
                redis_url="redis://localhost:6379/0",
                producer=prod,
            )
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.get("/ready")
            assert resp.status_code == 200

    async def test_ready_503_when_redis_fails(self, mock_orchestrator, shutdown, registry):
        from unittest.mock import patch

        fake_r = MagicMock()
        fake_r.ping = AsyncMock(side_effect=RuntimeError("no redis"))
        fake_r.aclose = AsyncMock()
        with patch(
            "redis.asyncio.Redis.from_url",
            return_value=fake_r,
        ):
            app = create_app(
                mock_orchestrator,
                shutdown,
                registry,
                redis_url="redis://localhost:6379/0",
                producer=None,
            )
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.get("/ready")
            assert resp.status_code == 503
            assert "not_ready" in resp.json().get("status", "")

    async def test_ready_503_when_kafka_fails(self, mock_orchestrator, shutdown, registry):
        prod = MagicMock()
        prod.ensure_ready = AsyncMock(side_effect=RuntimeError("kafka down"))
        app = create_app(
            mock_orchestrator,
            shutdown,
            registry,
            redis_url="",
            producer=prod,
        )
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/ready")
        assert resp.status_code == 503


class TestWebSocket:
    """WebSocket 端点测试。"""

    def test_ws_normal_ongoing(self, app, mock_orchestrator):
        client = TestClient(app)
        msg = {
            "metaData": {
                "conversationId": "conv-1",
                "agentId": "A1",
                "staffId": "S1",
                "customerId": "C1",
                "callStartTimeStamp": "2025-01-01T00:00:00Z",
                "callEndTimeStamp": None,
                "eventType": "SESSION_ONGOING",
            },
            "payload": {
                "sequenceNumber": 0,
                "speaker": "Agent",
                "transcript": "Hello",
                "engineProvider": "FanoLabs",
                "isFinal": True,
                "createdAtTimeStamp": "2025-01-01T00:00:01Z",
            },
        }
        with client.websocket_connect(
            "/ws/v1/realtime-transcriptions?conversationId=conv-1"
        ) as ws:
            ws.send_text(orjson.dumps(msg).decode())
            resp = orjson.loads(ws.receive_text())
            assert resp["metaData"]["eventType"] == "TRANSCRIPT_ACK"

    def test_ws_disconnect_on_error(self, app, mock_orchestrator):
        mock_orchestrator.handle_message.return_value = OrchestratorResult(
            response=build_error("conv-1", "E1006", "Out of order"),
            disconnect=True,
            close_code=1008,
        )
        client = TestClient(app)
        with client.websocket_connect(
            "/ws/v1/realtime-transcriptions?conversationId=conv-1"
        ) as ws:
            ws.send_text('{"metaData":{"conversationId":"conv-1"}}')
            resp = orjson.loads(ws.receive_text())
            assert resp["metaData"]["eventType"] == "ERROR"

    def test_ws_invalid_json(self, app):
        client = TestClient(app)
        with client.websocket_connect(
            "/ws/v1/realtime-transcriptions?conversationId=conv-1"
        ) as ws:
            ws.send_text("not json at all")
            resp = orjson.loads(ws.receive_text())
            assert resp["error"]["code"] == "E1001"

    def test_ws_missing_conversation_id(self, app):
        client = TestClient(app)
        with pytest.raises(Exception) as ei:
            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions"
            ) as ws:
                pass
        assert hasattr(ei.value, "status_code") and ei.value.status_code == 400
        body = getattr(ei.value, "text", "")
        assert "E1003" in body
        assert "Missing required field" in body

    def test_ws_draining_rejects(self, mock_orchestrator, shutdown, registry):
        shutdown._draining = True
        app = create_app(mock_orchestrator, shutdown, registry)
        client = TestClient(app)
        with pytest.raises(Exception) as ei:
            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-1"
            ):
                pass
        assert hasattr(ei.value, "status_code") and ei.value.status_code == 503
        body = getattr(ei.value, "text", "")
        assert "E1008" in body
        assert "Service draining" in body

    def test_ws_max_connections_rejects_429(self, mock_orchestrator, shutdown, registry):
        registry.add("existing", MagicMock())
        app = create_app(mock_orchestrator, shutdown, registry, max_connections=1)
        client = TestClient(app)
        with pytest.raises(Exception) as ei:
            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-2"
            ):
                pass
        assert hasattr(ei.value, "status_code") and ei.value.status_code == 429
        body = getattr(ei.value, "text", "")
        assert "E1008" in body
        assert "Too many connections" in body

    def test_ws_non_target_path_not_intercepted(self, app):
        client = TestClient(app)
        with pytest.raises(Exception) as ei:
            with client.websocket_connect("/ws/unknown?conversationId=conv-x"):
                pass
        assert isinstance(ei.value, WebSocketDisconnect)

    def test_ws_orchestrator_raises_internal_error(
        self, mock_orchestrator, shutdown, registry, app
    ):
        mock_orchestrator.handle_message.side_effect = RuntimeError("boom")
        client = TestClient(app)
        msg = {
            "metaData": {
                "conversationId": "conv-1",
                "agentId": "A1",
                "staffId": "S1",
                "customerId": "C1",
                "callStartTimeStamp": "2025-01-01T00:00:00Z",
                "callEndTimeStamp": None,
                "eventType": "SESSION_ONGOING",
            },
            "payload": {
                "sequenceNumber": 0,
                "speaker": "Agent",
                "transcript": "Hello",
                "engineProvider": "FanoLabs",
                "isFinal": True,
                "createdAtTimeStamp": "2025-01-01T00:00:01Z",
            },
        }
        with client.websocket_connect(
            "/ws/v1/realtime-transcriptions?conversationId=conv-1"
        ) as ws:
            ws.send_text(orjson.dumps(msg).decode())
            resp = orjson.loads(ws.receive_text())
            assert resp["metaData"]["eventType"] == "ERROR"


@pytest.mark.asyncio
async def test_send_error_and_close_swallows_inner_failure():
    from transcribe_service.transport import websocket_handler as wh

    ws = MagicMock()
    ws.client_state = WebSocketState.CONNECTED
    ws.send_text = AsyncMock(side_effect=RuntimeError("send failed"))
    ws.close = AsyncMock()
    await wh._send_error_and_close(
        ws, "conv-x", "E1001", "bad", wh.WsCloseCode.INVALID_PAYLOAD
    )


class TestConnectionRegistry:
    @pytest.mark.asyncio
    async def test_close_all_connected_ok(self, registry: ConnectionRegistry):
        ws = MagicMock()
        ws.client_state = WebSocketState.CONNECTED
        ws.close = AsyncMock()
        registry.add("c1", ws)
        await registry.close_all()
        ws.close.assert_awaited()
        assert registry.active_count == 0

    @pytest.mark.asyncio
    async def test_close_all_swallows_close_exception(
        self, registry: ConnectionRegistry
    ):
        ws = MagicMock()
        ws.client_state = WebSocketState.CONNECTED
        ws.close = AsyncMock(side_effect=RuntimeError("x"))
        registry.add("c1", ws)
        await registry.close_all()
        assert registry.active_count == 0

    @pytest.mark.asyncio
    async def test_close_all_skips_not_connected(self, registry: ConnectionRegistry):
        ws = MagicMock()
        ws.client_state = WebSocketState.DISCONNECTED
        ws.close = AsyncMock()
        registry.add("c1", ws)
        await registry.close_all()
        ws.close.assert_not_awaited()

    def test_add_remove(self, registry: ConnectionRegistry):
        mock_ws = AsyncMock()
        registry.add("conv-1", mock_ws)
        assert registry.active_count == 1
        registry.remove("conv-1")
        assert registry.active_count == 0

    def test_remove_nonexistent(self, registry: ConnectionRegistry):
        registry.remove("nonexistent")
        assert registry.active_count == 0

    def test_remove_with_ws_only_pops_same_instance(self, registry: ConnectionRegistry):
        """旧连接 finally 不应删掉已被同 conversationId 覆盖的新连接登记。"""
        ws_old = MagicMock()
        ws_new = MagicMock()
        registry.add("conv-1", ws_old)
        registry.add("conv-1", ws_new)
        assert registry.active_count == 1
        registry.remove("conv-1", ws_old)
        assert registry.active_count == 1
        registry.remove("conv-1", ws_new)
        assert registry.active_count == 0
