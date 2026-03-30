"""End-to-end tests for the transport layer WebSocket boundary."""

from __future__ import annotations

import asyncio
import copy
from unittest.mock import ANY, AsyncMock, MagicMock

import fakeredis.aioredis
import orjson
import pytest
from httpx import ASGITransport, AsyncClient
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect, WebSocketState
from unittest.mock import patch

from realtime_transcribe_service.converter.kafka_message_converter import KafkaMessageConverter
from realtime_transcribe_service.orchestrator.protocols import OrchestratorResult
from realtime_transcribe_service.orchestrator.two_phase import TwoPhaseOrchestrator
from realtime_transcribe_service.redis.ownership_guard import RedisConversationOwnershipGuard
from realtime_transcribe_service.redis.sequence_state_machine import RedisSequenceStateMachine
from realtime_transcribe_service.schemas.response import (
    build_eol_ack,
    build_error,
    build_transcript_ack,
)
from realtime_transcribe_service.shutdown.graceful import GracefulShutdown
from realtime_transcribe_service.transport.websocket_handler import (
    ConnectionRegistry,
    _format_client_addr,
    _ownership_refresh_loop,
    create_app,
)


class ScriptedOwnerBackend:
    """Tiny test double for conversation ownership-guard claim/release outcomes."""

    def __init__(self, claim_sequence: list[object], release_exc: Exception | None = None):
        self._claim_sequence = list(claim_sequence)
        self._release_exc = release_exc
        self.release_calls: list[tuple[str, str]] = []

    async def claim_or_refresh(self, conversation_id: str, ownership_token: str) -> bool:
        if self._claim_sequence:
            outcome = self._claim_sequence.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return bool(outcome)
        return True

    async def release(self, conversation_id: str, ownership_token: str) -> None:
        self.release_calls.append((conversation_id, ownership_token))
        if self._release_exc is not None:
            raise self._release_exc

    async def close(self) -> None:  # pragma: no cover - compatibility with backend protocol
        return None


def _ongoing_message(
    conversation_id: str = "conv-1",
    *,
    seq: int = 0,
    speaker: str = "Agent",
) -> dict:
    payload = {
        "sequenceNumber": seq,
        "speaker": speaker,
        "transcript": "Hello",
        "engineProvider": "FanoLabs",
        "dialect": "yue-x-auto",
        "isFinal": True,
        "speakTimeStamp": "2025-01-01T00:00:00Z",
        "transcriptGenerateTimeStamp": "2025-01-01T00:00:01Z",
    }

    return {
        "metaData": {
            "conversationId": conversation_id,
            "callStartTimeStamp": "2025-01-01T00:00:00Z",
            "callEndTimeStamp": None,
            "eventType": "SESSION_ONGOING",
        },
        "payload": payload,
    }


def _complete_message(conversation_id: str = "conv-1", *, seq: int = 42) -> dict:
    return {
        "metaData": {
            "conversationId": conversation_id,
            "callStartTimeStamp": "2025-01-01T00:00:00Z",
            "callEndTimeStamp": "2025-01-01T00:05:00Z",
            "eventType": "SESSION_COMPLETE",
        },
        "payload": {
            "sequenceNumber": seq,
            "speaker": "System",
            "transcript": "session ended",
            "engineProvider": "FanoLabs",
            "dialect": "yue-x-auto",
            "isFinal": True,
        },
    }


@pytest.fixture
def mock_orchestrator():
    orch = AsyncMock()
    orch.handle_message = AsyncMock(
        return_value=OrchestratorResult(
            response=build_transcript_ack("conv-1", 0),
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
    """HTTP health-check endpoint."""

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
                redis_url="redis://127.0.0.1:6379/0",
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
                redis_url="redis://127.0.0.1:6379/0",
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
    """WebSocket endpoint tests."""

    def test_ws_normal_ongoing(self, app, mock_orchestrator):
        client = TestClient(app)
        msg = _ongoing_message()
        with client.websocket_connect(
            "/ws/v1/realtime-transcriptions?conversationId=conv-1"
        ) as ws:
            ws.send_text(orjson.dumps(msg).decode())
            resp = orjson.loads(ws.receive_text())
            assert resp["metaData"]["eventType"] == "TRANSCRIPT_ACK"
        mock_orchestrator.handle_message.assert_awaited_once()

    def test_ws_complete_ack_includes_server_processing_ms_and_closes(
        self, mock_orchestrator, shutdown, registry
    ):
        mock_orchestrator.handle_message.return_value = OrchestratorResult(
            response=build_eol_ack("conv-1", 42),
            disconnect=True,
            close_code=1000,
        )
        app = create_app(mock_orchestrator, shutdown, registry)
        client = TestClient(app)

        with client.websocket_connect(
            "/ws/v1/realtime-transcriptions?conversationId=conv-1"
        ) as ws:
            ws.send_text(orjson.dumps(_complete_message()).decode())
            resp = orjson.loads(ws.receive_text())
            assert resp["metaData"]["eventType"] == "EOL_ACK"
            assert resp["payload"]["sequenceNumber"] == 42
            assert isinstance(resp["payload"]["serverProcessingMs"], (int, float))
            with pytest.raises(WebSocketDisconnect) as ei:
                ws.receive_text()
            assert ei.value.code == 1000

    def test_ws_disconnect_preserves_active_ttl_and_allows_resume(
        self, shutdown, registry, valid_ongoing_msg
    ):
        fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
        state_machine = RedisSequenceStateMachine(
            client=fake_redis,
            active_ttl_sec=120,
            final_ttl_sec=5,
            key_prefix="realtime-transcribe-service:expect-transcript-seq-num",
        )
        producer = AsyncMock()
        producer.send = AsyncMock()
        app = create_app(
            TwoPhaseOrchestrator(
                state_machine,
                producer,
                message_converter=KafkaMessageConverter(),
            ),
            shutdown,
            registry,
        )
        client = TestClient(app)

        msg0 = copy.deepcopy(valid_ongoing_msg)
        msg0["metaData"]["conversationId"] = "conv-reconnect"
        msg0["payload"]["sequenceNumber"] = 0
        msg1 = copy.deepcopy(msg0)
        msg1["payload"]["sequenceNumber"] = 1

        try:
            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-reconnect"
            ) as ws:
                ws.send_text(orjson.dumps(msg0).decode())
                resp = orjson.loads(ws.receive_text())
                assert resp["metaData"]["eventType"] == "TRANSCRIPT_ACK"

            key = "realtime-transcribe-service:expect-transcript-seq-num:conv-reconnect"
            ttl = asyncio.run(fake_redis.ttl(key))
            assert 5 < ttl <= 120
            assert asyncio.run(fake_redis.get(key)) == "1"

            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-reconnect"
            ) as ws:
                ws.send_text(orjson.dumps(msg0).decode())
                replay_resp = orjson.loads(ws.receive_text())
                assert replay_resp["metaData"]["eventType"] == "TRANSCRIPT_ACK"
                assert producer.send.await_count == 1

                ws.send_text(orjson.dumps(msg1).decode())
                next_resp = orjson.loads(ws.receive_text())
                assert next_resp["metaData"]["eventType"] == "TRANSCRIPT_ACK"
                assert producer.send.await_count == 2

            assert asyncio.run(fake_redis.get(key)) == "2"
        finally:
            asyncio.run(state_machine.close())

    def test_ws_second_active_writer_rejected_with_e1009(
        self, mock_orchestrator, shutdown, registry
    ):
        fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
        owner = RedisConversationOwnershipGuard(
            client=fake_redis,
            guard_ttl_sec=30,
            key_prefix="realtime-transcribe-service:conversation-owner",
        )
        app = create_app(
            mock_orchestrator,
            shutdown,
            registry,
            ownership_guard=owner,
        )
        client = TestClient(app)

        try:
            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-1"
            ) as ws1:
                with pytest.raises(Exception) as ei:
                    with client.websocket_connect(
                        "/ws/v1/realtime-transcriptions?conversationId=conv-1"
                    ):
                        pass
                assert hasattr(ei.value, "status_code") and ei.value.status_code == 403
                body = getattr(ei.value, "text", "")
                assert "E1009" in body
                assert "Only one sender connection is allowed" in body

                ws1.send_text(
                    orjson.dumps(_ongoing_message()).decode()
                )
                resp = orjson.loads(ws1.receive_text())
                assert resp["metaData"]["eventType"] == "TRANSCRIPT_ACK"
        finally:
            asyncio.run(owner.close())

    def test_ws_owner_released_after_disconnect_allows_new_writer(
        self, mock_orchestrator, shutdown, registry
    ):
        fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
        owner = RedisConversationOwnershipGuard(
            client=fake_redis,
            guard_ttl_sec=30,
            key_prefix="realtime-transcribe-service:conversation-owner",
        )
        app = create_app(
            mock_orchestrator,
            shutdown,
            registry,
            ownership_guard=owner,
        )
        client = TestClient(app)

        try:
            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-1"
            ):
                assert asyncio.run(
                    fake_redis.get("realtime-transcribe-service:conversation-owner:conv-1")
                ) is not None

            assert asyncio.run(
                fake_redis.get("realtime-transcribe-service:conversation-owner:conv-1")
            ) is None

            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-1"
            ) as ws:
                ws.send_text('{"metaData":{"conversationId":"conv-1"}}')
                resp = orjson.loads(ws.receive_text())
                assert resp["metaData"]["eventType"] == "TRANSCRIPT_ACK"
        finally:
            asyncio.run(owner.close())

    def test_ws_owner_store_unavailable_on_initial_claim_returns_e1008(
        self, mock_orchestrator, shutdown, registry
    ):
        owner = ScriptedOwnerBackend([RuntimeError("owner store down")])
        app = create_app(
            mock_orchestrator,
            shutdown,
            registry,
            ownership_guard=owner,
        )
        client = TestClient(app)

        with pytest.raises(Exception) as ei:
            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-1"
            ):
                pass
        assert hasattr(ei.value, "status_code") and ei.value.status_code == 503
        body = getattr(ei.value, "text", "")
        assert "E1008" in body
        assert "Downstream unavailable" in body
        assert "Conversation ownership guard store unavailable" in body
        mock_orchestrator.handle_message.assert_not_awaited()

    def test_ws_owner_store_unavailable_on_fallback_claim_returns_e1008(
        self, mock_orchestrator, shutdown, registry
    ):
        from realtime_transcribe_service.transport import websocket_handler as wh

        owner = ScriptedOwnerBackend([RuntimeError("owner store down")])

        async def passthrough(self, scope, receive, send):
            await self._app(scope, receive, send)

        with patch.object(wh._WsGuardMiddleware, "__call__", new=passthrough):
            app = create_app(
                mock_orchestrator,
                shutdown,
                registry,
                ownership_guard=owner,
            )
            client = TestClient(app)

            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-1"
            ) as ws:
                resp = orjson.loads(ws.receive_text())
                assert resp["error"]["code"] == "E1008"
                assert resp["error"]["message"] == "Downstream unavailable"
                assert resp["error"]["details"] == "Conversation ownership guard store unavailable"
                with pytest.raises(WebSocketDisconnect) as ei:
                    ws.receive_text()
                assert ei.value.code == 1013
        mock_orchestrator.handle_message.assert_not_awaited()

    def test_ws_owner_conflict_on_fallback_claim_returns_e1009(
        self, mock_orchestrator, shutdown, registry
    ):
        from realtime_transcribe_service.transport import websocket_handler as wh

        owner = ScriptedOwnerBackend([False])

        async def passthrough(self, scope, receive, send):
            await self._app(scope, receive, send)

        with patch.object(wh._WsGuardMiddleware, "__call__", new=passthrough):
            app = create_app(
                mock_orchestrator,
                shutdown,
                registry,
                ownership_guard=owner,
            )
            client = TestClient(app)

            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-1"
            ) as ws:
                resp = orjson.loads(ws.receive_text())
                assert resp["error"]["code"] == "E1009"
                assert resp["error"]["message"] == "Only one sender connection is allowed"
                with pytest.raises(WebSocketDisconnect) as ei:
                    ws.receive_text()
                assert ei.value.code == 1008
        mock_orchestrator.handle_message.assert_not_awaited()

    def test_ws_owner_store_unavailable_during_background_refresh_returns_e1008(
        self, mock_orchestrator, shutdown, registry
    ):
        owner = ScriptedOwnerBackend([True, RuntimeError("owner store down")])
        app = create_app(
            mock_orchestrator,
            shutdown,
            registry,
            ownership_guard=owner,
            ownership_guard_refresh_interval_sec=0.01,
        )
        client = TestClient(app)

        with client.websocket_connect(
            "/ws/v1/realtime-transcriptions?conversationId=conv-1"
        ) as ws:
            resp = orjson.loads(ws.receive_text())
            assert resp["error"]["code"] == "E1008"
            assert resp["error"]["message"] == "Downstream unavailable"
            with pytest.raises(WebSocketDisconnect) as ei:
                ws.receive_text()
            assert ei.value.code == 1013
        mock_orchestrator.handle_message.assert_not_awaited()

    def test_ws_owner_conflict_during_background_refresh_returns_e1009(
        self, mock_orchestrator, shutdown, registry
    ):
        owner = ScriptedOwnerBackend([True, False])
        app = create_app(
            mock_orchestrator,
            shutdown,
            registry,
            ownership_guard=owner,
            ownership_guard_refresh_interval_sec=0.01,
        )
        client = TestClient(app)

        with client.websocket_connect(
            "/ws/v1/realtime-transcriptions?conversationId=conv-1"
        ) as ws:
            resp = orjson.loads(ws.receive_text())
            assert resp["error"]["code"] == "E1009"
            assert resp["error"]["message"] == "Only one sender connection is allowed"
            with pytest.raises(WebSocketDisconnect) as ei:
                ws.receive_text()
            assert ei.value.code == 1008
        mock_orchestrator.handle_message.assert_not_awaited()

    def test_ws_owner_release_failure_is_swallowed(
        self, mock_orchestrator, shutdown, registry
    ):
        owner = ScriptedOwnerBackend([True], release_exc=RuntimeError("release failed"))
        app = create_app(
            mock_orchestrator,
            shutdown,
            registry,
            ownership_guard=owner,
        )
        client = TestClient(app)

        with patch("realtime_transcribe_service.transport.websocket_handler.log.warning") as warn_mock:
            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-1"
            ):
                pass
        assert len(owner.release_calls) == 1
        warn_mock.assert_any_call(
            "Transport: Failed to release conversation ownership guard",
            conversation_id="conv-1",
            error="release failed",
        )

    def test_ws_conversation_id_mismatch_e1009_not_orchestrator(
        self, app, mock_orchestrator
    ):
        """A string ``conversationId`` mismatch between query and body returns E1009 + 1008 and never reaches the orchestrator."""
        client = TestClient(app)
        with client.websocket_connect(
            "/ws/v1/realtime-transcriptions?conversationId=conv-1"
        ) as ws:
            ws.send_text('{"metaData":{"conversationId":"conv-2"}}')
            resp = orjson.loads(ws.receive_text())
            assert resp["error"]["code"] == "E1009"
            mock_orchestrator.handle_message.assert_not_awaited()
            with pytest.raises(WebSocketDisconnect) as ei:
                ws.receive_text()
            assert ei.value.code == 1008

    def test_ws_conversation_id_mismatch_warning_includes_error_and_close_code(
        self, app, mock_orchestrator
    ):
        client = TestClient(app)
        with patch("realtime_transcribe_service.transport.websocket_handler.log.warning") as warn_mock:
            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-1"
            ) as ws:
                ws.send_text('{"metaData":{"conversationId":"conv-2"}}')
                resp = orjson.loads(ws.receive_text())
                assert resp["error"]["code"] == "E1009"
                with pytest.raises(WebSocketDisconnect) as ei:
                    ws.receive_text()
                assert ei.value.code == 1008

        warn_mock.assert_any_call(
            "Transport: metaData.conversationId does not match the handshake query",
            conversation_id="conv-1",
            handshake_conversation_id="conv-1",
            metadata_conversation_id="conv-2",
            error_code="E1009",
            close_code=1008,
        )
        mock_orchestrator.handle_message.assert_not_awaited()

    def test_ws_meta_conversation_id_missing_still_invokes_orchestrator(
        self, shutdown, registry
    ):
        """If ``metaData.conversationId`` is missing, transport mismatch logic does not run and schema E1003 is returned instead."""
        sm = AsyncMock()
        sm.prepare = AsyncMock()
        sm.commit = AsyncMock()
        sm.cleanup = AsyncMock()
        producer = AsyncMock()
        producer.send = AsyncMock()
        app = create_app(
            TwoPhaseOrchestrator(sm, producer, message_converter=KafkaMessageConverter()),
            shutdown,
            registry,
        )
        client = TestClient(app)
        msg = {
            "metaData": {
                "callStartTimeStamp": "2025-01-01T00:00:00Z",
                "callEndTimeStamp": None,
                "eventType": "SESSION_ONGOING",
            },
            "payload": {
                "sequenceNumber": 0,
                "speaker": "Agent",
                "transcript": "Hello",
                "engineProvider": "FanoLabs",
                "dialect": "yue-x-auto",
                "isFinal": True,
                "speakTimeStamp": "2025-01-01T00:00:00Z",
                "transcriptGenerateTimeStamp": "2025-01-01T00:00:01Z",
            },
        }
        with client.websocket_connect(
            "/ws/v1/realtime-transcriptions?conversationId=conv-1"
        ) as ws:
            ws.send_text(orjson.dumps(msg).decode())
            resp = orjson.loads(ws.receive_text())
            assert resp["error"]["code"] == "E1003"
            assert resp["metaData"]["conversationId"] == "conv-1"
            with pytest.raises(WebSocketDisconnect) as ei:
                ws.receive_text()
            assert ei.value.code == 1008
        sm.prepare.assert_not_awaited()
        producer.send.assert_not_awaited()

    def test_ws_meta_conversation_id_wrong_type_still_invokes_orchestrator(
        self, shutdown, registry
    ):
        """If ``metaData.conversationId`` is not a string, transport does not run the consistency check and schema E1004 is returned instead."""
        sm = AsyncMock()
        sm.prepare = AsyncMock()
        sm.commit = AsyncMock()
        sm.cleanup = AsyncMock()
        producer = AsyncMock()
        producer.send = AsyncMock()
        app = create_app(
            TwoPhaseOrchestrator(sm, producer, message_converter=KafkaMessageConverter()),
            shutdown,
            registry,
        )
        client = TestClient(app)
        msg = {
            "metaData": {
                "conversationId": 123,
                "callStartTimeStamp": "2025-01-01T00:00:00Z",
                "callEndTimeStamp": None,
                "eventType": "SESSION_ONGOING",
            },
            "payload": {
                "sequenceNumber": 0,
                "speaker": "Agent",
                "transcript": "Hello",
                "engineProvider": "FanoLabs",
                "dialect": "yue-x-auto",
                "isFinal": True,
                "speakTimeStamp": "2025-01-01T00:00:00Z",
                "transcriptGenerateTimeStamp": "2025-01-01T00:00:01Z",
            },
        }
        with client.websocket_connect(
            "/ws/v1/realtime-transcriptions?conversationId=conv-1"
        ) as ws:
            ws.send_text(orjson.dumps(msg).decode())
            resp = orjson.loads(ws.receive_text())
            assert resp["error"]["code"] == "E1004"
            assert resp["metaData"]["conversationId"] == "conv-1"
            with pytest.raises(WebSocketDisconnect) as ei:
                ws.receive_text()
            assert ei.value.code == 1008
        sm.prepare.assert_not_awaited()
        producer.send.assert_not_awaited()

    def test_ws_non_object_json_returns_e1004_with_handshake_conversation_id(
        self, shutdown, registry
    ):
        """A non-object top-level JSON payload should return a client type error, not E1007."""
        sm = AsyncMock()
        sm.prepare = AsyncMock()
        sm.commit = AsyncMock()
        sm.cleanup = AsyncMock()
        producer = AsyncMock()
        producer.send = AsyncMock()
        app = create_app(
            TwoPhaseOrchestrator(sm, producer, message_converter=KafkaMessageConverter()),
            shutdown,
            registry,
        )
        client = TestClient(app)

        with client.websocket_connect(
            "/ws/v1/realtime-transcriptions?conversationId=conv-1"
        ) as ws:
            ws.send_text("[]")
            resp = orjson.loads(ws.receive_text())
            assert resp["error"]["code"] == "E1004"
            assert resp["metaData"]["conversationId"] == "conv-1"
            with pytest.raises(WebSocketDisconnect) as ei:
                ws.receive_text()
            assert ei.value.code == 1008

        sm.prepare.assert_not_awaited()
        producer.send.assert_not_awaited()

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

    def test_ws_invalid_json_warning_includes_error_and_close_code(self, app):
        client = TestClient(app)
        with patch("realtime_transcribe_service.transport.websocket_handler.log.warning") as warn_mock:
            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-1"
            ) as ws:
                ws.send_text("not json at all")
                resp = orjson.loads(ws.receive_text())
                assert resp["error"]["code"] == "E1001"
                with pytest.raises(WebSocketDisconnect) as ei:
                    ws.receive_text()
                assert ei.value.code == 1007

        warn_mock.assert_any_call(
            "Transport: JSON decode failed",
            conversation_id="conv-1",
            error=ANY,
            error_code="E1001",
            close_code=1007,
        )

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

    def test_format_client_addr_empty_without_client(self):
        assert _format_client_addr({"type": "websocket"}) == ""

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

    def test_ws_max_connections_counts_same_conversation_socket(
        self, mock_orchestrator, shutdown, registry
    ):
        registry.add("conv-1", MagicMock())
        app = create_app(mock_orchestrator, shutdown, registry, max_connections=1)
        client = TestClient(app)
        with pytest.raises(Exception) as ei:
            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-1"
            ):
                pass
        assert hasattr(ei.value, "status_code") and ei.value.status_code == 429

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
        msg = _ongoing_message()
        with client.websocket_connect(
            "/ws/v1/realtime-transcriptions?conversationId=conv-1"
        ) as ws:
            ws.send_text(orjson.dumps(msg).decode())
            resp = orjson.loads(ws.receive_text())
            assert resp["metaData"]["eventType"] == "ERROR"

    def test_ws_logs_error_frame_when_enabled(self, mock_orchestrator, shutdown, registry):
        error_resp = build_error("conv-1", "E1006", "Out of order")
        mock_orchestrator.handle_message.return_value = OrchestratorResult(
            response=error_resp,
            disconnect=False,
        )
        app = create_app(
            mock_orchestrator,
            shutdown,
            registry,
            log_ws_error_frames=True,
        )
        client = TestClient(app)
        msg = _ongoing_message()
        with patch("realtime_transcribe_service.transport.websocket_handler.log.info") as info_mock:
            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-1"
            ) as ws:
                ws.send_text(orjson.dumps(msg).decode())
                resp = orjson.loads(ws.receive_text())
                assert resp["metaData"]["eventType"] == "ERROR"
            info_mock.assert_any_call(
                "Transport: Sent ERROR response frame",
                conversation_id="conv-1",
                response=error_resp,
            )

    def test_ws_logs_slow_message_breakdown_when_threshold_exceeded(
        self, mock_orchestrator, shutdown, registry
    ):
        async def slow_handle(_raw_json, _conversation_id=""):
            return OrchestratorResult(
                response=build_transcript_ack("conv-1", 0),
                disconnect=False,
                timings_ms={
                    "validate_ms": 0.11,
                    "prepare_ms": 0.22,
                    "kafka_send_ms": 0.33,
                    "commit_ms": 0.44,
                    "ack_build_ms": 0.55,
                    "orchestrator_ms": 12.34,
                },
            )

        mock_orchestrator.handle_message.side_effect = slow_handle
        app = create_app(
            mock_orchestrator,
            shutdown,
            registry,
            log_slow_message_threshold_ms=1.0,
        )
        client = TestClient(app)

        with patch(
            "realtime_transcribe_service.transport.websocket_handler._elapsed_ms",
            side_effect=[0.12, 12.34, 0.45, 12.79],
        ), patch("realtime_transcribe_service.transport.websocket_handler.log.warning") as warn_mock:
            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-1"
            ) as ws:
                ws.send_text(orjson.dumps(_ongoing_message()).decode())
                resp = orjson.loads(ws.receive_text())
                assert resp["metaData"]["eventType"] == "TRANSCRIPT_ACK"

        slow_calls = [
            kwargs
            for args, kwargs in warn_mock.call_args_list
            if args == ("Transport: Slow message stage timings",)
        ]
        assert len(slow_calls) == 1
        slow_log = slow_calls[0]
        assert slow_log["conversation_id"] == "conv-1"
        assert slow_log["total_ms"] == 12.79
        assert slow_log["flow"] == {
            "request_event_type": "SESSION_ONGOING",
            "response_event_type": "TRANSCRIPT_ACK",
            "sequence_number": 0,
            "speaker": "Agent",
        }
        assert slow_log["outcome"] == {"disconnect": False}
        assert slow_log["timings_ms"] == {
            "decode_ms": 0.12,
            "server_processing_ms": 12.34,
            "send_ms": 0.45,
            "validate_ms": 0.11,
            "prepare_ms": 0.22,
            "kafka_send_ms": 0.33,
            "commit_ms": 0.44,
            "ack_build_ms": 0.55,
            "orchestrator_ms": 12.34,
        }
        assert slow_log["bottleneck"] == {
            "stage": "ack_build_ms",
            "ms": 0.55,
            "pct": 4.5,
        }
        assert slow_log["bottleneck_hint"] == "ack_build_ms=0.55ms (~4.5% of orchestrator_ms)"

    def test_ws_does_not_log_slow_message_when_threshold_disabled(
        self, mock_orchestrator, shutdown, registry
    ):
        async def slow_handle(_raw_json, _conversation_id=""):
            return OrchestratorResult(
                response=build_transcript_ack("conv-1", 0),
                disconnect=False,
                timings_ms={"orchestrator_ms": 12.34},
            )

        mock_orchestrator.handle_message.side_effect = slow_handle
        app = create_app(mock_orchestrator, shutdown, registry)
        client = TestClient(app)

        with patch(
            "realtime_transcribe_service.transport.websocket_handler._elapsed_ms",
            side_effect=[0.12, 12.34, 0.45],
        ), patch("realtime_transcribe_service.transport.websocket_handler.log.warning") as warn_mock:
            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-1"
            ) as ws:
                ws.send_text(orjson.dumps(_ongoing_message()).decode())
                resp = orjson.loads(ws.receive_text())
                assert resp["metaData"]["eventType"] == "TRANSCRIPT_ACK"

        assert not any(
            args == ("Transport: Slow message stage timings",)
            for args, _kwargs in warn_mock.call_args_list
        )


@pytest.mark.asyncio
async def test_send_error_and_close_swallows_inner_failure():
    from realtime_transcribe_service.transport import websocket_handler as wh

    ws = MagicMock()
    ws.client_state = WebSocketState.CONNECTED
    ws.send_text = AsyncMock(side_effect=RuntimeError("send failed"))
    ws.close = AsyncMock()
    await wh._send_error_and_close(
        ws, "conv-x", "E1001", "bad", wh.WsCloseCode.INVALID_PAYLOAD
    )


def test_orchestrator_bottleneck_prefers_leaf_max_and_falls_back_to_leaf_sum():
    from realtime_transcribe_service.transport import websocket_handler as wh

    out = wh._orchestrator_bottleneck(
        {"validate_ms": 1.0, "kafka_send_ms": 10.0, "orchestrator_ms": 50.0}
    )
    assert out is not None
    b, hint = out
    assert b["stage"] == "kafka_send_ms"
    assert b["ms"] == 10.0
    assert b["pct"] == 20.0
    assert "orchestrator_ms" in hint

    out2 = wh._orchestrator_bottleneck({"validate_ms": 2.0, "prepare_ms": 8.0})
    assert out2 is not None
    _b2, hint2 = out2
    assert _b2["stage"] == "prepare_ms"
    assert "summed leaf phases" in hint2


def test_orchestrator_bottleneck_none_when_no_leaf_timings():
    from realtime_transcribe_service.transport import websocket_handler as wh

    assert wh._orchestrator_bottleneck({"orchestrator_ms": 100.0}) is None
    assert wh._orchestrator_bottleneck(None) is None


def test_maybe_log_slow_message_skips_when_below_threshold():
    from realtime_transcribe_service.transport import websocket_handler as wh

    wh._slow_message_log_window_started_at = 0.0
    wh._slow_message_log_emitted_in_window = 0
    wh._slow_message_log_suppressed = 0

    with patch.object(wh, "_elapsed_ms", return_value=9.99), patch.object(
        wh.log, "warning"
    ) as warn_mock:
        wh._maybe_log_slow_message(
            threshold_ms=10.0,
            started_at=0.0,
            conversation_id="conv-x",
            raw_json=_ongoing_message("conv-x"),
            response=build_transcript_ack("conv-x", 0),
            disconnect=False,
        )

    warn_mock.assert_not_called()


def test_maybe_log_slow_message_rate_limits_and_reports_suppressed():
    from realtime_transcribe_service.transport import websocket_handler as wh

    wh._slow_message_log_window_started_at = 0.0
    wh._slow_message_log_emitted_in_window = 0
    wh._slow_message_log_suppressed = 0

    with patch.object(wh, "_elapsed_ms", return_value=120.0), patch(
        "realtime_transcribe_service.transport.websocket_handler.time.perf_counter",
        side_effect=[1.0, 1.2, 2.5],
    ), patch.object(wh.log, "warning") as warn_mock:
        for _ in range(3):
            wh._maybe_log_slow_message(
                threshold_ms=10.0,
                started_at=0.0,
                conversation_id="conv-x",
                raw_json=_ongoing_message("conv-x"),
                response=build_transcript_ack("conv-x", 0),
                disconnect=False,
            )

    assert warn_mock.call_count == 2
    first_kwargs = warn_mock.call_args_list[0].kwargs
    second_kwargs = warn_mock.call_args_list[1].kwargs
    assert first_kwargs["suppressed_since_last_emit"] == 0
    assert second_kwargs["suppressed_since_last_emit"] == 1


@pytest.mark.asyncio
async def test_send_error_and_close_logs_error_frame_when_enabled():
    from realtime_transcribe_service.transport import websocket_handler as wh

    ws = MagicMock()
    ws.client_state = WebSocketState.CONNECTED
    ws.send_text = AsyncMock()
    ws.close = AsyncMock()
    with patch("realtime_transcribe_service.transport.websocket_handler.log.info") as info_mock:
        await wh._send_error_and_close(
            ws,
            "conv-x",
            "E1001",
            "bad",
            wh.WsCloseCode.INVALID_PAYLOAD,
            log_ws_error_frames=True,
        )
    info_mock.assert_any_call(
        "Transport: Sent ERROR response frame",
        conversation_id="conv-x",
        response=ANY,
    )
    logged_response = next(
        kwargs["response"]
        for args, kwargs in info_mock.call_args_list
        if args == ("Transport: Sent ERROR response frame",) and kwargs.get("conversation_id") == "conv-x"
    )
    assert logged_response["error"]["code"] == "E1001"


@pytest.mark.asyncio
async def test_ownership_refresh_loop_error_closes_ws():
    from realtime_transcribe_service.transport import websocket_handler as wh

    ws = MagicMock()
    ws.client_state = WebSocketState.CONNECTED
    ws.send_text = AsyncMock()
    ws.close = AsyncMock()
    owner = ScriptedOwnerBackend([RuntimeError("owner store down")])

    await _ownership_refresh_loop(
        ws,
        "conv-x",
        ownership_guard=owner,
        ownership_token="owner-a",
        refresh_interval_sec=0.01,
    )

    ws.send_text.assert_awaited()
    ws.close.assert_awaited_once_with(code=wh.WsCloseCode.TRY_AGAIN_LATER)


@pytest.mark.asyncio
async def test_ownership_refresh_loop_conflict_closes_ws():
    from realtime_transcribe_service.transport import websocket_handler as wh

    ws = MagicMock()
    ws.client_state = WebSocketState.CONNECTED
    ws.send_text = AsyncMock()
    ws.close = AsyncMock()
    owner = ScriptedOwnerBackend([False])

    await _ownership_refresh_loop(
        ws,
        "conv-x",
        ownership_guard=owner,
        ownership_token="owner-a",
        refresh_interval_sec=0.01,
    )

    ws.send_text.assert_awaited()
    ws.close.assert_awaited_once_with(code=wh.WsCloseCode.POLICY_VIOLATION)


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

    @pytest.mark.asyncio
    async def test_close_all_closes_all_sockets_for_same_conversation(
        self, registry: ConnectionRegistry
    ):
        ws1 = MagicMock()
        ws1.client_state = WebSocketState.CONNECTED
        ws1.close = AsyncMock()
        ws2 = MagicMock()
        ws2.client_state = WebSocketState.CONNECTED
        ws2.close = AsyncMock()
        registry.add("c1", ws1)
        registry.add("c1", ws2)

        await registry.close_all()

        ws1.close.assert_awaited_once()
        ws2.close.assert_awaited_once()
        assert registry.active_count == 0

    def test_add_remove(self, registry: ConnectionRegistry):
        mock_ws = AsyncMock()
        registry.add("conv-1", mock_ws)
        assert registry.active_count == 1
        registry.remove("conv-1")
        assert registry.active_count == 0

    def test_active_count_counts_duplicate_conversation_sockets(
        self, registry: ConnectionRegistry
    ):
        ws_old = MagicMock()
        ws_new = MagicMock()
        registry.add("conv-1", ws_old)
        registry.add("conv-1", ws_new)
        assert registry.active_count == 2

    def test_remove_nonexistent(self, registry: ConnectionRegistry):
        registry.remove("nonexistent")
        assert registry.active_count == 0

    def test_remove_nonexistent_with_ws_is_noop(self, registry: ConnectionRegistry):
        registry.remove("nonexistent", MagicMock())
        assert registry.active_count == 0

    def test_remove_with_ws_only_pops_same_instance(self, registry: ConnectionRegistry):
        """An older connection's ``finally`` block must not delete a newer registration for the same conversationId."""
        ws_old = MagicMock()
        ws_new = MagicMock()
        registry.add("conv-1", ws_old)
        registry.add("conv-1", ws_new)
        assert registry.active_count == 2
        registry.remove("conv-1", ws_old)
        assert registry.active_count == 1
        registry.remove("conv-1", ws_new)
        assert registry.active_count == 0

    def test_remove_without_ws_drops_all_sockets_for_conversation(
        self, registry: ConnectionRegistry
    ):
        registry.add("conv-1", MagicMock())
        registry.add("conv-1", MagicMock())
        registry.add("conv-2", MagicMock())
        registry.remove("conv-1")
        assert registry.active_count == 1

