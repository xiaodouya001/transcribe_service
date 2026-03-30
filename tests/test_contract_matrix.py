"""Contract-level scenario-matrix tests.

Goal: lock the most important error-code, close-code, and ACK semantics into one
test suite so later implementation changes cannot drift away from the API contract
and established design.
"""

from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis
import jwt
import orjson
import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from realtime_transcribe_service.auth.jwt_bearer import JwtBearerAuthBackend
from realtime_transcribe_service.converter.kafka_message_converter import KafkaMessageConverter
from realtime_transcribe_service.orchestrator.two_phase import TwoPhaseOrchestrator
from realtime_transcribe_service.redis.ownership_guard import RedisConversationOwnershipGuard
from realtime_transcribe_service.redis.protocols import PrepareOutcome, PrepareResult
from realtime_transcribe_service.schemas.response import build_transcript_ack
from realtime_transcribe_service.shutdown.graceful import GracefulShutdown
from realtime_transcribe_service.transport.websocket_handler import ConnectionRegistry, create_app

AUTH_SIGNING_MATERIAL = "signing-material-0123456789-material-012345"
WRONG_AUTH_SIGNING_MATERIAL = "wrong-material-0123456789-material-012345"


@pytest.fixture
def mock_sm():
    sm = AsyncMock()
    sm.prepare = AsyncMock(return_value=PrepareOutcome(status=PrepareResult.PRE_CHECK_OK))
    sm.commit = AsyncMock()
    sm.cleanup = AsyncMock()
    return sm


@pytest.fixture
def mock_producer():
    producer = AsyncMock()
    producer.send = AsyncMock()
    return producer


def _build_app(orchestrator):
    return create_app(orchestrator, GracefulShutdown(), ConnectionRegistry())


def _bearer_headers(signing_material: str, *, exp_delta_sec: int = 3600) -> dict[str, str]:
    token = jwt.encode(
        {"sub": "fano-backend", "exp": datetime.now(timezone.utc) + timedelta(seconds=exp_delta_sec)},
        signing_material,
        algorithm="HS256",
    )
    return {"Authorization": f"Bearer {token}"}


class TestTransportContractMatrix:
    def test_missing_query_conversation_id_returns_e1003_and_http_400(self):
        """E-01: missing query ``conversationId`` before the handshake returns HTTP 400 / E1003."""
        orchestrator = AsyncMock()
        orchestrator.handle_message = AsyncMock()
        client = TestClient(_build_app(orchestrator))

        with pytest.raises(Exception) as ei:
            with client.websocket_connect("/ws/v1/realtime-transcriptions"):
                pass

        assert hasattr(ei.value, "status_code") and ei.value.status_code == 400
        body = getattr(ei.value, "text", "")
        assert "E1003" in body
        assert "Query parameter 'conversationId' is required" in body
        orchestrator.handle_message.assert_not_awaited()

    def test_draining_returns_e1008_and_http_503(self):
        """E-02: when the service is draining, the handshake is rejected with HTTP 503 / E1008."""
        orchestrator = AsyncMock()
        orchestrator.handle_message = AsyncMock()
        shutdown = GracefulShutdown()
        shutdown._draining = True
        client = TestClient(create_app(orchestrator, shutdown, ConnectionRegistry()))

        with pytest.raises(Exception) as ei:
            with client.websocket_connect("/ws/v1/realtime-transcriptions?conversationId=conv-1"):
                pass

        assert hasattr(ei.value, "status_code") and ei.value.status_code == 503
        body = getattr(ei.value, "text", "")
        assert "E1008" in body
        assert "Service draining" in body
        orchestrator.handle_message.assert_not_awaited()

    def test_max_connections_returns_e1008_and_http_429(self):
        """E-03: exceeding the connection limit before the handshake returns HTTP 429 / E1008."""
        orchestrator = AsyncMock()
        orchestrator.handle_message = AsyncMock()
        registry = ConnectionRegistry()
        registry.add("existing", MagicMock())
        client = TestClient(
            create_app(
                orchestrator,
                GracefulShutdown(),
                registry,
                max_connections=1,
            )
        )

        with pytest.raises(Exception) as ei:
            with client.websocket_connect("/ws/v1/realtime-transcriptions?conversationId=conv-2"):
                pass

        assert hasattr(ei.value, "status_code") and ei.value.status_code == 429
        body = getattr(ei.value, "text", "")
        assert "E1008" in body
        assert "Too many connections" in body
        orchestrator.handle_message.assert_not_awaited()

    def test_missing_or_invalid_bearer_jwt_returns_e1010_and_http_401(self):
        """E-17: missing or invalid Bearer JWT during handshake returns HTTP 401 / E1010."""
        orchestrator = AsyncMock()
        orchestrator.handle_message = AsyncMock()
        client = TestClient(
            create_app(
                orchestrator,
                GracefulShutdown(),
                ConnectionRegistry(),
                auth_backend=JwtBearerAuthBackend(signing_material=AUTH_SIGNING_MATERIAL),
            )
        )

        with pytest.raises(Exception) as ei:
            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-1",
                headers=_bearer_headers(WRONG_AUTH_SIGNING_MATERIAL),
            ):
                pass

        assert hasattr(ei.value, "status_code") and ei.value.status_code == 401
        body = getattr(ei.value, "text", "")
        assert "E1010" in body
        assert "Authentication failed" in body
        orchestrator.handle_message.assert_not_awaited()

    def test_invalid_json_returns_e1001_and_close_1007(self):
        """E-04: JSON parsing failure returns E1001 and closes with 1007."""
        orchestrator = AsyncMock()
        orchestrator.handle_message = AsyncMock(
            return_value=build_transcript_ack("conv-1", 0)  # pragma: no cover - should never be used
        )
        client = TestClient(_build_app(orchestrator))

        with client.websocket_connect("/ws/v1/realtime-transcriptions?conversationId=conv-1") as ws:
            ws.send_text("not json")
            resp = orjson.loads(ws.receive_text())
            assert resp["error"]["code"] == "E1001"
            with pytest.raises(WebSocketDisconnect) as ei:
                ws.receive_text()
            assert ei.value.code == 1007

        orchestrator.handle_message.assert_not_awaited()

    def test_non_object_json_returns_e1004_and_close_1008(self, mock_sm, mock_producer):
        """E-07: a non-object top-level JSON payload returns E1004 and closes with 1008."""
        orchestrator = TwoPhaseOrchestrator(
            mock_sm,
            mock_producer,
            message_converter=KafkaMessageConverter(),
        )
        client = TestClient(_build_app(orchestrator))

        with client.websocket_connect("/ws/v1/realtime-transcriptions?conversationId=conv-1") as ws:
            ws.send_text("[]")
            resp = orjson.loads(ws.receive_text())
            assert resp["error"]["code"] == "E1004"
            assert resp["metaData"]["conversationId"] == "conv-1"
            with pytest.raises(WebSocketDisconnect) as ei:
                ws.receive_text()
            assert ei.value.code == 1008

        mock_sm.prepare.assert_not_awaited()
        mock_producer.send.assert_not_awaited()

    def test_transport_internal_exception_returns_e1007_and_close_1011(self):
        """E-13: an uncaught transport-layer exception returns E1007 and closes with 1011."""
        orchestrator = AsyncMock()
        orchestrator.handle_message = AsyncMock()
        client = TestClient(_build_app(orchestrator))

        with patch(
            "realtime_transcribe_service.transport.websocket_handler.orjson.loads",
            side_effect=RuntimeError("boom"),
        ):
            with client.websocket_connect(
                "/ws/v1/realtime-transcriptions?conversationId=conv-1"
            ) as ws:
                ws.send_text("{}")
                resp = json.loads(ws.receive_text())
                assert resp["error"]["code"] == "E1007"
                with pytest.raises(WebSocketDisconnect) as ei:
                    ws.receive_text()
                assert ei.value.code == 1011

        orchestrator.handle_message.assert_not_awaited()

    def test_conversation_id_mismatch_returns_e1009_and_close_1008(self):
        """E-14: mismatched query/body conversationId returns E1009 and closes with 1008."""
        orchestrator = AsyncMock()
        orchestrator.handle_message = AsyncMock()
        client = TestClient(_build_app(orchestrator))

        with client.websocket_connect("/ws/v1/realtime-transcriptions?conversationId=conv-1") as ws:
            ws.send_text('{"metaData":{"conversationId":"conv-2"}}')
            resp = orjson.loads(ws.receive_text())
            assert resp["error"]["code"] == "E1009"
            with pytest.raises(WebSocketDisconnect) as ei:
                ws.receive_text()
            assert ei.value.code == 1008

        orchestrator.handle_message.assert_not_awaited()

    def test_active_writer_conflict_returns_e1009_and_http_403(self):
        """E-16: a second concurrent sender for the same conversationId is rejected during the handshake with HTTP 403 / E1009."""
        orchestrator = AsyncMock()
        orchestrator.handle_message = AsyncMock()
        owner = RedisConversationOwnershipGuard(
            client=fakeredis.aioredis.FakeRedis(decode_responses=True),
            guard_ttl_sec=30,
            key_prefix="realtime-transcribe-service:conversation-owner",
        )
        client = TestClient(
            create_app(
                orchestrator,
                GracefulShutdown(),
                ConnectionRegistry(),
                ownership_guard=owner,
            )
        )

        try:
            with client.websocket_connect("/ws/v1/realtime-transcriptions?conversationId=conv-1"):
                with pytest.raises(Exception) as ei:
                    with client.websocket_connect(
                        "/ws/v1/realtime-transcriptions?conversationId=conv-1"
                    ):
                        pass
                assert hasattr(ei.value, "status_code") and ei.value.status_code == 403
                body = getattr(ei.value, "text", "")
                assert "E1009" in body
                assert "Only one sender connection is allowed" in body
        finally:
            import asyncio

            asyncio.run(owner.close())

        orchestrator.handle_message.assert_not_awaited()


class TestOrchestratorContractMatrix:
    @pytest.mark.parametrize(
        ("mutator", "expected_code", "expected_close"),
        [
            pytest.param(
                lambda msg: msg["metaData"].__setitem__("eventType", "INVALID"),
                "E1002",
                1008,
                id="E-05",
            ),
            pytest.param(
                lambda msg: msg["payload"].pop("dialect"),
                "E1003",
                1008,
                id="E-06",
            ),
            pytest.param(
                lambda msg: msg["metaData"].__setitem__("conversationId", 123),
                "E1004",
                1008,
                id="E-07",
            ),
            pytest.param(
                lambda msg: msg["payload"].__setitem__(
                    "speakTimeStamp", "2025-03-21T18:32:20.000+08:00"
                ),
                "E1005",
                1008,
                id="E-08-speak",
            ),
            pytest.param(
                lambda msg: msg["payload"].__setitem__(
                    "transcriptGenerateTimeStamp", "2025-03-21T18:32:20.000+08:00"
                ),
                "E1005",
                1008,
                id="E-08-asr",
            ),
            pytest.param(
                lambda msg: msg["payload"].__setitem__("isFinal", False),
                "E1009",
                1008,
                id="E-15",
            ),
        ],
    )
    async def test_schema_error_matrix(
        self,
        valid_ongoing_msg,
        mock_sm,
        mock_producer,
        mutator,
        expected_code,
        expected_close,
    ):
        """E-05/E-06/E-07/E-08/E-15: schema and business-rule validation matrix."""
        msg = copy.deepcopy(valid_ongoing_msg)
        mutator(msg)
        orchestrator = TwoPhaseOrchestrator(
            mock_sm,
            mock_producer,
            message_converter=KafkaMessageConverter(),
        )

        result = await orchestrator.handle_message(msg)

        assert result.response["error"]["code"] == expected_code
        assert result.disconnect is True
        assert result.close_code == expected_close
        mock_sm.prepare.assert_not_awaited()
        mock_producer.send.assert_not_awaited()

    async def test_duplicate_seq_returns_ack_and_no_disconnect(
        self, valid_ongoing_msg, mock_sm, mock_producer
    ):
        """N-02: a duplicate seq hits the idempotent ACK path, stays connected, and does not write downstream again."""
        mock_sm.prepare.return_value = PrepareOutcome(status=PrepareResult.IDEMPOTENT)
        orchestrator = TwoPhaseOrchestrator(
            mock_sm,
            mock_producer,
            message_converter=KafkaMessageConverter(),
        )

        result = await orchestrator.handle_message(copy.deepcopy(valid_ongoing_msg))

        assert result.response["metaData"]["eventType"] == "TRANSCRIPT_ACK"
        assert result.disconnect is False
        mock_producer.send.assert_not_awaited()
        mock_sm.commit.assert_not_awaited()

    async def test_duplicate_complete_returns_eol_ack_and_close_1000(
        self, valid_complete_msg, mock_sm, mock_producer
    ):
        """N-02: a duplicate COMPLETE on the idempotent path returns EOL_ACK and closes normally with 1000."""
        mock_sm.prepare.return_value = PrepareOutcome(status=PrepareResult.IDEMPOTENT)
        orchestrator = TwoPhaseOrchestrator(
            mock_sm,
            mock_producer,
            message_converter=KafkaMessageConverter(),
        )

        result = await orchestrator.handle_message(copy.deepcopy(valid_complete_msg))

        assert result.response["metaData"]["eventType"] == "EOL_ACK"
        assert result.response["payload"]["sequenceNumber"] == 42
        assert result.disconnect is True
        assert result.close_code == 1000
        mock_producer.send.assert_not_awaited()
        mock_sm.commit.assert_not_awaited()
        mock_sm.cleanup.assert_not_awaited()

    async def test_complete_returns_eol_ack_and_close_1000(
        self, valid_complete_msg, mock_sm, mock_producer
    ):
        """N-03: successful SESSION_COMPLETE returns EOL_ACK and closes with 1000."""
        orchestrator = TwoPhaseOrchestrator(
            mock_sm,
            mock_producer,
            message_converter=KafkaMessageConverter(),
        )

        result = await orchestrator.handle_message(copy.deepcopy(valid_complete_msg))

        assert result.response["metaData"]["eventType"] == "EOL_ACK"
        assert result.response["payload"]["sequenceNumber"] == 42
        assert result.disconnect is True
        assert result.close_code == 1000
        mock_producer.send.assert_awaited_once()
        mock_sm.commit.assert_awaited_once()
        mock_sm.cleanup.assert_awaited_once()

    async def test_out_of_order_returns_e1006_and_close_1008(
        self, valid_ongoing_msg, mock_sm, mock_producer
    ):
        """E-09: out-of-order sequence numbers return E1006 and close with 1008."""
        mock_sm.prepare.return_value = PrepareOutcome(status=PrepareResult.OUT_OF_ORDER)
        orchestrator = TwoPhaseOrchestrator(
            mock_sm,
            mock_producer,
            message_converter=KafkaMessageConverter(),
        )

        result = await orchestrator.handle_message(copy.deepcopy(valid_ongoing_msg))

        assert result.response["error"]["code"] == "E1006"
        assert result.disconnect is True
        assert result.close_code == 1008
        mock_producer.send.assert_not_awaited()
        mock_sm.commit.assert_not_awaited()

    async def test_downstream_timeout_returns_e1011_and_close_1013(
        self, valid_ongoing_msg, mock_sm, mock_producer
    ):
        """E-10: downstream timeout returns E1011 and closes with 1013."""
        mock_producer.send.side_effect = TimeoutError()
        orchestrator = TwoPhaseOrchestrator(
            mock_sm,
            mock_producer,
            message_converter=KafkaMessageConverter(),
        )

        result = await orchestrator.handle_message(copy.deepcopy(valid_ongoing_msg))

        assert result.response["error"]["code"] == "E1011"
        assert result.disconnect is True
        assert result.close_code == 1013
        mock_sm.commit.assert_not_awaited()

    async def test_downstream_failure_returns_e1008_and_close_1013(
        self, valid_ongoing_msg, mock_sm, mock_producer
    ):
        """E-11: downstream failure returns E1008 and closes with 1013."""
        mock_producer.send.side_effect = RuntimeError("broker down")
        orchestrator = TwoPhaseOrchestrator(
            mock_sm,
            mock_producer,
            message_converter=KafkaMessageConverter(),
        )

        result = await orchestrator.handle_message(copy.deepcopy(valid_ongoing_msg))

        assert result.response["error"]["code"] == "E1008"
        assert result.disconnect is True
        assert result.close_code == 1013
        mock_sm.commit.assert_not_awaited()

    async def test_orchestrator_internal_exception_returns_e1007_and_close_1011(
        self, valid_ongoing_msg, mock_sm, mock_producer
    ):
        """E-12: an uncaught orchestrator exception returns E1007 and closes with 1011."""
        mock_sm.prepare.side_effect = RuntimeError("unexpected")
        orchestrator = TwoPhaseOrchestrator(
            mock_sm,
            mock_producer,
            message_converter=KafkaMessageConverter(),
        )

        result = await orchestrator.handle_message(copy.deepcopy(valid_ongoing_msg))

        assert result.response["error"]["code"] == "E1007"
        assert result.disconnect is True
        assert result.close_code == 1011

