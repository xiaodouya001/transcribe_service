"""契约级场景矩阵测试。

目标：将最关键的错误码 / 关闭码 / ACK 语义集中锁成一组测试，
避免后续实现调整时悄悄偏离 API 契约与既有设计。

说明：`E1010` / `E1011` 在契约中已预留，但鉴权与资源存在性校验当前尚未实现，
因此不纳入本文件的可执行场景矩阵。
"""

from __future__ import annotations

import copy
import json
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from transcribe_service.orchestrator.two_phase import TwoPhaseOrchestrator
from transcribe_service.schemas.response import build_ack
from transcribe_service.shutdown.graceful import GracefulShutdown
from transcribe_service.state_machine.base import PrepareResult
from transcribe_service.transport.websocket_handler import ConnectionRegistry, create_app


@pytest.fixture
def mock_sm():
    sm = AsyncMock()
    sm.prepare = AsyncMock(return_value=PrepareResult.PRE_CHECK_OK)
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


class TestTransportContractMatrix:
    def test_missing_query_conversation_id_returns_e1003_and_http_400(self):
        """E-01：握手前缺少 query conversationId，返回 HTTP 400 / E1003。"""
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
        """E-02：服务处于 draining 状态时，握手前返回 HTTP 503 / E1008。"""
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
        """E-03：连接数超限时，握手前返回 HTTP 429 / E1008。"""
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

    def test_invalid_json_returns_e1001_and_close_1007(self):
        """E-04：JSON 解析失败时返回 E1001，并以 1007 断开。"""
        orchestrator = AsyncMock()
        orchestrator.handle_message = AsyncMock(
            return_value=build_ack("conv-1", 0)  # pragma: no cover - should never be used
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

    def test_transport_internal_exception_returns_e1007_and_close_1011(self):
        """E-13：传输层未捕获异常时返回 E1007，并以 1011 断开。"""
        orchestrator = AsyncMock()
        orchestrator.handle_message = AsyncMock()
        client = TestClient(_build_app(orchestrator))

        with patch(
            "transcribe_service.transport.websocket_handler.orjson.loads",
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
        """E-14：query 与 body conversationId 不一致时返回 E1009，并以 1008 断开。"""
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
                lambda msg: msg["metaData"].pop("conversationId"),
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
                    "createdAtTimeStamp", "2025-03-21T18:32:20.000+08:00"
                ),
                "E1005",
                1008,
                id="E-08",
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
        """E-05/E-06/E-07/E-08/E-15：schema 与业务规则校验矩阵。"""
        msg = copy.deepcopy(valid_ongoing_msg)
        mutator(msg)
        orchestrator = TwoPhaseOrchestrator(mock_sm, mock_producer)

        result = await orchestrator.handle_message(msg)

        assert result.response["error"]["code"] == expected_code
        assert result.disconnect is True
        assert result.close_code == expected_close
        mock_sm.prepare.assert_not_awaited()
        mock_producer.send.assert_not_awaited()

    async def test_duplicate_seq_returns_ack_and_no_disconnect(
        self, valid_ongoing_msg, mock_sm, mock_producer
    ):
        """N-02：重复 seq 命中幂等 ACK，不断连且不重复写下游。"""
        mock_sm.prepare.return_value = PrepareResult.IDEMPOTENT
        orchestrator = TwoPhaseOrchestrator(mock_sm, mock_producer)

        result = await orchestrator.handle_message(copy.deepcopy(valid_ongoing_msg))

        assert result.response["metaData"]["eventType"] == "TRANSCRIPT_ACK"
        assert result.disconnect is False
        mock_producer.send.assert_not_awaited()
        mock_sm.commit.assert_not_awaited()

    async def test_out_of_order_returns_e1006_and_close_1008(
        self, valid_ongoing_msg, mock_sm, mock_producer
    ):
        """E-09：序列号乱序时返回 E1006，并以 1008 断开。"""
        mock_sm.prepare.return_value = PrepareResult.OUT_OF_ORDER
        orchestrator = TwoPhaseOrchestrator(mock_sm, mock_producer)

        result = await orchestrator.handle_message(copy.deepcopy(valid_ongoing_msg))

        assert result.response["error"]["code"] == "E1006"
        assert result.disconnect is True
        assert result.close_code == 1008
        mock_producer.send.assert_not_awaited()
        mock_sm.commit.assert_not_awaited()

    async def test_downstream_timeout_returns_e1012_and_close_1013(
        self, valid_ongoing_msg, mock_sm, mock_producer
    ):
        """E-10：下游超时时返回 E1012，并以 1013 断开。"""
        mock_producer.send.side_effect = TimeoutError()
        orchestrator = TwoPhaseOrchestrator(mock_sm, mock_producer)

        result = await orchestrator.handle_message(copy.deepcopy(valid_ongoing_msg))

        assert result.response["error"]["code"] == "E1012"
        assert result.disconnect is True
        assert result.close_code == 1013
        mock_sm.commit.assert_not_awaited()

    async def test_downstream_failure_returns_e1008_and_close_1013(
        self, valid_ongoing_msg, mock_sm, mock_producer
    ):
        """E-11：下游失败时返回 E1008，并以 1013 断开。"""
        mock_producer.send.side_effect = RuntimeError("broker down")
        orchestrator = TwoPhaseOrchestrator(mock_sm, mock_producer)

        result = await orchestrator.handle_message(copy.deepcopy(valid_ongoing_msg))

        assert result.response["error"]["code"] == "E1008"
        assert result.disconnect is True
        assert result.close_code == 1013
        mock_sm.commit.assert_not_awaited()

    async def test_orchestrator_internal_exception_returns_e1007_and_close_1011(
        self, valid_ongoing_msg, mock_sm, mock_producer
    ):
        """E-12：编排层未捕获异常时返回 E1007，并以 1011 断开。"""
        mock_sm.prepare.side_effect = RuntimeError("unexpected")
        orchestrator = TwoPhaseOrchestrator(mock_sm, mock_producer)

        result = await orchestrator.handle_message(copy.deepcopy(valid_ongoing_msg))

        assert result.response["error"]["code"] == "E1007"
        assert result.disconnect is True
        assert result.close_code == 1011
