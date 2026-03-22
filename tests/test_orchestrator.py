"""Tests for orchestrator 调度层 — 覆盖 7 种场景。"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import fakeredis.aioredis
import pytest

from unittest.mock import MagicMock

from transcribe_service.orchestrator.two_phase import TwoPhaseOrchestrator
from transcribe_service.schemas.errors import ErrorCode, WsCloseCode
from transcribe_service.state_machine.base import PrepareResult
from transcribe_service.state_machine.redis_state import RedisStateMachine


@pytest.fixture
def mock_sm():
    sm = AsyncMock()
    sm.prepare = AsyncMock(return_value=PrepareResult.PRE_CHECK_OK)
    sm.commit = AsyncMock()
    sm.cleanup = AsyncMock()
    return sm


@pytest.fixture
def mock_producer():
    p = AsyncMock()
    p.send = AsyncMock()
    return p


@pytest.fixture
def orchestrator(mock_sm, mock_producer) -> TwoPhaseOrchestrator:
    return TwoPhaseOrchestrator(state_machine=mock_sm, producer=mock_producer)


class TestScenarioA:
    """A. 请求合法 + PRE_CHECK_OK + Kafka 成功 (SESSION_ONGOING) → ACK, 不断连。"""

    async def test_normal_ongoing(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, mock_producer, valid_ongoing_msg
    ):
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["metaData"]["eventType"] == "TRANSCRIPT_ACK"
        assert result.response["payload"]["sequenceNumber"] == 0
        assert result.disconnect is False
        mock_sm.prepare.assert_awaited_once()
        mock_producer.send.assert_awaited_once()
        mock_sm.commit.assert_awaited_once()
        mock_sm.cleanup.assert_not_awaited()


class TestScenarioB:
    """B. IDEMPOTENT → ACK, 不写 Kafka, 不推进 Redis, 不断连。"""

    async def test_idempotent(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, mock_producer, valid_ongoing_msg
    ):
        mock_sm.prepare.return_value = PrepareResult.IDEMPOTENT
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["metaData"]["eventType"] == "TRANSCRIPT_ACK"
        assert result.disconnect is False
        mock_producer.send.assert_not_awaited()
        mock_sm.commit.assert_not_awaited()


class TestScenarioC:
    """C. OUT_OF_ORDER → E1006, 断连 1008。"""

    async def test_out_of_order(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, valid_ongoing_msg
    ):
        mock_sm.prepare.return_value = PrepareResult.OUT_OF_ORDER
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["metaData"]["eventType"] == "ERROR"
        assert result.response["error"]["code"] == "E1006"
        assert result.disconnect is True
        assert result.close_code == 1008


class TestScenarioD:
    """D. Schema 校验失败 → ERROR, 断连 1008 (或 1007)。"""

    async def test_missing_field(self, orchestrator: TwoPhaseOrchestrator, valid_ongoing_msg):
        del valid_ongoing_msg["metaData"]["conversationId"]
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["metaData"]["eventType"] == "ERROR"
        assert result.disconnect is True
        assert result.close_code == 1008

    async def test_invalid_event_type(self, orchestrator: TwoPhaseOrchestrator, valid_ongoing_msg):
        valid_ongoing_msg["metaData"]["eventType"] = "UNKNOWN"
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["error"]["code"] == "E1002"
        assert result.disconnect is True

    async def test_type_mismatch_payload(self, orchestrator: TwoPhaseOrchestrator, valid_ongoing_msg):
        valid_ongoing_msg["payload"] = "not-an-object"
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["error"]["code"] == "E1004"
        assert result.disconnect is True
        assert result.close_code == 1008

    async def test_wrong_type_conversation_id_returns_e1004(
        self, orchestrator: TwoPhaseOrchestrator, valid_ongoing_msg
    ):
        valid_ongoing_msg["metaData"]["conversationId"] = 123
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["error"]["code"] == "E1004"
        assert result.response["metaData"]["conversationId"] == ""
        assert result.disconnect is True
        assert result.close_code == 1008

    async def test_invalid_timestamp_returns_e1005(
        self, orchestrator: TwoPhaseOrchestrator, valid_ongoing_msg
    ):
        valid_ongoing_msg["metaData"]["callStartTimeStamp"] = "bad-ts"
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["error"]["code"] == "E1005"
        assert result.disconnect is True
        assert result.close_code == 1008

    async def test_non_utc_timestamp_returns_e1005(
        self, orchestrator: TwoPhaseOrchestrator, valid_ongoing_msg
    ):
        valid_ongoing_msg["payload"]["createdAtTimeStamp"] = "2025-03-21T18:32:20.000+08:00"
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["error"]["code"] == "E1005"
        assert result.disconnect is True
        assert result.close_code == 1008

    async def test_business_rule_violation_returns_e1009(
        self, orchestrator: TwoPhaseOrchestrator, valid_ongoing_msg
    ):
        valid_ongoing_msg["metaData"]["callEndTimeStamp"] = "2025-03-21T10:45:00.000Z"
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["error"]["code"] == "E1009"
        assert result.disconnect is True
        assert result.close_code == 1008

    async def test_is_final_false_returns_e1009(
        self, orchestrator: TwoPhaseOrchestrator, valid_ongoing_msg
    ):
        valid_ongoing_msg["payload"]["isFinal"] = False
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["error"]["code"] == "E1009"
        assert result.disconnect is True
        assert result.close_code == 1008


class TestScenarioE:
    """E. Kafka 失败/超时 → E1008/E1012, 不 commit, 断连 1013。"""

    async def test_kafka_timeout(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, mock_producer, valid_ongoing_msg
    ):
        mock_producer.send.side_effect = asyncio.TimeoutError()
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["error"]["code"] == "E1012"
        assert result.disconnect is True
        assert result.close_code == 1013
        mock_sm.commit.assert_not_awaited()

    async def test_kafka_failure(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, mock_producer, valid_ongoing_msg
    ):
        mock_producer.send.side_effect = RuntimeError("broker down")
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["error"]["code"] == "E1008"
        assert result.disconnect is True
        assert result.close_code == 1013
        mock_sm.commit.assert_not_awaited()

    async def test_retry_same_seq_after_kafka_failure_is_lossless(self, valid_ongoing_msg):
        """首次 Kafka 失败不 commit；同一 seq 重试成功；再次重放命中幂等 ACK。"""
        client = fakeredis.aioredis.FakeRedis(decode_responses=True)
        state_machine = RedisStateMachine(client=client, active_ttl_sec=3600, final_ttl_sec=60)
        producer = AsyncMock()
        producer.send = AsyncMock(side_effect=[RuntimeError("broker down"), None])
        orchestrator = TwoPhaseOrchestrator(state_machine=state_machine, producer=producer)

        try:
            first = await orchestrator.handle_message(valid_ongoing_msg)
            assert first.response["error"]["code"] == "E1008"
            assert first.disconnect is True
            assert first.close_code == 1013

            cid = valid_ongoing_msg["metaData"]["conversationId"]
            key = f"transcript:session:{cid}"
            assert await client.get(key) == "0"

            second = await orchestrator.handle_message(valid_ongoing_msg)
            assert second.response["metaData"]["eventType"] == "TRANSCRIPT_ACK"
            assert second.disconnect is False
            assert await client.get(key) == "1"

            third = await orchestrator.handle_message(valid_ongoing_msg)
            assert third.response["metaData"]["eventType"] == "TRANSCRIPT_ACK"
            assert third.disconnect is False
            assert await client.get(key) == "1"
            assert producer.send.await_count == 2
        finally:
            await state_machine.close()


class TestScenarioF:
    """F. 服务端未捕获异常 → E1007, 断连 1011。"""

    async def test_unexpected_exception(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, valid_ongoing_msg
    ):
        mock_sm.prepare.side_effect = RuntimeError("unexpected")
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["error"]["code"] == "E1007"
        assert result.disconnect is True
        assert result.close_code == 1011


class TestClassifyValidationError:
    """_classify_validation_error 分支（避免依赖 Pydantic 具体 type 字符串）。"""

    def test_branch_json(self):
        e = MagicMock()
        e.errors.return_value = [{"type": "json_invalid"}]
        c, w = TwoPhaseOrchestrator._classify_validation_error(e)
        assert c == ErrorCode.E1001
        assert w == WsCloseCode.INVALID_PAYLOAD

    def test_branch_missing(self):
        e = MagicMock()
        e.errors.return_value = [{"type": "missing"}]
        c, w = TwoPhaseOrchestrator._classify_validation_error(e)
        assert c == ErrorCode.E1003

    def test_branch_enum(self):
        e = MagicMock()
        e.errors.return_value = [{"type": "enum"}]
        c, w = TwoPhaseOrchestrator._classify_validation_error(e)
        assert c == ErrorCode.E1002

    def test_branch_literal(self):
        e = MagicMock()
        e.errors.return_value = [{"type": "literal_error"}]
        c, w = TwoPhaseOrchestrator._classify_validation_error(e)
        assert c == ErrorCode.E1002

    def test_branch_intish_without_parsing_substring(self):
        """避免 type 中含 'parsing' 子串，否则会命中 JSON 类分支。"""
        e = MagicMock()
        e.errors.return_value = [{"type": "int_type"}]
        c, w = TwoPhaseOrchestrator._classify_validation_error(e)
        assert c == ErrorCode.E1004

    def test_branch_datetime_without_parsing_substring(self):
        e = MagicMock()
        e.errors.return_value = [{"type": "clock_isoformat"}]
        c, w = TwoPhaseOrchestrator._classify_validation_error(e)
        assert c == ErrorCode.E1005

    def test_branch_datetime_with_parsing_substring(self):
        e = MagicMock()
        e.errors.return_value = [{"type": "datetime_from_date_parsing"}]
        c, w = TwoPhaseOrchestrator._classify_validation_error(e)
        assert c == ErrorCode.E1005

    def test_branch_bool_in_type(self):
        e = MagicMock()
        e.errors.return_value = [{"type": "boolean_error"}]
        c, w = TwoPhaseOrchestrator._classify_validation_error(e)
        assert c == ErrorCode.E1004

    def test_branch_value_error(self):
        e = MagicMock()
        e.errors.return_value = [{"type": "value_error"}]
        c, w = TwoPhaseOrchestrator._classify_validation_error(e)
        assert c == ErrorCode.E1009
        assert w == WsCloseCode.POLICY_VIOLATION

    def test_fallback_no_match(self):
        e = MagicMock()
        e.errors.return_value = [{"type": "other"}]
        c, w = TwoPhaseOrchestrator._classify_validation_error(e)
        assert c == ErrorCode.E1003


class TestScenarioG:
    """G. SESSION_COMPLETE → ACK, cleanup, 断连 1000。"""

    async def test_session_complete(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, mock_producer, valid_complete_msg
    ):
        result = await orchestrator.handle_message(valid_complete_msg)
        assert result.response["metaData"]["eventType"] == "TRANSCRIPT_ACK"
        assert result.response["payload"]["sequenceNumber"] == 42
        assert result.disconnect is True
        assert result.close_code == 1000
        mock_sm.commit.assert_awaited_once()
        mock_sm.cleanup.assert_awaited_once()

    async def test_session_complete_cleanup_failure_degrades_to_ack(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, mock_producer, valid_complete_msg
    ):
        mock_sm.cleanup.side_effect = RuntimeError("cleanup boom")

        result = await orchestrator.handle_message(valid_complete_msg)

        assert result.response["metaData"]["eventType"] == "TRANSCRIPT_ACK"
        assert result.response["payload"]["sequenceNumber"] == 42
        assert result.disconnect is True
        assert result.close_code == 1000
        mock_producer.send.assert_awaited_once()
        mock_sm.commit.assert_awaited_once()
        mock_sm.cleanup.assert_awaited_once()
