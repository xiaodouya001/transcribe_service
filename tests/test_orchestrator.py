"""Tests for the orchestrator layer covering seven scenarios."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import fakeredis.aioredis
import pytest

from unittest.mock import MagicMock

from realtime_transcribe_service.converter.kafka_message_converter import KafkaMessageConverter
from realtime_transcribe_service.orchestrator.two_phase import TwoPhaseOrchestrator
from realtime_transcribe_service.redis.protocols import PrepareOutcome, PrepareResult
from realtime_transcribe_service.redis.sequence_state_machine import RedisSequenceStateMachine
from realtime_transcribe_service.schemas.errors import ErrorCode, WsCloseCode


@pytest.fixture
def mock_sm():
    sm = AsyncMock()
    sm.prepare = AsyncMock(return_value=PrepareOutcome(status=PrepareResult.PRE_CHECK_OK))
    sm.commit = AsyncMock()
    sm.cleanup = AsyncMock()
    return sm


@pytest.fixture
def mock_producer():
    p = AsyncMock()
    p.send = AsyncMock()
    return p


@pytest.fixture
def mock_converter():
    converter = MagicMock()
    converter.to_kafka_payload = MagicMock(
        side_effect=lambda _msg, raw: {**raw, "enrich": {"eventProduceTimestamp": "2026-03-27T10:00:00.000Z"}}
    )
    return converter


@pytest.fixture
def orchestrator(mock_sm, mock_producer, mock_converter) -> TwoPhaseOrchestrator:
    return TwoPhaseOrchestrator(
        state_machine=mock_sm,
        producer=mock_producer,
        message_converter=mock_converter,
    )


class TestScenarioA:
    """A. Valid request + PRE_CHECK_OK + Kafka success (SESSION_ONGOING) -> ACK without disconnect."""

    async def test_normal_ongoing(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, mock_producer, mock_converter, valid_ongoing_msg
    ):
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["metaData"]["eventType"] == "TRANSCRIPT_ACK"
        assert result.response["payload"]["sequenceNumber"] == 0
        assert result.disconnect is False
        assert result.timings_ms is not None
        assert {"validate_ms", "prepare_ms", "kafka_send_ms", "commit_ms", "ack_build_ms", "orchestrator_ms"} <= set(result.timings_ms)
        mock_sm.prepare.assert_awaited_once()
        mock_converter.to_kafka_payload.assert_called_once()
        mock_producer.send.assert_awaited_once()
        sent_payload = mock_producer.send.await_args.args[1]
        assert "enrich" in sent_payload
        mock_sm.commit.assert_awaited_once()
        mock_sm.cleanup.assert_not_awaited()


class TestScenarioB:
    """B. IDEMPOTENT -> return the matching ACK. ONGOING stays connected; COMPLETE closes with 1000."""

    async def test_idempotent(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, mock_producer, mock_converter, valid_ongoing_msg
    ):
        mock_sm.prepare.return_value = PrepareOutcome(status=PrepareResult.IDEMPOTENT)
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["metaData"]["eventType"] == "TRANSCRIPT_ACK"
        assert result.disconnect is False
        mock_producer.send.assert_not_awaited()
        mock_converter.to_kafka_payload.assert_not_called()
        mock_sm.commit.assert_not_awaited()

    async def test_idempotent_complete_returns_eol_ack_and_close(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, mock_producer, mock_converter, valid_complete_msg
    ):
        mock_sm.prepare.return_value = PrepareOutcome(status=PrepareResult.IDEMPOTENT)
        result = await orchestrator.handle_message(valid_complete_msg)
        assert result.response["metaData"]["eventType"] == "EOL_ACK"
        assert result.response["payload"]["sequenceNumber"] == 42
        assert result.disconnect is True
        assert result.close_code == 1000
        mock_producer.send.assert_not_awaited()
        mock_converter.to_kafka_payload.assert_not_called()
        mock_sm.commit.assert_not_awaited()
        mock_sm.cleanup.assert_not_awaited()


class TestScenarioC:
    """C. OUT_OF_ORDER -> E1006 and disconnect 1008."""

    async def test_out_of_order(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, mock_converter, valid_ongoing_msg
    ):
        mock_sm.prepare.return_value = PrepareOutcome(status=PrepareResult.OUT_OF_ORDER)
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["metaData"]["eventType"] == "ERROR"
        assert result.response["error"]["code"] == "E1006"
        assert result.disconnect is True
        assert result.close_code == 1008
        mock_converter.to_kafka_payload.assert_not_called()

    async def test_out_of_order_warning_includes_error_and_close_code(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, valid_ongoing_msg
    ):
        mock_sm.prepare.return_value = PrepareOutcome(
            status=PrepareResult.OUT_OF_ORDER,
            expected_sequence=1,
        )
        with patch("realtime_transcribe_service.orchestrator.two_phase.log.warning") as warn_mock:
            result = await orchestrator.handle_message(valid_ongoing_msg)

        assert result.response["error"]["code"] == "E1006"
        assert result.disconnect is True
        assert result.close_code == 1008
        warn_mock.assert_any_call(
            "Orchestrator: Sequence number out of order",
            conversation_id=valid_ongoing_msg["metaData"]["conversationId"],
            seq=valid_ongoing_msg["payload"]["sequenceNumber"],
            actual_sequence=valid_ongoing_msg["payload"]["sequenceNumber"],
            expected_sequence=1,
            error_code="E1006",
            close_code=1008,
        )

    async def test_out_of_order_warning_includes_error_and_close_code(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, valid_ongoing_msg
    ):
        mock_sm.prepare.return_value = PrepareOutcome(
            status=PrepareResult.OUT_OF_ORDER,
            expected_sequence=1,
        )
        with patch("realtime_transcribe_service.orchestrator.two_phase.log.warning") as warn_mock:
            result = await orchestrator.handle_message(valid_ongoing_msg)

        assert result.response["error"]["code"] == "E1006"
        assert result.disconnect is True
        assert result.close_code == 1008
        warn_mock.assert_any_call(
            "Orchestrator: Sequence number out of order",
            conversation_id=valid_ongoing_msg["metaData"]["conversationId"],
            seq=valid_ongoing_msg["payload"]["sequenceNumber"],
            actual_sequence=valid_ongoing_msg["payload"]["sequenceNumber"],
            expected_sequence=1,
            error_code="E1006",
            close_code=1008,
        )


class TestScenarioD:
    """D. Schema validation failure -> ERROR and disconnect 1008 (or 1007)."""

    async def test_missing_field(
        self, orchestrator: TwoPhaseOrchestrator, mock_converter, valid_ongoing_msg
    ):
        del valid_ongoing_msg["metaData"]["conversationId"]
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["metaData"]["eventType"] == "ERROR"
        assert result.disconnect is True
        assert result.close_code == 1008
        assert result.timings_ms is not None
        assert {"validate_ms", "orchestrator_ms"} <= set(result.timings_ms)
        mock_converter.to_kafka_payload.assert_not_called()

    async def test_missing_field_uses_fallback_conversation_id(
        self, orchestrator: TwoPhaseOrchestrator, valid_ongoing_msg
    ):
        del valid_ongoing_msg["metaData"]["conversationId"]
        result = await orchestrator.handle_message(valid_ongoing_msg, "conv-1")
        assert result.response["error"]["code"] == "E1003"
        assert result.response["metaData"]["conversationId"] == "conv-1"
        assert result.disconnect is True
        assert result.close_code == 1008

    async def test_missing_required_dialect_returns_e1003(
        self, orchestrator: TwoPhaseOrchestrator, valid_ongoing_msg
    ):
        del valid_ongoing_msg["payload"]["dialect"]
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["error"]["code"] == "E1003"
        assert result.disconnect is True
        assert result.close_code == 1008

    async def test_missing_field_uses_fallback_conversation_id(
        self, orchestrator: TwoPhaseOrchestrator, valid_ongoing_msg
    ):
        del valid_ongoing_msg["metaData"]["conversationId"]
        result = await orchestrator.handle_message(valid_ongoing_msg, "conv-1")
        assert result.response["error"]["code"] == "E1003"
        assert result.response["metaData"]["conversationId"] == "conv-1"
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

    async def test_non_object_json_returns_e1004_with_fallback_conversation_id(
        self, orchestrator: TwoPhaseOrchestrator
    ):
        result = await orchestrator.handle_message([], "conv-1")
        assert result.response["error"]["code"] == "E1004"
        assert result.response["metaData"]["conversationId"] == "conv-1"
        assert result.disconnect is True
        assert result.close_code == 1008

    async def test_extra_payload_field_returns_e1004(
        self, orchestrator: TwoPhaseOrchestrator, valid_ongoing_msg
    ):
        valid_ongoing_msg["payload"]["agentId"] = "A1"
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["error"]["code"] == "E1004"
        assert result.disconnect is True
        assert result.close_code == 1008

    async def test_extra_metadata_field_returns_e1004(
        self, orchestrator: TwoPhaseOrchestrator, valid_ongoing_msg
    ):
        valid_ongoing_msg["metaData"]["staffId"] = "S1"
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["error"]["code"] == "E1004"
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

    async def test_non_utc_speak_timestamp_returns_e1005(
        self, orchestrator: TwoPhaseOrchestrator, valid_ongoing_msg
    ):
        valid_ongoing_msg["payload"]["speakTimeStamp"] = "2025-03-21T18:32:20.000+08:00"
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["error"]["code"] == "E1005"
        assert result.disconnect is True
        assert result.close_code == 1008

    async def test_non_utc_transcript_generate_timestamp_returns_e1005(
        self, orchestrator: TwoPhaseOrchestrator, valid_ongoing_msg
    ):
        valid_ongoing_msg["payload"]["transcriptGenerateTimeStamp"] = (
            "2025-03-21T18:32:20.000+08:00"
        )
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
    """E. Kafka failure/timeout -> E1008/E1011, no commit, disconnect 1013."""

    async def test_kafka_timeout(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, mock_producer, mock_converter, valid_ongoing_msg
    ):
        mock_producer.send.side_effect = asyncio.TimeoutError()
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["error"]["code"] == "E1011"
        assert result.disconnect is True
        assert result.close_code == 1013
        mock_converter.to_kafka_payload.assert_called_once()
        mock_sm.commit.assert_not_awaited()

    async def test_kafka_failure(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, mock_producer, mock_converter, valid_ongoing_msg
    ):
        mock_producer.send.side_effect = RuntimeError("broker down")
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["error"]["code"] == "E1008"
        assert result.disconnect is True
        assert result.close_code == 1013
        mock_converter.to_kafka_payload.assert_called_once()
        mock_sm.commit.assert_not_awaited()

    async def test_retry_same_seq_after_kafka_failure_is_lossless(self, valid_ongoing_msg):
        """The first Kafka failure must not commit; retrying the same seq succeeds; replaying again hits the idempotent ACK path."""
        client = fakeredis.aioredis.FakeRedis(decode_responses=True)
        state_machine = RedisSequenceStateMachine(
            client=client,
            active_ttl_sec=3600,
            final_ttl_sec=60,
            key_prefix="realtime-transcribe-service:expect-transcript-seq-num",
        )
        producer = AsyncMock()
        producer.send = AsyncMock(side_effect=[RuntimeError("broker down"), None])
        converter = KafkaMessageConverter()
        orchestrator = TwoPhaseOrchestrator(
            state_machine=state_machine,
            producer=producer,
            message_converter=converter,
        )

        ts = ["2020-01-01T00:00:00.001Z", "2020-01-01T00:00:00.002Z"]
        try:
            with patch(
                "realtime_transcribe_service.converter.kafka_message_converter.format_utc_timestamp",
                side_effect=ts,
            ):
                first = await orchestrator.handle_message(valid_ongoing_msg)
                assert first.response["error"]["code"] == "E1008"
                assert first.disconnect is True
                assert first.close_code == 1013

                cid = valid_ongoing_msg["metaData"]["conversationId"]
                key = f"realtime-transcribe-service:expect-transcript-seq-num:{cid}"
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
                first_payload = producer.send.await_args_list[0].args[1]
                second_payload = producer.send.await_args_list[1].args[1]
                assert first_payload["enrich"]["eventProduceTimestamp"] == ts[0]
                assert second_payload["enrich"]["eventProduceTimestamp"] == ts[1]
        finally:
            await state_machine.close()


class TestScenarioF:
    """F. Unhandled server exception -> E1007 and disconnect 1011."""

    async def test_unexpected_exception(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, valid_ongoing_msg
    ):
        mock_sm.prepare.side_effect = RuntimeError("unexpected")
        result = await orchestrator.handle_message(valid_ongoing_msg)
        assert result.response["error"]["code"] == "E1007"
        assert result.disconnect is True
        assert result.close_code == 1011


class TestClassifyValidationError:
    """Branches in ``_classify_validation_error`` without depending on Pydantic's exact type strings."""

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
        """Avoid putting the substring ``parsing`` in the type so it does not hit the JSON branch."""
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

    def test_branch_extra_forbidden(self):
        e = MagicMock()
        e.errors.return_value = [{"type": "extra_forbidden"}]
        c, w = TwoPhaseOrchestrator._classify_validation_error(e)
        assert c == ErrorCode.E1004
        assert w == WsCloseCode.POLICY_VIOLATION

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
    """G. SESSION_COMPLETE -> EOL_ACK, cleanup, and disconnect 1000."""

    async def test_session_complete(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, mock_producer, mock_converter, valid_complete_msg
    ):
        result = await orchestrator.handle_message(valid_complete_msg)
        assert result.response["metaData"]["eventType"] == "EOL_ACK"
        assert result.response["payload"]["sequenceNumber"] == 42
        assert result.disconnect is True
        assert result.close_code == 1000
        mock_sm.commit.assert_awaited_once()
        mock_sm.cleanup.assert_awaited_once()
        mock_converter.to_kafka_payload.assert_called_once()

    async def test_session_complete_cleanup_failure_degrades_to_ack(
        self, orchestrator: TwoPhaseOrchestrator, mock_sm, mock_producer, mock_converter, valid_complete_msg
    ):
        mock_sm.cleanup.side_effect = RuntimeError("cleanup boom")

        result = await orchestrator.handle_message(valid_complete_msg)

        assert result.response["metaData"]["eventType"] == "EOL_ACK"
        assert result.response["payload"]["sequenceNumber"] == 42
        assert result.disconnect is True
        assert result.close_code == 1000
        assert result.timings_ms is not None
        assert "cleanup_ms" in result.timings_ms
        mock_producer.send.assert_awaited_once()
        mock_sm.commit.assert_awaited_once()
        mock_sm.cleanup.assert_awaited_once()
        mock_converter.to_kafka_payload.assert_called_once()

