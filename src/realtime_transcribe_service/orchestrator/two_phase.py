"""Two-phase-commit orchestrator — coordinates the state machine and producer across seven scenarios.

Architectural boundary: depend only on abstract interfaces and never import concrete implementations.
"""

from __future__ import annotations

import asyncio
import time

from pydantic import ValidationError

from realtime_transcribe_service.config.logging_config import get_logger
from realtime_transcribe_service.converter.protocols import KafkaMessageConverterBackend
from realtime_transcribe_service.constants import MAX_ERROR_DETAILS_LEN, MAX_ERROR_MESSAGE_LEN
from realtime_transcribe_service.orchestrator.protocols import OrchestratorResult
from realtime_transcribe_service.producer.protocols import ProducerBackend
from realtime_transcribe_service.redis.protocols import PrepareResult, SequenceStateMachineBackend
from realtime_transcribe_service.schemas.error_codes import WsCloseCode
from realtime_transcribe_service.schemas.error_scenarios import ProtocolErrorScenario
from realtime_transcribe_service.schemas.events import EventType
from realtime_transcribe_service.schemas.request import InboundMessage
from realtime_transcribe_service.schemas.response import build_eol_ack, build_transcript_ack

log = get_logger(__name__)


class TwoPhaseOrchestrator:
    """Two-phase-commit orchestrator.

    Scenarios A-G follow the design plan exactly, with response frames, Kafka/Redis
    side effects, disconnect behavior, and close codes explicitly defined for each path.
    """

    def __init__(
        self,
        state_machine: SequenceStateMachineBackend,
        producer: ProducerBackend,
        message_converter: KafkaMessageConverterBackend,
    ) -> None:
        self._sm = state_machine
        self._producer = producer
        self._message_converter = message_converter

    @staticmethod
    def _build_success_ack(conversation_id: str, sequence_number: int, event_type: EventType) -> dict:
        if event_type == EventType.SESSION_COMPLETE:
            return build_eol_ack(conversation_id, sequence_number)
        return build_transcript_ack(conversation_id, sequence_number)

    @staticmethod
    def _disconnect_after_success(event_type: EventType) -> bool:
        return event_type == EventType.SESSION_COMPLETE

    @staticmethod
    def _elapsed_ms(started_at: float) -> float:
        return round((time.perf_counter() - started_at) * 1000, 2)

    @classmethod
    def _finalize_timings(cls, timings: dict[str, float], started_at: float) -> dict[str, float]:
        finalized = dict(timings)
        finalized["orchestrator_ms"] = cls._elapsed_ms(started_at)
        return finalized

    async def handle_message(
        self,
        raw_json: object,
        conversation_id: str = "",
    ) -> OrchestratorResult:
        """Handle one inbound message."""
        started_at = time.perf_counter()
        try:
            if isinstance(raw_json, dict):
                meta = raw_json.get("metaData")
                if isinstance(meta, dict) and "conversationId" in meta:
                    raw_conversation_id = meta.get("conversationId")
                    if isinstance(raw_conversation_id, str):
                        conversation_id = raw_conversation_id
            return await self._process(raw_json, conversation_id)
        except Exception as exc:
            # Scenario F: unhandled exception -> E1007 + disconnect 1011
            scenario = ProtocolErrorScenario.ORCHESTRATOR_INTERNAL_EXCEPTION
            log.exception(
                scenario.default_log_reason,
                conversation_id=conversation_id,
                error=str(exc),
            )
            return OrchestratorResult(
                response=scenario.build_response(
                    conversation_id,
                    details=str(exc)[:MAX_ERROR_MESSAGE_LEN],
                ),
                disconnect=True,
                close_code=scenario.require_ws_close_code(),
                timings_ms={"orchestrator_ms": self._elapsed_ms(started_at)},
            )

    async def _process(self, raw_json: object, conversation_id: str) -> OrchestratorResult:
        """Internal processing flow split by scenario."""
        process_started_at = time.perf_counter()
        timings: dict[str, float] = {}

        # ------------------------------------------------------------------
        # 1. Schema validation (Scenario D)
        # ------------------------------------------------------------------
        validate_started_at = time.perf_counter()
        try:
            msg = InboundMessage.model_validate(raw_json)
        except ValidationError as e:
            timings["validate_ms"] = self._elapsed_ms(validate_started_at)
            scenario = self._classify_validation_error(e)
            log.warning(
                scenario.default_log_reason,
                conversation_id=conversation_id,
                error_code=scenario.error_code.value,
                close_code=int(scenario.require_ws_close_code()),
                errors=str(e),
            )
            return OrchestratorResult(
                response=scenario.build_response(
                    conversation_id,
                    details=str(e)[:MAX_ERROR_DETAILS_LEN],
                ),
                disconnect=True,
                close_code=scenario.require_ws_close_code(),
                timings_ms=self._finalize_timings(timings, process_started_at),
            )
        timings["validate_ms"] = self._elapsed_ms(validate_started_at)

        cid = msg.metaData.conversationId
        seq = msg.payload.sequenceNumber
        event_type = msg.metaData.eventType

        # ------------------------------------------------------------------
        # 2. Prepare — atomic Lua pre-check
        # ------------------------------------------------------------------
        prepare_started_at = time.perf_counter()
        prepare = await self._sm.prepare(cid, seq)
        timings["prepare_ms"] = self._elapsed_ms(prepare_started_at)

        # Scenario B: IDEMPOTENT -> return ACK directly, do not write Kafka, do not advance Redis
        if prepare.status == PrepareResult.IDEMPOTENT:
            should_disconnect = self._disconnect_after_success(event_type)
            log.info(
                "Orchestrator: Idempotent replay hit, returning ACK directly",
                conversation_id=cid,
                seq=seq,
            )
            ack_started_at = time.perf_counter()
            ack = self._build_success_ack(cid, seq, event_type)
            timings["ack_build_ms"] = self._elapsed_ms(ack_started_at)
            if should_disconnect:
                return OrchestratorResult(
                    response=ack,
                    disconnect=True,
                    close_code=WsCloseCode.NORMAL,
                    timings_ms=self._finalize_timings(timings, process_started_at),
                )
            return OrchestratorResult(
                response=ack,
                disconnect=False,
                timings_ms=self._finalize_timings(timings, process_started_at),
            )

        # Scenario C: OUT_OF_ORDER -> E1006 + disconnect 1008
        if prepare.status == PrepareResult.OUT_OF_ORDER:
            scenario = ProtocolErrorScenario.SEQUENCE_OUT_OF_ORDER
            log.warning(
                scenario.default_log_reason,
                conversation_id=cid,
                seq=seq,
                actual_sequence=seq,
                expected_sequence=prepare.expected_sequence,
                error_code=scenario.error_code.value,
                close_code=int(scenario.require_ws_close_code()),
            )
            return OrchestratorResult(
                response=scenario.build_response(
                    cid,
                    details=scenario.format_details(sequence_number=seq),
                ),
                disconnect=True,
                close_code=scenario.require_ws_close_code(),
                timings_ms=self._finalize_timings(timings, process_started_at),
            )

        # ------------------------------------------------------------------
        # 3. Persistence — send to Kafka (Scenarios A / E / G)
        # ------------------------------------------------------------------
        assert isinstance(raw_json, dict)
        kafka_payload = self._message_converter.to_kafka_payload(msg, raw_json)
        kafka_send_started_at = time.perf_counter()
        try:
            await self._producer.send(cid, kafka_payload)
        except asyncio.TimeoutError:
            timings["kafka_send_ms"] = self._elapsed_ms(kafka_send_started_at)
            # Scenario E: Kafka timeout -> E1011 + no commit + disconnect 1013
            scenario = ProtocolErrorScenario.DOWNSTREAM_TIMEOUT
            log.error(
                scenario.default_log_reason,
                conversation_id=cid,
                seq=seq,
            )
            return OrchestratorResult(
                response=scenario.build_response(cid),
                disconnect=True,
                close_code=scenario.require_ws_close_code(),
                timings_ms=self._finalize_timings(timings, process_started_at),
            )
        except Exception as e:
            timings["kafka_send_ms"] = self._elapsed_ms(kafka_send_started_at)
            # Scenario E: Kafka failure -> E1008 + no commit + disconnect 1013
            scenario = ProtocolErrorScenario.DOWNSTREAM_UNAVAILABLE
            log.error(
                "Orchestrator: Kafka send failed",
                conversation_id=cid,
                seq=seq,
                error=str(e),
            )
            return OrchestratorResult(
                response=scenario.build_response(
                    cid,
                    details=str(e)[:MAX_ERROR_MESSAGE_LEN],
                ),
                disconnect=True,
                close_code=scenario.require_ws_close_code(),
                timings_ms=self._finalize_timings(timings, process_started_at),
            )
        timings["kafka_send_ms"] = self._elapsed_ms(kafka_send_started_at)

        # ------------------------------------------------------------------
        # 4. Redis sequence commit — advance expected seq in Redis (after Kafka OK)
        # ------------------------------------------------------------------
        commit_started_at = time.perf_counter()
        await self._sm.commit(cid, seq)
        timings["redis_commit_ms"] = self._elapsed_ms(commit_started_at)

        # ------------------------------------------------------------------
        # 5. SESSION_COMPLETE -> cleanup + proactive disconnect 1000 (Scenario G)
        # ------------------------------------------------------------------
        if event_type == EventType.SESSION_COMPLETE:
            cleanup_started_at = time.perf_counter()
            try:
                await self._sm.cleanup(cid)
            except Exception as e:
                timings["cleanup_ms"] = self._elapsed_ms(cleanup_started_at)
                # Kafka send and commit have already completed. cleanup only shortens TTL,
                # so a cleanup failure must not flip the successful completion semantics into E1007.
                log.warning(
                    "Orchestrator: SESSION_COMPLETE cleanup failed, downgrading to ACK",
                    conversation_id=cid,
                    seq=seq,
                    error=str(e),
                )
            else:
                timings["cleanup_ms"] = self._elapsed_ms(cleanup_started_at)
            log.info(
                "Orchestrator: SESSION_COMPLETE completed",
                conversation_id=cid,
                seq=seq,
            )
            ack_started_at = time.perf_counter()
            ack = self._build_success_ack(cid, seq, event_type)
            timings["ack_build_ms"] = self._elapsed_ms(ack_started_at)
            return OrchestratorResult(
                response=ack,
                disconnect=True,
                close_code=WsCloseCode.NORMAL,
                timings_ms=self._finalize_timings(timings, process_started_at),
            )

        # ------------------------------------------------------------------
        # Scenario A: successful SESSION_ONGOING -> ACK without disconnect
        # ------------------------------------------------------------------
        ack_started_at = time.perf_counter()
        ack = self._build_success_ack(cid, seq, event_type)
        timings["ack_build_ms"] = self._elapsed_ms(ack_started_at)
        return OrchestratorResult(
            response=ack,
            disconnect=False,
            timings_ms=self._finalize_timings(timings, process_started_at),
        )

    @staticmethod
    def _classify_validation_error(e: ValidationError) -> ProtocolErrorScenario:
        """Map a Pydantic ``ValidationError`` to the protocol error scenario."""
        for err in e.errors():
            err_type = err.get("type", "")
            if "datetime" in err_type or "time" in err_type or "iso" in err_type:
                return ProtocolErrorScenario.INVALID_TIMESTAMP_FORMAT
            if "json" in err_type:
                return ProtocolErrorScenario.INVALID_JSON
            if "missing" in err_type:
                return ProtocolErrorScenario.MISSING_REQUIRED_FIELD
            if "enum" in err_type or "literal" in err_type:
                return ProtocolErrorScenario.INVALID_ENUM_VALUE
            if err_type == "extra_forbidden":
                return ProtocolErrorScenario.INVALID_FIELD_TYPE
            if "type" in err_type or "int" in err_type or "bool" in err_type:
                return ProtocolErrorScenario.INVALID_FIELD_TYPE
            if err_type == "value_error":
                return ProtocolErrorScenario.BUSINESS_RULE_VIOLATION
        return ProtocolErrorScenario.MISSING_REQUIRED_FIELD

