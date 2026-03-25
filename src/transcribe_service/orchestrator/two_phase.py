"""两阶段提交编排器 — 调用 state_machine + producer，覆盖 7 种场景。

架构红线：只依赖 base 抽象接口，禁止 import 任何 impl 类。
"""

from __future__ import annotations

import asyncio
import time

import structlog
from pydantic import ValidationError

from transcribe_service.constants import MAX_ERROR_DETAILS_LEN, MAX_ERROR_MESSAGE_LEN
from transcribe_service.orchestrator.protocols import OrchestratorResult
from transcribe_service.producer.protocols import ProducerBackend
from transcribe_service.redis.protocols import PrepareResult, SequenceStateMachineBackend
from transcribe_service.schemas.errors import ErrorCode, WsCloseCode
from transcribe_service.schemas.events import EventType
from transcribe_service.schemas.request import InboundMessage
from transcribe_service.schemas.response import build_eol_ack, build_error, build_transcript_ack

log = structlog.get_logger(__name__)


class TwoPhaseOrchestrator:
    """两阶段提交编排器。

    场景 A-G 严格按 plan §2.5 执行，每种场景的返回帧、Kafka/Redis 动作、
    是否断连、Close Code 均已明确定义。
    """

    def __init__(
        self,
        state_machine: SequenceStateMachineBackend,
        producer: ProducerBackend,
    ) -> None:
        self._sm = state_machine
        self._producer = producer

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

    async def handle_message(self, raw_json: dict) -> OrchestratorResult:
        """处理一条上行消息。"""
        conversation_id = ""
        started_at = time.perf_counter()
        try:
            raw_conversation_id = (raw_json.get("metaData") or {}).get("conversationId", "")
            if isinstance(raw_conversation_id, str):
                conversation_id = raw_conversation_id
            return await self._process(raw_json, conversation_id)
        except Exception as exc:
            # 场景 F: 未捕获异常 → E1007 + 断连 1011
            log.exception(
                "Orchestrator: 未捕获异常",
                conversation_id=conversation_id,
                error=str(exc),
            )
            return OrchestratorResult(
                response=build_error(
                    conversation_id=conversation_id,
                    code=ErrorCode.E1007.value,
                    message="Internal server error",
                    details=str(exc)[:MAX_ERROR_MESSAGE_LEN],
                ),
                disconnect=True,
                close_code=WsCloseCode.INTERNAL_ERROR,
                timings_ms={"orchestrator_ms": self._elapsed_ms(started_at)},
            )

    async def _process(self, raw_json: dict, conversation_id: str) -> OrchestratorResult:
        """内部处理逻辑，按场景分支。"""
        process_started_at = time.perf_counter()
        timings: dict[str, float] = {}

        # ------------------------------------------------------------------
        # 1. Schema 校验 (场景 D)
        # ------------------------------------------------------------------
        validate_started_at = time.perf_counter()
        try:
            msg = InboundMessage.model_validate(raw_json)
        except ValidationError as e:
            timings["validate_ms"] = self._elapsed_ms(validate_started_at)
            error_code, close_code = self._classify_validation_error(e)
            log.warning(
                "Orchestrator: Schema 校验失败",
                conversation_id=conversation_id,
                error_code=error_code.value,
                errors=str(e),
            )
            return OrchestratorResult(
                response=build_error(
                    conversation_id=conversation_id,
                    code=error_code.value,
                    message="Validation failed",
                    details=str(e)[:MAX_ERROR_DETAILS_LEN],
                ),
                disconnect=True,
                close_code=close_code,
                timings_ms=self._finalize_timings(timings, process_started_at),
            )
        timings["validate_ms"] = self._elapsed_ms(validate_started_at)

        cid = msg.metaData.conversationId
        seq = msg.payload.sequenceNumber
        event_type = msg.metaData.eventType

        # ------------------------------------------------------------------
        # 2. Prepare — Lua 原子预检
        # ------------------------------------------------------------------
        prepare_started_at = time.perf_counter()
        result = await self._sm.prepare(cid, seq)
        timings["prepare_ms"] = self._elapsed_ms(prepare_started_at)

        # 场景 B: IDEMPOTENT → 直接 ACK，不写 Kafka，不推进 Redis
        if result == PrepareResult.IDEMPOTENT:
            should_disconnect = self._disconnect_after_success(event_type)
            log.info(
                "Orchestrator: 幂等命中，直接 ACK",
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

        # 场景 C: OUT_OF_ORDER → E1006 + 断连 1008
        if result == PrepareResult.OUT_OF_ORDER:
            log.warning(
                "Orchestrator: 序列号乱序",
                conversation_id=cid,
                seq=seq,
            )
            return OrchestratorResult(
                response=build_error(
                    conversation_id=cid,
                    code=ErrorCode.E1006.value,
                    message="Sequence number out of order",
                    details=f"sequenceNumber={seq} is not expected",
                ),
                disconnect=True,
                close_code=WsCloseCode.POLICY_VIOLATION,
                timings_ms=self._finalize_timings(timings, process_started_at),
            )

        # ------------------------------------------------------------------
        # 3. Persistence — 写入 Kafka (场景 A / E / G)
        # ------------------------------------------------------------------
        kafka_payload = raw_json
        kafka_send_started_at = time.perf_counter()
        try:
            await self._producer.send(cid, kafka_payload)
        except asyncio.TimeoutError:
            timings["kafka_send_ms"] = self._elapsed_ms(kafka_send_started_at)
            # 场景 E: Kafka 超时 → E1011 + 不 commit + 断连 1013
            log.error(
                "Orchestrator: Kafka 超时",
                conversation_id=cid,
                seq=seq,
            )
            return OrchestratorResult(
                response=build_error(
                    conversation_id=cid,
                    code=ErrorCode.E1011.value,
                    message="Downstream timeout",
                    details="Kafka send timed out",
                ),
                disconnect=True,
                close_code=WsCloseCode.TRY_AGAIN_LATER,
                timings_ms=self._finalize_timings(timings, process_started_at),
            )
        except Exception as e:
            timings["kafka_send_ms"] = self._elapsed_ms(kafka_send_started_at)
            # 场景 E: Kafka 失败 → E1008 + 不 commit + 断连 1013
            log.error(
                "Orchestrator: Kafka 失败",
                conversation_id=cid,
                seq=seq,
                error=str(e),
            )
            return OrchestratorResult(
                response=build_error(
                    conversation_id=cid,
                    code=ErrorCode.E1008.value,
                    message="Downstream unavailable",
                    details=str(e)[:MAX_ERROR_MESSAGE_LEN],
                ),
                disconnect=True,
                close_code=WsCloseCode.TRY_AGAIN_LATER,
                timings_ms=self._finalize_timings(timings, process_started_at),
            )
        timings["kafka_send_ms"] = self._elapsed_ms(kafka_send_started_at)

        # ------------------------------------------------------------------
        # 4. Commit — 推进 Redis 状态
        # ------------------------------------------------------------------
        commit_started_at = time.perf_counter()
        await self._sm.commit(cid, seq)
        timings["commit_ms"] = self._elapsed_ms(commit_started_at)

        # ------------------------------------------------------------------
        # 5. SESSION_COMPLETE → cleanup + 主动断连 1000 (场景 G)
        # ------------------------------------------------------------------
        if event_type == EventType.SESSION_COMPLETE:
            cleanup_started_at = time.perf_counter()
            try:
                await self._sm.cleanup(cid)
            except Exception as e:
                timings["cleanup_ms"] = self._elapsed_ms(cleanup_started_at)
                # Kafka 与 commit 已完成；cleanup 仅用于缩短 TTL，失败时不应把整次完成语义翻转为 E1007
                log.warning(
                    "Orchestrator: SESSION_COMPLETE cleanup 失败，降级为 ACK",
                    conversation_id=cid,
                    seq=seq,
                    error=str(e),
                )
            else:
                timings["cleanup_ms"] = self._elapsed_ms(cleanup_started_at)
            log.info(
                "Orchestrator: SESSION_COMPLETE 处理完成",
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
        # 场景 A: 正常 SESSION_ONGOING → ACK，不断连
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
    def _classify_validation_error(e: ValidationError) -> tuple[ErrorCode, WsCloseCode]:
        """将 Pydantic ValidationError 映射为 (应用错误码, WS Close Code)。"""
        for err in e.errors():
            err_type = err.get("type", "")
            if "datetime" in err_type or "time" in err_type or "iso" in err_type:
                return ErrorCode.E1005, WsCloseCode.POLICY_VIOLATION
            if "json" in err_type:
                return ErrorCode.E1001, WsCloseCode.INVALID_PAYLOAD
            if "missing" in err_type:
                return ErrorCode.E1003, WsCloseCode.POLICY_VIOLATION
            if "enum" in err_type or "literal" in err_type:
                return ErrorCode.E1002, WsCloseCode.POLICY_VIOLATION
            if "type" in err_type or "int" in err_type or "bool" in err_type:
                return ErrorCode.E1004, WsCloseCode.POLICY_VIOLATION
            if err_type == "value_error":
                return ErrorCode.E1009, WsCloseCode.POLICY_VIOLATION
        return ErrorCode.E1003, WsCloseCode.POLICY_VIOLATION
