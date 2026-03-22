"""两阶段提交编排器 — 调用 state_machine + producer，覆盖 7 种场景。

架构红线：只依赖 base 抽象接口，禁止 import 任何 impl 类。
"""

from __future__ import annotations

import asyncio

import structlog
from pydantic import ValidationError

from transcribe_service.constants import MAX_ERROR_DETAILS_LEN, MAX_ERROR_MESSAGE_LEN
from transcribe_service.orchestrator.base import OrchestratorResult
from transcribe_service.schemas.errors import ErrorCode, WsCloseCode
from transcribe_service.schemas.request import EventType, InboundMessage
from transcribe_service.schemas.response import build_ack, build_error
from transcribe_service.state_machine.base import PrepareResult, StateMachineBackend
from transcribe_service.producer.base import ProducerBackend

log = structlog.get_logger(__name__)


class TwoPhaseOrchestrator:
    """两阶段提交编排器。

    场景 A-G 严格按 plan §2.5 执行，每种场景的返回帧、Kafka/Redis 动作、
    是否断连、Close Code 均已明确定义。
    """

    def __init__(
        self,
        state_machine: StateMachineBackend,
        producer: ProducerBackend,
    ) -> None:
        self._sm = state_machine
        self._producer = producer

    async def handle_message(self, raw_json: dict) -> OrchestratorResult:
        """处理一条上行消息。"""
        conversation_id = ""
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
            )

    async def _process(self, raw_json: dict, conversation_id: str) -> OrchestratorResult:
        """内部处理逻辑，按场景分支。"""

        # ------------------------------------------------------------------
        # 1. Schema 校验 (场景 D)
        # ------------------------------------------------------------------
        try:
            msg = InboundMessage.model_validate(raw_json)
        except ValidationError as e:
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
            )

        cid = msg.metaData.conversationId
        seq = msg.payload.sequenceNumber
        event_type = msg.metaData.eventType

        # ------------------------------------------------------------------
        # 2. Prepare — Lua 原子预检
        # ------------------------------------------------------------------
        result = await self._sm.prepare(cid, seq)

        # 场景 B: IDEMPOTENT → 直接 ACK，不写 Kafka，不推进 Redis
        if result == PrepareResult.IDEMPOTENT:
            log.info(
                "Orchestrator: 幂等命中，直接 ACK",
                conversation_id=cid,
                seq=seq,
            )
            return OrchestratorResult(
                response=build_ack(cid, seq),
                disconnect=False,
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
            )

        # ------------------------------------------------------------------
        # 3. Persistence — 写入 Kafka (场景 A / E / G)
        # ------------------------------------------------------------------
        kafka_payload = raw_json
        try:
            await self._producer.send(cid, kafka_payload)
        except asyncio.TimeoutError:
            # 场景 E: Kafka 超时 → E1012 + 不 commit + 断连 1013
            log.error(
                "Orchestrator: Kafka 超时",
                conversation_id=cid,
                seq=seq,
            )
            return OrchestratorResult(
                response=build_error(
                    conversation_id=cid,
                    code=ErrorCode.E1012.value,
                    message="Downstream timeout",
                    details="Kafka send timed out",
                ),
                disconnect=True,
                close_code=WsCloseCode.TRY_AGAIN_LATER,
            )
        except Exception as e:
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
            )

        # ------------------------------------------------------------------
        # 4. Commit — 推进 Redis 状态
        # ------------------------------------------------------------------
        await self._sm.commit(cid, seq)

        # ------------------------------------------------------------------
        # 5. SESSION_COMPLETE → cleanup + 主动断连 1000 (场景 G)
        # ------------------------------------------------------------------
        if event_type == EventType.SESSION_COMPLETE:
            try:
                await self._sm.cleanup(cid)
            except Exception as e:
                # Kafka 与 commit 已完成；cleanup 仅用于缩短 TTL，失败时不应把整次完成语义翻转为 E1007
                log.warning(
                    "Orchestrator: SESSION_COMPLETE cleanup 失败，降级为 ACK",
                    conversation_id=cid,
                    seq=seq,
                    error=str(e),
                )
            log.info(
                "Orchestrator: SESSION_COMPLETE 处理完成",
                conversation_id=cid,
                seq=seq,
            )
            return OrchestratorResult(
                response=build_ack(cid, seq),
                disconnect=True,
                close_code=WsCloseCode.NORMAL,
            )

        # ------------------------------------------------------------------
        # 场景 A: 正常 SESSION_ONGOING → ACK，不断连
        # ------------------------------------------------------------------
        return OrchestratorResult(
            response=build_ack(cid, seq),
            disconnect=False,
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
