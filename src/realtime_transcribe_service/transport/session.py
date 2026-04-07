"""WebSocket session execution, refresh, and close-code mapping."""

from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import suppress
from typing import TYPE_CHECKING

import orjson
from fastapi import Query, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from realtime_transcribe_service.config.logging_config import get_logger
from realtime_transcribe_service.constants import (
    DEFAULT_WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC,
    MAX_ERROR_DETAILS_LEN,
)
from realtime_transcribe_service.schemas.error_codes import WsCloseCode  # noqa: F401
from realtime_transcribe_service.schemas.events import ResponseEventType
from realtime_transcribe_service.schemas.error_scenarios import ProtocolErrorScenario
from realtime_transcribe_service.transport.metrics import RuntimeMetrics

if TYPE_CHECKING:  # pragma: no cover
    from realtime_transcribe_service.orchestrator.protocols import OrchestratorBackend
    from realtime_transcribe_service.redis.protocols import ConversationOwnershipGuardBackend
    from realtime_transcribe_service.transport.registry import ConnectionRegistry

log = get_logger(__name__)
OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC = DEFAULT_WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC
SLOW_MESSAGE_LOG_WINDOW_SEC = 1.0
SLOW_MESSAGE_LOG_MAX_PER_WINDOW = 1
SCOPE_OWNERSHIP_TOKEN = "realtime_transcribe_service.ownership_token"
SCOPE_OWNERSHIP_ACQUIRED = "realtime_transcribe_service.ownership_acquired"
SCOPE_AUTH_SUBJECT = "realtime_transcribe_service.auth_subject"
_slow_message_log_window_started_at = 0.0
_slow_message_log_emitted_in_window = 0
_slow_message_log_suppressed = 0


def _elapsed_ms(started_at: float) -> float:
    return round((time.perf_counter() - started_at) * 1000, 2)


_ORCHESTRATOR_LEAF_TIMING_KEYS = (
    "validate_ms",
    "prepare_ms",
    "kafka_send_ms",
    "redis_commit_ms",
    "ack_build_ms",
)


def _orchestrator_bottleneck(timings_ms: dict[str, float] | None) -> tuple[dict[str, object], str] | None:
    """Pick the slowest orchestrator leaf phase and a short hint string for logs."""
    if not timings_ms:
        return None
    leaves = {k: timings_ms[k] for k in _ORCHESTRATOR_LEAF_TIMING_KEYS if k in timings_ms}
    if not leaves:
        return None
    stage, ms = max(leaves.items(), key=lambda kv: kv[1])
    orch = timings_ms.get("orchestrator_ms")
    if orch and orch > 0:
        pct = round(100.0 * ms / orch, 1)
        hint = f"{stage}={ms:.2f}ms (~{pct}% of orchestrator_ms)"
    else:
        total = sum(leaves.values())
        pct = round(100.0 * ms / total, 1) if total > 0 else 0.0
        hint = f"{stage}={ms:.2f}ms (~{pct}% of summed leaf phases)"
    bottleneck: dict[str, object] = {
        "stage": stage,
        "ms": round(ms, 2),
        "pct": pct,
    }
    return bottleneck, hint


def _maybe_log_slow_message(
    *,
    threshold_ms: float,
    started_at: float,
    conversation_id: str,
    raw_json: object | None,
    response: dict | None,
    disconnect: bool,
    close_code: int | None = None,
    decode_ms: float | None = None,
    send_ms: float | None = None,
    server_processing_ms: float | None = None,
    timings_ms: dict[str, float] | None = None,
) -> None:
    global _slow_message_log_window_started_at
    global _slow_message_log_emitted_in_window
    global _slow_message_log_suppressed

    if threshold_ms <= 0:
        return

    total_ms = _elapsed_ms(started_at)
    if total_ms < threshold_ms:
        return

    now = time.perf_counter()
    if (
        _slow_message_log_window_started_at == 0.0
        or now - _slow_message_log_window_started_at >= SLOW_MESSAGE_LOG_WINDOW_SEC
    ):
        suppressed_since_last_emit = _slow_message_log_suppressed
        _slow_message_log_window_started_at = now
        _slow_message_log_emitted_in_window = 0
        _slow_message_log_suppressed = 0
    else:
        suppressed_since_last_emit = 0

    if _slow_message_log_emitted_in_window >= SLOW_MESSAGE_LOG_MAX_PER_WINDOW:
        _slow_message_log_suppressed += 1
        return

    request_meta = raw_json.get("metaData") if isinstance(raw_json, dict) else None
    request_payload = raw_json.get("payload") if isinstance(raw_json, dict) else None
    response_meta = response.get("metaData") if isinstance(response, dict) else None
    response_error = response.get("error") if isinstance(response, dict) else None

    flow = {
        "request_event_type": request_meta.get("eventType")
        if isinstance(request_meta, dict)
        else None,
        "response_event_type": response_meta.get("eventType")
        if isinstance(response_meta, dict)
        else None,
        "sequence_number": request_payload.get("sequenceNumber")
        if isinstance(request_payload, dict)
        else None,
        "speaker": request_payload.get("speaker")
        if isinstance(request_payload, dict)
        else None,
    }
    outcome = {
        "disconnect": disconnect,
        "close_code": close_code,
        "error_code": response_error.get("code")
        if isinstance(response_error, dict)
        else None,
    }
    stage_timings: dict[str, float] = {}
    if decode_ms is not None:
        stage_timings["decode_ms"] = decode_ms
    if server_processing_ms is not None:
        stage_timings["server_processing_ms"] = server_processing_ms
    if send_ms is not None:
        stage_timings["send_ms"] = send_ms
    if timings_ms:
        stage_timings.update(timings_ms)

    flow = {key: value for key, value in flow.items() if value is not None}
    outcome = {key: value for key, value in outcome.items() if value is not None}

    bottleneck_info = _orchestrator_bottleneck(timings_ms)
    bottleneck: dict[str, object] | None = None
    bottleneck_hint: str | None = None
    if bottleneck_info is not None:
        bottleneck, bottleneck_hint = bottleneck_info

    log.warning(
        "Transport: Slow message stage timings",
        conversation_id=conversation_id,
        threshold_ms=threshold_ms,
        total_ms=total_ms,
        suppressed_since_last_emit=suppressed_since_last_emit,
        flow=flow,
        outcome=outcome,
        timings_ms=stage_timings,
        bottleneck=bottleneck,
        bottleneck_hint=bottleneck_hint,
    )
    _slow_message_log_emitted_in_window += 1


async def _release_ownership_guard(
    ownership_guard: ConversationOwnershipGuardBackend | None,
    conversation_id: str,
    ownership_token: str,
    *,
    acquired: bool,
) -> None:
    if ownership_guard is None or not acquired:
        return
    try:
        await ownership_guard.release(conversation_id, ownership_token)
    except Exception as exc:
        log.warning(
            "Transport: Failed to release conversation ownership guard",
            conversation_id=conversation_id,
            error=str(exc),
        )


async def _claim_runtime_ownership_or_close(
    ws: WebSocket,
    conversation_id: str,
    ownership_guard: ConversationOwnershipGuardBackend,
    ownership_token: str,
    *,
    log_ws_error_frames: bool,
) -> bool:
    try:
        owned = await ownership_guard.claim_or_refresh(conversation_id, ownership_token)
    except Exception as exc:
        log.error(
            "Transport: Failed to acquire conversation ownership guard",
            conversation_id=conversation_id,
            error=str(exc),
        )
        await _close_for_ownership_guard_unavailable(
            ws,
            conversation_id,
            log_ws_error_frames=log_ws_error_frames,
        )
        return False

    if owned:
        return True

    log.warning(
        "Transport: Conversation already has an active sender, rejecting new connection",
        conversation_id=conversation_id,
    )
    await _close_for_active_sender_conflict(
        ws,
        conversation_id,
        log_ws_error_frames=log_ws_error_frames,
    )
    return False


def build_ws_endpoint(
    *,
    orchestrator: OrchestratorBackend,
    registry: ConnectionRegistry,
    ownership_guard: ConversationOwnershipGuardBackend | None,
    runtime_metrics: RuntimeMetrics,
    ownership_guard_refresh_interval_sec: float = OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC,
    log_ws_error_frames: bool = False,
    log_slow_message_threshold_ms: float = 0.0,
):
    """Build the WebSocket endpoint bound to the current runtime dependencies."""

    async def ws_endpoint(
        ws: WebSocket,
        conversationId: str = Query("", max_length=64),
    ) -> None:
        scope_token = ws.scope.get(SCOPE_OWNERSHIP_TOKEN)
        ownership_token = scope_token if isinstance(scope_token, str) else uuid.uuid4().hex
        ownership_acquired = bool(ws.scope.get(SCOPE_OWNERSHIP_ACQUIRED))
        auth_subject = ws.scope.get(SCOPE_AUTH_SUBJECT)
        auth_subject_text = auth_subject if isinstance(auth_subject, str) else None
        ownership_refresh_task: asyncio.Task[None] | None = None
        owner = ownership_guard

        log.info(
            "Transport: About to accept WebSocket",
            conversation_id=conversationId,
            auth_subject=auth_subject_text,
        )
        try:
            await ws.accept()
        except Exception as exc:  # pragma: no cover - hard to deterministically trigger in TestClient
            log.warning(
                "Transport: WebSocket accept failed",
                conversation_id=conversationId,
                exc_type=type(exc).__name__,
                error=str(exc),
            )
            await _release_ownership_guard(
                owner,
                conversationId,
                ownership_token,
                acquired=ownership_acquired,
            )
            return

        if owner is not None and not ownership_acquired:
            ownership_acquired = await _claim_runtime_ownership_or_close(
                ws,
                conversationId,
                owner,
                ownership_token,
                log_ws_error_frames=log_ws_error_frames,
            )
            if not ownership_acquired:
                return

        registry.add(conversationId, ws)
        log.info(
            "Transport: Connection established",
            conversation_id=conversationId,
            auth_subject=auth_subject_text,
        )

        if owner is not None and ownership_acquired:
            ownership_refresh_task = asyncio.create_task(
                _ownership_refresh_loop(
                    ws,
                    conversationId,
                    ownership_guard=owner,
                    ownership_token=ownership_token,
                    refresh_interval_sec=ownership_guard_refresh_interval_sec,
                    runtime_metrics=runtime_metrics,
                    log_ws_error_frames=log_ws_error_frames,
                )
            )

        try:
            await _run_ws_message_loop(
                ws,
                orchestrator,
                conversationId,
                runtime_metrics=runtime_metrics,
                log_ws_error_frames=log_ws_error_frames,
                log_slow_message_threshold_ms=log_slow_message_threshold_ms,
            )
        except WebSocketDisconnect:
            log.info(
                "Transport: Client disconnected",
                conversation_id=conversationId,
            )
        except Exception as exc:
            log.exception(
                "Transport: Connection error",
                conversation_id=conversationId,
                error=str(exc),
            )
            await _send_error_and_close(
                ws,
                conversationId,
                ProtocolErrorScenario.TRANSPORT_INTERNAL_EXCEPTION,
                log_ws_error_frames=log_ws_error_frames,
            )
        finally:
            registry.remove(conversationId, ws)
            if ownership_refresh_task is not None:
                ownership_refresh_task.cancel()
                with suppress(asyncio.CancelledError):
                    await ownership_refresh_task
            await _release_ownership_guard(
                owner,
                conversationId,
                ownership_token,
                acquired=ownership_acquired,
            )

    return ws_endpoint


async def _run_ws_message_loop(
    ws: WebSocket,
    orchestrator: OrchestratorBackend,
    conversation_id: str,
    *,
    runtime_metrics: RuntimeMetrics | None = None,
    log_ws_error_frames: bool = False,
    log_slow_message_threshold_ms: float = 0.0,
) -> None:
    """Run the accepted WebSocket session loop: receive, validate, handle, respond, and close."""
    while True:
        raw_text = await ws.receive_text()
        t0 = time.perf_counter()
        decode_ms: float | None = None

        decode_started_at = time.perf_counter()
        try:
            raw_json = orjson.loads(raw_text)
        except (orjson.JSONDecodeError, ValueError) as e:
            decode_ms = _elapsed_ms(decode_started_at)
            scenario = ProtocolErrorScenario.INVALID_JSON
            details = str(e)[:MAX_ERROR_DETAILS_LEN]
            log.warning(
                scenario.default_log_reason,
                conversation_id=conversation_id,
                error=str(e),
                error_code=scenario.error_code.value,
                close_code=int(scenario.require_ws_close_code()),
            )
            error_response = scenario.build_response(conversation_id, details=details)
            await _send_error_and_close(
                ws,
                conversation_id,
                scenario,
                details=details,
                log_ws_error_frames=log_ws_error_frames,
            )
            _maybe_log_slow_message(
                threshold_ms=log_slow_message_threshold_ms,
                started_at=t0,
                conversation_id=conversation_id,
                raw_json=None,
                response=error_response,
                disconnect=True,
                close_code=scenario.require_ws_close_code(),
                decode_ms=decode_ms,
            )
            return
        decode_ms = _elapsed_ms(decode_started_at)

        if isinstance(raw_json, dict):
            meta = raw_json.get("metaData")
            if isinstance(meta, dict):
                body_cid = meta.get("conversationId")
                if isinstance(body_cid, str) and body_cid != conversation_id:
                    scenario = ProtocolErrorScenario.CONVERSATION_ID_MISMATCH
                    details = scenario.format_details(
                        expected_conversation_id=conversation_id,
                    )
                    error_response = scenario.build_response(conversation_id, details=details)
                    log.warning(
                        scenario.default_log_reason,
                        conversation_id=conversation_id,
                        handshake_conversation_id=conversation_id,
                        metadata_conversation_id=body_cid,
                        error_code=scenario.error_code.value,
                        close_code=int(scenario.require_ws_close_code()),
                    )
                    await _send_error_and_close(
                        ws,
                        conversation_id,
                        scenario,
                        details=details,
                        log_ws_error_frames=log_ws_error_frames,
                    )
                    _maybe_log_slow_message(
                        threshold_ms=log_slow_message_threshold_ms,
                        started_at=t0,
                        conversation_id=conversation_id,
                        raw_json=raw_json,
                        response=error_response,
                        disconnect=True,
                        close_code=scenario.require_ws_close_code(),
                        decode_ms=decode_ms,
                    )
                    return

        result = await orchestrator.handle_message(raw_json, conversation_id)
        if runtime_metrics is not None:
            runtime_metrics.observe_orchestrator_timings(result.timings_ms)
        server_processing_ms = _elapsed_ms(t0)
        resp = result.response
        if (
            isinstance(resp, dict)
            and (resp.get("metaData") or {}).get("eventType")
            in {
                ResponseEventType.TRANSCRIPT_ACK.value,
                ResponseEventType.EOL_ACK.value,
            }
            and isinstance(resp.get("payload"), dict)
        ):
            resp["payload"]["serverProcessingMs"] = server_processing_ms

        send_ms: float | None = None
        if ws.client_state == WebSocketState.CONNECTED:
            if (
                log_ws_error_frames
                and isinstance(resp, dict)
                and (resp.get("metaData") or {}).get("eventType")
                == ResponseEventType.ERROR.value
            ):
                log.info(
                    "Transport: Sent ERROR response frame",
                    conversation_id=conversation_id,
                    response=resp,
                )
            send_started_at = time.perf_counter()
            await ws.send_text(orjson.dumps(resp).decode("utf-8"))
            send_ms = _elapsed_ms(send_started_at)

        _maybe_log_slow_message(
            threshold_ms=log_slow_message_threshold_ms,
            started_at=t0,
            conversation_id=conversation_id,
            raw_json=raw_json,
            response=resp if isinstance(resp, dict) else None,
            disconnect=result.disconnect,
            close_code=int(result.close_code) if result.disconnect else None,
            decode_ms=decode_ms,
            send_ms=send_ms,
            server_processing_ms=server_processing_ms,
            timings_ms=result.timings_ms,
        )

        if result.disconnect:
            if ws.client_state == WebSocketState.CONNECTED:
                code = int(result.close_code)
                log.info(
                    "Transport: Server closing connection",
                    conversation_id=conversation_id,
                    close_code=code,
                )
                await ws.close(code=code)
            return


async def _ownership_refresh_loop(
    ws: WebSocket,
    conversation_id: str,
    *,
    ownership_guard: ConversationOwnershipGuardBackend,
    ownership_token: str,
    refresh_interval_sec: float = OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC,
    runtime_metrics: RuntimeMetrics | None = None,
    log_ws_error_frames: bool = False,
) -> None:
    """Refresh conversation ownership in the background to keep Redis off the hot path."""
    refresh_interval = max(0.1, refresh_interval_sec)
    while True:
        await asyncio.sleep(refresh_interval)
        try:
            if runtime_metrics is not None:
                runtime_metrics.redis_ownership_refresh_total += 1
            owned = await ownership_guard.claim_or_refresh(conversation_id, ownership_token)
        except Exception as exc:
            if runtime_metrics is not None:
                runtime_metrics.redis_ownership_refresh_failures_total += 1
            log.error(
                "Transport: Conversation ownership guard store unavailable",
                conversation_id=conversation_id,
                error=str(exc),
            )
            await _close_for_ownership_guard_unavailable(
                ws,
                conversation_id,
                log_ws_error_frames=log_ws_error_frames,
            )
            return
        if not owned:
            if runtime_metrics is not None:
                runtime_metrics.redis_ownership_refresh_conflicts_total += 1
            log.warning(
                "Transport: Concurrent sender conflict",
                conversation_id=conversation_id,
            )
            await _close_for_active_sender_conflict(
                ws,
                conversation_id,
                log_ws_error_frames=log_ws_error_frames,
            )
            return


async def _close_for_ownership_guard_unavailable(
    ws: WebSocket,
    conversation_id: str,
    *,
    log_ws_error_frames: bool = False,
) -> None:
    await _send_error_and_close(
        ws,
        conversation_id,
        ProtocolErrorScenario.DOWNSTREAM_UNAVAILABLE,
        details="Conversation ownership guard store unavailable",
        log_ws_error_frames=log_ws_error_frames,
    )


async def _close_for_active_sender_conflict(
    ws: WebSocket,
    conversation_id: str,
    *,
    log_ws_error_frames: bool = False,
) -> None:
    await _send_error_and_close(
        ws,
        conversation_id,
        ProtocolErrorScenario.ACTIVE_SENDER_CONFLICT,
        log_ws_error_frames=log_ws_error_frames,
    )


async def _send_error_and_close(
    ws: WebSocket,
    conversation_id: str,
    scenario: ProtocolErrorScenario,
    *,
    details: str | None = None,
    log_ws_error_frames: bool = False,
) -> None:
    """Send an ERROR frame and then close the connection."""
    try:
        if ws.client_state == WebSocketState.CONNECTED:
            close_code = scenario.require_ws_close_code()
            error_payload = scenario.build_response(conversation_id, details=details)
            if log_ws_error_frames:
                log.info(
                    "Transport: Sent ERROR response frame",
                    conversation_id=conversation_id,
                    response=error_payload,
                )
            await ws.send_text(orjson.dumps(error_payload).decode("utf-8"))
            log.info(
                "Transport: Server closed connection after sending error",
                conversation_id=conversation_id,
                error_code=scenario.error_code.value,
                close_code=int(close_code),
            )
            await ws.close(code=close_code)
    except Exception:
        pass
