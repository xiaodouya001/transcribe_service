"""WebSocket 接入层 — FastAPI 服务端，心跳、JSON 收发、Close Code 映射。

架构红线：只负责协议层面的脏活累活，不感知业务逻辑。
收到 JSON → 抛给 orchestrator → 将结果原封不动返回。
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import TYPE_CHECKING

import orjson
import structlog
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from starlette import status
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.websockets import WebSocketState

from transcribe_service.constants import (
    APP_TITLE,
    EVENT_EOL_ACK,
    EVENT_TRANSCRIPT_ACK,
    MAX_ERROR_DETAILS_LEN,
    WS_CLOSE_REASON_GOING_AWAY,
    WS_PATH,
)
from transcribe_service.schemas.errors import ErrorCode, WsCloseCode
from transcribe_service.schemas.response import build_error

if TYPE_CHECKING:  # pragma: no cover
    from transcribe_service.orchestrator.protocols import OrchestratorBackend
    from transcribe_service.redis.protocols import ConversationOwnershipGuardBackend
    from transcribe_service.shutdown.graceful import GracefulShutdown

log = structlog.get_logger(__name__)
OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC = 5.0
SCOPE_OWNERSHIP_TOKEN = "transcribe_service.ownership_token"
SCOPE_OWNERSHIP_ACQUIRED = "transcribe_service.ownership_acquired"
SLOW_MESSAGE_LOG_WINDOW_SEC = 1.0
SLOW_MESSAGE_LOG_MAX_PER_WINDOW = 1
_slow_message_log_window_started_at = 0.0
_slow_message_log_emitted_in_window = 0
_slow_message_log_suppressed = 0


def _elapsed_ms(started_at: float) -> float:
    return round((time.perf_counter() - started_at) * 1000, 2)


def _format_client_addr(scope: Scope) -> str:
    client = scope.get("client")
    if not client:
        return ""
    host, port = client
    return f"{host}:{port}"


def _log_handshake_reject(
    scope: Scope,
    *,
    reason: str,
    status_code: int,
    error_response: dict,
    conversation_id: str = "",
    **extra: object,
) -> None:
    log.warning(
        reason,
        path=scope.get("path", ""),
        client_addr=_format_client_addr(scope),
        conversation_id=conversation_id,
        http_status=status_code,
        error_code=((error_response.get("error") or {}).get("code")),
        error_message=((error_response.get("error") or {}).get("message")),
        error_details=((error_response.get("error") or {}).get("details")),
        error_response=error_response,
        **extra,
    )


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

    log.warning(
        "Transport: 慢消息分段耗时",
        conversation_id=conversation_id,
        threshold_ms=threshold_ms,
        total_ms=total_ms,
        suppressed_since_last_emit=suppressed_since_last_emit,
        flow=flow,
        outcome=outcome,
        timings_ms=stage_timings,
    )
    _slow_message_log_emitted_in_window += 1


class ConnectionRegistry:
    """追踪活跃 WebSocket 连接，支持优雅停机时批量关闭。"""

    def __init__(self) -> None:
        self._connections: dict[str, list[WebSocket]] = {}
        self._active_count = 0

    def add(self, conversation_id: str, ws: WebSocket) -> None:
        self._connections.setdefault(conversation_id, []).append(ws)
        self._active_count += 1

    def remove(self, conversation_id: str, ws: WebSocket | None = None) -> None:
        """移除登记。若传入 ``ws``，仅当登记对象仍是该实例时才删除，避免重复 conversationId 或
        旧连接 finally 误删新连接。
        """
        if ws is None:
            sockets = self._connections.pop(conversation_id, None)
            if sockets:
                self._active_count -= len(sockets)
            return
        sockets = self._connections.get(conversation_id)
        if not sockets:
            return
        remaining = [registered_ws for registered_ws in sockets if registered_ws is not ws]
        removed = len(sockets) - len(remaining)
        if removed:
            self._active_count -= removed
        if remaining:
            self._connections[conversation_id] = remaining
        else:
            self._connections.pop(conversation_id, None)

    @property
    def active_count(self) -> int:
        return self._active_count

    async def close_all(
        self, code: int = WsCloseCode.GOING_AWAY, reason: str = WS_CLOSE_REASON_GOING_AWAY
    ) -> None:
        """优雅停机：向所有存量连接发送 Close 帧。"""
        for cid, sockets in list(self._connections.items()):
            for ws in list(sockets):
                try:
                    if ws.client_state == WebSocketState.CONNECTED:
                        await ws.close(code=code, reason=reason)
                except Exception:
                    pass
            self._connections.pop(cid, None)
        self._active_count = 0


class _WsGuardMiddleware:
    """ASGI 中间件：WebSocket 握手前做准入检查。

    检查顺序：conversationId → draining → 连接数上限 → ownership guard。
    拒绝时使用 ASGI WebSocket Denial Response 协议返回 JSON ERROR 帧 + HTTP 状态码。
    """

    def __init__(
        self,
        app: ASGIApp,
        shutdown: GracefulShutdown,
        registry: ConnectionRegistry,
        max_connections: int,
        ownership_guard: ConversationOwnershipGuardBackend | None = None,
    ) -> None:
        self._app = app
        self._shutdown = shutdown
        self._registry = registry
        self._max_connections = max_connections
        self._ownership_guard = ownership_guard

    @staticmethod
    def _extract_conversation_id(scope: Scope) -> str:
        """从 query string 提取 conversationId，未找到返回空字符串。"""
        from urllib.parse import parse_qs

        qs = scope.get("query_string", b"").decode("latin-1")
        params = parse_qs(qs)
        vals = params.get("conversationId")
        return vals[0] if vals else ""

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "websocket":
            await self._app(scope, receive, send)
            return

        path = scope.get("path", "")
        if path != WS_PATH:
            await self._app(scope, receive, send)
            return

        cid = self._extract_conversation_id(scope)

        if not cid:
            error_response = build_error(
                "",
                ErrorCode.E1003.value,
                "Missing required field",
                "Query parameter 'conversationId' is required",
            )
            _log_handshake_reject(
                scope,
                reason="Transport: 缺少 conversationId，拒绝连接",
                status_code=status.HTTP_400_BAD_REQUEST,
                error_response=error_response,
            )
            await _deny_websocket(
                receive,
                send,
                status=status.HTTP_400_BAD_REQUEST,
                error_response=error_response,
            )
            return

        if self._shutdown.draining:
            error_response = build_error(
                cid,
                ErrorCode.E1008.value,
                "Service draining",
                "Server is shutting down, try again later",
            )
            _log_handshake_reject(
                scope,
                reason="Transport: 服务 draining，拒绝新连接",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                error_response=error_response,
                conversation_id=cid,
            )
            await _deny_websocket(
                receive,
                send,
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
                error_response=error_response,
            )
            return

        if self._max_connections > 0 and self._registry.active_count >= self._max_connections:
            error_response = build_error(
                cid,
                ErrorCode.E1008.value,
                "Too many connections",
                f"Active {self._registry.active_count} >= limit {self._max_connections}",
            )
            _log_handshake_reject(
                scope,
                reason="Transport: 连接数已达上限，拒绝新连接",
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                error_response=error_response,
                conversation_id=cid,
                active=self._registry.active_count,
                max=self._max_connections,
            )
            await _deny_websocket(
                receive,
                send,
                status=status.HTTP_429_TOO_MANY_REQUESTS,
                error_response=error_response,
            )
            return

        if self._ownership_guard is not None:
            ownership_token = uuid.uuid4().hex
            try:
                owned = await self._ownership_guard.claim_or_refresh(cid, ownership_token)
            except Exception as exc:
                error_response = build_error(
                    cid,
                    ErrorCode.E1008.value,
                    "Downstream unavailable",
                    "Conversation ownership guard store unavailable",
                )
                _log_handshake_reject(
                    scope,
                    reason="Transport: 会话发送所有权守卫获取失败，拒绝连接",
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    error_response=error_response,
                    conversation_id=cid,
                    error=str(exc),
                )
                await _deny_websocket(
                    receive,
                    send,
                    status=status.HTTP_503_SERVICE_UNAVAILABLE,
                    error_response=error_response,
                )
                return

            if not owned:
                error_response = build_error(
                    cid,
                    ErrorCode.E1009.value,
                    "Only one sender connection is allowed",
                    "another connection is already sending messages for this conversation",
                )
                _log_handshake_reject(
                    scope,
                    reason="Transport: 会话已有连接在发送，握手期拒绝新连接",
                    status_code=status.HTTP_403_FORBIDDEN,
                    error_response=error_response,
                    conversation_id=cid,
                )
                await _deny_websocket(
                    receive,
                    send,
                    status=status.HTTP_403_FORBIDDEN,
                    error_response=error_response,
                )
                return

            scope[SCOPE_OWNERSHIP_TOKEN] = ownership_token
            scope[SCOPE_OWNERSHIP_ACQUIRED] = True

        await self._app(scope, receive, send)


def create_app(
    orchestrator: OrchestratorBackend,
    shutdown: GracefulShutdown,
    registry: ConnectionRegistry,
    *,
    ownership_guard: ConversationOwnershipGuardBackend | None = None,
    redis_url: str = "",
    producer: object | None = None,
    max_connections: int = 0,
    ownership_guard_refresh_interval_sec: float = OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC,
    log_ws_error_frames: bool = False,
    log_slow_message_threshold_ms: float = 0.0,
) -> FastAPI:
    """构建 FastAPI 应用，包含 WebSocket 端点和健康检查。"""
    app = FastAPI(
        title=APP_TITLE,
        description=APP_TITLE,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    app.state.orchestrator = orchestrator
    app.state.shutdown = shutdown
    app.state.registry = registry
    app.state.ownership_guard = ownership_guard
    app.state.log_slow_message_threshold_ms = log_slow_message_threshold_ms

    app.add_middleware(
        _WsGuardMiddleware,
        shutdown=shutdown,
        registry=registry,
        max_connections=max_connections,
        ownership_guard=ownership_guard,
    )

    # ----- Health / Ready / Metrics -----

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/ready")
    async def ready():
        errors: list[str] = []
        if redis_url:
            try:
                from redis.asyncio import Redis

                client = Redis.from_url(redis_url, decode_responses=True)
                await client.ping()
                await client.aclose()
            except Exception as e:
                errors.append(f"redis:{e}")
        if producer and hasattr(producer, "ensure_ready"):
            try:
                await producer.ensure_ready()
            except Exception as e:
                errors.append(f"kafka:{e}")
        if errors:
            from fastapi.responses import JSONResponse

            return JSONResponse(status_code=503, content={"status": "not_ready", "errors": errors})
        return {"status": "ready"}

    @app.get("/metrics")
    async def metrics():
        return {"active_connections": registry.active_count}

    # ----- WebSocket Endpoint -----

    @app.websocket(WS_PATH)
    async def ws_endpoint(
        ws: WebSocket,
        conversationId: str = Query("", max_length=64),
    ):
        """主 WebSocket 端点：FanoLabs 作为客户端连接此服务端。"""
        scope_token = ws.scope.get(SCOPE_OWNERSHIP_TOKEN)
        ownership_token = scope_token if isinstance(scope_token, str) else uuid.uuid4().hex
        ownership_acquired = bool(ws.scope.get(SCOPE_OWNERSHIP_ACQUIRED))
        ownership_refresh_task: asyncio.Task[None] | None = None
        log.info(
            "Transport: WebSocket 即将 accept",
            conversation_id=conversationId,
        )
        try:
            await ws.accept()
        except Exception as exc:  # pragma: no cover - hard to deterministically trigger in TestClient
            log.warning(
                "Transport: WebSocket accept 失败",
                conversation_id=conversationId,
                exc_type=type(exc).__name__,
                error=str(exc),
            )
            if ownership_guard is not None and ownership_acquired:
                try:
                    await ownership_guard.release(conversationId, ownership_token)
                except Exception as release_exc:
                    log.warning(
                        "Transport: 会话发送所有权守卫释放失败",
                        conversation_id=conversationId,
                        error=str(release_exc),
                    )
            return
        if ownership_guard is not None and not ownership_acquired:
            try:
                ownership_acquired = await ownership_guard.claim_or_refresh(
                    conversationId, ownership_token
                )
            except Exception as exc:
                log.error(
                    "Transport: 会话发送所有权守卫获取失败",
                    conversation_id=conversationId,
                    error=str(exc),
                )
                await _send_error_and_close(
                    ws,
                    conversationId,
                    ErrorCode.E1008.value,
                    "Downstream unavailable",
                    WsCloseCode.TRY_AGAIN_LATER,
                    details="Conversation ownership guard store unavailable",
                    log_ws_error_frames=log_ws_error_frames,
                )
                return
            if not ownership_acquired:
                log.warning(
                    "Transport: 会话已有连接在发送，拒绝新连接",
                    conversation_id=conversationId,
                )
                await _send_error_and_close(
                    ws,
                    conversationId,
                    ErrorCode.E1009.value,
                    "Only one sender connection is allowed",
                    WsCloseCode.POLICY_VIOLATION,
                    details="another connection is already sending messages for this conversation",
                    log_ws_error_frames=log_ws_error_frames,
                )
                return
        registry.add(conversationId, ws)
        log.info(
            "Transport: 连接已建立",
            conversation_id=conversationId,
        )
        if ownership_guard is not None and ownership_acquired:
            ownership_refresh_task = asyncio.create_task(
                _ownership_refresh_loop(
                    ws,
                    conversationId,
                    ownership_guard=ownership_guard,
                    ownership_token=ownership_token,
                    refresh_interval_sec=ownership_guard_refresh_interval_sec,
                    log_ws_error_frames=log_ws_error_frames,
                )
            )

        try:
            await _message_loop(
                ws,
                orchestrator,
                conversationId,
                log_ws_error_frames=log_ws_error_frames,
                log_slow_message_threshold_ms=log_slow_message_threshold_ms,
            )
        except WebSocketDisconnect:
            log.info(
                "Transport: 客户端断开",
                conversation_id=conversationId,
            )
        except Exception as exc:
            log.exception(
                "Transport: 连接异常",
                conversation_id=conversationId,
                error=str(exc),
            )
            await _send_error_and_close(
                ws,
                conversationId,
                ErrorCode.E1007.value,
                "Internal server error",
                WsCloseCode.INTERNAL_ERROR,
                log_ws_error_frames=log_ws_error_frames,
            )
        finally:
            registry.remove(conversationId, ws)
            if ownership_refresh_task is not None:
                ownership_refresh_task.cancel()
                try:
                    await ownership_refresh_task
                except asyncio.CancelledError:
                    pass
            if ownership_guard is not None and ownership_acquired:
                try:
                    await ownership_guard.release(conversationId, ownership_token)
                except Exception as exc:
                    log.warning(
                        "Transport: 会话发送所有权守卫释放失败",
                        conversation_id=conversationId,
                        error=str(exc),
                    )

    return app


async def _message_loop(
    ws: WebSocket,
    orchestrator: OrchestratorBackend,
    conversation_id: str,
    *,
    log_ws_error_frames: bool = False,
    log_slow_message_threshold_ms: float = 0.0,
) -> None:
    """消息循环：接收 JSON → orchestrator 处理 → 发送响应。"""
    while True:
        raw_text = await ws.receive_text()
        t0 = time.perf_counter()
        decode_ms: float | None = None

        # JSON 解析
        decode_started_at = time.perf_counter()
        try:
            raw_json = orjson.loads(raw_text)
        except (orjson.JSONDecodeError, ValueError) as e:
            decode_ms = _elapsed_ms(decode_started_at)
            log.warning(
                "Transport: JSON 解析失败",
                conversation_id=conversation_id,
                error=str(e),
            )
            error_response = build_error(
                conversation_id,
                ErrorCode.E1001.value,
                "Invalid JSON",
                str(e)[:MAX_ERROR_DETAILS_LEN],
            )
            await _send_error_and_close(
                ws,
                conversation_id,
                ErrorCode.E1001.value,
                "Invalid JSON",
                WsCloseCode.INVALID_PAYLOAD,
                details=error_response["error"]["details"],
                log_ws_error_frames=log_ws_error_frames,
            )
            _maybe_log_slow_message(
                threshold_ms=log_slow_message_threshold_ms,
                started_at=t0,
                conversation_id=conversation_id,
                raw_json=None,
                response=error_response,
                disconnect=True,
                close_code=WsCloseCode.INVALID_PAYLOAD,
                decode_ms=decode_ms,
            )
            return
        decode_ms = _elapsed_ms(decode_started_at)

        # 握手 query 为会话唯一标识；若 body 显式提供字符串 conversationId，则必须与之一致
        if isinstance(raw_json, dict):
            meta = raw_json.get("metaData")
            if isinstance(meta, dict):
                body_cid = meta.get("conversationId")
                if isinstance(body_cid, str) and body_cid != conversation_id:
                    error_response = build_error(
                        conversation_id,
                        ErrorCode.E1009.value,
                        "conversationId mismatch",
                        (
                            "metaData.conversationId must match query parameter "
                            f"'conversationId' ({conversation_id!r})"
                        ),
                    )
                    log.warning(
                        "Transport: metaData.conversationId 与握手 query 不一致",
                        conversation_id=conversation_id,
                        body_conversation_id=body_cid,
                    )
                    await _send_error_and_close(
                        ws,
                        conversation_id,
                        ErrorCode.E1009.value,
                        "conversationId mismatch",
                        WsCloseCode.POLICY_VIOLATION,
                        details=error_response["error"]["details"],
                        log_ws_error_frames=log_ws_error_frames,
                    )
                    _maybe_log_slow_message(
                        threshold_ms=log_slow_message_threshold_ms,
                        started_at=t0,
                        conversation_id=conversation_id,
                        raw_json=raw_json,
                        response=error_response,
                        disconnect=True,
                        close_code=WsCloseCode.POLICY_VIOLATION,
                        decode_ms=decode_ms,
                    )
                    return

        result = await orchestrator.handle_message(raw_json)
        server_processing_ms = _elapsed_ms(t0)
        resp = result.response
        if (
            isinstance(resp, dict)
            and (resp.get("metaData") or {}).get("eventType")
            in {EVENT_TRANSCRIPT_ACK, EVENT_EOL_ACK}
            and isinstance(resp.get("payload"), dict)
        ):
            resp["payload"]["serverProcessingMs"] = server_processing_ms

        # 发送响应帧
        send_ms: float | None = None
        if ws.client_state == WebSocketState.CONNECTED:
            if (
                log_ws_error_frames
                and isinstance(resp, dict)
                and (resp.get("metaData") or {}).get("eventType") == "ERROR"
            ):
                log.info(
                    "Transport: 发出 ERROR 响应帧",
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

        # 是否断连
        if result.disconnect:
            if ws.client_state == WebSocketState.CONNECTED:
                code = int(result.close_code)
                log.info(
                    "Transport: 服务端主动断开连接",
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
    log_ws_error_frames: bool = False,
) -> None:
    """后台续租会话发送所有权，避免把 Redis 调用放进消息热路径。"""
    refresh_interval = max(0.1, refresh_interval_sec)
    while True:
        await asyncio.sleep(refresh_interval)
        try:
            owned = await ownership_guard.claim_or_refresh(conversation_id, ownership_token)
        except Exception as exc:
            log.error(
                "Transport: 会话发送所有权守卫存储不可用",
                conversation_id=conversation_id,
                error=str(exc),
            )
            await _send_error_and_close(
                ws,
                conversation_id,
                ErrorCode.E1008.value,
                "Downstream unavailable",
                WsCloseCode.TRY_AGAIN_LATER,
                details="Conversation ownership guard store unavailable",
                log_ws_error_frames=log_ws_error_frames,
            )
            return
        if not owned:
            log.warning(
                "Transport: 会话并发发送冲突",
                conversation_id=conversation_id,
            )
            await _send_error_and_close(
                ws,
                conversation_id,
                ErrorCode.E1009.value,
                "Only one sender connection is allowed",
                WsCloseCode.POLICY_VIOLATION,
                details="another connection is already sending messages for this conversation",
                log_ws_error_frames=log_ws_error_frames,
            )
            return


async def _send_error_and_close(
    ws: WebSocket,
    conversation_id: str,
    code: str,
    message: str,
    close_code: int,
    *,
    details: str | None = None,
    log_ws_error_frames: bool = False,
) -> None:
    """发送 ERROR 帧后关闭连接。"""
    try:
        if ws.client_state == WebSocketState.CONNECTED:
            error_payload = build_error(conversation_id, code, message, details)
            if log_ws_error_frames:
                log.info(
                    "Transport: 发出 ERROR 响应帧",
                    conversation_id=conversation_id,
                    response=error_payload,
                )
            await ws.send_text(orjson.dumps(error_payload).decode("utf-8"))
            log.info(
                "Transport: 服务端发送错误后主动断开连接",
                conversation_id=conversation_id,
                error_code=code,
                close_code=int(close_code),
            )
            await ws.close(code=close_code)
    except Exception:
        pass


async def _deny_websocket(
    receive: Receive,
    send: Send,
    *,
    status: int,
    error_response: dict,
) -> None:
    """在 WebSocket 握手前以 HTTP 状态码 + JSON ERROR 帧拒绝连接。"""
    body = orjson.dumps(error_response)
    await receive()
    await send(
        {
            "type": "websocket.http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"retry-after", b"5"),
            ],
        }
    )
    await send(
        {
            "type": "websocket.http.response.body",
            "body": body,
        }
    )
