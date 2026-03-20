"""WebSocket 接入层 — FastAPI 服务端，心跳、JSON 收发、Close Code 映射。

架构红线：只负责协议层面的脏活累活，不感知业务逻辑。
收到 JSON → 抛给 orchestrator → 将结果原封不动返回。
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import orjson
import structlog
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from starlette import status
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.websockets import WebSocketState

from transcribe_service.constants import (
    APP_TITLE,
    EVENT_TRANSCRIPT_ACK,
    MAX_ERROR_DETAILS_LEN,
    WS_CLOSE_REASON_GOING_AWAY,
    WS_PATH,
)
from transcribe_service.schemas.errors import ErrorCode, WsCloseCode
from transcribe_service.schemas.response import build_error

if TYPE_CHECKING:  # pragma: no cover
    from transcribe_service.orchestrator.base import OrchestratorBackend
    from transcribe_service.shutdown.graceful import GracefulShutdown

log = structlog.get_logger(__name__)


class ConnectionRegistry:
    """追踪活跃 WebSocket 连接，支持优雅停机时批量关闭。"""

    def __init__(self) -> None:
        self._connections: dict[str, WebSocket] = {}

    def add(self, conversation_id: str, ws: WebSocket) -> None:
        self._connections[conversation_id] = ws

    def remove(self, conversation_id: str, ws: WebSocket | None = None) -> None:
        """移除登记。若传入 ``ws``，仅当登记对象仍是该实例时才删除，避免重复 conversationId 或
        旧连接 finally 误删新连接。
        """
        if ws is None:
            self._connections.pop(conversation_id, None)
            return
        if self._connections.get(conversation_id) is ws:
            self._connections.pop(conversation_id, None)

    @property
    def active_count(self) -> int:
        return len(self._connections)

    async def close_all(
        self, code: int = WsCloseCode.GOING_AWAY, reason: str = WS_CLOSE_REASON_GOING_AWAY
    ) -> None:
        """优雅停机：向所有存量连接发送 Close 帧。"""
        for cid, ws in list(self._connections.items()):
            try:
                if ws.client_state == WebSocketState.CONNECTED:
                    await ws.close(code=code, reason=reason)
            except Exception:
                pass
            self._connections.pop(cid, None)


class _WsGuardMiddleware:
    """ASGI 中间件：WebSocket 握手前做准入检查。

    检查顺序：conversationId → draining → 连接数上限。
    拒绝时使用 ASGI WebSocket Denial Response 协议返回 JSON ERROR 帧 + HTTP 状态码。
    """

    def __init__(
        self,
        app: ASGIApp,
        shutdown: GracefulShutdown,
        registry: ConnectionRegistry,
        max_connections: int,
    ) -> None:
        self._app = app
        self._shutdown = shutdown
        self._registry = registry
        self._max_connections = max_connections

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
            log.warning("Transport: 缺少 conversationId，拒绝连接 (400)")
            await _deny_websocket(
                receive, send,
                status=status.HTTP_400_BAD_REQUEST,
                error_code=ErrorCode.E1003.value,
                message="Missing required field",
                details="Query parameter 'conversationId' is required",
            )
            return

        if self._shutdown.draining:
            log.warning("Transport: 服务 draining，拒绝新连接 (503)", conversation_id=cid)
            await _deny_websocket(
                receive, send,
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
                error_code=ErrorCode.E1008.value,
                message="Service draining",
                details="Server is shutting down, try again later",
                conversation_id=cid,
            )
            return

        if self._max_connections > 0 and self._registry.active_count >= self._max_connections:
            log.warning(
                "Transport: 连接数已达上限，拒绝新连接 (429)",
                conversation_id=cid,
                active=self._registry.active_count,
                max=self._max_connections,
            )
            await _deny_websocket(
                receive, send,
                status=status.HTTP_429_TOO_MANY_REQUESTS,
                error_code=ErrorCode.E1008.value,
                message="Too many connections",
                details=f"Active {self._registry.active_count} >= limit {self._max_connections}",
                conversation_id=cid,
            )
            return

        await self._app(scope, receive, send)


def create_app(
    orchestrator: OrchestratorBackend,
    shutdown: GracefulShutdown,
    registry: ConnectionRegistry,
    *,
    redis_url: str = "",
    producer: object | None = None,
    max_connections: int = 0,
    log_ws_error_frames: bool = False,
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

    app.add_middleware(
        _WsGuardMiddleware,
        shutdown=shutdown,
        registry=registry,
        max_connections=max_connections,
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
            return
        registry.add(conversationId, ws)
        log.info(
            "Transport: 连接已建立",
            conversation_id=conversationId,
        )

        try:
            await _message_loop(
                ws,
                orchestrator,
                conversationId,
                log_ws_error_frames=log_ws_error_frames,
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

    return app


async def _message_loop(
    ws: WebSocket,
    orchestrator: OrchestratorBackend,
    conversation_id: str,
    *,
    log_ws_error_frames: bool = False,
) -> None:
    """消息循环：接收 JSON → orchestrator 处理 → 发送响应。"""
    while True:
        raw_text = await ws.receive_text()
        t0 = time.perf_counter()

        # JSON 解析
        try:
            raw_json = orjson.loads(raw_text)
        except (orjson.JSONDecodeError, ValueError) as e:
            log.warning(
                "Transport: JSON 解析失败",
                conversation_id=conversation_id,
                error=str(e),
            )
            await _send_error_and_close(
                ws,
                conversation_id,
                ErrorCode.E1001.value,
                "Invalid JSON",
                WsCloseCode.INVALID_PAYLOAD,
                details=str(e)[:MAX_ERROR_DETAILS_LEN],
                log_ws_error_frames=log_ws_error_frames,
            )
            return

        result = await orchestrator.handle_message(raw_json)
        server_processing_ms = round((time.perf_counter() - t0) * 1000, 2)
        resp = result.response
        if (
            isinstance(resp, dict)
            and (resp.get("metaData") or {}).get("eventType") == EVENT_TRANSCRIPT_ACK
            and isinstance(resp.get("payload"), dict)
        ):
            resp["payload"]["serverProcessingMs"] = server_processing_ms

        # 发送响应帧
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
            await ws.send_text(orjson.dumps(resp).decode("utf-8"))

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
    error_code: str,
    message: str,
    details: str | None = None,
    conversation_id: str = "",
) -> None:
    """在 WebSocket 握手前以 HTTP 状态码 + JSON ERROR 帧拒绝连接。"""
    body = orjson.dumps(build_error(conversation_id, error_code, message, details))
    await receive()
    await send({
        "type": "websocket.http.response.start",
        "status": status,
        "headers": [
            (b"content-type", b"application/json"),
            (b"retry-after", b"5"),
        ],
    })
    await send({
        "type": "websocket.http.response.body",
        "body": body,
    })
