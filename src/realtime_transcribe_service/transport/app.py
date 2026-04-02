"""FastAPI app factory, probes, and handshake admission for the transport layer."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any, cast

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from realtime_transcribe_service.auth.protocols import AuthenticationError
from realtime_transcribe_service.config.logging_config import get_logger
from realtime_transcribe_service.config.settings import normalize_url_path_prefix_str
from realtime_transcribe_service.constants import (
    APP_TITLE,
    DEFAULT_WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC,
    WS_PATH,
)
from realtime_transcribe_service.schemas.error_scenarios import ProtocolErrorScenario
from realtime_transcribe_service.transport.metrics import RuntimeMetrics
from realtime_transcribe_service.transport.registry import ConnectionRegistry
from realtime_transcribe_service.transport.session import (
    OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC,
    SCOPE_AUTH_SUBJECT,
    SCOPE_OWNERSHIP_ACQUIRED,
    SCOPE_OWNERSHIP_TOKEN,
    build_ws_endpoint,
)

if TYPE_CHECKING:  # pragma: no cover
    from realtime_transcribe_service.auth.protocols import HandshakeAuthBackend
    from realtime_transcribe_service.orchestrator.protocols import OrchestratorBackend
    from realtime_transcribe_service.producer.protocols import ProducerBackend
    from realtime_transcribe_service.redis.protocols import ConversationOwnershipGuardBackend
    from realtime_transcribe_service.shutdown.graceful import GracefulShutdown

assert (
    OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC
    == DEFAULT_WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC
)
log = get_logger(__name__)


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


async def _deny_websocket(
    receive: Receive,
    send: Send,
    *,
    status_code: int,
    error_response: dict,
) -> None:
    """Reject the connection before the WebSocket handshake with HTTP status plus JSON ERROR."""
    import orjson

    body = orjson.dumps(error_response)
    await receive()
    await send(
        {
            "type": "websocket.http.response.start",
            "status": status_code,
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


class _WsGuardMiddleware:
    """ASGI middleware that enforces handshake admission checks before accepting WebSockets.

    Check order: conversationId -> auth -> draining -> connection limit -> ownership guard.
    Rejections use the ASGI WebSocket denial response protocol to return a JSON ERROR
    body together with the HTTP status code.
    """

    def __init__(
        self,
        app: ASGIApp,
        shutdown: GracefulShutdown,
        registry: ConnectionRegistry,
        max_connections: int,
        auth_backend: HandshakeAuthBackend | None = None,
        ownership_guard: ConversationOwnershipGuardBackend | None = None,
    ) -> None:
        self._app = app
        self._shutdown = shutdown
        self._registry = registry
        self._max_connections = max_connections
        self._auth_backend = auth_backend
        self._ownership_guard = ownership_guard

    @staticmethod
    def _normalize_ws_path(scope: Scope) -> str:
        """Return the inner-app websocket path, stripping any mount root prefix."""
        path = scope.get("path", "")
        root_path = scope.get("root_path", "")
        if (
            isinstance(path, str)
            and isinstance(root_path, str)
            and root_path
            and path.startswith(root_path)
        ):
            trimmed = path[len(root_path) :]
            return trimmed or "/"
        return path

    @staticmethod
    def _extract_conversation_id(scope: Scope) -> str:
        """Extract ``conversationId`` from the query string, or return an empty string."""
        from urllib.parse import parse_qs

        qs = scope.get("query_string", b"").decode("latin-1")
        params = parse_qs(qs)
        vals = params.get("conversationId")
        return vals[0] if vals else ""

    @staticmethod
    def _extract_header(scope: Scope, header_name: bytes) -> str | None:
        for raw_name, raw_value in scope.get("headers", []):
            if raw_name.lower() == header_name:
                return raw_value.decode("latin-1")
        return None

    async def _reject(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
        *,
        scenario: ProtocolErrorScenario,
        reason: str | None = None,
        conversation_id: str = "",
        details: str | None = None,
        **extra: object,
    ) -> None:
        status_code = scenario.require_http_status()
        error_response = scenario.build_response(conversation_id, details=details)
        log_reason = scenario.default_log_reason if reason is None else reason
        assert log_reason is not None
        _log_handshake_reject(
            scope,
            reason=log_reason,
            status_code=status_code,
            error_response=error_response,
            conversation_id=conversation_id,
            **extra,
        )
        await _deny_websocket(
            receive,
            send,
            status_code=status_code,
            error_response=error_response,
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "websocket":
            await self._app(scope, receive, send)
            return

        path = self._normalize_ws_path(scope)
        if path != WS_PATH:
            await self._app(scope, receive, send)
            return

        cid = self._extract_conversation_id(scope)

        if not cid:
            await self._reject(
                scope,
                receive,
                send,
                scenario=ProtocolErrorScenario.MISSING_QUERY_CONVERSATION_ID,
            )
            return

        if self._auth_backend is not None:
            authorization_header = self._extract_header(scope, b"authorization")
            try:
                principal = self._auth_backend.authenticate(authorization_header)
            except AuthenticationError as exc:
                await self._reject(
                    scope,
                    receive,
                    send,
                    scenario=ProtocolErrorScenario.AUTHENTICATION_FAILED,
                    conversation_id=cid,
                    details=exc.details,
                    auth_result="failed",
                )
                return

            scope[SCOPE_AUTH_SUBJECT] = principal.subject

        if self._shutdown.draining:
            await self._reject(
                scope,
                receive,
                send,
                scenario=ProtocolErrorScenario.SERVICE_DRAINING,
                conversation_id=cid,
            )
            return

        if self._max_connections > 0 and self._registry.active_count >= self._max_connections:
            await self._reject(
                scope,
                receive,
                send,
                scenario=ProtocolErrorScenario.CONNECTION_LIMIT_EXCEEDED,
                conversation_id=cid,
                details=ProtocolErrorScenario.CONNECTION_LIMIT_EXCEEDED.format_details(
                    active=self._registry.active_count,
                    max_connections=self._max_connections,
                ),
                active=self._registry.active_count,
                max=self._max_connections,
            )
            return

        if self._ownership_guard is not None:
            ownership_token = uuid.uuid4().hex
            try:
                owned = await self._ownership_guard.claim_or_refresh(cid, ownership_token)
            except Exception as exc:
                await self._reject(
                    scope,
                    receive,
                    send,
                    reason="Transport: Failed to acquire conversation ownership guard, rejecting connection",
                    scenario=ProtocolErrorScenario.DOWNSTREAM_UNAVAILABLE,
                    conversation_id=cid,
                    details="Conversation ownership guard store unavailable",
                    error=str(exc),
                )
                return

            if not owned:
                await self._reject(
                    scope,
                    receive,
                    send,
                    scenario=ProtocolErrorScenario.ACTIVE_SENDER_CONFLICT,
                    conversation_id=cid,
                )
                return

            scope[SCOPE_OWNERSHIP_TOKEN] = ownership_token
            scope[SCOPE_OWNERSHIP_ACQUIRED] = True

        await self._app(scope, receive, send)


def _create_transport_fastapi_app(
    orchestrator: OrchestratorBackend,
    shutdown: GracefulShutdown,
    registry: ConnectionRegistry,
    *,
    auth_backend: HandshakeAuthBackend | None = None,
    ownership_guard: ConversationOwnershipGuardBackend | None = None,
    redis_client: Any | None = None,
    redis_url: str = "",
    redis_username: str | None = None,
    redis_password: str | None = None,
    redis_ssl_check_hostname: bool = False,
    redis_max_connections: int = 100,
    producer: ProducerBackend | None = None,
    max_connections: int = 0,
    ownership_guard_refresh_interval_sec: float = OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC,
    log_ws_error_frames: bool = False,
    log_slow_message_threshold_ms: float = 0.0,
    http_enable_docs: bool = False,
    openapi_root_path: str = "",
) -> FastAPI:
    """Build the inner FastAPI app (paths are ``/health``, ``/ws/v1/...`` without ALB prefix)."""
    app = FastAPI(
        title=APP_TITLE,
        description=APP_TITLE,
        docs_url="/docs" if http_enable_docs else None,
        redoc_url="/redoc" if http_enable_docs else None,
        openapi_url="/openapi.json" if http_enable_docs else None,
        root_path=openapi_root_path or "",
    )

    app.state.orchestrator = orchestrator
    app.state.shutdown = shutdown
    app.state.registry = registry
    app.state.auth_backend = auth_backend
    app.state.ownership_guard = ownership_guard
    app.state.redis_client = redis_client
    app.state.log_slow_message_threshold_ms = log_slow_message_threshold_ms
    runtime_metrics = RuntimeMetrics()
    app.state.runtime_metrics = runtime_metrics

    app.add_middleware(
        _WsGuardMiddleware,
        shutdown=shutdown,
        registry=registry,
        max_connections=max_connections,
        auth_backend=auth_backend,
        ownership_guard=ownership_guard,
    )

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/ready")
    async def ready():
        errors: list[str] = []
        if redis_client is not None or redis_url:
            from realtime_transcribe_service.config.logging_config import redact_text_for_logs

            runtime_metrics.redis_ready_checks_total += 1
            client = redis_client
            owns_client = False
            try:
                if client is None:
                    from realtime_transcribe_service.redis.async_client import (
                        create_async_redis_client,
                    )

                    client = create_async_redis_client(
                        redis_url,
                        username=redis_username,
                        password=redis_password,
                        ssl_check_hostname=redis_ssl_check_hostname,
                        decode_responses=True,
                        max_connections=redis_max_connections,
                    )
                    owns_client = True
                await cast(Awaitable[Any], client.ping())
            except Exception as e:
                runtime_metrics.redis_ready_failures_total += 1
                log_error = redact_text_for_logs(str(e), extra_secret=redis_password)
                log.warning(
                    "Ready: Redis check failed",
                    error=log_error,
                )
                errors.append("redis:connection_failed")
            finally:
                if owns_client and client is not None:
                    try:
                        await client.aclose()
                    except Exception as close_exc:
                        log.warning(
                            "Ready: Redis client close failed",
                            error=redact_text_for_logs(
                                str(close_exc), extra_secret=redis_password
                            ),
                        )
        if producer is not None:
            try:
                await producer.ensure_ready()
            except Exception as e:
                errors.append(f"kafka:{e}")
        if errors:
            return JSONResponse(status_code=503, content={"status": "not_ready", "errors": errors})
        return {"status": "ready"}

    @app.get("/metrics")
    async def metrics():
        return runtime_metrics.snapshot(registry.active_count)

    app.add_api_websocket_route(
        WS_PATH,
        build_ws_endpoint(
            orchestrator=orchestrator,
            registry=registry,
            ownership_guard=ownership_guard,
            runtime_metrics=runtime_metrics,
            ownership_guard_refresh_interval_sec=ownership_guard_refresh_interval_sec,
            log_ws_error_frames=log_ws_error_frames,
            log_slow_message_threshold_ms=log_slow_message_threshold_ms,
        ),
    )

    return app


def create_app(
    orchestrator: OrchestratorBackend,
    shutdown: GracefulShutdown,
    registry: ConnectionRegistry,
    *,
    auth_backend: HandshakeAuthBackend | None = None,
    ownership_guard: ConversationOwnershipGuardBackend | None = None,
    redis_client: Any | None = None,
    redis_url: str = "",
    redis_username: str | None = None,
    redis_password: str | None = None,
    redis_ssl_check_hostname: bool = False,
    redis_max_connections: int = 100,
    producer: ProducerBackend | None = None,
    max_connections: int = 0,
    ownership_guard_refresh_interval_sec: float = OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC,
    log_ws_error_frames: bool = False,
    log_slow_message_threshold_ms: float = 0.0,
    http_enable_docs: bool = False,
    url_path_prefix: str = "",
) -> FastAPI:
    """Build the service ASGI app, optionally mounting all routes under ``url_path_prefix``."""
    mount_path = normalize_url_path_prefix_str(url_path_prefix)
    inner = _create_transport_fastapi_app(
        orchestrator,
        shutdown,
        registry,
        auth_backend=auth_backend,
        ownership_guard=ownership_guard,
        redis_client=redis_client,
        redis_url=redis_url,
        redis_username=redis_username,
        redis_password=redis_password,
        redis_ssl_check_hostname=redis_ssl_check_hostname,
        redis_max_connections=redis_max_connections,
        producer=producer,
        max_connections=max_connections,
        ownership_guard_refresh_interval_sec=ownership_guard_refresh_interval_sec,
        log_ws_error_frames=log_ws_error_frames,
        log_slow_message_threshold_ms=log_slow_message_threshold_ms,
        http_enable_docs=http_enable_docs,
        openapi_root_path=mount_path,
    )
    if not mount_path:
        return inner

    root = FastAPI(
        title=APP_TITLE,
        description="Mounted at URL_PATH_PREFIX; all HTTP and WebSocket routes live on the inner app.",
        openapi_url=None,
        docs_url=None,
        redoc_url=None,
    )
    root.mount(mount_path, inner)
    return root
