"""Service-level runtime bundle assembly and teardown."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import uvicorn
from fastapi import FastAPI

from realtime_transcribe_service.auth.protocols import HandshakeAuthBackend
from realtime_transcribe_service.auth.runtime import create_auth_backend
from realtime_transcribe_service.config.settings import Settings
from realtime_transcribe_service.constants import DEFAULT_HTTP_HOST
from realtime_transcribe_service.converter.kafka_message_converter import KafkaMessageConverter
from realtime_transcribe_service.orchestrator.two_phase import TwoPhaseOrchestrator
from realtime_transcribe_service.producer.kafka_producer import KafkaProducer
from realtime_transcribe_service.producer.runtime import create_kafka_producer
from realtime_transcribe_service.redis.ownership_guard import RedisConversationOwnershipGuard
from realtime_transcribe_service.redis.runtime import (
    close_redis_runtime,
    create_ownership_guard,
    create_sequence_state_machine,
    create_shared_redis_client,
)
from realtime_transcribe_service.redis.sequence_state_machine import RedisSequenceStateMachine
from realtime_transcribe_service.shutdown.graceful import GracefulShutdown
from realtime_transcribe_service.transport.app import create_app
from realtime_transcribe_service.transport.registry import ConnectionRegistry


@dataclass(slots=True)
class RuntimeBundle:
    """Concrete runtime dependencies owned by the service entrypoint."""

    shared_redis_client: Any
    sequence_state_machine: RedisSequenceStateMachine
    ownership_guard: RedisConversationOwnershipGuard
    producer: KafkaProducer
    orchestrator: TwoPhaseOrchestrator
    shutdown: GracefulShutdown
    registry: ConnectionRegistry
    auth_backend: HandshakeAuthBackend | None
    app: FastAPI
    server: uvicorn.Server


def build_web_app(
    settings: Settings,
    *,
    orchestrator: TwoPhaseOrchestrator,
    shutdown: GracefulShutdown,
    registry: ConnectionRegistry,
    auth_backend: HandshakeAuthBackend | None,
    ownership_guard: RedisConversationOwnershipGuard | None,
    redis_client: object | None,
    producer: KafkaProducer | None,
) -> FastAPI:
    """Build the FastAPI transport app from runtime dependencies and settings."""
    redis_url = settings.redis_url
    assert redis_url is not None
    return create_app(
        orchestrator=orchestrator,
        shutdown=shutdown,
        registry=registry,
        auth_backend=auth_backend,
        ownership_guard=ownership_guard,
        redis_client=redis_client,
        redis_url=redis_url,
        redis_username=settings.redis_username,
        redis_password=settings.redis_password,
        redis_ssl_check_hostname=settings.redis_ssl_check_hostname,
        redis_max_connections=settings.redis_max_connections,
        producer=producer,
        max_connections=settings.ws_max_connections,
        ownership_guard_refresh_interval_sec=settings.ws_ownership_guard_refresh_interval_sec,
        log_ws_error_frames=settings.log_ws_error_frames,
        log_slow_message_threshold_ms=settings.log_slow_message_threshold_ms,
        http_enable_docs=settings.http_enable_docs,
        url_path_prefix=settings.url_path_prefix,
    )


def create_uvicorn_server(settings: Settings, app: FastAPI) -> uvicorn.Server:
    """Create the uvicorn server configured for the WebSocket transport."""
    config = uvicorn.Config(
        app,
        host=DEFAULT_HTTP_HOST,
        port=settings.http_port,
        ws="websockets",
        access_log=True,
        backlog=settings.http_backlog,
        ws_ping_interval=settings.ws_ping_interval,
        ws_ping_timeout=settings.ws_ping_timeout,
        log_config=None,
        log_level=settings.log_level.lower(),
    )
    return uvicorn.Server(config)


async def create_runtime_bundle(settings: Settings) -> RuntimeBundle:
    """Assemble the runtime dependency graph for one application process."""
    shared_redis_client: Any | None = None
    sequence_state_machine: RedisSequenceStateMachine | None = None
    ownership_guard: RedisConversationOwnershipGuard | None = None
    producer: KafkaProducer | None = None
    try:
        shared_redis_client = create_shared_redis_client(settings)
        sequence_state_machine = create_sequence_state_machine(
            settings,
            client=shared_redis_client,
        )
        ownership_guard = create_ownership_guard(
            settings,
            client=shared_redis_client,
        )
        producer = create_kafka_producer(settings)
        orchestrator = TwoPhaseOrchestrator(
            state_machine=sequence_state_machine,
            producer=producer,
            message_converter=KafkaMessageConverter(),
        )
        shutdown = GracefulShutdown(stop_timeout=settings.stop_timeout)
        shutdown.register_signal()
        registry = ConnectionRegistry()
        auth_backend = create_auth_backend(settings)
        app = build_web_app(
            settings,
            orchestrator=orchestrator,
            shutdown=shutdown,
            registry=registry,
            auth_backend=auth_backend,
            ownership_guard=ownership_guard,
            redis_client=shared_redis_client,
            producer=producer,
        )
        server = create_uvicorn_server(settings, app)
        return RuntimeBundle(
            shared_redis_client=shared_redis_client,
            sequence_state_machine=sequence_state_machine,
            ownership_guard=ownership_guard,
            producer=producer,
            orchestrator=orchestrator,
            shutdown=shutdown,
            registry=registry,
            auth_backend=auth_backend,
            app=app,
            server=server,
        )
    except Exception:
        if producer is not None:
            await producer.close()
        await close_redis_runtime(
            client=shared_redis_client,
            sequence_state_machine=sequence_state_machine,
            ownership_guard=ownership_guard,
        )
        raise


async def close_runtime_bundle(bundle: RuntimeBundle) -> None:
    """Release process-owned runtime resources in deterministic order."""
    await bundle.producer.close()
    await close_redis_runtime(
        client=bundle.shared_redis_client,
        sequence_state_machine=bundle.sequence_state_machine,
        ownership_guard=bundle.ownership_guard,
    )
