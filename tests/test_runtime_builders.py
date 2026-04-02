"""coverage: runtime builder helpers."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from realtime_transcribe_service import service_runtime
from realtime_transcribe_service.auth import runtime as auth_runtime
from realtime_transcribe_service.auth.jwt_bearer import JwtBearerAuthBackend
from realtime_transcribe_service.config.settings import Settings
from realtime_transcribe_service.producer import runtime as producer_runtime
from realtime_transcribe_service.redis import runtime as redis_runtime


def _settings(**kwargs: Any) -> Settings:
    """Build ``Settings`` without loading repo ``.env``."""
    return Settings(**kwargs)  # pyright: ignore[reportCallIssue]


@pytest.mark.asyncio
async def test_create_shared_redis_client_uses_settings():
    settings = _settings(
        _env_file=None,
        app_env="local",
        redis_url="rediss://redis.example:6379/0",
        redis_username="acl-user",
        redis_password="secret",
        redis_ssl_check_hostname=True,
        redis_max_connections=321,
    )
    fake_client = MagicMock()

    with patch.object(redis_runtime, "create_async_redis_client", return_value=fake_client) as create_mock:
        out = redis_runtime.create_shared_redis_client(settings)

    assert out is fake_client
    create_mock.assert_called_once_with(
        "rediss://redis.example:6379/0",
        username="acl-user",
        password="secret",
        ssl_check_hostname=True,
        decode_responses=True,
        max_connections=321,
    )


def test_create_sequence_state_machine_uses_shared_client():
    settings = _settings(_env_file=None, app_env="local")
    fake_sm = MagicMock()
    shared_client = MagicMock()

    with patch.object(redis_runtime, "RedisSequenceStateMachine", return_value=fake_sm) as sm_cls:
        out = redis_runtime.create_sequence_state_machine(settings, client=shared_client)

    assert out is fake_sm
    sm_cls.assert_called_once_with(
        redis_url="redis://127.0.0.1:6379/0",
        max_connections=100,
        redis_username=None,
        redis_password=None,
        ssl_check_hostname=False,
        active_ttl_sec=3600,
        final_ttl_sec=60,
        key_prefix="realtime-transcribe-service:expect-transcript-seq-num",
        client=shared_client,
    )


def test_create_ownership_guard_uses_shared_client():
    settings = _settings(_env_file=None, app_env="local")
    fake_guard = MagicMock()
    shared_client = MagicMock()

    with patch.object(redis_runtime, "RedisConversationOwnershipGuard", return_value=fake_guard) as guard_cls:
        out = redis_runtime.create_ownership_guard(settings, client=shared_client)

    assert out is fake_guard
    guard_cls.assert_called_once_with(
        redis_url="redis://127.0.0.1:6379/0",
        max_connections=100,
        redis_username=None,
        redis_password=None,
        ssl_check_hostname=False,
        guard_ttl_sec=30,
        key_prefix="realtime-transcribe-service:conversation-owner",
        client=shared_client,
    )


@pytest.mark.asyncio
async def test_close_redis_runtime_closes_in_order():
    calls: list[str] = []
    shared_client = MagicMock()
    sequence_state_machine = MagicMock()
    ownership_guard = MagicMock()

    async def close_sm():
        calls.append("sequence_state_machine.close")

    async def close_owner():
        calls.append("ownership_guard.close")

    async def close_client():
        calls.append("redis_client.aclose")

    sequence_state_machine.close = AsyncMock(side_effect=close_sm)
    ownership_guard.close = AsyncMock(side_effect=close_owner)
    shared_client.aclose = AsyncMock(side_effect=close_client)

    await redis_runtime.close_redis_runtime(
        client=shared_client,
        sequence_state_machine=sequence_state_machine,
        ownership_guard=ownership_guard,
    )

    assert calls == [
        "sequence_state_machine.close",
        "ownership_guard.close",
        "redis_client.aclose",
    ]


def test_create_kafka_producer_uses_connection_profile():
    settings = _settings(
        _env_file=None,
        app_env="local",
        kafka_bootstrap_servers="127.0.0.1:9092",
        kafka_mode="local",
    )
    fake_connection = MagicMock()
    fake_producer = MagicMock()

    with (
        patch.object(
            producer_runtime,
            "kafka_connection_for_mode",
            return_value=fake_connection,
        ) as conn_mock,
        patch.object(
            producer_runtime,
            "KafkaProducer",
            return_value=fake_producer,
        ) as producer_cls,
    ):
        out = producer_runtime.create_kafka_producer(settings)

    assert out is fake_producer
    conn_mock.assert_called_once_with(
        "local",
        aws_region=None,
        ssl_ca_file=None,
        aws_debug_creds=False,
    )
    producer_cls.assert_called_once_with(
        bootstrap_servers="127.0.0.1:9092",
        topic="AI_STAGING_TRANSCRIPTION",
        connection=fake_connection,
        compression_type="zstd",
        send_timeout_sec=2.0,
        linger_ms=1,
        batch_size=32768,
    )


def test_create_auth_backend_returns_none_when_disabled():
    settings = _settings(_env_file=None, app_env="local", auth_enabled=False)
    assert auth_runtime.create_auth_backend(settings) is None


def test_create_auth_backend_uses_settings_when_enabled():
    settings = _settings(
        _env_file=None,
        app_env="local",
        auth_enabled=True,
        auth_jwt_signing_material="signing-material",
        auth_jwt_algorithm="HS256",
    )
    backend = auth_runtime.create_auth_backend(settings)
    assert isinstance(backend, JwtBearerAuthBackend)


@pytest.mark.asyncio
async def test_create_runtime_bundle_assembles_application_runtime():
    settings = _settings(_env_file=None, app_env="local")
    shared_client = MagicMock()
    sequence_state_machine = MagicMock()
    ownership_guard = MagicMock()
    producer = MagicMock()
    orchestrator = MagicMock()
    shutdown = MagicMock()
    registry = MagicMock()
    auth_backend = MagicMock()
    app = MagicMock()
    server = MagicMock()

    with (
        patch.object(
            service_runtime,
            "create_shared_redis_client",
            return_value=shared_client,
        ) as create_client,
        patch.object(
            service_runtime,
            "create_sequence_state_machine",
            return_value=sequence_state_machine,
        ) as create_sm,
        patch.object(
            service_runtime,
            "create_ownership_guard",
            return_value=ownership_guard,
        ) as create_owner,
        patch.object(
            service_runtime,
            "create_kafka_producer",
            return_value=producer,
        ) as create_producer,
        patch.object(
            service_runtime,
            "TwoPhaseOrchestrator",
            return_value=orchestrator,
        ) as orchestrator_cls,
        patch.object(
            service_runtime,
            "GracefulShutdown",
            return_value=shutdown,
        ) as shutdown_cls,
        patch.object(
            service_runtime,
            "ConnectionRegistry",
            return_value=registry,
        ) as registry_cls,
        patch.object(
            service_runtime,
            "create_auth_backend",
            return_value=auth_backend,
        ) as create_auth,
        patch.object(
            service_runtime,
            "build_web_app",
            return_value=app,
        ) as build_app,
        patch.object(
            service_runtime,
            "create_uvicorn_server",
            return_value=server,
        ) as create_server,
    ):
        out = await service_runtime.create_runtime_bundle(settings)

    assert out.shared_redis_client is shared_client
    assert out.sequence_state_machine is sequence_state_machine
    assert out.ownership_guard is ownership_guard
    assert out.producer is producer
    assert out.orchestrator is orchestrator
    assert out.shutdown is shutdown
    assert out.registry is registry
    assert out.auth_backend is auth_backend
    assert out.app is app
    assert out.server is server
    create_client.assert_called_once_with(settings)
    create_sm.assert_called_once_with(settings, client=shared_client)
    create_owner.assert_called_once_with(settings, client=shared_client)
    create_producer.assert_called_once_with(settings)
    orchestrator_cls.assert_called_once()
    shutdown_cls.assert_called_once_with(stop_timeout=settings.stop_timeout)
    shutdown.register_signal.assert_called_once_with()
    registry_cls.assert_called_once_with()
    create_auth.assert_called_once_with(settings)
    build_app.assert_called_once_with(
        settings,
        orchestrator=orchestrator,
        shutdown=shutdown,
        registry=registry,
        auth_backend=auth_backend,
        ownership_guard=ownership_guard,
        redis_client=shared_client,
        producer=producer,
    )
    create_server.assert_called_once_with(settings, app)


@pytest.mark.asyncio
async def test_create_runtime_bundle_failure_closes_partial_resources():
    settings = _settings(_env_file=None, app_env="local")
    shared_client = MagicMock()
    sequence_state_machine = MagicMock()
    ownership_guard = MagicMock()
    producer = MagicMock(close=AsyncMock())
    close_redis_runtime = AsyncMock()
    shutdown = MagicMock()
    registry = MagicMock()

    with (
        patch.object(
            service_runtime,
            "create_shared_redis_client",
            return_value=shared_client,
        ),
        patch.object(
            service_runtime,
            "create_sequence_state_machine",
            return_value=sequence_state_machine,
        ),
        patch.object(
            service_runtime,
            "create_ownership_guard",
            return_value=ownership_guard,
        ),
        patch.object(
            service_runtime,
            "create_kafka_producer",
            return_value=producer,
        ),
        patch.object(
            service_runtime,
            "GracefulShutdown",
            return_value=shutdown,
        ),
        patch.object(
            service_runtime,
            "ConnectionRegistry",
            return_value=registry,
        ),
        patch.object(
            service_runtime,
            "create_auth_backend",
            side_effect=RuntimeError("boom"),
        ),
        patch.object(
            service_runtime,
            "close_redis_runtime",
            close_redis_runtime,
        ),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            await service_runtime.create_runtime_bundle(settings)

    producer.close.assert_awaited_once()
    close_redis_runtime.assert_awaited_once_with(
        client=shared_client,
        sequence_state_machine=sequence_state_machine,
        ownership_guard=ownership_guard,
    )


@pytest.mark.asyncio
async def test_close_runtime_bundle_closes_producer_before_redis_runtime():
    calls: list[str] = []
    producer = MagicMock()
    shared_client = MagicMock()
    sequence_state_machine = MagicMock()
    ownership_guard = MagicMock()

    async def close_producer():
        calls.append("producer.close")

    async def close_redis(*, client: object, sequence_state_machine: object, ownership_guard: object) -> None:
        assert client is shared_client
        assert sequence_state_machine is sequence_state_machine_ref
        assert ownership_guard is ownership_guard_ref
        calls.append("close_redis_runtime")

    producer.close = AsyncMock(side_effect=close_producer)
    sequence_state_machine_ref = sequence_state_machine
    ownership_guard_ref = ownership_guard
    bundle = service_runtime.RuntimeBundle(
        shared_redis_client=shared_client,
        sequence_state_machine=sequence_state_machine,
        ownership_guard=ownership_guard,
        producer=producer,
        orchestrator=MagicMock(),
        shutdown=MagicMock(),
        registry=MagicMock(),
        auth_backend=None,
        app=MagicMock(),
        server=MagicMock(),
    )

    with patch.object(service_runtime, "close_redis_runtime", AsyncMock(side_effect=close_redis)) as close_redis_mock:
        await service_runtime.close_runtime_bundle(bundle)

    producer.close.assert_awaited_once()
    close_redis_mock.assert_awaited_once()
    assert calls == ["producer.close", "close_redis_runtime"]


def test_build_web_app_uses_settings_and_dependencies():
    settings = _settings(_env_file=None, app_env="local")
    fake_app = MagicMock()
    orchestrator = MagicMock()
    shutdown = MagicMock()
    registry = MagicMock()
    auth_backend = MagicMock()
    ownership_guard = MagicMock()
    redis_client = MagicMock()
    producer = MagicMock()

    with patch.object(service_runtime, "create_app", return_value=fake_app) as create_app_mock:
        out = service_runtime.build_web_app(
            settings,
            orchestrator=orchestrator,
            shutdown=shutdown,
            registry=registry,
            auth_backend=auth_backend,
            ownership_guard=ownership_guard,
            redis_client=redis_client,
            producer=producer,
        )

    assert out is fake_app
    create_app_mock.assert_called_once_with(
        orchestrator=orchestrator,
        shutdown=shutdown,
        registry=registry,
        auth_backend=auth_backend,
        ownership_guard=ownership_guard,
        redis_client=redis_client,
        redis_url="redis://127.0.0.1:6379/0",
        redis_username=None,
        redis_password=None,
        redis_ssl_check_hostname=False,
        redis_max_connections=100,
        producer=producer,
        max_connections=0,
        ownership_guard_refresh_interval_sec=15.0,
        log_ws_error_frames=False,
        log_slow_message_threshold_ms=0.0,
        http_enable_docs=False,
        url_path_prefix="",
    )


def test_create_uvicorn_server_uses_settings():
    settings = _settings(_env_file=None, app_env="local", http_port=18080)
    app = MagicMock()
    config = MagicMock()
    server = MagicMock()

    with (
        patch.object(service_runtime.uvicorn, "Config", return_value=config) as config_cls,
        patch.object(service_runtime.uvicorn, "Server", return_value=server) as server_cls,
    ):
        out = service_runtime.create_uvicorn_server(settings, app)

    assert out is server
    config_cls.assert_called_once_with(
        app,
        host="0.0.0.0",
        port=18080,
        ws="websockets",
        access_log=True,
        backlog=4096,
        ws_ping_interval=20.0,
        ws_ping_timeout=10.0,
        log_config=None,
        log_level="info",
    )
    server_cls.assert_called_once_with(config)
