"""coverage: main._check_*, main.run"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

import realtime_transcribe_service.main as main_mod
from realtime_transcribe_service.auth.jwt_bearer import JwtBearerAuthBackend
from realtime_transcribe_service.converter.kafka_message_converter import KafkaMessageConverter
from realtime_transcribe_service.config.settings import Settings


def _settings(**kwargs: Any) -> Settings:
    """Build ``Settings`` without loading repo ``.env`` (pydantic-settings internal kwargs)."""
    return Settings(**kwargs)  # pyright: ignore[reportCallIssue]


@pytest.mark.asyncio
async def test_check_redis_success():
    fake = MagicMock()
    fake.ping = AsyncMock()
    fake.aclose = AsyncMock()
    with patch("redis.asyncio.Redis.from_url", return_value=fake):
        await main_mod._check_redis("redis://127.0.0.1:6379/0")
    fake.ping.assert_awaited_once()
    fake.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_check_redis_failure():
    fake = MagicMock()
    fake.ping = AsyncMock(side_effect=RuntimeError("down"))
    fake.aclose = AsyncMock()
    with patch("redis.asyncio.Redis.from_url", return_value=fake):
        with pytest.raises(RuntimeError, match="Redis"):
            await main_mod._check_redis("redis://x")
    fake.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_check_kafka_success():
    prod = MagicMock()
    prod.ensure_ready = AsyncMock()
    await main_mod._check_kafka(prod, timeout=5.0)
    prod.ensure_ready.assert_awaited_once()


@pytest.mark.asyncio
async def test_check_kafka_timeout():
    async def slow():
        await asyncio.sleep(10)

    prod = MagicMock()
    prod.ensure_ready = AsyncMock(side_effect=slow)
    prod.close = AsyncMock()
    with pytest.raises(RuntimeError, match="Kafka.*timed out"):
        await main_mod._check_kafka(prod, timeout=0.05)
    prod.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_check_kafka_timeout_close_raises_logged(monkeypatch):
    async def slow():
        await asyncio.sleep(10)

    prod = MagicMock()
    prod.ensure_ready = AsyncMock(side_effect=slow)
    prod.close = AsyncMock(side_effect=RuntimeError("close failed"))
    warn_mock = MagicMock()
    monkeypatch.setattr(main_mod.log, "warning", warn_mock)
    with pytest.raises(RuntimeError, match="Kafka.*timed out"):
        await main_mod._check_kafka(prod, timeout=0.05)
    warn_mock.assert_called_once()


@pytest.mark.asyncio
async def test_check_kafka_other_error():
    prod = MagicMock()
    prod.ensure_ready = AsyncMock(side_effect=RuntimeError("broker"))
    with pytest.raises(RuntimeError, match="Kafka"):
        await main_mod._check_kafka(prod, timeout=5.0)


@pytest.mark.asyncio
async def test_run_invalid_settings_fail_before_startup_checks(monkeypatch):
    def invalid_settings():
        return _settings(_env_file=None, app_env="deployed")

    redis_check = AsyncMock()
    kafka_check = AsyncMock()
    monkeypatch.setattr(main_mod, "get_settings", invalid_settings)
    monkeypatch.setattr(main_mod, "_check_redis", redis_check)
    monkeypatch.setattr(main_mod, "_check_kafka", kafka_check)

    with pytest.raises(ValidationError):
        await main_mod.run()

    redis_check.assert_not_awaited()
    kafka_check.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_graceful_shutdown_path(monkeypatch):
    """Exercise run(): server serve loop, shutdown, registry, producer, sequence state machine."""
    settings = MagicMock()
    settings.redis_url = "redis://127.0.0.1:6379/0"
    settings.log_level = "INFO"
    settings.log_format = "json"
    settings.kafka_bootstrap_servers = "127.0.0.1:9092"
    settings.kafka_topic = "t"
    settings.kafka_compression_type = "none"
    settings.kafka_send_timeout_sec = 2.0
    settings.kafka_topic_num_partitions = 1
    settings.kafka_replication_factor = 1
    settings.redis_max_connections = 10
    settings.redis_active_ttl_sec = 3600
    settings.redis_final_ttl_sec = 60
    settings.stop_timeout = 120
    settings.http_host = "127.0.0.1"
    settings.http_port = 18080
    settings.http_backlog = 4096
    settings.http_enable_docs = True
    settings.kafka_startup_timeout_sec = 5.0
    settings.ws_ping_interval = 20.0
    settings.ws_ping_timeout = 21.0
    settings.ws_max_connections = 0
    settings.log_ws_error_frames = False
    settings.redis_ownership_guard_ttl_sec = 30
    settings.redis_sequence_state_key_prefix = "realtime-transcribe-service:expect-transcript-seq-num"
    settings.redis_ownership_guard_key_prefix = "realtime-transcribe-service:conversation-owner"
    settings.ws_ownership_guard_refresh_interval_sec = 5.0
    settings.auth_enabled = True
    settings.auth_jwt_signing_material = "signing-material"
    settings.auth_jwt_algorithm = "HS256"

    monkeypatch.setattr(main_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", MagicMock())

    sm = MagicMock()
    sm.close = AsyncMock()
    monkeypatch.setattr(main_mod, "RedisSequenceStateMachine", lambda **kw: sm)

    owner = MagicMock()
    owner.close = AsyncMock()
    monkeypatch.setattr(main_mod, "RedisConversationOwnershipGuard", lambda **kw: owner)

    prod = MagicMock()
    prod.flush = AsyncMock()
    prod.close = AsyncMock()
    monkeypatch.setattr(main_mod, "KafkaProducer", lambda **kw: prod)

    orch = MagicMock()
    orchestrator_kwargs: dict = {}

    def make_orchestrator(**kw):
        orchestrator_kwargs.update(kw)
        return orch

    monkeypatch.setattr(main_mod, "TwoPhaseOrchestrator", make_orchestrator)

    shutdown_inst = main_mod.GracefulShutdown(stop_timeout=1)

    def make_shutdown(**kw):
        return shutdown_inst

    monkeypatch.setattr(main_mod, "GracefulShutdown", make_shutdown)

    reg = MagicMock()
    reg.close_all = AsyncMock()
    monkeypatch.setattr(main_mod, "ConnectionRegistry", lambda: reg)

    app_obj = object()
    create_app_kwargs: dict = {}

    def capture_create_app(**kw):
        create_app_kwargs.update(kw)
        return app_obj

    monkeypatch.setattr(main_mod, "create_app", capture_create_app)

    monkeypatch.setattr(main_mod, "_check_redis", AsyncMock())
    monkeypatch.setattr(main_mod, "_check_kafka", AsyncMock())

    server_inst = MagicMock()
    server_inst.should_exit = False
    serve_started = asyncio.Event()

    async def fake_serve():
        serve_started.set()
        while not server_inst.should_exit:
            await asyncio.sleep(0.01)

    server_inst.serve = fake_serve

    config_calls: list[tuple[object, dict]] = []

    def capture_config(app, **kw):
        config_calls.append((app, kw))
        return MagicMock(app=app)

    def server_factory(cfg):
        assert cfg.app is app_obj
        return server_inst

    monkeypatch.setattr(main_mod.uvicorn, "Config", capture_config)
    monkeypatch.setattr(main_mod.uvicorn, "Server", server_factory)

    async def stop_soon():
        await serve_started.wait()
        await asyncio.sleep(0.02)
        await shutdown_inst._on_signal()

    stop_task = asyncio.create_task(stop_soon())
    try:
        await asyncio.wait_for(main_mod.run(), timeout=5.0)
    finally:
        await asyncio.wait([stop_task], timeout=1.0)

    assert len(config_calls) == 1
    _app, kw = config_calls[0]
    assert _app is app_obj
    assert kw["ws"] == "websockets"
    assert kw["ws_ping_interval"] == 20.0
    assert kw["ws_ping_timeout"] == 21.0
    assert kw["backlog"] == 4096
    assert kw["log_config"] is None
    assert kw["log_level"] == "info"
    assert create_app_kwargs["http_enable_docs"] is True
    assert isinstance(orchestrator_kwargs["message_converter"], KafkaMessageConverter)
    assert isinstance(create_app_kwargs["auth_backend"], JwtBearerAuthBackend)

    reg.close_all.assert_awaited()
    prod.flush.assert_awaited()
    prod.close.assert_awaited()
    sm.close.assert_awaited()
    owner.close.assert_awaited()


@pytest.mark.asyncio
async def test_run_graceful_shutdown_order(monkeypatch):
    settings = MagicMock()
    settings.redis_url = "redis://127.0.0.1:6379/0"
    settings.log_level = "INFO"
    settings.log_format = "json"
    settings.kafka_bootstrap_servers = "127.0.0.1:9092"
    settings.kafka_topic = "t"
    settings.kafka_compression_type = "none"
    settings.kafka_send_timeout_sec = 2.0
    settings.kafka_topic_num_partitions = 1
    settings.kafka_replication_factor = 1
    settings.redis_max_connections = 10
    settings.redis_active_ttl_sec = 3600
    settings.redis_final_ttl_sec = 60
    settings.stop_timeout = 120
    settings.http_host = "127.0.0.1"
    settings.http_port = 18083
    settings.http_backlog = 4096
    settings.kafka_startup_timeout_sec = 5.0
    settings.ws_ping_interval = 20.0
    settings.ws_ping_timeout = 21.0
    settings.ws_max_connections = 0
    settings.log_ws_error_frames = False
    settings.redis_ownership_guard_ttl_sec = 30
    settings.redis_sequence_state_key_prefix = "realtime-transcribe-service:expect-transcript-seq-num"
    settings.redis_ownership_guard_key_prefix = "realtime-transcribe-service:conversation-owner"
    settings.ws_ownership_guard_refresh_interval_sec = 5.0

    monkeypatch.setattr(main_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", MagicMock())

    calls: list[str] = []

    sm = MagicMock()

    async def sm_close():
        calls.append("sequence_state_machine.close")

    sm.close = AsyncMock(side_effect=sm_close)
    monkeypatch.setattr(main_mod, "RedisSequenceStateMachine", lambda **kw: sm)

    owner = MagicMock()

    async def owner_close():
        calls.append("ownership_guard.close")

    owner.close = AsyncMock(side_effect=owner_close)
    monkeypatch.setattr(main_mod, "RedisConversationOwnershipGuard", lambda **kw: owner)

    prod = MagicMock()

    async def prod_flush():
        calls.append("producer.flush")

    async def prod_close():
        calls.append("producer.close")

    prod.flush = AsyncMock(side_effect=prod_flush)
    prod.close = AsyncMock(side_effect=prod_close)
    monkeypatch.setattr(main_mod, "KafkaProducer", lambda **kw: prod)

    monkeypatch.setattr(main_mod, "TwoPhaseOrchestrator", lambda **kw: MagicMock())

    shutdown_inst = main_mod.GracefulShutdown(stop_timeout=1)
    monkeypatch.setattr(main_mod, "GracefulShutdown", lambda **kw: shutdown_inst)

    reg = MagicMock()

    async def reg_close_all(*args, **kwargs):
        calls.append("registry.close_all")

    reg.close_all = AsyncMock(side_effect=reg_close_all)
    monkeypatch.setattr(main_mod, "ConnectionRegistry", lambda: reg)
    monkeypatch.setattr(main_mod, "create_app", lambda **kw: object())
    monkeypatch.setattr(main_mod, "_check_redis", AsyncMock())
    monkeypatch.setattr(main_mod, "_check_kafka", AsyncMock())

    server_inst = MagicMock()
    server_inst.should_exit = False
    serve_started = asyncio.Event()

    async def fake_serve():
        serve_started.set()
        while not server_inst.should_exit:
            await asyncio.sleep(0.01)

    server_inst.serve = fake_serve
    monkeypatch.setattr(main_mod.uvicorn, "Config", lambda app, **kw: MagicMock(app=app))
    monkeypatch.setattr(main_mod.uvicorn, "Server", lambda cfg: server_inst)

    async def stop_soon():
        await serve_started.wait()
        await asyncio.sleep(0.02)
        await shutdown_inst._on_signal()

    stop_task = asyncio.create_task(stop_soon())
    try:
        await asyncio.wait_for(main_mod.run(), timeout=5.0)
    finally:
        await asyncio.wait([stop_task], timeout=1.0)

    assert calls == [
        "registry.close_all",
        "producer.flush",
        "producer.close",
        "sequence_state_machine.close",
        "ownership_guard.close",
    ]


@pytest.mark.asyncio
async def test_run_startup_checks_are_parallel(monkeypatch):
    settings = MagicMock()
    settings.redis_url = "redis://127.0.0.1:6379/0"
    settings.log_level = "INFO"
    settings.log_format = "json"
    settings.kafka_bootstrap_servers = "127.0.0.1:9092"
    settings.kafka_topic = "t"
    settings.kafka_compression_type = "none"
    settings.kafka_send_timeout_sec = 2.0
    settings.kafka_topic_num_partitions = 1
    settings.kafka_replication_factor = 1
    settings.redis_max_connections = 10
    settings.redis_active_ttl_sec = 3600
    settings.redis_final_ttl_sec = 60
    settings.stop_timeout = 120
    settings.http_host = "127.0.0.1"
    settings.http_port = 18084
    settings.http_backlog = 4096
    settings.kafka_startup_timeout_sec = 5.0
    settings.ws_ping_interval = 20.0
    settings.ws_ping_timeout = 21.0
    settings.ws_max_connections = 0
    settings.log_ws_error_frames = False
    settings.redis_ownership_guard_ttl_sec = 30
    settings.redis_sequence_state_key_prefix = "realtime-transcribe-service:expect-transcript-seq-num"
    settings.redis_ownership_guard_key_prefix = "realtime-transcribe-service:conversation-owner"
    settings.ws_ownership_guard_refresh_interval_sec = 5.0

    monkeypatch.setattr(main_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", MagicMock())
    monkeypatch.setattr(
        main_mod, "RedisSequenceStateMachine", lambda **kw: MagicMock(close=AsyncMock())
    )
    monkeypatch.setattr(
        main_mod,
        "RedisConversationOwnershipGuard",
        lambda **kw: MagicMock(close=AsyncMock()),
    )
    monkeypatch.setattr(
        main_mod,
        "KafkaProducer",
        lambda **kw: MagicMock(flush=AsyncMock(), close=AsyncMock()),
    )
    monkeypatch.setattr(main_mod, "TwoPhaseOrchestrator", lambda **kw: MagicMock())

    shutdown_inst = main_mod.GracefulShutdown(stop_timeout=1)
    monkeypatch.setattr(main_mod, "GracefulShutdown", lambda **kw: shutdown_inst)
    monkeypatch.setattr(
        main_mod, "ConnectionRegistry", lambda: MagicMock(close_all=AsyncMock())
    )
    monkeypatch.setattr(main_mod, "create_app", lambda **kw: object())

    order: list[str] = []
    redis_started = asyncio.Event()
    kafka_started = asyncio.Event()
    release_checks = asyncio.Event()

    async def fake_check_redis(_url: str) -> None:
        order.append("redis-start")
        redis_started.set()
        await kafka_started.wait()
        release_checks.set()
        await release_checks.wait()
        order.append("redis-end")

    async def fake_check_kafka(_producer, _timeout: float) -> None:
        order.append("kafka-start")
        kafka_started.set()
        await redis_started.wait()
        await release_checks.wait()
        order.append("kafka-end")

    monkeypatch.setattr(main_mod, "_check_redis", fake_check_redis)
    monkeypatch.setattr(main_mod, "_check_kafka", fake_check_kafka)

    server_inst = MagicMock()
    server_inst.should_exit = False
    serve_started = asyncio.Event()

    async def fake_serve():
        serve_started.set()
        while not server_inst.should_exit:
            await asyncio.sleep(0.01)

    server_inst.serve = fake_serve
    monkeypatch.setattr(main_mod.uvicorn, "Config", lambda app, **kw: MagicMock(app=app))
    monkeypatch.setattr(main_mod.uvicorn, "Server", lambda cfg: server_inst)

    async def stop_soon():
        await serve_started.wait()
        await asyncio.sleep(0.02)
        await shutdown_inst._on_signal()

    stop_task = asyncio.create_task(stop_soon())
    try:
        await asyncio.wait_for(main_mod.run(), timeout=5.0)
    finally:
        await asyncio.wait([stop_task], timeout=1.0)

    assert set(order[:2]) == {"redis-start", "kafka-start"}
    assert set(order[2:]) == {"redis-end", "kafka-end"}


@pytest.mark.asyncio
async def test_run_stop_timeout_forces_cleanup(monkeypatch):
    settings = MagicMock()
    settings.redis_url = "redis://127.0.0.1:6379/0"
    settings.log_level = "INFO"
    settings.log_format = "json"
    settings.kafka_bootstrap_servers = "127.0.0.1:9092"
    settings.kafka_topic = "t"
    settings.kafka_compression_type = "none"
    settings.kafka_send_timeout_sec = 2.0
    settings.kafka_topic_num_partitions = 1
    settings.kafka_replication_factor = 1
    settings.redis_max_connections = 10
    settings.redis_active_ttl_sec = 3600
    settings.redis_final_ttl_sec = 60
    settings.stop_timeout = 0.05
    settings.http_host = "127.0.0.1"
    settings.http_port = 18085
    settings.http_backlog = 4096
    settings.kafka_startup_timeout_sec = 5.0
    settings.ws_ping_interval = 20.0
    settings.ws_ping_timeout = 21.0
    settings.ws_max_connections = 0
    settings.log_ws_error_frames = False
    settings.redis_ownership_guard_ttl_sec = 30
    settings.redis_sequence_state_key_prefix = "realtime-transcribe-service:expect-transcript-seq-num"
    settings.redis_ownership_guard_key_prefix = "realtime-transcribe-service:conversation-owner"
    settings.ws_ownership_guard_refresh_interval_sec = 5.0

    monkeypatch.setattr(main_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", MagicMock())

    sm = MagicMock(close=AsyncMock())
    monkeypatch.setattr(main_mod, "RedisSequenceStateMachine", lambda **kw: sm)
    owner = MagicMock(close=AsyncMock())
    monkeypatch.setattr(main_mod, "RedisConversationOwnershipGuard", lambda **kw: owner)
    prod = MagicMock(flush=AsyncMock(), close=AsyncMock())
    monkeypatch.setattr(main_mod, "KafkaProducer", lambda **kw: prod)
    monkeypatch.setattr(main_mod, "TwoPhaseOrchestrator", lambda **kw: MagicMock())

    shutdown_inst = main_mod.GracefulShutdown(stop_timeout=settings.stop_timeout)
    monkeypatch.setattr(main_mod, "GracefulShutdown", lambda **kw: shutdown_inst)

    reg = MagicMock(close_all=AsyncMock())
    monkeypatch.setattr(main_mod, "ConnectionRegistry", lambda: reg)
    monkeypatch.setattr(main_mod, "create_app", lambda **kw: object())
    monkeypatch.setattr(main_mod, "_check_redis", AsyncMock())
    monkeypatch.setattr(main_mod, "_check_kafka", AsyncMock())

    server_inst = MagicMock()
    server_inst.should_exit = False
    serve_started = asyncio.Event()

    async def fake_serve():
        serve_started.set()
        while True:
            await asyncio.sleep(0.01)

    server_inst.serve = fake_serve
    monkeypatch.setattr(main_mod.uvicorn, "Config", lambda app, **kw: MagicMock(app=app))
    monkeypatch.setattr(main_mod.uvicorn, "Server", lambda cfg: server_inst)

    warn_mock = MagicMock()
    monkeypatch.setattr(main_mod.log, "warning", warn_mock)

    async def stop_soon():
        await serve_started.wait()
        await asyncio.sleep(0.02)
        await shutdown_inst._on_signal()

    stop_task = asyncio.create_task(stop_soon())
    try:
        await asyncio.wait_for(main_mod.run(), timeout=5.0)
    finally:
        await asyncio.wait([stop_task], timeout=1.0)

    warn_mock.assert_any_call(
        "Shutdown: Graceful shutdown timed out, forcing final cleanup",
        timeout_sec=shutdown_inst.stop_timeout,
    )
    reg.close_all.assert_awaited_once()
    prod.flush.assert_awaited_once()
    prod.close.assert_awaited_once()
    sm.close.assert_awaited_once()
    owner.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_stop_timeout_cancels_server_task_when_graceful_stop_stalls(monkeypatch):
    settings = MagicMock()
    settings.redis_url = "redis://127.0.0.1:6379/0"
    settings.log_level = "INFO"
    settings.log_format = "json"
    settings.kafka_bootstrap_servers = "127.0.0.1:9092"
    settings.kafka_topic = "t"
    settings.kafka_compression_type = "none"
    settings.kafka_send_timeout_sec = 2.0
    settings.kafka_topic_num_partitions = 1
    settings.kafka_replication_factor = 1
    settings.redis_max_connections = 10
    settings.redis_active_ttl_sec = 3600
    settings.redis_final_ttl_sec = 60
    settings.stop_timeout = 0.05
    settings.http_host = "127.0.0.1"
    settings.http_port = 18086
    settings.http_backlog = 4096
    settings.kafka_startup_timeout_sec = 5.0
    settings.ws_ping_interval = 20.0
    settings.ws_ping_timeout = 21.0
    settings.ws_max_connections = 0
    settings.log_ws_error_frames = False
    settings.redis_ownership_guard_ttl_sec = 30
    settings.redis_sequence_state_key_prefix = "realtime-transcribe-service:expect-transcript-seq-num"
    settings.redis_ownership_guard_key_prefix = "realtime-transcribe-service:conversation-owner"
    settings.ws_ownership_guard_refresh_interval_sec = 5.0

    monkeypatch.setattr(main_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", MagicMock())

    sm = MagicMock(close=AsyncMock())
    monkeypatch.setattr(main_mod, "RedisSequenceStateMachine", lambda **kw: sm)
    owner = MagicMock(close=AsyncMock())
    monkeypatch.setattr(main_mod, "RedisConversationOwnershipGuard", lambda **kw: owner)
    prod = MagicMock(flush=AsyncMock(), close=AsyncMock())
    monkeypatch.setattr(main_mod, "KafkaProducer", lambda **kw: prod)
    monkeypatch.setattr(main_mod, "TwoPhaseOrchestrator", lambda **kw: MagicMock())

    shutdown_inst = main_mod.GracefulShutdown(stop_timeout=settings.stop_timeout)
    monkeypatch.setattr(main_mod, "GracefulShutdown", lambda **kw: shutdown_inst)

    async def slow_close_all(*args, **kwargs):
        await asyncio.sleep(1)

    reg = MagicMock(close_all=AsyncMock(side_effect=slow_close_all))
    monkeypatch.setattr(main_mod, "ConnectionRegistry", lambda: reg)
    monkeypatch.setattr(main_mod, "create_app", lambda **kw: object())
    monkeypatch.setattr(main_mod, "_check_redis", AsyncMock())
    monkeypatch.setattr(main_mod, "_check_kafka", AsyncMock())

    server_inst = MagicMock()
    server_inst.should_exit = False
    serve_started = asyncio.Event()

    async def fake_serve():
        serve_started.set()
        while True:
            await asyncio.sleep(0.01)

    server_inst.serve = fake_serve
    monkeypatch.setattr(main_mod.uvicorn, "Config", lambda app, **kw: MagicMock(app=app))
    monkeypatch.setattr(main_mod.uvicorn, "Server", lambda cfg: server_inst)

    warn_mock = MagicMock()
    monkeypatch.setattr(main_mod.log, "warning", warn_mock)

    async def stop_soon():
        await serve_started.wait()
        await asyncio.sleep(0.02)
        await shutdown_inst._on_signal()

    stop_task = asyncio.create_task(stop_soon())
    try:
        await asyncio.wait_for(main_mod.run(), timeout=5.0)
    finally:
        await asyncio.wait([stop_task], timeout=1.0)

    warn_mock.assert_any_call(
        "Shutdown: Graceful shutdown timed out, forcing final cleanup",
        timeout_sec=shutdown_inst.stop_timeout,
    )
    reg.close_all.assert_awaited_once()
    prod.flush.assert_not_awaited()
    prod.close.assert_awaited_once()
    sm.close.assert_awaited_once()
    owner.close.assert_awaited_once()


def test_main_sync_entry_invokes_asyncio_run(monkeypatch):
    seen = []

    def capture_run(coro):
        seen.append(coro)
        coro.close()  # Avoid "coroutine was never awaited".
        return None

    monkeypatch.setattr(main_mod.asyncio, "run", capture_run)
    main_mod.main()
    assert seen


def test_main_catches_runtime_error_and_exits(monkeypatch):
    def boom(coro):
        coro.close()
        raise RuntimeError("port in use")

    monkeypatch.setattr(main_mod.asyncio, "run", boom)
    with pytest.raises(SystemExit) as ei:
        main_mod.main()
    assert ei.value.code == 1


def test_main_catches_validation_error_and_exits(monkeypatch, capsys):
    def boom(coro):
        coro.close()
        _settings(_env_file=None, app_env="deployed")

    monkeypatch.setattr(main_mod.asyncio, "run", boom)
    with pytest.raises(SystemExit) as ei:
        main_mod.main()
    assert ei.value.code == 1
    assert "Configuration invalid" in capsys.readouterr().err


def test_main_catches_keyboard_interrupt(monkeypatch):
    def interrupted(coro):
        coro.close()
        raise KeyboardInterrupt

    monkeypatch.setattr(main_mod.asyncio, "run", interrupted)
    main_mod.main()

@pytest.mark.asyncio
async def test_run_propagates_exception(monkeypatch):
    settings = MagicMock()
    settings.redis_url = "redis://127.0.0.1:6379/0"
    settings.log_level = "INFO"
    settings.log_format = "json"
    settings.kafka_bootstrap_servers = "127.0.0.1:9092"
    settings.kafka_topic = "t"
    settings.kafka_compression_type = "none"
    settings.kafka_send_timeout_sec = 2.0
    settings.kafka_topic_num_partitions = 1
    settings.kafka_replication_factor = 1
    settings.redis_max_connections = 10
    settings.redis_active_ttl_sec = 3600
    settings.redis_final_ttl_sec = 60
    settings.stop_timeout = 120
    settings.http_host = "127.0.0.1"
    settings.http_port = 18081
    settings.kafka_startup_timeout_sec = 5.0
    settings.redis_ownership_guard_ttl_sec = 30
    settings.redis_sequence_state_key_prefix = "realtime-transcribe-service:expect-transcript-seq-num"
    settings.redis_ownership_guard_key_prefix = "realtime-transcribe-service:conversation-owner"
    settings.ws_ownership_guard_refresh_interval_sec = 5.0

    monkeypatch.setattr(main_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", MagicMock())
    monkeypatch.setattr(
        main_mod, "RedisSequenceStateMachine", lambda **kw: MagicMock(close=AsyncMock())
    )
    monkeypatch.setattr(
        main_mod,
        "RedisConversationOwnershipGuard",
        lambda **kw: MagicMock(close=AsyncMock()),
    )
    monkeypatch.setattr(main_mod, "KafkaProducer", lambda **kw: MagicMock(flush=AsyncMock(), close=AsyncMock()))
    monkeypatch.setattr(main_mod, "TwoPhaseOrchestrator", lambda **kw: MagicMock())

    shutdown_inst = main_mod.GracefulShutdown(stop_timeout=1)
    monkeypatch.setattr(main_mod, "GracefulShutdown", lambda **kw: shutdown_inst)

    reg = MagicMock()
    reg.close_all = AsyncMock(side_effect=RuntimeError("close_all boom"))
    monkeypatch.setattr(main_mod, "ConnectionRegistry", lambda: reg)
    monkeypatch.setattr(main_mod, "create_app", lambda **kw: object())
    monkeypatch.setattr(main_mod, "_check_redis", AsyncMock())
    monkeypatch.setattr(main_mod, "_check_kafka", AsyncMock())

    server_inst = MagicMock()

    async def serve_boom():
        await shutdown_inst._on_signal()
        await asyncio.sleep(0)

    server_inst.serve = serve_boom
    server_inst.should_exit = False
    monkeypatch.setattr(main_mod.uvicorn, "Config", lambda app, **kw: MagicMock(app=app))
    monkeypatch.setattr(main_mod.uvicorn, "Server", lambda cfg: server_inst)

    with pytest.raises(RuntimeError, match="close_all boom"):
        await main_mod.run()


@pytest.mark.asyncio
async def test_run_port_conflict_system_exit(monkeypatch):
    """If ``server.serve()`` raises ``SystemExit`` because the port is in use, it is converted to ``RuntimeError``."""
    settings = MagicMock()
    settings.redis_url = "redis://127.0.0.1:6379/0"
    settings.log_level = "INFO"
    settings.log_format = "json"
    settings.kafka_bootstrap_servers = "127.0.0.1:9092"
    settings.kafka_topic = "t"
    settings.kafka_compression_type = "none"
    settings.kafka_send_timeout_sec = 2.0
    settings.kafka_topic_num_partitions = 1
    settings.kafka_replication_factor = 1
    settings.redis_max_connections = 10
    settings.redis_active_ttl_sec = 3600
    settings.redis_final_ttl_sec = 60
    settings.stop_timeout = 120
    settings.http_host = "127.0.0.1"
    settings.http_port = 18082
    settings.kafka_startup_timeout_sec = 5.0
    settings.redis_ownership_guard_ttl_sec = 30
    settings.redis_sequence_state_key_prefix = "realtime-transcribe-service:expect-transcript-seq-num"
    settings.redis_ownership_guard_key_prefix = "realtime-transcribe-service:conversation-owner"
    settings.ws_ownership_guard_refresh_interval_sec = 5.0

    monkeypatch.setattr(main_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", MagicMock())
    sm = MagicMock(close=AsyncMock())
    monkeypatch.setattr(main_mod, "RedisSequenceStateMachine", lambda **kw: sm)
    owner = MagicMock(close=AsyncMock())
    monkeypatch.setattr(main_mod, "RedisConversationOwnershipGuard", lambda **kw: owner)
    prod = MagicMock(flush=AsyncMock(), close=AsyncMock())
    monkeypatch.setattr(main_mod, "KafkaProducer", lambda **kw: prod)
    monkeypatch.setattr(main_mod, "TwoPhaseOrchestrator", lambda **kw: MagicMock())
    from realtime_transcribe_service.shutdown.graceful import GracefulShutdown as _GS
    shutdown_inst = _GS(stop_timeout=1)
    monkeypatch.setattr(main_mod, "GracefulShutdown", lambda **kw: shutdown_inst)
    monkeypatch.setattr(main_mod, "ConnectionRegistry", lambda: MagicMock(close_all=AsyncMock()))
    monkeypatch.setattr(main_mod, "create_app", lambda **kw: object())
    monkeypatch.setattr(main_mod, "_check_redis", AsyncMock())
    monkeypatch.setattr(main_mod, "_check_kafka", AsyncMock())

    server_inst = MagicMock()

    async def serve_exit():
        raise SystemExit(1)

    server_inst.serve = serve_exit
    server_inst.should_exit = False
    monkeypatch.setattr(main_mod.uvicorn, "Config", lambda app, **kw: MagicMock(app=app))
    monkeypatch.setattr(main_mod.uvicorn, "Server", lambda cfg: server_inst)

    with pytest.raises(RuntimeError, match="Uvicorn"):
        await main_mod.run()

    prod.close.assert_awaited()
    sm.close.assert_awaited()

