"""coverage: main._check_*, main.run"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

import realtime_transcribe_service.main as main_mod
from realtime_transcribe_service.config.settings import Settings
from realtime_transcribe_service.shutdown.graceful import GracefulShutdown


def _settings(**kwargs: Any) -> Settings:
    """Build ``Settings`` without loading repo ``.env`` (pydantic-settings internal kwargs)."""
    return Settings(**kwargs)  # pyright: ignore[reportCallIssue]


def _shared_redis_client() -> MagicMock:
    client = MagicMock()
    client.ping = AsyncMock()
    client.aclose = AsyncMock()
    return client


def _runtime_bundle(**overrides: Any) -> SimpleNamespace:
    shared_redis_client = overrides.pop("shared_redis_client", _shared_redis_client())
    sequence_state_machine = overrides.pop(
        "sequence_state_machine", MagicMock(close=AsyncMock())
    )
    ownership_guard = overrides.pop("ownership_guard", MagicMock(close=AsyncMock()))
    producer = overrides.pop("producer", MagicMock())
    if not isinstance(getattr(producer, "flush", None), AsyncMock):
        producer.flush = AsyncMock()
    if not isinstance(getattr(producer, "close", None), AsyncMock):
        producer.close = AsyncMock()
    shutdown = overrides.pop("shutdown", GracefulShutdown(stop_timeout=1))
    registry = overrides.pop("registry", MagicMock(close_all=AsyncMock()))
    server = overrides.pop("server", MagicMock())
    should_exit = getattr(server, "should_exit", False)
    server.should_exit = False if isinstance(should_exit, MagicMock) else should_exit
    defaults = {
        "shared_redis_client": shared_redis_client,
        "sequence_state_machine": sequence_state_machine,
        "ownership_guard": ownership_guard,
        "producer": producer,
        "orchestrator": overrides.pop("orchestrator", MagicMock()),
        "shutdown": shutdown,
        "registry": registry,
        "auth_backend": overrides.pop("auth_backend", None),
        "app": overrides.pop("app", object()),
        "server": server,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


@pytest.mark.asyncio
async def test_check_redis_success():
    fake = MagicMock()
    fake.ping = AsyncMock()
    fake.aclose = AsyncMock()
    s = _settings(_env_file=None, app_env="local")
    with patch.object(main_mod, "create_shared_redis_client", return_value=fake):
        await main_mod._check_redis(s)
    fake.ping.assert_awaited_once()
    fake.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_check_redis_failure():
    fake = MagicMock()
    fake.ping = AsyncMock(side_effect=RuntimeError("down"))
    fake.aclose = AsyncMock()
    s = _settings(_env_file=None, app_env="local", redis_url="redis://x")
    with patch.object(main_mod, "create_shared_redis_client", return_value=fake):
        with pytest.raises(RuntimeError, match="Redis"):
            await main_mod._check_redis(s)
    fake.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_check_redis_failure_does_not_leak_password():
    fake = MagicMock()
    fake.ping = AsyncMock(side_effect=RuntimeError("auth failed"))
    fake.aclose = AsyncMock()
    s = _settings(
        _env_file=None,
        app_env="local",
        redis_url="redis://127.0.0.1:6379/0",
        redis_password="topsecret",
    )
    with patch.object(main_mod, "create_shared_redis_client", return_value=fake):
        with pytest.raises(RuntimeError) as ei:
            await main_mod._check_redis(s)
    msg = str(ei.value)
    assert "topsecret" not in msg
    fake.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_check_redis_with_injected_client_does_not_close():
    fake = _shared_redis_client()
    s = _settings(_env_file=None, app_env="local", redis_url="redis://x")
    await main_mod._check_redis(s, client=fake)
    fake.ping.assert_awaited_once()
    fake.aclose.assert_not_awaited()


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
    with patch.object(main_mod.log, "exception") as exc_mock, pytest.raises(RuntimeError, match="Kafka"):
        await main_mod._check_kafka(prod, timeout=5.0)
    exc_mock.assert_called_once_with(
        "Startup failed: Kafka unavailable",
        error="RuntimeError('broker')",
        exc_type="RuntimeError",
    )


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
    """Exercise run(): startup checks, serve loop, graceful stop, final cleanup."""
    settings = MagicMock()
    settings.redis_url = "redis://127.0.0.1:6379/0"
    settings.log_level = "INFO"
    settings.log_format = "json"
    settings.kafka_bootstrap_servers = "127.0.0.1:9092"
    settings.http_port = 18080
    settings.kafka_startup_timeout_sec = 5.0

    monkeypatch.setattr(main_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", MagicMock())
    shutdown_inst = GracefulShutdown(stop_timeout=1)
    bundle = _runtime_bundle(shutdown=shutdown_inst)

    redis_check = AsyncMock()
    kafka_check = AsyncMock()
    create_runtime_bundle = AsyncMock(return_value=bundle)
    close_runtime_bundle = AsyncMock()
    monkeypatch.setattr(main_mod, "create_runtime_bundle", create_runtime_bundle)
    monkeypatch.setattr(main_mod, "close_runtime_bundle", close_runtime_bundle)
    monkeypatch.setattr(main_mod, "_check_redis", redis_check)
    monkeypatch.setattr(main_mod, "_check_kafka", kafka_check)

    serve_started = asyncio.Event()

    async def fake_serve():
        serve_started.set()
        while not bundle.server.should_exit:
            await asyncio.sleep(0.01)

    bundle.server.serve = fake_serve

    async def stop_soon():
        await serve_started.wait()
        await asyncio.sleep(0.02)
        await bundle.shutdown._on_signal()

    stop_task = asyncio.create_task(stop_soon())
    try:
        await asyncio.wait_for(main_mod.run(), timeout=5.0)
    finally:
        await asyncio.wait([stop_task], timeout=1.0)

    create_runtime_bundle.assert_awaited_once_with(settings)
    assert redis_check.await_args is not None
    assert redis_check.await_args.kwargs["client"] is bundle.shared_redis_client
    kafka_check.assert_awaited_once_with(bundle.producer, settings.kafka_startup_timeout_sec)
    bundle.registry.close_all.assert_awaited_once()
    bundle.producer.flush.assert_awaited_once()
    close_runtime_bundle.assert_awaited_once_with(bundle)


@pytest.mark.asyncio
async def test_run_graceful_shutdown_order(monkeypatch):
    settings = MagicMock()
    settings.redis_url = "redis://127.0.0.1:6379/0"
    settings.log_level = "INFO"
    settings.log_format = "json"
    settings.kafka_bootstrap_servers = "127.0.0.1:9092"
    settings.http_port = 18083
    settings.kafka_startup_timeout_sec = 5.0

    monkeypatch.setattr(main_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", MagicMock())

    calls: list[str] = []
    shutdown_inst = GracefulShutdown(stop_timeout=1)
    reg = MagicMock()

    async def reg_close_all(*args, **kwargs):
        calls.append("registry.close_all")

    reg.close_all = AsyncMock(side_effect=reg_close_all)
    prod = MagicMock()

    async def prod_flush():
        calls.append("producer.flush")

    prod.flush = AsyncMock(side_effect=prod_flush)
    bundle = _runtime_bundle(shutdown=shutdown_inst, registry=reg, producer=prod)
    monkeypatch.setattr(
        main_mod,
        "create_runtime_bundle",
        AsyncMock(return_value=bundle),
    )
    close_runtime_bundle = AsyncMock(side_effect=lambda _bundle: calls.append("close_runtime_bundle"))
    monkeypatch.setattr(main_mod, "close_runtime_bundle", close_runtime_bundle)
    monkeypatch.setattr(main_mod, "_check_redis", AsyncMock())
    monkeypatch.setattr(main_mod, "_check_kafka", AsyncMock())

    serve_started = asyncio.Event()

    async def fake_serve():
        serve_started.set()
        while not bundle.server.should_exit:
            await asyncio.sleep(0.01)

    bundle.server.serve = fake_serve

    async def stop_soon():
        await serve_started.wait()
        await asyncio.sleep(0.02)
        await bundle.shutdown._on_signal()

    stop_task = asyncio.create_task(stop_soon())
    try:
        await asyncio.wait_for(main_mod.run(), timeout=5.0)
    finally:
        await asyncio.wait([stop_task], timeout=1.0)

    assert calls == [
        "registry.close_all",
        "producer.flush",
        "close_runtime_bundle",
    ]


@pytest.mark.asyncio
async def test_run_startup_checks_are_parallel(monkeypatch):
    settings = MagicMock()
    settings.redis_url = "redis://127.0.0.1:6379/0"
    settings.log_level = "INFO"
    settings.log_format = "json"
    settings.kafka_bootstrap_servers = "127.0.0.1:9092"
    settings.http_port = 18084
    settings.kafka_startup_timeout_sec = 5.0

    monkeypatch.setattr(main_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", MagicMock())
    shutdown_inst = GracefulShutdown(stop_timeout=1)
    bundle = _runtime_bundle(shutdown=shutdown_inst)
    monkeypatch.setattr(
        main_mod,
        "create_runtime_bundle",
        AsyncMock(return_value=bundle),
    )
    monkeypatch.setattr(main_mod, "close_runtime_bundle", AsyncMock())

    order: list[str] = []
    redis_started = asyncio.Event()
    kafka_started = asyncio.Event()
    release_checks = asyncio.Event()

    async def fake_check_redis(_settings: object, *, client: object | None = None) -> None:
        assert client is bundle.shared_redis_client
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

    serve_started = asyncio.Event()

    async def fake_serve():
        serve_started.set()
        while not bundle.server.should_exit:
            await asyncio.sleep(0.01)

    bundle.server.serve = fake_serve

    async def stop_soon():
        await serve_started.wait()
        await asyncio.sleep(0.02)
        await bundle.shutdown._on_signal()

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
    settings.stop_timeout = 0.05
    settings.http_port = 18085
    settings.kafka_startup_timeout_sec = 5.0

    monkeypatch.setattr(main_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", MagicMock())
    shutdown_inst = GracefulShutdown(stop_timeout=settings.stop_timeout)
    bundle = _runtime_bundle(shutdown=shutdown_inst)
    monkeypatch.setattr(
        main_mod,
        "create_runtime_bundle",
        AsyncMock(return_value=bundle),
    )
    close_runtime_bundle = AsyncMock()
    monkeypatch.setattr(main_mod, "close_runtime_bundle", close_runtime_bundle)
    monkeypatch.setattr(main_mod, "_check_redis", AsyncMock())
    monkeypatch.setattr(main_mod, "_check_kafka", AsyncMock())

    serve_started = asyncio.Event()

    async def fake_serve():
        serve_started.set()
        while True:
            await asyncio.sleep(0.01)

    bundle.server.serve = fake_serve

    warn_mock = MagicMock()
    monkeypatch.setattr(main_mod.log, "warning", warn_mock)

    async def stop_soon():
        await serve_started.wait()
        await asyncio.sleep(0.02)
        await bundle.shutdown._on_signal()

    stop_task = asyncio.create_task(stop_soon())
    try:
        await asyncio.wait_for(main_mod.run(), timeout=5.0)
    finally:
        await asyncio.wait([stop_task], timeout=1.0)

    warn_mock.assert_any_call(
        "Shutdown: Graceful shutdown timed out, forcing final cleanup",
        timeout_sec=shutdown_inst.stop_timeout,
    )
    bundle.registry.close_all.assert_awaited_once()
    bundle.producer.flush.assert_awaited_once()
    close_runtime_bundle.assert_awaited_once_with(bundle)


@pytest.mark.asyncio
async def test_run_stop_timeout_cancels_server_task_when_graceful_stop_stalls(monkeypatch):
    settings = MagicMock()
    settings.redis_url = "redis://127.0.0.1:6379/0"
    settings.log_level = "INFO"
    settings.log_format = "json"
    settings.kafka_bootstrap_servers = "127.0.0.1:9092"
    settings.stop_timeout = 0.05
    settings.http_port = 18086
    settings.kafka_startup_timeout_sec = 5.0

    monkeypatch.setattr(main_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", MagicMock())
    shutdown_inst = GracefulShutdown(stop_timeout=settings.stop_timeout)

    async def slow_close_all(*args, **kwargs):
        await asyncio.sleep(1)

    reg = MagicMock(close_all=AsyncMock(side_effect=slow_close_all))
    bundle = _runtime_bundle(shutdown=shutdown_inst, registry=reg)
    monkeypatch.setattr(
        main_mod,
        "create_runtime_bundle",
        AsyncMock(return_value=bundle),
    )
    close_runtime_bundle = AsyncMock()
    monkeypatch.setattr(main_mod, "close_runtime_bundle", close_runtime_bundle)
    monkeypatch.setattr(main_mod, "_check_redis", AsyncMock())
    monkeypatch.setattr(main_mod, "_check_kafka", AsyncMock())

    serve_started = asyncio.Event()

    async def fake_serve():
        serve_started.set()
        while True:
            await asyncio.sleep(0.01)

    bundle.server.serve = fake_serve

    warn_mock = MagicMock()
    monkeypatch.setattr(main_mod.log, "warning", warn_mock)

    async def stop_soon():
        await serve_started.wait()
        await asyncio.sleep(0.02)
        await bundle.shutdown._on_signal()

    stop_task = asyncio.create_task(stop_soon())
    try:
        await asyncio.wait_for(main_mod.run(), timeout=5.0)
    finally:
        await asyncio.wait([stop_task], timeout=1.0)

    warn_mock.assert_any_call(
        "Shutdown: Graceful shutdown timed out, forcing final cleanup",
        timeout_sec=shutdown_inst.stop_timeout,
    )
    bundle.registry.close_all.assert_awaited_once()
    bundle.producer.flush.assert_not_awaited()
    close_runtime_bundle.assert_awaited_once_with(bundle)


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
    settings.http_port = 18081
    settings.kafka_startup_timeout_sec = 5.0

    monkeypatch.setattr(main_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", MagicMock())
    reg = MagicMock()
    reg.close_all = AsyncMock(side_effect=RuntimeError("close_all boom"))
    shutdown_inst = GracefulShutdown(stop_timeout=1)
    bundle = _runtime_bundle(shutdown=shutdown_inst, registry=reg)
    monkeypatch.setattr(
        main_mod,
        "create_runtime_bundle",
        AsyncMock(return_value=bundle),
    )
    close_runtime_bundle = AsyncMock()
    monkeypatch.setattr(main_mod, "close_runtime_bundle", close_runtime_bundle)
    monkeypatch.setattr(main_mod, "_check_redis", AsyncMock())
    monkeypatch.setattr(main_mod, "_check_kafka", AsyncMock())

    async def serve_boom():
        await bundle.shutdown._on_signal()
        await asyncio.sleep(0)

    bundle.server.serve = serve_boom

    with pytest.raises(RuntimeError, match="close_all boom"):
        await main_mod.run()

    close_runtime_bundle.assert_awaited_once_with(bundle)


@pytest.mark.asyncio
async def test_run_port_conflict_system_exit(monkeypatch):
    """If ``server.serve()`` raises ``SystemExit`` because the port is in use, it is converted to ``RuntimeError``."""
    settings = MagicMock()
    settings.redis_url = "redis://127.0.0.1:6379/0"
    settings.log_level = "INFO"
    settings.log_format = "json"
    settings.kafka_bootstrap_servers = "127.0.0.1:9092"
    settings.http_port = 18082
    settings.kafka_startup_timeout_sec = 5.0

    monkeypatch.setattr(main_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", MagicMock())
    bundle = _runtime_bundle()
    monkeypatch.setattr(
        main_mod,
        "create_runtime_bundle",
        AsyncMock(return_value=bundle),
    )
    close_runtime_bundle = AsyncMock()
    monkeypatch.setattr(main_mod, "close_runtime_bundle", close_runtime_bundle)
    monkeypatch.setattr(main_mod, "_check_redis", AsyncMock())
    monkeypatch.setattr(main_mod, "_check_kafka", AsyncMock())

    async def serve_exit():
        raise SystemExit(1)

    bundle.server.serve = serve_exit

    with pytest.raises(RuntimeError, match="Uvicorn"):
        await main_mod.run()

    close_runtime_bundle.assert_awaited_once_with(bundle)

