"""coverage: main._check_*, main.run"""

from __future__ import annotations

import asyncio
import importlib
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import transcribe_service.main as main_mod


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
    with pytest.raises(RuntimeError, match="Kafka.*超时"):
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
    with pytest.raises(RuntimeError, match="Kafka.*超时"):
        await main_mod._check_kafka(prod, timeout=0.05)
    warn_mock.assert_called_once()


@pytest.mark.asyncio
async def test_check_kafka_other_error():
    prod = MagicMock()
    prod.ensure_ready = AsyncMock(side_effect=RuntimeError("broker"))
    with pytest.raises(RuntimeError, match="Kafka"):
        await main_mod._check_kafka(prod, timeout=5.0)


@pytest.mark.asyncio
async def test_run_graceful_shutdown_path(monkeypatch):
    """Exercise run(): server serve loop, shutdown, registry, producer, state_machine."""
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
    settings.kafka_startup_timeout_sec = 5.0
    settings.ws_ping_interval = 20.0
    settings.ws_ping_timeout = 21.0
    settings.ws_max_connections = 0
    settings.log_ws_error_frames = False

    monkeypatch.setattr(main_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", MagicMock())

    sm = MagicMock()
    sm.close = AsyncMock()
    monkeypatch.setattr(main_mod, "RedisStateMachine", lambda **kw: sm)

    prod = MagicMock()
    prod.flush = AsyncMock()
    prod.close = AsyncMock()
    monkeypatch.setattr(main_mod, "KafkaProducer", lambda **kw: prod)

    orch = MagicMock()
    monkeypatch.setattr(main_mod, "TwoPhaseOrchestrator", lambda **kw: orch)

    shutdown_inst = main_mod.GracefulShutdown(stop_timeout=1)

    def make_shutdown(**kw):
        return shutdown_inst

    monkeypatch.setattr(main_mod, "GracefulShutdown", make_shutdown)

    reg = MagicMock()
    reg.close_all = AsyncMock()
    monkeypatch.setattr(main_mod, "ConnectionRegistry", lambda: reg)

    app_obj = object()
    monkeypatch.setattr(
        main_mod,
        "create_app",
        lambda **kw: app_obj,
    )

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

    reg.close_all.assert_awaited()
    prod.flush.assert_awaited()
    prod.close.assert_awaited()
    sm.close.assert_awaited()


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

    monkeypatch.setattr(main_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", MagicMock())

    calls: list[str] = []

    sm = MagicMock()

    async def sm_close():
        calls.append("state_machine.close")

    sm.close = AsyncMock(side_effect=sm_close)
    monkeypatch.setattr(main_mod, "RedisStateMachine", lambda **kw: sm)

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
        "state_machine.close",
    ]


def test_main_sync_entry_invokes_asyncio_run(monkeypatch):
    seen = []

    def capture_run(coro):
        seen.append(coro)
        coro.close()  # 避免 “coroutine was never awaited”
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


def test_main_catches_keyboard_interrupt(monkeypatch):
    def interrupted(coro):
        coro.close()
        raise KeyboardInterrupt

    monkeypatch.setattr(main_mod.asyncio, "run", interrupted)
    main_mod.main()


def test_bootstrap_inserts_project_root_into_syspath():
    """覆盖 main 模块顶部的 sys.path 注入分支。"""
    root = str(Path(main_mod.__file__).resolve().parents[2])
    saved = sys.path.copy()
    try:
        while root in sys.path:
            sys.path.remove(root)
        importlib.reload(main_mod)
        assert root in sys.path
    finally:
        sys.path[:] = saved


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

    monkeypatch.setattr(main_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", MagicMock())
    monkeypatch.setattr(main_mod, "RedisStateMachine", lambda **kw: MagicMock(close=AsyncMock()))
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
    """server.serve() 因端口占用抛出 SystemExit → RuntimeError。"""
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

    monkeypatch.setattr(main_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", MagicMock())
    sm = MagicMock(close=AsyncMock())
    monkeypatch.setattr(main_mod, "RedisStateMachine", lambda **kw: sm)
    prod = MagicMock(flush=AsyncMock(), close=AsyncMock())
    monkeypatch.setattr(main_mod, "KafkaProducer", lambda **kw: prod)
    monkeypatch.setattr(main_mod, "TwoPhaseOrchestrator", lambda **kw: MagicMock())
    from transcribe_service.shutdown.graceful import GracefulShutdown as _GS
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
