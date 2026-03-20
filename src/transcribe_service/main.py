"""主控入口 — 依赖注入与应用生命周期，禁止编写任何业务逻辑。"""

import asyncio
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import uvicorn

from config.logging_config import configure_logging, get_logger
from config.settings import get_settings
from transcribe_service.orchestrator.two_phase import TwoPhaseOrchestrator
from transcribe_service.producer.kafka_producer import KafkaProducer
from transcribe_service.shutdown.graceful import GracefulShutdown
from transcribe_service.state_machine.redis_state import RedisStateMachine
from transcribe_service.schemas.errors import WsCloseCode
from transcribe_service.constants import WS_CLOSE_REASON_GOING_AWAY, WS_PATH
from transcribe_service.transport.websocket_handler import (
    ConnectionRegistry,
    create_app,
)

log = get_logger(__name__)


async def _check_redis(redis_url: str) -> None:
    """验证 Redis 可达。"""
    from redis.asyncio import Redis

    client = Redis.from_url(redis_url, decode_responses=True)
    try:
        await client.ping()
    except Exception as e:
        log.error("启动失败: Redis 不可用", redis_url=redis_url, error=str(e))
        raise RuntimeError(f"Redis 不可用: {redis_url} - {e}") from e
    finally:
        await client.aclose()


async def _check_kafka(producer: KafkaProducer, timeout: float) -> None:
    """验证 Kafka 可达。"""
    try:
        await asyncio.wait_for(
            producer.ensure_ready(),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        log.error("启动失败: Kafka 连接超时", timeout_sec=timeout)
        raise RuntimeError(f"Kafka 不可用: 连接超时 {timeout}s") from None
    except Exception as e:
        log.error("启动失败: Kafka 不可用", error=str(e))
        raise RuntimeError(f"Kafka 不可用: {e}") from e


async def run() -> None:
    """启动 Transcribe Service。"""
    settings = get_settings()
    configure_logging(level=settings.log_level, format=settings.log_format)

    # --- 初始化组件 ---
    state_machine = RedisStateMachine(
        redis_url=settings.redis_url,
        max_connections=settings.redis_max_connections,
        active_ttl_sec=settings.redis_active_ttl_sec,
        final_ttl_sec=settings.redis_final_ttl_sec,
    )
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic,
        compression_type=settings.kafka_compression_type,
        send_timeout_sec=settings.kafka_send_timeout_sec,
        linger_ms=settings.kafka_linger_ms,
        batch_size=settings.kafka_batch_size,
        num_partitions=settings.kafka_topic_num_partitions,
        replication_factor=settings.kafka_replication_factor,
    )
    orchestrator = TwoPhaseOrchestrator(
        state_machine=state_machine,
        producer=producer,
    )
    shutdown = GracefulShutdown(stop_timeout=settings.stop_timeout)
    shutdown.register_signal()
    registry = ConnectionRegistry()

    # --- 启动前检查 ---
    await _check_redis(settings.redis_url)
    await _check_kafka(producer, settings.kafka_startup_timeout_sec)

    # --- 构建 FastAPI 应用 ---
    app = create_app(
        orchestrator=orchestrator,
        shutdown=shutdown,
        registry=registry,
        redis_url=settings.redis_url,
        producer=producer,
        max_connections=settings.ws_max_connections,
    )

    config = uvicorn.Config(
        app,
        host=settings.http_host,
        port=settings.http_port,
        ws="wsproto",
        access_log=True,
        backlog=settings.http_backlog,
    )
    server = uvicorn.Server(config)

    log.info(
        "Transcribe Service: 已启动",
        ws_endpoint=WS_PATH,
        host=settings.http_host,
        port=settings.http_port,
    )

    async def _safe_serve() -> None:
        try:
            await server.serve()
        except SystemExit as e:
            raise RuntimeError(f"Uvicorn 启动失败 (exit code {e.code})") from e

    try:
        server_task = asyncio.create_task(_safe_serve())
        shutdown_task = asyncio.create_task(shutdown.wait_for_shutdown())

        done, _ = await asyncio.wait(
            [server_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if server_task in done:
            shutdown_task.cancel()
            server_task.result()

        # --- 优雅停机 ---
        log.info("Shutdown: 开始优雅停机")
        await registry.close_all(
            code=WsCloseCode.GOING_AWAY, reason=WS_CLOSE_REASON_GOING_AWAY
        )
        await producer.flush()
        server.should_exit = True
        await server_task
    except Exception as e:
        log.exception("运行异常", error=str(e))
        raise
    finally:
        log.info("Shutdown: 释放资源")
        await producer.close()
        await state_machine.close()
        log.info("Transcribe Service: 已安全退出")


def main() -> None:
    """同步入口。"""
    try:
        asyncio.run(run())
    except RuntimeError as e:
        log.error("启动失败", error=str(e))
        sys.exit(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":  # pragma: no cover
    main()
