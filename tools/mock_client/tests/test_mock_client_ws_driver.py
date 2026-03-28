"""Scenario tests for tools.mock_client.ws_driver."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest
import uvicorn

from realtime_transcribe_service.converter.kafka_message_converter import KafkaMessageConverter
from realtime_transcribe_service.orchestrator.two_phase import TwoPhaseOrchestrator
from realtime_transcribe_service.redis.protocols import PrepareOutcome, PrepareResult
from realtime_transcribe_service.shutdown.graceful import GracefulShutdown
from realtime_transcribe_service.transport.websocket_handler import ConnectionRegistry, create_app
from tools.mock_client import ws_driver

pytestmark = [
    pytest.mark.filterwarnings(
        "ignore:websockets\\.legacy is deprecated; see .* upgrade instructions:DeprecationWarning"
    ),
    pytest.mark.filterwarnings(
        "ignore:websockets\\.server\\.WebSocketServerProtocol is deprecated:DeprecationWarning"
    ),
    pytest.mark.filterwarnings(
        "ignore:remove second argument of ws_handler:DeprecationWarning"
    ),
]


@pytest.fixture
async def live_ws_url(unused_tcp_port: int) -> str:
    sm = AsyncMock()
    sm.prepare = AsyncMock(return_value=PrepareOutcome(status=PrepareResult.PRE_CHECK_OK))
    sm.commit = AsyncMock()
    sm.cleanup = AsyncMock()
    producer = AsyncMock()
    producer.send = AsyncMock()

    app = create_app(
        TwoPhaseOrchestrator(sm, producer, message_converter=KafkaMessageConverter()),
        GracefulShutdown(),
        ConnectionRegistry(),
    )
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=unused_tcp_port,
        ws="websockets",
        access_log=False,
        log_level="warning",
        lifespan="off",
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())

    for _ in range(200):
        if getattr(server, "started", False):
            break
        if task.done():
            task.result()
        await asyncio.sleep(0.01)
    else:
        server.should_exit = True
        await task
        pytest.fail("mock scenario test server did not start")

    try:
        yield f"ws://127.0.0.1:{unused_tcp_port}/ws/v1/realtime-transcriptions"
    finally:
        server.should_exit = True
        await task


async def _collect_events(_event_type: str, _data: dict) -> None:
    return None


async def test_mock_client_e06_scenario_preserves_handshake_conversation_id(
    live_ws_url: str,
):
    result = await ws_driver.scenario_d2_schema_error(live_ws_url, _collect_events)

    assert result.passed is True
    error_step = next(step for step in result.steps if step["action"] == "send_bad_schema")
    assert error_step["error_code"] == "E1003"
    assert error_step["conversation_id"].startswith("mock-E06-")
    close_step = next(step for step in result.steps if step["action"] == "verify_close")
    assert close_step["close_code"] == 1008


async def test_mock_client_e07_scenario_covers_non_object_json_and_wrong_type(
    live_ws_url: str,
):
    result = await ws_driver.scenario_e07_wrong_type(live_ws_url, _collect_events)

    assert result.passed is True
    non_object_step = next(step for step in result.steps if step["action"] == "send_non_object_json")
    wrong_type_step = next(step for step in result.steps if step["action"] == "send_wrong_type_field")
    assert non_object_step["error_code"] == "E1004"
    assert wrong_type_step["error_code"] == "E1004"
    assert non_object_step["conversation_id"].startswith("mock-E07-")
    assert wrong_type_step["conversation_id"].startswith("mock-E07-")
    close_codes = [step["close_code"] for step in result.steps if step["action"] == "verify_close"]
    assert close_codes == [1008, 1008]
