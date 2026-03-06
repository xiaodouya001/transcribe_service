"""Auto-verification tests for instrumented E2E demo."""

import asyncio
import sys
from pathlib import Path

import pytest

_root = Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))


def _default_transcripts_path() -> Path:
    """Path to single-file transcripts (scenarios or example fallback)."""
    root = Path(__file__).resolve().parents[1]
    scenario = root / "src" / "asr_ingest" / "demo" / "scenarios" / "single_response_multi_transcriptions" / "transcripts.json"
    example = root / "src" / "asr_ingest" / "demo" / "example" / "transcripts.json"
    return scenario if scenario.exists() else example


@pytest.mark.asyncio
async def test_instrumented_run_no_duplicates() -> None:
    """Instrumented pipeline returns collector with correct structure (no inject)."""
    from asr_ingest.demo.run_e2e_instrumented import run_instrumented

    result = await run_instrumented(inject_duplicates=False, transcripts_path=_default_transcripts_path())

    assert result.error is None, f"Pipeline failed: {result.error}"
    assert result.collector is not None
    assert result.output_path

    # All events should pass dedup (no duplicates injected)
    assert result.collector.pass_count >= 1
    assert result.collector.filtered_count == 0
    assert len(result.collector.redis_events) == len(result.collector.kafka_events)

    # Structure validation
    for ev in result.collector.redis_events:
        assert "key" in ev
        assert "result" in ev
        assert ev["result"] in ("pass", "filtered")
        assert "session_id" in ev
        assert "seq_no" in ev

    for ev in result.collector.kafka_events:
        assert "session_id" in ev
        assert "seq_no" in ev
        assert "transcript" in ev
        assert "role" in ev


@pytest.mark.asyncio
async def test_instrumented_run_with_duplicates() -> None:
    """When inject_duplicates=True, filtered_count > 0."""
    from asr_ingest.demo.run_e2e_instrumented import run_instrumented

    result = await run_instrumented(inject_duplicates=True, transcripts_path=_default_transcripts_path())

    assert result.error is None, f"Pipeline failed: {result.error}"
    # Mock sends payload twice -> duplicate events -> dedup filters second batch
    assert result.collector.filtered_count >= 1
    assert result.collector.pass_count >= 1
    assert len(result.collector.redis_events) > len(result.collector.kafka_events)


@pytest.mark.asyncio
async def test_instrumented_run_websocket() -> None:
    """WebSocket mode: same pipeline logic, verifies WebSocketConnector connectivity."""
    from asr_ingest.demo.run_e2e_instrumented import run_instrumented

    result = await run_instrumented(
        inject_duplicates=False,
        mode="websocket",
        transcripts_path=_default_transcripts_path(),
    )

    assert result.error is None, f"Pipeline failed: {result.error}"
    assert result.collector.pass_count >= 1
    assert len(result.collector.kafka_events) >= 1
    for ev in result.collector.kafka_events:
        assert "session_id" in ev
        assert "seq_no" in ev
        assert "transcript" in ev


@pytest.mark.asyncio
async def test_sse_and_websocket_produce_same_output() -> None:
    """SSE and WebSocket modes produce identical Kafka output for same input."""
    from asr_ingest.demo.run_e2e_instrumented import run_instrumented

    path = _default_transcripts_path()
    if not path.exists():
        pytest.skip("transcripts.json not found")

    sse_result = await run_instrumented(transcripts_path=path, mode="sse")
    ws_result = await run_instrumented(transcripts_path=path, mode="websocket")

    assert sse_result.error is None, f"SSE failed: {sse_result.error}"
    assert ws_result.error is None, f"WebSocket failed: {ws_result.error}"
    assert len(sse_result.collector.kafka_events) == len(ws_result.collector.kafka_events)
    for s, w in zip(sse_result.collector.kafka_events, ws_result.collector.kafka_events):
        assert s["seq_no"] == w["seq_no"]
        assert s["transcript"] == w["transcript"]
        assert s["role"] == w["role"]


@pytest.mark.asyncio
async def test_scenario_single_response_multi_transcriptions() -> None:
    """Scenario: single_response_multi_transcriptions - 1 file, 3 transcripts."""
    from asr_ingest.demo.run_e2e_instrumented import run_instrumented
    from asr_ingest.demo.scenarios import SCENARIOS

    path = SCENARIOS["single_response_multi_transcriptions"]["path"]
    if not path.exists():
        pytest.skip(f"scenarios/single_response_multi_transcriptions not found: {path}")

    result = await run_instrumented(inject_duplicates=False, transcripts_path=path)
    assert result.error is None, f"Pipeline failed: {result.error}"
    assert len(result.collector.kafka_events) == 3
    assert all("raw_payload" in e for e in result.collector.redis_events)
    assert all("raw_payload" in e for e in result.collector.kafka_events)


@pytest.mark.asyncio
async def test_scenario_multi_response_single_transcriptions() -> None:
    """Scenario: multi_response_single_transcriptions - 3 files, 1 transcript each."""
    from asr_ingest.demo.run_e2e_instrumented import run_instrumented
    from asr_ingest.demo.scenarios import SCENARIOS

    path = SCENARIOS["multi_response_single_transcriptions"]["path"].resolve()
    if not path.exists():
        pytest.skip(f"scenarios/multi_response_single_transcriptions not found: {path}")

    result = await run_instrumented(inject_duplicates=False, transcripts_path=path)
    assert result.error is None, f"Pipeline failed: {result.error}"
    assert len(result.collector.kafka_events) == 3


@pytest.mark.asyncio
async def test_scenario_shuffle_multi_response_single_transcriptions() -> None:
    """Scenario: shuffle - events arrive out of order, Kafka receives in seq_no order."""
    from asr_ingest.demo.run_e2e_instrumented import run_instrumented
    from asr_ingest.demo.scenarios import SCENARIOS

    path = SCENARIOS["shuffle_multi_response_single_transcriptions"]["path"].resolve()
    if not path.exists():
        pytest.skip(f"scenarios/shuffle_multi_response_single_transcriptions not found: {path}")

    result = await run_instrumented(
        inject_duplicates=False,
        shuffle_order=True,
        transcripts_path=path,
    )
    assert result.error is None, f"Pipeline failed: {result.error}"
    assert len(result.collector.kafka_events) == 3
    seq_nos = [e["seq_no"] for e in result.collector.kafka_events]
    assert seq_nos == [0, 1, 2]


@pytest.mark.asyncio
async def test_scenario_shuffle_multi_response_multi_transcriptions() -> None:
    """Scenario: shuffle_multi_response_multi_transcriptions - 2 files, 2 transcripts each."""
    from asr_ingest.demo.run_e2e_instrumented import run_instrumented
    from asr_ingest.demo.scenarios import SCENARIOS

    path = SCENARIOS["shuffle_multi_response_multi_transcriptions"]["path"].resolve()
    if not path.exists():
        pytest.skip(f"scenarios/shuffle_multi_response_multi_transcriptions not found: {path}")

    result = await run_instrumented(
        inject_duplicates=False,
        shuffle_order=True,
        transcripts_path=path,
    )
    assert result.error is None, f"Pipeline failed: {result.error}"
    assert len(result.collector.kafka_events) == 4  # 2+2 transcripts


@pytest.mark.asyncio
async def test_collector_pass_filtered_counts() -> None:
    """DemoCollector pass_count and filtered_count are consistent."""
    from asr_ingest.demo.collector import DemoCollector

    c = DemoCollector()
    c.record_dedup("dedup:s1:0", "pass", "s1", 0)
    c.record_dedup("dedup:s1:0", "filtered", "s1", 0)
    c.record_dedup("dedup:s1:1", "pass", "s1", 1)

    assert c.pass_count == 2
    assert c.filtered_count == 1
    assert len(c.redis_events) == 3
