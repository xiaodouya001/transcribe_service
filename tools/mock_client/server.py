"""Mock Client 后端 — FastAPI 应用，驱动场景 / 压测 / Kafka 回显。

启动方式:
    cd tools/mock_client
    python server.py
    # 浏览器打开 http://127.0.0.1:8088
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

# make project root importable for shared config
_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from config.logging_config import configure_logging
from kafka_viewer import KafkaViewer, purge_topic_messages
from ws_driver import SCENARIOS, Stats, run_load_test

# ---------------------------------------------------------------------------
# 全局状态
# ---------------------------------------------------------------------------

stats = Stats()
_sse_queues: list[asyncio.Queue[str]] = []
_load_stop_event: asyncio.Event | None = None
_load_task: asyncio.Task | None = None
_kafka_viewer: KafkaViewer | None = None
_kafka_forward_task: asyncio.Task | None = None

DEFAULT_WS_URL = "ws://127.0.0.1:8080/ws/v1/realtime-transcriptions"
DEFAULT_KAFKA_BOOTSTRAP = "127.0.0.1:9092"
DEFAULT_KAFKA_TOPIC = "cc.transcript.realtime.v1"


# ---------------------------------------------------------------------------
# SSE 广播
# ---------------------------------------------------------------------------

def _sse_put_drop_oldest(q: asyncio.Queue[str], payload: str) -> None:
    """入队一条 SSE；若满则丢弃队列中最旧的事件再试，避免压测/Kafka 洪峰时 QueueFull 把整个订阅者从列表移除导致 UI 断流。"""
    for _ in range(10_000):
        try:
            q.put_nowait(payload)
            return
        except asyncio.QueueFull:
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                continue


def _broadcast_sse(event_type: str, data: dict[str, Any]) -> None:
    payload = f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"
    for q in _sse_queues:
        _sse_put_drop_oldest(q, payload)


async def _emit(event_type: str, data: dict[str, Any]) -> None:
    _broadcast_sse(event_type, data)


# ---------------------------------------------------------------------------
# 生命周期
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    _pusher = asyncio.create_task(_stats_pusher())
    yield
    _pusher.cancel()

    global _load_stop_event, _load_task, _kafka_forward_task, _kafka_viewer

    if _load_stop_event:
        _load_stop_event.set()
    if _load_task and not _load_task.done():
        _load_task.cancel()
        _load_task = None
    if _kafka_forward_task and not _kafka_forward_task.done():
        _kafka_forward_task.cancel()
        _kafka_forward_task = None
    if _kafka_viewer:
        await _kafka_viewer.stop()
        _kafka_viewer = None

    for q in _sse_queues:
        _sse_put_drop_oldest(q, "")
    _sse_queues.clear()


# ---------------------------------------------------------------------------
# FastAPI 应用
# ---------------------------------------------------------------------------

STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="Mock Client", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = STATIC_DIR / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.get("/api/events")
async def sse(request: Request):
    """SSE 流：推送场景进度、Kafka 消息、统计。"""
    q: asyncio.Queue[str] = asyncio.Queue(maxsize=50_000)
    _sse_queues.append(q)

    # 浏览器刷新会新建 SSE；服务端仍保留上一轮压测的内存统计，若不清理 UI 会一直显示旧数字。
    # 仅在「当前无压测任务」时清空，避免打断正在进行的压测（多标签页也能继续看到实时数据）。
    if not stats.load_running:
        stats.reset()
        _broadcast_sse("stats", stats.snapshot())

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=15)
                    yield msg
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            if q in _sse_queues:
                _sse_queues.remove(q)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/scenario/run")
async def run_scenario(
    name: str = Query(...),
    ws_url: str = Query(DEFAULT_WS_URL),
    n_messages: int = Query(
        5,
        ge=1,
        le=100,
        description=(
            "N-01：发送多少条 SESSION_ONGOING。"
            "N-02：幂等 seq 个数 [0..N)。"
            "N-03：会话业务消息总数（含 SESSION_COMPLETE）。"
            "E-09：乱序第二帧的 seq=max(2,N)。"
            "其余固定错误场景忽略本参数。"
        ),
    ),
):
    """运行单个预定义场景。"""
    if name not in SCENARIOS:
        return {"error": f"未知场景: {name}，可选: {list(SCENARIOS.keys())}"}

    fn = SCENARIOS[name]
    _broadcast_sse("scenario_start", {"scenario": name})

    import inspect
    sig = inspect.signature(fn)
    kwargs: dict[str, Any] = {"ws_url": ws_url, "emit": _emit}
    if "n_messages" in sig.parameters:
        kwargs["n_messages"] = n_messages

    result = await fn(**kwargs)
    summary = {"scenario": result.name, "passed": result.passed, "steps": result.steps}
    _broadcast_sse("scenario_done", summary)
    return summary


@app.post("/api/scenario/run-all")
async def run_all_scenarios(
    ws_url: str = Query(DEFAULT_WS_URL),
    n_messages: int = Query(5, ge=1, le=100),
):
    """顺序运行全部场景。"""
    results = []
    for name in SCENARIOS:
        r = await run_scenario(name=name, ws_url=ws_url, n_messages=n_messages)
        results.append(r)
    return results


@app.post("/api/load/start")
async def load_start(
    ws_url: str = Query(DEFAULT_WS_URL),
    concurrency: int = Query(
        10,
        ge=1,
        le=10_000,
        description="本轮同时进行的会话路数（≈ 峰值在线 WebSocket 数）；一轮共本值这么多路，无额外倍率。",
    ),
    messages_per_conv: int = Query(
        10,
        ge=1,
        le=1000,
        description="单连接发送的业务消息总数（含一条 SESSION_COMPLETE）。",
    ),
    interval_ms: float = Query(20, ge=0),
    ramp_up_ms: float = Query(
        0,
        ge=0,
        description="连接爬坡时间（毫秒）。> 0 时在此时段内均匀启动全部连接，避免瞬间洪峰打满 TCP backlog。0 = 同时发起。",
    ),
):
    """启动并发压测（正常闭环流：若干 `SESSION_ONGOING` + 最后一条 `SESSION_COMPLETE`，不包含错误/边界场景）。"""
    global _load_stop_event, _load_task
    if _load_task and not _load_task.done():
        return {"error": "压测已在运行中"}

    _load_stop_event = asyncio.Event()

    stats.reset()
    stats.load_running = True
    _broadcast_sse("stats", stats.snapshot())

    total_sessions = concurrency
    _broadcast_sse(
        "load_start",
        {
            "concurrency": concurrency,
            "messages_per_conv": messages_per_conv,
            "interval_ms": interval_ms,
            "ramp_up_ms": ramp_up_ms,
            "total_sessions": total_sessions,
            "note": (
                f"本轮共 {total_sessions} 路会话，同时在线约 {concurrency} 条连接"
                f"{'（爬坡 %.0fms）' % ramp_up_ms if ramp_up_ms > 0 else ''}；"
                "全部发完后结束。可先点「停止」不再排队尚未开始的会话。"
            ),
        },
    )

    async def _run():
        try:
            await run_load_test(
                ws_url=ws_url,
                stats=stats,
                emit=_emit,
                concurrency=concurrency,
                messages_per_conv=messages_per_conv,
                interval_ms=interval_ms,
                ramp_up_ms=ramp_up_ms,
                stop_event=_load_stop_event,
            )
        finally:
            stats.finish()

    _load_task = asyncio.create_task(_run())
    return {
        "status": "started",
        "concurrency": concurrency,
        "ramp_up_ms": ramp_up_ms,
        "total_sessions": total_sessions,
    }


@app.post("/api/load/stop")
async def load_stop():
    """停止压测（立即返回，后台收尾）。"""
    global _load_stop_event
    if _load_stop_event:
        _load_stop_event.set()
    return {"status": "stopping"}


@app.get("/api/status")
async def get_status():
    return stats.snapshot()


@app.post("/api/kafka/start")
async def kafka_start(
    bootstrap: str = Query(DEFAULT_KAFKA_BOOTSTRAP),
    topic: str = Query(DEFAULT_KAFKA_TOPIC),
):
    """启动 Kafka 消费者并通过 SSE 推送消息。"""
    global _kafka_viewer, _kafka_forward_task

    if _kafka_forward_task and not _kafka_forward_task.done():
        _kafka_forward_task.cancel()
    if _kafka_viewer:
        await _kafka_viewer.stop()

    def _on_kafka_error(err: str) -> None:
        _broadcast_sse("kafka_error", {"error": err})

    _kafka_viewer = KafkaViewer(
        bootstrap_servers=bootstrap, topic=topic, on_error=_on_kafka_error,
    )
    sid, q = _kafka_viewer.subscribe()

    async def _forward():
        try:
            while True:
                msg = await q.get()
                _broadcast_sse("kafka_message", msg)
        except asyncio.CancelledError:
            pass

    try:
        await _kafka_viewer.start()
    except Exception as e:
        _broadcast_sse("kafka_error", {"error": str(e)})
        return {"status": "kafka_consumer_failed", "error": str(e)}

    _kafka_forward_task = asyncio.create_task(_forward())
    _broadcast_sse("kafka_status", {"connected": True, "topic": topic})
    return {"status": "kafka_consumer_started", "topic": topic}


@app.post("/api/kafka/stop")
async def kafka_stop():
    global _kafka_viewer, _kafka_forward_task
    if _kafka_forward_task and not _kafka_forward_task.done():
        _kafka_forward_task.cancel()
        _kafka_forward_task = None
    if _kafka_viewer:
        await _kafka_viewer.stop()
        _kafka_viewer = None
    _broadcast_sse("kafka_status", {"connected": False})
    return {"status": "kafka_consumer_stopped"}


@app.post("/api/kafka/purge")
async def kafka_purge(
    bootstrap: str = Query(DEFAULT_KAFKA_BOOTSTRAP),
    topic: str = Query(DEFAULT_KAFKA_TOPIC),
    restart_consumer: bool = Query(
        True,
        description="若清空前正在消费同一 bootstrap+topic，是否在清空后自动重新订阅",
    ),
):
    """删除 topic 内已提交的全部消息（DeleteRecords）。执行前会停止本机的 Kafka 消费者，以免位移错乱。"""
    global _kafka_viewer, _kafka_forward_task

    resume_bs: str | None = None
    resume_topic: str | None = None
    if (
        _kafka_viewer
        and restart_consumer
        and _kafka_viewer.bootstrap_servers == bootstrap
        and _kafka_viewer.topic == topic
    ):
        resume_bs, resume_topic = bootstrap, topic

    if _kafka_forward_task and not _kafka_forward_task.done():
        _kafka_forward_task.cancel()
        _kafka_forward_task = None
    if _kafka_viewer:
        await _kafka_viewer.stop()
        _kafka_viewer = None
    _broadcast_sse("kafka_status", {"connected": False})

    try:
        result = await purge_topic_messages(bootstrap, topic)
    except Exception as e:
        return {"status": "error", "error": str(e)}

    if result.get("status") != "ok":
        return result

    _broadcast_sse("kafka_purged", {"topic": topic, "detail": result})

    if resume_bs and resume_topic:
        started = await kafka_start(bootstrap=resume_bs, topic=resume_topic)
        return {"purge": result, **started}

    return {**result, "consumer": "stopped", "hint": "可再次点击「开始消费」查看最新状态"}


# ---------------------------------------------------------------------------
# 统计定时推送
# ---------------------------------------------------------------------------

async def _stats_pusher():
    while True:
        await asyncio.sleep(1)
        _broadcast_sse("stats", stats.snapshot())


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    configure_logging()
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8088,
        reload=False,
        log_level="info",
        timeout_graceful_shutdown=3,
        log_config=None,
    )
