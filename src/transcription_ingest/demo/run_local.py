"""Local demo: 启动 Transcribe Service（Webhook 模式）+ Mock STT 服务，启动时自动 POST Webhook。"""

import asyncio
import os
import sys
from pathlib import Path

# Ensure project root in path
_project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_project_root))

# Load .env so ingest uses same Redis/Kafka config
try:
    from dotenv import load_dotenv
    load_dotenv(_project_root / ".env")
except ImportError:
    pass

# Demo 使用 127.0.0.1 Mock，需允许 localhost
os.environ.setdefault("TRANSCRIBE_SERVICE_SSRF_ALLOW_LOCALHOST", "true")

WEBHOOK_URL = "http://127.0.0.1:8080/webhook/session"  # 127.0.0.1 避免 Windows localhost IPv6 解析问题
MOCK_SSE_BASE = "http://127.0.0.1:8765/sse"
# 多会话并发：启动时 POST 的 session 列表
DEMO_SESSIONS = ["demo-session-1", "demo-session-2", "demo-session-3"]


async def _send_webhook(session_id: str) -> None:
    """POST Webhook 到 Transcribe Service，sse_url 指向 Mock STT（带 session_id）。"""
    import httpx
    sse_url = f"{MOCK_SSE_BASE}?session_id={session_id}"
    payload = {
        "metadata": {"session_id": session_id},
        "ws_url": "",
        "sse_url": sse_url,
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(WEBHOOK_URL, json=payload)
        resp.raise_for_status()
    print(f"Demo: 已向 Webhook POST session_id={session_id}", flush=True)


async def _send_all_webhooks() -> bool:
    """并发 POST 所有 demo sessions，全部成功返回 True。"""
    for sid in DEMO_SESSIONS:
        await _send_webhook(sid)
    return True


async def _run_mock() -> None:
    from transcription_ingest.demo.mock_server import run
    await run(host="127.0.0.1", port=8765)


async def _run_webhook_mode() -> None:
    from transcription_ingest.main import run_webhook_mode
    await run_webhook_mode()


async def main() -> None:
    from config.logging_config import configure_logging
    configure_logging(format="console")

    # 启动 Transcribe Service（Webhook 模式）+ Mock STT 服务
    print("Demo: 启动 Transcribe Service 与 Mock STT…", flush=True)
    webhook_task = asyncio.create_task(_run_webhook_mode())
    mock_task = asyncio.create_task(_run_mock())

    # 等待 Redis/Kafka 校验及 Uvicorn 绑定 8080（Kafka 启动较慢，最多等 60s）
    for i in range(12):
        await asyncio.sleep(5)
        if webhook_task.done() and webhook_task.exception():
            raise webhook_task.exception()
        try:
            await _send_all_webhooks()
            break
        except Exception as e:
            if i < 11:
                print(f"Demo: 等待服务就绪… ({i*5+5}s) [{type(e).__name__}: {e}]", flush=True)
            else:
                print(f"Demo: Webhook POST 失败: {e}", flush=True)

    print(
        f"Local demo: http://127.0.0.1:8765/  (inject JSON，session_id 需为 {DEMO_SESSIONS} 之一)",
        flush=True,
    )

    try:
        await webhook_task
    except asyncio.CancelledError:
        pass
    finally:
        print("Local demo: 正在关闭…", flush=True)
        mock_task.cancel()
        try:
            await mock_task
        except asyncio.CancelledError:
            pass
        print("Local demo: 已退出", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
