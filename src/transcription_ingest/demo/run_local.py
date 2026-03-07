"""Local demo: 启动 Mock 服务 + Transcription Ingest。前端注入 JSON，Ingest 消费并写入 Kafka。"""

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

# Point ingest to mock server (run_local only)
os.environ["STT_PROVIDER_URL"] = "http://127.0.0.1:8765/sse"
os.environ["MODE"] = "sse"
os.environ["SSE_READ_TIMEOUT"] = "130"

async def _run_mock() -> None:
    from transcription_ingest.demo.mock_server import run
    await run(host="127.0.0.1", port=8765)


async def _run_ingest() -> None:
    from transcription_ingest.main import run_ingest
    await run_ingest()


async def main() -> None:
    from config.logging_config import configure_logging
    configure_logging(format="console")
    # Redis/Kafka 启动校验由 run_ingest 统一处理，失败会输出明确错误并退出
    server_task = asyncio.create_task(_run_mock())
    await asyncio.sleep(0.5)
    print("Local demo: http://127.0.0.1:8765/  (inject JSON, watch console)")
    try:
        await _run_ingest()
    finally:
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
