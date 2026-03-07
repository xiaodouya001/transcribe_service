"""Local demo: start mock server + pipeline. Frontend injects JSON, pipeline consumes."""

import asyncio
import os
import sys
from pathlib import Path

# Ensure project root in path
_project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_project_root))

# Load .env so pipeline uses same Redis/Kafka config
try:
    from dotenv import load_dotenv
    load_dotenv(_project_root / ".env")
except ImportError:
    pass

# Point pipeline to mock server (run_local only)
os.environ["FANOLAB_URL"] = "http://127.0.0.1:8765/sse"
os.environ["MODE"] = "sse"
os.environ["SSE_READ_TIMEOUT"] = "130"

async def _run_mock() -> None:
    from asr_ingest.demo.mock_server import run
    await run(host="127.0.0.1", port=8765)


async def _run_pipeline() -> None:
    from asr_ingest.main import run_pipeline
    await run_pipeline()


async def main() -> None:
    from config.logging_config import configure_logging
    configure_logging(format="console")
    # Redis/Kafka 启动校验由 run_pipeline 统一处理，失败会输出明确错误并退出
    server_task = asyncio.create_task(_run_mock())
    await asyncio.sleep(0.5)
    print("Local demo: http://127.0.0.1:8765/  (inject JSON, watch console)")
    try:
        await _run_pipeline()
    finally:
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
