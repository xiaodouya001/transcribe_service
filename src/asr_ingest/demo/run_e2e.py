"""One-command E2E Demo: start mock server, run pipeline."""

import asyncio
import os
import sys
from pathlib import Path

# Ensure project root in path
_project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_project_root))

# Force demo mode and point to mock server
os.environ["DEMO_MODE"] = "true"
os.environ["FANOLAB_URL"] = "http://127.0.0.1:8765/sse"
os.environ["MODE"] = "sse"
os.environ["CLEANER_MODE"] = "identity"
os.environ["dedup_key_parts"] = "session_id,processing_id"

async def _run_mock_server() -> None:
    """Run mock server in background."""
    from asr_ingest.demo.mock_server import run_server

    await run_server(host="127.0.0.1", port=8765)


async def _run_pipeline() -> None:
    """Run the relay pipeline (import after env is set)."""
    from asr_ingest.main import run_pipeline

    await run_pipeline()


async def main() -> None:
    """Start mock server, wait a moment, then run pipeline."""
    server_task = asyncio.create_task(_run_mock_server())
    await asyncio.sleep(0.5)  # Let server start
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
