"""Clear Redis Stream and consumer group for clean local demo. Run before run_local."""
import asyncio
import os
import sys
from pathlib import Path

_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_root))

# Load .env so REDIS_URL matches main/run_local
try:
    from dotenv import load_dotenv
    load_dotenv(_root / ".env")
except ImportError:
    pass
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")


async def main() -> None:
    from redis.asyncio import Redis

    stream = "transcription:ingest:buffer"
    group = "transcription:ingest:consumer"
    url = os.environ["REDIS_URL"]
    print(f"Redis: {url}")

    client = Redis.from_url(url, decode_responses=True)
    try:
        await client.xgroup_destroy(stream, group)
        print(f"Destroyed consumer group {group}")
    except Exception as e:
        if "NOGROUP" not in str(e):
            print(f"xgroup_destroy: {e}")
    n = await client.delete(stream)
    if n:
        print(f"Deleted stream {stream}")
    else:
        print(f"Stream {stream} was not found (already empty or deleted)")
    # Verify
    if await client.exists(stream):
        print(f"WARNING: stream still exists after delete - check Redis connection/URL")
    await client.aclose()
    print("Done. Run: python -m transcription_ingest.demo.run_local")


if __name__ == "__main__":
    asyncio.run(main())
