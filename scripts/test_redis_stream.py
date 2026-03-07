"""Diagnostic: verify Redis Stream + consumer works with real Redis."""
import asyncio
import json
import os
import sys
from pathlib import Path

_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_root))

os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")


async def main() -> None:
    from redis.asyncio import Redis

    stream = "asr:ingest:buffer"
    group = "asr:ingest:consumer"
    url = os.environ["REDIS_URL"]

    client = Redis.from_url(url, decode_responses=True)

    # 1. Add a test message
    payload = {
        "success": True,
        "result": {
            "processingId": "test",
            "callStatus": {"sessionId": "test-session"},
            "transcripts": [{"seqNo": 0, "transcript": "hello", "role": "Agent"}],
        },
    }
    msg_id = await client.xadd(stream, {"payload": json.dumps(payload)})
    print(f"Pushed: {msg_id}")

    # 2. Create group if not exist
    try:
        await client.xgroup_create(stream, group, id="0", mkstream=True)
        print("Created consumer group")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            print("Consumer group exists")
        else:
            raise

    # 3. Read with XREADGROUP
    result = await client.xreadgroup(group, "worker1", {stream: ">"}, count=10, block=2000)
    print(f"XREADGROUP result: {result}")

    if result:
        for _sn, messages in result:
            for mid, fields in messages:
                print(f"  msg {mid}: {list(fields.keys())}")
    else:
        print("No messages received - check Redis connection and stream")

    await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
