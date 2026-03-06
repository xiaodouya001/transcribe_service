"""DemoCollector - records dedup and Kafka events for Streamlit visualization."""

from dataclasses import dataclass, field


@dataclass
class DemoCollector:
    """Collects dedup and Kafka events during instrumented pipeline run."""

    redis_events: list[dict] = field(default_factory=list)
    kafka_events: list[dict] = field(default_factory=list)

    def record_dedup(
        self,
        key: str,
        result: str,
        session_id: str,
        seq_no: int,
        arrival_order: int | None = None,
        source_json: dict | None = None,
    ) -> None:
        """Record a dedup check (pass or filtered). arrival_order = 1-based arrival index."""
        # value: pass 时写入 "1" (模拟 Redis SETNX), filtered 时 key 已存在无写入
        value = "1" if result == "pass" else "-"
        ev = {
            "key": key,
            "value": value,
            "result": result,
            "session_id": session_id,
            "seq_no": seq_no,
        }
        if arrival_order is not None:
            ev["arrival_order"] = arrival_order
        if source_json is not None:
            ev["raw_payload"] = source_json
        self.redis_events.append(ev)

    def record_kafka(
        self,
        session_id: str,
        seq_no: int,
        transcript: str,
        role: str = "",
        created_at: str = "",
        processing_status: str = "",
        source_json: dict | None = None,
        **kwargs: object,
    ) -> None:
        """Record a Kafka send payload."""
        payload = {
            "session_id": session_id,
            "seq_no": seq_no,
            "transcript": transcript,
            "role": role,
            "created_at": created_at,
            "processing_status": processing_status,
        }
        payload.update(kwargs)
        if source_json is not None:
            payload["raw_payload"] = source_json
        self.kafka_events.append(payload)

    @property
    def pass_count(self) -> int:
        """Number of dedup pass events."""
        return sum(1 for e in self.redis_events if e.get("result") == "pass")

    @property
    def filtered_count(self) -> int:
        """Number of dedup filtered events."""
        return sum(1 for e in self.redis_events if e.get("result") == "filtered")
