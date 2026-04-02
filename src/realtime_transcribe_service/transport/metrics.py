"""Transport runtime metrics captured in-process."""

from __future__ import annotations


class RuntimeMetrics:
    """In-process counters and last-value timings for Redis-facing runtime behavior."""

    def __init__(self) -> None:
        self.redis_ready_checks_total = 0
        self.redis_ready_failures_total = 0
        self.redis_ownership_refresh_total = 0
        self.redis_ownership_refresh_failures_total = 0
        self.redis_ownership_refresh_conflicts_total = 0
        self.redis_last_prepare_ms: float | None = None
        self.redis_last_commit_ms: float | None = None

    def observe_orchestrator_timings(self, timings_ms: dict[str, float] | None) -> None:
        if not timings_ms:
            return
        if "prepare_ms" in timings_ms:
            self.redis_last_prepare_ms = timings_ms["prepare_ms"]
        if "redis_commit_ms" in timings_ms:
            self.redis_last_commit_ms = timings_ms["redis_commit_ms"]

    def snapshot(self, active_connections: int) -> dict[str, object]:
        return {
            "active_connections": active_connections,
            "redis_ready_checks_total": self.redis_ready_checks_total,
            "redis_ready_failures_total": self.redis_ready_failures_total,
            "redis_ownership_refresh_total": self.redis_ownership_refresh_total,
            "redis_ownership_refresh_failures_total": self.redis_ownership_refresh_failures_total,
            "redis_ownership_refresh_conflicts_total": self.redis_ownership_refresh_conflicts_total,
            "redis_last_prepare_ms": self.redis_last_prepare_ms,
            "redis_last_commit_ms": self.redis_last_commit_ms,
        }
