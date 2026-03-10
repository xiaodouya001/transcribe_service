"""CleanerBackend protocol - data cleaning/transformation interface."""

from typing import Protocol

from transcribe_service.connector.base import TranscriptionEvent


class CleanerBackend(Protocol):
    """Protocol for data cleaning backends. Extensible for future custom logic."""

    def clean(self, raw: dict, event: TranscriptionEvent) -> dict:
        """Transform raw vendor payload into output for Kafka.

        Args:
            raw: Original vendor JSON payload.
            event: Parsed TranscriptionEvent.

        Returns:
            Dict for Kafka value, e.g. {"raw": raw, "cleaned": {...}}.
        """
        ...
