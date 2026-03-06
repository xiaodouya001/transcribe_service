"""IdentityCleaner - pass through raw only."""

from asr_ingest.connector.base import TranscriptionEvent


class IdentityCleaner:
    """Identity cleaner: returns raw only, no cleaned extraction."""

    def clean(self, raw: dict, event: TranscriptionEvent) -> dict:
        """Return dict with raw only."""
        return {"raw": raw}
