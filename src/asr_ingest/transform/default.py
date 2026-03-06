"""DefaultCleaner - returns raw + cleaned structured fields."""

from asr_ingest.connector.base import TranscriptionEvent


class DefaultCleaner:
    """Default cleaner: returns raw payload plus extracted cleaned fields."""

    def clean(self, raw: dict, event: TranscriptionEvent) -> dict:
        """Return dict with raw and cleaned keys for downstream flexibility."""
        cleaned = {
            "session_id": event.session_id,
            "seq_no": event.seq_no,
            "transcript": event.transcript,
            "role": event.role,
            "created_at": event.created_at,
            "processing_status": event.processing_status,
            "processing_id": event.processing_id,
        }
        return {"raw": raw, "cleaned": cleaned}
