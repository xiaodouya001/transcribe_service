"""TranscriptionEvent and connector interface."""

from dataclasses import dataclass
from typing import AsyncIterator


@dataclass
class TranscriptionEvent:
    """Single transcription segment from Vendor (transcripts.json structure)."""

    session_id: str
    seq_no: int
    transcript: str
    role: str = ""
    created_at: str = ""
    processing_status: str = ""
    processing_id: str = ""

    @classmethod
    def from_vendor_payload(cls, payload: dict) -> list["TranscriptionEvent"]:
        """Parse Vendor response, expand result.transcripts into events."""
        result = payload.get("result") or {}
        call_status = result.get("callStatus") or {}
        session_id = call_status.get("sessionId", "")
        processing_status = result.get("processingStatus", "")
        processing_id = result.get("processingId", "")
        transcripts = result.get("transcripts") or []
        events = []
        for t in transcripts:
            events.append(
                cls(
                    session_id=session_id,
                    seq_no=t.get("seqNo", 0),
                    transcript=t.get("transcript", ""),
                    role=t.get("role", ""),
                    created_at=t.get("createdAt", ""),
                    processing_status=processing_status,
                    processing_id=processing_id,
                )
            )
        return events
