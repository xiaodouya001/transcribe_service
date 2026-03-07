"""Transform layer - data cleaning for Kafka output."""

from transcription_ingest.transform.base import CleanerBackend
from transcription_ingest.transform.default import DefaultCleaner
from transcription_ingest.transform.identity import IdentityCleaner

__all__ = ["CleanerBackend", "DefaultCleaner", "IdentityCleaner", "get_cleaner"]


def get_cleaner(mode: str = "default") -> CleanerBackend:
    """Factory: return cleaner by mode. Future: custom, identity."""
    if mode == "identity":
        from transcription_ingest.transform.identity import IdentityCleaner
        return IdentityCleaner()
    return DefaultCleaner()
