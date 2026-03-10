"""Transform layer - data cleaning for Kafka output."""

from transcribe_service.transform.base import CleanerBackend
from transcribe_service.transform.default import DefaultCleaner
from transcribe_service.transform.identity import IdentityCleaner

__all__ = ["CleanerBackend", "DefaultCleaner", "IdentityCleaner", "get_cleaner"]


def get_cleaner(mode: str = "default") -> CleanerBackend:
    """Factory: return cleaner by mode. Future: custom, identity."""
    if mode == "identity":
        from transcribe_service.transform.identity import IdentityCleaner
        return IdentityCleaner()
    return DefaultCleaner()
