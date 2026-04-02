"""Runtime wiring helpers for handshake authentication."""

from __future__ import annotations

from realtime_transcribe_service.auth.jwt_bearer import JwtBearerAuthBackend
from realtime_transcribe_service.auth.protocols import HandshakeAuthBackend
from realtime_transcribe_service.config.settings import Settings


def create_auth_backend(settings: Settings) -> HandshakeAuthBackend | None:
    """Create the configured handshake authentication backend, if enabled."""
    if settings.auth_enabled is not True:
        return None
    return JwtBearerAuthBackend(
        signing_material=settings.auth_jwt_signing_material or "",
        algorithm=settings.auth_jwt_algorithm,
    )
