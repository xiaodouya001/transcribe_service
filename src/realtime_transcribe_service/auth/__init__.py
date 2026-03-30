"""Authentication helpers for WebSocket handshake admission."""

from realtime_transcribe_service.auth.jwt_bearer import JwtBearerAuthBackend
from realtime_transcribe_service.auth.protocols import (
    AuthenticationError,
    AuthPrincipal,
    HandshakeAuthBackend,
)

__all__ = [
    "AuthenticationError",
    "AuthPrincipal",
    "HandshakeAuthBackend",
    "JwtBearerAuthBackend",
]
