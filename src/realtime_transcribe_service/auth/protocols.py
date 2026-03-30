"""Authentication protocols shared by transport and auth backends."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class AuthPrincipal:
    """Validated authentication result safe to attach to the connection scope."""

    subject: str | None
    claims: dict[str, Any]


class AuthenticationError(Exception):
    """Raised when a bearer token is missing, malformed, invalid, or expired."""

    def __init__(self, details: str) -> None:
        super().__init__(details)
        self.details = details


class HandshakeAuthBackend(Protocol):
    """Authentication backend used by the WebSocket handshake middleware."""

    def authenticate(self, authorization_header: str | None) -> AuthPrincipal:
        """Validate the caller's Authorization header or raise AuthenticationError."""
