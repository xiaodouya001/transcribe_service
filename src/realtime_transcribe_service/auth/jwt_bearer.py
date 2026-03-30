"""HS256 Bearer JWT authentication for WebSocket handshake admission."""

from __future__ import annotations

from typing import Literal

import jwt

from realtime_transcribe_service.auth.protocols import (
    AuthenticationError,
    AuthPrincipal,
)


class JwtBearerAuthBackend:
    """Authenticate a caller using ``Authorization: Bearer <JWT>``."""

    def __init__(
        self,
        *,
        signing_material: str,
        algorithm: Literal["HS256"] = "HS256",
    ) -> None:
        self._signing_material = signing_material
        self._algorithm = algorithm

    def authenticate(self, authorization_header: str | None) -> AuthPrincipal:
        token = self._extract_bearer_token(authorization_header)
        try:
            claims = jwt.decode(
                token,
                self._signing_material,
                algorithms=[self._algorithm],
                options={"require": ["exp"]},
            )
        except jwt.ExpiredSignatureError as exc:
            raise AuthenticationError("Bearer token has expired") from exc
        except jwt.InvalidTokenError as exc:
            raise AuthenticationError("Bearer token is invalid") from exc

        subject = claims.get("sub")
        return AuthPrincipal(
            subject=subject if isinstance(subject, str) else None,
            claims=claims,
        )

    @staticmethod
    def _extract_bearer_token(authorization_header: str | None) -> str:
        if authorization_header is None or not authorization_header.strip():
            raise AuthenticationError(
                "Authorization header with Bearer token is required"
            )

        scheme, _, token = authorization_header.strip().partition(" ")
        if scheme.lower() != "bearer" or not token.strip():
            raise AuthenticationError(
                "Authorization header must use the Bearer scheme"
            )
        return token.strip()
