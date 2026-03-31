"""Tests for HS256 bearer JWT handshake authentication."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import jwt
import pytest

from realtime_transcribe_service.auth.jwt_bearer import JwtBearerAuthBackend
from realtime_transcribe_service.auth.protocols import AuthenticationError

SIGNING_MATERIAL = "signing-material-0123456789-material-012345"
WRONG_SIGNING_MATERIAL = "wrong-material-0123456789-material-012345"


def _token(signing_material: str, *, exp_delta_sec: int, sub: str | None = "fano-client") -> str:
    claims: dict[str, object] = {
        "exp": datetime.now(timezone.utc) + timedelta(seconds=exp_delta_sec),
    }
    if sub is not None:
        claims["sub"] = sub
    return jwt.encode(claims, signing_material, algorithm="HS256")


def test_authenticate_rejects_missing_header():
    backend = JwtBearerAuthBackend(signing_material=SIGNING_MATERIAL)

    with pytest.raises(
        AuthenticationError,
        match="Authorization header with Bearer token is required",
    ):
        backend.authenticate(None)


def test_authenticate_rejects_non_bearer_scheme():
    backend = JwtBearerAuthBackend(signing_material=SIGNING_MATERIAL)

    with pytest.raises(
        AuthenticationError,
        match="Authorization header must use the Bearer scheme",
    ):
        backend.authenticate("Token abc")


def test_authenticate_rejects_expired_token():
    backend = JwtBearerAuthBackend(signing_material=SIGNING_MATERIAL)
    token = _token(SIGNING_MATERIAL, exp_delta_sec=-30)

    with pytest.raises(AuthenticationError, match="Bearer token has expired"):
        backend.authenticate(f"Bearer {token}")


def test_authenticate_rejects_invalid_signature():
    backend = JwtBearerAuthBackend(signing_material=SIGNING_MATERIAL)
    token = _token(WRONG_SIGNING_MATERIAL, exp_delta_sec=300)

    with pytest.raises(AuthenticationError, match="Bearer token is invalid"):
        backend.authenticate(f"Bearer {token}")


def test_authenticate_returns_subject_and_claims_for_valid_token():
    backend = JwtBearerAuthBackend(signing_material=SIGNING_MATERIAL)
    token = _token(SIGNING_MATERIAL, exp_delta_sec=300, sub="fano-client-001")

    principal = backend.authenticate(f"Bearer {token}")

    assert principal.subject == "fano-client-001"
    assert principal.claims["sub"] == "fano-client-001"
    assert "exp" in principal.claims


def test_authenticate_allows_missing_subject_and_returns_none():
    backend = JwtBearerAuthBackend(signing_material=SIGNING_MATERIAL)
    token = _token(SIGNING_MATERIAL, exp_delta_sec=300, sub=None)

    principal = backend.authenticate(f"Bearer {token}")

    assert principal.subject is None
    assert "sub" not in principal.claims
