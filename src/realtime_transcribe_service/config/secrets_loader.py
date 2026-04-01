"""Load flat JSON configuration from AWS Secrets Manager when ``APP_ENV=deployed``.

Bootstrap (plain task / container environment, not from the secret body):

- ``APP_ENV=deployed`` — triggers this loader before :func:`get_settings` builds :class:`Settings`.
- ``AWS_SECRETS_MANAGER_SECRET_ID`` — secret name or ARN for ``boto3`` Secrets Manager ``get_secret_value``.
- ``AWS_REGION`` or ``AWS_DEFAULT_REGION`` — optional; passed to the Secrets Manager client when set.

The secret string must be a JSON object whose keys are environment variable names (recommended:
``UPPER_SNAKE`` matching ``.env.example``). Values must be ``str``, ``int``, ``float``, ``bool``, or
``null``. Nested objects are rejected.

**Merge order when ``APP_ENV=deployed``** (lowest → highest precedence; later layers override):

1. ``.env`` in the process current working directory (if present)
2. The process environment as it existed when the loader started
3. Key-value pairs from the Secrets Manager JSON

Bootstrap keys (``APP_ENV``, ``AWS_SECRETS_MANAGER_SECRET_ID``, ``AWS_REGION``, ``AWS_DEFAULT_REGION``)
always keep their **initial** process values so the secret body cannot repoint or break the loader.

After merge, :class:`~realtime_transcribe_service.config.settings.Settings` is built with
``_env_file=None`` so pydantic does not read ``.env`` again.

**Local** (``APP_ENV=local``): pydantic-settings default order applies — **process environment
overrides ``.env``** (see :class:`~realtime_transcribe_service.config.settings.Settings`).
"""

from __future__ import annotations

import json
import os
from typing import Any

from dotenv import dotenv_values

from realtime_transcribe_service.constants import (
    APP_ENV_DEPLOYED,
    APP_ENV_VAR,
    AWS_DEFAULT_REGION_ENV,
    AWS_REGION_ENV,
    AWS_SECRETS_MANAGER_SECRET_ID_ENV,
    DEPLOYED_BOOTSTRAP_ENV_KEYS,
)

_DEPLOYED_SECRETS_LOADED: bool = False

DOTENV_FILENAME = ".env"


def reset_deployed_secrets_loader_state() -> None:
    """Clear one-shot load flag (for tests that swap ``APP_ENV`` in-process)."""
    global _DEPLOYED_SECRETS_LOADED
    _DEPLOYED_SECRETS_LOADED = False


def _secret_scalar_to_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    raise ValueError(
        f"Unsupported JSON value type in secret: {type(value).__name__} "
        "(use string, number, boolean, or null)"
    )


def _load_dotenv_layer() -> dict[str, str]:
    """Parse ``.env`` in :data:`os.getcwd` into uppercase keys (same as secret keys)."""
    path = os.path.join(os.getcwd(), DOTENV_FILENAME)
    if not os.path.isfile(path):
        return {}
    raw = dotenv_values(path)
    out: dict[str, str] = {}
    for key, val in raw.items():
        if not isinstance(key, str) or not key.strip():
            continue
        if val is None:
            continue
        s = str(val).strip()
        if not s:
            continue
        out[key.strip().upper()] = s
    return out


def _secret_json_to_env_kv(payload: dict[Any, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, val in payload.items():
        if not isinstance(key, str):
            continue
        name = key.strip()
        if not name:
            continue
        env_key = name.upper()
        try:
            out[env_key] = _secret_scalar_to_str(val)
        except ValueError as exc:
            raise RuntimeError(f"Invalid value for secret key {env_key!r}: {exc}") from exc
    return out


def merge_deployed_secrets_into_environ() -> None:
    """If ``APP_ENV=deployed``, merge ``.env``, process env, and Secret into :data:`os.environ`."""
    global _DEPLOYED_SECRETS_LOADED
    if _DEPLOYED_SECRETS_LOADED:
        return

    initial: dict[str, str] = {str(k): str(v) for k, v in os.environ.items()}
    app_env = initial.get(APP_ENV_VAR, "").strip().lower()
    if app_env != APP_ENV_DEPLOYED:
        return

    secret_id = initial.get(AWS_SECRETS_MANAGER_SECRET_ID_ENV, "").strip()
    if not secret_id:
        raise RuntimeError(
            f"APP_ENV=deployed requires {AWS_SECRETS_MANAGER_SECRET_ID_ENV} "
            "to load configuration from AWS Secrets Manager"
        )

    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required when APP_ENV=deployed (install the runtime dependency)"
        ) from exc

    region = (
        (initial.get(AWS_REGION_ENV) or initial.get(AWS_DEFAULT_REGION_ENV) or "").strip()
    )
    client_kw: dict[str, str] = {}
    if region:
        client_kw["region_name"] = region
    client = boto3.client("secretsmanager", **client_kw)

    try:
        resp = client.get_secret_value(SecretId=secret_id)
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to read AWS Secrets Manager secret {secret_id!r}: {exc}"
        ) from exc

    raw = resp.get("SecretString")
    if raw is None:
        raise RuntimeError(
            f"Secret {secret_id!r} has no SecretString (binary-only secrets are not supported)"
        )

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Secret {secret_id!r} must contain a JSON object (UTF-8 string)"
        ) from exc

    if not isinstance(payload, dict):
        raise RuntimeError(f"Secret {secret_id!r} JSON root must be an object, not a list or scalar")

    secret_kv = _secret_json_to_env_kv(payload)
    dot_layer = _load_dotenv_layer()

    merged: dict[str, str] = {}
    merged.update(dot_layer)
    merged.update(initial)
    merged.update(secret_kv)
    for bk in DEPLOYED_BOOTSTRAP_ENV_KEYS:
        if bk in initial:
            merged[bk] = initial[bk]

    for key, val in merged.items():
        os.environ[key] = val

    _DEPLOYED_SECRETS_LOADED = True
