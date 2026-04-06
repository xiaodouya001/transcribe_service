"""Render and sync deployed app configuration into AWS Secrets Manager.

Usage:
    python ci-cd/sync_secret.py --env dev --dry-run
    python ci-cd/sync_secret.py --env dev --dry-run --output-dir ci-cd/build/review
    python ci-cd/sync_secret.py --env dev --sync

Parameters:
    --env:
        Target deployment environment. Supported values are ``dev``, ``preprod``, and ``prod``.
    --dry-run:
        Render and validate the secret payload locally, then print the redacted result without
        writing to AWS.
    --sync:
        Create or update the target Secrets Manager secret in AWS.
    --output-dir:
        Optional output directory for local inspection artifacts produced by ``--dry-run``.
        The command writes ``<env>.bootstrap.json`` and ``<env>.secret.json`` there.
        Defaults to ``ci-cd/build`` when omitted.
    --config-dir:
        Optional override for the config directory. Defaults to ``ci-cd/secrets`` and is mainly
        useful for tests or ad-hoc local experiments.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tomllib
from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence, TextIO

from botocore.exceptions import ClientError
from pydantic import ValidationError

_SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

JsonScalar = str | int | float | bool

DEFAULT_CONFIG_DIR = Path(__file__).resolve().parent / "secrets"
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "build"
UNSET_SENTINEL = "__UNSET__"
SERVICE_TAGS = (
    ("service", "realtime-transcribe-service"),
    ("managed-by", "repo-script"),
)
SENSITIVE_VALUE_KEYS = frozenset(
    {
        "AUTH_JWT_SIGNING_MATERIAL",
        "REDIS_PASSWORD",
    }
)
ALLOWED_AWS_KEYS = frozenset({"region", "profile", "secret_name", "kms_key_id"})
REQUIRED_AWS_KEYS = frozenset({"region", "profile", "secret_name"})


@dataclass(frozen=True)
class AwsSecretConfig:
    """Connection details for the target Secrets Manager secret."""

    region: str
    profile: str
    secret_name: str
    kms_key_id: str | None = None


@dataclass(frozen=True)
class RenderedSecretBundle:
    """Fully rendered secret payload plus ECS bootstrap environment."""

    environment: str
    aws: AwsSecretConfig
    app_secret: dict[str, JsonScalar]
    bootstrap_environment: dict[str, list[dict[str, str]]]

    @property
    def secret_string(self) -> str:
        return json.dumps(self.app_secret, indent=2, sort_keys=True)

    @property
    def redacted_app_secret(self) -> dict[str, JsonScalar]:
        return {
            key: _redact_value_for_output(key, value)
            for key, value in sorted(self.app_secret.items())
        }


@dataclass(frozen=True)
class SecretSyncResult:
    """Outcome of a sync operation."""

    action: str
    secret_name: str


@dataclass(frozen=True)
class RenderedFilePaths:
    """Local inspection file paths produced for one rendered bundle."""

    bootstrap_path: Path
    secret_path: Path


@dataclass(frozen=True)
class RuntimeBindings:
    """Lazy bindings to the service runtime contract."""

    app_env_deployed: str
    app_env_var: str
    aws_region_env: str
    aws_secrets_manager_secret_id_env: str
    deployed_bootstrap_env_keys: frozenset[str]
    allowed_secret_app_keys: frozenset[str]
    redact_redis_url_for_logs: Any
    settings_class: Any


def render_secret_bundle(environment: str, *, config_dir: Path = DEFAULT_CONFIG_DIR) -> RenderedSecretBundle:
    """Load, merge, validate, and render one environment's Secrets Manager payload."""

    env_name = environment.strip().lower()
    if not env_name:
        raise ValueError("Environment name must not be empty")

    base_doc = _load_toml_document(config_dir / "base.toml", allow_aws=True)
    env_doc = _load_toml_document(config_dir / f"{env_name}.toml", allow_aws=True)
    secret_doc = _load_toml_document(
        config_dir / f"{env_name}.secrets.toml",
        allow_aws=False,
    )

    aws = _load_aws_config(base_doc.get("aws", {}), env_doc.get("aws", {}))
    app_secret = _merge_app_layers(
        base_doc.get("app", {}),
        env_doc.get("app", {}),
        secret_doc.get("app", {}),
    )
    _validate_secret_app_keys(app_secret)
    _validate_deployed_settings(app_secret, aws_region=aws.region)

    runtime = _runtime_bindings()
    bootstrap_environment = {
        "environment": [
            {"name": runtime.app_env_var, "value": runtime.app_env_deployed},
            {"name": runtime.aws_region_env, "value": aws.region},
            {
                "name": runtime.aws_secrets_manager_secret_id_env,
                "value": aws.secret_name,
            },
        ]
    }
    return RenderedSecretBundle(
        environment=env_name,
        aws=aws,
        app_secret=app_secret,
        bootstrap_environment=bootstrap_environment,
    )


def sync_secret_bundle(bundle: RenderedSecretBundle) -> SecretSyncResult:
    """Create or update the configured Secrets Manager secret."""

    client = _build_secrets_manager_client(bundle.aws)
    payload = bundle.secret_string
    try:
        client.describe_secret(SecretId=bundle.aws.secret_name)
    except ClientError as exc:
        if _client_error_code(exc) != "ResourceNotFoundException":
            raise
        create_kwargs: dict[str, Any] = {
            "Name": bundle.aws.secret_name,
            "SecretString": payload,
            "Tags": [
                {"Key": key, "Value": value}
                for key, value in (*SERVICE_TAGS, ("environment", bundle.environment))
            ],
        }
        if bundle.aws.kms_key_id:
            create_kwargs["KmsKeyId"] = bundle.aws.kms_key_id
        client.create_secret(**create_kwargs)
        return SecretSyncResult(action="created", secret_name=bundle.aws.secret_name)

    client.put_secret_value(
        SecretId=bundle.aws.secret_name,
        SecretString=payload,
    )
    return SecretSyncResult(action="updated", secret_name=bundle.aws.secret_name)


def write_bootstrap_environment(path: Path, bundle: RenderedSecretBundle) -> Path:
    """Write ECS bootstrap environment JSON."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(bundle.bootstrap_environment, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def write_secret_payload(path: Path, bundle: RenderedSecretBundle) -> Path:
    """Write the exact Secrets Manager payload JSON used for sync."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(bundle.secret_string + "\n", encoding="utf-8")
    return path


def write_rendered_files(output_dir: Path, bundle: RenderedSecretBundle) -> RenderedFilePaths:
    """Write local inspection files for both bootstrap and secret payload JSON."""

    bootstrap_path = output_dir / f"{bundle.environment}.bootstrap.json"
    secret_path = output_dir / f"{bundle.environment}.secret.json"
    return RenderedFilePaths(
        bootstrap_path=write_bootstrap_environment(bootstrap_path, bundle),
        secret_path=write_secret_payload(secret_path, bundle),
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """CLI entrypoint for secret rendering and sync."""

    stdout_handle: TextIO = stdout or sys.stdout
    stderr_handle: TextIO = stderr or sys.stderr
    parser = _build_argument_parser()
    args = parser.parse_args(argv)
    if args.sync and args.output_dir is not None:
        parser.error("--output-dir can only be used with --dry-run")

    try:
        bundle = render_secret_bundle(args.env, config_dir=args.config_dir)
        if args.dry_run:
            output_dir = args.output_dir or DEFAULT_OUTPUT_DIR
            output_paths = write_rendered_files(output_dir, bundle)
            print(
                f"Wrote ECS bootstrap environment JSON to {output_paths.bootstrap_path}",
                file=stdout_handle,
            )
            print(
                f"Wrote Secrets Manager payload JSON to {output_paths.secret_path}",
                file=stdout_handle,
            )
            _print_dry_run(bundle, stdout_handle)
            return 0

        result = sync_secret_bundle(bundle)
        print(
            f"Secret {result.secret_name} {result.action} in region {bundle.aws.region}",
            file=stdout_handle,
        )
        runtime = _runtime_bindings()
        print(
            f"ECS bootstrap uses {runtime.aws_secrets_manager_secret_id_env}={bundle.aws.secret_name}",
            file=stdout_handle,
        )
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=stderr_handle)
        return 1


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render and sync ECS application configuration into AWS Secrets Manager.",
        epilog=(
            "Examples:\n"
            "  python ci-cd/sync_secret.py --env dev --dry-run\n"
            "  python ci-cd/sync_secret.py --env dev --dry-run --output-dir ci-cd/build/review\n"
            "  python ci-cd/sync_secret.py --env dev --sync"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--env",
        required=True,
        choices=("dev", "preprod", "prod"),
        help="Target deployment environment to render and sync.",
    )
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--dry-run",
        action="store_true",
        help="Render and validate configuration locally without writing to AWS.",
    )
    mode_group.add_argument(
        "--sync",
        action="store_true",
        help="Create or update the target Secrets Manager secret in AWS.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help=(
            "Directory for dry-run review files. Only valid with --dry-run. "
            "When omitted, dry-run writes <env>.bootstrap.json and <env>.secret.json "
            "under ci-cd/build."
        ),
    )
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=DEFAULT_CONFIG_DIR,
        help="Override the config directory. Defaults to ci-cd/secrets.",
    )
    return parser


def _load_toml_document(path: Path, *, allow_aws: bool) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(
            f"Missing config file: {path}. Create it before running the secret sync."
        )
    with path.open("rb") as handle:
        raw = tomllib.load(handle)
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must contain a TOML table at the document root")

    allowed_sections = {"app", "aws"} if allow_aws else {"app"}
    unknown_sections = sorted(set(raw.keys()) - allowed_sections)
    if unknown_sections:
        raise ValueError(
            f"{path} contains unsupported top-level sections: {', '.join(unknown_sections)}"
        )
    if "app" in raw and not isinstance(raw["app"], dict):
        raise ValueError(f"{path} section [app] must be a TOML table")
    if allow_aws and "aws" in raw and not isinstance(raw["aws"], dict):
        raise ValueError(f"{path} section [aws] must be a TOML table")
    return raw


def _load_aws_config(base_aws: Mapping[str, Any], env_aws: Mapping[str, Any]) -> AwsSecretConfig:
    merged = dict(base_aws)
    merged.update(env_aws)

    unknown_keys = sorted(set(merged.keys()) - ALLOWED_AWS_KEYS)
    if unknown_keys:
        raise ValueError(f"Unsupported [aws] keys: {', '.join(unknown_keys)}")

    missing_keys = sorted(
        key for key in REQUIRED_AWS_KEYS if not _is_non_empty_string(merged.get(key))
    )
    if missing_keys:
        raise ValueError(f"Missing required [aws] keys: {', '.join(missing_keys)}")

    kms_key_id = merged.get("kms_key_id")
    if kms_key_id is not None and not _is_non_empty_string(kms_key_id):
        raise ValueError("[aws].kms_key_id must be a non-empty string when set")

    return AwsSecretConfig(
        region=str(merged["region"]).strip(),
        profile=str(merged["profile"]).strip(),
        secret_name=str(merged["secret_name"]).strip(),
        kms_key_id=str(kms_key_id).strip() if kms_key_id is not None else None,
    )


def _merge_app_layers(*layers: Mapping[str, Any]) -> dict[str, JsonScalar]:
    merged: dict[str, JsonScalar] = {}
    for layer in layers:
        for raw_key, raw_value in layer.items():
            if not isinstance(raw_key, str) or not raw_key.strip():
                raise ValueError("Application config keys must be non-empty strings")
            key = raw_key.strip().upper()
            normalized_value = _normalize_app_value(key, raw_value)
            if normalized_value == UNSET_SENTINEL:
                merged.pop(key, None)
                continue
            merged[key] = normalized_value
    return merged


def _normalize_app_value(key: str, value: Any) -> JsonScalar:
    if isinstance(value, bool):
        return value
    if isinstance(value, (str, int, float)):
        return value
    raise ValueError(
        f"Unsupported value type for app key {key}: {type(value).__name__}. "
        "Use string, integer, float, or boolean values."
    )


def _validate_secret_app_keys(app_secret: Mapping[str, JsonScalar]) -> None:
    runtime = _runtime_bindings()
    reserved = sorted(key for key in app_secret if key in runtime.deployed_bootstrap_env_keys)
    if reserved:
        raise ValueError(
            "Bootstrap keys must not appear in the secret body: " + ", ".join(reserved)
        )

    unknown = sorted(key for key in app_secret if key not in runtime.allowed_secret_app_keys)
    if unknown:
        raise ValueError(
            "Unsupported app configuration keys for Secrets Manager payload: "
            + ", ".join(unknown)
        )


def _validate_deployed_settings(app_secret: Mapping[str, JsonScalar], *, aws_region: str) -> None:
    runtime = _runtime_bindings()
    env_map = {
        runtime.app_env_var: runtime.app_env_deployed,
        runtime.aws_region_env: aws_region,
    }
    env_map.update({key: _json_scalar_to_env_str(value) for key, value in app_secret.items()})
    try:
        with _isolated_environ(env_map):
            runtime.settings_class(_env_file=None)  # pyright: ignore[reportCallIssue]
    except ValidationError as exc:
        raise ValueError(f"Invalid deployed application configuration: {exc}") from exc


def _json_scalar_to_env_str(value: JsonScalar) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


@contextmanager
def _isolated_environ(values: Mapping[str, str]) -> Iterator[None]:
    snapshot = dict(os.environ)
    os.environ.clear()
    os.environ.update(values)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(snapshot)


def _build_secrets_manager_client(aws: AwsSecretConfig) -> Any:
    import boto3

    session = boto3.Session(profile_name=aws.profile)
    return session.client("secretsmanager", region_name=aws.region)


def _client_error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


def _redact_value_for_output(key: str, value: JsonScalar) -> JsonScalar:
    if key in SENSITIVE_VALUE_KEYS:
        return "***"
    if isinstance(value, str) and "redis" in value.lower():
        return _runtime_bindings().redact_redis_url_for_logs(value)
    return value


def _print_dry_run(bundle: RenderedSecretBundle, stdout: TextIO) -> None:
    print(
        f"Dry run: {bundle.aws.secret_name} (profile={bundle.aws.profile}, region={bundle.aws.region})",
        file=stdout,
    )
    print("Secret payload (redacted):", file=stdout)
    print(json.dumps(bundle.redacted_app_secret, indent=2, sort_keys=True), file=stdout)
    print("ECS bootstrap environment:", file=stdout)
    print(json.dumps(bundle.bootstrap_environment, indent=2), file=stdout)


def _is_non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


@lru_cache(maxsize=1)
def _runtime_bindings() -> RuntimeBindings:
    from realtime_transcribe_service.config.logging_config import redact_redis_url_for_logs
    from realtime_transcribe_service.config.settings import Settings
    from realtime_transcribe_service.constants import (
        APP_ENV_DEPLOYED,
        APP_ENV_VAR,
        AWS_REGION_ENV,
        AWS_SECRETS_MANAGER_SECRET_ID_ENV,
        DEPLOYED_BOOTSTRAP_ENV_KEYS,
    )

    return RuntimeBindings(
        app_env_deployed=APP_ENV_DEPLOYED,
        app_env_var=APP_ENV_VAR,
        aws_region_env=AWS_REGION_ENV,
        aws_secrets_manager_secret_id_env=AWS_SECRETS_MANAGER_SECRET_ID_ENV,
        deployed_bootstrap_env_keys=DEPLOYED_BOOTSTRAP_ENV_KEYS,
        allowed_secret_app_keys=frozenset(
            field_name.upper() for field_name in Settings.model_fields.keys()
        )
        - DEPLOYED_BOOTSTRAP_ENV_KEYS,
        redact_redis_url_for_logs=redact_redis_url_for_logs,
        settings_class=Settings,
    )


if __name__ == "__main__":
    raise SystemExit(main())
