"""Tests for repo-managed Secrets Manager sync automation."""

from __future__ import annotations

import importlib.util
import json
import sys
from io import StringIO
from pathlib import Path
from typing import Any, Mapping
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

_MODULE_PATH = Path(__file__).resolve().parents[1] / "sync_secret.py"
_MODULE_SPEC = importlib.util.spec_from_file_location("repo_secret_sync", _MODULE_PATH)
assert _MODULE_SPEC is not None
assert _MODULE_SPEC.loader is not None
ss = importlib.util.module_from_spec(_MODULE_SPEC)
sys.modules[_MODULE_SPEC.name] = ss
_MODULE_SPEC.loader.exec_module(ss)


def _write_secret_sync_files(
    config_dir: Path,
    *,
    base_aws: dict[str, object] | None = None,
    env_aws: dict[str, object] | None = None,
    base_app: dict[str, object] | None = None,
    env_app: dict[str, object] | None = None,
    secret_app: dict[str, object] | None = None,
    include_secret_file: bool = True,
    include_secret_aws: bool = False,
) -> None:
    base_aws = {"region": "ap-east-1", **(base_aws or {})}
    env_aws = {
        "profile": "realtime-transcribe-service-dev",
        "secret_name": "realtime-transcribe-service/dev/app-config",
        **(env_aws or {}),
    }
    base_app = {
        "KAFKA_MODE": "aws_msk",
        "KAFKA_TOPIC": "AI_STAGING_TRANSCRIPTION",
        "KAFKA_COMPRESSION_TYPE": "zstd",
        "REDIS_SSL_CHECK_HOSTNAME": False,
        "REDIS_MAX_CONNECTIONS": 1600,
        "REDIS_ACTIVE_TTL_SEC": 3600,
        "REDIS_FINAL_TTL_SEC": 30,
        "REDIS_OWNERSHIP_GUARD_TTL_SEC": 30,
        "WS_PING_INTERVAL": 20.0,
        "WS_PING_TIMEOUT": 10.0,
        "WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC": 15.0,
        "AUTH_ENABLED": False,
        "AUTH_JWT_ALGORITHM": "HS256",
        "HTTP_PORT": 8080,
        "HTTP_BACKLOG": 4096,
        "HTTP_ENABLE_DOCS": False,
        "WS_MAX_CONNECTIONS": 600,
        "KAFKA_STARTUP_TIMEOUT_SEC": 30.0,
        "STOP_TIMEOUT": 120.0,
        "LOG_LEVEL": "INFO",
        "LOG_FORMAT": "json",
        "LOG_WS_ERROR_FRAMES": False,
        "LOG_SLOW_MESSAGE_THRESHOLD_MS": 0.0,
        **(base_app or {}),
    }
    env_app = {
        "REDIS_SEQUENCE_STATE_KEY_PREFIX": "dev:realtime-transcribe-service:expect-transcript-seq-num",
        "REDIS_OWNERSHIP_GUARD_KEY_PREFIX": "dev:realtime-transcribe-service:conversation-owner",
        **(env_app or {}),
    }
    if secret_app is None:
        secret_app = {
            "REDIS_URL": "rediss://cache.example:6379/0",
            "KAFKA_BOOTSTRAP_SERVERS": "b-1.example.amazonaws.com:9098",
        }

    (config_dir / "base.toml").write_text(
        _toml_text(base_aws, base_app),
        encoding="utf-8",
    )
    (config_dir / "dev.toml").write_text(
        _toml_text(env_aws, env_app),
        encoding="utf-8",
    )
    if include_secret_file:
        secret_aws = {"profile": "should-not-be-here"} if include_secret_aws else None
        (config_dir / "dev.secrets.toml").write_text(
            _toml_text(secret_aws, secret_app),
            encoding="utf-8",
        )


def _toml_text(aws_values: Mapping[str, object] | None, app_values: Mapping[str, object] | None) -> str:
    lines: list[str] = []
    if aws_values is not None:
        lines.append("[aws]")
        lines.extend(_toml_assignment(key, value) for key, value in aws_values.items())
        lines.append("")
    if app_values is not None:
        lines.append("[app]")
        lines.extend(_toml_assignment(key, value) for key, value in app_values.items())
    return "\n".join(lines) + "\n"


def _toml_assignment(key: str, value: object) -> str:
    if isinstance(value, bool):
        rendered = "true" if value else "false"
    elif isinstance(value, str):
        rendered = json.dumps(value)
    else:
        rendered = str(value)
    return f"{key} = {rendered}"


def test_render_secret_bundle_merges_app_layers_in_order(tmp_path: Path):
    _write_secret_sync_files(
        tmp_path,
        base_app={"LOG_LEVEL": "WARNING"},
        env_app={"LOG_LEVEL": "ERROR"},
        secret_app={
            "REDIS_URL": "rediss://cache.example:6379/0",
            "KAFKA_BOOTSTRAP_SERVERS": "b-1.example.amazonaws.com:9098",
            "LOG_LEVEL": "DEBUG",
        },
    )

    bundle = ss.render_secret_bundle("dev", config_dir=tmp_path)

    assert bundle.app_secret["LOG_LEVEL"] == "DEBUG"
    assert "KAFKA_AWS_REGION" not in bundle.app_secret
    assert bundle.bootstrap_environment == {
        "environment": [
            {"name": "APP_ENV", "value": "deployed"},
            {"name": "AWS_REGION", "value": "ap-east-1"},
            {
                "name": "AWS_SECRETS_MANAGER_SECRET_ID",
                "value": "realtime-transcribe-service/dev/app-config",
            },
        ]
    }


def test_render_secret_bundle_omits_unset_sentinel_values(tmp_path: Path):
    _write_secret_sync_files(
        tmp_path,
        base_app={
            "REDIS_URL": "__UNSET__",
            "REDIS_USERNAME": "__UNSET__",
            "KAFKA_BOOTSTRAP_SERVERS": "__UNSET__",
        },
        secret_app={
            "REDIS_URL": "rediss://cache.example:6379/0",
            "KAFKA_BOOTSTRAP_SERVERS": "b-1.example.amazonaws.com:9098",
        },
    )

    bundle = ss.render_secret_bundle("dev", config_dir=tmp_path)

    assert bundle.app_secret["REDIS_URL"] == "rediss://cache.example:6379/0"
    assert bundle.app_secret["KAFKA_BOOTSTRAP_SERVERS"] == "b-1.example.amazonaws.com:9098"
    assert "REDIS_USERNAME" not in bundle.app_secret


def test_render_secret_bundle_rejects_bootstrap_keys_in_secret_body(tmp_path: Path):
    _write_secret_sync_files(
        tmp_path,
        secret_app={
            "APP_ENV": "deployed",
            "REDIS_URL": "rediss://cache.example:6379/0",
            "KAFKA_BOOTSTRAP_SERVERS": "b-1.example.amazonaws.com:9098",
        },
    )

    with pytest.raises(ValueError, match="Bootstrap keys must not appear in the secret body: APP_ENV"):
        ss.render_secret_bundle("dev", config_dir=tmp_path)


def test_render_secret_bundle_rejects_unknown_app_keys(tmp_path: Path):
    _write_secret_sync_files(
        tmp_path,
        secret_app={
            "REDIS_URL": "rediss://cache.example:6379/0",
            "KAFKA_BOOTSTRAP_SERVERS": "b-1.example.amazonaws.com:9098",
            "NOT_A_REAL_KEY": "boom",
        },
    )

    with pytest.raises(ValueError, match="NOT_A_REAL_KEY"):
        ss.render_secret_bundle("dev", config_dir=tmp_path)


def test_render_secret_bundle_requires_deployed_values(tmp_path: Path):
    _write_secret_sync_files(
        tmp_path,
        secret_app={"KAFKA_BOOTSTRAP_SERVERS": "b-1.example.amazonaws.com:9098"},
    )

    with pytest.raises(ValueError, match="REDIS_URL"):
        ss.render_secret_bundle("dev", config_dir=tmp_path)


def test_render_secret_bundle_requires_jwt_material_when_auth_enabled(tmp_path: Path):
    _write_secret_sync_files(
        tmp_path,
        base_app={"AUTH_ENABLED": True},
    )

    with pytest.raises(ValueError, match="AUTH_JWT_SIGNING_MATERIAL"):
        ss.render_secret_bundle("dev", config_dir=tmp_path)


def test_render_secret_bundle_requires_aws_msk_mode_for_deployed(tmp_path: Path):
    _write_secret_sync_files(
        tmp_path,
        base_app={"KAFKA_MODE": "local"},
    )

    with pytest.raises(ValueError, match="APP_ENV=deployed requires KAFKA_MODE=aws_msk"):
        ss.render_secret_bundle("dev", config_dir=tmp_path)


def test_render_secret_bundle_uses_bootstrap_region_when_kafka_region_not_set(tmp_path: Path):
    _write_secret_sync_files(tmp_path)

    bundle = ss.render_secret_bundle("dev", config_dir=tmp_path)

    assert "KAFKA_AWS_REGION" not in bundle.app_secret


def test_dry_run_does_not_sync_secret(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    _write_secret_sync_files(tmp_path)
    stdout = StringIO()
    stderr = StringIO()
    sync_calls: list[Any] = []
    output_dir = tmp_path / "build"

    def _unexpected_sync(bundle: Any) -> Any:
        sync_calls.append(bundle)
        raise AssertionError("sync_secret_bundle should not run during --dry-run")

    monkeypatch.setattr(ss, "DEFAULT_CONFIG_DIR", tmp_path)
    monkeypatch.setattr(ss, "DEFAULT_OUTPUT_DIR", output_dir)
    monkeypatch.setattr(ss, "sync_secret_bundle", _unexpected_sync)

    exit_code = ss.main(["--env", "dev", "--dry-run"], stdout=stdout, stderr=stderr)

    assert exit_code == 0
    assert sync_calls == []
    assert (output_dir / "dev.bootstrap.json").is_file()
    assert (output_dir / "dev.secret.json").is_file()
    assert f"Wrote ECS bootstrap environment JSON to {output_dir / 'dev.bootstrap.json'}" in stdout.getvalue()
    assert f"Wrote Secrets Manager payload JSON to {output_dir / 'dev.secret.json'}" in stdout.getvalue()
    assert "Dry run: realtime-transcribe-service/dev/app-config" in stdout.getvalue()
    assert stderr.getvalue() == ""


def test_sync_does_not_write_review_files(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    _write_secret_sync_files(config_dir)
    default_output_dir = tmp_path / "build"
    stdout = StringIO()
    stderr = StringIO()

    monkeypatch.setattr(ss, "DEFAULT_CONFIG_DIR", config_dir)
    monkeypatch.setattr(ss, "DEFAULT_OUTPUT_DIR", default_output_dir)
    monkeypatch.setattr(
        ss,
        "sync_secret_bundle",
        lambda bundle: ss.SecretSyncResult(action="updated", secret_name=bundle.aws.secret_name),
    )

    exit_code = ss.main(["--env", "dev", "--sync"], stdout=stdout, stderr=stderr)

    assert exit_code == 0
    assert not (default_output_dir / "dev.bootstrap.json").exists()
    assert not (default_output_dir / "dev.secret.json").exists()
    assert "updated in region ap-east-1" in stdout.getvalue()
    assert "Wrote ECS bootstrap environment JSON" not in stdout.getvalue()
    assert stderr.getvalue() == ""


def test_sync_rejects_output_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    _write_secret_sync_files(config_dir)

    monkeypatch.setattr(ss, "DEFAULT_CONFIG_DIR", config_dir)

    with pytest.raises(SystemExit) as exc_info:
        ss.main(["--env", "dev", "--sync", "--output-dir", str(tmp_path / "review")])

    assert exc_info.value.code == 2


def test_sync_secret_bundle_creates_secret_when_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _write_secret_sync_files(tmp_path)
    bundle = ss.render_secret_bundle("dev", config_dir=tmp_path)
    client = MagicMock()
    client.describe_secret.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "missing"}},
        "DescribeSecret",
    )
    session = MagicMock()
    session.client.return_value = client
    session_ctor = MagicMock(return_value=session)

    monkeypatch.setattr("boto3.Session", session_ctor)

    result = ss.sync_secret_bundle(bundle)

    assert result.action == "created"
    session_ctor.assert_called_once_with(profile_name="realtime-transcribe-service-dev")
    session.client.assert_called_once_with("secretsmanager", region_name="ap-east-1")
    client.create_secret.assert_called_once()
    create_kwargs = client.create_secret.call_args.kwargs
    assert create_kwargs["Name"] == "realtime-transcribe-service/dev/app-config"
    assert create_kwargs["Tags"] == [
        {"Key": "service", "Value": "realtime-transcribe-service"},
        {"Key": "managed-by", "Value": "repo-script"},
        {"Key": "environment", "Value": "dev"},
    ]


def test_sync_secret_bundle_updates_secret_when_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _write_secret_sync_files(tmp_path)
    bundle = ss.render_secret_bundle("dev", config_dir=tmp_path)
    client = MagicMock()
    client.describe_secret.return_value = {"Name": bundle.aws.secret_name}
    session = MagicMock()
    session.client.return_value = client
    session_ctor = MagicMock(return_value=session)

    monkeypatch.setattr("boto3.Session", session_ctor)

    result = ss.sync_secret_bundle(bundle)

    assert result.action == "updated"
    client.put_secret_value.assert_called_once_with(
        SecretId="realtime-transcribe-service/dev/app-config",
        SecretString=bundle.secret_string,
    )
    client.create_secret.assert_not_called()


def test_write_rendered_files_uses_aws_section_values(tmp_path: Path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    _write_secret_sync_files(
        config_dir,
        base_aws={"region": "us-west-2"},
        env_aws={
            "profile": "special-profile",
            "secret_name": "custom/dev/app-config",
        },
    )
    bundle = ss.render_secret_bundle("dev", config_dir=config_dir)
    output_dir = tmp_path / "build"

    outputs = ss.write_rendered_files(output_dir, bundle)

    bootstrap = json.loads(outputs.bootstrap_path.read_text(encoding="utf-8"))
    assert bootstrap == {
        "environment": [
            {"name": "APP_ENV", "value": "deployed"},
            {"name": "AWS_REGION", "value": "us-west-2"},
            {
                "name": "AWS_SECRETS_MANAGER_SECRET_ID",
                "value": "custom/dev/app-config",
            },
        ]
    }
    assert outputs.bootstrap_path.name == "dev.bootstrap.json"
    assert outputs.secret_path.name == "dev.secret.json"
    assert json.loads(outputs.secret_path.read_text(encoding="utf-8")) == bundle.app_secret


def test_main_allows_custom_output_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    _write_secret_sync_files(config_dir)
    output_dir = tmp_path / "review-files"
    stdout = StringIO()
    stderr = StringIO()

    monkeypatch.setattr(ss, "DEFAULT_CONFIG_DIR", config_dir)

    exit_code = ss.main(
        ["--env", "dev", "--dry-run", "--output-dir", str(output_dir)],
        stdout=stdout,
        stderr=stderr,
    )

    assert exit_code == 0
    assert (output_dir / "dev.bootstrap.json").is_file()
    assert (output_dir / "dev.secret.json").is_file()
    assert stderr.getvalue() == ""


def test_secret_file_rejects_aws_section(tmp_path: Path):
    _write_secret_sync_files(tmp_path, include_secret_aws=True)

    with pytest.raises(ValueError, match="unsupported top-level sections: aws"):
        ss.render_secret_bundle("dev", config_dir=tmp_path)
