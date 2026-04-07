"""Tests for AWS Secrets Manager bootstrap when ``APP_ENV=deployed``."""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import pytest

import realtime_transcribe_service.config.secrets_loader as sl


@pytest.fixture(autouse=True)
def _reset_loader_state():
    sl.reset_deployed_secrets_loader_state()
    yield
    sl.reset_deployed_secrets_loader_state()


@pytest.fixture(autouse=True)
def _secrets_loader_isolated_cwd(tmp_path, monkeypatch):
    """Avoid picking up the repo's ``.env`` when testing merge order."""
    monkeypatch.chdir(tmp_path)


@pytest.fixture(autouse=True)
def _restore_os_environ_after_secret_tests():
    """``merge_deployed_secrets_into_environ`` mutates ``os.environ`` broadly; avoid leaking."""
    snapshot = dict(os.environ)
    yield
    os.environ.clear()
    os.environ.update(snapshot)


def test_load_dotenv_layer_skips_invalid_entries():
    with (
        patch("realtime_transcribe_service.config.secrets_loader.os.path.isfile", return_value=True),
        patch.object(
            sl,
            "dotenv_values",
            return_value={
                "KEEP": " x ",
                "NONE_VAL": None,
                "EMPTY": "",
                "SPACE": "   ",
                99: "ignored-non-str-key",
            },
        ),
    ):
        d = sl._load_dotenv_layer()
    assert d == {"KEEP": "x"}


def test_merge_skips_when_app_env_not_deployed(monkeypatch):
    monkeypatch.setenv("APP_ENV", "local")
    monkeypatch.delenv("AWS_SECRETS_MANAGER_SECRET_ID", raising=False)
    sl.merge_deployed_secrets_into_environ()
    with patch("boto3.client", MagicMock()) as client:
        sl.merge_deployed_secrets_into_environ()
    client.assert_not_called()


def test_merge_requires_secret_id_when_deployed(monkeypatch):
    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.delenv("AWS_SECRETS_MANAGER_SECRET_ID", raising=False)
    with pytest.raises(RuntimeError, match="AWS_SECRETS_MANAGER_SECRET_ID"):
        sl.merge_deployed_secrets_into_environ()


def test_merge_loads_json_into_environ(monkeypatch):
    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.setenv("AWS_SECRETS_MANAGER_SECRET_ID", "my/secret")
    monkeypatch.setenv("AWS_REGION", "ap-east-1")

    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps(
            {
                "REDIS_URL": "redis://h:6379/0",
                "KAFKA_BOOTSTRAP_SERVERS": "b:9098",
                "KAFKA_MODE": "aws_msk",
                "KAFKA_AWS_REGION": "ap-east-1",
                "AUTH_ENABLED": True,
                "HTTP_PORT": 8080,
                "LOG_LEVEL": "INFO",
            }
        )
    }

    with patch("boto3.client", return_value=mock_client):
        sl.merge_deployed_secrets_into_environ()

    mock_client.get_secret_value.assert_called_once_with(SecretId="my/secret")
    assert os.environ["REDIS_URL"] == "redis://h:6379/0"
    assert os.environ["KAFKA_BOOTSTRAP_SERVERS"] == "b:9098"
    assert os.environ["KAFKA_MODE"] == "aws_msk"
    assert os.environ["AUTH_ENABLED"] == "true"
    assert os.environ["HTTP_PORT"] == "8080"


def test_merge_idempotent_second_call_no_extra_boto(monkeypatch):
    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.setenv("AWS_SECRETS_MANAGER_SECRET_ID", "s")
    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps({"REDIS_URL": "redis://x/0"})
    }
    with patch("boto3.client", return_value=mock_client):
        sl.merge_deployed_secrets_into_environ()
        sl.merge_deployed_secrets_into_environ()
    assert mock_client.get_secret_value.call_count == 1


def test_merge_rejects_non_object_json(monkeypatch):
    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.setenv("AWS_SECRETS_MANAGER_SECRET_ID", "s")
    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {"SecretString": json.dumps([1, 2])}
    with patch("boto3.client", return_value=mock_client), pytest.raises(
        RuntimeError, match="JSON root must be an object"
    ):
        sl.merge_deployed_secrets_into_environ()


def test_merge_rejects_invalid_json(monkeypatch):
    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.setenv("AWS_SECRETS_MANAGER_SECRET_ID", "s")
    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {"SecretString": "not-json"}
    with patch("boto3.client", return_value=mock_client), pytest.raises(
        RuntimeError, match="must contain a JSON object"
    ):
        sl.merge_deployed_secrets_into_environ()


def test_merge_rejects_nested_secret_value(monkeypatch):
    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.setenv("AWS_SECRETS_MANAGER_SECRET_ID", "s")
    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps({"REDIS_URL": {"nested": 1}})
    }
    with patch("boto3.client", return_value=mock_client), pytest.raises(
        RuntimeError, match="Invalid value for secret key 'REDIS_URL'"
    ):
        sl.merge_deployed_secrets_into_environ()


def test_merge_rejects_missing_secret_string(monkeypatch):
    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.setenv("AWS_SECRETS_MANAGER_SECRET_ID", "s")
    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {}
    with patch("boto3.client", return_value=mock_client), pytest.raises(
        RuntimeError, match="no SecretString"
    ):
        sl.merge_deployed_secrets_into_environ()


def test_merge_propagates_client_error(monkeypatch):
    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.setenv("AWS_SECRETS_MANAGER_SECRET_ID", "s")

    from botocore.exceptions import ClientError

    err = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
        "GetSecretValue",
    )
    mock_client = MagicMock()
    mock_client.get_secret_value.side_effect = err

    with patch("boto3.client", return_value=mock_client), pytest.raises(
        RuntimeError, match="Failed to read AWS Secrets Manager"
    ):
        sl.merge_deployed_secrets_into_environ()


def test_merge_normalizes_key_to_uppercase(monkeypatch):
    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.setenv("AWS_SECRETS_MANAGER_SECRET_ID", "s")
    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps({"redis_url": "redis://z/0"})
    }
    with patch("boto3.client", return_value=mock_client):
        sl.merge_deployed_secrets_into_environ()
    assert os.environ["REDIS_URL"] == "redis://z/0"


def test_merge_null_json_value_becomes_empty_string(monkeypatch):
    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.setenv("AWS_SECRETS_MANAGER_SECRET_ID", "s")
    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps({"EMPTYish": None})
    }
    with patch("boto3.client", return_value=mock_client):
        sl.merge_deployed_secrets_into_environ()
    assert os.environ["EMPTYISH"] == ""


def test_merge_skips_non_string_dict_keys(monkeypatch):
    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.setenv("AWS_SECRETS_MANAGER_SECRET_ID", "s")
    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {"SecretString": "{}"}

    real_loads = json.loads

    def loads_with_int_key(raw: str) -> dict:
        out: dict = real_loads(raw)
        out[999] = "ignored-non-str-keys"
        return out

    with patch("boto3.client", return_value=mock_client), patch(
        "realtime_transcribe_service.config.secrets_loader.json.loads",
        side_effect=loads_with_int_key,
    ):
        sl.merge_deployed_secrets_into_environ()
    assert "999" not in os.environ


def test_merge_skips_whitespace_only_json_keys(monkeypatch):
    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.setenv("AWS_SECRETS_MANAGER_SECRET_ID", "s")
    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps({"KEEP": "1", "   ": "drop"})
    }
    with patch("boto3.client", return_value=mock_client):
        sl.merge_deployed_secrets_into_environ()
    assert os.environ["KEEP"] == "1"
    assert "   " not in os.environ


def test_merge_without_aws_region_uses_default_client(monkeypatch):
    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.setenv("AWS_SECRETS_MANAGER_SECRET_ID", "s")
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps({"REDIS_URL": "redis://z/0"})
    }
    with patch("boto3.client", return_value=mock_client) as client_ctor:
        sl.merge_deployed_secrets_into_environ()
    client_ctor.assert_called_once_with("secretsmanager")


def test_merge_import_error_when_boto3_missing(monkeypatch):
    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.setenv("AWS_SECRETS_MANAGER_SECRET_ID", "s")

    real_import = __import__

    def fake_import(name: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        if name == "boto3":
            raise ImportError("simulated missing boto3")
        return real_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=fake_import), pytest.raises(
        RuntimeError, match="boto3 is required"
    ):
        sl.merge_deployed_secrets_into_environ()


def test_merge_process_env_overrides_dotenv(monkeypatch):
    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.setenv("AWS_SECRETS_MANAGER_SECRET_ID", "s")
    monkeypatch.setenv("AWS_REGION", "ap-east-1")
    monkeypatch.setenv("REDIS_URL", "redis://from-env:6379/0")

    env_file = os.path.join(os.getcwd(), ".env")
    with open(env_file, "w", encoding="utf-8") as f:
        f.write("REDIS_URL=redis://from-dotenv:6379/0\n")

    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps({"OTHER_ONLY": "x"})
    }
    with patch("boto3.client", return_value=mock_client):
        sl.merge_deployed_secrets_into_environ()

    assert os.environ["REDIS_URL"] == "redis://from-env:6379/0"


def test_merge_secret_overrides_process_env(monkeypatch):
    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.setenv("AWS_SECRETS_MANAGER_SECRET_ID", "s")
    monkeypatch.setenv("AWS_REGION", "ap-east-1")
    monkeypatch.setenv("REDIS_URL", "redis://from-env:6379/0")

    env_file = os.path.join(os.getcwd(), ".env")
    with open(env_file, "w", encoding="utf-8") as f:
        f.write("REDIS_URL=redis://from-dotenv:6379/0\n")

    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps({"REDIS_URL": "redis://from-secret:6379/0"})
    }
    with patch("boto3.client", return_value=mock_client):
        sl.merge_deployed_secrets_into_environ()

    assert os.environ["REDIS_URL"] == "redis://from-secret:6379/0"


def test_merge_bootstrap_keys_ignore_secret_body(monkeypatch):
    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.setenv("AWS_SECRETS_MANAGER_SECRET_ID", "s")
    monkeypatch.setenv("AWS_REGION", "ap-east-1")

    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps(
            {
                "APP_ENV": "local",
                "AWS_SECRETS_MANAGER_SECRET_ID": "evil",
                "AWS_REGION": "us-west-2",
            }
        )
    }
    with patch("boto3.client", return_value=mock_client):
        sl.merge_deployed_secrets_into_environ()

    assert os.environ["APP_ENV"] == "deployed"
    assert os.environ["AWS_SECRETS_MANAGER_SECRET_ID"] == "s"
    assert os.environ["AWS_REGION"] == "ap-east-1"


def test_get_settings_deployed_secret_overrides_dotenv(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    bad_env = tmp_path / ".env"
    bad_env.write_text(
        "APP_ENV=deployed\n"
        "REDIS_URL=redis://bad:6379/0\n"
        "REDIS_SEQUENCE_STATE_KEY_PREFIX=bad:realtime-transcribe-service:expect-transcript-seq-num\n"
        "REDIS_OWNERSHIP_GUARD_KEY_PREFIX=bad:realtime-transcribe-service:conversation-owner\n"
        "KAFKA_BOOTSTRAP_SERVERS=bad:9098\n"
        "KAFKA_MODE=aws_msk\n"
        "KAFKA_AWS_REGION=ap-east-1\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.setenv("AWS_SECRETS_MANAGER_SECRET_ID", "s")
    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps(
            {
                "APP_ENV": "deployed",
                "REDIS_URL": "redis://good:6379/0",
                "REDIS_SEQUENCE_STATE_KEY_PREFIX": "good:realtime-transcribe-service:expect-transcript-seq-num",
                "REDIS_OWNERSHIP_GUARD_KEY_PREFIX": "good:realtime-transcribe-service:conversation-owner",
                "KAFKA_BOOTSTRAP_SERVERS": "good:9098",
                "KAFKA_MODE": "aws_msk",
                "KAFKA_AWS_REGION": "ap-east-1",
                "AUTH_ENABLED": False,
            }
        )
    }

    from realtime_transcribe_service.config.settings import get_settings

    get_settings.cache_clear()
    with patch("boto3.client", return_value=mock_client):
        s = get_settings()
    # Secret wins over .env for application keys.
    assert s.redis_url == "redis://good:6379/0"
    assert s.kafka_bootstrap_servers == "good:9098"
    assert s.redis_sequence_state_key_prefix == "good:realtime-transcribe-service:expect-transcript-seq-num"
    assert s.redis_ownership_guard_key_prefix == "good:realtime-transcribe-service:conversation-owner"


def test_get_settings_deployed_uses_bootstrap_region_when_kafka_region_is_omitted(monkeypatch):
    monkeypatch.setenv("APP_ENV", "deployed")
    monkeypatch.setenv("AWS_SECRETS_MANAGER_SECRET_ID", "s")
    monkeypatch.setenv("AWS_REGION", "ap-east-1")
    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps(
            {
                "REDIS_URL": "redis://good:6379/0",
                "REDIS_SEQUENCE_STATE_KEY_PREFIX": "good:realtime-transcribe-service:expect-transcript-seq-num",
                "REDIS_OWNERSHIP_GUARD_KEY_PREFIX": "good:realtime-transcribe-service:conversation-owner",
                "KAFKA_BOOTSTRAP_SERVERS": "good:9098",
                "KAFKA_MODE": "aws_msk",
                "AUTH_ENABLED": False,
            }
        )
    }

    from realtime_transcribe_service.config.settings import get_settings

    get_settings.cache_clear()
    with patch("boto3.client", return_value=mock_client):
        s = get_settings()

    assert s.kafka_aws_region == "ap-east-1"
