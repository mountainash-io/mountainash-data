from __future__ import annotations

from pathlib import Path

import pytest

from scripts.live_db_harness.config import (
    BackendSelection,
    build_backend_selection,
    default_config_files,
    load_unresolved_harness,
)
from scripts.live_db_harness.models import HarnessError


SUITE = """
[backends.postgres]
settings_profile = "postgresql"
selector = "postgres"
runnable = true
compose = { profile = "postgres", service = "postgres" }

[backends.sqlite]
settings_profile = "sqlite"
selector = "sqlite"
runnable = true
compose = { profile = "sqlite", service = "sqlite" }
"""

TARGET = """
[targets.local]
transport = "compose"

[targets.local.backends.postgres.connection]
HOST = "127.0.0.1"
PORT = 5432

[targets.local.backends.postgres.auth]
profile = "password"

[targets.local.backends.postgres.auth.values]
USERNAME = "postgres"
PASSWORD = "postgres"

[targets.local.backends.sqlite.connection]
DATABASE = ":memory:"

[targets.local.backends.sqlite.auth]
profile = "none"
"""


def _write_config(path: Path, body: str = SUITE + TARGET) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


def test_default_files_keep_tracked_then_user_order(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    home = tmp_path / "home"
    tracked = repo / "tests/config/live-db.toml"
    user = home / ".config/mountainash-data/live-db.toml"
    tracked.parent.mkdir(parents=True)
    user.parent.mkdir(parents=True)
    tracked.write_text(SUITE, encoding="utf-8")
    user.write_text(TARGET, encoding="utf-8")

    assert default_config_files(repo, home) == (tracked.resolve(), user.resolve())


def test_missing_user_file_is_optional(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    tracked = repo / "tests/config/live-db.toml"
    tracked.parent.mkdir(parents=True)
    tracked.write_text(SUITE, encoding="utf-8")

    assert default_config_files(repo, tmp_path / "home") == (tracked.resolve(),)


def test_missing_tracked_file_fails(tmp_path: Path) -> None:
    with pytest.raises(HarnessError, match="Missing tracked settings file"):
        default_config_files(tmp_path / "repo", tmp_path / "home")


def test_unknown_connection_key_fails_before_profile_construction(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path / "config.toml",
        SUITE
        + TARGET.replace('PORT = 5432', 'PORT = 5432\nUNKNOWN = "bad"'),
    )
    loaded = load_unresolved_harness((path,), selected_target="local", selected_backend="postgres")

    with pytest.raises(HarnessError, match="Unknown connection field: UNKNOWN"):
        build_backend_selection(loaded)


def test_unknown_auth_key_fails_before_profile_construction(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path / "config.toml",
        SUITE
        + TARGET.replace('PASSWORD = "postgres"', 'PASSWORD = "postgres"\nUNKNOWN = "bad"'),
    )
    loaded = load_unresolved_harness((path,), selected_target="local", selected_backend="postgres")

    with pytest.raises(HarnessError, match="Unknown authentication field: UNKNOWN"):
        build_backend_selection(loaded)


def test_backend_registry_builds_known_profile(tmp_path: Path) -> None:
    path = _write_config(tmp_path / "config.toml")
    selection = build_backend_selection(
        load_unresolved_harness((path,), selected_target="local", selected_backend="postgres")
    )

    assert isinstance(selection, BackendSelection)
    assert selection.settings_parameters.get_settings().HOST == "127.0.0.1"
    assert selection.settings_parameters.get_settings().PORT == 5432


def test_auth_registry_builds_known_profile(tmp_path: Path) -> None:
    path = _write_config(tmp_path / "config.toml")
    selection = build_backend_selection(
        load_unresolved_harness((path,), selected_target="local", selected_backend="postgres")
    )
    assert selection.auth_profile.USERNAME == "postgres"
    assert selection.auth_profile.PASSWORD.get_secret_value() == "postgres"


def test_backend_rejects_unsupported_auth_profile_before_secret_resolution(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path / "config.toml",
        (SUITE + TARGET).replace('profile = "password"', 'profile = "token"'),
    )
    loaded = load_unresolved_harness((path,), selected_target="local", selected_backend="postgres")

    with pytest.raises(HarnessError, match="does not support authentication profile"):
        build_backend_selection(loaded)


def test_connection_rejects_inherited_settings_metadata_field(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path / "config.toml",
        SUITE + TARGET.replace('PORT = 5432', 'PORT = 5432\nSETTINGS_CLASS = "bad"'),
    )
    loaded = load_unresolved_harness((path,), selected_target="local", selected_backend="postgres")

    with pytest.raises(HarnessError, match="Unknown connection field: SETTINGS_CLASS"):
        build_backend_selection(loaded)


def test_auth_rejects_inherited_settings_metadata_field(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path / "config.toml",
        SUITE + TARGET.replace('PASSWORD = "postgres"', 'PASSWORD = "postgres"\nSETTINGS_CLASS = "bad"'),
    )
    loaded = load_unresolved_harness((path,), selected_target="local", selected_backend="postgres")

    with pytest.raises(HarnessError, match="Unknown authentication field: SETTINGS_CLASS"):
        build_backend_selection(loaded)

def test_external_target_requires_secrets_provider(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path / "config.toml",
        (SUITE + TARGET).replace('transport = "compose"', 'transport = "direct"'),
    )
    loaded = load_unresolved_harness((path,), selected_target="local", selected_backend="postgres")

    with pytest.raises(HarnessError, match="no secrets provider"):
        build_backend_selection(loaded)


def test_external_target_rejects_plaintext_auth_value(tmp_path: Path) -> None:
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    path = _write_config(
        tmp_path / "config.toml",
        (SUITE + TARGET).replace(
            'transport = "compose"',
            'transport = "direct"\nsecrets_provider = "local"',
        )
        + f'\n[secret_providers.local]\ntype = "filesystem"\npath = "{secrets}"\n',
    )
    loaded = load_unresolved_harness((path,), selected_target="local", selected_backend="postgres")

    with pytest.raises(HarnessError, match="secret:"):
        build_backend_selection(loaded)
