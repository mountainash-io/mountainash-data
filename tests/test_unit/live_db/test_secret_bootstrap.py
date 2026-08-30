from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr

from mountainash_secrets import FilesystemSecretStore
from mountainash_settings import clear_secrets_registry

from scripts.live_db_harness.config import (
    TrackingSecretsBackend,
    build_backend_selection,
    load_unresolved_harness,
    walk_secret_scalars,
)
from scripts.live_db_harness.models import HarnessError


SUITE = """
[backends.postgres]
settings_profile = "postgresql"
selector = "postgres"
runnable = true

[backends.sqlite]
settings_profile = "sqlite"
selector = "sqlite"
runnable = true
"""


def _target(provider: str = "local") -> str:
    return f"""
[targets.local]
transport = "direct"
secrets_provider = "{provider}"

[targets.local.backends.postgres.connection]
HOST = "secret:db.host"
PORT = 5432

[targets.local.backends.postgres.auth]
profile = "password"

[targets.local.backends.postgres.auth.values]
USERNAME = "secret:db.username"
PASSWORD = "secret:db.password"

[targets.local.backends.sqlite.connection]
DATABASE = "secret:missing.database"

[targets.local.backends.sqlite.auth]
profile = "none"
"""


def _config(path: Path, target: str = "local") -> Path:
    path.write_text(SUITE + _target(target), encoding="utf-8")
    return path


@pytest.fixture(autouse=True)
def _clear_secret_registry():
    clear_secrets_registry()
    yield
    clear_secrets_registry()


def test_nested_secret_references_resolve_in_dict_list_and_tuple(tmp_path: Path) -> None:
    directory = tmp_path / "secrets"
    directory.mkdir(mode=0o700)
    store = FilesystemSecretStore(directory)
    store.set("db", {"host": "localhost", "username": "postgres", "password": "secret"})
    tracker = TrackingSecretsBackend(
        definition=None,  # type: ignore[arg-type]
        delegate=store,
    )

    assert list(walk_secret_scalars({"a": [SecretStr("one"), ("two",)]})) == ["one", "two"]
    assert tracker.get("db") == {
        "host": "localhost",
        "username": "postgres",
        "password": "secret",
    }
    assert tracker.secret_values == {"localhost", "postgres", "secret"}


def test_missing_secret_names_key_without_plaintext(tmp_path: Path) -> None:
    directory = tmp_path / "secrets"
    directory.mkdir(mode=0o700)
    store = FilesystemSecretStore(directory)
    store.set("db", {"password": "secret"})
    tracker = TrackingSecretsBackend(definition=None, delegate=store)  # type: ignore[arg-type]

    record = tracker.get("db")
    assert record is not None
    assert "names" not in record
    assert tracker.secret_values == {"secret"}


def test_different_definition_cannot_replace_registered_provider(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir(mode=0o700)
    second.mkdir(mode=0o700)
    first_config = tmp_path / "first.toml"
    first_config.write_text(
        SUITE
        + _target("provider")
        + f'\n[secret_providers.provider]\ntype = "filesystem"\npath = "{first}"\n',
        encoding="utf-8",
    )
    FilesystemSecretStore(first).set(
        "db",
        {"host": "localhost", "username": "postgres", "password": "secret"},
    )
    second_config = tmp_path / "second.toml"
    second_config.write_text(
        SUITE
        + _target("provider")
        + f'\n[secret_providers.provider]\ntype = "filesystem"\npath = "{second}"\n',
        encoding="utf-8",
    )

    build_backend_selection(
        load_unresolved_harness((first_config,), selected_target="local", selected_backend="postgres")
    )
    with pytest.raises(HarnessError, match="different definition"):
        build_backend_selection(
            load_unresolved_harness((second_config,), selected_target="local", selected_backend="postgres")
        )


def test_selected_target_chooses_provider_from_two_definitions(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir(mode=0o700)
    second.mkdir(mode=0o700)
    store = FilesystemSecretStore(second)
    store.set("db", {"host": "localhost", "username": "postgres", "password": "secret"})
    path = tmp_path / "config.toml"
    path.write_text(
        SUITE
        + _target("second")
        + f'\n[secret_providers.first]\ntype = "filesystem"\npath = "{first}"\n'
        + f'\n[secret_providers.second]\ntype = "filesystem"\npath = "{second}"\n',
        encoding="utf-8",
    )

    selection = build_backend_selection(
        load_unresolved_harness((path,), selected_target="local", selected_backend="postgres")
    )
    assert selection.auth_profile.PASSWORD.get_secret_value() == "secret"
    assert selection.secret_values == {"localhost", "postgres", "secret"}


def test_selected_backend_ignores_missing_unavailable_backend_secret(tmp_path: Path) -> None:
    directory = tmp_path / "secrets"
    directory.mkdir(mode=0o700)
    FilesystemSecretStore(directory).set(
        "db",
        {"host": "localhost", "username": "postgres", "password": "secret"},
    )
    path = tmp_path / "config.toml"
    path.write_text(
        SUITE + _target("local")
        + f'\n[secret_providers.local]\ntype = "filesystem"\npath = "{directory}"\n',
        encoding="utf-8",
    )
    selection = build_backend_selection(
        load_unresolved_harness((path,), selected_target="local", selected_backend="postgres")
    )

    assert selection.target_name == "local"
    assert selection.backend_name == "postgres"
