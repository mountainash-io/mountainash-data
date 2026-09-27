from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr

from mountainash_settings import FilesystemBackend

from scripts.live_db_harness import config

from scripts.live_db_harness.config import (
    TrackingSecretsBackend,
    build_backend_selection,
    load_unresolved_harness,
    walk_secret_scalars,
)
from scripts.live_db_harness.models import HarnessError, SecretProviderDefinition


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


def test_nested_secret_references_resolve_in_dict_list_and_tuple(tmp_path: Path) -> None:
    directory = tmp_path / "secrets"
    directory.mkdir(mode=0o700)
    with FilesystemBackend(directory) as store:
        store.set("db", {"host": "localhost", "username": "postgres", "password": "secret"})
        tracker = TrackingSecretsBackend(
            definition=SecretProviderDefinition(type="filesystem", path=directory), delegate=store,
        )
        assert list(walk_secret_scalars({"a": [SecretStr("one"), ("two",)]})) == ["one", "two"]
        assert tracker.get("db") == {
            "host": "localhost", "username": "postgres", "password": "secret",
        }
        assert tracker.secret_values == {"localhost", "postgres", "secret"}


def test_missing_secret_names_key_without_plaintext(tmp_path: Path) -> None:
    directory = tmp_path / "secrets"
    directory.mkdir(mode=0o700)
    with FilesystemBackend(directory) as store:
        store.set("db", {"password": "secret"})
        tracker = TrackingSecretsBackend(
            definition=SecretProviderDefinition(type="filesystem", path=directory), delegate=store,
        )
        record = tracker.get("db")
        assert record is not None
        assert "names" not in record
        assert tracker.secret_values == {"secret"}


def test_same_local_label_is_isolated_in_first_second_first_order(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir(mode=0o700)
    second.mkdir(mode=0o700)
    first_config = tmp_path / "first.toml"
    first_config.write_text(
        SUITE
        + _target("local")
        + f'\n[secret_providers.local]\ntype = "filesystem"\npath = "{first}"\n',
        encoding="utf-8",
    )
    for root, password in [(first, "first-sentinel"), (second, "second-sentinel")]:
        with FilesystemBackend(root) as store:
            store.set("db", {"host": "localhost", "username": "postgres", "password": password})
    second_config = tmp_path / "second.toml"
    second_config.write_text(
        SUITE
        + _target("local")
        + f'\n[secret_providers.local]\ntype = "filesystem"\npath = "{second}"\n',
        encoding="utf-8",
    )

    for path, password in [(first_config, "first-sentinel"), (second_config, "second-sentinel"),
                           (first_config, "first-sentinel")]:
        selection = build_backend_selection(
            load_unresolved_harness((path,), selected_target="local", selected_backend="postgres")
        )
        assert selection.auth_profile.PASSWORD.get_secret_value() == password
        assert selection.secret_values == {"localhost", "postgres", password}
        assert selection.settings_parameters.secret_store is None


def test_selected_target_chooses_provider_from_two_definitions(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir(mode=0o700)
    second.mkdir(mode=0o700)
    with FilesystemBackend(second) as store:
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
    with FilesystemBackend(directory) as store:
        store.set("db", {"host": "localhost", "username": "postgres", "password": "secret"})
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


def test_raw_loading_never_opens_store_or_resolves_unselected_backend(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("raw loading must not construct or read a store")

    monkeypatch.setattr(FilesystemBackend, "__init__", forbidden)
    monkeypatch.setattr(FilesystemBackend, "get", forbidden)
    loaded = load_unresolved_harness(
        (_config(tmp_path / "config.toml"),), selected_target="local", selected_backend=None
    )
    backends = loaded.settings.targets["local"].backends
    assert backends["postgres"].auth.values["PASSWORD"] == "secret:db.password"
    assert backends["sqlite"].connection["DATABASE"] == "secret:missing.database"


@pytest.mark.parametrize("case", ["success", "missing_record", "missing_field", "invalid_auth",
                                  "missing_username", "invalid_port", "reader_failure"])
def test_selected_store_closes_once_and_failures_are_sanitized(tmp_path, monkeypatch, case):
    sentinel = "private-sentinel-password"
    root = tmp_path / "secrets"
    root.mkdir(mode=0o700)
    record = {"host": "localhost", "username": "postgres", "password": sentinel}
    if case == "missing_field":
        del record["password"]
    if case == "invalid_port":
        record["port"] = sentinel
    with FilesystemBackend(root) as store:
        if case != "missing_record":
            store.set("db", record)
    body = SUITE + _target()
    if case == "missing_username":
        body = body.replace('USERNAME = "secret:db.username"\n', "")
    if case in {"invalid_auth", "missing_username"}:
        body = body.replace('transport = "direct"', 'transport = "compose"')
        body = body.replace('selector = "postgres"', 'selector = "postgres"\ncompose = { profile = "postgres", service = "postgres" }')
        body = body.replace('selector = "sqlite"', 'selector = "sqlite"\ncompose = { profile = "sqlite", service = "sqlite" }')
    if case == "invalid_auth":
        body = body.replace('USERNAME = "secret:db.username"', 'USERNAME = { invalid = "secret:db.username" }')
    if case == "invalid_port":
        body = body.replace("PORT = 5432", 'PORT = "secret:db.port"')
    path = tmp_path / "config.toml"
    path.write_text(body + f'\n[secret_providers.local]\ntype = "filesystem"\npath = "{root}"\n')
    opened_stores, closed_stores = [], []

    class TrackingStore(FilesystemBackend):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            opened_stores.append(self)

        def get(self, key):
            if case == "reader_failure":
                raise RuntimeError(sentinel)
            return super().get(key)

        def close(self):
            closed_stores.append(self)
            super().close()

    monkeypatch.setattr(config, "FilesystemBackend", TrackingStore)
    loaded = load_unresolved_harness((path,), selected_target="local", selected_backend="postgres")
    if case == "missing_username":
        loaded.settings.targets["local"].backends["postgres"].auth.values["PASSWORD"] = SecretStr(sentinel)
    if case == "success":
        selection = build_backend_selection(loaded)
        assert selection.auth_profile.PASSWORD.get_secret_value() == sentinel
        assert selection.settings_parameters.get_settings().HOST == "localhost"
    else:
        with pytest.raises(HarnessError) as caught:
            build_backend_selection(loaded)
        assert sentinel not in str(caught.value)
        assert sentinel not in repr(caught.value)
        assert caught.value.__cause__ is None
        assert caught.value.__context__ is None
        assert caught.value.phase.value == "configuration"
    assert len(opened_stores) == 1
    assert closed_stores == opened_stores


@pytest.mark.parametrize("replacement,expected", [
    (('profile = "password"', 'profile = "token"'), "does not support"),
    (("PORT = 5432", 'PORT = 5432\nUNKNOWN = "bad"'), "Unknown connection"),
    (('PASSWORD = "secret:db.password"', 'PASSWORD = "secret:db.password"\nUNKNOWN = "bad"'),
     "Unknown authentication"),
])
def test_invalid_selection_does_not_construct_store(tmp_path, monkeypatch, replacement, expected):
    def forbidden(*args, **kwargs):
        pytest.fail("invalid selection must fail before store construction")

    monkeypatch.setattr(config, "FilesystemBackend", forbidden)
    path = tmp_path / "config.toml"
    path.write_text((SUITE + _target()).replace(*replacement))
    loaded = load_unresolved_harness((path,), selected_target="local", selected_backend="postgres")
    with pytest.raises(HarnessError, match=expected):
        build_backend_selection(loaded)
