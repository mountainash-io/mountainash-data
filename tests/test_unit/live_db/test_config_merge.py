from __future__ import annotations

from pathlib import Path

import json
import tomllib

import pytest
import yaml
from pydantic import ValidationError

from scripts.live_db_harness.sources import load_harness_settings


POSTGRES_SUITE = '''[backends.postgres]
settings_profile = "postgresql"
selector = "postgres"
runnable = true
compose = { profile = "postgres", service = "postgres" }
'''

DOCKER_TARGET = '''[targets.docker]
transport = "compose"
max_parallel = 1
test_timeout_seconds = 30

[targets.docker.backends.postgres.connection]
HOST = "127.0.0.1"
PORT = 5432

[targets.docker.backends.postgres.auth]
profile = "password"
'''

MPNAS_TARGET = '''[targets.mpnas]
transport = "direct"
max_parallel = 2
test_timeout_seconds = 30

[targets.mpnas.backends.postgres.connection]
HOST = "127.0.0.1"
PORT = 5432

[targets.mpnas.backends.postgres.auth]
profile = "password"

'''


def test_tracked_and_user_files_add_targets(tmp_path: Path):
    tracked = tmp_path / "tracked.toml"
    user = tmp_path / "user.toml"
    tracked.write_text(POSTGRES_SUITE + DOCKER_TARGET)
    user.write_text(MPNAS_TARGET)

    settings = load_harness_settings((tracked, user))

    assert set(settings.targets) == {"docker", "mpnas"}


def test_user_scalar_values_replace_tracked_values(tmp_path: Path):
    tracked = tmp_path / "tracked.toml"
    user = tmp_path / "user.toml"
    tracked.write_text(POSTGRES_SUITE + DOCKER_TARGET + MPNAS_TARGET)
    user.write_text(
        '''[targets.docker]
max_parallel = 3

'''
    )

    settings = load_harness_settings((tracked, user))

    assert settings.targets["docker"].max_parallel == 3


def test_keyword_target_selection_wins_over_file_values(tmp_path: Path):
    tracked = tmp_path / "tracked.toml"
    user = tmp_path / "user.toml"
    tracked.write_text(POSTGRES_SUITE + DOCKER_TARGET)
    user.write_text('selected_target = "mpnas"\nselected_backend = "postgres"\n' + MPNAS_TARGET)

    settings = load_harness_settings(
        (tracked, user), selected_target="docker", selected_backend="postgres"
    )

    assert settings.selected_target == "docker"
    assert settings.selected_backend == "postgres"


def test_explicit_selection_over_environment_over_file(tmp_path, monkeypatch):
    path = tmp_path / "config.toml"
    path.write_text('selected_target = "docker"\n' + POSTGRES_SUITE + DOCKER_TARGET + MPNAS_TARGET)
    monkeypatch.setenv("MOUNTAINASH_LIVE_DB_selected_target", "mpnas")
    assert load_harness_settings((path,)).selected_target == "mpnas"
    assert load_harness_settings((path,), selected_target="docker").selected_target == "docker"


def test_backend_selection_precedence_and_case_sensitive_environment(tmp_path, monkeypatch):
    path = tmp_path / "config.toml"
    body = POSTGRES_SUITE + DOCKER_TARGET
    body += '''
[backends.sqlite]
settings_profile = "sqlite"
selector = "sqlite"
runnable = true
compose = { profile = "sqlite", service = "sqlite" }
[targets.docker.backends.sqlite.connection]
DATABASE = ":memory:"
[targets.docker.backends.sqlite.auth]
profile = "none"
'''
    path.write_text('selected_backend = "postgres"\n' + body)
    monkeypatch.setenv("MOUNTAINASH_LIVE_DB_SELECTED_BACKEND", "unknown")
    assert load_harness_settings((path,)).selected_backend == "postgres"
    monkeypatch.setenv("MOUNTAINASH_LIVE_DB_selected_backend", "sqlite")
    assert load_harness_settings((path,)).selected_backend == "sqlite"
    assert load_harness_settings((path,), selected_backend="postgres").selected_backend == "postgres"


@pytest.mark.parametrize("value", ["", "None"])
def test_list_backend_ignores_empty_or_none_environment(tmp_path, monkeypatch, value):
    path = tmp_path / "config.toml"
    path.write_text(POSTGRES_SUITE + DOCKER_TARGET)
    monkeypatch.setenv("MOUNTAINASH_LIVE_DB_selected_backend", value)
    assert load_harness_settings((path,), selected_backend=None).selected_backend is None


@pytest.mark.parametrize("value,expected", [("", "postgres"), ("None", None)])
def test_empty_backend_environment_defers_to_file_but_none_clears_it(tmp_path, monkeypatch, value, expected):
    path = tmp_path / "config.toml"
    path.write_text('selected_backend = "postgres"\n' + POSTGRES_SUITE + DOCKER_TARGET)
    monkeypatch.setenv("MOUNTAINASH_LIVE_DB_selected_backend", value)
    assert load_harness_settings((path,)).selected_backend == expected


def test_interleaved_loads_keep_paths_local(tmp_path):
    first = tmp_path / "first.toml"
    second = tmp_path / "second.toml"
    first.write_text(POSTGRES_SUITE + DOCKER_TARGET)
    second.write_text(POSTGRES_SUITE + MPNAS_TARGET)
    for path, expected in [(first, {"docker"}), (second, {"mpnas"}), (first, {"docker"})]:
        assert set(load_harness_settings((path,)).targets) == expected


@pytest.mark.parametrize("extension", ["yaml", "json", "env"])
def test_supported_sources_preserve_raw_references(tmp_path, extension):
    data = tomllib.loads(POSTGRES_SUITE + DOCKER_TARGET)
    data["targets"]["docker"]["backends"]["postgres"]["auth"]["values"] = {
        "PASSWORD": "secret:db.password"
    }
    path = tmp_path / f"config.{extension}"
    if extension == "yaml":
        path.write_text(yaml.safe_dump(data))
    elif extension == "json":
        path.write_text(json.dumps(data))
    else:
        path.write_text("\n".join(f"{key}='{json.dumps(value)}'" for key, value in data.items()))
    settings = load_harness_settings((path,))
    assert settings.targets["docker"].backends["postgres"].auth.values["PASSWORD"] == "secret:db.password"


@pytest.mark.parametrize("fallback", ["", "selected_target=mpnas\n"])
def test_prefixed_dotenv_selection_wins_over_unprefixed(tmp_path, fallback):
    config = tmp_path / "config.toml"
    config.write_text(POSTGRES_SUITE + DOCKER_TARGET + MPNAS_TARGET)
    dotenv = tmp_path / "control.env"
    dotenv.write_text(fallback + "MOUNTAINASH_LIVE_DB_selected_target=docker\n")

    assert load_harness_settings((config, dotenv)).selected_target == "docker"


def test_explicit_and_environment_override_mixed_dotenv(tmp_path, monkeypatch):
    config = tmp_path / "config.toml"
    config.write_text(POSTGRES_SUITE + DOCKER_TARGET + MPNAS_TARGET)
    dotenv = tmp_path / "control.env"
    dotenv.write_text("selected_target=mpnas\nMOUNTAINASH_LIVE_DB_selected_target=docker\n")
    monkeypatch.setenv("MOUNTAINASH_LIVE_DB_selected_target", "mpnas")

    assert load_harness_settings((config, dotenv)).selected_target == "mpnas"
    assert load_harness_settings((config, dotenv), selected_target="docker").selected_target == "docker"


@pytest.mark.parametrize("unknown", ["typo", "MOUNTAINASH_LIVE_DB_typo"])
def test_dotenv_unknown_fields_remain_forbidden(tmp_path, unknown):
    config = tmp_path / "config.toml"
    config.write_text(POSTGRES_SUITE + DOCKER_TARGET)
    dotenv = tmp_path / "control.env"
    dotenv.write_text(f"MOUNTAINASH_LIVE_DB_selected_target=docker\n{unknown}=unexpected\n")

    with pytest.raises(ValidationError) as caught:
        load_harness_settings((config, dotenv))

    assert [(error["loc"], error["type"]) for error in caught.value.errors()] == [
        ((unknown,), "extra_forbidden")
    ]
