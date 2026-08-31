from __future__ import annotations

import tomllib
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[3]
HATCH_PATH = ROOT / "hatch.toml"
COMPOSE_PATH = ROOT / "compose.yaml"
LIVE_WORKFLOW_PATH = ROOT / ".github/workflows/python-verify-live-db.yml"
PULL_REQUEST_WORKFLOW_PATH = ROOT / ".github/workflows/python-run-pytest.yml"
LIVE_DB_CONFIG_PATH = ROOT / "tests/config/live-db.toml"
SINGLESTORE_INIT_PATH = ROOT / "tests/config/singlestore-init.sql"


def _hatch_config() -> dict:
    with HATCH_PATH.open("rb") as stream:
        return tomllib.load(stream)


def _workflow(path: Path) -> dict:
    with path.open() as stream:
        return yaml.load(stream, Loader=yaml.BaseLoader)


def _compose() -> dict:
    with COMPOSE_PATH.open() as stream:
        return yaml.load(stream, Loader=yaml.BaseLoader)


def _strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [item for key, item in value.items() for item in (*_strings(key), *_strings(item))]
    if isinstance(value, list):
        return [item for entry in value for item in _strings(entry)]
    return []


def test_local_and_live_ci_environments_expose_live_db_script() -> None:
    config = _hatch_config()

    assert config["envs"]["test"]["scripts"]["live-db"] == "python -m scripts.live_db {args}"
    assert (
        config["envs"]["test_github_live"]["scripts"]["live-db"]
        == "python -m scripts.live_db {args}"
    )


def test_legacy_live_scripts_are_absent() -> None:
    scripts = _hatch_config()["envs"]["test"]["scripts"]

    assert not {"test-live-up", "test-live-down", "test-live"} & scripts.keys()


def test_live_workflow_has_no_ibis_test_variables() -> None:
    workflow = _workflow(LIVE_WORKFLOW_PATH)

    assert not any("IBIS_TEST_" in value for value in _strings(workflow))


def test_live_workflow_has_exact_backend_options_and_runner_owned_jobs() -> None:
    workflow = _workflow(LIVE_WORKFLOW_PATH)
    dispatch = workflow["on"]["workflow_dispatch"]
    assert dispatch["inputs"]["backend"]["options"] == [
        "postgres",
        "mysql",
        "oracle",
        "singlestoredb",
    ]

    jobs = workflow["jobs"]
    expected = {
        "postgres": "hatch run test_github_live:live-db test --target docker postgres",
        "mysql": "hatch run test_github_live:live-db test --target docker mysql",
        "oracle": "hatch run test_github_live:live-db test --target docker oracle",
        "singlestoredb": (
            "hatch run test_github_live:live-db test --target docker singlestoredb"
        ),
    }
    assert set(jobs) == set(expected)
    for backend, command in expected.items():
        assert jobs[backend]["if"] == f"${{{{ inputs.backend == '{backend}' }}}}"
        run_commands = [step["run"] for step in jobs[backend]["steps"] if "run" in step]
        assert command in run_commands
        assert "services" not in jobs[backend]


def test_singlestore_compose_service_is_apple_silicon_compatible_and_authenticated() -> None:
    service = _compose()["services"]["singlestoredb"]

    assert service["image"] == "ghcr.io/singlestore-labs/singlestoredb-dev:latest"
    assert service["profiles"] == ["singlestoredb"]
    assert service["platform"] == "linux/amd64"
    assert service["ports"] == ["3307:3306"]
    assert service["environment"]["ROOT_PASSWORD"] == "singlestore"
    assert "./tests/config/singlestore-init.sql:/init.sql:ro" in service["volumes"]

    healthcheck = service["healthcheck"]
    assert healthcheck["test"][0] == "CMD-SHELL"
    healthcheck_command = " ".join(healthcheck["test"])
    assert "singlestore" in healthcheck_command
    assert "root" in healthcheck_command
    assert "ROOT_PASSWORD" in healthcheck_command
    assert "SELECT 1" in healthcheck_command


def test_singlestore_init_sql_creates_authenticated_ibis_testing_database() -> None:
    init_sql = SINGLESTORE_INIT_PATH.read_text()

    assert "CREATE DATABASE IF NOT EXISTS ibis_testing" in init_sql
    assert "CREATE USER IF NOT EXISTS 'ibis'@'%'" in init_sql
    assert "IDENTIFIED BY 'ibis'" in init_sql
    assert "GRANT ALL PRIVILEGES ON ibis_testing.* TO 'ibis'@'%'" in init_sql


def test_local_and_live_ci_environments_include_singlestore_driver() -> None:
    config = _hatch_config()

    for environment in ("test", "test_github_live"):
        assert "ibis-framework[singlestoredb]>=12.0.0" in config["envs"][environment][
            "dependencies"
        ]


def test_tracked_singlestore_backend_matches_compose_and_initialized_auth() -> None:
    with LIVE_DB_CONFIG_PATH.open("rb") as stream:
        config = tomllib.load(stream)

    suite = config["backends"]["singlestoredb"]
    assert suite == {
        "settings_profile": "singlestoredb",
        "selector": "singlestoredb",
        "runnable": True,
        "compose": {"profile": "singlestoredb", "service": "singlestoredb"},
    }

    target = config["targets"]["docker"]["backends"]["singlestoredb"]
    assert target["connection"] == {
        "HOST": "127.0.0.1",
        "PORT": 3307,
        "DATABASE": "ibis_testing",
    }
    assert target["auth"] == {
        "profile": "password",
        "values": {"USERNAME": "ibis", "PASSWORD": "ibis"},
    }


def test_live_workflow_has_no_ibis_test_variables() -> None:
    workflow = _workflow(LIVE_WORKFLOW_PATH)

    assert not any("IBIS_TEST_" in value for value in _strings(workflow))

def test_pull_request_workflow_stays_container_and_driver_free() -> None:
    config = _hatch_config()
    pull_request_workflow = _workflow(PULL_REQUEST_WORKFLOW_PATH)
    test_github = config["envs"]["test_github"]
    live_driver_markers = ("ibis-framework[", "psycopg", "mysqlclient", "oracledb")

    assert not any("services" in job for job in pull_request_workflow["jobs"].values())
    assert not any(
        any(marker in dependency.lower() for marker in live_driver_markers)
        for dependency in test_github["dependencies"]
    )
    assert "live-db" not in test_github["scripts"]
