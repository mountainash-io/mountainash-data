from __future__ import annotations

import configparser
import subprocess
import sys
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
PYTEST_PATH = ROOT / "pytest.ini"
PYPROJECT_PATH = ROOT / "pyproject.toml"
OPTIONAL_TEST_ROOT = ROOT / "tests/test_optional_backends"
OPTIONAL_BACKENDS = {
    "oracle": {"feature": "oracle", "module": "oracledb"},
    "trino": {"feature": "trino", "module": "trino"},
    "bigquery": {"feature": "bigquery", "module": "google.oauth2"},
}
REQUIRED_PULL_REQUEST_PATHS = {
    "src/mountainash_data/**",
    "scripts/**",
    "tests/**",
    "hatch.toml",
    "pytest.ini",
    "pyproject.toml",
    ".github/actions/**",
    ".github/config/mountainash_dependencies.yml",
    ".github/workflows/**",
}


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
        "postgres": "hatch run test_github_live:live-db run --target docker postgres",
        "mysql": "hatch run test_github_live:live-db run --target docker mysql",
        "oracle": "hatch run test_github_live:live-db run --target docker oracle",
        "singlestoredb": (
            "hatch run test_github_live:live-db run --target docker singlestoredb"
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
    assert "-u ibis" in healthcheck_command
    assert "-pibis" in healthcheck_command
    assert "-D ibis_testing" in healthcheck_command
    assert "SELECT 1" in healthcheck_command
    assert "root" not in healthcheck_command.lower()
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


def test_pytest_default_paths_are_core_only() -> None:
    parser = configparser.ConfigParser()
    parser.read(PYTEST_PATH)

    assert parser["pytest"]["testpaths"].split() == [
        "tests/test_unit",
        "tests/test_integration",
    ]


def test_default_pytest_collection_is_core_only() -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    node_ids = [
        line
        for line in completed.stdout.splitlines()
        if "::" in line
    ]

    assert completed.returncode == 0, completed.stderr
    assert node_ids
    assert all(
        node_id.startswith(("tests/test_unit/", "tests/test_integration/"))
        for node_id in node_ids
    )


def test_optional_backend_matrix_matches_test_directories() -> None:
    full_config = _hatch_config()
    config = full_config["envs"]["test_optional"]
    directories = {
        path.name
        for path in OPTIONAL_TEST_ROOT.iterdir()
        if path.is_dir() and not path.name.startswith("__")
    }

    assert directories == set(OPTIONAL_BACKENDS)
    assert config["python"] == "3.12"
    assert config["matrix-name-format"] == "{value}"
    assert config["matrix"] == [{"backend": list(OPTIONAL_BACKENDS)}]

    rules = config["overrides"]["matrix"]["backend"]["features"]
    assert rules == [
        {"value": data["feature"], "if": [backend]}
        for backend, data in OPTIONAL_BACKENDS.items()
    ]

    scripts = config["scripts"]
    assert scripts["test"] == (
        "pytest tests/test_optional_backends/{matrix:backend} {args}"
    )
    assert "coverage-optional-{matrix:backend}.xml" in scripts["test-cov"]

    assert config["dependencies"] == full_config["envs"]["test_github"]["dependencies"]
    assert "live-db" not in config["scripts"]

    with PYPROJECT_PATH.open("rb") as stream:
        project_features = tomllib.load(stream)["project"]["optional-dependencies"]
    assert {
        data["feature"]
        for data in OPTIONAL_BACKENDS.values()
    } <= set(project_features)


def test_local_non_live_commands_select_all_non_live_paths() -> None:
    test_environment = _hatch_config()["envs"]["test"]
    expected_paths = (
        "tests/test_unit tests/test_integration tests/test_optional_backends"
    )

    assert expected_paths in test_environment["scripts"]["test"][0]
    assert test_environment["scripts"]["test-quick"] == f"pytest {expected_paths}"
    assert test_environment["scripts"]["test-core"] == (
        "pytest tests/test_unit tests/test_integration {args}"
    )

    with PYPROJECT_PATH.open("rb") as stream:
        project_features = tomllib.load(stream)["project"]["optional-dependencies"]
    local_dependencies = set(test_environment["dependencies"])
    for data in OPTIONAL_BACKENDS.values():
        assert set(project_features[data["feature"]]) <= local_dependencies


def test_pull_request_workflow_covers_test_contract_paths() -> None:
    workflow = _workflow(PULL_REQUEST_WORKFLOW_PATH)

    assert REQUIRED_PULL_REQUEST_PATHS <= set(workflow["on"]["pull_request"]["paths"])


def test_pull_request_workflow_has_isolated_optional_backend_matrix() -> None:
    workflow = _workflow(PULL_REQUEST_WORKFLOW_PATH)
    job = workflow["jobs"]["optional-backend"]
    expected = [
        {"backend": "oracle", "module": "oracledb"},
        {"backend": "trino", "module": "trino"},
        {"backend": "bigquery", "module": "google.oauth2"},
    ]

    assert job["strategy"]["matrix"]["include"] == expected
    assert "services" not in job

    commands = [step["run"] for step in job["steps"] if "run" in step]
    assert "hatch env create test_optional.${{ matrix.backend }}" in commands
    assert (
        "hatch run test_optional.${{ matrix.backend }}:test-cov"
        in commands
    )

    upload = next(
        step
        for step in job["steps"]
        if str(step.get("uses", "")).startswith("codecov/codecov-action@")
    )
    assert upload["with"]["files"] == (
        "coverage-optional-${{ matrix.backend }}.xml"
    )
    assert upload["with"]["flags"] == "optional-${{ matrix.backend }}"
