from __future__ import annotations

import tomllib
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[3]
HATCH_PATH = ROOT / "hatch.toml"
LIVE_WORKFLOW_PATH = ROOT / ".github/workflows/python-verify-live-db.yml"
PULL_REQUEST_WORKFLOW_PATH = ROOT / ".github/workflows/python-run-pytest.yml"


def _hatch_config() -> dict:
    with HATCH_PATH.open("rb") as stream:
        return tomllib.load(stream)


def _workflow(path: Path) -> dict:
    with path.open() as stream:
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


def test_live_workflow_calls_test_github_live_runner() -> None:
    jobs = _workflow(LIVE_WORKFLOW_PATH)["jobs"]
    expected = {
        "postgres": "hatch run test_github_live:live-db test --target docker postgres",
        "mysql": "hatch run test_github_live:live-db test --target docker mysql",
        "oracle": "hatch run test_github_live:live-db test --target docker oracle",
    }

    for backend, command in expected.items():
        steps = jobs[backend]["steps"]
        run_commands = [step["run"] for step in steps if "run" in step]
        assert command in run_commands
        assert list(jobs[backend]["services"]) == [backend]


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
