from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from mountainash_secrets import FilesystemSecretStore
from mountainash_settings import clear_secrets_registry

from scripts.live_db_harness.config import build_backend_selection, load_unresolved_harness


pytest_plugins = ("pytester",)


BACKEND_SUITE = """
[backends.postgres]
settings_profile = "postgresql"
selector = "postgres"
runnable = true

[backends.unavailable]
settings_profile = "postgresql"
selector = "unavailable"
runnable = false
backlog = "TASK-UNAVAILABLE"
"""


TARGET = """
[targets.local]
transport = "direct"
secrets_provider = "selected"

[targets.local.backends.postgres.connection]
HOST = "secret:database.host"
PORT = 5432

[targets.local.backends.postgres.auth]
profile = "password"

[targets.local.backends.postgres.auth.values]
USERNAME = "secret:database.username"
PASSWORD = "secret:database.password"

[targets.local.backends.unavailable.connection]
HOST = "secret:missing.host"
PORT = 5432

[targets.local.backends.unavailable.auth]
profile = "none"
"""


@pytest.fixture(autouse=True)
def _clear_secret_registry():
    clear_secrets_registry()
    yield
    clear_secrets_registry()


def _enable_fixtures(pytester: pytest.FixtureRequest) -> None:
    tests_root = Path(__file__).parents[2]
    pytester.makeconftest(
        f"import sys\nsys.path.insert(0, {str(tests_root)!r})\n"
        'pytest_plugins = ("fixtures.live_db_fixtures",)'
    )


def test_normal_run_without_target_skips_with_exact_message(pytester, monkeypatch) -> None:
    for key in (
        "MOUNTAINASH_LIVE_DB_CONFIG",
        "MOUNTAINASH_LIVE_DB_TARGET",
        "MOUNTAINASH_LIVE_DB_BACKEND",
        "MOUNTAINASH_REQUIRE_LIVE_DB",
    ):
        monkeypatch.delenv(key, raising=False)
    _enable_fixtures(pytester)
    pytester.makepyfile(test_probe="def test_probe(postgres_backend):\n    pass\n")

    result = pytester.runpytest("-q", "-rs")

    result.assert_outcomes(skipped=1)
    result.stdout.fnmatch_lines(["*no live backend target selected*"])


def test_required_run_without_target_fails(pytester, monkeypatch) -> None:
    monkeypatch.delenv("MOUNTAINASH_LIVE_DB_TARGET", raising=False)
    monkeypatch.setenv("MOUNTAINASH_REQUIRE_LIVE_DB", "1")
    _enable_fixtures(pytester)
    pytester.makepyfile(test_probe="def test_probe(postgres_backend):\n    pass\n")

    result = pytester.runpytest("-q", "-rs")

    result.assert_outcomes(errors=1)
    result.stdout.fnmatch_lines(["*no live backend target selected*"])


def test_fixture_ignores_legacy_ibis_test_variables(pytester, monkeypatch) -> None:
    for key in (
        "MOUNTAINASH_LIVE_DB_CONFIG",
        "MOUNTAINASH_LIVE_DB_TARGET",
        "MOUNTAINASH_LIVE_DB_BACKEND",
        "MOUNTAINASH_REQUIRE_LIVE_DB",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("IBIS_TEST_POSTGRES_HOST", "legacy-host")
    monkeypatch.setenv("IBIS_TEST_POSTGRES_PASSWORD", "legacy-password")
    _enable_fixtures(pytester)
    pytester.makepyfile(test_probe="def test_probe(postgres_backend):\n    pass\n")

    result = pytester.runpytest("-q", "-rs")

    result.assert_outcomes(skipped=1)
    result.stdout.fnmatch_lines(["*no live backend target selected*"])


def test_fixture_rejects_backend_other_than_selected_backend(pytester, monkeypatch) -> None:
    monkeypatch.setenv("MOUNTAINASH_LIVE_DB_TARGET", "local")
    monkeypatch.setenv("MOUNTAINASH_LIVE_DB_BACKEND", "mysql")
    _enable_fixtures(pytester)
    pytester.makepyfile(test_probe="def test_probe(postgres_backend):\n    pass\n")

    result = pytester.runpytest("-q", "-rs")

    result.assert_outcomes(skipped=1)
    result.stdout.fnmatch_lines(["*postgres_backend*mysql*"])

    monkeypatch.setenv("MOUNTAINASH_REQUIRE_LIVE_DB", "1")
    result = pytester.runpytest("-q", "-rs")

    result.assert_outcomes(errors=1)
    result.stdout.fnmatch_lines(["*postgres_backend*mysql*"])


def test_singlestore_fixture_without_target_skips_with_exact_message(
    pytester, monkeypatch
) -> None:
    for key in (
        "MOUNTAINASH_LIVE_DB_CONFIG",
        "MOUNTAINASH_LIVE_DB_TARGET",
        "MOUNTAINASH_LIVE_DB_BACKEND",
        "MOUNTAINASH_REQUIRE_LIVE_DB",
    ):
        monkeypatch.delenv(key, raising=False)
    _enable_fixtures(pytester)
    pytester.makepyfile(
        test_probe="def test_probe(singlestore_backend):\n    pass\n"
    )

    result = pytester.runpytest("-q", "-rs")

    result.assert_outcomes(skipped=1)
    result.stdout.fnmatch_lines(["*no live backend target selected*"])


def test_singlestore_fixture_rejects_backend_other_than_selected_backend(
    pytester, monkeypatch
) -> None:
    monkeypatch.setenv("MOUNTAINASH_LIVE_DB_TARGET", "local")
    monkeypatch.setenv("MOUNTAINASH_LIVE_DB_BACKEND", "mysql")
    _enable_fixtures(pytester)
    pytester.makepyfile(
        test_probe="def test_probe(singlestore_backend):\n    pass\n"
    )

    result = pytester.runpytest("-q", "-rs")

    result.assert_outcomes(skipped=1)
    result.stdout.fnmatch_lines(["*singlestore_backend*mysql*"])

    monkeypatch.setenv("MOUNTAINASH_REQUIRE_LIVE_DB", "1")
    result = pytester.runpytest("-q", "-rs")

    result.assert_outcomes(errors=1)
    result.stdout.fnmatch_lines(["*singlestore_backend*mysql*"])


def test_child_process_reloads_provider_and_auth_profile(pytester, tmp_path: Path) -> None:
    tracked = tmp_path / "tracked.toml"
    user = tmp_path / "user.toml"
    selected_secrets = tmp_path / "selected-secrets"
    tracked_secrets = tmp_path / "tracked-secrets"
    selected_secrets.mkdir(mode=0o700)
    tracked_secrets.mkdir(mode=0o700)
    FilesystemSecretStore(selected_secrets).set(
        "database",
        {
            "host": "child-sentinel-host",
            "username": "child-sentinel-user",
            "password": "child-sentinel-password",
        },
    )
    FilesystemSecretStore(tracked_secrets).set(
        "database",
        {"host": "wrong-host", "username": "wrong-user", "password": "wrong-password"},
    )
    tracked.write_text(BACKEND_SUITE + TARGET, encoding="utf-8")
    user.write_text(
        f"""
[secret_providers.tracked]
type = "filesystem"
path = "{tracked_secrets}"

[secret_providers.selected]
type = "filesystem"
path = "{selected_secrets}"
""",
        encoding="utf-8",
    )

    loaded = load_unresolved_harness(
        (tracked, user), selected_target="local", selected_backend="postgres"
    )
    parent_selection = build_backend_selection(loaded)
    assert parent_selection.auth_profile.PASSWORD.get_secret_value() == "child-sentinel-password"
    clear_secrets_registry()

    tests_root = Path(__file__).parents[2]
    probe = pytester.makepyfile(
        test_probe=f"""
import sys
sys.path.insert(0, {str(tests_root)!r})

from mountainash_auth_client import PasswordAuthProfile
from mountainash_settings import get_secrets_backend
from fixtures.live_db_fixtures import load_fixture_selection_from_environment


def test_child_selection():
    selection = load_fixture_selection_from_environment()
    assert get_secrets_backend("selected") is not None
    assert selection.target_name == "local"
    assert selection.backend_name == "postgres"
    assert isinstance(selection.auth_profile, PasswordAuthProfile)
    assert selection.auth_profile.PASSWORD.get_secret_value() == "child-sentinel-password"
"""
    )
    child_env = {
        "MOUNTAINASH_LIVE_DB_CONFIG": json.dumps([str(tracked.resolve()), str(user.resolve())]),
        "MOUNTAINASH_LIVE_DB_TARGET": "local",
        "MOUNTAINASH_LIVE_DB_BACKEND": "postgres",
        "MOUNTAINASH_REQUIRE_LIVE_DB": "1",
    }
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", str(probe), "-q"],
        cwd=Path(__file__).parents[3],
        env=child_env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, "pytest subprocess failed"


_INTEGRATION_FILES = [
    "tests/test_integration/test_live_smoke.py",
    "tests/test_integration/test_write_ops_live.py",
    "tests/test_integration/test_index_ops_live.py",
]
_LIVE_ENVIRONMENT_KEYS = (
    "MOUNTAINASH_LIVE_DB_CONFIG",
    "MOUNTAINASH_LIVE_DB_TARGET",
    "MOUNTAINASH_LIVE_DB_BACKEND",
    "MOUNTAINASH_REQUIRE_LIVE_DB",
)


def _selected_integration_node_ids(selector: str) -> set[str]:
    repo_root = Path(__file__).parents[3]
    child_env = os.environ.copy()
    for key in _LIVE_ENVIRONMENT_KEYS:
        child_env.pop(key, None)
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            "-k",
            selector,
            "-m",
            "integration",
            *_INTEGRATION_FILES,
        ],
        cwd=repo_root,
        env=child_env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return {
        line
        for line in completed.stdout.splitlines()
        if line.startswith("tests/test_integration/") and "::" in line
    }


def test_singlestoredb_integration_selection_contract() -> None:
    assert _selected_integration_node_ids("singlestoredb") == {
        "tests/test_integration/test_live_smoke.py::test_singlestoredb_smoke",
        "tests/test_integration/test_write_ops_live.py::test_rename_table_live_singlestoredb",
        "tests/test_integration/test_write_ops_live.py::test_upsert_via_dispatch_singlestoredb",
        "tests/test_integration/test_index_ops_live.py::test_singlestoredb_table_scoped_index_roundtrip",
    }


def test_mssql_integration_selection_contract() -> None:
    assert _selected_integration_node_ids("mssql") == {
        "tests/test_integration/test_live_smoke.py::test_mssql_smoke",
        "tests/test_integration/test_write_ops_live.py::test_rename_table_live_mssql",
        "tests/test_integration/test_write_ops_live.py::test_upsert_via_dispatch_mssql",
        "tests/test_integration/test_index_ops_live.py::test_mssql_table_scoped_partial_index_roundtrip",
    }


def test_trino_integration_selection_contract() -> None:
    assert _selected_integration_node_ids("trino") == {
        "tests/test_integration/test_live_smoke.py::test_trino_smoke",
        "tests/test_integration/test_write_ops_live.py::test_rename_table_live_trino",
        "tests/test_integration/test_write_ops_live.py::test_upsert_via_dispatch_trino",
    }


def test_exasol_integration_selection_contract() -> None:
    assert _selected_integration_node_ids("exasol") == {
        "tests/test_integration/test_live_smoke.py::test_exasol_smoke",
        "tests/test_integration/test_write_ops_live.py::test_rename_table_live_exasol",
        "tests/test_integration/test_write_ops_live.py::test_upsert_via_dispatch_exasol",
    }


def test_pyspark_integration_selection_contract() -> None:
    assert _selected_integration_node_ids("pyspark") == {
        "tests/test_integration/test_live_smoke.py::test_pyspark_smoke",
        "tests/test_integration/test_write_ops_live.py::test_rename_table_live_pyspark",
    }


def test_cleanup_helper_fails_when_body_succeeds_and_cleanup_fails() -> None:
    from fixtures.database_fixtures import cleanup_test_objects

    def cleanup() -> None:
        raise RuntimeError("cleanup failed")

    with pytest.raises(RuntimeError, match="cleanup failed"):
        with cleanup_test_objects(cleanup):
            pass


def test_cleanup_helper_preserves_body_failure() -> None:
    from fixtures.database_fixtures import cleanup_test_objects
    cleanup_called = False

    def cleanup() -> None:
        nonlocal cleanup_called
        cleanup_called = True
        raise RuntimeError("cleanup failed")

    with pytest.raises(ValueError, match="body failed"):
        with cleanup_test_objects(cleanup):
            raise ValueError("body failed")
    assert cleanup_called is True


def test_cleanup_helper_propagates_keyboardinterrupt_after_all_cleanups() -> None:
    from fixtures.database_fixtures import cleanup_test_objects
    called: list[str] = []

    def interrupt() -> None:
        called.append("interrupt")
        raise KeyboardInterrupt("cleanup interrupted")

    def follow_up() -> None:
        called.append("follow-up")

    with pytest.raises(KeyboardInterrupt, match="cleanup interrupted"):
        with cleanup_test_objects(interrupt, follow_up):
            pass
    assert called == ["interrupt", "follow-up"]


def test_cleanup_helper_chains_keyboardinterrupt_after_body_failure() -> None:
    from fixtures.database_fixtures import cleanup_test_objects
    called: list[str] = []

    def interrupt() -> None:
        called.append("interrupt")
        raise KeyboardInterrupt("cleanup interrupted")

    def follow_up() -> None:
        called.append("follow-up")

    with pytest.raises(KeyboardInterrupt, match="cleanup interrupted") as caught:
        with cleanup_test_objects(interrupt, follow_up):
            raise ValueError("body failed")
    assert called == ["interrupt", "follow-up"]
    assert isinstance(caught.value.__cause__, ValueError)


def test_cleanup_helper_propagates_systemexit_after_all_cleanups() -> None:
    from fixtures.database_fixtures import cleanup_test_objects
    called: list[str] = []

    def exit_cleanup() -> None:
        called.append("exit")
        raise SystemExit("cleanup exited")

    def follow_up() -> None:
        called.append("follow-up")

    with pytest.raises(SystemExit, match="cleanup exited"):
        with cleanup_test_objects(exit_cleanup, follow_up):
            pass
    assert called == ["exit", "follow-up"]


def test_cleanup_helper_chains_systemexit_after_body_failure() -> None:
    from fixtures.database_fixtures import cleanup_test_objects
    called: list[str] = []

    def exit_cleanup() -> None:
        called.append("exit")
        raise SystemExit("cleanup exited")

    def follow_up() -> None:
        called.append("follow-up")

    with pytest.raises(SystemExit, match="cleanup exited") as caught:
        with cleanup_test_objects(exit_cleanup, follow_up):
            raise ValueError("body failed")
    assert called == ["exit", "follow-up"]
    assert isinstance(caught.value.__cause__, ValueError)
