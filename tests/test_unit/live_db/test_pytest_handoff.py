from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from mountainash_settings import FilesystemBackend

from scripts.live_db_harness.config import build_backend_selection, load_unresolved_harness
from scripts.live_db_harness.runner import LiveDbRunner


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


def _enable_fixtures(pytester: pytest.Pytester) -> None:
    pytester.makeini("[pytest]\nasyncio_default_fixture_loop_scope = function\n")
    tests_root = Path(__file__).parents[2]
    pytester.makeconftest(
        f"import sys\nsys.path.insert(0, {str(tests_root)!r})\n"
        f"sys.path.insert(0, {str(tests_root.parent)!r})\n"
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

    result = pytester.runpytest_subprocess("-q", "-rs")

    result.assert_outcomes(skipped=1)
    result.stdout.fnmatch_lines(["*no live backend target selected*"])


def test_required_run_without_target_fails(pytester, monkeypatch) -> None:
    monkeypatch.delenv("MOUNTAINASH_LIVE_DB_TARGET", raising=False)
    monkeypatch.setenv("MOUNTAINASH_REQUIRE_LIVE_DB", "1")
    _enable_fixtures(pytester)
    pytester.makepyfile(test_probe="def test_probe(postgres_backend):\n    pass\n")

    result = pytester.runpytest_subprocess("-q", "-rs")

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

    result = pytester.runpytest_subprocess("-q", "-rs")

    result.assert_outcomes(skipped=1)
    result.stdout.fnmatch_lines(["*no live backend target selected*"])


def test_fixture_rejects_backend_other_than_selected_backend(pytester, monkeypatch) -> None:
    monkeypatch.setenv("MOUNTAINASH_LIVE_DB_TARGET", "local")
    monkeypatch.setenv("MOUNTAINASH_LIVE_DB_BACKEND", "mysql")
    _enable_fixtures(pytester)
    pytester.makepyfile(test_probe="def test_probe(postgres_backend):\n    pass\n")

    result = pytester.runpytest_subprocess("-q", "-rs")

    result.assert_outcomes(skipped=1)
    result.stdout.fnmatch_lines(["*postgres_backend*mysql*"])

    monkeypatch.setenv("MOUNTAINASH_REQUIRE_LIVE_DB", "1")
    result = pytester.runpytest_subprocess("-q", "-rs")

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

    result = pytester.runpytest_subprocess("-q", "-rs")

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

    result = pytester.runpytest_subprocess("-q", "-rs")

    result.assert_outcomes(skipped=1)
    result.stdout.fnmatch_lines(["*singlestore_backend*mysql*"])

    monkeypatch.setenv("MOUNTAINASH_REQUIRE_LIVE_DB", "1")
    result = pytester.runpytest_subprocess("-q", "-rs")

    result.assert_outcomes(errors=1)
    result.stdout.fnmatch_lines(["*singlestore_backend*mysql*"])


def test_child_process_reconstructs_selection_after_stores_close(
    pytester: pytest.Pytester, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Observe real store closure without replacing resolution or filesystem IO.
    closed_stores: list[FilesystemBackend] = []
    original_exit = FilesystemBackend.__exit__

    def track_exit(store: FilesystemBackend, *args: object) -> None:
        original_exit(store, *args)
        closed_stores.append(store)

    # __del__ also calls close; observing context exit avoids resurrecting stores
    # from their destructor when the observer's retained references are released.
    monkeypatch.setattr(FilesystemBackend, "__exit__", track_exit)
    tracked = tmp_path / "tracked.toml"
    user = tmp_path / "user.toml"
    selected_secrets = tmp_path / "selected-secrets"
    tracked_secrets = tmp_path / "tracked-secrets"
    selected_secrets.mkdir(mode=0o700)
    tracked_secrets.mkdir(mode=0o700)
    with FilesystemBackend(selected_secrets) as store:
        store.set("database", {
            "host": "child-sentinel-host",
            "username": "child-sentinel-user",
            "password": "child-sentinel-password",
        })
    with FilesystemBackend(tracked_secrets) as store:
        store.set("database", {
            "host": "wrong-host", "username": "wrong-user", "password": "wrong-password",
        })
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
    assert len(closed_stores) == 3  # Two seed contexts and the parent selection context.
    assert len({id(store) for store in closed_stores}) == 3

    calls = []

    def capture_command(argv: list[str], **kwargs: object) -> None:
        calls.append((argv, kwargs))

    runner = LiveDbRunner((tracked.resolve(), user.resolve()))
    # The process boundary is the only fake: exercise the actual argv/env builder.
    with monkeypatch.context() as env_patch:
        env_patch.setattr(os, "environ", {
            "PATH": os.defpath,
            "IBIS_TEST_POSTGRES_PASSWORD": "legacy-password",
            "IBIS_TEST_MYSQL_HOST": "legacy-host",
            "MOUNTAINASH_LIVE_DB_CONFIG": "stale-config",
            "MOUNTAINASH_LIVE_DB_TARGET": "stale-target",
            "MOUNTAINASH_LIVE_DB_BACKEND": "stale-backend",
            "MOUNTAINASH_REQUIRE_LIVE_DB": "0",
        })
        runner._run_pytest(
            parent_selection, command_runner=SimpleNamespace(run=capture_command),
        )
    assert len(calls) == 1
    argv, kwargs = calls[0]
    assert argv == [
        sys.executable, "-m", "pytest", "tests/test_live_backends",
        "-k", "postgres", "-m", "integration",
    ]
    child_env = kwargs["env"]
    assert child_env == {
        "PATH": os.defpath,
        "MOUNTAINASH_LIVE_DB_CONFIG": json.dumps([str(tracked.resolve()), str(user.resolve())]),
        "MOUNTAINASH_LIVE_DB_TARGET": "local",
        "MOUNTAINASH_LIVE_DB_BACKEND": "postgres",
        "MOUNTAINASH_REQUIRE_LIVE_DB": "1",
    }
    assert all(isinstance(value, str) for value in child_env.values())
    serialized_boundary = json.dumps([argv, child_env])
    for forbidden in (
        "child-sentinel-host", "child-sentinel-user", "child-sentinel-password",
        "legacy-password", "legacy-host", str(selected_secrets), str(tracked_secrets),
        "FilesystemBackend", "PosixPath",
    ):
        assert forbidden not in serialized_boundary

    tests_root = Path(__file__).parents[2]
    child_ini = pytester.makeini("[pytest]\nasyncio_default_fixture_loop_scope = function\n")
    probe = pytester.makepyfile(
        test_probe=f"""
import importlib.util
import sys
sys.path.insert(0, {str(tests_root)!r})

from mountainash_auth_client import PasswordAuthProfile
from fixtures.live_db_fixtures import load_fixture_selection_from_environment


def test_child_selection(pytestconfig):
    assert pytestconfig.getini("asyncio_default_fixture_loop_scope") == "function"
    selection = load_fixture_selection_from_environment()
    assert selection.target_name == "local"
    assert selection.backend_name == "postgres"
    assert isinstance(selection.auth_profile, PasswordAuthProfile)
    assert selection.auth_profile.profile_name == "password"
    assert selection.auth_profile.USERNAME == "child-sentinel-user"
    assert selection.auth_profile.PASSWORD.get_secret_value() == "child-sentinel-password"
    assert selection.settings_parameters.get_settings().HOST == "child-sentinel-host"
    assert importlib.util.find_spec("mountainash_secrets") is None
"""
    )
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "-c", str(child_ini), str(probe), "-q"],
        cwd=Path(__file__).parents[3],
        env=child_env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "asyncio_default_fixture_loop_scope" not in completed.stderr


_LIVE_BACKEND_FILES = [
    "tests/test_live_backends/test_live_smoke.py",
    "tests/test_live_backends/test_write_ops_live.py",
    "tests/test_live_backends/test_index_ops_live.py",
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
            *_LIVE_BACKEND_FILES,
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
        if line.startswith("tests/test_live_backends/") and "::" in line
    }


def test_singlestoredb_integration_selection_contract() -> None:
    assert _selected_integration_node_ids("singlestoredb") == {
        "tests/test_live_backends/test_live_smoke.py::test_singlestoredb_smoke",
        "tests/test_live_backends/test_write_ops_live.py::test_rename_table_live_singlestoredb",
        "tests/test_live_backends/test_write_ops_live.py::test_upsert_via_dispatch_singlestoredb",
        "tests/test_live_backends/test_index_ops_live.py::test_singlestoredb_table_scoped_index_roundtrip",
    }


def test_mssql_integration_selection_contract() -> None:
    assert _selected_integration_node_ids("mssql") == {
        "tests/test_live_backends/test_live_smoke.py::test_mssql_smoke",
        "tests/test_live_backends/test_write_ops_live.py::test_rename_table_live_mssql",
        "tests/test_live_backends/test_write_ops_live.py::test_upsert_via_dispatch_mssql",
        "tests/test_live_backends/test_index_ops_live.py::test_mssql_table_scoped_partial_index_roundtrip",
    }


def test_trino_integration_selection_contract() -> None:
    assert _selected_integration_node_ids("trino") == {
        "tests/test_live_backends/test_live_smoke.py::test_trino_smoke",
        "tests/test_live_backends/test_write_ops_live.py::test_rename_table_live_trino",
        "tests/test_live_backends/test_write_ops_live.py::test_upsert_via_dispatch_trino",
    }


def test_exasol_integration_selection_contract() -> None:
    assert _selected_integration_node_ids("exasol") == {
        "tests/test_live_backends/test_live_smoke.py::test_exasol_smoke",
        "tests/test_live_backends/test_write_ops_live.py::test_rename_table_live_exasol",
        "tests/test_live_backends/test_write_ops_live.py::test_upsert_via_dispatch_exasol",
    }


def test_pyspark_integration_selection_contract() -> None:
    assert _selected_integration_node_ids("pyspark") == {
        "tests/test_live_backends/test_live_smoke.py::test_pyspark_smoke",
        "tests/test_live_backends/test_write_ops_live.py::test_rename_table_live_pyspark",
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
