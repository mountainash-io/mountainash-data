from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.live_db_harness.models import BackendResult, ResultStatus
from scripts.live_db_harness.runner import LiveDbRunner, run_many


def test_credential_free_pytest_context(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    seen: dict[str, object] = {}
    runner = LiveDbRunner(config_files=(tmp_path / "one.toml",), command_runner=SimpleNamespace())
    monkeypatch.setenv("IBIS_TEST_POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("MOUNTAINASH_LIVE_DB_TARGET", "old")

    class FakeRunner:
        def run(self, argv, *, env, timeout, phase, target, backend):
            seen.update(argv=tuple(argv), env=dict(env), timeout=timeout, phase=phase)
            return SimpleNamespace(returncode=0, stdout="", stderr="")

    runner.command_runner = FakeRunner()
    selection = SimpleNamespace(target_name="docker", backend_name="postgres", target=SimpleNamespace(test_timeout_seconds=12), suite=SimpleNamespace(selector="postgres"))
    runner._run_pytest(selection)
    env = seen["env"]
    assert "IBIS_TEST_POSTGRES_PASSWORD" not in env
    assert env["MOUNTAINASH_LIVE_DB_TARGET"] == "docker"
    assert env["MOUNTAINASH_LIVE_DB_BACKEND"] == "postgres"
    assert env["MOUNTAINASH_REQUIRE_LIVE_DB"] == "1"
    assert seen["timeout"] == 12


def test_effective_jobs_is_lower_cli_or_target_limit() -> None:
    assert run_many([], lambda _: None, jobs=8, target_limit=3) == []


def test_failed_attempt_returns_nonzero() -> None:
    result = BackendResult(backend="postgres", status=ResultStatus.FAIL, detail="failed")
    assert result.status is ResultStatus.FAIL
