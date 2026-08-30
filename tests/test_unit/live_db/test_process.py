from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pytest

from scripts.live_db_harness.models import HarnessError, Phase
from scripts.live_db_harness.process import CommandRunner, Redactor


def test_redactor_replaces_secret_scalar() -> None:
    redactor = Redactor(["short", "a much longer secret"])
    assert redactor.redact("short and a much longer secret") == "[REDACTED] and [REDACTED]"


def test_redactor_replaces_url_encoded_secret() -> None:
    redactor = Redactor(["pa:ss word"])
    assert redactor.redact("postgres://user:pa%3Ass%20word@db.example") == "postgres://[REDACTED]@db.example"


def test_redactor_replaces_credential_url_authority() -> None:
    redactor = Redactor(["password"])
    assert redactor.redact("postgresql://alice:password@db.example:5432/schema") == "postgresql://[REDACTED]@db.example:5432/schema"


def test_command_error_redacts_stdout_stderr_and_command_preview() -> None:
    secret = "top-secret-value"
    code = "import sys; print(sys.argv[1]); print(sys.argv[1], file=sys.stderr); sys.exit(3)"
    runner = CommandRunner(Redactor([secret]))

    with pytest.raises(HarnessError) as exc_info:
        runner.run([sys.executable, "-c", code, secret], phase=Phase.AUTHENTICATION, target="prod", backend="postgres")

    rendered = str(exc_info.value)
    assert "[REDACTED]" in rendered
    assert secret not in rendered
    assert "stderr" in rendered.lower()


def test_command_preview_never_contains_environment() -> None:
    secret = "environment-secret"
    code = "import os; print(os.environ['LIVE_DB_SECRET']); raise SystemExit(2)"
    runner = CommandRunner(Redactor([secret]))

    with pytest.raises(HarnessError) as exc_info:
        runner.run(
            [sys.executable, "-c", code],
            env={"LIVE_DB_SECRET": secret},
            phase=Phase.TRANSPORT,
            target="prod",
            backend="postgres",
        )

    rendered = str(exc_info.value)
    assert secret not in rendered
    assert "{'LIVE_DB_SECRET': 'environment-secret'}" not in rendered
    assert "env=" not in rendered


def test_timeout_returns_a_typed_phase_error() -> None:
    runner = CommandRunner(Redactor())
    with pytest.raises(HarnessError) as exc_info:
        runner.run(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            timeout=0.05,
            phase=Phase.PYTEST,
            target="prod",
            backend="postgres",
        )
    assert exc_info.value.phase is Phase.PYTEST
    assert "timed out" in str(exc_info.value).lower()


def _group_tree_script(pid_file: Path) -> str:
    return (
        "import pathlib, subprocess, sys, time, os; "
        f"pathlib.Path({str(pid_file)!r}).write_text(str(os.getpid())); "
        "child=subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']); "
        "pathlib.Path(sys.argv[1]).write_text(str(child.pid)); time.sleep(30)"
    )


def _wait_for_pid(path: Path) -> int:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if path.exists():
            return int(path.read_text())
        time.sleep(0.01)
    raise AssertionError(f"process did not create {path}")


def _assert_pid_gone(pid: int) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.01)
    raise AssertionError(f"process {pid} still exists")


def test_timeout_terminates_child_process_group(tmp_path: Path) -> None:
    parent_pid_file = tmp_path / "parent.pid"
    child_pid_file = tmp_path / "child.pid"
    runner = CommandRunner(Redactor())

    with pytest.raises(HarnessError):
        runner.run(
            [sys.executable, "-c", _group_tree_script(parent_pid_file), str(child_pid_file)],
            timeout=0.2,
            phase=Phase.PYTEST,
            target="prod",
            backend="postgres",
        )

    _assert_pid_gone(_wait_for_pid(child_pid_file))


def test_interrupt_terminates_child_process_group(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    parent_pid_file = tmp_path / "parent.pid"
    child_pid_file = tmp_path / "child.pid"
    runner = CommandRunner(Redactor())
    original_wait = runner._wait

    def interrupt_once(process: object, timeout: float | None) -> tuple[str, str]:
        _wait_for_pid(child_pid_file)
        monkeypatch.setattr(runner, "_wait", original_wait)
        raise KeyboardInterrupt

    monkeypatch.setattr(runner, "_wait", interrupt_once)
    with pytest.raises(HarnessError) as exc_info:
        runner.run(
            [sys.executable, "-c", _group_tree_script(parent_pid_file), str(child_pid_file)],
            phase=Phase.PYTEST,
            target="prod",
            backend="postgres",
        )
    assert "interrupt" in str(exc_info.value).lower()
    _assert_pid_gone(_wait_for_pid(child_pid_file))
