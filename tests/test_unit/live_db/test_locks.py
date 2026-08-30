from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from scripts.live_db_harness.locks import BackendLock
from scripts.live_db_harness.models import HarnessError


def _child_lock_code() -> str:
    return (
        "import pathlib, sys, time; "
        "from scripts.live_db_harness.locks import BackendLock; "
        "lock=BackendLock(sys.argv[1], sys.argv[2], wait_timeout=5); "
        "lock.acquire(); pathlib.Path(sys.argv[3]).write_text(str(lock.path)); "
        "time.sleep(float(sys.argv[4])); lock.release()"
    )


def _spawn_lock(tmp_path: Path, target: str = "prod", backend: str = "postgres", hold: float = 30) -> tuple[subprocess.Popen[str], Path]:
    ready = tmp_path / f"{target}-{backend}.ready"
    child = subprocess.Popen(
        [sys.executable, "-c", _child_lock_code(), target, backend, str(ready), str(hold)],
        text=True,
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not ready.exists():
        time.sleep(0.01)
    assert ready.exists()
    return child, ready


def _wait_for_exit(process: subprocess.Popen[str]) -> None:
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired as exc:
        raise AssertionError("owner process did not exit") from exc


def test_second_process_fails_and_reports_lock_owner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    owner, ready = _spawn_lock(tmp_path)
    try:
        lock = BackendLock("prod", "postgres", wait_timeout=0.1)
        with pytest.raises(HarnessError) as exc_info:
            lock.acquire()
        rendered = str(exc_info.value)
        assert str(owner.pid) in rendered
        assert "lock" in rendered.lower()
        assert "command" not in rendered.lower()
    finally:
        owner.send_signal(signal.SIGTERM)
        _wait_for_exit(owner)


def test_wait_lock_acquires_after_owner_exits(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    owner, _ = _spawn_lock(tmp_path, hold=0.2)
    lock = BackendLock("prod", "postgres", wait_timeout=5, poll_interval=0.02)
    lock.acquire()
    try:
        assert lock.path.exists()
    finally:
        lock.release()
    _wait_for_exit(owner)


def test_different_target_backend_pairs_do_not_block(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    first, _ = _spawn_lock(tmp_path, "prod", "postgres")
    second, _ = _spawn_lock(tmp_path, "prod", "mysql")
    try:
        assert first.poll() is None
        assert second.poll() is None
    finally:
        first.send_signal(signal.SIGTERM)
        second.send_signal(signal.SIGTERM)
        _wait_for_exit(first)
        _wait_for_exit(second)


def test_lock_file_never_contains_command_arguments(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    lock = BackendLock("prod", "postgres", wait_timeout=1)
    lock.acquire()
    try:
        record = json.loads(lock.path.read_text())
        assert set(record) == {"pid", "started_at"}
        assert record["pid"] == os.getpid()
        assert isinstance(record["started_at"], str)
    finally:
        lock.release()
