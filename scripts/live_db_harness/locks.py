from __future__ import annotations

import fcntl
import json
import os
import platform
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import HarnessError, Phase

_SEGMENT = re.compile(r"^[a-z0-9_-]+$")


def _segment(value: str, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    normalized = value.lower()
    try:
        normalized.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{name} contains unsupported characters") from exc
    if not _SEGMENT.fullmatch(normalized):
        raise ValueError(f"{name} must contain only letters, numbers, underscore, and hyphen")
    return normalized


def _cache_root() -> Path:
    if platform.system() == "Darwin":
        return Path.home() / "Library" / "Caches"
    configured = os.environ.get("XDG_CACHE_HOME")
    return Path(configured).expanduser() if configured else Path.home() / ".cache"


class BackendLock:
    """Coordinate one target/backend pair across processes using flock."""

    def __init__(
        self,
        target: str,
        backend: str,
        wait_timeout: float = 30.0,
        poll_interval: float = 0.05,
        *,
        timeout: float | None = None,
    ) -> None:
        self.target = _segment(target, "target")
        self.backend = _segment(backend, "backend")
        self.wait_timeout = wait_timeout if timeout is None else timeout
        self.poll_interval = max(0.001, poll_interval)
        self.path = _cache_root() / "mountainash-data" / "live-db" / self.target / f"{self.backend}.lock"
        self._file: Any = None
        self._acquired = False

    @property
    def lock_path(self) -> Path:
        return self.path

    def acquire(self) -> BackendLock:
        if self._acquired:
            return self
        if self.wait_timeout < 0:
            raise ValueError("wait_timeout must not be negative")
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            self.path.touch(mode=0o600, exist_ok=True)
            self._file = self.path.open("r+", encoding="utf-8")
        except OSError as exc:
            raise HarnessError(
                self.target,
                self.backend,
                Phase.TRANSPORT,
                f"Could not open lock file: {exc}",
                "Check cache directory permissions.",
            ) from None

        deadline = time.monotonic() + self.wait_timeout
        while True:
            try:
                fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    owner = self._read_owner()
                    self._close_file()
                    owner_detail = f" Owner PID: {owner}." if owner else ""
                    raise HarnessError(
                        self.target,
                        self.backend,
                        Phase.TRANSPORT,
                        f"Could not acquire backend lock.{owner_detail}",
                        "Wait for the other harness process to finish, then try again.",
                    ) from None
                time.sleep(min(self.poll_interval, max(0.0, deadline - time.monotonic())))
            except OSError as exc:
                self._close_file()
                raise HarnessError(
                    self.target,
                    self.backend,
                    Phase.TRANSPORT,
                    f"Could not acquire backend lock: {exc}",
                    "Check cache directory permissions.",
                ) from None

        # Truncate stale owner content only after flock grants exclusive access.
        started_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        record = {"pid": os.getpid(), "started_at": started_at}
        self._file.seek(0)
        self._file.truncate()
        json.dump(record, self._file, separators=(",", ":"))
        self._file.flush()
        os.fsync(self._file.fileno())
        self._acquired = True
        return self

    def _read_owner(self) -> int | None:
        try:
            self._file.seek(0)
            data = json.load(self._file)
            pid = data.get("pid")
            return pid if isinstance(pid, int) else None
        except (OSError, ValueError, TypeError, AttributeError):
            return None

    def release(self) -> None:
        if self._file is None:
            return
        try:
            if self._acquired:
                fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        finally:
            self._acquired = False
            self._close_file()

    def _close_file(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def __enter__(self) -> BackendLock:
        return self.acquire()

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.release()
