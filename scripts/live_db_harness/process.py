from __future__ import annotations

import os
import re
import shlex
import signal
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Iterable, Mapping, Protocol, Sequence
from urllib.parse import quote, quote_plus

import psutil

from .models import HarnessError, Phase


_REDACTED = "[REDACTED]"
_URL_AUTHORITY = re.compile(
    r"(?P<prefix>[A-Za-z][A-Za-z0-9+.-]*://)(?P<authority>[^/?#\s]*)(?P<suffix>[/\?#][^\s]*)?"
)


class Redactor:
    """Redact configured secrets from text and URL credentials."""

    def __init__(self, secrets: Iterable[str] = ()) -> None:
        values = {secret for secret in secrets if isinstance(secret, str) and secret}
        self.secrets = tuple(sorted(values, key=len, reverse=True))
        encoded: set[str] = set()
        for secret in self.secrets:
            encoded.add(quote(secret, safe=""))
            encoded.add(quote_plus(secret, safe=""))
        self._values = tuple(sorted(encoded | set(self.secrets), key=len, reverse=True))

    def redact(self, value: str) -> str:
        if not isinstance(value, str) or not value:
            return value

        def redact_authority(match: re.Match[str]) -> str:
            authority = match.group("authority")
            if "@" not in authority:
                return match.group(0)
            _, host = authority.rsplit("@", 1)
            return f"{match.group('prefix')}{_REDACTED}@{host}{match.group('suffix') or ''}"

        value = _URL_AUTHORITY.sub(redact_authority, value)
        for secret in self._values:
            if re.search(r"%[0-9A-Fa-f]{2}", secret):
                value = re.sub(re.escape(secret), _REDACTED, value, flags=re.IGNORECASE)
            else:
                value = value.replace(secret, _REDACTED)
        return value

    __call__ = redact


@dataclass(frozen=True)
class CompletedCommand:
    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str

    @property
    def command(self) -> tuple[str, ...]:
        return self.argv


class CommandRunner:
    """Run commands in isolated process groups and redact their diagnostics."""

    _active_lock = threading.RLock()
    _active: dict[int, subprocess.Popen[str]] = {}
    _grace_period = 1.0

    def __init__(self, redactor: Redactor | None = None, *, grace_period: float = 1.0) -> None:
        self.redactor = redactor or Redactor()
        self._grace_period = max(0.01, grace_period)

    def run(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        phase: Phase,
        target: str,
        backend: str,
    ) -> CompletedCommand:
        command = tuple(str(part) for part in argv)
        if not command:
            raise HarnessError(target, backend, phase, "The command is empty.", "Provide a command to run.")
        preview = self._preview(command)
        process: subprocess.Popen[str] | None = None
        stdout = ""
        stderr = ""
        try:
            with self._active_lock:
                process = subprocess.Popen(
                    command,
                    shell=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=dict(env) if env is not None else None,
                    start_new_session=True,
                )
                self._active[process.pid] = process
            stdout, stderr = self._wait(process, timeout)
        except subprocess.TimeoutExpired as exc:
            stdout = self._output(exc.stdout)
            stderr = self._output(exc.stderr)
            if process is not None:
                self._terminate(process)
                stdout, stderr = self._communicate(process, stdout, stderr)
            raise HarnessError(
                target,
                backend,
                phase,
                f"Command timed out after {timeout:g} seconds: {preview}. "
                f"stdout: {self.redactor.redact(stdout)} stderr: {self.redactor.redact(stderr)}",
                "Check the target and increase the timeout only when the command is expected to take longer.",
            ) from None
        except KeyboardInterrupt:
            if process is not None:
                self._terminate(process)
                stdout, stderr = self._communicate(process, "", "")
            raise HarnessError(
                target,
                backend,
                phase,
                f"Command interrupted: {preview}. stdout: {self.redactor.redact(stdout)} "
                f"stderr: {self.redactor.redact(stderr)}",
                "Run the command again after checking the target state.",
            ) from None
        except OSError as exc:
            raise HarnessError(
                target,
                backend,
                phase,
                f"Could not run command {preview}: {self.redactor.redact(str(exc))}",
                "Check that the command exists and that the target is available.",
            ) from None
        finally:
            if process is not None:
                with self._active_lock:
                    self._active.pop(process.pid, None)

        result = CompletedCommand(
            tuple(self.redactor.redact(part) for part in command),
            process.returncode,
            self.redactor.redact(stdout),
            self.redactor.redact(stderr),
        )
        if process.returncode != 0:
            raise HarnessError(
                target,
                backend,
                phase,
                f"Command failed with exit code {process.returncode}: {preview}. "
                f"stdout: {result.stdout} stderr: {result.stderr}",
                "Check the command output and target configuration.",
            )
        return result

    def _wait(self, process: subprocess.Popen[str], timeout: float | None) -> tuple[str, str]:
        stdout, stderr = process.communicate(timeout=timeout)
        return stdout or "", stderr or ""

    @staticmethod
    def _output(value: str | bytes | None) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode(errors="replace")
        return value

    def _preview(self, argv: Sequence[str]) -> str:
        return shlex.join(self.redactor.redact(str(part)) for part in argv)

    def _communicate(self, process: subprocess.Popen[str], stdout: str, stderr: str) -> tuple[str, str]:
        try:
            out, err = process.communicate(timeout=self._grace_period)
            return out or stdout, err or stderr
        except subprocess.TimeoutExpired as exc:
            return self._output(exc.stdout) or stdout, self._output(exc.stderr) or stderr

    @staticmethod
    def _terminate_group(process: subprocess.Popen[str], grace_period: float) -> None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return

        deadline = time.monotonic() + grace_period
        try:
            process.wait(timeout=max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            process.wait()
        except ChildProcessError:
            pass

    def _terminate(self, process: subprocess.Popen[str]) -> None:
        self._terminate_group(process, self._grace_period)

    @classmethod
    def terminate_active(cls) -> None:
        with cls._active_lock:
            processes = tuple(cls._active.values())
        for process in processes:
            cls._terminate_group(process, cls._grace_period)
            try:
                process.communicate(timeout=cls._grace_period)
            except subprocess.TimeoutExpired:
                cls._terminate_group(process, cls._grace_period)


@dataclass(frozen=True)
class ProcessDetails:
    """The process data needed to validate a transport identity."""

    cmdline: tuple[str, ...]
    parent_pid: int | None


class ListenerInspector(Protocol):
    """Resolve the PID listening on a local TCP port."""

    def pid_for_port(self, port: int) -> int | None:
        """Return the listener PID for ``port``, or ``None`` if absent."""


class PsutilListenerInspector:
    """Resolve local TCP listeners with psutil."""

    def pid_for_port(self, port: int) -> int | None:
        for connection in psutil.net_connections(kind="tcp"):
            if connection.status != psutil.CONN_LISTEN:
                continue
            address = connection.laddr
            if not address:
                continue
            local_port = address.port if hasattr(address, "port") else address[1]
            if local_port == port and isinstance(connection.pid, int) and connection.pid > 0:
                return connection.pid
        return None


class ProcessInspector(Protocol):
    """Read the command line and parent PID for a process."""

    def inspect(self, pid: int) -> ProcessDetails | None:
        """Return process details, or ``None`` if the process is unavailable."""


class PsutilProcessInspector:
    """Read process identity data with psutil."""

    def inspect(self, pid: int) -> ProcessDetails | None:
        if pid <= 0:
            return None
        try:
            process = psutil.Process(pid)
            parent = process.parent()
            parent_pid = parent.pid if parent is not None else None
            return ProcessDetails(tuple(process.cmdline()), parent_pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return None
