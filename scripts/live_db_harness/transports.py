from __future__ import annotations

import json
import os
import re
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

from .models import (
    BackendDefinition,
    ComposeService,
    HarnessError,
    Phase,
    TargetBackendDefinition,
    TargetDefinition,
)
from .process import (
    CommandRunner,
    ListenerInspector,
    ProcessDetails,
    ProcessInspector,
    PsutilListenerInspector,
    PsutilProcessInspector,
)


@dataclass(frozen=True)
class ServiceState:
    """The observable state of one Compose service."""

    exists: bool
    running: bool


@dataclass(frozen=True)
class ComposeInspection:
    """Resolved published ports and state for one Compose service."""

    profile: str
    service: str
    published_ports: tuple[int, ...]
    state: ServiceState

    @property
    def exists(self) -> bool:
        return self.state.exists

    @property
    def running(self) -> bool:
        return self.state.running


class ComposeInspector:
    """Read resolved Compose metadata and service state without mutating it."""

    def __init__(
        self,
        runner: CommandRunner | Any | None = None,
        listener_inspector: ListenerInspector | Any | None = None,
        *,
        command_runner: CommandRunner | Any | None = None,
    ) -> None:
        self.runner = command_runner or runner or CommandRunner()
        self.listener_inspector = listener_inspector or PsutilListenerInspector()

    def inspect(
        self,
        profile: str,
        service: str,
        *,
        target: str = "<compose>",
        backend: str = "<compose>",
    ) -> ComposeInspection:
        ports = self.published_ports(profile, service, target=target, backend=backend)
        state = self.service_state(profile, service, target=target, backend=backend)
        return ComposeInspection(profile, service, ports, state)

    def published_ports(
        self,
        profile: str,
        service: str,
        *,
        target: str = "<compose>",
        backend: str = "<compose>",
    ) -> tuple[int, ...]:
        config = self._run(
            ["docker", "compose", "--profile", profile, "config", "--format", "json"],
            target=target,
            backend=backend,
        )
        document = _parse_json(config, target=target, backend=backend)
        return _published_tcp_ports(document, service, target=target, backend=backend)

    def service_state(
        self,
        profile: str,
        service: str,
        *,
        target: str = "<compose>",
        backend: str = "<compose>",
    ) -> ServiceState:
        exists_result = self._run(
            ["docker", "compose", "ps", "--all", "--services", service],
            target=target,
            backend=backend,
        )
        existing_services = {line.strip() for line in exists_result.splitlines() if line.strip()}
        running_result = self._run(
            ["docker", "compose", "ps", "--status", "running", "--services", service],
            target=target,
            backend=backend,
        )
        running_services = {line.strip() for line in running_result.splitlines() if line.strip()}
        return ServiceState(exists=service in existing_services, running=service in running_services)

    def preflight(
        self,
        profile: str,
        service: str,
        *,
        target: str = "<compose>",
        backend: str = "<compose>",
    ) -> ComposeInspection:
        inspection = self.inspect(profile, service, target=target, backend=backend)
        if inspection.running:
            return inspection
        for port in inspection.published_ports:
            listener_pid = self.listener_inspector.pid_for_port(port)
            if listener_pid is not None:
                raise _error(
                    target,
                    backend,
                    f"Compose service {service!r} cannot start because port {port} is already in use.",
                    "Stop the unrelated listener or choose a target with non-conflicting ports.",
                )
        return inspection

    def _run(self, argv: list[str], *, target: str, backend: str) -> str:
        result = self.runner.run(
            argv,
            phase=Phase.TRANSPORT,
            target=target,
            backend=backend,
        )
        if isinstance(result, str):
            return result
        stdout = getattr(result, "stdout", None)
        if not isinstance(stdout, str):
            raise _error(
                target,
                backend,
                f"Command returned no readable output: {' '.join(argv)}.",
                "Check Docker and Compose availability, then retry.",
            )
        return stdout


def check_transport(
    target_name: str | Any | None = None,
    target: TargetDefinition | None = None,
    backend_name: str | None = None,
    backend: TargetBackendDefinition | BackendDefinition | None = None,
    *,
    target_backend: TargetBackendDefinition | None = None,
    suite_backend: BackendDefinition | None = None,
    suite: BackendDefinition | None = None,
    selection: Any | None = None,
    compose_service: ComposeService | None = None,
    listener_inspector: ListenerInspector | Any | None = None,
    process_inspector: ProcessInspector | Any | None = None,
    command_runner: CommandRunner | Any | None = None,
    compose_inspector: ComposeInspector | Any | None = None,
) -> None:
    """Validate the selected target's socket and transport identity.

    The first four positional arguments are the target/backend selection. A
    ``BackendSelection`` from ``config.py`` is accepted as the first argument,
    or through ``selection=``.
    """

    if selection is not None:
        if target_name is not None:
            raise TypeError("provide selection either positionally or by keyword")
        target_name = selection
    if target_backend is not None:
        if backend is not None:
            raise TypeError("provide only one of backend and target_backend")
        backend = target_backend
    suite_backend = suite_backend or suite
    target_name, target, backend_name, backend, suite_backend = _resolve_selection(
        target_name, target, backend_name, backend, suite_backend
    )
    listener = listener_inspector or PsutilListenerInspector()
    runner = command_runner or CommandRunner()

    if target.transport == "compose":
        service = compose_service or (suite_backend.compose if suite_backend is not None else None)
        if service is None:
            raise _error(
                target_name,
                backend_name,
                "The Compose backend has no selected service metadata.",
                "Configure the backend's Compose profile and service.",
            )
        inspector = compose_inspector or ComposeInspector(runner, listener)
        try:
            _invoke_compose(
                inspector.preflight,
                service.profile,
                service.service,
                target=target_name,
                backend=backend_name,
            )
        except HarnessError:
            raise
        except Exception as exc:
            raise _error(
                target_name,
                backend_name,
                f"Could not inspect Compose service {service.service!r}: {_safe_text(exc)}",
                "Check Docker Compose configuration and retry.",
            ) from None
        return

    if target.transport == "direct":
        # Direct targets perform a pure socket-reachability preflight here.
        # The authenticated connection is performed by the runner's later phase.
        host, port = _connection_endpoint(
            backend.connection,
            target=target_name,
            backend=backend_name,
        )
        _reject_external_compose_collision(
            port,
            suite_backend=suite_backend,
            compose_inspector=compose_inspector or ComposeInspector(runner, listener),
            target=target_name,
            backend=backend_name,
        )
        _require_reachable(host, port, target_name, backend_name)
        return

    identity = backend.tunnel
    if identity is None:
        raise _error(
            target_name,
            backend_name,
            "The SSH-tunnel backend has no tunnel identity.",
            "Configure the launchd label, destination, and forwarding tuple.",
        )
    connection_host, connection_port = _connection_endpoint(
        backend.connection,
        target=target_name,
        backend=backend_name,
    )
    client_host = identity.client_host if identity.client_host is not None else identity.local_host
    client_port = identity.client_port if identity.client_port is not None else identity.local_port
    if (connection_host, connection_port) != (client_host, client_port):
        raise _error(
            target_name,
            backend_name,
            (
                f"The selected connection endpoint {connection_host}:{connection_port} "
                f"does not match the SSH tunnel client endpoint "
                f"{client_host}:{client_port}."
            ),
            "Configure connection HOST and PORT to match the SSH tunnel client endpoint.",
        )
    _reject_external_compose_collision(
        identity.local_port,
        suite_backend=suite_backend,
        compose_inspector=compose_inspector or ComposeInspector(runner, listener),
        target=target_name,
        backend=backend_name,
    )
    _check_ssh_identity(
        identity,
        listener_pid=listener.pid_for_port(identity.local_port),
        process_inspector=process_inspector or PsutilProcessInspector(),
        command_runner=runner,
        target=target_name,
        backend=backend_name,
    )


def _resolve_selection(
    target_name: str | Any | None,
    target: TargetDefinition | None,
    backend_name: str | None,
    backend: TargetBackendDefinition | BackendDefinition | None,
    suite_backend: BackendDefinition | None,
) -> tuple[str, TargetDefinition, str, TargetBackendDefinition, BackendDefinition | None]:
    selection = target_name if not isinstance(target_name, str) else None
    if selection is not None:
        target_name = getattr(selection, "target_name", None)
        backend_name = backend_name or getattr(selection, "backend_name", None)
        target = target or getattr(selection, "target", None)
        if isinstance(backend, BackendDefinition):
            suite_backend = suite_backend or backend
            backend = None
        backend = backend or getattr(selection, "target_backend", None)
        if backend is None:
            backend = _selection_target_backend(selection, backend_name)
        suite_backend = suite_backend or getattr(selection, "suite", None)
    elif isinstance(backend, BackendDefinition):
        suite_backend = suite_backend or backend
        backend = None
    if not isinstance(target_name, str) or target is None or not isinstance(backend_name, str):
        raise _error(
            target_name if isinstance(target_name, str) else None,
            backend_name,
            "The target/backend selection is incomplete.",
            "Select a configured target and backend before checking transport.",
        )
    if not isinstance(backend, TargetBackendDefinition):
        raise _error(
            target_name,
            backend_name,
            "The target/backend selection is incomplete.",
            "Select a configured target and backend before checking transport.",
        )
    return target_name, target, backend_name, backend, suite_backend


def _selection_target_backend(selection: Any, backend_name: str | None) -> Any:
    if backend_name is None:
        return None
    selected_target = getattr(selection, "target", None)
    backends = getattr(selected_target, "backends", {})
    return backends.get(backend_name)


def _connection_endpoint(
    connection: Mapping[str, object],
    *,
    target: str | None = None,
    backend: str | None = None,
) -> tuple[str, int]:
    host_values = [
        value
        for key, value in connection.items()
        if key.lower() in {"host", "local_host"}
    ]
    port_values = [
        value
        for key, value in connection.items()
        if key.lower() in {"port", "local_port"}
    ]
    if host_values or port_values:
        if len(host_values) != 1 or not isinstance(host_values[0], str) or not host_values[0]:
            raise _error(
                target,
                backend,
                "The selected backend has no valid TCP host.",
                "Configure one non-empty connection HOST.",
            )
        if len(port_values) != 1:
            raise _error(
                target,
                backend,
                "The selected backend has no valid TCP port.",
                "Configure one connection PORT between 1 and 65535.",
            )
        try:
            port = int(port_values[0])
        except (TypeError, ValueError):
            port = 0
        if not 1 <= port <= 65535:
            raise _error(
                target,
                backend,
                "The selected backend has no valid TCP port.",
                "Configure one connection PORT between 1 and 65535.",
            )
        return host_values[0], port

    endpoint_urls = [
        value
        for key, value in connection.items()
        if key.lower() in {"connection_string", "spark_master", "spark_remote", "url"}
        and isinstance(value, str)
        and "://" in value
    ]
    if len(endpoint_urls) == 1:
        try:
            parsed = urlsplit(endpoint_urls[0])
            parsed_port = parsed.port
        except ValueError:
            parsed_port = None
            parsed = None
        if parsed is not None and parsed.hostname and parsed_port is not None:
            return parsed.hostname, parsed_port

    raise _error(
        target,
        backend,
        "The selected backend has no valid TCP connection endpoint.",
        "Configure HOST/PORT fields or one supported URL containing a host and port.",
    )


def _require_listener(listener: ListenerInspector | Any, port: int, target: str, backend: str) -> None:
    if listener.pid_for_port(port) is None:
        raise _error(
            target,
            backend,
            f"No process is listening on required port {port}.",
            "Start the selected database service or correct the target port.",
        )

def _require_reachable(host: str, port: int, target: str, backend: str) -> None:
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return
    except OSError as exc:
        raise _error(
            target,
            backend,
            f"Could not reach selected connection endpoint {host}:{port}: {_safe_text(exc)}",
            "Check that the selected database endpoint is reachable and retry.",
        ) from None


def _reject_external_compose_collision(
    port: int,
    *,
    suite_backend: BackendDefinition | None,
    compose_inspector: ComposeInspector | Any,
    target: str,
    backend: str,
) -> None:
    if suite_backend is None or suite_backend.compose is None:
        return
    service = suite_backend.compose
    try:
        ports_method = getattr(compose_inspector, "published_ports", None)
        if callable(ports_method):
            ports = _invoke_compose(
                ports_method,
                service.profile,
                service.service,
                target=target,
                backend=backend,
            )
        else:
            inspection = _invoke_compose(
                compose_inspector.inspect,
                service.profile,
                service.service,
                target=target,
                backend=backend,
            )
            ports = getattr(inspection, "published_ports", ())
        if port in tuple(ports):
            raise _error(
                target,
                backend,
                f"External target port {port} conflicts with the Compose published port.",
                "Choose a different external local port or Compose mapping.",
            )
    except HarnessError:
        raise
    except Exception as exc:
        raise _error(
            target,
            backend,
            f"Could not inspect Compose published ports: {_safe_text(exc)}",
            "Check resolved Compose configuration and retry.",
        ) from None

def _check_ssh_identity(
    identity: Any,
    *,
    listener_pid: int | None,
    process_inspector: ProcessInspector | Any,
    command_runner: CommandRunner | Any,
    target: str,
    backend: str,
) -> None:
    if listener_pid is None:
        raise _error(
            target,
            backend,
            f"No SSH listener is present on port {identity.local_port}.",
            "Start the configured launchd tunnel before running tests.",
        )

    ancestry = tuple(identity.process_ancestry)
    ancestry_text = " -> ".join(ancestry)
    supported_ancestries = (
        ("launchd", "ssh"),
        ("launchd", "autossh", "ssh"),
    )
    if ancestry not in supported_ancestries:
        supported_text = ", ".join(" -> ".join(chain) for chain in supported_ancestries)
        raise _error(
            target,
            backend,
            (
                f"Unsupported SSH process ancestry {ancestry_text}; "
                f"supported chains are {supported_text}."
            ),
            "Configure one of the supported launchd-managed SSH process chains.",
        )
    expected_forward = f"{identity.local_port}:{identity.remote_host}:{identity.remote_port}"
    expected_bound_forward = (
        f"{identity.local_host}:{identity.local_port}:{identity.remote_host}:{identity.remote_port}"
    )
    details_by_name: dict[str, tuple[int, ProcessDetails]] = {}
    pid = listener_pid
    for expected_name in reversed(ancestry):
        if expected_name == "launchd":
            if pid != 1:
                raise _error(
                    target,
                    backend,
                    f"The listener process ancestry does not match the expected {ancestry_text} chain.",
                    "Stop the unrelated listener and start the configured tunnel.",
                )
            pid = -1
            continue
        details = process_inspector.inspect(pid)
        if details is None or not details.cmdline:
            raise _error(
                target,
                backend,
                "The listener process identity could not be inspected.",
                "Restart the configured tunnel and retry.",
            )
        actual_name = Path(details.cmdline[0]).name
        if actual_name != expected_name:
            raise _error(
                target,
                backend,
                f"The listener process ancestry does not match the expected {ancestry_text} chain.",
                "Stop the unrelated listener and start the configured tunnel.",
            )
        details_by_name[expected_name] = (pid, details)
        if details.parent_pid is None:
            pid = -1
        else:
            pid = details.parent_pid
    if pid != -1:
        raise _error(
            target,
            backend,
            "The listener has an unexpected process ancestor.",
            "Use only the configured launchd-managed tunnel on this port.",
        )

    ssh_pid, ssh = details_by_name[ancestry[-1]]
    del ssh_pid
    if not (
        _has_forward(ssh.cmdline, expected_forward)
        or _has_forward(ssh.cmdline, expected_bound_forward)
    ) or identity.ssh_destination not in ssh.cmdline:
        raise _error(
            target,
            backend,
            "The SSH listener command does not match the configured destination or forwarding tuple.",
            "Start the tunnel with the configured destination and -L forwarding tuple.",
        )

    launchd_child_pid = details_by_name[ancestry[1]][0]
    launchd_result = command_runner.run(
        ["launchctl", "print", f"gui/{os.getuid()}/{identity.launchd_label}"],
        phase=Phase.TRANSPORT,
        target=target,
        backend=backend,
    )
    output = launchd_result if isinstance(launchd_result, str) else getattr(launchd_result, "stdout", "")
    if not isinstance(output, str):
        output = ""
    match = re.search(r"\bpid\s*=\s*(\d+)", output)
    if match is None or int(match.group(1)) != launchd_child_pid:
        raise _error(
            target,
            backend,
            "The launchd job PID does not own the expected SSH process ancestry.",
            "Check the launchd label and restart only the configured tunnel job.",
        )
    label_match = re.search(r"\blabel\s*=\s*([^\s]+)", output)
    if label_match is not None and label_match.group(1) != identity.launchd_label:
        raise _error(
            target,
            backend,
            "The launchd output label does not match the configured label.",
            "Use the configured launchd job label.",
        )


def _has_forward(cmdline: tuple[str, ...], expected: str) -> bool:
    for index, argument in enumerate(cmdline):
        if argument == expected and index > 0 and cmdline[index - 1] == "-L":
            return True
        if argument == f"-L{expected}":
            return True
    return False


def _parse_json(output: str, *, target: str, backend: str) -> Mapping[str, Any]:
    try:
        value = json.loads(output)
    except (TypeError, json.JSONDecodeError):
        raise _error(
            target,
            backend,
            "Compose returned invalid JSON configuration.",
            "Check the Compose file and retry.",
        ) from None
    if not isinstance(value, dict):
        raise _error(
            target,
            backend,
            "Compose returned an invalid configuration document.",
            "Check the Compose file and retry.",
        )
    return value


def _published_tcp_ports(
    document: Mapping[str, Any], service: str, *, target: str, backend: str
) -> tuple[int, ...]:
    services = document.get("services")
    service_config = services.get(service) if isinstance(services, dict) else None
    if not isinstance(service_config, dict):
        raise _error(
            target,
            backend,
            f"Compose service {service!r} is missing from resolved configuration.",
            "Check the selected Compose profile and service name.",
        )
    ports = service_config.get("ports", ())
    if not isinstance(ports, list):
        return ()
    published: list[int] = []
    for entry in ports:
        port = _published_port(entry)
        if port is not None:
            published.append(port)
    return tuple(dict.fromkeys(published))


def _published_port(entry: Any) -> int | None:
    if isinstance(entry, dict):
        protocol = str(entry.get("protocol", "tcp")).lower()
        value = entry.get("published")
        if protocol != "tcp" or value in (None, ""):
            return None
        try:
            port = int(value)
        except (TypeError, ValueError):
            return None
        return port if 1 <= port <= 65535 else None
    if not isinstance(entry, str):
        return None
    value, _, protocol = entry.partition("/")
    if protocol and protocol.lower() != "tcp":
        return None
    segments = value.split(":")
    try:
        # Compose short syntax is [HOST_IP:]HOST_PORT:CONTAINER_PORT.
        port = int(segments[-2] if len(segments) >= 2 else segments[-1])
    except (TypeError, ValueError):
        return None
    return port if 1 <= port <= 65535 else None


def _error(target: str | None, backend: str | None, detail: str, corrective: str) -> HarnessError:
    return HarnessError(target, backend, Phase.TRANSPORT, detail, corrective)


def _safe_text(value: object) -> str:
    return str(value).replace("\n", " ")[:300]


def _invoke_compose(method: Any, profile: str, service: str, **kwargs: str) -> Any:
    try:
        return method(profile, service, **kwargs)
    except TypeError as exc:
        if "unexpected keyword argument" not in str(exc):
            raise
        return method(profile, service)
