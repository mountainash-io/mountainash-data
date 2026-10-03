from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

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
    PsutilListenerInspector,
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
    command_runner: CommandRunner | Any | None = None,
    compose_inspector: ComposeInspector | Any | None = None,
) -> None:
    """Validate a harness-managed Compose service before startup.

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

    raise _error(
        target_name, backend_name,
        "Transport preflight applies only to harness-managed Compose services.",
        "Connect to operator-managed endpoints through the database backend.",
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
