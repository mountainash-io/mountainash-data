from __future__ import annotations

import json
import os
import socket

import pytest

from scripts.live_db_harness.models import (
    AuthDefinition,
    BackendDefinition,
    ComposeService,
    HarnessError,
    Phase,
    TargetBackendDefinition,
    TargetDefinition,
    TunnelIdentity,
)
from scripts.live_db_harness.process import ProcessDetails
from scripts.live_db_harness.transports import ComposeInspector, ServiceState, check_transport


class FakeListener:
    def __init__(self, listeners: dict[int, int]) -> None:
        self.listeners = listeners

    def pid_for_port(self, port: int) -> int | None:
        return self.listeners.get(port)


class FakeProcess:
    def __init__(self, processes: dict[int, ProcessDetails]) -> None:
        self.processes = processes
        self.inspected_pids: list[int] = []

    def inspect(self, pid: int) -> ProcessDetails | None:
        self.inspected_pids.append(pid)
        return self.processes.get(pid)


class FakeRunner:
    def __init__(self, output: str = "pid = 100\n") -> None:
        self.output = output
        self.commands: list[tuple[str, ...]] = []

    def run(self, argv: object, **kwargs: object) -> object:
        command = tuple(str(part) for part in argv)  # type: ignore[arg-type]
        self.commands.append(command)
        from scripts.live_db_harness.process import CompletedCommand

        return CompletedCommand(command, 0, self.output, "")


def _ssh_target(*, identity: TunnelIdentity | None = None) -> tuple[TargetDefinition, TargetBackendDefinition]:
    identity = identity or TunnelIdentity(
        launchd_label="com.example.postgres",
        ssh_destination="mpnas",
        local_host="127.0.0.1",
        local_port=25432,
        remote_host="127.0.0.1",
        remote_port=5432,
    )
    backend = TargetBackendDefinition(
        connection={"HOST": identity.local_host, "PORT": identity.local_port},
        auth=AuthDefinition(profile="password"),
        tunnel=identity,
    )
    return TargetDefinition(transport="ssh-tunnel", backends={"postgres": backend}), backend


def _tree(*, destination: str = "mpnas", forwarding: str = "25432:127.0.0.1:5432") -> FakeProcess:
    return FakeProcess(
        {
            300: ProcessDetails(("/usr/bin/ssh", "-L", forwarding, destination), 200),
            200: ProcessDetails(("/usr/local/bin/autossh", "-M", "0"), 1),
        }
    )


def _check_ssh(
    target: TargetDefinition,
    backend: TargetBackendDefinition,
    *,
    listener_pid: int = 300,
    process: FakeProcess | None = None,
    runner: FakeRunner | None = None,
) -> FakeRunner:
    runner = runner or FakeRunner("pid = 200\n")
    check_transport(
        "mpnas",
        target,
        "postgres",
        backend,
        listener_inspector=FakeListener({backend.tunnel.local_port: listener_pid}),  # type: ignore[union-attr]
        process_inspector=process or _tree(),
        command_runner=runner,
    )
    return runner




def test_ssh_identity_accepts_exact_label_destination_tuple_and_ancestry() -> None:
    target, backend = _ssh_target()
    process = _tree()

    runner = _check_ssh(target, backend, process=process)

    assert process.inspected_pids == [300, 200]
    assert runner.commands == [
        ("launchctl", "print", f"gui/{os.getuid()}/com.example.postgres")
    ]


def test_ssh_identity_rejects_wrong_launchd_label() -> None:
    target, backend = _ssh_target(
        identity=TunnelIdentity(
            launchd_label="com.example.wrong",
            ssh_destination="mpnas",
            local_host="127.0.0.1",
            local_port=25432,
            remote_host="127.0.0.1",
            remote_port=5432,
        )
    )

    with pytest.raises(HarnessError):
        _check_ssh(target, backend, runner=FakeRunner("pid = 999\n"))


def test_ssh_identity_rejects_wrong_destination() -> None:
    target, backend = _ssh_target()

    with pytest.raises(HarnessError):
        _check_ssh(target, backend, process=_tree(destination="other-host"))


def test_ssh_identity_rejects_wrong_remote_tuple() -> None:
    target, backend = _ssh_target()

    with pytest.raises(HarnessError):
        _check_ssh(target, backend, process=_tree(forwarding="25432:127.0.0.1:3306"))


def test_ssh_identity_rejects_wrong_ancestry() -> None:
    target, backend = _ssh_target()
    process = _tree()
    process.processes[200] = ProcessDetails(("/usr/local/bin/autossh", "-M", "0"), 42)

    with pytest.raises(HarnessError):
        _check_ssh(target, backend, process=process)

    assert process.inspected_pids == [300, 200]


def test_other_ssh_listener_on_same_port_is_rejected() -> None:
    target, backend = _ssh_target()
    process = _tree(destination="other-host")

    with pytest.raises(HarnessError):
        _check_ssh(target, backend, process=process)

@pytest.mark.parametrize(
    "connection",
    (
        {"HOST": "127.0.0.1", "PORT": 25433},
        {"HOST": "localhost", "PORT": 25432},
    ),
    ids=("wrong-port", "wrong-host"),
)
def test_ssh_identity_rejects_connection_endpoint_mismatch(connection: dict[str, object]) -> None:
    target, backend = _ssh_target()
    mismatched_backend = backend.model_copy(update={"connection": connection})

    with pytest.raises(HarnessError, match="does not match"):
        _check_ssh(target, mismatched_backend)


def test_direct_target_uses_socket_reachability_not_process_identity() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        server.listen()
        host, port = server.getsockname()
        backend = TargetBackendDefinition(
            connection={"HOST": host, "PORT": port},
            auth=AuthDefinition(profile="password"),
        )
        target = TargetDefinition(transport="direct", backends={"postgres": backend})

        check_transport(
            "external",
            target,
            "postgres",
            backend,
            listener_inspector=FakeListener({}),
            process_inspector=None,
            command_runner=None,
        )

def test_direct_target_rejects_unreachable_socket() -> None:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    host, port = probe.getsockname()
    probe.close()
    backend = TargetBackendDefinition(
        connection={"HOST": host, "PORT": port},
        auth=AuthDefinition(profile="password"),
    )
    target = TargetDefinition(transport="direct", backends={"postgres": backend})

    with pytest.raises(HarnessError, match="Could not reach"):
        check_transport(
            "external",
            target,
            "postgres",
            backend,
            listener_inspector=FakeListener({port: 999}),
            process_inspector=None,
            command_runner=None,
        )


def test_external_port_equal_to_compose_port_is_rejected() -> None:
    backend = TargetBackendDefinition(
        connection={"HOST": "127.0.0.1", "PORT": 25432},
        auth=AuthDefinition(profile="password"),
    )
    target = TargetDefinition(transport="direct", backends={"postgres": backend})
    suite = BackendDefinition(
        settings_profile="postgresql",
        selector="postgres",
        runnable=True,
        compose=ComposeService(profile="postgres", service="postgres"),
    )
    runner = FakeRunner(
        json.dumps(
            {
                "services": {
                    "postgres": {
                        "ports": [{"target": 5432, "published": 25432, "protocol": "tcp"}]
                    }
                }
            }
        )
    )

    with pytest.raises(HarnessError):
        check_transport(
            "docker",
            target,
            "postgres",
            backend,
            suite_backend=suite,
            listener_inspector=FakeListener({25432: 999}),
            process_inspector=None,
            command_runner=runner,
            compose_inspector=ComposeInspector(runner, FakeListener({25432: 999})),
        )

    assert all("down" not in command for command in runner.commands)

def test_service_state_distinguishes_stopped_existing_service() -> None:
    class StateRunner:
        def __init__(self) -> None:
            self.commands: list[tuple[str, ...]] = []

        def run(self, argv: object, **kwargs: object) -> object:
            command = tuple(str(part) for part in argv)  # type: ignore[arg-type]
            self.commands.append(command)
            from scripts.live_db_harness.process import CompletedCommand

            output = {
                ("docker", "compose", "ps", "--all", "--services", "postgres"): "postgres\n",
                ("docker", "compose", "ps", "--status", "running", "--services", "postgres"): "",
            }.get(command, "")
            return CompletedCommand(command, 0, output, "")

    runner = StateRunner()
    inspector = ComposeInspector(runner)

    assert inspector.service_state("postgres", "postgres") == ServiceState(exists=True, running=False)
    assert runner.commands == [
        ("docker", "compose", "ps", "--all", "--services", "postgres"),
        ("docker", "compose", "ps", "--status", "running", "--services", "postgres"),
    ]
