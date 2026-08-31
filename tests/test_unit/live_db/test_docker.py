from __future__ import annotations

from dataclasses import dataclass

import pytest

from scripts.live_db_harness.models import HarnessError, Phase
from scripts.live_db_harness.process import CompletedCommand
from scripts.live_db_harness.docker import DockerServiceLease, ServiceState


@dataclass
class FakeComposeInspector:
    states: list[ServiceState]

    def service_state(self, profile: str, service: str) -> ServiceState:
        if len(self.states) > 1:
            return self.states.pop(0)
        return self.states[0]


class FakeRunner:
    def __init__(self, *, failures: dict[tuple[str, ...], BaseException] | None = None) -> None:
        self.commands: list[tuple[str, ...]] = []
        self.failures = failures or {}

    def run(self, argv: object, **kwargs: object) -> CompletedCommand:
        command = tuple(str(part) for part in argv)  # type: ignore[arg-type]
        self.commands.append(command)
        assert "down" not in command
        failure = self.failures.get(command)
        if failure is not None:
            raise failure
        return CompletedCommand(command, 0, "", "")


def _lease(
    runner: FakeRunner,
    states: list[ServiceState],
) -> DockerServiceLease:
    return DockerServiceLease(
        "postgres",
        "postgres",
        runner=runner,
        compose_inspector=FakeComposeInspector(states),
        target="docker",
        backend="postgres",
    )


def test_up_starts_only_selected_service() -> None:
    runner = FakeRunner()
    lease = _lease(runner, [ServiceState(False, False), ServiceState(True, True)])

    lease.start()

    assert runner.commands == [("docker", "compose", "--profile", "postgres", "up", "-d", "--wait", "postgres")]


def test_unrelated_listener_stops_startup() -> None:
    runner = FakeRunner()
    lease = _lease(runner, [ServiceState(False, False), ServiceState(False, False)])

    with pytest.raises(HarnessError):
        lease.start()

    assert runner.commands == [
        ("docker", "compose", "--profile", "postgres", "up", "-d", "--wait", "postgres"),
        ("docker", "compose", "stop", "postgres"),
        ("docker", "compose", "rm", "-f", "postgres"),
    ]


def test_passing_run_removes_owned_service() -> None:
    runner = FakeRunner()
    lease = _lease(runner, [ServiceState(False, False), ServiceState(True, True)])

    with lease:
        pass

    assert runner.commands == [
        ("docker", "compose", "--profile", "postgres", "up", "-d", "--wait", "postgres"),
        ("docker", "compose", "stop", "postgres"),
        ("docker", "compose", "rm", "-f", "postgres"),
    ]


def test_failing_run_removes_owned_service() -> None:
    runner = FakeRunner()
    lease = _lease(runner, [ServiceState(False, False), ServiceState(True, True)])

    with pytest.raises(RuntimeError, match="test failed"):
        with lease:
            raise RuntimeError("test failed")

    assert runner.commands[-2:] == [
        ("docker", "compose", "stop", "postgres"),
        ("docker", "compose", "rm", "-f", "postgres"),
    ]


def test_partial_up_failure_removes_new_service() -> None:
    up = ("docker", "compose", "--profile", "postgres", "up", "-d", "--wait", "postgres")
    runner = FakeRunner(failures={up: HarnessError("docker", "postgres", Phase.TRANSPORT, "up failed", "retry")})
    lease = _lease(runner, [ServiceState(False, False), ServiceState(True, True)])

    with pytest.raises(HarnessError):
        lease.start()

    assert runner.commands[-2:] == [
        ("docker", "compose", "stop", "postgres"),
        ("docker", "compose", "rm", "-f", "postgres"),
    ]


def test_timeout_removes_owned_service_and_fails() -> None:
    up = ("docker", "compose", "--profile", "postgres", "up", "-d", "--wait", "postgres")
    runner = FakeRunner(failures={up: HarnessError("docker", "postgres", Phase.TRANSPORT, "timed out", "retry")})
    lease = _lease(runner, [ServiceState(False, False), ServiceState(True, True)])

    with pytest.raises(HarnessError):
        lease.start()

    assert runner.commands[-2:] == [
        ("docker", "compose", "stop", "postgres"),
        ("docker", "compose", "rm", "-f", "postgres"),
    ]


def test_interrupt_removes_owned_service_and_fails() -> None:
    up = ("docker", "compose", "--profile", "postgres", "up", "-d", "--wait", "postgres")
    runner = FakeRunner(failures={up: KeyboardInterrupt()})
    lease = _lease(runner, [ServiceState(False, False), ServiceState(True, True)])

    with pytest.raises(KeyboardInterrupt):
        lease.start()

    assert runner.commands[-2:] == [
        ("docker", "compose", "stop", "postgres"),
        ("docker", "compose", "rm", "-f", "postgres"),
    ]


def test_stop_failure_still_attempts_rm() -> None:
    stop = ("docker", "compose", "stop", "postgres")
    runner = FakeRunner(failures={stop: HarnessError("docker", "postgres", Phase.CLEANUP, "stop failed", "retry")})
    lease = _lease(runner, [ServiceState(False, False), ServiceState(True, True)])
    lease.start()

    with pytest.raises(HarnessError):
        lease.close()

    assert runner.commands[-2:] == [stop, ("docker", "compose", "rm", "-f", "postgres")]


def test_rm_failure_returns_cleanup_error() -> None:
    rm = ("docker", "compose", "rm", "-f", "postgres")
    runner = FakeRunner(failures={rm: HarnessError("docker", "postgres", Phase.CLEANUP, "rm failed", "retry")})
    lease = _lease(runner, [ServiceState(False, False), ServiceState(True, True)])
    lease.start()

    with pytest.raises(HarnessError, match="rm failed"):
        lease.close()


def test_preexisting_service_remains_running() -> None:
    runner = FakeRunner()
    lease = _lease(runner, [ServiceState(True, True), ServiceState(True, True)])

    with lease:
        pass

    assert runner.commands == [
        ("docker", "compose", "--profile", "postgres", "up", "-d", "--wait", "postgres"),
    ]


def test_stopped_preexisting_service_is_not_removed() -> None:
    runner = FakeRunner()
    lease = _lease(runner, [ServiceState(True, False), ServiceState(True, True)])

    with lease:
        pass

    assert runner.commands == [
        ("docker", "compose", "--profile", "postgres", "up", "-d", "--wait", "postgres"),
    ]


def test_ambiguous_post_start_state_removes_possible_owned_service() -> None:
    runner = FakeRunner()
    lease = _lease(runner, [ServiceState(False, False), ServiceState(True, False)])

    with pytest.raises(HarnessError, match="was not running after startup"):
        lease.start()

    assert runner.commands == [
        ("docker", "compose", "--profile", "postgres", "up", "-d", "--wait", "postgres"),
        ("docker", "compose", "stop", "postgres"),
        ("docker", "compose", "rm", "-f", "postgres"),
    ]


def test_other_services_receive_no_commands() -> None:
    runner = FakeRunner()
    lease = _lease(runner, [ServiceState(False, False), ServiceState(True, True)])

    with lease:
        pass

    assert all("mysql" not in command and "oracle" not in command for command in runner.commands)
