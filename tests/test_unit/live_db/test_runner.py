from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
import sys
import threading
import pytest

from scripts.live_db_harness.models import BackendResult, HarnessError, Phase, ResultStatus
from scripts.live_db_harness import runner as runner_module
from scripts.live_db_harness.runner import LiveDbRunner, run_many

 
 
 
 
 
 
 
 


def _loaded(runnable: dict[str, bool], *, max_parallel: int = 1) -> SimpleNamespace:
    suites = {
        name: SimpleNamespace(
            runnable=is_runnable,
            backlog=None if is_runnable else f"DEBT-{name}",
            selector=name,
            compose=SimpleNamespace(profile=name, service=name),
        )
        for name, is_runnable in runnable.items()
    }
    target = SimpleNamespace(
        transport="compose",
        max_parallel=max_parallel,
        test_timeout_seconds=30.0,
        allow_destructive_tests=False,
        backends={name: SimpleNamespace() for name in runnable},
    )
    return SimpleNamespace(settings=SimpleNamespace(targets={"docker": target}, backends=suites))


def _compose_selection() -> SimpleNamespace:
    return SimpleNamespace(
        target_name="docker",
        backend_name="postgres",
        target=SimpleNamespace(transport="compose", allow_destructive_tests=False),
        suite=SimpleNamespace(compose=SimpleNamespace(profile="postgres", service="postgres")),
    )


def test_test_one_uses_connection_check_for_preprovisioned_compose_service() -> None:
    selection = _compose_selection()
    selection.settings_parameters = SimpleNamespace()
    selection.auth_profile = SimpleNamespace()
    selection.secret_values = ()
    events: list[str] = []

    class NotTrackedComposeInspector:
        calls = 0

        def service_state(self, profile: str, service: str, **kwargs: str) -> object:
            self.calls += 1
            raise HarnessError(
                "docker",
                "postgres",
                Phase.TRANSPORT,
                "Compose service 'postgres' is not running.",
                "Start the selected service before running test.",
            )

    inspector = NotTrackedComposeInspector()

    class Backend:
        def connect(self, *, auth_profile: object) -> None:
            events.append("connect")

        def close(self) -> None:
            events.append("close")

    class Lock:
        def __enter__(self) -> Lock:
            return self

        def __exit__(self, *args: object) -> bool:
            return False

    runner = LiveDbRunner(
        (),
        lock_factory=lambda *args, **kwargs: Lock(),
        backend_factory=lambda parameters: Backend(),
        compose_inspector=inspector,
    )
    runner._selection = lambda target, backend: selection
    runner._run_pytest = lambda selection, timeout=None, **kwargs: events.append("pytest")

    runner.test_one("docker", "postgres")

    assert events == ["connect", "close", "pytest"]
    assert inspector.calls == 0


def test_check_one_still_runs_transport_check_before_connection() -> None:
    selection = _compose_selection()
    selection.settings_parameters = SimpleNamespace()
    selection.auth_profile = SimpleNamespace()
    selection.secret_values = ()
    events: list[str] = []

    class NotTrackedComposeInspector:
        calls = 0

        def service_state(self, profile: str, service: str, **kwargs: str) -> object:
            self.calls += 1
            raise AssertionError("check_one must not use the test-only service-state path")

    inspector = NotTrackedComposeInspector()

    class Backend:
        def connect(self, *, auth_profile: object) -> None:
            events.append("connect")

        def close(self) -> None:
            events.append("close")

    class Lock:
        def __enter__(self) -> Lock:
            return self

        def __exit__(self, *args: object) -> bool:
            return False

    runner = LiveDbRunner(
        (),
        lock_factory=lambda *args, **kwargs: Lock(),
        backend_factory=lambda parameters: Backend(),
        transport_checker=lambda **kwargs: events.append("transport"),
        compose_inspector=inspector,
    )
    runner._selection = lambda target, backend: selection

    runner.check_one("docker", "postgres")

    assert events == ["transport", "connect", "close"]
    assert inspector.calls == 0


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
    assert seen["argv"] == (
        sys.executable,
        "-m",
        "pytest",
        "tests/test_live_backends",
        "-k",
        "postgres",
        "-m",
        "integration",
    )
    env = seen["env"]
    assert "IBIS_TEST_POSTGRES_PASSWORD" not in env
    assert env["MOUNTAINASH_LIVE_DB_TARGET"] == "docker"
    assert env["MOUNTAINASH_LIVE_DB_BACKEND"] == "postgres"
    assert env["MOUNTAINASH_REQUIRE_LIVE_DB"] == "1"
    assert seen["timeout"] == 12


def test_effective_jobs_is_lower_cli_or_target_limit() -> None:
    active = 0
    maximum = 0
    guard = threading.Lock()
    started = threading.Event()
    release = threading.Event()
 
    def operation(name: str) -> BackendResult:
        nonlocal active, maximum
        with guard:
            active += 1
            maximum = max(maximum, active)
            if active == 2:
                started.set()
        release.wait(timeout=2)
        with guard:
            active -= 1
        return BackendResult(backend=name, status=ResultStatus.PASS)
 
    holder: dict[str, object] = {}
 
    def invoke() -> None:
        holder["results"] = run_many(("one", "two", "three"), operation, jobs=8, target_limit=2)
 
    thread = threading.Thread(target=invoke)
    thread.start()
    assert started.wait(timeout=1)
    with guard:
        assert maximum == 2
    release.set()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert [result.status for result in holder["results"]] == [ResultStatus.PASS] * 3
 
 
def test_default_continues_after_failure() -> None:
    loaded = _loaded({"one": True, "two": True, "three": True}, max_parallel=2)
    runner = LiveDbRunner(())
    runner._load = lambda target, backend: loaded
    started_one = threading.Event()
    started_two = threading.Event()
    started_three = threading.Event()
    release_one = threading.Event()
    release_two = threading.Event()
    release_three = threading.Event()
    results_holder: dict[str, object] = {}

    def operation(target: str, backend: str, *, wait_lock: float = 0.0) -> BackendResult:
        {"one": started_one, "two": started_two, "three": started_three}[backend].set()
        {"one": release_one, "two": release_two, "three": release_three}[backend].wait(timeout=2)
        return BackendResult(
            backend=backend,
            status=ResultStatus.FAIL if backend == "one" else ResultStatus.PASS,
            detail="failed" if backend == "one" else None,
        )

    runner.check_one = operation

    def invoke() -> None:
        results_holder["value"] = runner.aggregate("docker", "check", jobs=2)

    thread = threading.Thread(target=invoke)
    thread.start()
    assert started_one.wait(timeout=1)
    assert started_two.wait(timeout=1)
    assert not started_three.is_set()
    release_one.set()
    assert started_three.wait(timeout=1)
    release_two.set()
    release_three.set()
    thread.join(timeout=2)
    assert not thread.is_alive()
    results, code = results_holder["value"]
    assert [result.status for result in results] == [ResultStatus.FAIL, ResultStatus.PASS, ResultStatus.PASS]
    assert code == 1
 
 
def test_all_reports_pass_fail_and_unavailable_in_config_order() -> None:
    loaded = _loaded({"postgres": True, "mysql": True, "mssql": False})
    runner = LiveDbRunner(())
    runner._load = lambda target, backend: loaded
 
    def operation(target: str, backend: str, *, wait_lock: float = 0.0) -> BackendResult:
        return BackendResult(
            backend=backend,
            status=ResultStatus.FAIL if backend == "mysql" else ResultStatus.PASS,
            detail="connection failed" if backend == "mysql" else None,
        )
 
    runner.check_one = operation
    results, code = runner.aggregate("docker", "check")
    assert [result.backend for result in results] == ["postgres", "mysql", "mssql"]
    assert [result.status for result in results] == [
        ResultStatus.PASS,
        ResultStatus.FAIL,
        ResultStatus.UNAVAILABLE,
    ]
    assert results[1].detail == "connection failed"
    assert results[2].backlog == "DEBT-mssql"
    assert code == 1


def test_fail_fast_stops_new_backend_starts() -> None:
    calls: list[str] = []

    def operation(name: str) -> BackendResult:
        calls.append(name)
        return BackendResult(backend=name, status=ResultStatus.FAIL, detail="failed")

    results = run_many(("one", "two", "three"), operation, jobs=1, target_limit=1, fail_fast=True)
    assert calls == ["one"]
    assert [result.backend for result in results] == ["one"]


def test_cleanup_failure_preserves_prior_test_failure_detail() -> None:
    runner = LiveDbRunner(())
    runner._selection = lambda target, backend: _compose_selection()

    class Lease:
        def start(self) -> None:
            pass

        def close(self) -> None:
            raise RuntimeError("remove failed")

    runner.lease_factory = lambda **kwargs: Lease()
    prior = HarnessError("docker", "postgres", Phase.PYTEST, "pytest failed", "inspect pytest output")
    runner._check_unlocked = lambda selection, **kwargs: (_ for _ in ()).throw(prior)
    runner._run_pytest = lambda selection, timeout=None, **kwargs: None
    with pytest.raises(HarnessError) as caught:
        runner.run_one("docker", "postgres")
    assert "pytest failed" in str(caught.value)
    assert "Cleanup failed" in str(caught.value)
    assert "remove failed" in str(caught.value)


def test_unavailable_results_do_not_change_exit_status() -> None:
    loaded = _loaded({"postgres": True, "mssql": False})
    runner = LiveDbRunner(())
    runner._load = lambda target, backend: loaded
    runner.check_one = lambda target, backend, *, wait_lock=0.0: BackendResult(
        backend=backend, status=ResultStatus.PASS
    )
    results, code = runner.aggregate("docker", "check", jobs=1)
    assert [result.status for result in results] == [ResultStatus.PASS, ResultStatus.UNAVAILABLE]
    assert code == 0


def test_cleanup_failure_overrides_test_pass() -> None:
    loaded = _loaded({"postgres": True})
    runner = LiveDbRunner(())
    runner._load = lambda target, backend: loaded
    runner._selection = lambda target, backend: _compose_selection()
    runner._check_unlocked = lambda selection, **kwargs: None
    runner._run_pytest = lambda selection, timeout=None, **kwargs: None

    class Lease:
        def start(self) -> None:
            pass

        def close(self) -> None:
            raise RuntimeError("cleanup failed")

    runner.lease_factory = lambda **kwargs: Lease()
    results, code = runner.aggregate("docker", "run")
    assert results[0].status is ResultStatus.FAIL
    assert "cleanup failed" in (results[0].detail or "")
    assert code == 1


def test_fail_fast_marks_remaining_runnable_backends_not_run() -> None:
    loaded = _loaded({"one": True, "two": True, "three": True})
    runner = LiveDbRunner(())
    runner._load = lambda target, backend: loaded
    runner.check_one = lambda target, backend, *, wait_lock=0.0: BackendResult(
        backend=backend, status=ResultStatus.FAIL, detail="failed"
    )
    results, code = runner.aggregate("docker", "check", fail_fast=True)
    assert [result.status for result in results] == [
        ResultStatus.FAIL,
        ResultStatus.NOT_RUN,
        ResultStatus.NOT_RUN,
    ]
    assert all(result.detail == "stopped by --fail-fast" for result in results[1:])
    assert code == 1


def test_aggregate_interrupt_terminates_active_pytest_and_cleans_owned_services(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = _loaded({"postgres": True})
    runner = LiveDbRunner(())
    runner._load = lambda target, backend: loaded
    pytest_started = threading.Event()
    service_cleaned = threading.Event()
    terminated = threading.Event()

    def interrupted(target: str, backend: str, *, timeout=None, wait_lock=0.0) -> BackendResult:
        pytest_started.set()
        try:
            raise KeyboardInterrupt
        finally:
            service_cleaned.set()

    runner.run_one = interrupted
    monkeypatch.setattr(
        "scripts.live_db_harness.runner.CommandRunner.terminate_active",
        lambda: terminated.set(),
    )
    results, code = runner.aggregate("docker", "run")
    assert pytest_started.is_set()
    assert service_cleaned.is_set()
    assert terminated.is_set()
    assert code == 130
    assert results[0].status is ResultStatus.FAIL
    assert results[0].detail == "interrupted"

def test_failed_attempt_returns_nonzero() -> None:
    loaded = _loaded({"postgres": True})
    runner = LiveDbRunner(())
    runner._load = lambda target, backend: loaded
 
    def fail(target: str, backend: str, *, wait_lock: float = 0.0) -> BackendResult:
        return BackendResult(backend=backend, status=ResultStatus.FAIL, detail="failed")
 
    runner.check_one = fail
    results, code = runner.aggregate("docker", "check")
    assert results[0].status is ResultStatus.FAIL
    assert code == 1
def test_concurrent_backend_errors_use_their_own_secret_redactor() -> None:
    selections = {
        "one": SimpleNamespace(
            target_name="docker",
            backend_name="one",
            target=SimpleNamespace(transport="direct", allow_destructive_tests=True),
            suite=SimpleNamespace(),
            settings_parameters=SimpleNamespace(secret="one-secret"),
            auth_profile=SimpleNamespace(),
            secret_values=frozenset({"one-secret"}),
        ),
        "two": SimpleNamespace(
            target_name="docker",
            backend_name="two",
            target=SimpleNamespace(transport="direct", allow_destructive_tests=True),
            suite=SimpleNamespace(),
            settings_parameters=SimpleNamespace(secret="two-secret"),
            auth_profile=SimpleNamespace(),
            secret_values=frozenset({"two-secret"}),
        ),
    }
    barrier = threading.Barrier(2)
    runner = LiveDbRunner(())
    runner._selection = lambda target, backend: selections[backend]
    runner.transport_checker = lambda **kwargs: None
    runner.lock_factory = lambda *args, **kwargs: _NoopLock()

    def connect(parameters: SimpleNamespace) -> None:
        barrier.wait(timeout=2)
        raise RuntimeError(parameters.secret)
    runner.lock_factory = lambda *args, **kwargs: nullcontext()
    runner.backend_factory = connect
    errors: dict[str, str] = {}

    def invoke(backend: str) -> None:
        with pytest.raises(HarnessError) as caught:
            runner.check_one("docker", backend)
        errors[backend] = str(caught.value)

    threads = [threading.Thread(target=invoke, args=(backend,)) for backend in selections]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)
        assert not thread.is_alive()

    assert "[REDACTED]" in errors["one"]
    assert "one-secret" not in errors["one"]
    assert "[REDACTED]" in errors["two"]
    assert "two-secret" not in errors["two"]


def test_aggregate_interrupt_preserves_completed_backend_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = _loaded({"one": True, "two": True}, max_parallel=2)
    runner = LiveDbRunner(())
    runner._load = lambda target, backend: loaded
    second_started = threading.Event()
    release_interrupt = threading.Event()

    def operation(target: str, backend: str, *, wait_lock: float = 0.0) -> BackendResult:
        if backend == "one":
            return BackendResult(backend=backend, status=ResultStatus.PASS)
        second_started.set()
        release_interrupt.wait(timeout=2)
        raise KeyboardInterrupt

    runner.check_one = operation
    results_holder: dict[str, object] = {}
    original_wait = runner_module.wait
    wait_calls = 0

    def controlled_wait(futures, *, return_when):
        nonlocal wait_calls
        wait_calls += 1
        if wait_calls == 2:
            release_interrupt.set()
            raise KeyboardInterrupt
        return original_wait(futures, return_when=return_when)

    monkeypatch.setattr(runner_module, "wait", controlled_wait)

    def invoke() -> None:
        results_holder["value"] = runner.aggregate("docker", "check", jobs=2)

    thread = threading.Thread(target=invoke)
    thread.start()
    assert second_started.wait(timeout=2)
    thread.join(timeout=3)
    assert not thread.is_alive()

    results, code = results_holder["value"]
    assert results[0].status is ResultStatus.PASS
    assert results[1].status is ResultStatus.FAIL
    assert results[1].detail == "interrupted"
    assert code == 130

def test_aggregate_interrupt_from_command_runner_stops_scheduling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = _loaded({"one": True, "two": True, "three": True}, max_parallel=1)
    runner = LiveDbRunner(())
    runner._load = lambda target, backend: loaded
    selection = _compose_selection()
    selection.suite.selector = "this-test-is-interrupted"
    selection.target.test_timeout_seconds = 30.0
    selected: list[str] = []
    runner._selection = lambda target, backend: (selected.append(backend) or selection)
    events: list[str] = []

    class Lease:
        def start(self) -> None:
            events.append("start")

        def close(self) -> None:
            events.append("close")

    runner.lease_factory = lambda **kwargs: Lease()
    runner._check_unlocked = lambda selection, **kwargs: None
    interrupted_pids: list[int] = []

    def interrupt(self: object, process: object, timeout: float | None) -> tuple[str, str]:
        interrupted_pids.append(process.pid)  # type: ignore[attr-defined]
        raise KeyboardInterrupt

    monkeypatch.setattr(runner_module.CommandRunner, "_wait", interrupt)

    results, code = runner.aggregate("docker", "run", jobs=1)

    assert interrupted_pids
    assert selected == ["one"]
    assert events == ["start", "close"]
    assert [result.detail for result in results] == ["interrupted"] * 3
    assert code == 130
