from __future__ import annotations

import copy
import json
import os
import sys
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from mountainash_auth_client import AUTH_REGISTRY
from mountainash_data import IbisBackend
from mountainash_data.core.settings import DATABASES_REGISTRY

from .config import (
    BackendSelection,
    build_backend_selection,
    default_config_files,
    load_unresolved_harness,
    reject_unknown_keys,
)
from .docker import DockerServiceLease
from .locks import BackendLock
from .models import BackendResult, HarnessError, Phase, ResultStatus
from .process import CommandRunner, Redactor
from .transports import ComposeInspector, check_transport

LEGACY_PREFIX = "IBIS_TEST_"
CONTEXT_KEYS = {
    "MOUNTAINASH_LIVE_DB_CONFIG",
    "MOUNTAINASH_LIVE_DB_TARGET",
    "MOUNTAINASH_LIVE_DB_BACKEND",
    "MOUNTAINASH_REQUIRE_LIVE_DB",
}


def _error(target: str, backend: str, phase: Phase, detail: str, action: str) -> HarnessError:
    return HarnessError(target, backend, phase, detail, action)


def run_many(
    backends: Iterable[str],
    operation: Callable[[str], BackendResult],
    *,
    jobs: int | None = None,
    target_limit: int = 1,
    fail_fast: bool = False,
    partial_results: dict[str, BackendResult] | None = None,
) -> list[BackendResult]:
    """Run named backend operations with bounded, completion-driven scheduling."""
    names = tuple(backends)
    limit = 1 if jobs is None else min(jobs, target_limit)
    limit = max(1, limit)
    if not names:
        return []

    results: dict[str, BackendResult] = {}
    pending_index = 0
    in_flight: dict[Future[BackendResult], str] = {}
    stopped = False
    executor = ThreadPoolExecutor(max_workers=limit)

    def record(name: str, result: BackendResult) -> None:
        results[name] = result
        if partial_results is not None:
            partial_results[name] = result

    def record_done_futures() -> None:
        for future, name in tuple(in_flight.items()):
            if not future.done():
                continue
            in_flight.pop(future)
            try:
                result = future.result()
            except KeyboardInterrupt:
                continue
            except BaseException as exc:
                result = BackendResult(backend=name, status=ResultStatus.FAIL, detail=str(exc))
            record(name, result)

    try:
        while pending_index < len(names) and len(in_flight) < limit:
            name = names[pending_index]
            pending_index += 1
            in_flight[executor.submit(operation, name)] = name

        while in_flight:
            try:
                completed, _ = wait(tuple(in_flight), return_when=FIRST_COMPLETED)
            except KeyboardInterrupt:
                CommandRunner.terminate_active()
                record_done_futures()
                for future in tuple(in_flight):
                    try:
                        future.result()
                    except BaseException:
                        pass
                raise
            for future in completed:
                name = in_flight.pop(future)
                try:
                    result = future.result()
                except KeyboardInterrupt:
                    CommandRunner.terminate_active()
                    record_done_futures()
                    for running in tuple(in_flight):
                        try:
                            running.result()
                        except BaseException:
                            pass
                    raise
                except BaseException as exc:
                    result = BackendResult(backend=name, status=ResultStatus.FAIL, detail=str(exc))
                record(name, result)
                if fail_fast and result.status is ResultStatus.FAIL:
                    stopped = True
            while not stopped and pending_index < len(names) and len(in_flight) < limit:
                name = names[pending_index]
                pending_index += 1
                in_flight[executor.submit(operation, name)] = name
    finally:
        executor.shutdown(wait=True, cancel_futures=True)
    return [results[name] for name in names if name in results]


class LiveDbRunner:
    """Orchestrate one explicit target and its configured backend suites."""

    def __init__(
        self,
        config_files: tuple[Path, ...],
        *,
        command_runner: Any | None = None,
        lock_factory: Callable[..., Any] = BackendLock,
        backend_factory: Callable[..., Any] = IbisBackend,
        transport_checker: Callable[..., Any] = check_transport,
        lease_factory: Callable[..., Any] = DockerServiceLease,
        compose_inspector: Any | None = None,
        redactor: Redactor | None = None,
    ) -> None:
        self.config_files = tuple(Path(path) for path in config_files)
        self.redactor = redactor or Redactor()
        self.command_runner = command_runner or CommandRunner(self.redactor)
        self.lock_factory = lock_factory
        self.backend_factory = backend_factory
        self.transport_checker = transport_checker
        self.lease_factory = lease_factory
        self.compose_inspector = compose_inspector or ComposeInspector(self.command_runner)
    def _scoped_command_runner(self, selection: BackendSelection) -> Any:
        redactor = Redactor(getattr(selection, "secret_values", ()))
        if isinstance(self.command_runner, CommandRunner):
            return CommandRunner(redactor, grace_period=self.command_runner._grace_period)
        runner = copy.copy(self.command_runner)
        if hasattr(runner, "redactor"):
            runner.redactor = redactor
        return runner

    def _scoped_compose_inspector(self, command_runner: Any) -> Any:
        if isinstance(self.compose_inspector, ComposeInspector):
            return ComposeInspector(
                command_runner,
                listener_inspector=self.compose_inspector.listener_inspector,
            )
        inspector = copy.copy(self.compose_inspector)
        if hasattr(inspector, "runner"):
            inspector.runner = command_runner
        return inspector
    def _load(self, target: str, backend: str | None) -> Any:
        return load_unresolved_harness(
            self.config_files, selected_target=target, selected_backend=backend
        )


    def _selection(self, target: str, backend: str) -> BackendSelection:
        return build_backend_selection(self._load(target, backend))

    def status(self, target: str) -> int:
        loaded = self._load(target, None)
        settings = loaded.settings
        target_def = settings.targets.get(target)
        if target_def is None:
            raise _error(target, None, Phase.CONFIGURATION, "Unknown target.", "Select a configured target.")
        print(f"TARGET {target}: {len(target_def.backends)} configured")
        for backend_name, target_backend in target_def.backends.items():
            suite = settings.backends.get(backend_name)
            if suite is None:
                raise _error(target, backend_name, Phase.CONFIGURATION, "The target references an unknown backend.", "Fix the target backend inventory.")
            self._validate_unresolved_profile(target, backend_name, target_backend, suite)
            if not suite.runnable:
                print(f"{backend_name:<14} UNAVAILABLE  {suite.backlog}")
                continue
            print(f"{backend_name:<14} PASS")
        return 0

    def _validate_unresolved_profile(self, target_name: str, backend_name: str, target_backend: Any, suite: Any) -> None:
        try:
            backend_class = DATABASES_REGISTRY.get_settings_class(suite.settings_profile)
            backend_spec = DATABASES_REGISTRY.get_descriptor(suite.settings_profile)
            auth_class = AUTH_REGISTRY.get_settings_class(target_backend.auth.profile)
        except KeyError as exc:
            raise _error(
                target_name,
                backend_name,
                Phase.CONFIGURATION,
                f"Unknown registered profile: {exc.args[0]}",
                "Use registered backend and authentication profiles.",
            ) from None
        if auth_class not in backend_spec.supported_auth:
            raise _error(
                target_name,
                backend_name,
                Phase.CONFIGURATION,
                f"Database profile {suite.settings_profile!r} does not support authentication profile {target_backend.auth.profile!r}.",
                "Use a supported authentication profile.",
            )
        reject_unknown_keys(
            target_backend.connection,
            backend_class,
            target=target_name,
            backend=backend_name,
            section="connection",
        )
        reject_unknown_keys(
            target_backend.auth.values,
            auth_class,
            target=target_name,
            backend=backend_name,
            section="authentication",
        )
        try:
            backend_class(**target_backend.connection)
            auth_class(**target_backend.auth.values)
        except Exception:
            raise _error(
                target_name,
                backend_name,
                Phase.CONFIGURATION,
                "Unable to validate the selected profile fields.",
                "Fix the registered connection and authentication fields.",
            ) from None

    def _require_destructive_opt_in(self, selection: BackendSelection) -> None:
        if selection.target.transport != "compose" and not selection.target.allow_destructive_tests:
            raise _error(
                selection.target_name,
                selection.backend_name,
                Phase.CONFIGURATION,
                "The external target does not allow destructive tests.",
                "Set allow_destructive_tests = true for this target.",
            )

    def _compose_parts(self, target: str, backend: str) -> tuple[Any, Any, Any]:
        loaded = self._load(target, backend)
        target_def = loaded.settings.targets.get(target)
        suite = loaded.settings.backends.get(backend)
        target_backend = target_def.backends.get(backend) if target_def is not None else None
        if target_def is None or suite is None or target_backend is None:
            raise _error(
                target,
                backend,
                Phase.CONFIGURATION,
                "The selected backend is not configured for the target.",
                "Select a backend listed for the target.",
            )
        if not suite.runnable:
            raise _error(
                target,
                backend,
                Phase.CONFIGURATION,
                f"Backend {backend!r} is unavailable.",
                f"Complete backlog item {suite.backlog or '<none>'} first.",
            )
        if target_def.transport != "compose":
            raise _error(
                target,
                backend,
                Phase.CONFIGURATION,
                "This command requires a Compose target.",
                "Select a Compose target.",
            )
        if suite.compose is None:
            raise _error(
                target,
                backend,
                Phase.CONFIGURATION,
                "The selected backend has no Compose service.",
                "Configure Compose profile and service metadata.",
            )
        return target_def, target_backend, suite

    def _compose_service(self, selection: BackendSelection) -> Any:
        service = selection.suite.compose
        if service is None:
            raise _error(selection.target_name, selection.backend_name, Phase.CONFIGURATION, "The selected backend has no Compose service.", "Configure Compose profile and service metadata.")
        return service

    def _check_compose_service_available(
        self,
        selection: BackendSelection,
        *,
        compose_inspector: Any | None = None,
        redactor: Redactor | None = None,
    ) -> None:
        service = self._compose_service(selection)
        inspector = compose_inspector or self.compose_inspector
        try:
            state = inspector.service_state(
                service.profile,
                service.service,
                target=selection.target_name,
                backend=selection.backend_name,
            )
        except HarnessError as exc:
            if redactor is None:
                raise
            raise _error(
                exc.target,
                exc.backend,
                exc.phase,
                redactor(exc.detail),
                exc.corrective_action,
            ) from None
        except Exception as exc:
            detail = redactor(str(exc)) if redactor is not None else str(exc)
            raise _error(
                selection.target_name,
                selection.backend_name,
                Phase.TRANSPORT,
                f"Could not inspect Compose service: {detail}",
                "Check Docker Compose state and retry.",
            ) from None
        if not getattr(state, "running", False):
            raise _error(
                selection.target_name,
                selection.backend_name,
                Phase.TRANSPORT,
                f"Compose service {service.service!r} is not running.",
                "Start the selected service before running test.",
            )

    def _check_transport(
        self,
        selection: BackendSelection,
        *,
        startup: bool = False,
        command_runner: Any | None = None,
        compose_inspector: Any | None = None,
        redactor: Redactor | None = None,
    ) -> None:
        runner = command_runner or self.command_runner
        inspector = compose_inspector or self._scoped_compose_inspector(runner)
        if selection.target.transport == "compose" and not startup:
            self._check_compose_service_available(
                selection,
                compose_inspector=inspector,
                redactor=redactor,
            )
            return
        self.transport_checker(
            selection=selection,
            compose_inspector=inspector,
            command_runner=runner,
        )

    def _connection_check(
        self,
        selection: BackendSelection,
        *,
        redactor: Redactor | None = None,
    ) -> None:
        selection_redactor = redactor or Redactor(selection.secret_values)
        backend = None
        try:
            backend = self.backend_factory(selection.settings_parameters)
            backend.connect(auth_profile=selection.auth_profile)
        except HarnessError:
            raise
        except Exception as exc:
            raise _error(
                selection.target_name,
                selection.backend_name,
                Phase.AUTHENTICATION,
                f"Could not connect to the backend: {selection_redactor(str(exc))}",
                "Check the target endpoint and authentication settings.",
            ) from None
        finally:
            if backend is not None:
                try:
                    backend.close()
                except Exception as exc:
                    raise _error(
                        selection.target_name,
                        selection.backend_name,
                        Phase.CLEANUP,
                        f"Could not close the backend connection: {selection_redactor(str(exc))}",
                        "Check the backend driver and retry.",
                    ) from None

    def _check_unlocked(
        self,
        selection: BackendSelection,
        *,
        test_mode: bool = False,
        redactor: Redactor | None = None,
        command_runner: Any | None = None,
        compose_inspector: Any | None = None,
    ) -> None:
        self._require_destructive_opt_in(selection)
        if not (test_mode and selection.target.transport == "compose"):
            self._check_transport(
                selection,
                startup=not test_mode,
                command_runner=command_runner,
                compose_inspector=compose_inspector,
                redactor=redactor,
            )
        self._connection_check(selection, redactor=redactor)

    def _lock(self, selection: BackendSelection, wait_lock: float) -> Any:
        return self.lock_factory(selection.target_name, selection.backend_name, wait_timeout=wait_lock)

    def check_one(self, target: str, backend: str, *, wait_lock: float = 0.0) -> BackendResult:
        selection = self._selection(target, backend)
        command_runner = self._scoped_command_runner(selection)
        redactor = command_runner.redactor if hasattr(command_runner, "redactor") else Redactor(selection.secret_values)
        compose_inspector = self._scoped_compose_inspector(command_runner)
        with self._lock(selection, wait_lock):
            self._check_unlocked(
                selection,
                redactor=redactor,
                command_runner=command_runner,
                compose_inspector=compose_inspector,
            )
        return BackendResult(backend=backend, status=ResultStatus.PASS)

    def _pytest_env(self, selection: BackendSelection) -> dict[str, str]:
        env = dict(os.environ)
        for key in tuple(env):
            if key.startswith(LEGACY_PREFIX) or key in CONTEXT_KEYS:
                env.pop(key, None)
        env.update({
            "MOUNTAINASH_LIVE_DB_CONFIG": json.dumps([str(path) for path in self.config_files]),
            "MOUNTAINASH_LIVE_DB_TARGET": selection.target_name,
            "MOUNTAINASH_LIVE_DB_BACKEND": selection.backend_name,
            "MOUNTAINASH_REQUIRE_LIVE_DB": "1",
        })
        return env

    def _run_pytest(
        self,
        selection: BackendSelection,
        *,
        timeout: float | None = None,
        command_runner: Any | None = None,
    ) -> None:
        timeout_seconds = selection.target.test_timeout_seconds if timeout is None else timeout
        (command_runner or self.command_runner).run(
            [sys.executable, "-m", "pytest", "tests/test_live_backends", "-k", selection.suite.selector, "-m", "integration"],
            env=self._pytest_env(selection),
            timeout=timeout_seconds,
            phase=Phase.PYTEST,
            target=selection.target_name,
            backend=selection.backend_name,
        )

    def test_one(self, target: str, backend: str, *, timeout: float | None = None, wait_lock: float = 0.0) -> BackendResult:
        selection = self._selection(target, backend)
        command_runner = self._scoped_command_runner(selection)
        redactor = command_runner.redactor if hasattr(command_runner, "redactor") else Redactor(selection.secret_values)
        compose_inspector = self._scoped_compose_inspector(command_runner)
        with self._lock(selection, wait_lock):
            self._check_unlocked(
                selection,
                test_mode=True,
                redactor=redactor,
                command_runner=command_runner,
                compose_inspector=compose_inspector,
            )
            self._run_pytest(selection, timeout=timeout, command_runner=command_runner)
        return BackendResult(backend=backend, status=ResultStatus.PASS)

    def up_one(self, target: str, backend: str, *, wait_lock: float = 0.0) -> BackendResult:
        selection = self._selection(target, backend)
        if selection.target.transport != "compose":
            raise _error(target, backend, Phase.CONFIGURATION, "This command requires a Compose target.", "Select a Compose target.")
        command_runner = self._scoped_command_runner(selection)
        compose_inspector = self._scoped_compose_inspector(command_runner)
        redactor = command_runner.redactor if hasattr(command_runner, "redactor") else Redactor(selection.secret_values)
        with self._lock(selection, wait_lock):
            lease = self.lease_factory(
                compose=self._compose_service(selection),
                runner=command_runner,
                compose_inspector=compose_inspector,
                target=target,
                backend=backend,
                redactor=redactor,
            )
            lease.start()
        return BackendResult(backend=backend, status=ResultStatus.PASS)

    def down_one(self, target: str, backend: str, *, wait_lock: float = 0.0) -> BackendResult:
        selection = self._selection(target, backend)
        if selection.target.transport != "compose":
            raise _error(target, backend, Phase.CONFIGURATION, "This command requires a Compose target.", "Select a Compose target.")
        service = self._compose_service(selection)
        command_runner = self._scoped_command_runner(selection)
        redactor = command_runner.redactor if hasattr(command_runner, "redactor") else Redactor(selection.secret_values)
        with self._lock(selection, wait_lock):
            errors: list[str] = []
            for argv, action in (
                (["docker", "compose", "stop", service.service], "stop"),
                (["docker", "compose", "rm", "-f", service.service], "rm"),
            ):
                try:
                    command_runner.run(
                        argv,
                        phase=Phase.CLEANUP,
                        target=target,
                        backend=backend,
                    )
                except BaseException as exc:
                    errors.append(f"{action} failed: {redactor(str(exc))}")
            if errors:
                raise _error(
                    target,
                    backend,
                    Phase.CLEANUP,
                    "; ".join(errors),
                    "Check Docker Compose state and retry cleanup.",
                )
        return BackendResult(backend=backend, status=ResultStatus.PASS)

    def run_one(
        self,
        target: str,
        backend: str,
        *,
        timeout: float | None = None,
        wait_lock: float = 0.0,
    ) -> BackendResult:
        selection = self._selection(target, backend)
        if selection.target.transport != "compose":
            raise _error(
                target,
                backend,
                Phase.CONFIGURATION,
                "The run command requires a Compose target.",
                "Select a Compose target.",
            )
        command_runner = self._scoped_command_runner(selection)
        compose_inspector = self._scoped_compose_inspector(command_runner)
        redactor = command_runner.redactor if hasattr(command_runner, "redactor") else Redactor(selection.secret_values)
        with self._lock(selection, wait_lock):
            lease = self.lease_factory(
                compose=self._compose_service(selection),
                runner=command_runner,
                compose_inspector=compose_inspector,
                target=target,
                backend=backend,
                redactor=redactor,
            )
            primary: BaseException | None = None
            try:
                lease.start()
                self._check_unlocked(
                    selection,
                    redactor=redactor,
                    command_runner=command_runner,
                    compose_inspector=compose_inspector,
                )
                self._run_pytest(selection, timeout=timeout, command_runner=command_runner)
            except BaseException as exc:
                primary = exc
            cleanup: BaseException | None = None
            try:
                lease.close()
            except BaseException as exc:
                cleanup = exc
            if primary is not None:
                if isinstance(primary, KeyboardInterrupt):
                    raise primary
                detail = redactor(str(primary))
                if cleanup is not None:
                    detail = f"{detail} Cleanup failed: {redactor(str(cleanup))}"
                raise _error(
                    target,
                    backend,
                    getattr(primary, "phase", Phase.PYTEST),
                    detail,
                    "Check the command output and target configuration.",
                ) from None
            if cleanup is not None:
                raise cleanup
        return BackendResult(backend=backend, status=ResultStatus.PASS)

    def aggregate(self, target: str, command: str, *, jobs: int | None = None, fail_fast: bool = False, timeout: float | None = None, wait_lock: float = 0.0) -> tuple[list[BackendResult], int]:
        loaded = self._load(target, None)
        target_def = loaded.settings.targets[target]
        configured = tuple(target_def.backends)
        results: dict[str, BackendResult] = {}
        runnable: list[str] = []
        for name in configured:
            suite = loaded.settings.backends[name]
            if not suite.runnable:
                results[name] = BackendResult(backend=name, status=ResultStatus.UNAVAILABLE, detail=suite.backlog, backlog=suite.backlog)
            else:
                runnable.append(name)

        def operation(name: str) -> BackendResult:
            try:
                if command == "check":
                    return self.check_one(target, name, wait_lock=wait_lock)
                if command == "test":
                    return self.test_one(target, name, timeout=timeout, wait_lock=wait_lock)
                return self.run_one(target, name, timeout=timeout, wait_lock=wait_lock)
            except KeyboardInterrupt:
                raise
            except HarnessError as exc:
                return BackendResult(backend=name, status=ResultStatus.FAIL, detail=Redactor()(str(exc)))
            except BaseException as exc:
                return BackendResult(backend=name, status=ResultStatus.FAIL, detail=Redactor()(str(exc)))

        try:
            results.update({
                result.backend: result
                for result in run_many(
                    runnable,
                    operation,
                    jobs=jobs,
                    target_limit=target_def.max_parallel,
                    fail_fast=fail_fast,
                    partial_results=results,
                )
            })
        except KeyboardInterrupt:
            interrupted: list[BackendResult] = []
            for name in configured:
                if name in results:
                    interrupted.append(results[name])
                elif name in runnable:
                    interrupted.append(
                        BackendResult(
                            backend=name,
                            status=ResultStatus.FAIL,
                            detail="interrupted",
                        )
                    )
                else:
                    interrupted.append(
                        BackendResult(
                            backend=name,
                            status=ResultStatus.UNAVAILABLE,
                            detail=loaded.settings.backends[name].backlog,
                            backlog=loaded.settings.backends[name].backlog,
                        )
                    )
            return interrupted, 130
        if fail_fast:
            failure_seen = False
            for name in runnable:
                if name in results and results[name].status is ResultStatus.FAIL:
                    failure_seen = True
                elif failure_seen and name not in results:
                    results[name] = BackendResult(backend=name, status=ResultStatus.NOT_RUN, detail="stopped by --fail-fast")
        ordered = [results[name] for name in configured]
        code = 0 if all(result.status is not ResultStatus.FAIL for result in ordered) else 1
        return ordered, code

    def render(self, target: str, results: Sequence[BackendResult]) -> None:
        runnable = sum(result.status is not ResultStatus.UNAVAILABLE for result in results)
        print(f"TARGET {target}: {runnable}/{len(results)} runnable")
        for result in results:
            detail = f"  {result.detail}" if result.detail else (f"  {result.backlog}" if result.backlog else "")
            print(f"{result.backend:<14} {result.status.value}{detail}")
