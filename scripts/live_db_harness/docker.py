from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .models import ComposeService, HarnessError, Phase
from .process import CommandRunner, Redactor
from .transports import ComposeInspection, ComposeInspector, ServiceState


class DockerServiceLease:
    """Own and clean up only a Compose service started by this invocation."""

    def __init__(
        self,
        profile: str | ComposeService | None = None,
        service: str | None = None,
        *,
        compose: ComposeService | None = None,
        runner: CommandRunner | Any | None = None,
        compose_inspector: ComposeInspector | Any | None = None,
        target: str = "<compose>",
        backend: str = "<compose>",
        redactor: Redactor | Any | None = None,
    ) -> None:
        selected = compose
        if isinstance(profile, ComposeService):
            if selected is not None:
                raise ValueError("provide ComposeService either positionally or by keyword")
            selected = profile
        if selected is not None:
            profile = selected.profile
            service = selected.service
        if not isinstance(profile, str) or not profile or not isinstance(service, str) or not service:
            raise ValueError("DockerServiceLease requires a Compose profile and service")

        self.profile = profile
        self.service = service
        self.runner = runner or CommandRunner()
        self.compose_inspector = compose_inspector or ComposeInspector(self.runner)
        self.target = target
        self.backend = backend
        self.redactor = redactor or getattr(self.runner, "redactor", None) or Redactor()
        self._pre_start: ServiceState | None = None
        self._started_by_runner = False
        self._started = False
        self._closed = False

    @property
    def started_by_runner(self) -> bool:
        return self._started_by_runner

    @property
    def preexisting(self) -> bool:
        return self._pre_start is not None and self._pre_start.exists

    def start(self) -> DockerServiceLease:
        """Run service-scoped Compose startup and record ownership."""
        if self._started:
            return self
        if self._closed:
            raise _cleanup_error(
                self.target,
                self.backend,
                ["cannot start a closed Docker service lease"],
            )

        before = self._preflight_or_state()
        self._pre_start = before
        startup_error: BaseException | None = None
        try:
            self.runner.run(
                [
                    "docker",
                    "compose",
                    "--profile",
                    self.profile,
                    "up",
                    "-d",
                    "--wait",
                    self.service,
                ],
                phase=Phase.TRANSPORT,
                target=self.target,
                backend=self.backend,
            )
        except BaseException as exc:
            startup_error = exc
        after = self._state_after_attempt()
        if before.exists is False and after is not None and after.exists:
            self._started_by_runner = True
            self._started = True
        if startup_error is not None:
            cleanup_error = self._close_internal()
            if cleanup_error is not None:
                raise cleanup_error from startup_error
            raise startup_error
        if after is None or not after.running:
            post_start_error = _error(
                self.target,
                self.backend,
                f"Compose service {self.service!r} was not running after startup.",
                "Check the Compose service health and retry.",
            )
            if not before.exists:
                # A successful `up` owns a previously absent service even when
                # the follow-up observation cannot confirm its final state.
                self._started_by_runner = True
                cleanup_error = self._close_internal()
                if cleanup_error is not None:
                    raise cleanup_error from post_start_error
            raise post_start_error
        # Ownership requires that the container did not exist before startup;
        # starting a pre-existing stopped container does not transfer it.
        self._started_by_runner = before.exists is False
        self._started = self._started_by_runner
        return self

    def close(self) -> None:
        """Stop and remove an owned service, reporting all cleanup failures."""
        cleanup_error = self._close_internal()
        if cleanup_error is not None:
            raise cleanup_error

    def run(self, operation: Callable[[], Any]) -> Any:
        """Run an operation under this lease and always clean up afterwards."""
        self.start()
        try:
            return operation()
        finally:
            self.close()

    def __enter__(self) -> DockerServiceLease:
        return self.start()

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def _preflight_or_state(self) -> ServiceState:
        try:
            preflight = getattr(self.compose_inspector, "preflight", None)
            if callable(preflight):
                inspection = _invoke_inspector(
                    preflight,
                    self.profile,
                    self.service,
                    target=self.target,
                    backend=self.backend,
                )
                return _as_state(inspection)
            return _as_state(
                _invoke_inspector(
                    self.compose_inspector.service_state,
                    self.profile,
                    self.service,
                    target=self.target,
                    backend=self.backend,
                )
            )
        except HarnessError:
            raise
        except Exception as exc:
            raise _error(
                self.target,
                self.backend,
                f"Could not inspect Compose service {self.service!r}: {_safe_text(exc)}",
                "Check Docker Compose state and retry.",
            ) from None

    def _state_after_attempt(self) -> ServiceState | None:
        try:
            return _as_state(
                _invoke_inspector(
                    self.compose_inspector.service_state,
                    self.profile,
                    self.service,
                    target=self.target,
                    backend=self.backend,
                )
            )
        except Exception:
            # Unknown post-start state is fail-closed: never claim ownership
            # when the service cannot be observed.
            return None

    def _close_internal(self) -> HarnessError | None:
        if self._closed:
            return None
        self._closed = True
        if not self._started_by_runner:
            return None

        errors: list[str] = []
        try:
            self.runner.run(
                ["docker", "compose", "stop", self.service],
                phase=Phase.CLEANUP,
                target=self.target,
                backend=self.backend,
            )
        except BaseException as exc:
            errors.append(f"stop failed: {_redact(self.redactor, exc)}")
        try:
            self.runner.run(
                ["docker", "compose", "rm", "-f", self.service],
                phase=Phase.CLEANUP,
                target=self.target,
                backend=self.backend,
            )
        except BaseException as exc:
            errors.append(f"rm failed: {_redact(self.redactor, exc)}")
        if errors:
            return _cleanup_error(self.target, self.backend, errors)
        return None


def _as_state(value: ServiceState | ComposeInspection | Any) -> ServiceState:
    if isinstance(value, ServiceState):
        return value
    if isinstance(value, ComposeInspection):
        return value.state
    state = getattr(value, "state", None)
    if isinstance(state, ServiceState):
        return state
    exists = getattr(value, "exists", None)
    running = getattr(value, "running", exists)
    if isinstance(exists, bool) and isinstance(running, bool):
        return ServiceState(exists, running)
    raise TypeError("Compose inspector returned an invalid service state")


def _error(target: str, backend: str, detail: str, corrective: str) -> HarnessError:
    return HarnessError(target, backend, Phase.TRANSPORT, detail, corrective)


def _cleanup_error(target: str, backend: str, errors: list[str]) -> HarnessError:
    return HarnessError(
        target,
        backend,
        Phase.CLEANUP,
        "Docker service cleanup failed: " + "; ".join(errors),
        "Inspect the selected service and remove only that service before retrying.",
    )


def _redact(redactor: Any, value: BaseException) -> str:
    text = _safe_text(value)
    redact = getattr(redactor, "redact", None)
    if callable(redact):
        text = redact(text)
    return text


def _safe_text(value: object) -> str:
    return str(value).replace("\n", " ")[:300]


def _invoke_inspector(method: Any, profile: str, service: str, **kwargs: str) -> Any:
    try:
        return method(profile, service, **kwargs)
    except TypeError as exc:
        if "unexpected keyword argument" not in str(exc):
            raise
        return method(profile, service)
