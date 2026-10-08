from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest
from pydantic import ValidationError

from scripts.live_db_harness.models import (
    AuthDefinition,
    BackendDefinition,
    BackendResult,
    ComposeService,
    HarnessError,
    HarnessSettings,
    Phase,
    ResultStatus,
    TargetBackendDefinition,
    TargetDefinition,
)


def _auth() -> AuthDefinition:
    return AuthDefinition(profile="password", values={"USERNAME": "user"})


def _target_backend() -> TargetBackendDefinition:
    return TargetBackendDefinition(
        connection={"HOST": "127.0.0.1", "PORT": 5432}, auth=_auth()
    )


def _suite_backend(*, runnable: bool = True, backlog: str | None = None, compose: bool = True):
    return BackendDefinition(
        settings_profile="postgresql",
        selector="postgres",
        runnable=runnable,
        backlog=backlog,
        compose=ComposeService(profile="postgres", service="postgres") if compose else None,
    )


def test_target_requires_known_transport():
    with pytest.raises(ValidationError):
        TargetDefinition(transport="unknown", backends={})


def test_runnable_backend_rejects_backlog_item():
    with pytest.raises(ValidationError):
        BackendDefinition(
            settings_profile="postgresql",
            selector="postgres",
            runnable=True,
            backlog="DEBT-1",
        )


def test_unavailable_backend_requires_backlog_item():
    with pytest.raises(ValidationError):
        BackendDefinition(
            settings_profile="postgresql",
            selector="postgres",
            runnable=False,
        )


def test_compose_backend_requires_profile_and_service():
    with pytest.raises(ValidationError):
        HarnessSettings(
            backends={"postgres": _suite_backend(compose=False)},
            targets={
                "docker": TargetDefinition(
                    transport="compose",
                    backends={"postgres": _target_backend()},
                )
            },
        )


def test_harness_rejects_unknown_orchestration_fields():
    with pytest.raises(ValidationError):
        HarnessSettings(
            backends={"postgres": _suite_backend()},
            targets={
                "docker": {
                    "transport": "compose",
                    "safety_check": "disabled",
                    "backends": {"postgres": _target_backend()},
                }
            },
        )


def test_backend_result_invariants():
    with pytest.raises(ValidationError):
        BackendResult(backend="mssql", status=ResultStatus.UNAVAILABLE)
    with pytest.raises(ValidationError):
        BackendResult(backend="mssql", status=ResultStatus.NOT_RUN, detail="not attempted")
    assert BackendResult(
        backend="mssql", status=ResultStatus.UNAVAILABLE, backlog="DEBT-17"
    ).backlog == "DEBT-17"


def test_target_backend_and_selection_references_are_validated():
    with pytest.raises(ValidationError):
        HarnessSettings(
            selected_target="missing",
            backends={"postgres": _suite_backend()},
            targets={
                "docker": TargetDefinition(
                    transport="compose", backends={"unknown": _target_backend()}
                )
            },
        )


def test_harness_error_is_frozen_and_rendered():
    error = HarnessError(
        target="docker",
        backend="postgres",
        phase=Phase.TRANSPORT,
        detail="service did not start",
        corrective_action="Start the Docker service and retry.",
    )
    rendered = str(error)
    assert all(value in rendered for value in (
        "docker", "postgres", "transport", "service did not start", "Start the Docker service"
    ))
    with pytest.raises(FrozenInstanceError):
        error.detail = "changed"  # type: ignore[misc]  # asserting the frozen field rejects writes


