from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, PositiveFloat, PositiveInt, model_validator
from pydantic_settings import SettingsConfigDict

from mountainash_settings import MountainAshBaseSettings


class Phase(StrEnum):
    CONFIGURATION = "configuration"
    SECRET_RESOLUTION = "secret resolution"
    TRANSPORT = "transport"
    AUTHENTICATION = "authentication"
    PYTEST = "pytest"
    CLEANUP = "cleanup"


class ResultStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    UNAVAILABLE = "UNAVAILABLE"
    NOT_RUN = "NOT_RUN"


class BackendResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    backend: str
    status: ResultStatus
    detail: str | None = None
    backlog: str | None = None

    @model_validator(mode="after")
    def validate_status_fields(self) -> BackendResult:
        if self.status is ResultStatus.UNAVAILABLE and not self.backlog:
            raise ValueError("an unavailable backend result requires a backlog item")
        if self.status is ResultStatus.NOT_RUN and self.detail != "stopped by --fail-fast":
            raise ValueError("a not-run result must be stopped by --fail-fast")
        return self


class SecretProviderDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    type: Literal["filesystem"]
    path: Path


class ComposeService(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    profile: str
    service: str


class TunnelIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    launchd_label: str
    ssh_destination: str
    local_host: str
    local_port: int
    remote_host: str
    remote_port: int
    client_host: str | None = None
    client_port: int | None = None
    process_ancestry: tuple[str, ...] = ("launchd", "autossh", "ssh")


class AuthDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    profile: str
    values: dict[str, object] = Field(default_factory=dict)


class TargetBackendDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    connection: dict[str, object]
    auth: AuthDefinition
    tunnel: TunnelIdentity | None = None


class BackendDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    settings_profile: str
    selector: str
    runnable: bool
    backlog: str | None = None
    compose: ComposeService | None = None

    @model_validator(mode="after")
    def validate_availability(self) -> BackendDefinition:
        if self.runnable and self.backlog is not None:
            raise ValueError("a runnable backend cannot have a backlog item")
        if not self.runnable and not self.backlog:
            raise ValueError("an unavailable backend requires a backlog item")
        return self


class TargetDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    transport: Literal["compose", "ssh-tunnel", "direct"]
    secrets_provider: str | None = None
    allow_destructive_tests: bool = False
    max_parallel: PositiveInt = 1
    test_timeout_seconds: PositiveFloat = 900.0
    backends: dict[str, TargetBackendDefinition]


@dataclass(frozen=True)
class HarnessError(Exception):
    target: str | None
    backend: str | None
    phase: Phase
    detail: str
    corrective_action: str

    def __post_init__(self) -> None:
        Exception.__init__(self, self._render())

    def _render(self) -> str:
        target = self.target if self.target is not None else "<none>"
        backend = self.backend if self.backend is not None else "<none>"
        return (
            f"Live database harness error (target={target}, backend={backend}, "
            f"phase={self.phase.value}): {self.detail} "
            f"Corrective action: {self.corrective_action}"
        )

    def __str__(self) -> str:
        return self._render()


class HarnessSettings(MountainAshBaseSettings):
    model_config = SettingsConfigDict(extra="forbid", env_prefix="MOUNTAINASH_LIVE_DB_")

    selected_target: str | None = None
    selected_backend: str | None = None
    secret_providers: dict[str, SecretProviderDefinition] = Field(default_factory=dict)
    backends: dict[str, BackendDefinition]
    targets: dict[str, TargetDefinition]

    @model_validator(mode="after")
    def validate_target_references(self) -> HarnessSettings:
        if self.selected_target is not None and self.selected_target not in self.targets:
            raise ValueError(f"selected target does not exist: {self.selected_target}")
        if self.selected_backend is not None:
            if self.selected_backend not in self.backends:
                raise ValueError(f"selected backend does not exist: {self.selected_backend}")
            if (
                self.selected_target is not None
                and self.selected_backend not in self.targets[self.selected_target].backends
            ):
                raise ValueError(
                    f"selected backend is not configured for target: {self.selected_backend}"
                )

        for target_name, target in self.targets.items():
            for backend_name, target_backend in target.backends.items():
                suite_backend = self.backends.get(backend_name)
                if suite_backend is None:
                    raise ValueError(
                        f"target {target_name!r} references unknown backend {backend_name!r}"
                    )
                if target.transport == "compose":
                    if suite_backend.compose is None:
                        raise ValueError(
                            f"compose target {target_name!r} backend {backend_name!r} "
                            "requires suite Compose metadata"
                        )
                    if target_backend.tunnel is not None:
                        raise ValueError(
                            f"compose target {target_name!r} backend {backend_name!r} "
                            "cannot define a tunnel identity"
                        )
                elif target.transport == "ssh-tunnel":
                    if target_backend.tunnel is None:
                        raise ValueError(
                            f"SSH-tunnel target {target_name!r} backend {backend_name!r} "
                            "requires a complete tunnel identity"
                        )
                elif target_backend.tunnel is not None:
                    raise ValueError(
                        f"direct target {target_name!r} backend {backend_name!r} "
                        "cannot define a tunnel identity"
                    )
        return self
