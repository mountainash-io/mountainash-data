"""Database resource provider with explicit, redacted URL mode."""

from __future__ import annotations

from dataclasses import dataclass

from mountainash_resource_provider import (
    RESOURCE_PROVIDER_API_VERSION,
    FrozenMap,
    NativeReadRequest,
    ProviderCompatibilityError,
    ProviderReadError,
    ProviderReadResult,
    ReaderBackend,
    ResourceRequest,
)

from mountainash_data.resource_provider.types import (
    DatabaseConnectionMode,
    DatabaseConnectionParameters,
    SensitiveDatabaseUrl,
)


@dataclass(frozen=True)
class DatabaseProviderReadPlan:
    provider_key: str
    dialect_fields: FrozenMap
    payload: FrozenMap


class DatabaseResourceProvider:
    key = "database"
    api_version = RESOURCE_PROVIDER_API_VERSION

    def __init__(self, parameters: DatabaseConnectionParameters) -> None:
        self._parameters = parameters

    @classmethod
    def from_resource_url(cls, url: str) -> "DatabaseResourceProvider":
        backend = url.split(":", 1)[0].casefold()
        return cls(
            DatabaseConnectionParameters(
                backend=backend,
                mode=DatabaseConnectionMode.RESOURCE_URL,
                resource_url=SensitiveDatabaseUrl(url, label="database URL"),
            )
        )

    def plan(self, request: ResourceRequest) -> DatabaseProviderReadPlan:
        if request.detected_format.provider_format_key != self._parameters.backend:
            raise ProviderCompatibilityError("resource format does not match database provider backend")
        return DatabaseProviderReadPlan(
            provider_key=self.key,
            dialect_fields=FrozenMap({key: "consumed" for key in request.dialect}),
            payload=FrozenMap(
                {
                    "backend": self._parameters.backend,
                    "mode": self._parameters.mode,
                    "resource_url": self._parameters.resource_url,
                }
            ),
        )

    def read_arrow(self, plan: DatabaseProviderReadPlan) -> ProviderReadResult:
        raise ProviderReadError("database Arrow snapshots are not implemented")

    def native_request(self, plan: DatabaseProviderReadPlan, backend: ReaderBackend) -> NativeReadRequest | None:
        return None
