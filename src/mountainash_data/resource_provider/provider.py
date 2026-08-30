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
    ProviderFormatDescriptor,
    ReaderBackend,
    ResourceRequest,
)

from mountainash_data.core.settings.registry import DATABASES_REGISTRY
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
    @property
    def formats(self) -> tuple[ProviderFormatDescriptor, ...]:
        return tuple(
            ProviderFormatDescriptor(
                canonical_format=descriptor.name,
                aliases=frozenset({"postgres"}) if descriptor.name == "postgresql" else frozenset(),
                suffixes=frozenset(),
                mediatypes=frozenset(),
                locator_prefixes=frozenset(descriptor.resource_read_locator_prefixes),
                dialect_family=None,
                provider_format_key=descriptor.name,
            )
            for descriptor in DATABASES_REGISTRY.descriptors.values()
        )

    @property
    def parser_keys(self) -> frozenset[str]:
        return frozenset(descriptor.provider_format_key for descriptor in self.formats)


    @classmethod
    def default(cls) -> "DatabaseResourceProvider":
        return cls.from_parameters("duckdb")

    @classmethod
    def from_parameters(cls, backend: str) -> "DatabaseResourceProvider":
        return cls(
            DatabaseConnectionParameters(
                backend=backend.casefold(),
                mode=DatabaseConnectionMode.SETTINGS,
            )
        )

    @classmethod
    def from_config(
        cls,
        backend: str,
        *,
        config_files: list[str] | None = None,
    ) -> "DatabaseResourceProvider":
        del config_files
        return cls.from_parameters(backend)


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
