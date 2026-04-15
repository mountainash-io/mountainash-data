"""Declarative descriptors for backend settings.

A :class:`BackendDescriptor` captures everything the generic
:class:`~mountainash_data.core.settings.profile.ConnectionProfile` base needs
to configure pydantic fields and driver mappings for a given backend. A
:class:`ParameterSpec` describes one field within a descriptor.
"""

from __future__ import annotations

import typing as t
from dataclasses import dataclass, field

__all__ = ["MISSING", "ParameterSpec", "BackendDescriptor"]


class _Missing:
    """Sentinel indicating a required (no-default) field.

    Pydantic ``Field(...)`` is emitted when a :class:`ParameterSpec` default
    is this sentinel; ``Field(default=...)`` otherwise.
    """

    _instance: "t.ClassVar[_Missing | None]" = None

    def __new__(cls) -> "_Missing":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "MISSING"

    def __bool__(self) -> bool:
        return False


MISSING: _Missing = _Missing()


@dataclass(frozen=True, kw_only=True)
class ParameterSpec:
    """One settings field on a backend.

    Attributes:
        name: Settings-facing uppercase name (e.g. ``"SSL_CERT"``).
        type: Pydantic-compatible annotation (``str``, ``int | None``, enum, …).
        tier: ``"core"`` or ``"advanced"`` — audit-style severity tier.
        default: Default value; :data:`MISSING` means the field is required.
        description: Optional docstring for generated schemas / help output.
        driver_key: Driver kwarg name for 1:1 mappings (e.g. ``"sslcert"``).
            ``None`` means the adapter handles it.
        secret: If ``True``, wrap ``type`` as :class:`pydantic.SecretStr` and
            auto-unwrap via ``.get_secret_value()`` at the kwargs boundary.
        transform: Optional callable applied when emitting driver kwargs
            (e.g. :class:`~pathlib.Path` → ``str``, ``bool`` → ``"0"``/``"1"``).
        validator: Optional pydantic-compatible field-level validator.
    """

    name: str
    type: t.Any
    tier: t.Literal["core", "advanced"]
    default: t.Any = MISSING
    description: str = ""
    driver_key: str | None = None
    secret: bool = False
    transform: t.Callable[[t.Any], t.Any] | None = None
    validator: t.Callable[[t.Any], t.Any] | None = None


@dataclass(frozen=True, kw_only=True)
class BackendDescriptor:
    """Immutable description of a single backend.

    Attributes:
        name: Lowercase short name (``"postgresql"``, ``"pyiceberg_rest"``).
        provider_type: Canonical provider identifier
            (``CONST_DB_PROVIDER_TYPE`` member).
        default_port: Default TCP port if the backend listens on one.
        parameters: Ordered list of :class:`ParameterSpec`.
        auth_modes: Tuple of :class:`~...settings.auth.base.AuthSpec` subclasses
            this backend accepts.
        connection_string_scheme: Scheme prefix (``"postgresql://"``) or
            ``None`` if the backend does not use a URL form.
        ibis_dialect: Name of the Ibis backend if Ibis handles this backend.
        rides_on: Name of another backend whose Ibis path this one routes
            through (e.g. ``motherduck`` → ``duckdb``). Metadata only — no
            runtime behavior.
    """

    name: str
    provider_type: t.Any  # CONST_DB_PROVIDER_TYPE member
    parameters: list[ParameterSpec]
    auth_modes: list[type]  # list[type[AuthSpec]] — forward-refd to avoid cycle
    default_port: int | None = None
    connection_string_scheme: str | None = None
    ibis_dialect: str | None = None
    rides_on: str | None = None
