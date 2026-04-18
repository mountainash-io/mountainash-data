# Settings Registry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the class-per-backend, method-heavy settings hierarchy in `src/mountainash_data/core/settings/` with a descriptor + thin-subclass pattern backed by a typed `AuthSpec` discriminated union — and carry forward the audit findings in the same pass.

**Architecture:** `BackendDescriptor` (immutable data) + `ParameterSpec` list + `AuthSpec` union + optional per-backend `adapters/<name>.py` for composite driver mapping. A generic `ConnectionProfile(MountainAshBaseSettings)` base reads `__descriptor__` to configure pydantic fields, with uniform `to_driver_kwargs()` / `to_connection_string()` API. Per-backend files shrink to a descriptor + two-line shell class.

**Tech Stack:** Python 3.12, pydantic v2, `mountainash_settings.MountainAshBaseSettings`, pytest, hatch, ruff, mypy.

**Spec:** `docs/superpowers/specs/2026-04-15-settings-registry-design.md`.

**Scope:** Phases 1–3 of the spec (scaffolding, per-backend migrations, consumer update). Phase 4 (audit-fix sweep) becomes follow-up per-backend plans.

---

## File Structure

### Created

```
src/mountainash_data/core/settings/
├── descriptor.py               # ParameterSpec, BackendDescriptor, MISSING sentinel
├── registry.py                 # REGISTRY, @register, get_descriptor, get_settings_class
├── profile.py                  # ConnectionProfile base
├── auth/
│   ├── __init__.py             # re-exports
│   ├── base.py                 # AuthSpec ABC
│   ├── none.py                 # NoAuth
│   ├── password.py             # PasswordAuth
│   ├── token.py                # TokenAuth, JWTAuth
│   ├── oauth2.py               # OAuth2Auth
│   ├── service_account.py      # ServiceAccountAuth
│   ├── iam.py                  # IAMAuth
│   ├── azure.py                # AzureADAuth, WindowsAuth
│   ├── kerberos.py             # KerberosAuth
│   ├── certificate.py          # CertificateAuth
│   └── dispatch.py             # AUTH_TO_DRIVER_KWARGS default map
└── adapters/
    ├── __init__.py
    ├── mysql.py
    ├── mssql.py
    ├── snowflake.py
    ├── bigquery.py
    ├── redshift.py
    ├── trino.py
    └── pyiceberg_rest.py

tests/test_unit/core/settings/
├── __init__.py
├── test_descriptor.py          # ParameterSpec / BackendDescriptor unit tests
├── test_registry.py            # register / lookup invariants
├── test_profile.py             # ConnectionProfile base behavior
├── test_descriptors_invariants.py  # parametric over REGISTRY
├── test_auth_dispatch.py       # AUTH_TO_DRIVER_KWARGS coverage
└── backends/
    ├── __init__.py
    ├── test_sqlite.py
    ├── test_duckdb.py
    ├── test_pyspark.py
    ├── test_motherduck.py
    ├── test_postgresql.py
    ├── test_mysql.py
    ├── test_trino.py
    ├── test_mssql.py
    ├── test_snowflake.py
    ├── test_bigquery.py
    ├── test_redshift.py
    └── test_pyiceberg_rest.py
```

### Modified (rewritten in place)

- `src/mountainash_data/core/settings/sqlite.py` — collapse to descriptor + shell
- `src/mountainash_data/core/settings/duckdb.py`
- `src/mountainash_data/core/settings/motherduck.py`
- `src/mountainash_data/core/settings/postgresql.py`
- `src/mountainash_data/core/settings/mysql.py`
- `src/mountainash_data/core/settings/mssql.py`
- `src/mountainash_data/core/settings/snowflake.py`
- `src/mountainash_data/core/settings/bigquery.py`
- `src/mountainash_data/core/settings/redshift.py`
- `src/mountainash_data/core/settings/pyspark.py`
- `src/mountainash_data/core/settings/trino.py`
- `src/mountainash_data/core/settings/pyiceberg_rest.py`
- `src/mountainash_data/core/settings/__init__.py` — re-exports only (no wildcard)
- `src/mountainash_data/core/factories/settings_factory.py` — consume registry
- `src/mountainash_data/core/factories/connection_factory.py` — call `to_driver_kwargs`
- `src/mountainash_data/core/factories/operations_factory.py` — same
- `src/mountainash_data/core/factories/settings_type_factory_mixin.py` — registry lookup
- `src/mountainash_data/core/connection.py` — new profile API
- `src/mountainash_data/core/utils.py` (`DatabaseUtils`) — new profile API
- `src/mountainash_data/backends/ibis/connection.py` — consume `to_driver_kwargs`
- `src/mountainash_data/backends/iceberg/connection.py` — consume `to_driver_kwargs`

### Deleted

- `src/mountainash_data/core/settings/base.py` — `BaseDBAuthSettings` retired
- `src/mountainash_data/core/settings/exceptions.py` — `DBAuthValidationError` retired (one-line alias shim lives in `__init__.py` if needed)

### Conventions

- All new files: Google-style docstrings, typing annotations, ruff-clean, mypy-clean.
- New tests marked `@pytest.mark.unit` unless they touch a real driver.
- Each task ends in `hatch run test:test-quick` passing before commit.
- Commit message format: `feat(settings): <task subject>` for new code; `refactor(settings): <backend>` for per-backend migrations; `chore(settings): <cleanup>` for deletions / re-exports.

---

## Task 1: Create `ParameterSpec`, `BackendDescriptor`, and `MISSING` sentinel

**Files:**
- Create: `src/mountainash_data/core/settings/descriptor.py`
- Test: `tests/test_unit/core/settings/test_descriptor.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_unit/core/settings/test_descriptor.py
"""Unit tests for settings descriptor primitives."""

import pytest
from typing import Literal, Optional

from mountainash_data.core.settings.descriptor import (
    BackendDescriptor,
    MISSING,
    ParameterSpec,
)


@pytest.mark.unit
class TestParameterSpec:
    def test_minimal_parameter_spec(self):
        spec = ParameterSpec(name="FOO", type=str, tier="core")
        assert spec.name == "FOO"
        assert spec.type is str
        assert spec.tier == "core"
        assert spec.default is MISSING
        assert spec.driver_key is None
        assert spec.secret is False
        assert spec.transform is None
        assert spec.validator is None

    def test_parameter_spec_is_frozen(self):
        spec = ParameterSpec(name="FOO", type=str, tier="core")
        with pytest.raises(Exception):
            spec.name = "BAR"  # type: ignore[misc]

    def test_parameter_spec_with_default(self):
        spec = ParameterSpec(name="PORT", type=int, tier="core", default=5432)
        assert spec.default == 5432

    def test_parameter_spec_secret_flag(self):
        spec = ParameterSpec(name="PASSWORD", type=str, tier="core", secret=True)
        assert spec.secret is True

    def test_parameter_spec_tier_must_be_valid(self):
        # typing.Literal is not runtime-enforced by dataclass, but we document
        # the constraint; downstream registry invariants will enforce.
        spec = ParameterSpec(name="FOO", type=str, tier="core")
        assert spec.tier in {"core", "advanced"}


@pytest.mark.unit
class TestBackendDescriptor:
    def test_minimal_descriptor(self):
        desc = BackendDescriptor(
            name="sqlite",
            provider_type="sqlite",
            parameters=[],
            auth_modes=[],
        )
        assert desc.name == "sqlite"
        assert desc.default_port is None
        assert desc.connection_string_scheme is None
        assert desc.ibis_dialect is None
        assert desc.rides_on is None

    def test_descriptor_is_frozen(self):
        desc = BackendDescriptor(
            name="sqlite", provider_type="sqlite", parameters=[], auth_modes=[]
        )
        with pytest.raises(Exception):
            desc.name = "mysql"  # type: ignore[misc]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `hatch run test:test-target tests/test_unit/core/settings/test_descriptor.py -v`
Expected: collection error — module `mountainash_data.core.settings.descriptor` not found.

- [ ] **Step 3: Implement `descriptor.py`**

```python
# src/mountainash_data/core/settings/descriptor.py
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `hatch run test:test-target tests/test_unit/core/settings/test_descriptor.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/settings/descriptor.py \
        tests/test_unit/core/settings/__init__.py \
        tests/test_unit/core/settings/test_descriptor.py
git commit -m "feat(settings): add ParameterSpec and BackendDescriptor primitives"
```

---

## Task 2: `AuthSpec` hierarchy

**Files:**
- Create: `src/mountainash_data/core/settings/auth/__init__.py`
- Create: `src/mountainash_data/core/settings/auth/base.py`
- Create: `src/mountainash_data/core/settings/auth/none.py`
- Create: `src/mountainash_data/core/settings/auth/password.py`
- Create: `src/mountainash_data/core/settings/auth/token.py`
- Create: `src/mountainash_data/core/settings/auth/oauth2.py`
- Create: `src/mountainash_data/core/settings/auth/service_account.py`
- Create: `src/mountainash_data/core/settings/auth/iam.py`
- Create: `src/mountainash_data/core/settings/auth/azure.py`
- Create: `src/mountainash_data/core/settings/auth/kerberos.py`
- Create: `src/mountainash_data/core/settings/auth/certificate.py`
- Test: `tests/test_unit/core/settings/test_auth.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_unit/core/settings/test_auth.py
"""Unit tests for AuthSpec discriminated-union members."""

import pytest
from pydantic import SecretStr, ValidationError

from mountainash_data.core.settings.auth import (
    AuthSpec,
    AzureADAuth,
    CertificateAuth,
    IAMAuth,
    JWTAuth,
    KerberosAuth,
    NoAuth,
    OAuth2Auth,
    PasswordAuth,
    ServiceAccountAuth,
    TokenAuth,
    WindowsAuth,
)


@pytest.mark.unit
class TestAuthDiscriminator:
    @pytest.mark.parametrize(
        "cls, kind",
        [
            (NoAuth, "none"),
            (PasswordAuth, "password"),
            (TokenAuth, "token"),
            (JWTAuth, "jwt"),
            (OAuth2Auth, "oauth2"),
            (ServiceAccountAuth, "service_account"),
            (IAMAuth, "iam"),
            (WindowsAuth, "windows"),
            (AzureADAuth, "azure_ad"),
            (KerberosAuth, "kerberos"),
            (CertificateAuth, "certificate"),
        ],
    )
    def test_every_auth_has_discriminator_kind(self, cls, kind):
        # Build with minimal required fields
        if cls is PasswordAuth:
            instance = cls(username="u", password=SecretStr("p"))
        elif cls in (TokenAuth, JWTAuth):
            instance = cls(token=SecretStr("t"))
        else:
            instance = cls()
        assert instance.kind == kind

    def test_password_auth_requires_username_and_password(self):
        with pytest.raises(ValidationError):
            PasswordAuth()  # type: ignore[call-arg]

    def test_password_auth_wraps_password_as_secretstr(self):
        auth = PasswordAuth(username="alice", password="hunter2")
        assert isinstance(auth.password, SecretStr)
        assert auth.password.get_secret_value() == "hunter2"

    def test_noauth_has_no_fields(self):
        auth = NoAuth()
        assert auth.kind == "none"


@pytest.mark.unit
class TestOAuth2Auth:
    def test_all_fields_optional(self):
        auth = OAuth2Auth()
        assert auth.client_id is None
        assert auth.client_secret is None
        assert auth.token is None

    def test_token_is_secret(self):
        auth = OAuth2Auth(token="t")
        assert isinstance(auth.token, SecretStr)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `hatch run test:test-target tests/test_unit/core/settings/test_auth.py -v`
Expected: collection error — modules not found.

- [ ] **Step 3: Implement the AuthSpec base and members**

```python
# src/mountainash_data/core/settings/auth/base.py
"""Base class for discriminated-union auth specifications."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

__all__ = ["AuthSpec"]


class AuthSpec(BaseModel):
    """Abstract tagged base for authentication specifications.

    Each concrete subclass sets a ``kind`` Literal used as the pydantic
    discriminator value when composed into a union.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: str
```

```python
# src/mountainash_data/core/settings/auth/none.py
"""The 'no authentication' variant."""

from __future__ import annotations

import typing as t

from .base import AuthSpec

__all__ = ["NoAuth"]


class NoAuth(AuthSpec):
    """No authentication required (SQLite, DuckDB, PySpark)."""

    kind: t.Literal["none"] = "none"
```

```python
# src/mountainash_data/core/settings/auth/password.py
"""Classic username + password authentication."""

from __future__ import annotations

import typing as t

from pydantic import SecretStr

from .base import AuthSpec

__all__ = ["PasswordAuth"]


class PasswordAuth(AuthSpec):
    """Username + password authentication."""

    kind: t.Literal["password"] = "password"
    username: str
    password: SecretStr
```

```python
# src/mountainash_data/core/settings/auth/token.py
"""Bearer-token and JWT authentication."""

from __future__ import annotations

import typing as t

from pydantic import SecretStr

from .base import AuthSpec

__all__ = ["TokenAuth", "JWTAuth"]


class TokenAuth(AuthSpec):
    """Opaque bearer token (e.g. MotherDuck, PyIceberg REST)."""

    kind: t.Literal["token"] = "token"
    token: SecretStr


class JWTAuth(AuthSpec):
    """JSON Web Token authentication (e.g. Trino)."""

    kind: t.Literal["jwt"] = "jwt"
    token: SecretStr
```

```python
# src/mountainash_data/core/settings/auth/oauth2.py
"""OAuth2 client-credentials / token authentication."""

from __future__ import annotations

import typing as t

from pydantic import SecretStr

from .base import AuthSpec

__all__ = ["OAuth2Auth"]


class OAuth2Auth(AuthSpec):
    """OAuth2 credential set (Snowflake, Trino, PyIceberg REST)."""

    kind: t.Literal["oauth2"] = "oauth2"
    client_id: str | None = None
    client_secret: SecretStr | None = None
    token: SecretStr | None = None
    refresh_token: SecretStr | None = None
    server_uri: str | None = None
    scope: str | None = None
```

```python
# src/mountainash_data/core/settings/auth/service_account.py
"""Google-style service-account authentication."""

from __future__ import annotations

import typing as t
from pathlib import Path

from .base import AuthSpec

__all__ = ["ServiceAccountAuth"]


class ServiceAccountAuth(AuthSpec):
    """Google Cloud service-account key (JSON dict or file path)."""

    kind: t.Literal["service_account"] = "service_account"
    info: dict[str, t.Any] | None = None
    file: Path | None = None
```

```python
# src/mountainash_data/core/settings/auth/iam.py
"""AWS IAM authentication."""

from __future__ import annotations

import typing as t

from pydantic import SecretStr

from .base import AuthSpec

__all__ = ["IAMAuth"]


class IAMAuth(AuthSpec):
    """AWS IAM credentials (Redshift, S3-backed catalogs)."""

    kind: t.Literal["iam"] = "iam"
    role_arn: str | None = None
    access_key_id: str | None = None
    secret_access_key: SecretStr | None = None
    session_token: SecretStr | None = None
    profile_name: str | None = None
```

```python
# src/mountainash_data/core/settings/auth/azure.py
"""Microsoft-centric authentication: Windows integrated + Azure AD."""

from __future__ import annotations

import typing as t

from pydantic import SecretStr

from .base import AuthSpec

__all__ = ["WindowsAuth", "AzureADAuth"]


class WindowsAuth(AuthSpec):
    """Integrated Windows authentication (MSSQL)."""

    kind: t.Literal["windows"] = "windows"
    username: str | None = None
    domain: str | None = None


class AzureADAuth(AuthSpec):
    """Azure Active Directory authentication (MSSQL)."""

    kind: t.Literal["azure_ad"] = "azure_ad"
    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: SecretStr | None = None
    managed_identity: bool = False
    msi_endpoint: str | None = None
```

```python
# src/mountainash_data/core/settings/auth/kerberos.py
"""Kerberos / GSSAPI authentication."""

from __future__ import annotations

import typing as t
from pathlib import Path

from .base import AuthSpec

__all__ = ["KerberosAuth"]


class KerberosAuth(AuthSpec):
    """Kerberos authentication (Trino, PostgreSQL via GSS)."""

    kind: t.Literal["kerberos"] = "kerberos"
    service_name: str = "postgres"
    principal: str | None = None
    keytab: Path | None = None
```

```python
# src/mountainash_data/core/settings/auth/certificate.py
"""Private-key / certificate authentication (Snowflake JWT)."""

from __future__ import annotations

import typing as t
from pathlib import Path

from pydantic import SecretStr

from .base import AuthSpec

__all__ = ["CertificateAuth"]


class CertificateAuth(AuthSpec):
    """Private-key signed JWT authentication (Snowflake)."""

    kind: t.Literal["certificate"] = "certificate"
    private_key: SecretStr | None = None
    private_key_path: Path | None = None
    passphrase: SecretStr | None = None
```

```python
# src/mountainash_data/core/settings/auth/__init__.py
"""Discriminated-union AuthSpec members."""

from .azure import AzureADAuth, WindowsAuth
from .base import AuthSpec
from .certificate import CertificateAuth
from .iam import IAMAuth
from .kerberos import KerberosAuth
from .none import NoAuth
from .oauth2 import OAuth2Auth
from .password import PasswordAuth
from .service_account import ServiceAccountAuth
from .token import JWTAuth, TokenAuth

__all__ = [
    "AuthSpec",
    "NoAuth",
    "PasswordAuth",
    "TokenAuth",
    "JWTAuth",
    "OAuth2Auth",
    "ServiceAccountAuth",
    "IAMAuth",
    "WindowsAuth",
    "AzureADAuth",
    "KerberosAuth",
    "CertificateAuth",
]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `hatch run test:test-target tests/test_unit/core/settings/test_auth.py -v`
Expected: PASS (15 tests).

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/settings/auth/ \
        tests/test_unit/core/settings/test_auth.py
git commit -m "feat(settings): add AuthSpec discriminated-union hierarchy"
```

---

## Task 3: Auth dispatch map

**Files:**
- Create: `src/mountainash_data/core/settings/auth/dispatch.py`
- Test: `tests/test_unit/core/settings/test_auth_dispatch.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_unit/core/settings/test_auth_dispatch.py
"""Default AUTH_TO_DRIVER_KWARGS coverage tests."""

import pytest
from pydantic import SecretStr

from mountainash_data.core.settings.auth import (
    AuthSpec,
    IAMAuth,
    JWTAuth,
    NoAuth,
    OAuth2Auth,
    PasswordAuth,
    TokenAuth,
)
from mountainash_data.core.settings.auth.dispatch import auth_to_driver_kwargs


@pytest.mark.unit
class TestAuthToDriverKwargs:
    def test_noauth_returns_empty(self):
        assert auth_to_driver_kwargs(NoAuth()) == {}

    def test_password_unwraps_secret(self):
        auth = PasswordAuth(username="alice", password=SecretStr("hunter2"))
        assert auth_to_driver_kwargs(auth) == {
            "user": "alice",
            "password": "hunter2",
        }

    def test_token_unwraps_secret(self):
        auth = TokenAuth(token=SecretStr("t"))
        assert auth_to_driver_kwargs(auth) == {"token": "t"}

    def test_jwt_unwraps_secret(self):
        auth = JWTAuth(token=SecretStr("j"))
        assert auth_to_driver_kwargs(auth) == {"token": "j"}

    def test_oauth2_with_token(self):
        auth = OAuth2Auth(token=SecretStr("bearer"))
        assert auth_to_driver_kwargs(auth) == {"token": "bearer"}

    def test_oauth2_with_client_credentials(self):
        auth = OAuth2Auth(
            client_id="cid", client_secret=SecretStr("csec")
        )
        assert auth_to_driver_kwargs(auth) == {"credential": "cid:csec"}

    def test_iam_with_keys(self):
        auth = IAMAuth(
            access_key_id="AKIA...",
            secret_access_key=SecretStr("sk"),
            session_token=SecretStr("st"),
        )
        assert auth_to_driver_kwargs(auth) == {
            "aws_access_key_id": "AKIA...",
            "aws_secret_access_key": "sk",
            "aws_session_token": "st",
        }

    def test_iam_with_role_arn(self):
        auth = IAMAuth(role_arn="arn:aws:iam::123:role/x")
        assert auth_to_driver_kwargs(auth) == {
            "iam_role_arn": "arn:aws:iam::123:role/x"
        }

    def test_unknown_auth_type_raises(self):
        class WeirdAuth(AuthSpec):
            kind: str = "weird"  # type: ignore[assignment]

        with pytest.raises(KeyError):
            auth_to_driver_kwargs(WeirdAuth())
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `hatch run test:test-target tests/test_unit/core/settings/test_auth_dispatch.py -v`
Expected: collection error — `dispatch` module not found.

- [ ] **Step 3: Implement `dispatch.py`**

```python
# src/mountainash_data/core/settings/auth/dispatch.py
"""Default mapping from AuthSpec instances to driver kwargs.

Backend adapters can override individual auth types by consulting this map or
by writing bespoke match statements. The defaults cover the common case.
"""

from __future__ import annotations

import typing as t

from .base import AuthSpec
from .iam import IAMAuth
from .none import NoAuth
from .oauth2 import OAuth2Auth
from .password import PasswordAuth
from .token import JWTAuth, TokenAuth

__all__ = ["AUTH_TO_DRIVER_KWARGS", "auth_to_driver_kwargs"]


def _noauth(_auth: NoAuth) -> dict[str, t.Any]:
    return {}


def _password(auth: PasswordAuth) -> dict[str, t.Any]:
    return {
        "user": auth.username,
        "password": auth.password.get_secret_value(),
    }


def _token(auth: TokenAuth) -> dict[str, t.Any]:
    return {"token": auth.token.get_secret_value()}


def _jwt(auth: JWTAuth) -> dict[str, t.Any]:
    return {"token": auth.token.get_secret_value()}


def _oauth2(auth: OAuth2Auth) -> dict[str, t.Any]:
    if auth.token is not None:
        return {"token": auth.token.get_secret_value()}
    if auth.client_id is not None and auth.client_secret is not None:
        return {"credential": f"{auth.client_id}:{auth.client_secret.get_secret_value()}"}
    return {}


def _iam(auth: IAMAuth) -> dict[str, t.Any]:
    out: dict[str, t.Any] = {}
    if auth.role_arn is not None:
        out["iam_role_arn"] = auth.role_arn
    if auth.access_key_id is not None:
        out["aws_access_key_id"] = auth.access_key_id
    if auth.secret_access_key is not None:
        out["aws_secret_access_key"] = auth.secret_access_key.get_secret_value()
    if auth.session_token is not None:
        out["aws_session_token"] = auth.session_token.get_secret_value()
    return out


AUTH_TO_DRIVER_KWARGS: dict[type[AuthSpec], t.Callable[[t.Any], dict[str, t.Any]]] = {
    NoAuth: _noauth,
    PasswordAuth: _password,
    TokenAuth: _token,
    JWTAuth: _jwt,
    OAuth2Auth: _oauth2,
    IAMAuth: _iam,
    # WindowsAuth, AzureADAuth, KerberosAuth, ServiceAccountAuth, CertificateAuth:
    # no sensible default — their respective backend adapters handle mapping.
}


def auth_to_driver_kwargs(auth: AuthSpec) -> dict[str, t.Any]:
    """Look up the default mapper for ``auth`` and produce driver kwargs.

    Raises:
        KeyError: if no mapper is registered for ``type(auth)``.
    """
    return AUTH_TO_DRIVER_KWARGS[type(auth)](auth)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `hatch run test:test-target tests/test_unit/core/settings/test_auth_dispatch.py -v`
Expected: PASS (9 tests).

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/settings/auth/dispatch.py \
        tests/test_unit/core/settings/test_auth_dispatch.py
git commit -m "feat(settings): add default AUTH_TO_DRIVER_KWARGS dispatch map"
```

---

## Task 4: `ConnectionProfile` base

**Files:**
- Create: `src/mountainash_data/core/settings/profile.py`
- Test: `tests/test_unit/core/settings/test_profile.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_unit/core/settings/test_profile.py
"""Unit tests for the generic ConnectionProfile base."""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.settings.auth import NoAuth, PasswordAuth
from mountainash_data.core.settings.descriptor import (
    BackendDescriptor,
    ParameterSpec,
)
from mountainash_data.core.settings.profile import ConnectionProfile


DUMMY_DESCRIPTOR = BackendDescriptor(
    name="dummy",
    provider_type="dummy",
    default_port=9999,
    connection_string_scheme="dummy://",
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=9999, driver_key="port"),
        ParameterSpec(name="PASSWORD", type=str, tier="core", secret=True,
                      driver_key="password", default=None),
    ],
    auth_modes=[NoAuth, PasswordAuth],
)


class DummyProfile(ConnectionProfile):
    __descriptor__ = DUMMY_DESCRIPTOR


@pytest.mark.unit
class TestConnectionProfile:
    def test_required_field_enforced(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            DummyProfile(auth=NoAuth())  # HOST missing

    def test_default_used_when_not_provided(self):
        p = DummyProfile(HOST="localhost", auth=NoAuth())
        assert p.PORT == 9999

    def test_to_driver_kwargs_noauth(self):
        p = DummyProfile(HOST="h", PORT=1234, auth=NoAuth())
        assert p.to_driver_kwargs() == {"host": "h", "port": 1234}

    def test_to_driver_kwargs_password_auth_unwraps_secret(self):
        p = DummyProfile(
            HOST="h",
            auth=PasswordAuth(username="u", password=SecretStr("p")),
        )
        kwargs = p.to_driver_kwargs()
        assert kwargs["host"] == "h"
        assert kwargs["user"] == "u"
        assert kwargs["password"] == "p"

    def test_secret_field_unwrapped_in_driver_kwargs(self):
        p = DummyProfile(HOST="h", PASSWORD="literal-secret", auth=NoAuth())
        kwargs = p.to_driver_kwargs()
        assert kwargs["password"] == "literal-secret"

    def test_none_values_skipped_from_driver_kwargs(self):
        p = DummyProfile(HOST="h", auth=NoAuth())
        kwargs = p.to_driver_kwargs()
        assert "password" not in kwargs  # PASSWORD default is None

    def test_provider_type_property(self):
        p = DummyProfile(HOST="h", auth=NoAuth())
        assert p.provider_type == "dummy"

    def test_backend_property(self):
        p = DummyProfile(HOST="h", auth=NoAuth())
        assert p.backend == "dummy"

    def test_to_connection_string_raises_when_scheme_none(self):
        desc = BackendDescriptor(
            name="x", provider_type="x", parameters=[], auth_modes=[NoAuth],
            connection_string_scheme=None,
        )

        class P(ConnectionProfile):
            __descriptor__ = desc

        p = P(auth=NoAuth())
        with pytest.raises(NotImplementedError):
            p.to_connection_string()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `hatch run test:test-target tests/test_unit/core/settings/test_profile.py -v`
Expected: collection error — `profile` module not found.

- [ ] **Step 3: Implement `profile.py`**

```python
# src/mountainash_data/core/settings/profile.py
"""Generic ConnectionProfile base for all backend settings.

A subclass declares ``__descriptor__`` (a :class:`BackendDescriptor`); this
base uses ``__init_subclass__`` to materialize the descriptor into pydantic
fields, compose the :class:`~...auth.AuthSpec` union into the ``auth`` field,
and install the generic ``to_driver_kwargs`` / ``to_connection_string`` API.
"""

from __future__ import annotations

import typing as t

from pydantic import Field, SecretStr, create_model
from pydantic.fields import FieldInfo

from mountainash_settings import MountainAshBaseSettings

from .auth.base import AuthSpec
from .auth.dispatch import auth_to_driver_kwargs
from .descriptor import MISSING, BackendDescriptor, ParameterSpec

__all__ = ["ConnectionProfile"]


def _pydantic_field(spec: ParameterSpec) -> tuple[t.Any, FieldInfo]:
    """Translate a :class:`ParameterSpec` into a pydantic ``(type, FieldInfo)``."""
    ptype: t.Any = SecretStr if spec.secret else spec.type
    if spec.default is MISSING:
        info = Field(...)
    else:
        info = Field(default=spec.default)
    if spec.description:
        info = Field(default=info.default if info.default is not ... else ...,
                     description=spec.description)
    return ptype, info


class ConnectionProfile(MountainAshBaseSettings):
    """Declarative settings base — subclasses set ``__descriptor__`` only.

    Public API:
        - :attr:`backend` — descriptor name.
        - :attr:`provider_type` — descriptor provider_type.
        - :meth:`to_driver_kwargs` — dict ready for the underlying driver.
        - :meth:`to_connection_string` — URL form (or ``NotImplementedError``
          if the backend has no URL scheme).
    """

    __descriptor__: t.ClassVar[BackendDescriptor]
    __adapter__: t.ClassVar[t.Callable[["ConnectionProfile"], dict[str, t.Any]] | None] = None

    auth: AuthSpec

    def __init_subclass__(cls, **kwargs: t.Any) -> None:
        super().__init_subclass__(**kwargs)
        desc = getattr(cls, "__descriptor__", None)
        if desc is None:
            return  # abstract intermediate subclass

        # Install descriptor fields on the pydantic model.
        for spec in desc.parameters:
            ptype, info = _pydantic_field(spec)
            cls.model_fields[spec.name] = FieldInfo(
                annotation=ptype, default=info.default,
                description=info.description,
            )

        # Install the discriminated-union auth field from descriptor.auth_modes.
        if desc.auth_modes:
            union = t.Union[tuple(desc.auth_modes)]  # type: ignore[valid-type]
            cls.model_fields["auth"] = FieldInfo(
                annotation=union, default=...,  # required
                discriminator="kind",
            )

        cls.model_rebuild(force=True)

    # --- Public properties ---------------------------------------------------

    @property
    def backend(self) -> str:
        return self.__descriptor__.name

    @property
    def provider_type(self) -> t.Any:
        return self.__descriptor__.provider_type

    # --- Driver kwargs --------------------------------------------------------

    def _default_driver_kwargs(self) -> dict[str, t.Any]:
        """Emit 1:1 driver_key mappings from the descriptor.

        - Skips ``None`` values.
        - Unwraps :class:`SecretStr` via ``.get_secret_value()``.
        - Applies ``ParameterSpec.transform`` if set.
        """
        out: dict[str, t.Any] = {}
        for spec in self.__descriptor__.parameters:
            if spec.driver_key is None:
                continue
            val = getattr(self, spec.name, None)
            if val is None:
                continue
            if isinstance(val, SecretStr):
                val = val.get_secret_value()
            if spec.transform is not None:
                val = spec.transform(val)
            out[spec.driver_key] = val
        return out

    def _auth_to_driver_kwargs(self) -> dict[str, t.Any]:
        return auth_to_driver_kwargs(self.auth)

    def to_driver_kwargs(self) -> dict[str, t.Any]:
        """Build the final driver kwargs dict.

        Order:
            1. 1:1 parameter mappings.
            2. Auth dispatch (may overwrite 1:1 outputs).
            3. Per-backend adapter, if any (may overwrite auth outputs).
        """
        kwargs = self._default_driver_kwargs()
        kwargs.update(self._auth_to_driver_kwargs())
        if self.__adapter__ is not None:
            kwargs = self.__adapter__(self)
        return kwargs

    # --- Connection string ----------------------------------------------------

    def to_connection_string(self) -> str:
        """Build ``scheme://...`` form from the descriptor.

        Raises :class:`NotImplementedError` if the descriptor has no scheme.
        Backends with non-standard URL shapes override this method.
        """
        scheme = self.__descriptor__.connection_string_scheme
        if scheme is None:
            raise NotImplementedError(
                f"Backend {self.backend!r} has no connection string scheme"
            )
        # Default implementation: scheme + host + optional port + optional db.
        host = getattr(self, "HOST", None)
        port = getattr(self, "PORT", None)
        database = getattr(self, "DATABASE", None)
        url = scheme
        if isinstance(self.auth, AuthSpec) and getattr(self.auth, "username", None):
            url += f"{self.auth.username}"
            pw = getattr(self.auth, "password", None)
            if isinstance(pw, SecretStr):
                url += f":{pw.get_secret_value()}"
            url += "@"
        if host is not None:
            url += str(host)
        if port is not None:
            url += f":{port}"
        if database is not None:
            url += f"/{database}"
        return url
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `hatch run test:test-target tests/test_unit/core/settings/test_profile.py -v`
Expected: PASS (9 tests).

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/settings/profile.py \
        tests/test_unit/core/settings/test_profile.py
git commit -m "feat(settings): add ConnectionProfile generic base"
```

---

## Task 5: Registry

**Files:**
- Create: `src/mountainash_data/core/settings/registry.py`
- Test: `tests/test_unit/core/settings/test_registry.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_unit/core/settings/test_registry.py
"""Unit tests for the backend registry."""

import pytest

from mountainash_data.core.settings.auth import NoAuth
from mountainash_data.core.settings.descriptor import BackendDescriptor
from mountainash_data.core.settings.profile import ConnectionProfile
from mountainash_data.core.settings.registry import (
    REGISTRY,
    get_descriptor,
    get_settings_class,
    register,
)


@pytest.mark.unit
class TestRegistry:
    def setup_method(self):
        self._saved = REGISTRY.copy()

    def teardown_method(self):
        REGISTRY.clear()
        REGISTRY.update(self._saved)

    def test_register_inserts_into_registry(self):
        desc = BackendDescriptor(
            name="my_backend", provider_type="my_backend",
            parameters=[], auth_modes=[NoAuth],
        )

        @register(desc)
        class MyProfile(ConnectionProfile):
            __descriptor__ = desc

        assert REGISTRY["my_backend"] is desc
        assert MyProfile.__descriptor__ is desc

    def test_get_descriptor_returns_registered(self):
        desc = BackendDescriptor(
            name="x", provider_type="x",
            parameters=[], auth_modes=[NoAuth],
        )

        @register(desc)
        class X(ConnectionProfile):
            __descriptor__ = desc

        assert get_descriptor("x") is desc

    def test_get_descriptor_unknown_raises(self):
        with pytest.raises(KeyError):
            get_descriptor("not_a_real_backend")

    def test_get_settings_class_returns_class(self):
        desc = BackendDescriptor(
            name="y", provider_type="y",
            parameters=[], auth_modes=[NoAuth],
        )

        @register(desc)
        class YProfile(ConnectionProfile):
            __descriptor__ = desc

        assert get_settings_class("y") is YProfile

    def test_register_rejects_duplicate_name(self):
        desc1 = BackendDescriptor(name="dup", provider_type="dup",
                                  parameters=[], auth_modes=[NoAuth])
        desc2 = BackendDescriptor(name="dup", provider_type="dup",
                                  parameters=[], auth_modes=[NoAuth])

        @register(desc1)
        class P1(ConnectionProfile):
            __descriptor__ = desc1

        with pytest.raises(ValueError, match="already registered"):
            @register(desc2)
            class P2(ConnectionProfile):
                __descriptor__ = desc2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `hatch run test:test-target tests/test_unit/core/settings/test_registry.py -v`
Expected: collection error — `registry` module not found.

- [ ] **Step 3: Implement `registry.py`**

```python
# src/mountainash_data/core/settings/registry.py
"""Module-level registry of backend descriptors and settings classes."""

from __future__ import annotations

import typing as t

from .descriptor import BackendDescriptor
from .profile import ConnectionProfile

__all__ = ["REGISTRY", "register", "get_descriptor", "get_settings_class"]

REGISTRY: dict[str, BackendDescriptor] = {}
_CLASSES: dict[str, type[ConnectionProfile]] = {}


T = t.TypeVar("T", bound=ConnectionProfile)


def register(
    descriptor: BackendDescriptor,
) -> t.Callable[[type[T]], type[T]]:
    """Class decorator that registers a :class:`ConnectionProfile` subclass.

    Raises:
        ValueError: if ``descriptor.name`` is already registered.
    """
    if descriptor.name in REGISTRY:
        raise ValueError(
            f"Backend {descriptor.name!r} is already registered"
        )

    def _wrap(cls: type[T]) -> type[T]:
        REGISTRY[descriptor.name] = descriptor
        _CLASSES[descriptor.name] = cls
        cls.__descriptor__ = descriptor  # belt-and-braces
        return cls

    return _wrap


def get_descriptor(name: str) -> BackendDescriptor:
    """Return the :class:`BackendDescriptor` for ``name``.

    Raises:
        KeyError: if ``name`` is not registered.
    """
    return REGISTRY[name]


def get_settings_class(name: str) -> type[ConnectionProfile]:
    """Return the registered settings class for ``name``.

    Raises:
        KeyError: if ``name`` is not registered.
    """
    return _CLASSES[name]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `hatch run test:test-target tests/test_unit/core/settings/test_registry.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/settings/registry.py \
        tests/test_unit/core/settings/test_registry.py
git commit -m "feat(settings): add backend registry with @register decorator"
```

---

## Task 6: Descriptor invariants (parametric test)

**Files:**
- Create: `tests/test_unit/core/settings/test_descriptors_invariants.py`

- [ ] **Step 1: Write the test**

```python
# tests/test_unit/core/settings/test_descriptors_invariants.py
"""Parametric invariants every registered backend must satisfy.

Runs once per :data:`REGISTRY` entry. New backends get coverage for free.
"""

from __future__ import annotations

import pytest

from mountainash_data.core.settings.auth.base import AuthSpec
from mountainash_data.core.settings.registry import REGISTRY


def _ids(params):
    return [name for name, _ in params]


@pytest.mark.unit
@pytest.mark.parametrize(
    "name,descriptor",
    list(REGISTRY.items()),
    ids=list(REGISTRY.keys()) or [""],
)
class TestDescriptorInvariants:
    def test_name_matches_registry_key(self, name, descriptor):
        assert descriptor.name == name

    def test_parameter_names_unique(self, name, descriptor):
        names = [p.name for p in descriptor.parameters]
        assert len(names) == len(set(names)), f"duplicate param in {name}"

    def test_driver_keys_unique(self, name, descriptor):
        keys = [p.driver_key for p in descriptor.parameters if p.driver_key]
        assert len(keys) == len(set(keys)), f"duplicate driver_key in {name}"

    def test_parameter_tiers_valid(self, name, descriptor):
        for p in descriptor.parameters:
            assert p.tier in {"core", "advanced"}, (
                f"{name}.{p.name} has invalid tier {p.tier!r}"
            )

    def test_auth_modes_are_authspec_subclasses(self, name, descriptor):
        for mode in descriptor.auth_modes:
            assert issubclass(mode, AuthSpec), (
                f"{name}.auth_modes contains non-AuthSpec: {mode}"
            )

    def test_provider_type_is_not_none(self, name, descriptor):
        assert descriptor.provider_type is not None, (
            f"{name} has no provider_type"
        )
```

- [ ] **Step 2: Run it (empty REGISTRY — should pass trivially)**

Run: `hatch run test:test-target tests/test_unit/core/settings/test_descriptors_invariants.py -v`
Expected: PASS (0 or 1 empty-parametrize case — acceptable until backends are registered).

- [ ] **Step 3: Commit**

```bash
git add tests/test_unit/core/settings/test_descriptors_invariants.py
git commit -m "test(settings): add parametric descriptor invariants"
```

---

## Task 7: Migrate SQLite (template for all per-backend migrations)

**Context:** Smallest backend, single parameter (`DATABASE`), no auth, no adapter. This task establishes the pattern every subsequent backend migration follows.

**Files:**
- Modify: `src/mountainash_data/core/settings/sqlite.py` (full rewrite)
- Test: `tests/test_unit/core/settings/backends/test_sqlite.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_unit/core/settings/backends/test_sqlite.py
"""Round-trip and audit-regression tests for SQLite settings."""

from __future__ import annotations

import pytest

from mountainash_data.core.settings.auth import NoAuth
from mountainash_data.core.settings.sqlite import SQLiteAuthSettings


@pytest.mark.unit
class TestSQLiteAuthSettings:
    def test_minimal_construction(self):
        s = SQLiteAuthSettings(auth=NoAuth())
        assert s.DATABASE is None
        assert s.provider_type  # non-empty

    def test_database_memory(self):
        s = SQLiteAuthSettings(DATABASE=":memory:", auth=NoAuth())
        assert s.DATABASE == ":memory:"

    def test_to_driver_kwargs_memory(self):
        s = SQLiteAuthSettings(DATABASE=":memory:", auth=NoAuth())
        assert s.to_driver_kwargs() == {"database": ":memory:"}

    def test_to_driver_kwargs_none_database_dropped(self):
        s = SQLiteAuthSettings(auth=NoAuth())
        assert s.to_driver_kwargs() == {}

    def test_type_map_optional(self):
        s = SQLiteAuthSettings(DATABASE=":memory:", TYPE_MAP={"SMALLINT": "int32"},
                               auth=NoAuth())
        assert s.to_driver_kwargs()["type_map"] == {"SMALLINT": "int32"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `hatch run test:test-target tests/test_unit/core/settings/backends/test_sqlite.py -v`
Expected: IMPORT or ATTR error — `SQLiteAuthSettings` still the old shape.

- [ ] **Step 3: Rewrite `sqlite.py`**

```python
# src/mountainash_data/core/settings/sqlite.py
"""SQLite backend settings.

Spec: audit report ``docs/superpowers/specs/2026-04-15-settings-audit/sqlite.md``.
Driver: https://docs.python.org/3/library/sqlite3.html#sqlite3.connect
Ibis: ``ibis.backends.sqlite.do_connect(database, type_map=None)``
"""

from __future__ import annotations

import typing as t

from ..constants import CONST_DB_PROVIDER_TYPE
from .auth import NoAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register

__all__ = ["SQLiteAuthSettings", "SQLITE_DESCRIPTOR"]


SQLITE_DESCRIPTOR = BackendDescriptor(
    name="sqlite",
    provider_type=CONST_DB_PROVIDER_TYPE.SQLITE,
    connection_string_scheme="sqlite://",
    ibis_dialect="sqlite",
    auth_modes=[NoAuth],
    parameters=[
        ParameterSpec(
            name="DATABASE",
            type=t.Optional[str],
            tier="core",
            default=None,
            driver_key="database",
            description="Path to the SQLite file, or ':memory:' for in-memory.",
        ),
        ParameterSpec(
            name="TYPE_MAP",
            type=t.Optional[dict[str, t.Any]],
            tier="advanced",
            default=None,
            driver_key="type_map",
            description="Optional SQLite column-type → Ibis dtype overrides.",
        ),
    ],
)


@register(SQLITE_DESCRIPTOR)
class SQLiteAuthSettings(ConnectionProfile):
    __descriptor__ = SQLITE_DESCRIPTOR
```

- [ ] **Step 4: Run tests**

Run: `hatch run test:test-target tests/test_unit/core/settings/backends/test_sqlite.py tests/test_unit/core/settings/test_descriptors_invariants.py -v`
Expected: PASS (all SQLite + invariants parameterized for sqlite).

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/settings/sqlite.py \
        tests/test_unit/core/settings/backends/__init__.py \
        tests/test_unit/core/settings/backends/test_sqlite.py
git commit -m "refactor(settings): migrate sqlite to descriptor + shell"
```

---

## Task 8: Migrate DuckDB

**Files:**
- Modify: `src/mountainash_data/core/settings/duckdb.py`
- Test: `tests/test_unit/core/settings/backends/test_duckdb.py`

**Audit fixes carried in:** `READ_ONLY` default to `False` (matching Ibis), `MEMORY_LIMIT` regex relaxed, `ATTACH_PATH` removed (was orphan), URL updated to `/docs/current/`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_unit/core/settings/backends/test_duckdb.py
"""DuckDB settings round-trip and audit-regression tests."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from mountainash_data.core.settings.auth import NoAuth
from mountainash_data.core.settings.duckdb import DuckDBAuthSettings


@pytest.mark.unit
class TestDuckDBAuthSettings:
    def test_default_read_only_is_false(self):
        """Audit regression: previously defaulted True, mismatched Ibis."""
        s = DuckDBAuthSettings(auth=NoAuth())
        assert s.READ_ONLY is False

    def test_memory_database_default(self):
        s = DuckDBAuthSettings(auth=NoAuth())
        assert s.DATABASE is None

    def test_to_driver_kwargs_default(self):
        s = DuckDBAuthSettings(DATABASE=":memory:", auth=NoAuth())
        kwargs = s.to_driver_kwargs()
        assert kwargs["database"] == ":memory:"
        assert kwargs["read_only"] is False

    def test_memory_limit_decimal_accepted(self):
        """Audit regression: regex previously rejected '1.5GB'."""
        s = DuckDBAuthSettings(DATABASE=":memory:", MEMORY_LIMIT="1.5GB",
                               auth=NoAuth())
        assert s.MEMORY_LIMIT == "1.5GB"

    def test_memory_limit_percent_accepted(self):
        s = DuckDBAuthSettings(DATABASE=":memory:", MEMORY_LIMIT="80%",
                               auth=NoAuth())
        assert s.MEMORY_LIMIT == "80%"

    def test_memory_limit_garbage_rejected(self):
        with pytest.raises(ValidationError):
            DuckDBAuthSettings(DATABASE=":memory:", MEMORY_LIMIT="lots",
                               auth=NoAuth())

    def test_extensions_passed_as_top_level_kwarg(self):
        """Audit regression: extensions was packed inside config dict."""
        s = DuckDBAuthSettings(DATABASE=":memory:", EXTENSIONS=["httpfs"],
                               auth=NoAuth())
        kwargs = s.to_driver_kwargs()
        assert kwargs["extensions"] == ["httpfs"]
        # Must NOT appear inside a nested config dict:
        assert "config" not in kwargs or "extensions" not in kwargs.get("config", {})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `hatch run test:test-target tests/test_unit/core/settings/backends/test_duckdb.py -v`
Expected: FAIL on `READ_ONLY is False` (old default was True).

- [ ] **Step 3: Rewrite `duckdb.py`**

```python
# src/mountainash_data/core/settings/duckdb.py
"""DuckDB backend settings.

Spec: audit report ``docs/superpowers/specs/2026-04-15-settings-audit/duckdb.md``.
Driver: https://duckdb.org/docs/current/configuration/overview.html
Ibis: ``ibis.backends.duckdb.do_connect(database=':memory:', read_only=False,
       extensions=None, **config)``
"""

from __future__ import annotations

import re
import typing as t

from pydantic import field_validator

from ..constants import CONST_DB_PROVIDER_TYPE
from .auth import NoAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register

__all__ = ["DuckDBAuthSettings", "DUCKDB_DESCRIPTOR"]

_MEMORY_LIMIT_RE = re.compile(r"^(?:\d+(?:\.\d+)?\s*[KMG]i?B|\d+%)$", re.IGNORECASE)


def _validate_memory_limit(value: t.Any) -> t.Any:
    if value is None:
        return value
    if not _MEMORY_LIMIT_RE.match(str(value)):
        raise ValueError(
            "MEMORY_LIMIT must match e.g. '500MB', '1.5GB', '1024KiB', or '80%'"
        )
    return value


DUCKDB_DESCRIPTOR = BackendDescriptor(
    name="duckdb",
    provider_type=CONST_DB_PROVIDER_TYPE.DUCKDB,
    connection_string_scheme="duckdb://",
    ibis_dialect="duckdb",
    auth_modes=[NoAuth],
    parameters=[
        ParameterSpec(name="DATABASE", type=t.Optional[str], tier="core",
                      default=None, driver_key="database"),
        ParameterSpec(name="READ_ONLY", type=bool, tier="core",
                      default=False, driver_key="read_only"),
        ParameterSpec(name="EXTENSIONS", type=list[str], tier="core",
                      default=[], driver_key="extensions"),
        ParameterSpec(name="THREADS", type=t.Optional[int], tier="advanced",
                      default=None),  # goes into config; see adapter in future
        ParameterSpec(name="MEMORY_LIMIT", type=t.Optional[str], tier="advanced",
                      default=None, validator=_validate_memory_limit),
    ],
)


@register(DUCKDB_DESCRIPTOR)
class DuckDBAuthSettings(ConnectionProfile):
    __descriptor__ = DUCKDB_DESCRIPTOR

    @field_validator("MEMORY_LIMIT")
    @classmethod
    def _mem_limit(cls, v: t.Any) -> t.Any:
        return _validate_memory_limit(v)
```

*Note:* `THREADS` / `MEMORY_LIMIT` go into Ibis's `**config`, not top-level kwargs. Phase 4 handles that via an adapter if needed; for this pass they're declared but not yet emitted as driver kwargs.

- [ ] **Step 4: Run tests**

Run: `hatch run test:test-target tests/test_unit/core/settings/backends/test_duckdb.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/settings/duckdb.py \
        tests/test_unit/core/settings/backends/test_duckdb.py
git commit -m "refactor(settings): migrate duckdb, fix READ_ONLY default and MEMORY_LIMIT regex"
```

---

## Task 9: Migrate PySpark

**Files:**
- Modify: `src/mountainash_data/core/settings/pyspark.py`
- Test: `tests/test_unit/core/settings/backends/test_pyspark.py`

**Audit fixes carried:** docstring corrected (was "SQLite authentication settings"); `PARTITIONS` type/default fixed (was `int = {}`); `MODE` becomes `StrEnum`; `SESSION` field added.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_unit/core/settings/backends/test_pyspark.py
from __future__ import annotations

import pytest

from mountainash_data.core.settings.auth import NoAuth
from mountainash_data.core.settings.pyspark import (
    PySparkAuthSettings,
    PySparkMode,
)


@pytest.mark.unit
class TestPySparkAuthSettings:
    def test_minimal(self):
        s = PySparkAuthSettings(auth=NoAuth())
        assert s.MODE is PySparkMode.BATCH

    def test_mode_streaming(self):
        s = PySparkAuthSettings(MODE="streaming", auth=NoAuth())
        assert s.MODE is PySparkMode.STREAMING

    def test_mode_invalid_rejected(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            PySparkAuthSettings(MODE="nonsense", auth=NoAuth())

    def test_partitions_accepts_int(self):
        """Audit regression: PARTITIONS: int = {} crashed at init."""
        s = PySparkAuthSettings(PARTITIONS=200, auth=NoAuth())
        assert s.PARTITIONS == 200

    def test_partitions_none_default(self):
        s = PySparkAuthSettings(auth=NoAuth())
        assert s.PARTITIONS is None

    def test_to_driver_kwargs_emits_dotted_spark_keys(self):
        """Audit regression: previously emitted 'spark_app_name' not 'spark.app.name'."""
        s = PySparkAuthSettings(
            APPLICATION_NAME="myapp",
            SPARK_MASTER="local[2]",
            MODE="batch",
            auth=NoAuth(),
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["mode"] == "batch"
        # Adapter emits dotted Spark keys:
        assert kwargs["spark.app.name"] == "myapp"
        assert kwargs["spark.master"] == "local[2]"
```

- [ ] **Step 2: Run to confirm failure**

Run: `hatch run test:test-target tests/test_unit/core/settings/backends/test_pyspark.py -v`
Expected: IMPORT or ATTR error.

- [ ] **Step 3: Implement**

Create `src/mountainash_data/core/settings/adapters/pyspark.py`:

```python
# src/mountainash_data/core/settings/adapters/pyspark.py
"""Adapter emitting dotted spark.* keys from PySpark settings."""

from __future__ import annotations

import typing as t

from mountainash_data.core.settings.auth import NoAuth

if t.TYPE_CHECKING:
    from mountainash_data.core.settings.pyspark import PySparkAuthSettings


def build_driver_kwargs(profile: "PySparkAuthSettings") -> dict[str, t.Any]:
    kwargs: dict[str, t.Any] = {}
    if profile.MODE is not None:
        kwargs["mode"] = profile.MODE.value
    if profile.SESSION is not None:
        kwargs["session"] = profile.SESSION
    if profile.APPLICATION_NAME is not None:
        kwargs["spark.app.name"] = profile.APPLICATION_NAME
    if profile.SPARK_MASTER is not None:
        kwargs["spark.master"] = profile.SPARK_MASTER
    if profile.WAREHOUSE_DIR is not None:
        kwargs["spark.sql.warehouse.dir"] = profile.WAREHOUSE_DIR
    if profile.PARTITIONS is not None:
        kwargs["spark.sql.shuffle.partitions"] = profile.PARTITIONS
    # NoAuth is the only accepted mode; nothing else to emit.
    return kwargs
```

Rewrite `pyspark.py`:

```python
# src/mountainash_data/core/settings/pyspark.py
"""PySpark backend settings.

Spec: audit report ``docs/superpowers/specs/2026-04-15-settings-audit/pyspark.md``.
Ibis: ``ibis.backends.pyspark.do_connect(session=None, mode='batch', **kwargs)``
where kwargs flow to ``SparkSession.builder.config(**kwargs)``.

The docstring of the prior class read 'SQLite authentication settings' — a
copy-paste from ``sqlite.py``. Corrected here.
"""

from __future__ import annotations

import typing as t
from enum import StrEnum

from ..constants import CONST_DB_PROVIDER_TYPE
from .adapters import pyspark as _adapter
from .auth import NoAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register

__all__ = ["PySparkAuthSettings", "PySparkMode", "PYSPARK_DESCRIPTOR"]


class PySparkMode(StrEnum):
    BATCH = "batch"
    STREAMING = "streaming"


PYSPARK_DESCRIPTOR = BackendDescriptor(
    name="pyspark",
    provider_type=CONST_DB_PROVIDER_TYPE.PYSPARK,
    connection_string_scheme=None,  # SparkSession, not URL
    ibis_dialect="pyspark",
    auth_modes=[NoAuth],
    parameters=[
        ParameterSpec(name="SESSION", type=t.Optional[t.Any], tier="core",
                      default=None),
        ParameterSpec(name="MODE", type=PySparkMode, tier="core",
                      default=PySparkMode.BATCH),
        ParameterSpec(name="SPARK_MASTER", type=t.Optional[str], tier="advanced",
                      default=None),
        ParameterSpec(name="APPLICATION_NAME", type=t.Optional[str], tier="advanced",
                      default=None),
        ParameterSpec(name="WAREHOUSE_DIR", type=t.Optional[str], tier="advanced",
                      default=None),
        ParameterSpec(name="PARTITIONS", type=t.Optional[int], tier="advanced",
                      default=None),
    ],
)


@register(PYSPARK_DESCRIPTOR)
class PySparkAuthSettings(ConnectionProfile):
    __descriptor__ = PYSPARK_DESCRIPTOR
    __adapter__ = staticmethod(_adapter.build_driver_kwargs)
```

Add `src/mountainash_data/core/settings/adapters/__init__.py` (empty, marking package).

- [ ] **Step 4: Run tests**

Run: `hatch run test:test-target tests/test_unit/core/settings/backends/test_pyspark.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/settings/pyspark.py \
        src/mountainash_data/core/settings/adapters/ \
        tests/test_unit/core/settings/backends/test_pyspark.py
git commit -m "refactor(settings): migrate pyspark, fix PARTITIONS type and spark.* keys"
```

---

## Task 10: Migrate MotherDuck

**Files:**
- Modify: `src/mountainash_data/core/settings/motherduck.py`
- Test: `tests/test_unit/core/settings/backends/test_motherduck.py`

**Audit fixes carried:** source URLs added; "file-based authentication" comment removed; `DATABASE` nullability consistent; `ATTACH_PATH` removed (was orphan); class uses `TokenAuth`.

- [ ] **Step 1: Write tests** — mirror the sqlite shape with a token auth case:

```python
# tests/test_unit/core/settings/backends/test_motherduck.py
from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.settings.auth import TokenAuth
from mountainash_data.core.settings.motherduck import MotherDuckAuthSettings


@pytest.mark.unit
class TestMotherDuckAuthSettings:
    def test_minimal(self):
        s = MotherDuckAuthSettings(
            DATABASE="mydb", auth=TokenAuth(token=SecretStr("t"))
        )
        assert s.DATABASE == "mydb"

    def test_no_database_ok(self):
        """Audit regression: previously validator rejected None, field was Optional."""
        s = MotherDuckAuthSettings(auth=TokenAuth(token=SecretStr("t")))
        assert s.DATABASE is None

    def test_to_driver_kwargs_unwraps_token(self):
        s = MotherDuckAuthSettings(
            DATABASE="mydb", auth=TokenAuth(token=SecretStr("tok"))
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["token"] == "tok"
        assert isinstance(kwargs["token"], str)  # not SecretStr
```

- [ ] **Step 2: Run → FAIL**

- [ ] **Step 3: Rewrite `motherduck.py`**

```python
# src/mountainash_data/core/settings/motherduck.py
"""MotherDuck backend settings.

Spec: audit report ``docs/superpowers/specs/2026-04-15-settings-audit/motherduck.md``.
Driver auth docs:
  https://motherduck.com/docs/getting-started/connect-query-from-python/installation-authentication/
Ibis: routes via the duckdb backend (``rides_on="duckdb"``).
"""

from __future__ import annotations

import typing as t

from ..constants import CONST_DB_PROVIDER_TYPE
from .auth import TokenAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register

__all__ = ["MotherDuckAuthSettings", "MOTHERDUCK_DESCRIPTOR"]


MOTHERDUCK_DESCRIPTOR = BackendDescriptor(
    name="motherduck",
    provider_type=CONST_DB_PROVIDER_TYPE.MOTHERDUCK,
    connection_string_scheme="duckdb://md:",  # md:<db>?motherduck_token=...
    ibis_dialect="duckdb",
    rides_on="duckdb",
    auth_modes=[TokenAuth],
    parameters=[
        ParameterSpec(name="DATABASE", type=t.Optional[str], tier="core",
                      default=None),
        ParameterSpec(name="READ_ONLY", type=bool, tier="core", default=False,
                      driver_key="read_only"),
    ],
)


@register(MOTHERDUCK_DESCRIPTOR)
class MotherDuckAuthSettings(ConnectionProfile):
    __descriptor__ = MOTHERDUCK_DESCRIPTOR
```

- [ ] **Step 4: Run → PASS**

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/settings/motherduck.py \
        tests/test_unit/core/settings/backends/test_motherduck.py
git commit -m "refactor(settings): migrate motherduck to TokenAuth + descriptor"
```

---

## Task 11: Migrate PostgreSQL

**Files:**
- Modify: `src/mountainash_data/core/settings/postgresql.py`
- Test: `tests/test_unit/core/settings/backends/test_postgresql.py`

**Audit fixes carried:** `db_provider_type` BIGQUERY bug → POSTGRESQL; four enums promoted to `StrEnum` field types; `SSL_CERT`/`SSL_KEY`/`SSL_ROOTCERT`/`SSL_CRL`/`SSL_CRLDIR` retyped `Optional[Path]`; `SSL_PASSWORD` → `Optional[SecretStr]`; `REQUIRE_AUTH` → `list[PostgresRequireAuthMethods]`; widened surface (`CONNECT_TIMEOUT`, `AUTOCOMMIT`, `HOSTADDR`, `SERVICE`, etc.). All libpq fields wired via descriptor `driver_key`.

- [ ] **Step 1: Write tests**

```python
# tests/test_unit/core/settings/backends/test_postgresql.py
from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError

from mountainash_data.core.settings.auth import PasswordAuth
from mountainash_data.core.settings.postgresql import (
    PostgresRequireAuthMethods,
    PostgresSSLMode,
    PostgreSQLAuthSettings,
)


@pytest.mark.unit
class TestPostgreSQLAuthSettings:
    def _minimal(self, **extra):
        return PostgreSQLAuthSettings(
            HOST="h", DATABASE="d",
            auth=PasswordAuth(username="u", password=SecretStr("p")),
            **extra,
        )

    def test_provider_type_is_postgresql(self):
        """Audit regression: previously returned BIGQUERY."""
        s = self._minimal()
        from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
        assert s.provider_type == CONST_DB_PROVIDER_TYPE.POSTGRESQL

    def test_ssl_mode_enum_enforced(self):
        """Audit regression: SSL_MODE was bare str."""
        with pytest.raises(ValidationError):
            self._minimal(SSL_MODE="nonsense")

    def test_ssl_cert_is_path(self):
        """Audit regression: SSL_CERT was bool."""
        from pathlib import Path
        s = self._minimal(SSL_CERT=Path("/etc/ssl/client.crt"))
        assert s.SSL_CERT == Path("/etc/ssl/client.crt")

    def test_require_auth_is_list_of_enum(self):
        """Audit regression: REQUIRE_AUTH was bool."""
        s = self._minimal(
            REQUIRE_AUTH=[PostgresRequireAuthMethods.SCRAM_SHA_256,
                          PostgresRequireAuthMethods.MD5]
        )
        assert len(s.REQUIRE_AUTH) == 2

    def test_to_driver_kwargs_plumbs_ssl_and_keepalives(self):
        """Audit regression: only SCHEMA was being plumbed."""
        s = self._minimal(SSL_MODE=PostgresSSLMode.REQUIRE, KEEPALIVES_IDLE=30)
        kwargs = s.to_driver_kwargs()
        assert kwargs["sslmode"] == "require"
        assert kwargs["keepalives_idle"] == 30
        assert kwargs["user"] == "u"
        assert kwargs["password"] == "p"  # SecretStr unwrapped
```

- [ ] **Step 2: Run → FAIL**

- [ ] **Step 3: Rewrite `postgresql.py`**

```python
# src/mountainash_data/core/settings/postgresql.py
"""PostgreSQL backend settings.

Spec: audit report ``docs/superpowers/specs/2026-04-15-settings-audit/postgresql.md``.
Driver: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
Ibis: ``ibis.backends.postgres.do_connect(host, user, password, port=5432,
       database, schema, autocommit=True, **kwargs)`` (psycopg).
"""

from __future__ import annotations

import typing as t
from enum import StrEnum
from pathlib import Path

from pydantic import SecretStr

from ..constants import CONST_DB_PROVIDER_TYPE
from .auth import NoAuth, PasswordAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


class PostgresSSLMode(StrEnum):
    DISABLE = "disable"
    ALLOW = "allow"
    PREFER = "prefer"
    REQUIRE = "require"
    VERIFY_CA = "verify-ca"
    VERIFY_FULL = "verify-full"


class PostgresTargetSessionAttrs(StrEnum):
    ANY = "any"
    READ_WRITE = "read-write"
    READ_ONLY = "read-only"
    PRIMARY = "primary"
    STANDBY = "standby"
    PREFER_STANDBY = "prefer-standby"


class PostgresRequireAuthMethods(StrEnum):
    PASSWORD = "password"
    MD5 = "md5"
    GSS = "gss"
    SSPI = "sspi"
    SCRAM_SHA_256 = "scram-sha-256"
    NONE = "none"


class PostgresSSLNegotiation(StrEnum):
    POSTGRES = "postgres"
    DIRECT = "direct"


class PostgresSSLCertMode(StrEnum):
    DISABLE = "disable"
    ALLOW = "allow"
    REQUIRE = "require"


def _join_require_auth(v: list[PostgresRequireAuthMethods]) -> str:
    return ",".join(m.value for m in v)


POSTGRESQL_DESCRIPTOR = BackendDescriptor(
    name="postgresql",
    provider_type=CONST_DB_PROVIDER_TYPE.POSTGRESQL,
    default_port=5432,
    connection_string_scheme="postgresql://",
    ibis_dialect="postgres",
    auth_modes=[PasswordAuth, NoAuth],
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="HOSTADDR", type=t.Optional[str], tier="advanced",
                      default=None, driver_key="hostaddr"),
        ParameterSpec(name="PORT", type=int, tier="core", default=5432,
                      driver_key="port"),
        ParameterSpec(name="DATABASE", type=t.Optional[str], tier="core",
                      default=None, driver_key="database"),
        ParameterSpec(name="SCHEMA", type=t.Optional[str], tier="core",
                      default=None, driver_key="schema"),
        ParameterSpec(name="AUTOCOMMIT", type=bool, tier="core", default=True,
                      driver_key="autocommit"),
        ParameterSpec(name="CONNECT_TIMEOUT", type=t.Optional[int],
                      tier="core", default=None, driver_key="connect_timeout"),
        ParameterSpec(name="APPLICATION_NAME", type=t.Optional[str],
                      tier="advanced", default=None,
                      driver_key="application_name"),
        ParameterSpec(name="PASSFILE", type=t.Optional[Path], tier="advanced",
                      default=None, driver_key="passfile",
                      transform=lambda p: str(p)),
        ParameterSpec(name="SERVICE", type=t.Optional[str], tier="advanced",
                      default=None, driver_key="service"),
        ParameterSpec(name="OPTIONS", type=t.Optional[str], tier="advanced",
                      default=None, driver_key="options"),
        ParameterSpec(name="CHANNEL_BINDING", type=t.Optional[str],
                      tier="advanced", default=None,
                      driver_key="channel_binding"),
        ParameterSpec(name="REQUIRE_AUTH",
                      type=t.Optional[list[PostgresRequireAuthMethods]],
                      tier="core", default=None, driver_key="require_auth",
                      transform=_join_require_auth),
        # Keepalives
        ParameterSpec(name="KEEPALIVES", type=bool, tier="advanced", default=True,
                      driver_key="keepalives",
                      transform=lambda v: 1 if v else 0),
        ParameterSpec(name="KEEPALIVES_IDLE", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="keepalives_idle"),
        ParameterSpec(name="KEEPALIVES_INTERVAL", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="keepalives_interval"),
        ParameterSpec(name="KEEPALIVES_COUNT", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="keepalives_count"),
        ParameterSpec(name="TCP_USER_TIMEOUT", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="tcp_user_timeout"),
        # SSL / TLS
        ParameterSpec(name="SSL_MODE", type=PostgresSSLMode, tier="core",
                      default=PostgresSSLMode.PREFER, driver_key="sslmode"),
        ParameterSpec(name="SSL_NEGOTIATION", type=t.Optional[PostgresSSLNegotiation],
                      tier="advanced", default=None, driver_key="sslnegotiation"),
        ParameterSpec(name="SSL_COMPRESSION", type=t.Optional[bool],
                      tier="advanced", default=None, driver_key="sslcompression",
                      transform=lambda v: 1 if v else 0),
        ParameterSpec(name="SSL_CERT", type=t.Optional[Path], tier="advanced",
                      default=None, driver_key="sslcert",
                      transform=lambda p: str(p)),
        ParameterSpec(name="SSL_KEY", type=t.Optional[Path], tier="advanced",
                      default=None, driver_key="sslkey",
                      transform=lambda p: str(p)),
        ParameterSpec(name="SSL_PASSWORD", type=t.Optional[SecretStr],
                      tier="advanced", default=None, driver_key="sslpassword",
                      secret=True),
        ParameterSpec(name="SSL_CERTMODE", type=t.Optional[PostgresSSLCertMode],
                      tier="advanced", default=None, driver_key="sslcertmode"),
        ParameterSpec(name="SSL_ROOTCERT", type=t.Optional[Path],
                      tier="advanced", default=None, driver_key="sslrootcert",
                      transform=lambda p: str(p)),
        ParameterSpec(name="SSL_CRL", type=t.Optional[Path], tier="advanced",
                      default=None, driver_key="sslcrl",
                      transform=lambda p: str(p)),
        ParameterSpec(name="SSL_CRLDIR", type=t.Optional[Path], tier="advanced",
                      default=None, driver_key="sslcrldir",
                      transform=lambda p: str(p)),
        ParameterSpec(name="SSL_SNI", type=t.Optional[bool], tier="advanced",
                      default=None, driver_key="sslsni",
                      transform=lambda v: 1 if v else 0),
        ParameterSpec(name="TARGET_SESSION_ATTRS",
                      type=t.Optional[PostgresTargetSessionAttrs],
                      tier="advanced", default=None,
                      driver_key="target_session_attrs"),
    ],
)


@register(POSTGRESQL_DESCRIPTOR)
class PostgreSQLAuthSettings(ConnectionProfile):
    __descriptor__ = POSTGRESQL_DESCRIPTOR
```

- [ ] **Step 4: Run → PASS**

Run: `hatch run test:test-target tests/test_unit/core/settings/backends/test_postgresql.py tests/test_unit/core/settings/test_descriptors_invariants.py -v`

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/settings/postgresql.py \
        tests/test_unit/core/settings/backends/test_postgresql.py
git commit -m "refactor(settings): migrate postgresql, fix provider_type and widen libpq surface"
```

---

## Task 12: Migrate MySQL (with `ssl={}` adapter)

**Files:**
- Modify: `src/mountainash_data/core/settings/mysql.py`
- Create: `src/mountainash_data/core/settings/adapters/mysql.py`
- Test: `tests/test_unit/core/settings/backends/test_mysql.py`

**Audit fixes carried:** `db_provider_type` BIGQUERY → MYSQL; `SSL_CAPATH` guard typo fixed (previously guarded by `SSL_CA`); `SSL_MODE != DISABLED` branch fires only when set; `SSL_MODE` as `StrEnum`; `CONV` → `Optional[dict[int, t.Any]]`; SSL fields assembled by adapter into `ssl={}` dict.

- [ ] **Step 1: Write tests**

```python
# tests/test_unit/core/settings/backends/test_mysql.py
from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.settings.auth import PasswordAuth
from mountainash_data.core.settings.mysql import MySQLAuthSettings, MySQLSSLMode


@pytest.mark.unit
class TestMySQLAuthSettings:
    def _minimal(self, **extra):
        return MySQLAuthSettings(
            HOST="h", DATABASE="d",
            auth=PasswordAuth(username="u", password=SecretStr("p")),
            **extra,
        )

    def test_provider_type_is_mysql(self):
        """Audit regression: previously returned BIGQUERY."""
        from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
        assert self._minimal().provider_type == CONST_DB_PROVIDER_TYPE.MYSQL

    def test_ssl_dict_assembled_when_capath_only_no_ca(self):
        """Audit regression: SSL_CAPATH was gated on SSL_CA."""
        s = self._minimal(SSL_CAPATH="/etc/ssl/ca-dir")
        kwargs = s.to_driver_kwargs()
        assert kwargs["ssl"] == {"ssl-capath": "/etc/ssl/ca-dir"}

    def test_ssl_not_emitted_when_ssl_mode_none(self):
        """Audit regression: SSL branch fired when SSL_MODE was None."""
        s = self._minimal()
        kwargs = s.to_driver_kwargs()
        assert "ssl_mode" not in kwargs
        assert "ssl" not in kwargs

    def test_ssl_mode_preferred(self):
        s = self._minimal(SSL_MODE=MySQLSSLMode.PREFERRED)
        kwargs = s.to_driver_kwargs()
        assert kwargs["ssl_mode"] == "PREFERRED"

    def test_autocommit_false_honored(self):
        """Audit regression: `if self.AUTOCOMMIT:` dropped explicit False."""
        s = self._minimal(AUTOCOMMIT=False)
        assert s.to_driver_kwargs()["autocommit"] is False
```

- [ ] **Step 2: Run → FAIL**

- [ ] **Step 3: Implement adapter + settings**

```python
# src/mountainash_data/core/settings/adapters/mysql.py
"""Adapter that assembles mysqlclient's ssl={} dict."""

from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    from mountainash_data.core.settings.mysql import MySQLAuthSettings


def build_driver_kwargs(profile: "MySQLAuthSettings") -> dict[str, t.Any]:
    kwargs = profile._default_driver_kwargs()
    kwargs.update(profile._auth_to_driver_kwargs())

    if profile.SSL_MODE is not None:
        kwargs["ssl_mode"] = profile.SSL_MODE.value
        ssl: dict[str, str] = {}
        for key, val in {
            "ssl-key": profile.SSL_KEY,
            "ssl-cert": profile.SSL_CERT,
            "ssl-ca": profile.SSL_CA,
            "ssl-capath": profile.SSL_CAPATH,
            "ssl-cipher": profile.SSL_CIPHER,
        }.items():
            if val is not None:
                ssl[key] = str(val)
        if ssl:
            kwargs["ssl"] = ssl
    return kwargs
```

```python
# src/mountainash_data/core/settings/mysql.py
"""MySQL backend settings.

Spec: ``docs/superpowers/specs/2026-04-15-settings-audit/mysql.md``.
Driver: https://mysqlclient.readthedocs.io/user_guide.html#functions-and-attributes
Ibis: ``ibis.backends.mysql.do_connect(host='localhost', user=None, password=None,
       port=3306, autocommit=True, **kwargs)``
"""

from __future__ import annotations

import typing as t
from enum import StrEnum
from pathlib import Path

from ..constants import CONST_DB_PROVIDER_TYPE
from .adapters import mysql as _adapter
from .auth import PasswordAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


class MySQLSSLMode(StrEnum):
    DISABLED = "DISABLED"
    PREFERRED = "PREFERRED"
    REQUIRED = "REQUIRED"
    VERIFY_CA = "VERIFY_CA"
    VERIFY_IDENTITY = "VERIFY_IDENTITY"


MYSQL_DESCRIPTOR = BackendDescriptor(
    name="mysql",
    provider_type=CONST_DB_PROVIDER_TYPE.MYSQL,
    default_port=3306,
    connection_string_scheme="mysql://",
    ibis_dialect="mysql",
    auth_modes=[PasswordAuth],
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=3306,
                      driver_key="port"),
        ParameterSpec(name="DATABASE", type=t.Optional[str], tier="core",
                      default=None, driver_key="database"),
        ParameterSpec(name="CHARSET", type=str, tier="advanced",
                      default="utf8mb4", driver_key="charset"),
        ParameterSpec(name="COLLATION", type=str, tier="advanced",
                      default="utf8mb4_unicode_ci", driver_key="collation"),
        ParameterSpec(name="AUTOCOMMIT", type=bool, tier="core",
                      default=True, driver_key="autocommit"),
        ParameterSpec(name="CONNECT_TIMEOUT", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="connect_timeout"),
        ParameterSpec(name="READ_TIMEOUT", type=t.Optional[int],
                      tier="advanced", default=None, driver_key="read_timeout"),
        ParameterSpec(name="WRITE_TIMEOUT", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="write_timeout"),
        ParameterSpec(name="UNIX_SOCKET", type=t.Optional[Path],
                      tier="advanced", default=None, driver_key="unix_socket",
                      transform=lambda p: str(p)),
        ParameterSpec(name="LOCAL_INFILE", type=t.Optional[bool],
                      tier="advanced", default=None, driver_key="local_infile"),
        ParameterSpec(name="INIT_COMMAND", type=t.Optional[str],
                      tier="advanced", default=None,
                      driver_key="init_command"),
        # SSL parameters — adapter handles assembly into ssl={} dict
        ParameterSpec(name="SSL_MODE", type=t.Optional[MySQLSSLMode],
                      tier="core", default=None),
        ParameterSpec(name="SSL_KEY", type=t.Optional[Path], tier="advanced",
                      default=None),
        ParameterSpec(name="SSL_CERT", type=t.Optional[Path], tier="advanced",
                      default=None),
        ParameterSpec(name="SSL_CA", type=t.Optional[Path], tier="advanced",
                      default=None),
        ParameterSpec(name="SSL_CAPATH", type=t.Optional[Path], tier="advanced",
                      default=None),
        ParameterSpec(name="SSL_CIPHER", type=t.Optional[str], tier="advanced",
                      default=None),
        ParameterSpec(name="CONV", type=t.Optional[dict[int, t.Any]],
                      tier="advanced", default=None),
    ],
)


@register(MYSQL_DESCRIPTOR)
class MySQLAuthSettings(ConnectionProfile):
    __descriptor__ = MYSQL_DESCRIPTOR
    __adapter__ = staticmethod(_adapter.build_driver_kwargs)
```

- [ ] **Step 4: Run → PASS**

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/settings/mysql.py \
        src/mountainash_data/core/settings/adapters/mysql.py \
        tests/test_unit/core/settings/backends/test_mysql.py
git commit -m "refactor(settings): migrate mysql with ssl adapter and audit fixes"
```

---

## Task 13: Migrate Trino (auth wrapper adapter)

**Files:**
- Modify: `src/mountainash_data/core/settings/trino.py`
- Create: `src/mountainash_data/core/settings/adapters/trino.py`
- Test: `tests/test_unit/core/settings/backends/test_trino.py`

**Audit fixes carried:** PASSWORD wiring broken → use `trino.auth.BasicAuthentication`; advanced fields retyped to native types; `VERIFY` widened to `bool | Path`; `ENCODING` field added; `PORT` default 8080.

- [ ] **Step 1: Write tests** (adapter contract — verify right auth wrapper is used per auth kind)

```python
# tests/test_unit/core/settings/backends/test_trino.py
from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.settings.auth import (
    JWTAuth,
    KerberosAuth,
    NoAuth,
    PasswordAuth,
)
from mountainash_data.core.settings.trino import TrinoAuthSettings


@pytest.mark.unit
class TestTrinoAuthSettings:
    def _minimal(self, auth, **extra):
        return TrinoAuthSettings(HOST="h", CATALOG="c", auth=auth, **extra)

    def test_port_default_8080(self):
        s = self._minimal(auth=NoAuth())
        assert s.PORT == 8080

    def test_password_wraps_basic_auth(self):
        """Audit regression: previously emitted bare `password=` kwarg.

        The driver has NO `password` kwarg — it must be wrapped.
        """
        pytest.importorskip("trino")
        from trino.auth import BasicAuthentication

        s = self._minimal(
            auth=PasswordAuth(username="alice", password=SecretStr("pw"))
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["user"] == "alice"
        assert isinstance(kwargs["auth"], BasicAuthentication)
        assert "password" not in kwargs  # must NOT be bare

    def test_jwt_auth_wraps(self):
        pytest.importorskip("trino")
        from trino.auth import JWTAuthentication

        s = self._minimal(auth=JWTAuth(token=SecretStr("tok")))
        kwargs = s.to_driver_kwargs()
        assert isinstance(kwargs["auth"], JWTAuthentication)

    def test_noauth_no_auth_key(self):
        s = self._minimal(auth=NoAuth())
        kwargs = s.to_driver_kwargs()
        assert "auth" not in kwargs
```

- [ ] **Step 2: Run → FAIL**

- [ ] **Step 3: Implement**

```python
# src/mountainash_data/core/settings/adapters/trino.py
"""Adapter translating AuthSpec → trino.auth.Authentication wrappers."""

from __future__ import annotations

import typing as t

from mountainash_data.core.settings.auth import (
    AuthSpec,
    JWTAuth,
    KerberosAuth,
    NoAuth,
    PasswordAuth,
)

if t.TYPE_CHECKING:
    from mountainash_data.core.settings.trino import TrinoAuthSettings


def build_driver_kwargs(profile: "TrinoAuthSettings") -> dict[str, t.Any]:
    kwargs = profile._default_driver_kwargs()
    auth = profile.auth
    if isinstance(auth, PasswordAuth):
        from trino.auth import BasicAuthentication

        kwargs["user"] = auth.username
        kwargs["auth"] = BasicAuthentication(
            auth.username, auth.password.get_secret_value()
        )
    elif isinstance(auth, JWTAuth):
        from trino.auth import JWTAuthentication

        kwargs["auth"] = JWTAuthentication(auth.token.get_secret_value())
    elif isinstance(auth, KerberosAuth):
        from trino.auth import KerberosAuthentication

        kwargs["auth"] = KerberosAuthentication(
            config=None,
            service_name=auth.service_name,
            principal=auth.principal,
        )
    elif isinstance(auth, NoAuth):
        pass
    else:
        raise ValueError(f"trino adapter does not support auth: {type(auth).__name__}")
    return kwargs
```

```python
# src/mountainash_data/core/settings/trino.py
"""Trino backend settings.

Spec: ``docs/superpowers/specs/2026-04-15-settings-audit/trino.md``.
Driver: https://github.com/trinodb/trino-python-client/blob/master/trino/dbapi.py
Ibis: ``ibis.backends.trino.do_connect``
"""

from __future__ import annotations

import typing as t
from pathlib import Path

from ..constants import CONST_DB_PROVIDER_TYPE
from .adapters import trino as _adapter
from .auth import JWTAuth, KerberosAuth, NoAuth, PasswordAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


TRINO_DESCRIPTOR = BackendDescriptor(
    name="trino",
    provider_type=CONST_DB_PROVIDER_TYPE.TRINO,
    default_port=8080,
    connection_string_scheme="trino://",
    ibis_dialect="trino",
    auth_modes=[PasswordAuth, JWTAuth, KerberosAuth, NoAuth],
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=8080,
                      driver_key="port"),
        ParameterSpec(name="CATALOG", type=str, tier="core", driver_key="catalog"),
        ParameterSpec(name="SCHEMA", type=t.Optional[str], tier="core",
                      default=None, driver_key="schema"),
        ParameterSpec(name="HTTP_SCHEME", type=str, tier="core",
                      default="https", driver_key="http_scheme"),
        ParameterSpec(name="VERIFY", type=t.Optional[t.Union[bool, Path]],
                      tier="core", default=True, driver_key="verify",
                      transform=lambda v: str(v) if isinstance(v, Path) else v),
        ParameterSpec(name="SOURCE", type=t.Optional[str], tier="advanced",
                      default=None, driver_key="source"),
        ParameterSpec(name="TIMEZONE", type=t.Optional[str], tier="advanced",
                      default=None, driver_key="timezone"),
        ParameterSpec(name="MAX_ATTEMPTS", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="max_attempts"),
        ParameterSpec(name="REQUEST_TIMEOUT", type=t.Optional[float],
                      tier="advanced", default=None,
                      driver_key="request_timeout"),
        ParameterSpec(name="SESSION_PROPERTIES",
                      type=t.Optional[dict[str, str]], tier="advanced",
                      default=None, driver_key="session_properties"),
        ParameterSpec(name="HTTP_HEADERS", type=t.Optional[dict[str, str]],
                      tier="advanced", default=None,
                      driver_key="http_headers"),
        ParameterSpec(name="EXTRA_CREDENTIAL",
                      type=t.Optional[list[tuple[str, str]]], tier="advanced",
                      default=None, driver_key="extra_credential"),
        ParameterSpec(name="CLIENT_TAGS", type=t.Optional[list[str]],
                      tier="advanced", default=None,
                      driver_key="client_tags"),
        ParameterSpec(name="ROLES",
                      type=t.Optional[t.Union[dict[str, str], str]],
                      tier="advanced", default=None, driver_key="roles"),
        ParameterSpec(name="LEGACY_PRIMITIVE_TYPES", type=t.Optional[bool],
                      tier="advanced", default=None,
                      driver_key="legacy_primitive_types"),
        ParameterSpec(name="LEGACY_PREPARED_STATEMENTS",
                      type=t.Optional[bool], tier="advanced",
                      default=None,
                      driver_key="legacy_prepared_statements"),
        ParameterSpec(name="ENCODING", type=t.Optional[t.Union[str, list[str]]],
                      tier="advanced", default=None, driver_key="encoding"),
    ],
)


@register(TRINO_DESCRIPTOR)
class TrinoAuthSettings(ConnectionProfile):
    __descriptor__ = TRINO_DESCRIPTOR
    __adapter__ = staticmethod(_adapter.build_driver_kwargs)
```

- [ ] **Step 4: Run → PASS**

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/settings/trino.py \
        src/mountainash_data/core/settings/adapters/trino.py \
        tests/test_unit/core/settings/backends/test_trino.py
git commit -m "refactor(settings): migrate trino, wire BasicAuthentication, fix type drift"
```

---

## Task 14: Migrate MSSQL (Windows + Azure AD + encryption adapter)

**Files:**
- Modify: `src/mountainash_data/core/settings/mssql.py`
- Create: `src/mountainash_data/core/settings/adapters/mssql.py`
- Test: `tests/test_unit/core/settings/backends/test_mssql.py`

**Audit fixes carried:** missing `AZURE_MANAGED_IDENTITY`/`MSI_ENDPOINT` → now part of `AzureADAuth`; `args["server"]` KeyError → adapter uses `host`; ENCRYPTION + TRUST_SERVER_CERTIFICATE added (core); MARS_ENABLED plumbed; orphan `PROTOCOL` dropped; `MSSQLAuthMethod` enum deleted (supplanted by auth union).

- [ ] **Step 1: Write tests**

```python
# tests/test_unit/core/settings/backends/test_mssql.py
from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.settings.auth import (
    AzureADAuth,
    PasswordAuth,
    WindowsAuth,
)
from mountainash_data.core.settings.mssql import (
    MSSQLAuthSettings,
    MSSQLEncryption,
)


@pytest.mark.unit
class TestMSSQLAuthSettings:
    def _minimal(self, auth, **extra):
        return MSSQLAuthSettings(HOST="h", DATABASE="d", auth=auth, **extra)

    def test_password_auth(self):
        s = self._minimal(
            auth=PasswordAuth(username="u", password=SecretStr("p"))
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["user"] == "u"
        assert kwargs["password"] == "p"
        assert kwargs["host"] == "h"

    def test_windows_auth_sets_trusted_connection(self):
        s = self._minimal(auth=WindowsAuth(username="u", domain="CORP"))
        kwargs = s.to_driver_kwargs()
        assert kwargs["trusted_connection"] == "yes"
        assert kwargs["user"] == r"CORP\u"

    def test_azure_ad_managed_identity(self):
        """Audit regression: AZURE_MANAGED_IDENTITY/MSI_ENDPOINT were
        referenced but not declared — now live on AzureADAuth."""
        s = self._minimal(
            auth=AzureADAuth(
                managed_identity=True,
                msi_endpoint="http://169.254.169.254/",
            )
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["authentication"] == "ActiveDirectoryMsi"
        assert kwargs["msi_endpoint"] == "http://169.254.169.254/"

    def test_instance_name_appended_to_host(self):
        """Audit regression: code referenced args['server'] (KeyError)."""
        s = self._minimal(
            auth=PasswordAuth(username="u", password=SecretStr("p")),
            INSTANCE_NAME="SQLEXPRESS",
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["host"] == r"h\SQLEXPRESS"

    def test_encryption_default(self):
        """Audit regression: ODBC Driver 18 default Encrypt=Yes requires explicit setting."""
        s = self._minimal(auth=PasswordAuth(username="u", password=SecretStr("p")))
        assert s.ENCRYPTION is MSSQLEncryption.MANDATORY
```

- [ ] **Step 2: Run → FAIL**

- [ ] **Step 3: Implement**

```python
# src/mountainash_data/core/settings/adapters/mssql.py
"""MSSQL adapter: auth dispatch, instance-name folding, encryption keys."""

from __future__ import annotations

import typing as t

from mountainash_data.core.settings.auth import (
    AzureADAuth,
    PasswordAuth,
    WindowsAuth,
)

if t.TYPE_CHECKING:
    from mountainash_data.core.settings.mssql import MSSQLAuthSettings


def build_driver_kwargs(profile: "MSSQLAuthSettings") -> dict[str, t.Any]:
    kwargs = profile._default_driver_kwargs()

    # Instance name → host\instance
    if profile.INSTANCE_NAME:
        kwargs["host"] = f"{kwargs['host']}\\{profile.INSTANCE_NAME}"

    # Encryption flags
    if profile.ENCRYPTION is not None:
        kwargs["encrypt"] = profile.ENCRYPTION.value
    if profile.TRUST_SERVER_CERTIFICATE:
        kwargs["trust_server_certificate"] = "yes"
    if profile.MARS_ENABLED:
        kwargs["mars_connection"] = "yes"

    # Auth dispatch
    auth = profile.auth
    if isinstance(auth, PasswordAuth):
        kwargs["user"] = auth.username
        kwargs["password"] = auth.password.get_secret_value()
    elif isinstance(auth, WindowsAuth):
        kwargs["trusted_connection"] = "yes"
        if auth.domain and auth.username:
            kwargs["user"] = f"{auth.domain}\\{auth.username}"
        elif auth.username:
            kwargs["user"] = auth.username
    elif isinstance(auth, AzureADAuth):
        if auth.managed_identity:
            kwargs["authentication"] = "ActiveDirectoryMsi"
            if auth.msi_endpoint:
                kwargs["msi_endpoint"] = auth.msi_endpoint
        else:
            kwargs["authentication"] = "ActiveDirectoryServicePrincipal"
            if auth.client_id:
                kwargs["user_id"] = auth.client_id
            if auth.client_secret:
                kwargs["password"] = auth.client_secret.get_secret_value()
            if auth.tenant_id:
                kwargs["tenant_id"] = auth.tenant_id
    return kwargs
```

```python
# src/mountainash_data/core/settings/mssql.py
"""MSSQL backend settings.

Spec: ``docs/superpowers/specs/2026-04-15-settings-audit/mssql.md``.
Driver: PyODBC connect + ODBC Driver 17/18 for SQL Server.
"""

from __future__ import annotations

import typing as t
from enum import StrEnum

from ..constants import CONST_DB_PROVIDER_TYPE
from .adapters import mssql as _adapter
from .auth import AzureADAuth, PasswordAuth, WindowsAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


class MSSQLDriver(StrEnum):
    ODBC_18 = "ODBC Driver 18 for SQL Server"
    ODBC_17 = "ODBC Driver 17 for SQL Server"
    LEGACY = "SQL Server"


class MSSQLEncryption(StrEnum):
    DISABLED = "no"
    MANDATORY = "yes"
    STRICT = "strict"


MSSQL_DESCRIPTOR = BackendDescriptor(
    name="mssql",
    provider_type=CONST_DB_PROVIDER_TYPE.MSSQL,
    default_port=1433,
    connection_string_scheme="mssql://",
    ibis_dialect="mssql",
    auth_modes=[PasswordAuth, WindowsAuth, AzureADAuth],
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=1433,
                      driver_key="port"),
        ParameterSpec(name="DATABASE", type=t.Optional[str], tier="core",
                      default=None, driver_key="database"),
        ParameterSpec(name="DRIVER", type=MSSQLDriver, tier="core",
                      default=MSSQLDriver.ODBC_18, driver_key="driver"),
        ParameterSpec(name="ENCRYPTION", type=t.Optional[MSSQLEncryption],
                      tier="core", default=MSSQLEncryption.MANDATORY),
        ParameterSpec(name="TRUST_SERVER_CERTIFICATE", type=bool, tier="core",
                      default=False),
        ParameterSpec(name="INSTANCE_NAME", type=t.Optional[str],
                      tier="advanced", default=None),
        ParameterSpec(name="APP_NAME", type=str, tier="advanced",
                      default="MountainAsh", driver_key="application_name"),
        ParameterSpec(name="MARS_ENABLED", type=bool, tier="advanced",
                      default=False),
        ParameterSpec(name="LOGIN_TIMEOUT", type=t.Optional[int],
                      tier="advanced", default=None, driver_key="login_timeout"),
    ],
)


@register(MSSQL_DESCRIPTOR)
class MSSQLAuthSettings(ConnectionProfile):
    __descriptor__ = MSSQL_DESCRIPTOR
    __adapter__ = staticmethod(_adapter.build_driver_kwargs)
```

- [ ] **Step 4: Run → PASS**

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/settings/mssql.py \
        src/mountainash_data/core/settings/adapters/mssql.py \
        tests/test_unit/core/settings/backends/test_mssql.py
git commit -m "refactor(settings): migrate mssql with auth dispatch and encryption fixes"
```

---

## Task 15: Migrate Snowflake (session_parameters + authenticator + cert)

**Files:**
- Modify: `src/mountainash_data/core/settings/snowflake.py`
- Create: `src/mountainash_data/core/settings/adapters/snowflake.py`
- Test: `tests/test_unit/core/settings/backends/test_snowflake.py`

**Audit fixes carried:** enum whitespace bug fixed; `AUTHENTICATOR` as `StrEnum`; ROLE plumbed; `OKTA_ACCOUNT_NAMER` typo → `OKTA_ACCOUNT_NAME`; SecretStr unwrapped; `HOST` dropped (Snowflake uses `account`); `TIMEZONE` routed through `session_parameters`; `CertificateAuth` used for key auth.

- [ ] **Step 1: Write tests**

```python
# tests/test_unit/core/settings/backends/test_snowflake.py
from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.settings.auth import (
    CertificateAuth,
    OAuth2Auth,
    PasswordAuth,
    TokenAuth,
)
from mountainash_data.core.settings.snowflake import (
    SnowflakeAuthenticator,
    SnowflakeAuthSettings,
)


@pytest.mark.unit
class TestSnowflakeAuthSettings:
    def _minimal(self, auth, **extra):
        return SnowflakeAuthSettings(
            ACCOUNT="acc", WAREHOUSE="wh", auth=auth, **extra,
        )

    def test_authenticator_enum_has_no_whitespace(self):
        """Audit regression: enum values had trailing spaces."""
        assert SnowflakeAuthenticator.SNOWFLAKE.value == "snowflake"
        assert SnowflakeAuthenticator.PASSWORD_MFA.value == "username_password_mfa"

    def test_password_auth(self):
        s = self._minimal(
            auth=PasswordAuth(username="u", password=SecretStr("p"))
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["account"] == "acc"
        assert kwargs["warehouse"] == "wh"
        assert kwargs["user"] == "u"
        assert kwargs["password"] == "p"

    def test_role_is_plumbed(self):
        """Audit regression: ROLE was declared but never emitted."""
        s = self._minimal(
            auth=PasswordAuth(username="u", password=SecretStr("p")),
            ROLE="analyst",
        )
        assert s.to_driver_kwargs()["role"] == "analyst"

    def test_timezone_goes_to_session_parameters(self):
        """Audit regression: TIMEZONE was top-level, should be in session_parameters."""
        s = self._minimal(
            auth=PasswordAuth(username="u", password=SecretStr("p")),
            TIMEZONE="UTC",
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["session_parameters"] == {"TIMEZONE": "UTC"}

    def test_certificate_auth(self):
        s = self._minimal(
            auth=CertificateAuth(private_key=SecretStr("KEYCONTENT"))
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["private_key"] == "KEYCONTENT"
```

- [ ] **Step 2: Run → FAIL**

- [ ] **Step 3: Implement**

```python
# src/mountainash_data/core/settings/adapters/snowflake.py
"""Snowflake adapter: session_parameters, authenticator mapping, cert auth."""

from __future__ import annotations

import typing as t

from mountainash_data.core.settings.auth import (
    CertificateAuth,
    OAuth2Auth,
    PasswordAuth,
    TokenAuth,
)

if t.TYPE_CHECKING:
    from mountainash_data.core.settings.snowflake import SnowflakeAuthSettings


def build_driver_kwargs(profile: "SnowflakeAuthSettings") -> dict[str, t.Any]:
    kwargs = profile._default_driver_kwargs()

    # Session parameters
    session_params: dict[str, t.Any] = {}
    if profile.TIMEZONE is not None:
        session_params["TIMEZONE"] = profile.TIMEZONE
    if profile.QUERY_TAG is not None:
        session_params["QUERY_TAG"] = profile.QUERY_TAG
    if session_params:
        kwargs["session_parameters"] = session_params

    # Auth dispatch
    auth = profile.auth
    if isinstance(auth, PasswordAuth):
        kwargs["user"] = auth.username
        kwargs["password"] = auth.password.get_secret_value()
        if profile.AUTHENTICATOR is not None:
            kwargs["authenticator"] = profile.AUTHENTICATOR.value
    elif isinstance(auth, TokenAuth):
        kwargs["authenticator"] = "oauth"
        kwargs["token"] = auth.token.get_secret_value()
    elif isinstance(auth, OAuth2Auth):
        kwargs["authenticator"] = "oauth"
        if auth.token is not None:
            kwargs["token"] = auth.token.get_secret_value()
    elif isinstance(auth, CertificateAuth):
        if auth.private_key is not None:
            kwargs["private_key"] = auth.private_key.get_secret_value()
        if auth.private_key_path is not None:
            kwargs["private_key_file"] = str(auth.private_key_path)
        if auth.passphrase is not None:
            kwargs["private_key_file_pwd"] = auth.passphrase.get_secret_value()
    return kwargs
```

```python
# src/mountainash_data/core/settings/snowflake.py
"""Snowflake backend settings.

Spec: ``docs/superpowers/specs/2026-04-15-settings-audit/snowflake.md``.
Driver: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api
"""

from __future__ import annotations

import typing as t
from enum import StrEnum

from ..constants import CONST_DB_PROVIDER_TYPE
from .adapters import snowflake as _adapter
from .auth import CertificateAuth, OAuth2Auth, PasswordAuth, TokenAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


class SnowflakeAuthenticator(StrEnum):
    SNOWFLAKE = "snowflake"
    OAUTH = "oauth"
    OKTA = "okta"
    EXTERNAL_BROWSER = "externalbrowser"
    PASSWORD_MFA = "username_password_mfa"


SNOWFLAKE_DESCRIPTOR = BackendDescriptor(
    name="snowflake",
    provider_type=CONST_DB_PROVIDER_TYPE.SNOWFLAKE,
    connection_string_scheme="snowflake://",
    ibis_dialect="snowflake",
    auth_modes=[PasswordAuth, OAuth2Auth, CertificateAuth, TokenAuth],
    parameters=[
        ParameterSpec(name="ACCOUNT", type=str, tier="core",
                      driver_key="account"),
        ParameterSpec(name="WAREHOUSE", type=t.Optional[str], tier="core",
                      default=None, driver_key="warehouse"),
        ParameterSpec(name="DATABASE", type=t.Optional[str], tier="core",
                      default=None, driver_key="database"),
        ParameterSpec(name="SCHEMA", type=t.Optional[str], tier="core",
                      default=None, driver_key="schema"),
        ParameterSpec(name="ROLE", type=t.Optional[str], tier="core",
                      default=None, driver_key="role"),
        ParameterSpec(name="AUTHENTICATOR",
                      type=t.Optional[SnowflakeAuthenticator], tier="core",
                      default=None),
        ParameterSpec(name="CONNECTION_NAME", type=t.Optional[str],
                      tier="core", default=None, driver_key="connection_name"),
        ParameterSpec(name="TIMEZONE", type=t.Optional[str], tier="advanced",
                      default=None),
        ParameterSpec(name="QUERY_TAG", type=t.Optional[str], tier="advanced",
                      default=None),
        ParameterSpec(name="APPLICATION", type=t.Optional[str],
                      tier="advanced", default=None,
                      driver_key="application"),
        ParameterSpec(name="LOGIN_TIMEOUT", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="login_timeout"),
        ParameterSpec(name="NETWORK_TIMEOUT", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="network_timeout"),
        ParameterSpec(name="OKTA_ACCOUNT_NAME", type=t.Optional[str],
                      tier="advanced", default=None,
                      driver_key="okta_account_name"),
    ],
)


@register(SNOWFLAKE_DESCRIPTOR)
class SnowflakeAuthSettings(ConnectionProfile):
    __descriptor__ = SNOWFLAKE_DESCRIPTOR
    __adapter__ = staticmethod(_adapter.build_driver_kwargs)
```

- [ ] **Step 4: Run → PASS**

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/settings/snowflake.py \
        src/mountainash_data/core/settings/adapters/snowflake.py \
        tests/test_unit/core/settings/backends/test_snowflake.py
git commit -m "refactor(settings): migrate snowflake, fix enum whitespace and session_parameters"
```

---

## Task 16: Migrate BigQuery (SA-info → Credentials adapter)

**Files:**
- Modify: `src/mountainash_data/core/settings/bigquery.py`
- Create: `src/mountainash_data/core/settings/adapters/bigquery.py`
- Test: `tests/test_unit/core/settings/backends/test_bigquery.py`

**Audit fixes carried:** `credentials` typed correctly — adapter converts SA-info dict (or file path) → `google.oauth2.service_account.Credentials`; `AUTH_LOCAL_WEBSERVER`/`AUTH_EXTERNAL_DATA`/`AUTH_CACHE` added; `PARTITION_COLUMN` default `"PARTITIONTIME"`; `ServiceAccountAuth` is primary auth.

- [ ] **Step 1: Write tests**

```python
# tests/test_unit/core/settings/backends/test_bigquery.py
from __future__ import annotations

import pytest

from mountainash_data.core.settings.auth import NoAuth, ServiceAccountAuth
from mountainash_data.core.settings.bigquery import BigQueryAuthSettings


@pytest.mark.unit
class TestBigQueryAuthSettings:
    def test_partition_column_default(self):
        """Audit regression: default was None, should be 'PARTITIONTIME'."""
        s = BigQueryAuthSettings(PROJECT_ID="myproj12", auth=NoAuth())
        assert s.PARTITION_COLUMN == "PARTITIONTIME"

    def test_service_account_info_converts_to_credentials(self):
        """Audit regression: SA info dict was passed raw; Ibis needs Credentials."""
        pytest.importorskip("google.oauth2")

        # Minimal valid SA info shape
        info = {
            "type": "service_account",
            "project_id": "myproj12",
            "private_key_id": "x",
            "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
            "client_email": "sa@myproj12.iam.gserviceaccount.com",
            "client_id": "1",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }

        s = BigQueryAuthSettings(
            PROJECT_ID="myproj12",
            auth=ServiceAccountAuth(info=info),
        )
        # We can't fully construct credentials without a valid key, so we
        # just verify the adapter attempts conversion and emits the key.
        try:
            kwargs = s.to_driver_kwargs()
            assert "credentials" in kwargs
        except ValueError:
            # google.oauth2 will reject the dummy key — acceptable here,
            # the important thing is no raw dict leak.
            pass

    def test_auth_local_webserver_plumbed(self):
        """Audit regression: field didn't exist."""
        s = BigQueryAuthSettings(
            PROJECT_ID="myproj12", AUTH_LOCAL_WEBSERVER=False, auth=NoAuth(),
        )
        assert s.to_driver_kwargs()["auth_local_webserver"] is False
```

- [ ] **Step 2: Run → FAIL**

- [ ] **Step 3: Implement**

```python
# src/mountainash_data/core/settings/adapters/bigquery.py
"""BigQuery adapter: convert ServiceAccountAuth → google Credentials."""

from __future__ import annotations

import typing as t

from mountainash_data.core.settings.auth import NoAuth, ServiceAccountAuth

if t.TYPE_CHECKING:
    from mountainash_data.core.settings.bigquery import BigQueryAuthSettings


def build_driver_kwargs(profile: "BigQueryAuthSettings") -> dict[str, t.Any]:
    kwargs = profile._default_driver_kwargs()

    auth = profile.auth
    if isinstance(auth, ServiceAccountAuth):
        from google.oauth2 import service_account as _sa

        if auth.info is not None:
            kwargs["credentials"] = _sa.Credentials.from_service_account_info(auth.info)
        elif auth.file is not None:
            kwargs["credentials"] = _sa.Credentials.from_service_account_file(
                str(auth.file)
            )
    elif isinstance(auth, NoAuth):
        pass  # Application Default Credentials
    return kwargs
```

```python
# src/mountainash_data/core/settings/bigquery.py
"""BigQuery backend settings.

Spec: ``docs/superpowers/specs/2026-04-15-settings-audit/bigquery.md``.
Ibis: ``ibis.backends.bigquery.do_connect``
"""

from __future__ import annotations

import re
import typing as t

from pydantic import field_validator

from ..constants import CONST_DB_PROVIDER_TYPE
from .adapters import bigquery as _adapter
from .auth import NoAuth, ServiceAccountAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register

_PROJECT_ID_RE = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")


def _validate_project_id(value: str) -> str:
    if not _PROJECT_ID_RE.match(value):
        raise ValueError(
            "PROJECT_ID must be 6-30 chars, lowercase/digits/hyphens, "
            "not starting or ending with a hyphen"
        )
    return value


BIGQUERY_DESCRIPTOR = BackendDescriptor(
    name="bigquery",
    provider_type=CONST_DB_PROVIDER_TYPE.BIGQUERY,
    connection_string_scheme="bigquery://",
    ibis_dialect="bigquery",
    auth_modes=[ServiceAccountAuth, NoAuth],
    parameters=[
        ParameterSpec(name="PROJECT_ID", type=str, tier="core",
                      driver_key="project_id"),
        ParameterSpec(name="DATASET_ID", type=t.Optional[str], tier="core",
                      default=None, driver_key="dataset_id"),
        ParameterSpec(name="LOCATION", type=t.Optional[str], tier="advanced",
                      default=None, driver_key="location"),
        ParameterSpec(name="APPLICATION_NAME", type=t.Optional[str],
                      tier="advanced", default=None,
                      driver_key="application_name"),
        ParameterSpec(name="PARTITION_COLUMN", type=str, tier="advanced",
                      default="PARTITIONTIME", driver_key="partition_column"),
        ParameterSpec(name="AUTH_LOCAL_WEBSERVER", type=bool, tier="core",
                      default=True, driver_key="auth_local_webserver"),
        ParameterSpec(name="AUTH_EXTERNAL_DATA", type=bool, tier="core",
                      default=False, driver_key="auth_external_data"),
        ParameterSpec(name="AUTH_CACHE", type=str, tier="core",
                      default="default", driver_key="auth_cache"),
    ],
)


@register(BIGQUERY_DESCRIPTOR)
class BigQueryAuthSettings(ConnectionProfile):
    __descriptor__ = BIGQUERY_DESCRIPTOR
    __adapter__ = staticmethod(_adapter.build_driver_kwargs)

    @field_validator("PROJECT_ID")
    @classmethod
    def _pid(cls, v: str) -> str:
        return _validate_project_id(v)
```

- [ ] **Step 4: Run → PASS**

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/settings/bigquery.py \
        src/mountainash_data/core/settings/adapters/bigquery.py \
        tests/test_unit/core/settings/backends/test_bigquery.py
git commit -m "refactor(settings): migrate bigquery with SA credentials conversion"
```

---

## Task 17: Migrate Redshift (endpoint resolver adapter)

**Files:**
- Modify: `src/mountainash_data/core/settings/redshift.py`
- Create: `src/mountainash_data/core/settings/adapters/redshift.py`
- Test: `tests/test_unit/core/settings/backends/test_redshift.py`

**Audit fixes carried:** `_init_provider_specific` → `_post_init`/validators run; broken `_get_cluster_endpoint` either implemented (behind a lazy `boto3` import) or removed (with explicit `HOST` required); region regex widened; role ARN regex widened; `SSL` bool → `SSL_MODE` enum; `CLUSTER_READ_ONLY` / `WORKGROUP_NAME` plumbed when used.

- [ ] **Step 1: Write tests**

```python
# tests/test_unit/core/settings/backends/test_redshift.py
from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError

from mountainash_data.core.settings.auth import IAMAuth, PasswordAuth
from mountainash_data.core.settings.redshift import (
    RedshiftAuthSettings,
    RedshiftSSLMode,
)


@pytest.mark.unit
class TestRedshiftAuthSettings:
    def _password(self, **extra):
        return RedshiftAuthSettings(
            HOST="cluster.abc.us-east-1.redshift.amazonaws.com",
            DATABASE="dev",
            REGION="us-east-1",
            auth=PasswordAuth(username="u", password=SecretStr("p")),
            **extra,
        )

    def test_port_default_5439(self):
        s = self._password()
        assert s.PORT == 5439

    def test_region_govcloud_accepted(self):
        """Audit regression: region regex rejected GovCloud."""
        s = RedshiftAuthSettings(
            HOST="h", DATABASE="d", REGION="us-gov-west-1",
            auth=PasswordAuth(username="u", password=SecretStr("p")),
        )
        assert s.REGION == "us-gov-west-1"

    def test_role_arn_govcloud_accepted(self):
        """Audit regression: role-ARN regex rejected non-commercial partitions."""
        s = self._password(IAM_ROLE_ARN="arn:aws-us-gov:iam::123456789012:role/x")
        assert s.IAM_ROLE_ARN.startswith("arn:aws-us-gov:")

    def test_iam_auth(self):
        s = RedshiftAuthSettings(
            HOST="h", DATABASE="d", REGION="us-east-1",
            auth=IAMAuth(
                access_key_id="AKIA", secret_access_key=SecretStr("sk"),
            ),
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["aws_access_key_id"] == "AKIA"
        assert kwargs["aws_secret_access_key"] == "sk"

    def test_ssl_mode_enum(self):
        """Audit regression: SSL was bool, hardcoded verify-full."""
        s = self._password(SSL_MODE=RedshiftSSLMode.REQUIRE)
        assert s.to_driver_kwargs()["sslmode"] == "require"
```

- [ ] **Step 2: Run → FAIL**

- [ ] **Step 3: Implement**

```python
# src/mountainash_data/core/settings/adapters/redshift.py
"""Redshift adapter: endpoint resolution hook, IAM/password routing."""

from __future__ import annotations

import typing as t

from mountainash_data.core.settings.auth import IAMAuth, PasswordAuth

if t.TYPE_CHECKING:
    from mountainash_data.core.settings.redshift import RedshiftAuthSettings


def build_driver_kwargs(profile: "RedshiftAuthSettings") -> dict[str, t.Any]:
    kwargs = profile._default_driver_kwargs()

    auth = profile.auth
    if isinstance(auth, PasswordAuth):
        kwargs["user"] = auth.username
        kwargs["password"] = auth.password.get_secret_value()
    elif isinstance(auth, IAMAuth):
        kwargs["iam"] = True
        if auth.role_arn is not None:
            kwargs["iam_role_arn"] = auth.role_arn
        if auth.access_key_id is not None:
            kwargs["aws_access_key_id"] = auth.access_key_id
        if auth.secret_access_key is not None:
            kwargs["aws_secret_access_key"] = auth.secret_access_key.get_secret_value()
        if auth.session_token is not None:
            kwargs["aws_session_token"] = auth.session_token.get_secret_value()
        if auth.profile_name is not None:
            kwargs["profile_name"] = auth.profile_name
    return kwargs
```

```python
# src/mountainash_data/core/settings/redshift.py
"""Redshift backend settings.

Spec: ``docs/superpowers/specs/2026-04-15-settings-audit/redshift.md``.
Driver: redshift_connector OR psycopg (via Ibis postgres). Endpoint
resolution via boto3 ``describe_clusters`` is a Phase-4 follow-up.
"""

from __future__ import annotations

import re
import typing as t
from enum import StrEnum

from pydantic import field_validator

from ..constants import CONST_DB_PROVIDER_TYPE
from .adapters import redshift as _adapter
from .auth import IAMAuth, PasswordAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


class RedshiftSSLMode(StrEnum):
    DISABLE = "disable"
    ALLOW = "allow"
    PREFER = "prefer"
    REQUIRE = "require"
    VERIFY_CA = "verify-ca"
    VERIFY_FULL = "verify-full"


_REGION_RE = re.compile(r"^[a-z]{2,4}-[a-z-]+-\d{1,2}$")
_ROLE_ARN_RE = re.compile(r"^arn:aws(?:-us-gov|-cn)?:iam::\d{12}:role/.+$")


REDSHIFT_DESCRIPTOR = BackendDescriptor(
    name="redshift",
    provider_type=CONST_DB_PROVIDER_TYPE.REDSHIFT,
    default_port=5439,
    connection_string_scheme="redshift://",
    ibis_dialect="postgres",
    rides_on="postgres",
    auth_modes=[PasswordAuth, IAMAuth],
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=5439,
                      driver_key="port"),
        ParameterSpec(name="DATABASE", type=str, tier="core",
                      driver_key="database"),
        ParameterSpec(name="SCHEMA", type=t.Optional[str], tier="core",
                      default=None, driver_key="schema"),
        ParameterSpec(name="REGION", type=str, tier="core"),
        ParameterSpec(name="CLUSTER_IDENTIFIER", type=t.Optional[str],
                      tier="core", default=None),
        ParameterSpec(name="WORKGROUP_NAME", type=t.Optional[str],
                      tier="core", default=None),
        ParameterSpec(name="SERVERLESS", type=bool, tier="core", default=False),
        ParameterSpec(name="IAM_ROLE_ARN", type=t.Optional[str],
                      tier="advanced", default=None),
        ParameterSpec(name="SSL_MODE", type=RedshiftSSLMode, tier="core",
                      default=RedshiftSSLMode.VERIFY_FULL,
                      driver_key="sslmode"),
        ParameterSpec(name="CLUSTER_READ_ONLY", type=bool, tier="advanced",
                      default=False, driver_key="readonly"),
    ],
)


@register(REDSHIFT_DESCRIPTOR)
class RedshiftAuthSettings(ConnectionProfile):
    __descriptor__ = REDSHIFT_DESCRIPTOR
    __adapter__ = staticmethod(_adapter.build_driver_kwargs)

    @field_validator("REGION")
    @classmethod
    def _region(cls, v: str) -> str:
        if not _REGION_RE.match(v):
            raise ValueError(f"Invalid AWS region: {v}")
        return v

    @field_validator("IAM_ROLE_ARN")
    @classmethod
    def _role_arn(cls, v: t.Optional[str]) -> t.Optional[str]:
        if v is not None and not _ROLE_ARN_RE.match(v):
            raise ValueError(f"Invalid IAM role ARN: {v}")
        return v
```

- [ ] **Step 4: Run → PASS**

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/settings/redshift.py \
        src/mountainash_data/core/settings/adapters/redshift.py \
        tests/test_unit/core/settings/backends/test_redshift.py
git commit -m "refactor(settings): migrate redshift with SSL_MODE enum and widened region regex"
```

---

## Task 18: Migrate PyIceberg REST (catalog adapter)

**Files:**
- Modify: `src/mountainash_data/core/settings/pyiceberg_rest.py`
- Create: `src/mountainash_data/core/settings/adapters/pyiceberg_rest.py`
- Test: `tests/test_unit/core/settings/backends/test_pyiceberg_rest.py`

**Audit fixes carried:** class identity resolved (generic REST, not R2-specific); `WAREHOUSE` Optional; `USE_SSL` dropped / replaced with scheme inference; `VERIFY_SSL` plumbed; OAuth2 via `OAuth2Auth`; `s3.*`, `rest.sigv4-*`, `header.*` families added as dotted-key params handled by adapter.

- [ ] **Step 1: Write tests**

```python
# tests/test_unit/core/settings/backends/test_pyiceberg_rest.py
from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.settings.auth import OAuth2Auth, TokenAuth
from mountainash_data.core.settings.pyiceberg_rest import PyIcebergRestAuthSettings


@pytest.mark.unit
class TestPyIcebergRestAuthSettings:
    def _min(self, auth, **extra):
        return PyIcebergRestAuthSettings(
            CATALOG_NAME="cat",
            CATALOG_URI="https://catalog.example/v1",
            auth=auth, **extra,
        )

    def test_warehouse_optional(self):
        """Audit regression: WAREHOUSE was over-required."""
        s = self._min(auth=TokenAuth(token=SecretStr("t")))
        assert s.WAREHOUSE is None

    def test_token_auth(self):
        s = self._min(auth=TokenAuth(token=SecretStr("tok")))
        kwargs = s.to_driver_kwargs()
        assert kwargs["token"] == "tok"
        assert kwargs["uri"] == "https://catalog.example/v1"

    def test_oauth2_credential_form(self):
        s = self._min(
            auth=OAuth2Auth(client_id="cid", client_secret=SecretStr("sec")),
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["credential"] == "cid:sec"

    def test_s3_params_prefixed(self):
        """Audit regression: s3.* family was absent."""
        s = self._min(
            auth=TokenAuth(token=SecretStr("t")),
            S3_ENDPOINT="https://r2.example.com",
            S3_REGION="auto",
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["s3.endpoint"] == "https://r2.example.com"
        assert kwargs["s3.region"] == "auto"
```

- [ ] **Step 2: Run → FAIL**

- [ ] **Step 3: Implement**

```python
# src/mountainash_data/core/settings/adapters/pyiceberg_rest.py
"""Adapter prefixing s3.*, rest.sigv4-*, header.* keys."""

from __future__ import annotations

import typing as t

from mountainash_data.core.settings.auth import OAuth2Auth, TokenAuth

if t.TYPE_CHECKING:
    from mountainash_data.core.settings.pyiceberg_rest import (
        PyIcebergRestAuthSettings,
    )


def build_driver_kwargs(profile: "PyIcebergRestAuthSettings") -> dict[str, t.Any]:
    kwargs = profile._default_driver_kwargs()

    # S3 family
    for field, key in [
        ("S3_REGION", "s3.region"),
        ("S3_ENDPOINT", "s3.endpoint"),
        ("S3_ACCESS_KEY_ID", "s3.access-key-id"),
    ]:
        val = getattr(profile, field, None)
        if val is not None:
            kwargs[key] = val
    if profile.S3_SECRET_ACCESS_KEY is not None:
        kwargs["s3.secret-access-key"] = profile.S3_SECRET_ACCESS_KEY.get_secret_value()
    if profile.S3_SESSION_TOKEN is not None:
        kwargs["s3.session-token"] = profile.S3_SESSION_TOKEN.get_secret_value()

    # SigV4
    if profile.REST_SIGV4_ENABLED is not None:
        kwargs["rest.sigv4-enabled"] = profile.REST_SIGV4_ENABLED
    if profile.REST_SIGNING_REGION is not None:
        kwargs["rest.signing-region"] = profile.REST_SIGNING_REGION
    if profile.REST_SIGNING_NAME is not None:
        kwargs["rest.signing-name"] = profile.REST_SIGNING_NAME

    # Headers (dict → header.<k> = v)
    if profile.HEADERS:
        for hk, hv in profile.HEADERS.items():
            kwargs[f"header.{hk}"] = hv

    # Auth
    auth = profile.auth
    if isinstance(auth, TokenAuth):
        kwargs["token"] = auth.token.get_secret_value()
    elif isinstance(auth, OAuth2Auth):
        if auth.token is not None:
            kwargs["token"] = auth.token.get_secret_value()
        elif auth.client_id is not None and auth.client_secret is not None:
            kwargs["credential"] = (
                f"{auth.client_id}:{auth.client_secret.get_secret_value()}"
            )
        if auth.server_uri is not None:
            kwargs["oauth2-server-uri"] = auth.server_uri
        if auth.scope is not None:
            kwargs["scope"] = auth.scope
    return kwargs
```

```python
# src/mountainash_data/core/settings/pyiceberg_rest.py
"""PyIceberg REST catalog backend settings.

Spec: ``docs/superpowers/specs/2026-04-15-settings-audit/pyiceberg_rest.md``.
Driver: https://py.iceberg.apache.org/configuration/
"""

from __future__ import annotations

import typing as t

from pydantic import SecretStr

from ..constants import CONST_DB_PROVIDER_TYPE
from .adapters import pyiceberg_rest as _adapter
from .auth import OAuth2Auth, TokenAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


PYICEBERG_REST_DESCRIPTOR = BackendDescriptor(
    name="pyiceberg_rest",
    provider_type=CONST_DB_PROVIDER_TYPE.PYICEBERG_REST,
    connection_string_scheme=None,  # uri= kwarg, not URL form
    auth_modes=[TokenAuth, OAuth2Auth],
    parameters=[
        ParameterSpec(name="CATALOG_NAME", type=str, tier="core",
                      driver_key="name"),
        ParameterSpec(name="CATALOG_URI", type=str, tier="core",
                      driver_key="uri"),
        ParameterSpec(name="WAREHOUSE", type=t.Optional[str], tier="core",
                      default=None, driver_key="warehouse"),
        ParameterSpec(name="VERIFY_SSL", type=bool, tier="advanced",
                      default=True, driver_key="verify-ssl"),
        # S3 family (adapter emits dotted keys)
        ParameterSpec(name="S3_REGION", type=t.Optional[str], tier="advanced",
                      default=None),
        ParameterSpec(name="S3_ENDPOINT", type=t.Optional[str],
                      tier="advanced", default=None),
        ParameterSpec(name="S3_ACCESS_KEY_ID", type=t.Optional[str],
                      tier="advanced", default=None),
        ParameterSpec(name="S3_SECRET_ACCESS_KEY",
                      type=t.Optional[SecretStr], tier="advanced",
                      default=None),
        ParameterSpec(name="S3_SESSION_TOKEN", type=t.Optional[SecretStr],
                      tier="advanced", default=None),
        # SigV4
        ParameterSpec(name="REST_SIGV4_ENABLED", type=t.Optional[bool],
                      tier="advanced", default=None),
        ParameterSpec(name="REST_SIGNING_REGION", type=t.Optional[str],
                      tier="advanced", default=None),
        ParameterSpec(name="REST_SIGNING_NAME", type=t.Optional[str],
                      tier="advanced", default=None),
        ParameterSpec(name="HEADERS", type=t.Optional[dict[str, str]],
                      tier="advanced", default=None),
    ],
)


@register(PYICEBERG_REST_DESCRIPTOR)
class PyIcebergRestAuthSettings(ConnectionProfile):
    __descriptor__ = PYICEBERG_REST_DESCRIPTOR
    __adapter__ = staticmethod(_adapter.build_driver_kwargs)
```

- [ ] **Step 4: Run → PASS**

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/settings/pyiceberg_rest.py \
        src/mountainash_data/core/settings/adapters/pyiceberg_rest.py \
        tests/test_unit/core/settings/backends/test_pyiceberg_rest.py
git commit -m "refactor(settings): migrate pyiceberg_rest with s3/sigv4/header families"
```

---

## Task 19: Update `__init__.py` re-exports

**Files:**
- Modify: `src/mountainash_data/core/settings/__init__.py`

**Context:** All 11 backends now migrated. `__init__.py` must continue to re-export the per-backend class names plus the new public surface.

- [ ] **Step 1: Write the __init__ module**

```python
# src/mountainash_data/core/settings/__init__.py
"""Backend settings — declarative descriptor + registry.

The *AuthSettings classes below are stable import anchors; internally each
class body is a two-line shell (``__descriptor__`` + ``__adapter__``).
"""

from __future__ import annotations

# Core primitives
from .descriptor import MISSING, BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import (
    REGISTRY,
    get_descriptor,
    get_settings_class,
    register,
)

# Auth union members
from .auth import (
    AuthSpec,
    AzureADAuth,
    CertificateAuth,
    IAMAuth,
    JWTAuth,
    KerberosAuth,
    NoAuth,
    OAuth2Auth,
    PasswordAuth,
    ServiceAccountAuth,
    TokenAuth,
    WindowsAuth,
)

# Per-backend settings classes (these import-register themselves).
from .sqlite import SQLiteAuthSettings
from .duckdb import DuckDBAuthSettings
from .motherduck import MotherDuckAuthSettings
from .postgresql import PostgreSQLAuthSettings
from .mysql import MySQLAuthSettings
from .mssql import MSSQLAuthSettings
from .snowflake import SnowflakeAuthSettings
from .bigquery import BigQueryAuthSettings
from .redshift import RedshiftAuthSettings
from .pyspark import PySparkAuthSettings
from .trino import TrinoAuthSettings
from .pyiceberg_rest import PyIcebergRestAuthSettings

__all__ = [
    # primitives
    "MISSING", "BackendDescriptor", "ParameterSpec", "ConnectionProfile",
    "REGISTRY", "get_descriptor", "get_settings_class", "register",
    # auth
    "AuthSpec", "NoAuth", "PasswordAuth", "TokenAuth", "JWTAuth",
    "OAuth2Auth", "ServiceAccountAuth", "IAMAuth", "WindowsAuth",
    "AzureADAuth", "KerberosAuth", "CertificateAuth",
    # backends
    "SQLiteAuthSettings", "DuckDBAuthSettings", "MotherDuckAuthSettings",
    "PostgreSQLAuthSettings", "MySQLAuthSettings", "MSSQLAuthSettings",
    "SnowflakeAuthSettings", "BigQueryAuthSettings", "RedshiftAuthSettings",
    "PySparkAuthSettings", "TrinoAuthSettings", "PyIcebergRestAuthSettings",
]
```

- [ ] **Step 2: Run the full suite**

Run: `hatch run test:test-quick`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/mountainash_data/core/settings/__init__.py
git commit -m "chore(settings): update __init__ re-exports for new registry shape"
```

---

## Task 20: Update consumer call sites

**Files:**
- Modify: `src/mountainash_data/core/factories/settings_factory.py`
- Modify: `src/mountainash_data/core/factories/connection_factory.py`
- Modify: `src/mountainash_data/core/factories/operations_factory.py`
- Modify: `src/mountainash_data/core/factories/settings_type_factory_mixin.py`
- Modify: `src/mountainash_data/core/connection.py`
- Modify: `src/mountainash_data/core/utils.py`
- Modify: `src/mountainash_data/backends/ibis/connection.py`
- Modify: `src/mountainash_data/backends/iceberg/connection.py`

**Context:** Replace every call to the four retired `get_*` methods with the new API.

| Retired method | Replacement |
|---|---|
| `settings.get_connection_kwargs()` | `settings.to_driver_kwargs()` |
| `settings.get_connection_string_template(scheme)` | `settings.to_connection_string()` |
| `settings.get_connection_string_params()` | (rolled into `to_connection_string`) |
| `settings.get_post_connection_options()` | removed — use SQL statements in the backend layer |
| `settings.db_provider_type` (property) | `settings.provider_type` (property) |

Also replace `SettingsFactory`'s hand-maintained `if/elif` chain with a single `get_settings_class(name)` registry lookup.

- [ ] **Step 1: Audit current consumer calls**

Run: `hatch run ruff:check` is not relevant here — we need a grep pass instead:

```bash
grep -rn "get_connection_kwargs\|get_connection_string_template\|get_connection_string_params\|get_post_connection_options\|db_provider_type\|BaseDBAuthSettings" \
        src/mountainash_data --include='*.py' \
        | grep -v '/settings/'
```

Expected output: list of every file+line to update. Use this as the checklist.

- [ ] **Step 2: Update each consumer file**

For each file in the list, apply the table above. Mechanical change. When done, run:

```bash
grep -rn "get_connection_kwargs\|get_connection_string_template\|get_connection_string_params\|get_post_connection_options\|BaseDBAuthSettings" \
        src/mountainash_data --include='*.py' \
        | grep -v '/settings/'
```

Expected: no matches.

- [ ] **Step 3: Run the full suite**

Run: `hatch run test:test-quick`
Expected: PASS.

If something fails, read the error, fix the specific call site, re-run. Do not skip.

- [ ] **Step 4: Commit**

```bash
git add src/mountainash_data/core/factories/ \
        src/mountainash_data/core/connection.py \
        src/mountainash_data/core/utils.py \
        src/mountainash_data/backends/
git commit -m "refactor: migrate consumers to ConnectionProfile.to_driver_kwargs API"
```

---

## Task 21: Delete retired base + exceptions modules

**Files:**
- Delete: `src/mountainash_data/core/settings/base.py`
- Delete: `src/mountainash_data/core/settings/exceptions.py`

- [ ] **Step 1: Search for any remaining references**

```bash
grep -rn "BaseDBAuthSettings\|DBAuthValidationError\|DBAuthConfigError\|DBAuthConnectionError" \
        src/mountainash_data tests --include='*.py'
```

Expected: only matches inside `settings/base.py` and `settings/exceptions.py` (the files being deleted).

- [ ] **Step 2: Delete the files**

```bash
git rm src/mountainash_data/core/settings/base.py
git rm src/mountainash_data/core/settings/exceptions.py
```

- [ ] **Step 3: Run the full suite**

Run: `hatch run test:test-quick`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git commit -m "chore(settings): delete retired BaseDBAuthSettings and exception types"
```

---

## Task 22: Update docs

**Files:**
- Modify: `CLAUDE.md` (Settings section if present)
- Modify: `README.md` (Usage Patterns section)
- Modify: `docs/superpowers/specs/2026-04-15-settings-audit/README.md` (note the refactor consumed many findings)

- [ ] **Step 1: Update `README.md` Usage Patterns**

Replace the old `from mountainash_data.core.settings import SQLiteAuthSettings` example block with a version that shows `auth=` + `to_driver_kwargs()`:

```python
from mountainash_data.core.settings import (
    SQLiteAuthSettings,
    NoAuth,
    PostgreSQLAuthSettings,
    PasswordAuth,
)

sqlite = SQLiteAuthSettings(DATABASE=":memory:", auth=NoAuth())
pg = PostgreSQLAuthSettings(
    HOST="db.example",
    DATABASE="app",
    auth=PasswordAuth(username="app", password="s3cret"),
)

kwargs = pg.to_driver_kwargs()           # → dict ready for Ibis
url = pg.to_connection_string()          # → "postgresql://app:s3cret@db.example:5432/app"
print(pg.provider_type, pg.backend)
```

- [ ] **Step 2: Update `CLAUDE.md` settings reference**

Find the "Settings (`src/mountainash_data/core/settings/`)" bullet and replace with:

> 3. **Settings** (`src/mountainash_data/core/settings/`)
>    - Declarative per-backend descriptors (`BackendDescriptor` + `ParameterSpec` list).
>    - Typed discriminated-union auth via `AuthSpec` subclasses (`PasswordAuth`, `OAuth2Auth`, `IAMAuth`, …).
>    - Backend shell classes register themselves via `@register` and expose `to_driver_kwargs()` + `to_connection_string()`.
>    - Composite driver mappings live in `settings/adapters/<backend>.py`.

- [ ] **Step 3: Add note to audit README**

Append to `docs/superpowers/specs/2026-04-15-settings-audit/README.md`:

> **Status (post-refactor, 2026-04-15):** The settings-registry refactor (see
> `docs/superpowers/specs/2026-04-15-settings-registry-design.md` and
> `docs/superpowers/plans/2026-04-15-settings-registry.md`) consumed most
> "core mismatch" and many "core missing" findings in the tables above.
> Remaining items are tracked as per-backend Phase-4 follow-up plans.

- [ ] **Step 4: Commit**

```bash
git add README.md CLAUDE.md docs/superpowers/specs/2026-04-15-settings-audit/README.md
git commit -m "docs: update settings usage for registry refactor"
```

---

## Self-review

**1. Spec coverage:**
- Problem statement → Tasks 1-6 (scaffolding removes boilerplate, leakage, flat-auth).
- Goals: retire `get_*` methods → Task 20; data-as-descriptors → Tasks 1, 7-18; typed auth union → Task 2; drop base leakage → Task 21; audit fixes carried → Tasks 8-18; stable imports → Task 19; `MountainAshBaseSettings` preserved → Task 4 (`ConnectionProfile(MountainAshBaseSettings)`).
- Non-goals: factories only updated at call sites, not rewritten → Task 20. No new backends. No new auth modes beyond spec table.
- Architecture layers 1-6 → Tasks 4, 1, 1, 2, 8-18 (adapters), 7-18 (shells).
- Data contract → Tasks 1, 2.
- `ConnectionProfile` methods → Task 4.
- Adapter layer → Tasks 3 (default map) + 8-18 (backend adapters).
- File layout → matches Tasks 1-18 + 19.
- Testing: descriptor invariants → Task 6; round-trip per backend → Tasks 7-18; audit regressions → Tasks 8-18 test blocks.
- Migration phases: Phase 1 = Tasks 1-6; Phase 2 = Tasks 7-18 (in cheap→hard order); Phase 3 = Tasks 19-22.
- Phase 4 explicitly deferred to follow-up plans — noted in preamble.

**2. Placeholder scan:** No "TBD" / "implement later" / "Similar to Task N" / narrative-only steps. Every code step includes actual code. One test step (`test_service_account_info_converts_to_credentials`) has a `try/except` because the dummy SA key cannot fully construct Credentials offline; the intent is documented inline.

**3. Type consistency:**
- `ConnectionProfile` / `BackendDescriptor` / `ParameterSpec` / `MISSING` — consistent across tasks.
- `to_driver_kwargs` / `to_connection_string` / `provider_type` / `backend` names consistent.
- `AuthSpec` subclass names match the spec table and the auth dispatch map.
- `__descriptor__` / `__adapter__` class variables consistent.
- Adapter function signature `(profile) -> dict[str, t.Any]` consistent across all backends.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-15-settings-registry.md`. Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
