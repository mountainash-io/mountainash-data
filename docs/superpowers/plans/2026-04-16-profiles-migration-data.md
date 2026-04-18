# Profiles Promotion — Phase 2: `mountainash-data` Migration

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `mountainash-data`'s internal descriptor/auth/profile/registry modules with thin re-exports + subclasses of the new `mountainash-settings.profiles` and `mountainash-settings.auth` sub-packages. The external API of `mountainash_data.core.settings` (imported by downstream packages and app code) stays byte-identical.

**Architecture:** `ConnectionProfile` becomes a thin subclass of `DescriptorProfile` that adds the database-flavored `to_driver_kwargs()` and `to_connection_string()` output methods. A module-level `DATABASES_REGISTRY = Registry("databases")` + bound `register` decorator replaces the current module-level `REGISTRY` dict. All 12 per-backend shell classes and all per-backend adapter modules are unchanged beyond their import statements.

**Tech Stack:** Python 3.12, pydantic v2, `mountainash-settings` (now exporting profiles + auth), `MountainAshBaseSettings`.

**Prerequisites:**
- Phase 1 (`mountainash-settings` scaffolding) merged and a new `mountainash-settings` release published so this package can pin the version.
- Current branch tip: `2ec5079` on `feat/settings-registry` (settings-registry refactor complete).

**Working directory:** `/home/nathanielramm/git/mountainash-io/mountainash/mountainash-data`

**Branch:** Create `feat/profiles-migration` from `main` **after** `feat/settings-registry` is merged. If `feat/settings-registry` has NOT been merged yet at the time this plan starts, branch from `feat/settings-registry` instead and note the merge dependency in the PR.

**Spec:** `../../mountainash-settings/docs/superpowers/specs/2026-04-16-profiles-promotion-design.md`

---

## Task 1: Branch + dependency bump

**Files:**
- Modify: `hatch.toml`
- Modify: `pyproject.toml` (if pinning present there)

- [ ] **Step 1: Branch**

```bash
cd /home/nathanielramm/git/mountainash-io/mountainash/mountainash-data
git checkout -b feat/profiles-migration
```

- [ ] **Step 2: Pin the new `mountainash-settings` version**

Grep current pin:

```bash
grep -rn "mountainash-settings\|mountainash_settings" hatch.toml pyproject.toml 2>/dev/null
```

Update the pinned version to the release that includes Phase 1. The `hatch.toml` in this repo uses relative-path deps for the sibling monorepo (`mountainash_settings @ {root:uri}/../mountainash-settings`); that path is unchanged — **but** ensure the sibling checkout is at the Phase-1 merge commit before running tests.

```bash
cd ../mountainash-settings && git pull --ff-only && git log --oneline -1
# Confirm Phase-1 commits are present.
cd ../mountainash-data
```

- [ ] **Step 3: Run tests to confirm environment is stable**

```bash
hatch run test:test-target tests/test_unit/core/settings/ -v 2>&1 | tail -5
```

Expected: existing 235 tests pass, 2 skipped. No migration work yet — just confirming the new `mountainash-settings` doesn't break the current code (it shouldn't; Phase 1 was purely additive).

- [ ] **Step 4: Commit (trivially if no file changes; skip if none)**

If `hatch.toml` needed no change, skip this commit. Otherwise:

```bash
git add hatch.toml pyproject.toml
git commit -m "chore(deps): pin mountainash-settings to profiles-promotion release"
```

---

## Task 2: New `ConnectionProfile` subclass + `DATABASES_REGISTRY`

**Files:**
- Modify: `src/mountainash_data/core/settings/profile.py` (full rewrite — much smaller)
- Modify: `src/mountainash_data/core/settings/registry.py` (full rewrite — just a wrapper)

- [ ] **Step 1: Rewrite `profile.py` as thin subclass**

```python
# src/mountainash_data/core/settings/profile.py
"""ConnectionProfile — database-flavored subclass of DescriptorProfile.

Adds ``to_driver_kwargs()`` and ``to_connection_string()`` on top of the
generic mechanism provided by
:class:`mountainash_settings.profiles.DescriptorProfile`.
"""

from __future__ import annotations

import typing as t
from urllib.parse import quote

from pydantic import SecretStr

from mountainash_settings.profiles import DescriptorProfile

__all__ = ["ConnectionProfile"]


class ConnectionProfile(DescriptorProfile):
    """Database connection settings.

    Public API:
        - :meth:`to_driver_kwargs` — dict ready for the Ibis driver.
        - :meth:`to_connection_string` — URL form, or ``NotImplementedError``
          if the descriptor has no ``connection_string_scheme`` metadata.

    Subclasses set ``__descriptor__`` (a :class:`ProfileDescriptor`) and
    optionally ``__adapter__``. Field installation, auth union, and template
    wiring are inherited from :class:`DescriptorProfile`.
    """

    def to_driver_kwargs(self) -> dict[str, t.Any]:
        """Build the final driver kwargs dict.

        If ``__adapter__`` is set, it owns the full pipeline — typically it
        calls :meth:`_default_kwargs` and :meth:`_auth_kwargs` and layers
        composite mappings on top. Otherwise defaults to descriptor
        ``driver_key`` mappings + default auth dispatch.
        """
        adapter = type(self).__dict__.get("__adapter__")
        if adapter is None:
            for base in type(self).__mro__[1:]:
                candidate = base.__dict__.get("__adapter__")
                if candidate is not None:
                    adapter = candidate
                    break
        if adapter is not None:
            return adapter(self)
        kwargs = self._default_kwargs()
        kwargs.update(self._auth_kwargs())
        return kwargs

    def to_connection_string(self) -> str:
        """Build ``scheme://user:pass@host:port/database`` from the descriptor.

        Reads the scheme from ``descriptor.metadata['connection_string_scheme']``
        (or a typed ``connection_string_scheme`` attribute if the descriptor
        subclass provides one). Raises :class:`NotImplementedError` if absent.
        """
        desc = self.__descriptor__
        scheme = getattr(desc, "connection_string_scheme", None)
        if scheme is None:
            scheme = desc.metadata.get("connection_string_scheme")
        if scheme is None:
            raise NotImplementedError(
                f"Profile {self.backend!r} has no connection string scheme"
            )
        host = getattr(self, "HOST", None)
        port = getattr(self, "PORT", None)
        database = getattr(self, "DATABASE", None)
        url = scheme
        auth = getattr(self, "auth", None)
        if auth is not None:
            username = getattr(auth, "username", None)
            if username:
                url += quote(str(username), safe="")
                pw = getattr(auth, "password", None)
                if isinstance(pw, SecretStr):
                    url += ":" + quote(pw.get_secret_value(), safe="")
                url += "@"
        if host is not None:
            url += str(host)
        if port is not None:
            url += f":{port}"
        if database is not None:
            url += f"/{database}"
        return url
```

- [ ] **Step 2: Rewrite `registry.py` as a wrapper around `DATABASES_REGISTRY`**

```python
# src/mountainash_data/core/settings/registry.py
"""Module-level registry of database backend descriptors.

Backed by :class:`mountainash_settings.profiles.Registry` — a per-domain
registry class. The old module-level ``REGISTRY`` dict is preserved as a
property-style alias for any downstream consumer that imports it directly.
"""

from __future__ import annotations

import typing as t

from mountainash_settings.profiles import Registry

if t.TYPE_CHECKING:
    from mountainash_settings.profiles import ProfileDescriptor
    from .profile import ConnectionProfile

__all__ = [
    "DATABASES_REGISTRY",
    "REGISTRY",
    "get_descriptor",
    "get_settings_class",
    "register",
]

DATABASES_REGISTRY = Registry("databases")

register = DATABASES_REGISTRY.decorator()


def get_descriptor(name: str) -> "ProfileDescriptor":
    return DATABASES_REGISTRY.get_descriptor(name)


def get_settings_class(name: str) -> type["ConnectionProfile"]:
    return DATABASES_REGISTRY.get_settings_class(name)  # type: ignore[return-value]


# Backwards-compatibility alias — preserves ``from ... import REGISTRY`` imports.
# Read-only from the outside; mutations should go through ``@register``.
class _RegistryDictView:
    """Dict-like view that delegates to DATABASES_REGISTRY.descriptors."""

    def __contains__(self, name: str) -> bool:
        return name in DATABASES_REGISTRY

    def __getitem__(self, name: str) -> "ProfileDescriptor":
        return DATABASES_REGISTRY.get_descriptor(name)

    def __iter__(self):
        return iter(DATABASES_REGISTRY.descriptors)

    def __len__(self) -> int:
        return len(DATABASES_REGISTRY)

    def items(self):
        return DATABASES_REGISTRY.descriptors.items()

    def keys(self):
        return DATABASES_REGISTRY.descriptors.keys()

    def values(self):
        return DATABASES_REGISTRY.descriptors.values()


REGISTRY = _RegistryDictView()
```

- [ ] **Step 3: Delete the retired local files**

```bash
git rm src/mountainash_data/core/settings/descriptor.py
git rm -r src/mountainash_data/core/settings/auth/
```

- [ ] **Step 4: Run tests to see what breaks**

```bash
hatch run test:test-target tests/test_unit/core/settings/ -v 2>&1 | tail -30
```

Expected: massive import breakage across per-backend files (sqlite.py, duckdb.py, etc.) because they import `from .descriptor import ...` and `from .auth import ...` — those paths no longer exist.

This is intentional — fix in Task 3.

**Do NOT commit yet.**

---

## Task 3: Fix per-backend imports across all 12 backends + adapters

**Files:**
- Modify: `src/mountainash_data/core/settings/sqlite.py`
- Modify: `src/mountainash_data/core/settings/duckdb.py`
- Modify: `src/mountainash_data/core/settings/motherduck.py`
- Modify: `src/mountainash_data/core/settings/postgresql.py`
- Modify: `src/mountainash_data/core/settings/mysql.py`
- Modify: `src/mountainash_data/core/settings/mssql.py`
- Modify: `src/mountainash_data/core/settings/snowflake.py`
- Modify: `src/mountainash_data/core/settings/bigquery.py`
- Modify: `src/mountainash_data/core/settings/redshift.py`
- Modify: `src/mountainash_data/core/settings/pyspark.py`
- Modify: `src/mountainash_data/core/settings/trino.py`
- Modify: `src/mountainash_data/core/settings/pyiceberg_rest.py`
- Modify: `src/mountainash_data/core/settings/adapters/*.py` (7 files)

- [ ] **Step 1: Mechanical import rewrites**

Every per-backend and adapter file needs three import changes:

| Old | New |
|-----|-----|
| `from .descriptor import BackendDescriptor, ParameterSpec` | `from mountainash_settings.profiles import ParameterSpec, ProfileDescriptor as BackendDescriptor` |
| `from .auth import NoAuth, PasswordAuth, ...` | `from mountainash_settings.auth import NoAuth, PasswordAuth, ...` |
| `from .registry import register` | (unchanged — local `register` is still the exported bound decorator) |
| `from .profile import ConnectionProfile` | (unchanged — local `ConnectionProfile` still exists as subclass) |

Adapter files also have:

| Old | New |
|-----|-----|
| `from mountainash_data.core.settings.auth import ...` | `from mountainash_settings.auth import ...` |
| `profile._default_driver_kwargs()` | `profile._default_kwargs()` |
| `profile._auth_to_driver_kwargs()` | `profile._auth_kwargs()` |

The method rename in adapters is the only non-trivial change — `DescriptorProfile` renamed the helpers to drop the "driver" prefix.

Run a bulk edit script:

```bash
# In the mountainash-data repo root:

# 1. Per-backend settings files: swap descriptor + auth imports
for f in src/mountainash_data/core/settings/{sqlite,duckdb,motherduck,postgresql,mysql,mssql,snowflake,bigquery,redshift,pyspark,trino,pyiceberg_rest}.py; do
  sed -i '
    s|from \.descriptor import BackendDescriptor, ParameterSpec|from mountainash_settings.profiles import ParameterSpec, ProfileDescriptor as BackendDescriptor|
    s|from \.auth import|from mountainash_settings.auth import|
  ' "$f"
done

# 2. Adapter files: swap auth imports + method rename
for f in src/mountainash_data/core/settings/adapters/*.py; do
  sed -i '
    s|from mountainash_data\.core\.settings\.auth import|from mountainash_settings.auth import|g
    s|_default_driver_kwargs()|_default_kwargs()|g
    s|_auth_to_driver_kwargs()|_auth_kwargs()|g
  ' "$f"
done
```

- [ ] **Step 2: Sanity-check the rewrites**

```bash
grep -rn "from \.descriptor\|from \.auth import\|from mountainash_data\.core\.settings\.auth\|_default_driver_kwargs\|_auth_to_driver_kwargs" \
    src/mountainash_data/core/settings/ 2>&1 | grep -v "^Binary"
```

Expected: no matches. Every old-path import or method call should be gone.

- [ ] **Step 3: Run the settings suite**

```bash
hatch run test:test-target tests/test_unit/core/settings/ -v 2>&1 | tail -20
```

Expected: most backend tests pass, some may fail due to descriptor-subclassing quirks (see Task 4) or `REGISTRY` being empty on first import.

If a test fails with "module has no attribute `ConnectionProfile`", check that `profile.py` was rewritten in Task 2 Step 1.

- [ ] **Step 4: Commit (even if some tests still fail — next task fixes them)**

```bash
git add src/mountainash_data/core/settings/
git commit -m "refactor(settings): rewire imports to mountainash-settings.profiles + auth"
```

---

## Task 4: Adjust `ProfileDescriptor` usage in per-backend files

**Context:** `mountainash-data` descriptors use several typed metadata fields that `ProfileDescriptor` moved to a generic `metadata: dict[str, Any]`: `default_port`, `connection_string_scheme`, `ibis_dialect`, `rides_on`. Two options:

1. **Stuff into metadata dict** — quick, works everywhere: `metadata={"default_port": 5432, "connection_string_scheme": "postgresql://", ...}`.
2. **Subclass `ProfileDescriptor`** — typed. Preferred for mountainash-data given the fields are heavily used by adapters and `to_connection_string()`.

Use Option 2.

**Files:**
- Create: `src/mountainash_data/core/settings/descriptor.py` (small — just the subclass)
- Modify: 12 per-backend settings files (revert the `ProfileDescriptor as BackendDescriptor` alias to use the typed subclass)

- [ ] **Step 1: Create a typed subclass**

```python
# src/mountainash_data/core/settings/descriptor.py
"""Database-flavored ProfileDescriptor with typed metadata fields.

Retained in mountainash-data (rather than lifted to mountainash-settings)
because these fields are domain-specific: ``connection_string_scheme`` and
``ibis_dialect`` are meaningful only for SQL-like databases.
"""

from __future__ import annotations

import typing as t
from dataclasses import dataclass, field

from mountainash_settings.profiles import (
    MISSING,
    ParameterSpec,
    ProfileDescriptor,
)

__all__ = ["MISSING", "BackendDescriptor", "ParameterSpec"]


@dataclass(frozen=True, kw_only=True)
class BackendDescriptor(ProfileDescriptor):
    """ProfileDescriptor with database-specific typed metadata.

    Extra fields:
        default_port: Default TCP port if the backend listens on one.
        connection_string_scheme: URL scheme prefix (``"postgresql://"``) or
            ``None`` if the backend has no URL form.
        ibis_dialect: Name of the Ibis backend if Ibis handles this backend.
        rides_on: Name of another backend whose Ibis path this one routes
            through (e.g. ``motherduck`` → ``duckdb``). Metadata only.
    """

    default_port: int | None = None
    connection_string_scheme: str | None = None
    ibis_dialect: str | None = None
    rides_on: str | None = None
```

- [ ] **Step 2: Revert per-backend imports to use the local subclass**

```bash
for f in src/mountainash_data/core/settings/{sqlite,duckdb,motherduck,postgresql,mysql,mssql,snowflake,bigquery,redshift,pyspark,trino,pyiceberg_rest}.py; do
  sed -i '
    s|from mountainash_settings\.profiles import ParameterSpec, ProfileDescriptor as BackendDescriptor|from .descriptor import BackendDescriptor, ParameterSpec|
  ' "$f"
done
```

- [ ] **Step 3: Run tests**

```bash
hatch run test:test-target tests/test_unit/core/settings/ -v 2>&1 | tail -20
```

Expected: all 235 settings tests pass, 2 skipped. If `_default_kwargs()` or `_auth_kwargs()` method-rename errors remain in some adapter, fix them (they should have been caught in Task 3 Step 1).

- [ ] **Step 4: Commit**

```bash
git add src/mountainash_data/core/settings/descriptor.py \
        src/mountainash_data/core/settings/{sqlite,duckdb,motherduck,postgresql,mysql,mssql,snowflake,bigquery,redshift,pyspark,trino,pyiceberg_rest}.py
git commit -m "refactor(settings): re-introduce typed BackendDescriptor subclass"
```

---

## Task 5: Update `settings/__init__.py` re-exports

**Files:**
- Modify: `src/mountainash_data/core/settings/__init__.py`

The external API must remain byte-identical to what downstream packages consume. Symbols move upstream, imports move upstream, but the names exported from `mountainash_data.core.settings` stay the same.

- [ ] **Step 1: Rewrite `__init__.py`**

```python
# src/mountainash_data/core/settings/__init__.py
"""Backend settings — declarative descriptor + registry.

The *AuthSettings classes below are stable import anchors; internally each
class body is a two-line shell (``__descriptor__`` + ``__adapter__``).

As of 2026-04-16, the descriptor/registry/auth machinery lives in
``mountainash-settings``. Symbols re-exported here preserve the external API.
"""

from __future__ import annotations

# Core primitives (auth + profiles from mountainash-settings)
from mountainash_settings.auth import (
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

# Local database-flavored subclasses
from .descriptor import MISSING, BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import (
    DATABASES_REGISTRY,
    REGISTRY,
    get_descriptor,
    get_settings_class,
    register,
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
    "DATABASES_REGISTRY", "REGISTRY",
    "get_descriptor", "get_settings_class", "register",
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

- [ ] **Step 2: Run full suite**

```bash
hatch run test:test-target tests/test_unit/ -v 2>&1 | tail -20
```

Expected: all 483 tests pass, 5 skipped. No regressions.

- [ ] **Step 3: Commit**

```bash
git add src/mountainash_data/core/settings/__init__.py
git commit -m "chore(settings): update __init__ re-exports for mountainash-settings promotion"
```

---

## Task 6: Replace the invariants test with the shared helper

**Files:**
- Modify: `tests/test_unit/core/settings/test_descriptors_invariants.py` (replace contents)

The existing file hand-rolls 10 parametric invariants. The promoted helper
`descriptor_invariants_for(registry)` gives the same coverage from one line.

- [ ] **Step 1: Replace the file**

```python
# tests/test_unit/core/settings/test_descriptors_invariants.py
"""Parametric descriptor invariants for all registered database backends.

Generated from the shared ``descriptor_invariants_for`` helper in
``mountainash-settings``. Every descriptor in ``DATABASES_REGISTRY`` gets
checked against the invariants for free — no per-backend test additions
required.
"""

from __future__ import annotations

# Ensure every backend module's @register decorator has fired before we
# snapshot the registry for the parametrize decorator.
import mountainash_data.core.settings  # noqa: F401

from mountainash_data.core.settings.registry import DATABASES_REGISTRY
from mountainash_settings.profiles import descriptor_invariants_for

TestDatabaseInvariants = descriptor_invariants_for(DATABASES_REGISTRY)
```

- [ ] **Step 2: Run the invariants suite**

```bash
hatch run test:test-target tests/test_unit/core/settings/test_descriptors_invariants.py -v 2>&1 | tail -10
```

Expected: 120 parametric cases pass (12 backends × 10 invariants).

- [ ] **Step 3: Run the full settings suite**

```bash
hatch run test:test-target tests/test_unit/core/settings/ -v 2>&1 | tail -5
```

Expected: all 235+ tests pass.

- [ ] **Step 4: Commit**

```bash
git add tests/test_unit/core/settings/test_descriptors_invariants.py
git commit -m "test(settings): switch to shared descriptor_invariants_for helper"
```

---

## Task 7: Delete the now-dead local auth/dispatch tests

**Files:**
- Delete: `tests/test_unit/core/settings/test_auth.py`
- Delete: `tests/test_unit/core/settings/test_auth_dispatch.py`
- Keep: `tests/test_unit/core/settings/test_descriptor.py` — rewrite to import from mountainash-settings
- Keep: `tests/test_unit/core/settings/test_profile.py` — rewrite to test `ConnectionProfile` specifically (the data-flavored subclass)
- Keep: `tests/test_unit/core/settings/test_registry.py` — rewrite to test `DATABASES_REGISTRY` specifically

- [ ] **Step 1: Delete the duplicated auth tests**

Auth coverage now lives in `mountainash-settings`'s test suite. Deleting the copies here avoids duplicate execution.

```bash
git rm tests/test_unit/core/settings/test_auth.py
git rm tests/test_unit/core/settings/test_auth_dispatch.py
```

- [ ] **Step 2: Rewrite `test_descriptor.py` to exercise the typed subclass**

```python
# tests/test_unit/core/settings/test_descriptor.py
"""Tests for the database-flavored BackendDescriptor subclass."""

import pytest

from mountainash_data.core.settings.auth import NoAuth
from mountainash_data.core.settings.descriptor import (
    BackendDescriptor,
    ParameterSpec,
)


@pytest.mark.unit
class TestBackendDescriptor:
    def test_default_port_field(self):
        d = BackendDescriptor(
            name="x", provider_type="x",
            parameters=[], auth_modes=[NoAuth],
            default_port=5432,
        )
        assert d.default_port == 5432

    def test_connection_string_scheme_field(self):
        d = BackendDescriptor(
            name="x", provider_type="x",
            parameters=[], auth_modes=[NoAuth],
            connection_string_scheme="postgresql://",
        )
        assert d.connection_string_scheme == "postgresql://"

    def test_rides_on_field(self):
        d = BackendDescriptor(
            name="motherduck", provider_type="motherduck",
            parameters=[], auth_modes=[NoAuth],
            rides_on="duckdb",
        )
        assert d.rides_on == "duckdb"

    def test_frozen(self):
        d = BackendDescriptor(
            name="x", provider_type="x",
            parameters=[], auth_modes=[NoAuth],
        )
        with pytest.raises(Exception):
            d.name = "y"  # type: ignore
```

- [ ] **Step 3: Rewrite `test_profile.py` to test only ConnectionProfile-specific methods**

```python
# tests/test_unit/core/settings/test_profile.py
"""Tests for ConnectionProfile — database-flavored DescriptorProfile.

DescriptorProfile mechanism tests live in mountainash-settings. Here we only
exercise the database-specific methods: to_driver_kwargs() and
to_connection_string().
"""

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
        ParameterSpec(name="DATABASE", type=str, tier="core", default=None,
                      driver_key="database"),
    ],
    auth_modes=[NoAuth, PasswordAuth],
)


class DummyProfile(ConnectionProfile):
    __descriptor__ = DUMMY_DESCRIPTOR


@pytest.mark.unit
class TestConnectionProfile:
    def test_to_driver_kwargs_default(self):
        p = DummyProfile(HOST="h", PORT=1234, DATABASE="db", auth=NoAuth())
        kwargs = p.to_driver_kwargs()
        assert kwargs["host"] == "h"
        assert kwargs["port"] == 1234
        assert kwargs["database"] == "db"

    def test_to_driver_kwargs_password_unwrapped(self):
        p = DummyProfile(
            HOST="h", DATABASE="db",
            auth=PasswordAuth(username="u", password=SecretStr("p")),
        )
        kwargs = p.to_driver_kwargs()
        assert kwargs["user"] == "u"
        assert kwargs["password"] == "p"

    def test_to_driver_kwargs_adapter_owns_pipeline(self):
        def _adapter(profile):
            return {"only": "thing"}

        class Adapted(ConnectionProfile):
            __descriptor__ = DUMMY_DESCRIPTOR
            __adapter__ = staticmethod(_adapter)

        p = Adapted(HOST="h", auth=NoAuth())
        assert p.to_driver_kwargs() == {"only": "thing"}

    def test_to_connection_string_full(self):
        p = DummyProfile(
            HOST="h", DATABASE="db",
            auth=PasswordAuth(username="u", password=SecretStr("p")),
        )
        url = p.to_connection_string()
        assert url == "dummy://u:p@h:9999/db"

    def test_to_connection_string_url_encodes_secrets(self):
        p = DummyProfile(
            HOST="h", DATABASE="db",
            auth=PasswordAuth(username="user@corp", password=SecretStr("p@ss:w/ord")),
        )
        url = p.to_connection_string()
        assert "user%40corp" in url
        assert "p%40ss%3Aw%2Ford" in url

    def test_to_connection_string_no_scheme_raises(self):
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

- [ ] **Step 4: Rewrite `test_registry.py` to test `DATABASES_REGISTRY` wrapper**

```python
# tests/test_unit/core/settings/test_registry.py
"""Tests for the DATABASES_REGISTRY wrapper + back-compat REGISTRY alias."""

import pytest

from mountainash_data.core.settings.registry import (
    DATABASES_REGISTRY,
    REGISTRY,
    get_descriptor,
    get_settings_class,
)


@pytest.mark.unit
class TestDatabasesRegistry:
    def test_registry_is_populated_after_import(self):
        """All 12 backends register themselves at import time."""
        import mountainash_data.core.settings  # noqa: F401

        for name in ["sqlite", "duckdb", "postgresql", "mysql", "mssql",
                     "snowflake", "bigquery", "redshift", "pyspark",
                     "trino", "motherduck", "pyiceberg_rest"]:
            assert name in DATABASES_REGISTRY, f"{name} missing from registry"

    def test_get_descriptor_returns_correct_type(self):
        import mountainash_data.core.settings  # noqa: F401
        desc = get_descriptor("sqlite")
        assert desc.name == "sqlite"

    def test_get_settings_class_returns_correct_type(self):
        import mountainash_data.core.settings  # noqa: F401
        from mountainash_data.core.settings.sqlite import SQLiteAuthSettings
        assert get_settings_class("sqlite") is SQLiteAuthSettings

    def test_legacy_REGISTRY_alias_still_works(self):
        import mountainash_data.core.settings  # noqa: F401
        assert "sqlite" in REGISTRY
        assert REGISTRY["sqlite"].name == "sqlite"
        # Iterate
        names = list(REGISTRY.keys())
        assert "sqlite" in names
```

- [ ] **Step 5: Run the full suite**

```bash
hatch run test:test-target tests/test_unit/ -v 2>&1 | tail -10
```

Expected: all tests pass. Some test counts shift — auth tests went away (now in mountainash-settings); new registry tests added; descriptor/profile tests trimmed to data-specific cases.

- [ ] **Step 6: Commit**

```bash
git add tests/test_unit/core/settings/
git commit -m "test(settings): trim tests to data-specific cases; delete duplicates"
```

---

## Task 8: Update CLAUDE.md references

**Files:**
- Modify: `CLAUDE.md`

The settings section currently describes the pattern. Update it to point at the new location.

- [ ] **Step 1: Find and update the Settings section**

Find the bullet about `src/mountainash_data/core/settings/` in `CLAUDE.md`. Replace with:

```markdown
3. **Settings** (`src/mountainash_data/core/settings/`)
   - Database-flavored layer over `mountainash-settings`'s `profiles` and
     `auth` sub-packages.
   - `BackendDescriptor` is a typed `ProfileDescriptor` subclass; every
     backend is a two-line shell registered via `@register`.
   - `ConnectionProfile` adds `to_driver_kwargs()` and `to_connection_string()`
     on top of the generic `DescriptorProfile` base.
   - Composite driver mappings live in `settings/adapters/<backend>.py`.
```

- [ ] **Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: update CLAUDE.md settings section for mountainash-settings promotion"
```

---

## Task 9: Open PR

- [ ] **Step 1: Push**

```bash
git push -u origin feat/profiles-migration
```

- [ ] **Step 2: Open PR**

Title: `Profiles promotion — Phase 2: mountainash-data migrates to mountainash-settings.profiles`

Body should include:

- Link to design spec
- Link to Phase 1 PR / release
- Confirmation that external API (`mountainash_data.core.settings.__all__`) is byte-identical: diff the old and new `__all__` lists as evidence
- Test count: 483 passed, 5 skipped (unchanged from current branch)

---

## Self-Review Notes

- **Spec coverage:**
  - Section 1 (layout `mountainash-data` shrinks) → Tasks 2–5
  - Section 2 (thin `ConnectionProfile` subclass) → Task 2
  - Section 3 (per-backend imports) → Task 3
  - Section 4 (typed `BackendDescriptor`) → Task 4
  - Section 5 (public re-exports unchanged) → Task 5
  - Section 6 (shared invariants helper) → Task 6
  - Section 7 (docs) → Task 8
- **Placeholder scan:** No "TBD"s. Every step has verbatim code or exact commands.
- **Type consistency:** `ConnectionProfile` / `BackendDescriptor` / `ParameterSpec` / `DATABASES_REGISTRY` consistent throughout.
- **Setattr-bypass limitation:** Inherited from `DescriptorProfile`. Existing workarounds (PySpark `__setattr__`, adapter `str(enum)` defense) unchanged by this migration — they still live in their respective files.
- **Back-compat alias:** `REGISTRY` dict-like view preserves any downstream `from mountainash_data.core.settings.registry import REGISTRY` imports.

## Execution Handoff

After Phase 2 lands and merges, three migration plans remain — they can be drafted as needed using this plan and Phase 1 as the template:

- Phase 3: `mountainash-utils-secrets` (5 providers)
- Phase 4: `mountainash-utils-files` (18 providers)
- Phase 5: `mountainash-acrds-core` (single descriptor; primary Pattern B validation)

Each follows the same skeleton as Phase 2: branch, add thin subclass + per-domain `Registry`, rewrite per-provider imports, update `__init__.py`, delete retired base class, run tests. They are left unwritten until Phase 2 proves the pattern in production.
