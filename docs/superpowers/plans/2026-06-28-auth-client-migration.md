# Auth-Client Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate `mountainash-data` off the deleted `mountainash_settings.auth` onto `mountainash-auth-client`, with auth decoupled from the backend config profile and composed in a factory.

**Architecture:** Three layers mirroring `mountainash-transport`: (L1) `*BackendProfile.emit(provider_type)` renders config only; (L2) `*AuthProfile` is pure credential data from auth-client; (L3) a data-owned `(provider_type, auth_class)→fn` dispatch table renders auth onto the config; the `ConnectionFactory` composes them and the runtime (`IbisBackend`/`IcebergConnection`) consumes a finished dict. No `register_adapter`, no `emit()` on auth profiles, no cross-package mutation.

**Tech Stack:** Python 3.12, `mountainash-settings` 26.5.0 (`Profile`/`ProfileSpec`/`ParameterSpec`/`emit()`), `mountainash-auth-client` (`*AuthProfile`), Ibis 11, PyIceberg, hatch + uv, pytest.

## Global Constraints

- **Clean break, zero downstream consumers.** No backward-compat aliases, no deprecation shims. Old names are deleted, not aliased.
- **`provider_type` enum is `CONST_DB_PROVIDER_TYPE`** in `core/constants.py`; the PostgreSQL member is **`POSTGRESQL`** (NOT `POSTGRES`). Members: `MYSQL, POSTGRESQL, MSSQL, SNOWFLAKE, BIGQUERY, REDSHIFT, SQLITE, DUCKDB, MOTHERDUCK, TRINO, PYICEBERG_REST, ORACLE, CLICKHOUSE, DATABRICKS, SINGLESTOREDB, EXASOL, IMPALA, MATERIALIZE, RISINGWAVE, DRUID, PYSPARK`.
- **Auth profile fields are UPPERCASE `ParameterSpec` names**; secret fields are pydantic `SecretStr` (use `.get_secret_value()`); path fields are `Path | None` (use `str(...)` where a driver wants a string).
- **Auth profiles are pure data.** Data NEVER calls `auth_profile.emit()` and registers NOTHING onto auth-client classes.
- **`Profile.emit(target, *, base=None)`** runs `driver_key` renames via `_default_kwargs(target)`, then a 2-arg compose adapter from `__adapters__.get(target)` if present. A profile with any `__adapters__` is "target-scoped": `emit()` with no target raises. A profile WITHOUT `__adapters__` (bare-string `driver_key`s) accepts any explicit target — so `emit(provider_type)` is uniform across all 20 (confirmed: flat backends resolve bare driver_keys regardless of target; shaping backends route through their `__adapters__[provider_type]`).
- **Compose adapters ADD only.** Never overwrite a key `driver_key` produced, except the single sanctioned mssql `host` rewrite. Fields a compose folds carry NO flat `driver_key`.
- **Auth dispatch is MRO-aware** (`functools.singledispatch` semantics): most-specific registered base wins; two incomparable sibling registrations for one `provider_type` raise `TypeError`.
- **Fail-closed everywhere.** Unsupported auth → `ValueError`; supported-but-no-adapter → `ValueError`; unsupported URL auth → `NotImplementedError`.
- **Driver imports are LOCAL** to each adapter function (e.g. `from trino.auth import ...` inside the fn), so importing the settings layer never pulls `trino`/`google`.
- **Test integrity:** if a golden disagrees with the implementation, STOP and surface it — do not edit the test to pass. Never encode counts of backends as test assertions.
- **Commit trailer (every commit):**
  ```
  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
  ```
- **Branch:** all work on `feature/auth-client-migration` (already checked out); PR targets `develop`.

### Spec deviations carried by this plan (verified against the real tree)

1. **4 shaping backends, not 3:** add **pyiceberg** (`HEADERS`→`header.<k>` compose) to mysql/mssql/snowflake. pyiceberg's `s3.*`/`rest.*` become flat `driver_key`s.
2. **`POSTGRESQL`** is the real enum member (spec said `POSTGRES`).
3. **`core/factories/` does not exist on this branch** — Task 6 creates it fresh. (A different `ConnectionFactory` exists on the `settings-registry` worktree branch; flag at PR time for merge awareness.)
4. **hatch:** auth-client is absent from all envs (add it); `mountainash_utils_ssh` is a dead path-dep in `dev`/`build_github`/`test_github` (remove it).
5. **pyspark** is pure-flat — give params `driver_key`s, delete its adapter.
6. **`UrlParts` lives in `core/settings/profile.py`** (the L1 output type), imported by the factory — NOT in the factory — so the settings flip doesn't depend on the factory.

### CRITICAL ordering constraint (why the flip is Task 2)

At HEAD the suite fails at collection: `core/settings/__init__.py` and every backend module import the deleted `mountainash_settings.auth`. **Importing ANY submodule of `core.settings` runs `core/settings/__init__.py` first**, so until the whole settings layer is migrated, *nothing* under `core.settings` — including new adapter/registry modules placed there — can be imported or tested. Therefore the atomic settings flip (Task 2) MUST precede the adapter/registry/factory tasks. After Task 2, `import mountainash_data.core.settings` succeeds and every later task's tests can collect. Full green (`hatch run test:test`) is asserted in Task 9.

---

## File Structure

**New files**
- `core/settings/adapters/sql.py` — shared flat `userpass(auth, base)`.
- `core/settings/adapters/registry.py` — `_AUTH_ADAPTERS` + MRO `auth_adapter()`.
- `core/factories/__init__.py`, `core/factories/connection_factory.py` — compose, URL appliers, `apply_auth_adapter`, dialect/scheme→provider helpers.
- Tests under `tests/test_unit/core/settings/adapters/`, `tests/test_unit/core/factories/`.

**Heavily modified**
- `core/settings/descriptor.py` (`supported_auth`), `core/settings/profile.py` (`BackendProfile` + `UrlParts` + `to_url_parts`), `core/settings/__init__.py` (import swap), the 20 backend modules, the per-backend adapter modules, `backends/ibis/backend.py`, `backends/iceberg/connection.py`, `hatch.toml`, `pyproject.toml`.

**Deleted**
- `core/settings/adapters/pyspark.py`, `core/settings/auth/` (3 shim files).

---

## Task 1: Dependency wiring

**Files:** Modify `pyproject.toml`, `hatch.toml`.

**Interfaces:** Produces `mountainash_auth_client` importable in all hatch envs.

- [ ] **Step 1: Add auth-client to `pyproject.toml`**

In `[project] dependencies`, after `"sqlalchemy",`, add:
```toml
    "mountainash-auth-client",
```

- [ ] **Step 2: Wire auth-client + remove dead utils-ssh in `hatch.toml`**

In `envs.dev` and `envs.test`, add (local format) and DELETE any `mountainash_utils_ssh` line:
```toml
    "mountainash_auth_client @  {root:uri}/../mountainash-auth-client",
```
In `envs.test_github` and `envs.build_github`, add (CI format) and DELETE their `mountainash_utils_ssh` lines:
```toml
    "mountainash_auth_client @  {root:uri}/temp/mountainash-auth-client",
```

- [ ] **Step 3: Verify auth-client imports**

Run: `hatch run test:python -c "import mountainash_auth_client as a; print(a.PasswordAuthProfile, a.NoAuthProfile, a.AuthProfile)"`
Expected: prints the three classes. If env is stale: `hatch env prune` then re-run.

- [ ] **Step 4: Confirm utils-ssh gone**

Run: `grep -rn "mountainash_utils_ssh" hatch.toml`
Expected: no output.

- [ ] **Step 5: Commit**
```bash
git add pyproject.toml hatch.toml
git commit -m "build: add mountainash-auth-client dep; drop dead utils-ssh path-dep

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Settings core flip (descriptor + base profile + 20 backends + __init__)

The irreducible atomic flip — `__init__.py` imports the renamed backends, which import the renamed base, which uses the new descriptor. After this task `import mountainash_data.core.settings` succeeds and flat backends emit correct config. The 4 shaping backends import but emit config WITHOUT their nested keys (completed in Task 3).

**Files:**
- Modify: `core/settings/descriptor.py`, `core/settings/profile.py`, all 20 backend modules, `core/settings/__init__.py`
- Delete: `core/settings/auth/` (3 files), `core/settings/adapters/pyspark.py`
- Test: `tests/test_unit/core/settings/test_settings_flip.py`

**Interfaces:**
- Produces: `UrlParts` (in `profile.py`); `BackendProfile` base with `to_url_parts()`; `BackendSpec.supported_auth: tuple[type, ...]`; 20 `*BackendProfile` classes (rename table below).

- [ ] **Step 1: Add `supported_auth` to `descriptor.py`**
```python
@dataclass(frozen=True, kw_only=True)
class BackendSpec(ProfileSpec):
    default_port: int | None = None
    connection_string_scheme: str | None = None
    ibis_dialect: str | None = None
    rides_on: str | None = None
    supported_auth: tuple[type, ...] = ()

    def __post_init__(self) -> None:
        if not self.supported_auth:
            raise ValueError(f"{self.name}: supported_auth must be non-empty")
```
> If `ProfileSpec` defines `__post_init__`, call `super().__post_init__()` first. Check: `hatch run test:python -c "from mountainash_settings.profiles import ProfileSpec; print(hasattr(ProfileSpec,'__post_init__'))"`.

- [ ] **Step 2: Rewrite `profile.py` — `UrlParts` + `BackendProfile`**
```python
from dataclasses import dataclass, field
# ... keep existing Profile / lookup_class_var imports; REMOVE the quote import.


@dataclass(frozen=True)
class UrlParts:
    """Credential-free URL skeleton (L1). Every authority component optional."""
    scheme: str
    database: str | None = None
    host: str | None = None
    port: int | None = None
    path: str | None = None
    query: dict[str, str] = field(default_factory=dict)


class BackendProfile(Profile):
    """Database backend CONFIG. Pure L1 emitter — no auth methods.

    Auth is orthogonal, applied by ConnectionFactory, never here.
    """

    def to_url_parts(self) -> UrlParts:
        desc = lookup_class_var(type(self), "__spec__")
        scheme = getattr(desc, "connection_string_scheme", None)
        if scheme is None:
            raise NotImplementedError(f"Profile {self.backend!r} has no URL form")
        scheme = scheme.removesuffix("://").removesuffix(":")
        return UrlParts(
            scheme=scheme,
            host=getattr(self, "HOST", None),
            port=getattr(self, "PORT", None),
            database=getattr(self, "DATABASE", None),
        )
```
Remove `to_driver_kwargs`, `to_connection_string`, `_auth_kwargs`/`__adapter__` references.

- [ ] **Step 3: The 16 flat backends — rename + import-swap + supported_auth**

For each, apply: (1) `from mountainash_settings.auth import (...)` → `from mountainash_auth_client import (<classes>)`; (2) `from .profile import ConnectionProfile` → `from .profile import BackendProfile`; (3) replace `auth_modes=[...]` with `supported_auth=(<tuple>),`; (4) rename `class <Old>AuthSettings(ConnectionProfile):` → `class <New>BackendProfile(BackendProfile):`.

Worked example — `postgresql.py`:
```python
from mountainash_auth_client import NoAuthProfile, PasswordAuthProfile
from .profile import BackendProfile
#   ...inside POSTGRESQL_SPEC:
    supported_auth=(PasswordAuthProfile, NoAuthProfile),
#   ...
@register
class PostgreSQLBackendProfile(BackendProfile):
    __spec__ = POSTGRESQL_SPEC
```

| File | Old → New class | `supported_auth=` |
|---|---|---|
| `postgresql.py` | `PostgreSQLAuthSettings` → `PostgreSQLBackendProfile` | `(PasswordAuthProfile, NoAuthProfile)` |
| `clickhouse.py` | `ClickHouseAuthSettings` → `ClickHouseBackendProfile` | `(PasswordAuthProfile, NoAuthProfile)` |
| `singlestoredb.py` | `SingleStoreDBAuthSettings` → `SingleStoreDBBackendProfile` | `(PasswordAuthProfile, NoAuthProfile)` |
| `druid.py` | `DruidAuthSettings` → `DruidBackendProfile` | `(PasswordAuthProfile, NoAuthProfile)` |
| `impala.py` | `ImpalaAuthSettings` → `ImpalaBackendProfile` | `(PasswordAuthProfile, NoAuthProfile)` |
| `materialize.py` | `MaterializeAuthSettings` → `MaterializeBackendProfile` | `(PasswordAuthProfile, NoAuthProfile)` |
| `risingwave.py` | `RisingWaveAuthSettings` → `RisingWaveBackendProfile` | `(PasswordAuthProfile, NoAuthProfile)` |
| `exasol.py` | `ExasolAuthSettings` → `ExasolBackendProfile` | `(PasswordAuthProfile,)` |
| `sqlite.py` | `SQLiteAuthSettings` → `SQLiteBackendProfile` | `(NoAuthProfile,)` |
| `duckdb.py` | `DuckDBAuthSettings` → `DuckDBBackendProfile` | `(NoAuthProfile,)` |
| `redshift.py` | `RedshiftAuthSettings` → `RedshiftBackendProfile` | `(PasswordAuthProfile, IAMAuthProfile)` |
| `databricks.py` | `DatabricksAuthSettings` → `DatabricksBackendProfile` | `(TokenAuthProfile, PasswordAuthProfile, NoAuthProfile)` |
| `trino.py` | `TrinoAuthSettings` → `TrinoBackendProfile` | `(PasswordAuthProfile, JWTAuthProfile, KerberosAuthProfile, NoAuthProfile)` |
| `bigquery.py` | `BigQueryAuthSettings` → `BigQueryBackendProfile` | `(ServiceAccountAuthProfile, NoAuthProfile)` |

> `redshift/databricks/trino/bigquery` carry `__adapter__ = staticmethod(_adapter.build_driver_kwargs)` + a `from .adapters import X as _adapter` line — **DELETE both** (their config is flat `driver_key`s; auth moves to the registry). No `__adapters__` needed for them.

- [ ] **Step 4: pyspark — flat, delete adapter, add `driver_key`s**

`pyspark.py`: delete `from .adapters import pyspark as _adapter` + the `__adapter__` line; rename → `PySparkBackendProfile`; `supported_auth=(NoAuthProfile,)`; add `driver_key`s:
```python
        ParameterSpec(name="SESSION", type=t.Optional[t.Any], tier="core", default=None, driver_key="session"),
        ParameterSpec(name="MODE", type=PySparkMode, tier="core", default=PySparkMode.BATCH, driver_key="mode"),
        ParameterSpec(name="SPARK_MASTER", type=t.Optional[str], tier="advanced", default=None, driver_key="spark.master"),
        ParameterSpec(name="APPLICATION_NAME", type=t.Optional[str], tier="advanced", default=None, driver_key="spark.app.name"),
        ParameterSpec(name="WAREHOUSE_DIR", type=t.Optional[str], tier="advanced", default=None, driver_key="spark.sql.warehouse.dir"),
        ParameterSpec(name="PARTITIONS", type=t.Optional[int], tier="advanced", default=None, driver_key="spark.sql.shuffle.partitions"),
```
Update `__all__` → `["PySparkBackendProfile", "PySparkMode", "PYSPARK_SPEC"]`. `git rm core/settings/adapters/pyspark.py`.

- [ ] **Step 5: motherduck — TokenAuth + URL override**

`motherduck.py`: rename → `MotherDuckBackendProfile`; `supported_auth=(TokenAuthProfile,)`; import `TokenAuthProfile`; override (scheme `"duckdb://md:"` would mangle under the base logic):
```python
    def to_url_parts(self):
        from .profile import UrlParts
        return UrlParts(scheme="md", database=getattr(self, "DATABASE", None))
```

- [ ] **Step 6: The 4 shaping backends — rename only (compose in Task 3)**

`mysql.py`/`mssql.py`/`snowflake.py`/`pyiceberg_rest.py`: apply rename + import-swap + supported_auth, and **DELETE** the `__adapter__` line + `from .adapters import X as _adapter` import. Do NOT add `__adapters__` yet.
- `mysql.py` → `MySQLBackendProfile`, `(PasswordAuthProfile,)`
- `mssql.py` → `MSSQLBackendProfile`, `(PasswordAuthProfile, WindowsAuthProfile, AzureADAuthProfile)`
- `snowflake.py` → `SnowflakeBackendProfile`, `(PasswordAuthProfile, OAuth2AuthProfile, CertificateAuthProfile, TokenAuthProfile)`
- `pyiceberg_rest.py` → `PyIcebergRestBackendProfile`, `(TokenAuthProfile,)`

- [ ] **Step 7: Delete the auth shim**
```bash
git rm core/settings/auth/__init__.py core/settings/auth/base.py core/settings/auth/dispatch.py
```

- [ ] **Step 8: Rewrite `core/settings/__init__.py`**

Replace the `from mountainash_settings.auth import (...)` block with:
```python
from mountainash_auth_client import (
    AuthProfile, NoAuthProfile, PasswordAuthProfile, TokenAuthProfile,
    JWTAuthProfile, OAuth2AuthProfile, IAMAuthProfile, WindowsAuthProfile,
    AzureADAuthProfile, KerberosAuthProfile, CertificateAuthProfile,
    ServiceAccountAuthProfile,
)
```
Change `from .profile import ConnectionProfile` → `from .profile import BackendProfile, UrlParts`. Rewrite the 20 backend imports to the new names. Rewrite `__all__`: drop every `*Auth`/`AuthSpec`/`*AuthSettings` name; add the `*AuthProfile` names + `AuthProfile`; add `"UrlParts"`; replace `"ConnectionProfile"` with `"BackendProfile"`; list the 20 `*BackendProfile` names.

- [ ] **Step 9: Write the smoke test**

`tests/test_unit/core/settings/test_settings_flip.py`:
```python
import pytest
from mountainash_auth_client import PasswordAuthProfile, NoAuthProfile
from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE as P
from mountainash_data.core.settings import (
    BackendProfile, PostgreSQLBackendProfile, MotherDuckBackendProfile,
)
from mountainash_data.core.settings.descriptor import BackendSpec


def test_supported_auth_present():
    assert PostgreSQLBackendProfile.__spec__.supported_auth == (PasswordAuthProfile, NoAuthProfile)


def test_flat_emit_is_config_only():
    out = PostgreSQLBackendProfile(HOST="db", PORT=5432, DATABASE="app").emit(P.POSTGRESQL)
    assert out["host"] == "db" and out["port"] == 5432 and out["database"] == "app"
    assert "user" not in out and "password" not in out


def test_to_url_parts_standard():
    parts = PostgreSQLBackendProfile(HOST="db", PORT=5432, DATABASE="app").to_url_parts()
    assert (parts.scheme, parts.host, parts.port, parts.database) == ("postgresql", "db", 5432, "app")


def test_motherduck_url_parts_authority_less():
    parts = MotherDuckBackendProfile(DATABASE="mydb").to_url_parts()
    assert parts.scheme == "md" and parts.host is None and parts.database == "mydb"


def test_empty_supported_auth_invariant():
    with pytest.raises(ValueError, match="supported_auth"):
        BackendSpec(name="x", provider_type=P.SQLITE, parameters=[], supported_auth=())
```

- [ ] **Step 10: Verify settings imports + smoke passes**

Run: `hatch run test:python -c "import mountainash_data.core.settings as s; print(sum(1 for n in dir(s) if n.endswith('BackendProfile')))"`
Expected: `20`.
Run: `hatch run test:test-target tests/test_unit/core/settings/test_settings_flip.py -q`
Expected: PASS.

- [ ] **Step 11: Commit**
```bash
git add -A core/settings/ tests/test_unit/core/settings/test_settings_flip.py
git commit -m "refactor(settings)!: flip to *BackendProfile + supported_auth; drop auth shim

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Config-shaping compose adapters (mysql, mssql, snowflake, pyiceberg)

**Files:**
- Rewrite: `core/settings/adapters/{mysql,mssql,snowflake,pyiceberg_rest}.py` (compose fn ONLY this task; auth fns in Task 4)
- Modify: `core/settings/{mysql,mssql,snowflake,pyiceberg_rest}.py` (wire `__adapters__`; pyiceberg `driver_key`s)
- Test: `tests/test_unit/core/settings/test_config_shaping.py`

**Interfaces:** Produces `mysql.ssl_compose`, `mssql.host_fold`, `snowflake.session_params`, `pyiceberg_rest.headers_compose` — each `(profile, base) -> dict`.

- [ ] **Step 1: Write failing goldens (full-dict equality = mechanical key-delta)**

`tests/test_unit/core/settings/test_config_shaping.py`:
```python
from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE as P
from mountainash_data.core.settings import (
    MySQLBackendProfile, MSSQLBackendProfile, SnowflakeBackendProfile,
    PyIcebergRestBackendProfile,
)


def test_mysql_ssl_compose_full_dict():
    out = MySQLBackendProfile(HOST="h", PORT=3306, SSL_CA="/ca.pem", SSL_CIPHER="HIGH").emit(P.MYSQL)
    # full equality: nested ssl ADDED, no flat ssl_* leaked, config unchanged
    assert out == {
        "host": "h", "port": 3306, "charset": "utf8mb4",
        "collation": "utf8mb4_unicode_ci", "autocommit": True,
        "ssl": {"ssl-ca": "/ca.pem", "ssl-cipher": "HIGH"},
    }


def test_mssql_host_fold_full_dict():
    out = MSSQLBackendProfile(HOST="srv", PORT=1433, INSTANCE_NAME="INST").emit(P.MSSQL)
    assert out["host"] == "srv\\INST" and "instance_name" not in out


def test_snowflake_session_parameters_added_only():
    out = SnowflakeBackendProfile(ACCOUNT="acct", QUERY_TAG="etl", TIMEZONE="UTC").emit(P.SNOWFLAKE)
    assert out["session_parameters"] == {"QUERY_TAG": "etl", "TIMEZONE": "UTC"}
    assert "query_tag" not in out and "timezone" not in out


def test_pyiceberg_headers_expand_s3_flat():
    out = PyIcebergRestBackendProfile(
        CATALOG_NAME="c", CATALOG_URI="http://x", S3_REGION="us-east-1",
        HEADERS={"X-A": "1", "X-B": "2"},
    ).emit(P.PYICEBERG_REST)
    assert out["name"] == "c" and out["uri"] == "http://x" and out["s3.region"] == "us-east-1"
    assert out["header.X-A"] == "1" and out["header.X-B"] == "2" and "headers" not in out
```
> Confirm exact flat defaults in `test_mysql_ssl_compose_full_dict` against `mysql.py` params (charset/collation/autocommit). If they differ, fix the EXPECTED dict to match the real spec — that is reading ground truth, not weakening the test.

- [ ] **Step 2: Run to verify it fails**

Run: `hatch run test:test-target tests/test_unit/core/settings/test_config_shaping.py -q`
Expected: FAIL — composes not wired.

- [ ] **Step 3: Write the compose functions**

`core/settings/adapters/mysql.py` (replace the file's old `build_driver_kwargs`):
```python
"""MySQL config-shaping adapter."""
from __future__ import annotations
import typing as t


def ssl_compose(profile: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    out = dict(base)
    if profile.SSL_MODE is not None:
        out["ssl_mode"] = str(profile.SSL_MODE)
    ssl: dict[str, str] = {}
    for key, val in {
        "ssl-key": profile.SSL_KEY, "ssl-cert": profile.SSL_CERT,
        "ssl-ca": profile.SSL_CA, "ssl-capath": profile.SSL_CAPATH,
        "ssl-cipher": profile.SSL_CIPHER,
    }.items():
        if val is not None:
            ssl[key] = str(val)
    if ssl:
        out["ssl"] = ssl
    return out
```
`core/settings/adapters/mssql.py` (compose part — auth fns appended in Task 4):
```python
"""MSSQL adapters."""
from __future__ import annotations
import typing as t


def host_fold(profile: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    out = dict(base)
    if profile.INSTANCE_NAME:
        out["host"] = f"{out['host']}\\{profile.INSTANCE_NAME}"
    if profile.ENCRYPTION is not None:
        out["encrypt"] = str(profile.ENCRYPTION)
    if profile.TRUST_SERVER_CERTIFICATE:
        out["trust_server_certificate"] = "yes"
    if profile.MARS_ENABLED:
        out["mars_connection"] = "yes"
    return out
```
`core/settings/adapters/snowflake.py` (compose part — auth fns appended in Task 4):
```python
"""Snowflake adapters."""
from __future__ import annotations
import typing as t


def session_params(profile: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    out = dict(base)
    params: dict[str, t.Any] = {}
    if profile.QUERY_TAG is not None:
        params["QUERY_TAG"] = profile.QUERY_TAG
    if profile.TIMEZONE is not None:
        params["TIMEZONE"] = profile.TIMEZONE
    if params:
        out["session_parameters"] = params
    return out
```
`core/settings/adapters/pyiceberg_rest.py` (compose part — auth fn appended in Task 4):
```python
"""PyIceberg REST adapters."""
from __future__ import annotations
import typing as t


def headers_compose(profile: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    out = dict(base)
    if profile.HEADERS:
        for hk, hv in profile.HEADERS.items():
            out[f"header.{hk}"] = hv
    return out
```

- [ ] **Step 4: Wire `__adapters__` + pyiceberg `driver_key`s**

`mysql.py`: `from .adapters import mysql as _mysql` and in the class body `__adapters__ = {CONST_DB_PROVIDER_TYPE.MYSQL: _mysql.ssl_compose}`.
`mssql.py`: `from .adapters import mssql as _mssql`; `__adapters__ = {CONST_DB_PROVIDER_TYPE.MSSQL: _mssql.host_fold}`.
`snowflake.py`: `from .adapters import snowflake as _snow`; `__adapters__ = {CONST_DB_PROVIDER_TYPE.SNOWFLAKE: _snow.session_params}`.
`pyiceberg_rest.py`: add `driver_key`s to the s3/rest params (`S3_REGION→"s3.region"`, `S3_ENDPOINT→"s3.endpoint"`, `S3_ACCESS_KEY_ID→"s3.access-key-id"`, `S3_SECRET_ACCESS_KEY→"s3.secret-access-key"` keep `secret=True`, `S3_SESSION_TOKEN→"s3.session-token"` keep `secret=True`, `REST_SIGV4_ENABLED→"rest.sigv4-enabled"`, `REST_SIGNING_REGION→"rest.signing-region"`, `REST_SIGNING_NAME→"rest.signing-name"`; `HEADERS` keeps NO driver_key), then `from .adapters import pyiceberg_rest as _ice`; `__adapters__ = {CONST_DB_PROVIDER_TYPE.PYICEBERG_REST: _ice.headers_compose}`.

- [ ] **Step 5: Run goldens**

Run: `hatch run test:test-target tests/test_unit/core/settings/test_config_shaping.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**
```bash
git add core/settings/
git commit -m "feat(settings): config-shaping compose adapters (mysql/mssql/snowflake/pyiceberg)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Auth adapter functions

**Files:**
- Create: `core/settings/adapters/sql.py`
- Append auth fns to `core/settings/adapters/{trino,snowflake,mssql,redshift,databricks,bigquery,pyiceberg_rest}.py`
- Test: `tests/test_unit/core/settings/adapters/test_auth_adapters.py`

**Interfaces:** Each is `(auth_profile, base: dict) -> dict`, returns a NEW dict, never mutates `base`. Produces: `sql.userpass`; `trino.{password,jwt,kerberos}`; `snowflake.{password,token,oauth2,certificate}`; `mssql.{password,windows,azure_ad}`; `redshift.{password,iam}`; `databricks.{token,password}`; `bigquery.service_account`; `pyiceberg_rest.token`.

> `sql.userpass` emits `{user,password}` — confirmed correct for all 9 flat backends (ibis `do_connect`); databricks uses `{username,password}` via its own adapter.

- [ ] **Step 1: Write failing tests**

`tests/test_unit/core/settings/adapters/test_auth_adapters.py`:
```python
import pytest
from mountainash_auth_client import (
    PasswordAuthProfile, TokenAuthProfile, OAuth2AuthProfile,
    CertificateAuthProfile, WindowsAuthProfile, AzureADAuthProfile,
    IAMAuthProfile, ServiceAccountAuthProfile,
)
from mountainash_data.core.settings.adapters import (
    sql as _sql, snowflake as _snow, mssql as _mssql,
    redshift as _rs, databricks as _dbx, pyiceberg_rest as _ice,
)


def test_sql_userpass():
    assert _sql.userpass(PasswordAuthProfile(USERNAME="u", PASSWORD="p"), {"host": "h"}) == {
        "host": "h", "user": "u", "password": "p"}


def test_userpass_no_mutate():
    base = {"host": "h"}
    _sql.userpass(PasswordAuthProfile(USERNAME="u", PASSWORD="p"), base)
    assert base == {"host": "h"}


def test_snowflake_token_oauth():
    assert _snow.token(TokenAuthProfile(TOKEN="t"), {}) == {"authenticator": "oauth", "token": "t"}


def test_snowflake_oauth2_token_only():
    assert _snow.oauth2(OAuth2AuthProfile(TOKEN="t"), {}) == {"authenticator": "oauth", "token": "t"}


def test_snowflake_password():
    assert _snow.password(PasswordAuthProfile(USERNAME="u", PASSWORD="p"), {}) == {"user": "u", "password": "p"}


def test_snowflake_certificate():
    assert _snow.certificate(CertificateAuthProfile(PRIVATE_KEY="KEY", PASSPHRASE="ph"), {}) == {
        "private_key": "KEY", "private_key_file_pwd": "ph"}


def test_mssql_password():
    assert _mssql.password(PasswordAuthProfile(USERNAME="u", PASSWORD="p"), {}) == {"user": "u", "password": "p"}


def test_mssql_windows():
    assert _mssql.windows(WindowsAuthProfile(USERNAME="u", DOMAIN="D"), {}) == {
        "trusted_connection": "yes", "user": "D\\u"}


def test_mssql_azure_ad_sp():
    assert _mssql.azure_ad(AzureADAuthProfile(CLIENT_ID="cid", CLIENT_SECRET="sec", TENANT_ID="t"), {}) == {
        "authentication": "ActiveDirectoryServicePrincipal", "user_id": "cid",
        "password": "sec", "tenant_id": "t"}


def test_redshift_iam():
    assert _rs.iam(IAMAuthProfile(ROLE_ARN="arn", ACCESS_KEY_ID="ak"), {}) == {
        "iam": True, "iam_role_arn": "arn", "aws_access_key_id": "ak"}


def test_databricks_token():
    assert _dbx.token(TokenAuthProfile(TOKEN="tok"), {}) == {"access_token": "tok"}


def test_pyiceberg_token():
    assert _ice.token(TokenAuthProfile(TOKEN="tok"), {"uri": "u"}) == {"uri": "u", "token": "tok"}


def test_trino_password_builds_basic_auth():
    pytest.importorskip("trino")
    from trino.auth import BasicAuthentication
    from mountainash_data.core.settings.adapters import trino as _trino
    out = _trino.password(PasswordAuthProfile(USERNAME="u", PASSWORD="p"), {"host": "h"})
    assert out["host"] == "h" and out["user"] == "u" and isinstance(out["auth"], BasicAuthentication)


def test_bigquery_service_account(monkeypatch):
    pytest.importorskip("google.oauth2")
    from google.oauth2 import service_account as _sa
    from mountainash_data.core.settings.adapters import bigquery as _bq
    sentinel = object()
    monkeypatch.setattr(_sa.Credentials, "from_service_account_info", classmethod(lambda cls, info: sentinel))
    assert _bq.service_account(ServiceAccountAuthProfile(INFO={"k": "v"}), {}) == {"credentials": sentinel}
```

- [ ] **Step 2: Run to verify it fails**

Run: `hatch run test:test-target tests/test_unit/core/settings/adapters/test_auth_adapters.py -q`
Expected: FAIL.

- [ ] **Step 3: Implement `sql.py`**
```python
"""Shared auth adapter for flat user/password SQL backends."""
from __future__ import annotations
import typing as t


def userpass(auth: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    return {**base, "user": auth.USERNAME, "password": auth.PASSWORD.get_secret_value()}
```

- [ ] **Step 4: Append auth fns to the per-backend adapter modules**

Append to `snowflake.py`:
```python
def password(auth, base):
    return {**base, "user": auth.USERNAME, "password": auth.PASSWORD.get_secret_value()}


def token(auth, base):
    return {**base, "authenticator": "oauth", "token": auth.TOKEN.get_secret_value()}


def oauth2(auth, base):
    # token-only: never reads CLIENT_ID/SECRET/SERVER_URI/SCOPE (smell #1)
    return {**base, "authenticator": "oauth", "token": auth.TOKEN.get_secret_value()}


def certificate(auth, base):
    out = dict(base)
    if auth.PRIVATE_KEY is not None:
        out["private_key"] = auth.PRIVATE_KEY.get_secret_value()
    if auth.PRIVATE_KEY_PATH is not None:
        out["private_key_file"] = str(auth.PRIVATE_KEY_PATH)
    if auth.PASSPHRASE is not None:
        out["private_key_file_pwd"] = auth.PASSPHRASE.get_secret_value()
    return out
```
Append to `mssql.py`:
```python
def password(auth, base):
    return {**base, "user": auth.USERNAME, "password": auth.PASSWORD.get_secret_value()}


def windows(auth, base):
    out = {**base, "trusted_connection": "yes"}
    if auth.DOMAIN and auth.USERNAME:
        out["user"] = f"{auth.DOMAIN}\\{auth.USERNAME}"
    elif auth.USERNAME:
        out["user"] = auth.USERNAME
    return out


def azure_ad(auth, base):
    out = dict(base)
    if auth.MANAGED_IDENTITY:
        out["authentication"] = "ActiveDirectoryMsi"
        if auth.MSI_ENDPOINT:
            out["msi_endpoint"] = auth.MSI_ENDPOINT
    else:
        out["authentication"] = "ActiveDirectoryServicePrincipal"
        if auth.CLIENT_ID:
            out["user_id"] = auth.CLIENT_ID
        if auth.CLIENT_SECRET:
            out["password"] = auth.CLIENT_SECRET.get_secret_value()
        if auth.TENANT_ID:
            out["tenant_id"] = auth.TENANT_ID
    return out
```
Append to `pyiceberg_rest.py`:
```python
def token(auth, base):
    return {**base, "token": auth.TOKEN.get_secret_value()}
```
Create/replace `redshift.py`, `databricks.py`, `trino.py`, `bigquery.py` (these are NOT shaping, so the whole file is auth fns):
```python
# redshift.py
def password(auth, base):
    return {**base, "user": auth.USERNAME, "password": auth.PASSWORD.get_secret_value()}


def iam(auth, base):
    out = {**base, "iam": True}
    if auth.ROLE_ARN is not None: out["iam_role_arn"] = auth.ROLE_ARN
    if auth.ACCESS_KEY_ID is not None: out["aws_access_key_id"] = auth.ACCESS_KEY_ID
    if auth.SECRET_ACCESS_KEY is not None: out["aws_secret_access_key"] = auth.SECRET_ACCESS_KEY.get_secret_value()
    if auth.SESSION_TOKEN is not None: out["aws_session_token"] = auth.SESSION_TOKEN.get_secret_value()
    if auth.PROFILE_NAME is not None: out["profile_name"] = auth.PROFILE_NAME
    return out
```
```python
# databricks.py
def token(auth, base):
    return {**base, "access_token": auth.TOKEN.get_secret_value()}


def password(auth, base):
    return {**base, "username": auth.USERNAME, "password": auth.PASSWORD.get_secret_value()}
```
```python
# trino.py
def password(auth, base):
    from trino.auth import BasicAuthentication
    return {**base, "user": auth.USERNAME,
            "auth": BasicAuthentication(auth.USERNAME, auth.PASSWORD.get_secret_value())}


def jwt(auth, base):
    from trino.auth import JWTAuthentication
    return {**base, "auth": JWTAuthentication(auth.TOKEN.get_secret_value())}


def kerberos(auth, base):
    from trino.auth import KerberosAuthentication
    return {**base, "auth": KerberosAuthentication(config=None, service_name=auth.SERVICE_NAME, principal=auth.PRINCIPAL)}
```
```python
# bigquery.py
def service_account(auth, base):
    from google.oauth2 import service_account as _sa
    out = dict(base)
    if auth.INFO is not None:
        out["credentials"] = _sa.Credentials.from_service_account_info(auth.INFO)
    elif auth.FILE is not None:
        out["credentials"] = _sa.Credentials.from_service_account_file(str(auth.FILE))
    return out
```
(Each file starts with `from __future__ import annotations` and `import typing as t` where types are referenced; the shaping files keep their compose fn from Task 3.)

- [ ] **Step 5: Run tests**

Run: `hatch run test:test-target tests/test_unit/core/settings/adapters/test_auth_adapters.py -q`
Expected: PASS (trino/bigquery skip without extras).

- [ ] **Step 6: Commit**
```bash
git add core/settings/adapters/ tests/test_unit/core/settings/adapters/test_auth_adapters.py
git commit -m "feat(settings): data-owned auth adapter functions

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Auth dispatch registry (MRO-aware)

**Files:** Create `core/settings/adapters/registry.py`; Test `tests/test_unit/core/settings/adapters/test_registry.py`.

**Interfaces:** Produces `auth_adapter(provider_type, auth_class) -> Callable | None` (MRO-aware; `TypeError` on sibling ambiguity); `_AUTH_ADAPTERS`.

- [ ] **Step 1: Write failing tests**

`tests/test_unit/core/settings/adapters/test_registry.py`:
```python
import pytest
from mountainash_auth_client import PasswordAuthProfile, TokenAuthProfile, NoAuthProfile
from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE as P
from mountainash_data.core.settings.adapters import sql as _sql, snowflake as _snow
from mountainash_data.core.settings.adapters.registry import auth_adapter, _AUTH_ADAPTERS


def test_exact_lookup():
    assert auth_adapter(P.SNOWFLAKE, TokenAuthProfile) is _snow.token


def test_flat_userpass_shared():
    assert auth_adapter(P.POSTGRESQL, PasswordAuthProfile) is _sql.userpass


def test_miss_returns_none():
    assert auth_adapter(P.SQLITE, PasswordAuthProfile) is None


def test_noauth_not_in_table():
    assert all(k[1] is not NoAuthProfile for k in _AUTH_ADAPTERS)


def test_subclass_resolves_to_base():
    class MyPw(PasswordAuthProfile): pass
    assert auth_adapter(P.POSTGRESQL, MyPw) is _sql.userpass


def test_specialization_wins():
    fn = lambda a, b: b
    class Special(PasswordAuthProfile): pass
    _AUTH_ADAPTERS[(P.POSTGRESQL, Special)] = fn
    try:
        assert auth_adapter(P.POSTGRESQL, Special) is fn
    finally:
        del _AUTH_ADAPTERS[(P.POSTGRESQL, Special)]


def test_sibling_ambiguity_raises():
    fn = lambda a, b: b
    _AUTH_ADAPTERS[(P.POSTGRESQL, TokenAuthProfile)] = fn
    class Hybrid(PasswordAuthProfile, TokenAuthProfile): pass
    try:
        with pytest.raises(TypeError, match="ambiguous"):
            auth_adapter(P.POSTGRESQL, Hybrid)
    finally:
        del _AUTH_ADAPTERS[(P.POSTGRESQL, TokenAuthProfile)]
```

- [ ] **Step 2: Run to verify it fails**

Run: `hatch run test:test-target tests/test_unit/core/settings/adapters/test_registry.py -q`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement `registry.py`**
```python
"""Data-owned auth dispatch: (provider_type, auth_class) -> adapter fn."""
from __future__ import annotations
import typing as t

from mountainash_auth_client import (
    PasswordAuthProfile, JWTAuthProfile, KerberosAuthProfile,
    ServiceAccountAuthProfile, IAMAuthProfile, TokenAuthProfile,
    OAuth2AuthProfile, CertificateAuthProfile, WindowsAuthProfile, AzureADAuthProfile,
)
from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE as P
from . import (sql as _sql, trino as _trino, snowflake as _snow, bigquery as _bq,
               databricks as _dbx, mssql as _mssql, redshift as _rs, pyiceberg_rest as _ice)

_AUTH_ADAPTERS: dict[tuple[t.Any, type], t.Callable[[t.Any, dict], dict]] = {
    (P.TRINO,          PasswordAuthProfile):       _trino.password,
    (P.TRINO,          JWTAuthProfile):            _trino.jwt,
    (P.TRINO,          KerberosAuthProfile):       _trino.kerberos,
    (P.SNOWFLAKE,      PasswordAuthProfile):       _snow.password,
    (P.SNOWFLAKE,      TokenAuthProfile):          _snow.token,
    (P.SNOWFLAKE,      OAuth2AuthProfile):         _snow.oauth2,
    (P.SNOWFLAKE,      CertificateAuthProfile):    _snow.certificate,
    (P.BIGQUERY,       ServiceAccountAuthProfile): _bq.service_account,
    (P.DATABRICKS,     TokenAuthProfile):          _dbx.token,
    (P.DATABRICKS,     PasswordAuthProfile):       _dbx.password,
    (P.MSSQL,          PasswordAuthProfile):       _mssql.password,
    (P.MSSQL,          WindowsAuthProfile):        _mssql.windows,
    (P.MSSQL,          AzureADAuthProfile):        _mssql.azure_ad,
    (P.REDSHIFT,       PasswordAuthProfile):       _rs.password,
    (P.REDSHIFT,       IAMAuthProfile):            _rs.iam,
    (P.PYICEBERG_REST, TokenAuthProfile):          _ice.token,
}
for _p in (P.POSTGRESQL, P.MYSQL, P.CLICKHOUSE, P.MATERIALIZE, P.RISINGWAVE,
           P.DRUID, P.SINGLESTOREDB, P.IMPALA, P.EXASOL):
    _AUTH_ADAPTERS[(_p, PasswordAuthProfile)] = _sql.userpass


def auth_adapter(provider_type: t.Any, auth_class: type) -> t.Callable[[t.Any, dict], dict] | None:
    matches = [k for k in auth_class.__mro__ if (provider_type, k) in _AUTH_ADAPTERS]
    if not matches:
        return None
    winner = matches[0]
    ambiguous = [k for k in matches[1:] if not issubclass(winner, k)]
    if ambiguous:
        raise TypeError(
            f"ambiguous auth adapter for {auth_class.__name__} on {provider_type}: "
            f"{winner.__name__} vs {[k.__name__ for k in ambiguous]} "
            f"(multiply-inherits unrelated registered auth types)"
        )
    return _AUTH_ADAPTERS[(provider_type, winner)]
```

- [ ] **Step 4: Run tests**

Run: `hatch run test:test-target tests/test_unit/core/settings/adapters/test_registry.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add core/settings/adapters/registry.py tests/test_unit/core/settings/adapters/test_registry.py
git commit -m "feat(settings): MRO-aware auth dispatch registry

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: ConnectionFactory (compose + URL + non-profile auth)

**Files:** Create `core/factories/__init__.py`, `core/factories/connection_factory.py`; Test `tests/test_unit/core/factories/test_connection_factory.py`.

**Interfaces:**
- Consumes: `auth_adapter` (Task 5); `UrlParts` (from `core.settings.profile`); a profile with `.emit(target)`, `.to_url_parts()`, `.__spec__.{provider_type,supported_auth}`, `.backend`.
- Produces:
  - `_normalize_and_validate_auth(profile, auth) -> AuthProfile`
  - `build_driver_kwargs(profile, auth_profile=None) -> dict`
  - `build_connection_string(profile, auth_profile=None) -> str`
  - `apply_auth_adapter(provider_type, base, auth_profile) -> dict` — non-profile auth application (for the ibis dialect/URL paths, no `supported_auth` to validate).
  - `provider_for_dialect(dialect) -> provider_type`, `provider_for_scheme(scheme) -> provider_type` — derived from the registered specs.

- [ ] **Step 1: Write failing tests**

`tests/test_unit/core/factories/test_connection_factory.py`:
```python
import pytest
from dataclasses import dataclass

from mountainash_auth_client import (
    PasswordAuthProfile, TokenAuthProfile, NoAuthProfile, WindowsAuthProfile,
)
from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE as P
from mountainash_data.core.settings.profile import UrlParts
from mountainash_data.core.factories.connection_factory import (
    build_driver_kwargs, build_connection_string, _normalize_and_validate_auth,
    apply_auth_adapter, provider_for_dialect,
)


@dataclass
class _Spec:
    provider_type: object
    supported_auth: tuple
    name: str = "stub"


class _Stub:
    def __init__(self, pt, sa, base, url=None):
        self.__spec__ = _Spec(pt, sa)
        self._base, self._url = base, url or UrlParts(scheme="stub", host="h", port=1, database="db")
    @property
    def backend(self): return self.__spec__.name
    def emit(self, target):
        assert target is self.__spec__.provider_type
        return dict(self._base)
    def to_url_parts(self): return self._url


def test_noauth_short_circuits():
    assert build_driver_kwargs(_Stub(P.SQLITE, (NoAuthProfile,), {"database": ":memory:"}), None) == {"database": ":memory:"}


def test_password_dispatch():
    out = build_driver_kwargs(_Stub(P.POSTGRESQL, (PasswordAuthProfile, NoAuthProfile), {"host": "h"}),
                              PasswordAuthProfile(USERNAME="u", PASSWORD="p"))
    assert out == {"host": "h", "user": "u", "password": "p"}


def test_unsupported_auth_valueerror():
    with pytest.raises(ValueError, match="does not support auth"):
        build_driver_kwargs(_Stub(P.SQLITE, (NoAuthProfile,), {}), PasswordAuthProfile(USERNAME="u", PASSWORD="p"))


def test_supported_but_no_adapter_fails_closed():
    with pytest.raises(ValueError, match="no auth adapter"):
        build_driver_kwargs(_Stub(P.POSTGRESQL, (WindowsAuthProfile,), {"host": "h"}), WindowsAuthProfile(USERNAME="u"))


def test_none_normalizes_when_supported():
    assert isinstance(_normalize_and_validate_auth(_Stub(P.SQLITE, (NoAuthProfile,), {}), None), NoAuthProfile)


def test_none_rejected_when_noauth_unsupported():
    with pytest.raises(ValueError, match="does not support auth"):
        _normalize_and_validate_auth(_Stub(P.MYSQL, (PasswordAuthProfile,), {}), None)


def test_apply_auth_adapter_non_profile():
    out = apply_auth_adapter(P.POSTGRESQL, {"host": "h"}, PasswordAuthProfile(USERNAME="u", PASSWORD="p"))
    assert out == {"host": "h", "user": "u", "password": "p"}
    assert apply_auth_adapter(P.POSTGRESQL, {"host": "h"}, None) == {"host": "h"}


def test_provider_for_dialect():
    assert provider_for_dialect("postgres") is P.POSTGRESQL


def test_url_password():
    s = _Stub(P.POSTGRESQL, (PasswordAuthProfile,), {}, url=UrlParts(scheme="postgresql", host="db", port=5432, database="app"))
    assert build_connection_string(s, PasswordAuthProfile(USERNAME="u", PASSWORD="p@s")) == "postgresql://u:p%40s@db:5432/app"


def test_url_token_authority_less():
    s = _Stub(P.MOTHERDUCK, (TokenAuthProfile,), {}, url=UrlParts(scheme="md", database="mydb"))
    assert build_connection_string(s, TokenAuthProfile(TOKEN="T")) == "md:mydb?motherduck_token=T"


@pytest.mark.parametrize("auth", [WindowsAuthProfile(USERNAME="u"), TokenAuthProfile(TOKEN="T")])
def test_url_unsupported_auth_not_implemented(auth):
    s = _Stub(P.POSTGRESQL, (type(auth),), {}, url=UrlParts(scheme="postgresql", host="db"))
    with pytest.raises(NotImplementedError):
        build_connection_string(s, auth)
```

- [ ] **Step 2: Run to verify it fails**

Run: `hatch run test:test-target tests/test_unit/core/factories/test_connection_factory.py -q`
Expected: FAIL — module missing.

- [ ] **Step 3: Create `core/factories/__init__.py`**
```python
"""Factories that compose backend config + auth into runtime kwargs."""
```

- [ ] **Step 4: Implement `connection_factory.py`**
```python
"""ConnectionFactory: compose BackendProfile config + AuthProfile creds."""
from __future__ import annotations
import typing as t
from urllib.parse import quote

from mountainash_auth_client import NoAuthProfile, PasswordAuthProfile, TokenAuthProfile, AuthProfile
from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE as P
from mountainash_data.core.settings.profile import UrlParts
from mountainash_data.core.settings.adapters.registry import auth_adapter


def _iter_specs() -> t.Iterator[t.Any]:
    """Registered BackendSpecs, regardless of whether the registry stores
    specs or profile classes."""
    from mountainash_data.core.settings.registry import REGISTRY
    for v in REGISTRY.values():
        yield v.__spec__ if hasattr(v, "__spec__") else v


def provider_for_dialect(dialect: str) -> t.Any:
    for spec in _iter_specs():
        if getattr(spec, "ibis_dialect", None) == dialect:
            return spec.provider_type
    raise KeyError(f"no provider_type for ibis dialect {dialect!r}")


def provider_for_scheme(scheme: str) -> t.Any:
    norm = scheme.rstrip(":/")
    for spec in _iter_specs():
        s = getattr(spec, "connection_string_scheme", None)
        if s and s.rstrip(":/") == norm:
            return spec.provider_type
    raise KeyError(f"no provider_type for URL scheme {scheme!r}")


def _normalize_and_validate_auth(profile: t.Any, auth_profile: AuthProfile | None) -> AuthProfile:
    auth = NoAuthProfile() if auth_profile is None else auth_profile
    if not isinstance(auth, tuple(profile.__spec__.supported_auth)):
        raise ValueError(f"{profile.backend} does not support auth: {type(auth).__name__}")
    return auth


def apply_auth_adapter(provider_type: t.Any, base: dict, auth_profile: AuthProfile | None) -> dict:
    """Apply auth WITHOUT a profile (ibis dialect / URL paths). No supported_auth gate."""
    if auth_profile is None or isinstance(auth_profile, NoAuthProfile):
        return base
    fn = auth_adapter(provider_type, type(auth_profile))
    if fn is None:
        raise ValueError(f"{provider_type}: no auth adapter for {type(auth_profile).__name__}")
    return fn(auth_profile, base)


def build_driver_kwargs(profile: t.Any, auth_profile: AuthProfile | None = None) -> dict:
    auth = _normalize_and_validate_auth(profile, auth_profile)
    target = profile.__spec__.provider_type
    base = profile.emit(target)
    if isinstance(auth, NoAuthProfile):
        return base
    return apply_auth_adapter(target, base, auth)


# --- URL appliers (L3 for the URL target) ---------------------------------

def _url_password(parts: UrlParts, auth: t.Any) -> str:
    if parts.host is None:
        raise NotImplementedError("password URL form requires a host authority")
    user, pw = quote(str(auth.USERNAME), safe=""), quote(auth.PASSWORD.get_secret_value(), safe="")
    url = f"{parts.scheme}://{user}:{pw}@{parts.host}"
    if parts.port is not None: url += f":{parts.port}"
    if parts.database is not None: url += f"/{parts.database}"
    return url


def _url_noauth(parts: UrlParts) -> str:
    url = parts.scheme + "://"
    if parts.host is not None:
        url += parts.host + (f":{parts.port}" if parts.port is not None else "")
    if parts.database is not None: url += f"/{parts.database}"
    return url


def _url_motherduck_token(parts: UrlParts, auth: t.Any) -> str:
    return f"{parts.scheme}:{parts.database}?motherduck_token={auth.TOKEN.get_secret_value()}"


_URL_APPLIERS: dict[t.Any, dict[type, t.Callable]] = {
    P.MOTHERDUCK: {TokenAuthProfile: _url_motherduck_token},
}


def build_connection_string(profile: t.Any, auth_profile: AuthProfile | None = None) -> str:
    auth = _normalize_and_validate_auth(profile, auth_profile)
    parts = profile.to_url_parts()                       # L1
    if isinstance(auth, NoAuthProfile):
        return _url_noauth(parts)
    if isinstance(auth, PasswordAuthProfile):
        return _url_password(parts, auth)                # L3
    applier = _URL_APPLIERS.get(profile.__spec__.provider_type, {}).get(type(auth))
    if applier is None:
        raise NotImplementedError(f"{profile.backend}: no URL form for {type(auth).__name__}")
    return applier(parts, auth)
```
> Confirm the registry accessor name in `core/settings/registry.py` (`REGISTRY` vs `DATABASES_REGISTRY`) and adjust `_iter_specs`. The `hasattr(v, "__spec__")` branch handles either specs or classes.

- [ ] **Step 5: Run tests**

Run: `hatch run test:test-target tests/test_unit/core/factories/test_connection_factory.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**
```bash
git add core/factories/ tests/test_unit/core/factories/test_connection_factory.py
git commit -m "feat(factories): ConnectionFactory compose, URL appliers, non-profile auth

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Ibis entry points (deferred auth across all three paths)

**Files:** Modify `backends/ibis/backend.py`; Test `tests/test_unit/backends/ibis/test_backend_auth.py`.

**Interfaces:** Produces `IbisBackend.connect(self, auth_profile=None)` applying auth on the **settings**, **direct-dialect**, and **URL** paths; fail-closed URL-creds-vs-explicit precedence.

- [ ] **Step 1: Write failing tests**

`tests/test_unit/backends/ibis/test_backend_auth.py`:
```python
import pytest
from mountainash_auth_client import NoAuthProfile, PasswordAuthProfile
from mountainash_data.backends.ibis.backend import IbisBackend


def test_sqlite_dialect_connect_noauth(tmp_path):
    be = IbisBackend(dialect="sqlite", database=str(tmp_path / "t.db")).connect(auth_profile=NoAuthProfile())
    assert be is not None


def test_dialect_path_applies_password(monkeypatch):
    # direct-dialect + explicit auth: auth adapter must run for the dialect's provider.
    seen = {}
    import mountainash_data.backends.ibis.backend as mod
    def fake_apply(pt, base, auth):
        seen["pt"], seen["auth"] = pt, auth
        return {**base, "user": auth.USERNAME}
    monkeypatch.setattr(mod, "apply_auth_adapter", fake_apply)
    monkeypatch.setattr(mod, "provider_for_dialect", lambda d: "PG")
    IbisBackend(dialect="postgres", host="h", database="db")._resolve_dialect_auth(
        PasswordAuthProfile(USERNAME="u", PASSWORD="p")
    )
    assert seen["pt"] == "PG" and seen["auth"].USERNAME == "u"


def test_url_and_explicit_auth_conflict_raises():
    with pytest.raises(ValueError, match="both"):
        IbisBackend("postgresql://u:p@host/db").connect(
            auth_profile=PasswordAuthProfile(USERNAME="x", PASSWORD="y"))
```
> The settings-path end-to-end (`SettingsParameters` → `connect(auth_profile=...)`) is added in Task 9 with the migrated fixtures.

- [ ] **Step 2: Run to verify it fails**

Run: `hatch run test:test-target tests/test_unit/backends/ibis/test_backend_auth.py -q`
Expected: FAIL — `connect()` takes no `auth_profile`; `_resolve_dialect_auth` missing.

- [ ] **Step 3: Defer config build in `_init_from_settings`**

In `backends/ibis/backend.py`, DELETE the eager `driver_kwargs = obj_settings.to_driver_kwargs()` (line ~242). Store the profile + extras instead:
```python
    self.dialect = resolved_dialect
    self._spec = DIALECTS[resolved_dialect]
    self._url = None
    self._profile = obj_settings        # settings path
    self._extra_config = config         # caller **config overrides
    self._config = None
    self._conn = None
```
In the direct-dialect path (`_init_from_dialect`), set `self._profile = None`, `self._url = None`, `self._dialect_config = config`, `self._config = None`. In the URL path set `self._profile = None`, keep `self._url = <url>`, `self._config = None`.

- [ ] **Step 4: Add imports + helpers + thread `connect`**
```python
from mountainash_data.core.factories.connection_factory import (
    build_driver_kwargs, apply_auth_adapter, provider_for_dialect, provider_for_scheme,
)
from mountainash_auth_client import PasswordAuthProfile
from urllib.parse import urlsplit, urlunsplit, unquote


def connect(self, auth_profile=None):
    if self._conn is not None:
        return self
    if self._profile is not None:                                   # settings path
        cfg = build_driver_kwargs(self._profile, auth_profile)
        cfg.update(self._extra_config)
        self._config = cfg
    elif self._url is not None:                                     # URL path
        self._config, self._url = self._resolve_url_auth(self._url, auth_profile)
    else:                                                           # direct-dialect path
        self._config = self._resolve_dialect_auth(auth_profile)
    # ...existing connection_builder / ibis.connect(self._url, **self._config) logic...
    return self


def _resolve_dialect_auth(self, auth_profile):
    base = dict(self._dialect_config)
    if auth_profile is None:
        return base
    provider = provider_for_dialect(self.dialect)
    return apply_auth_adapter(provider, base, auth_profile)


def _resolve_url_auth(self, url, auth_profile):
    parts = urlsplit(url)
    has_url_creds = bool(parts.username)
    if has_url_creds and auth_profile is not None:
        raise ValueError("both URL credentials and an explicit auth_profile given")
    config: dict = {}
    clean = url
    if has_url_creds:
        netloc = parts.hostname or ""
        if parts.port: netloc += f":{parts.port}"
        clean = urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
        auth_profile = PasswordAuthProfile(
            USERNAME=unquote(parts.username),
            PASSWORD=unquote(parts.password) if parts.password else "",
        )
    if auth_profile is not None:
        provider = provider_for_scheme(parts.scheme)
        config = apply_auth_adapter(provider, config, auth_profile)
    return config, clean
```
> The existing `connect` body that reads `self._config`/`self._url`/`self._spec.connection_builder` runs UNCHANGED after `self._config` is set above. Confirm no code path reads `self._config` before `connect()` (it is now `None` until `connect`).

- [ ] **Step 5: Run tests**

Run: `hatch run test:test-target tests/test_unit/backends/ibis/test_backend_auth.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**
```bash
git add backends/ibis/backend.py tests/test_unit/backends/ibis/test_backend_auth.py
git commit -m "feat(ibis): deferred auth across settings/dialect/URL paths

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Iceberg auth threading (testable kwargs helper)

**Files:** Modify `backends/iceberg/connection.py`; Test `tests/test_unit/backends/iceberg/test_iceberg_auth.py`.

**Interfaces:** Produces `_build_catalog_kwargs(self, auth_profile, **kwargs) -> dict` (no pyiceberg import — real gate); `connect_default(self, *, auth_profile=None, **kwargs)` and `connect(..., auth_profile=None)` threading auth; precedence **profile-derived < explicit `**kwargs`**.

- [ ] **Step 1: Write failing test (no live catalog needed)**

`tests/test_unit/backends/iceberg/test_iceberg_auth.py`:
```python
from types import SimpleNamespace
from unittest.mock import patch
from mountainash_auth_client import TokenAuthProfile
from mountainash_data.backends.iceberg.connection import IcebergConnectionBase  # confirm exact name


def test_build_catalog_kwargs_threads_auth_and_merges():
    obj_settings = object()
    # plain stubs — no property/attribute conflict: get_settings returns obj_settings
    params = SimpleNamespace(
        settings_class=SimpleNamespace(get_settings=lambda settings_parameters: obj_settings)
    )
    conn = IcebergConnectionBase.__new__(IcebergConnectionBase)
    conn.db_auth_settings_parameters = params

    auth = TokenAuthProfile(TOKEN="T")
    with patch(
        "mountainash_data.backends.iceberg.connection.build_driver_kwargs",
        return_value={"uri": "http://x", "token": "T", "name": "c"},
    ) as bk:
        out = conn._build_catalog_kwargs(auth, warehouse="w")

    bk.assert_called_once_with(obj_settings, auth)   # profile + auth_profile threaded
    assert out["warehouse"] == "w"                   # explicit kwargs win
    assert out["uri"] == "http://x"
```
> Confirm the real `IcebergConnectionBase` class/attribute names at implementation time (the `.db_auth_settings_parameters` + `.settings_class.get_settings(...)` shape is from the current `connect_default`); the load-bearing assertions (profile+auth threaded; explicit kwargs win) stay. `build_driver_kwargs` is patched at the name bound INSIDE `connection.py`, not at its definition site.

- [ ] **Step 2: Run to verify it fails**

Run: `hatch run test:test-target tests/test_unit/backends/iceberg/test_iceberg_auth.py -q`
Expected: FAIL — `_build_catalog_kwargs` missing.

- [ ] **Step 3: Extract the kwargs helper + thread auth**

Replace line ~112's `connection_kwargs = obj_settings.to_driver_kwargs()` path:
```python
from mountainash_data.core.factories.connection_factory import build_driver_kwargs


def _build_catalog_kwargs(self, auth_profile=None, **kwargs):
    settings_class = self.db_auth_settings_parameters.settings_class
    if settings_class is None:
        raise ValueError("Settings class is required for the database connection")
    obj_settings = settings_class.get_settings(settings_parameters=self.db_auth_settings_parameters)
    connection_kwargs = build_driver_kwargs(obj_settings, auth_profile)
    connection_kwargs.update(kwargs)                 # explicit caller kwargs win
    return connection_kwargs


def connect_default(self, *, auth_profile=None, **kwargs):
    if self.catalog_backend is None:
        connection_kwargs = self._build_catalog_kwargs(auth_profile, **kwargs)
        from pyiceberg.catalog.rest import RestCatalog
        self._catalog_backend = RestCatalog(**connection_kwargs)
    return self.catalog_backend


def connect(self, connection_string=None, connection_kwargs=None, *, auth_profile=None, **kwargs):
    if self.catalog_backend is None:
        self.connect_default(auth_profile=auth_profile, **(connection_kwargs or {}), **kwargs)
    return self.catalog_backend
```
Document the precedence in both docstrings.

- [ ] **Step 4: Run test**

Run: `hatch run test:test-target tests/test_unit/backends/iceberg/test_iceberg_auth.py -q`
Expected: PASS (no pyiceberg needed — `RestCatalog` import is inside `connect_default`, not reached by the helper test).

- [ ] **Step 5: Commit**
```bash
git add backends/iceberg/connection.py tests/test_unit/backends/iceberg/test_iceberg_auth.py
git commit -m "feat(iceberg): thread auth via testable _build_catalog_kwargs

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: Migrate existing tests + consistency goldens + green gate

**Files:** Modify `tests/fixtures/settings_fixtures.py` + the ~25 test files; Create `tests/test_unit/core/settings/test_supported_auth_consistency.py`, `tests/test_unit/core/factories/test_url_consistency.py`; full-suite gate.

- [ ] **Step 1: Migrate fixtures**

`tests/fixtures/settings_fixtures.py`: replace `NoAuth`→`NoAuthProfile`, `SQLiteAuthSettings`→`SQLiteBackendProfile`, `DuckDBAuthSettings`→`DuckDBBackendProfile`. DELETE every `"auth": NoAuth()` from `kwargs={...}` (auth is no longer a profile field). Where a test needs auth, yield `(backend_profile, auth_profile)` pairs.

- [ ] **Step 2: Migrate per-backend tests (mechanical)**

For each `tests/test_unit/core/settings/backends/test_<backend>.py`: imports → `<X>BackendProfile` + `*AuthProfile`; construction → `<X>BackendProfile(...)` (no `auth=`), UPPERCASE auth kwargs with plain strings (`PasswordAuthProfile(USERNAME="u", PASSWORD="p")` — pydantic wraps secrets); replace `s.to_driver_kwargs()` with `build_driver_kwargs(s, <auth>)` from `mountainash_data.core.factories.connection_factory`.

Worked example — `test_postgresql.py`:
```python
from mountainash_auth_client import PasswordAuthProfile
from mountainash_data.core.settings import PostgreSQLBackendProfile
from mountainash_data.core.factories.connection_factory import build_driver_kwargs


def test_postgres_driver_kwargs():
    s = PostgreSQLBackendProfile(HOST="db", DATABASE="app")
    out = build_driver_kwargs(s, PasswordAuthProfile(USERNAME="u", PASSWORD="p"))
    assert out["host"] == "db" and out["user"] == "u" and out["password"] == "p"
```

- [ ] **Step 3: supported_auth ↔ table consistency**

`tests/test_unit/core/settings/test_supported_auth_consistency.py`:
```python
from mountainash_auth_client import NoAuthProfile
from mountainash_data.core.settings.adapters.registry import auth_adapter
from mountainash_data.core.factories.connection_factory import _iter_specs


def test_every_supported_pair_has_an_adapter():
    for spec in _iter_specs():
        for auth_cls in spec.supported_auth:
            if auth_cls is NoAuthProfile:
                continue
            assert auth_adapter(spec.provider_type, auth_cls) is not None, (
                f"{spec.name}: supported {auth_cls.__name__} has no adapter"
            )
```
> Uses `_iter_specs()` (Task 6) which normalises the registry; this is a structural invariant, NOT a count assertion.

- [ ] **Step 4: URL applier coverage**

`tests/test_unit/core/factories/test_url_consistency.py`:
```python
import pytest
from mountainash_auth_client import PasswordAuthProfile, TokenAuthProfile
from mountainash_data.core.settings import PostgreSQLBackendProfile, MotherDuckBackendProfile
from mountainash_data.core.factories.connection_factory import build_connection_string


def test_postgres_password_url():
    s = PostgreSQLBackendProfile(HOST="db", PORT=5432, DATABASE="app")
    assert build_connection_string(s, PasswordAuthProfile(USERNAME="u", PASSWORD="p@s")) == "postgresql://u:p%40s@db:5432/app"


def test_motherduck_token_url():
    assert build_connection_string(MotherDuckBackendProfile(DATABASE="mydb"), TokenAuthProfile(TOKEN="T")) == "md:mydb?motherduck_token=T"


def test_snowflake_token_url_not_implemented():
    # snowflake supports TokenAuthProfile for kwargs but has no URL form → fail-closed
    from mountainash_data.core.settings import SnowflakeBackendProfile
    with pytest.raises(NotImplementedError):
        build_connection_string(SnowflakeBackendProfile(ACCOUNT="a"), TokenAuthProfile(TOKEN="T"))
```

- [ ] **Step 5: Migrate remaining unit/integration tests**

`tests/test_integration/test_end_to_end_workflows.py`, `tests/test_unit/backends/ibis/test_backend.py`, `tests/test_unit/core/settings/test_{descriptor,profile,registry}.py`, `tests/test_unit/databases/settings/test_settings_parametrized.py`: swap to new names; move any `auth=` on a profile to the `connect(auth_profile=...)` / `build_driver_kwargs(profile, auth)` call. Add the settings-path ibis auth test deferred from Task 7 (a `SQLiteBackendProfile` via `SettingsParameters` through `connect(auth_profile=NoAuthProfile())`).

- [ ] **Step 6: Full suite**

Run: `hatch run test:test`
Expected: PASS (driver-gated tests skip without extras). Root-cause any failure — never silence. If a flat backend's `{user,password}` golden disagrees, STOP and surface (test-integrity).

- [ ] **Step 7: Type + lint gate**

Run: `hatch run mypy:check`
Run: `hatch run ruff:check`
Expected: both clean.

- [ ] **Step 8: Commit**
```bash
git add -A tests/
git commit -m "test: migrate suite to *BackendProfile + factory; add consistency goldens

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

**1. Spec coverage** — §3.1 rename → T2; §3.2 decouple → T6,7,8; §3.3 emit + 4-backend shaping → T2,T3; §3.4 MRO dispatch → T4,T5; §3.5 factory fail-closed → T6; §4.3 supported_auth+invariant → T2; §4.4 BackendProfile/UrlParts/to_url_parts → T2; §4.6 two-layer URL → T6; §4.8 deferred auth + URL precedence (all 3 paths) → T7; iceberg → T8; §4.9 deps → T1; §5 TOKEN-only OAuth2 + iceberg token-only → T4,T2; §6 validation/fail-closed/consistency → T6,T9; §7 testing → T2–T9; §10 deferred → out of scope, preserved. ✓

**2. Placeholder scan** — T7/T8 settings-path/ctor specifics are confirmed at implementation against real names; the load-bearing assertions are concrete. Two VERIFY callouts (registry accessor in T6; flat `{user,password}` goldens in T9) are test-integrity-gated (surface, don't guess), not placeholders.

**3. Type consistency** — `*BackendProfile`, `build_driver_kwargs(profile, auth_profile=None)`, `apply_auth_adapter(provider_type, base, auth_profile)`, `auth_adapter(provider_type, auth_class)`, `UrlParts(...)`, compose `(profile, base)→dict`, auth `(auth, base)→dict`, `provider_for_dialect/scheme` used identically across tasks. `UrlParts` defined once in `core/settings/profile.py`, imported by the factory.

---

## Execution Handoff

Verify-at-implementation points (all test-integrity-gated): (1) registry accessor name (`REGISTRY`/`DATABASES_REGISTRY`) in `_iter_specs` (T6); (2) flat `{user,password}` goldens (T9); (3) real `IcebergConnectionBase` ctor/attr names (T8); (4) `ProfileSpec.__post_init__` presence (T2). PR-time flag: `core/factories/` also exists on the `settings-registry` worktree branch — watch for merge conflict.
