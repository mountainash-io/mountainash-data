# Settings Registry Redesign — Design Spec

**Date:** 2026-04-15
**Scope:** `src/mountainash_data/core/settings/` — the 11 per-backend settings classes + their shared base.
**Input from:** `docs/superpowers/specs/2026-04-15-settings-audit/` (per-backend findings, cross-cutting patterns).
**Goal:** Replace the current class-per-backend, method-heavy settings hierarchy with a declarative descriptor + thin subclass pattern. Preserve pydantic typing and `MountainAshBaseSettings` integration. Carry forward all audit fixes in the same pass.

---

## Problem statement

The audit surfaced three structural issues that sit underneath most of the per-backend bugs:

1. **Boilerplate.** Every settings class repeats the same four methods (`get_connection_string_template`, `get_connection_string_params`, `get_connection_kwargs`, `get_post_connection_options`), the same `__init__` passthrough, the same `db_provider_type` property, and a hand-rolled `_post_init`. The repeated code is where the copy-paste bugs live (postgres and mysql both return `BIGQUERY`; pyspark's docstring says "SQLite authentication settings").

2. **Base-class leakage.** `BaseDBAuthSettings` declares `HOST`/`PORT`/`DATABASE`/`SCHEMA`/`USERNAME`/`PASSWORD`/`TOKEN`/`AUTH_METHOD` as inherited fields. These are meaningless for SQLite, DuckDB, PySpark, BigQuery, MotherDuck, PyIceberg REST — but they're silently accepted as config keys and can't be rejected. Every per-backend audit flagged this as noise.

3. **Polymorphic auth is flattened.** Every backend with multiple auth modes (Snowflake, MSSQL, Redshift, Trino, PyIceberg REST) puts *every* possible auth field on the class as `Optional[...]` and uses an `AUTH_METHOD` string selector plus hand-rolled model validators to enforce "if method X then fields Y required." The result: users can set `PRIVATE_KEY` alongside `AUTH_METHOD=password` with no type error, and the same auth shape (OAuth2 client-credentials; password+SecretStr) is redefined in three or four places.

The current design also makes it hard to generate documentation, produce tier-filtered views (core vs. advanced), or introspect what a backend supports — because the knowledge is distributed across Python class bodies rather than collected in data.

## Goals

- Eliminate the four `get_*` methods from the per-backend surface — they become responsibility of the generic base.
- Represent each backend as **data** (a `BackendDescriptor`) plus **optional code** (a small adapter for composite driver mapping).
- Model auth as a **pydantic discriminated union** of typed `AuthSpec` subclasses, reused across backends.
- Drop `HOST`/`PORT`/…/`AUTH_METHOD` base-class leakage. Backends declare what they need; backends that don't need those fields don't get them.
- Carry forward every applicable audit fix in the same refactor: broken wiring, wrong types, unused enums, stringly-typed fields, missing parameters.
- Preserve stable import paths (`from ...settings import PostgreSQLAuthSettings`) so downstream call sites that *do* import by name keep working.
- Preserve `MountainAshBaseSettings` integration (config-file loading + `SettingsParameters`).

## Non-goals

- Rewriting factories, `DatabaseUtils`, or the `backends/ibis/iceberg` layers beyond updating their call sites to the new profile API.
- Adding new backends beyond the 11 currently audited.
- Adding new auth modes beyond what the 11 backends actually use.
- Runtime introspection against live driver modules (e.g. walking `RestCatalog.__init__` for parameters) — that's a separate initiative.
- Backward compatibility with the four retired `get_*` methods or with `DBAuthValidationError`. Callers of those (factories, `DatabaseUtils`, backend connection classes) are updated in the same change set.

---

## Architecture

Layers, outside-in:

1. **`ConnectionProfile`** — subclass of `MountainAshBaseSettings`. Defines the uniform public API: `profile.to_driver_kwargs()`, `profile.to_connection_string()`, `profile.backend`, `profile.auth`. No backend-specific methods.

2. **`BackendDescriptor`** — immutable dataclass per backend, registered in a module-level `REGISTRY`. Carries `name`, `provider_type`, `default_port`, `parameters: list[ParameterSpec]`, `auth_modes: list[type[AuthSpec]]`, `connection_string_scheme`, `ibis_dialect`, optional `rides_on` (for MotherDuck → duckdb, Redshift → postgres).

3. **`ParameterSpec`** — describes one field. Covers the 80% case (name, type, tier, default, optional `driver_key` for 1:1 driver mapping, optional `transform`, `secret` flag, optional field-level validator) without needing any adapter code.

4. **`AuthSpec` hierarchy** — a family of small pydantic models, each with a `kind` discriminator literal: `NoAuth`, `PasswordAuth`, `TokenAuth`, `JWTAuth`, `OAuth2Auth`, `ServiceAccountAuth`, `IAMAuth`, `WindowsAuth`, `AzureADAuth`, `KerberosAuth`, `CertificateAuth`. Reused across backends.

5. **Adapter module (optional per backend)** — `adapters/<backend>.py` exporting `build_driver_kwargs(profile) -> dict`. Handles composite mappings that don't fit `ParameterSpec`: `trino.auth.BasicAuthentication` wrapper, mysqlclient's nested `ssl={}` dict, BigQuery's SA-info → `Credentials` conversion, Redshift endpoint resolution, Snowflake `session_parameters` merge, PyIceberg REST `s3.*` key prefixing. Backends without composite needs have no adapter module.

6. **Thin subclass per backend** — one file per backend (`postgresql.py`, `snowflake.py`, etc.) containing the `BackendDescriptor`, any backend-specific enums, an optional adapter function, and a two-line subclass shell.

### Data flow

```
user kwargs / config file
        ↓
  *AuthSettings subclass        ← BackendDescriptor
  (pydantic validation, auth
   discrimination, SecretStr)
        ↓  .to_driver_kwargs()
  ConnectionProfile._default_driver_kwargs()   (1:1 mappings from descriptor)
        ↓
  AUTH_TO_DRIVER_KWARGS[type(auth)](auth, descriptor)   (auth dispatch)
        ↓
  adapter.build_driver_kwargs(profile)   (composite overrides, if any)
        ↓
  driver / Ibis do_connect(**kwargs)
```

---

## The data contract

### `ParameterSpec`

```python
@dataclass(frozen=True, kw_only=True)
class ParameterSpec:
    name: str                                       # settings-facing name, e.g. "SSL_CERT"
    type: type | TypeAlias                          # pydantic-compatible annotation
    tier: Literal["core", "advanced"]
    default: Any = MISSING                          # MISSING → Field(...) (required)
    description: str = ""
    driver_key: str | None = None                   # driver kwarg name, e.g. "sslcert"; None = adapter-only
    secret: bool = False                            # wrap as SecretStr, auto-unwrap at kwargs boundary
    transform: Callable[[Any], Any] | None = None   # inline 1:1 transform (Path→str, bool→"0"/"1", ...)
    validator: Callable[[Any], Any] | None = None   # pydantic-style field validator
```

`MISSING` is a module-level sentinel. When `default is MISSING`, the base emits `Field(...)` (required). Otherwise `Field(default=...)`. Setting `secret=True` automatically wraps the declared type as `SecretStr` and arranges for `.get_secret_value()` to be called at the `to_driver_kwargs` boundary — fixes the inconsistent secret handling flagged in the audit.

### `AuthSpec` hierarchy

Discriminated-union members, each in its own small module under `settings/auth/`:

```python
class AuthSpec(BaseModel):
    kind: str                                   # discriminator

class NoAuth(AuthSpec):             kind: Literal["none"] = "none"
class PasswordAuth(AuthSpec):       kind: Literal["password"] = "password"
                                    username: str
                                    password: SecretStr
class TokenAuth(AuthSpec):          kind: Literal["token"] = "token"
                                    token: SecretStr
class JWTAuth(AuthSpec):            kind: Literal["jwt"] = "jwt"
                                    token: SecretStr
class OAuth2Auth(AuthSpec):         kind: Literal["oauth2"] = "oauth2"
                                    client_id: str | None = None
                                    client_secret: SecretStr | None = None
                                    token: SecretStr | None = None
                                    refresh_token: SecretStr | None = None
                                    server_uri: str | None = None
                                    scope: str | None = None
class ServiceAccountAuth(AuthSpec): kind: Literal["service_account"] = "service_account"
                                    info: dict | None = None
                                    file: Path | None = None
class IAMAuth(AuthSpec):            kind: Literal["iam"] = "iam"
                                    role_arn: str | None = None
                                    access_key_id: str | None = None
                                    secret_access_key: SecretStr | None = None
                                    session_token: SecretStr | None = None
                                    profile_name: str | None = None
class WindowsAuth(AuthSpec):        kind: Literal["windows"] = "windows"
                                    username: str | None = None
                                    domain: str | None = None
class AzureADAuth(AuthSpec):        kind: Literal["azure_ad"] = "azure_ad"
                                    tenant_id: str | None = None
                                    client_id: str | None = None
                                    client_secret: SecretStr | None = None
                                    managed_identity: bool = False
                                    msi_endpoint: str | None = None
class KerberosAuth(AuthSpec):       kind: Literal["kerberos"] = "kerberos"
                                    service_name: str = "postgres"
                                    principal: str | None = None
                                    keytab: Path | None = None
class CertificateAuth(AuthSpec):    kind: Literal["certificate"] = "certificate"
                                    private_key: SecretStr | None = None
                                    private_key_path: Path | None = None
                                    passphrase: SecretStr | None = None
```

Backends declare which auth modes they accept via `BackendDescriptor.auth_modes`. Pydantic composes these into a discriminated union for the profile's `auth` field — wrong fields for the wrong mode become construction-time validation errors, not runtime surprises.

**Backend → auth mapping (derived from the audit):**

| Backend | Auth modes |
|---|---|
| sqlite, duckdb, pyspark | `NoAuth` |
| motherduck | `TokenAuth` |
| postgresql | `PasswordAuth`, `NoAuth` |
| mysql | `PasswordAuth` |
| mssql | `PasswordAuth`, `WindowsAuth`, `AzureADAuth` |
| snowflake | `PasswordAuth`, `OAuth2Auth`, `CertificateAuth`, `TokenAuth` |
| bigquery | `ServiceAccountAuth`, `NoAuth` (ADC) |
| redshift | `PasswordAuth`, `IAMAuth` |
| trino | `PasswordAuth`, `JWTAuth`, `KerberosAuth`, `OAuth2Auth`, `NoAuth` |
| pyiceberg_rest | `TokenAuth`, `OAuth2Auth` |

### `BackendDescriptor`

```python
@dataclass(frozen=True, kw_only=True)
class BackendDescriptor:
    name: str                                       # "postgresql", "pyiceberg_rest", ...
    provider_type: CONST_DB_PROVIDER_TYPE           # authoritative provider identifier
    default_port: int | None = None
    parameters: list[ParameterSpec]
    auth_modes: list[type[AuthSpec]]
    connection_string_scheme: str | None = None     # "postgresql://", "bigquery://", ...; None if N/A
    ibis_dialect: str | None = None                 # "postgres", "duckdb", None (for pyiceberg_rest)
    rides_on: str | None = None                     # metadata only: "duckdb" for motherduck, "postgres" for redshift
```

`rides_on` is **metadata only** — it records that the backend routes through another backend's Ibis `do_connect()` (motherduck→duckdb, redshift→postgres). It has no runtime behavior: each backend still has its own descriptor and own `to_driver_kwargs()`. Consumers that care (e.g., audit tooling, docs generation) can read the field.

`connection_string_scheme` may be `None` for backends that don't use a URL form (e.g., `pyspark` uses a `SparkSession`; `pyiceberg_rest` takes `uri=` as a kwarg). When `None`, the base `to_connection_string()` raises `NotImplementedError` — backends that need URL construction override the method on their subclass.

---

## `ConnectionProfile` base

```python
class ConnectionProfile(MountainAshBaseSettings):
    __descriptor__: ClassVar[BackendDescriptor]
    __adapter__:    ClassVar[Callable[["ConnectionProfile"], dict] | None] = None

    auth: AuthSpec                                  # union; pydantic discriminator="kind"

    @property
    def backend(self) -> str:
        return self.__descriptor__.name

    @property
    def provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        return self.__descriptor__.provider_type

    def _default_driver_kwargs(self) -> dict:
        """1:1 driver mappings from the descriptor.
        Skip None values. Unwrap SecretStr. Apply ParameterSpec.transform."""
        ...

    def _auth_to_driver_kwargs(self) -> dict:
        """Dispatch on type(self.auth) via AUTH_TO_DRIVER_KWARGS default map."""
        ...

    def to_driver_kwargs(self) -> dict:
        kwargs = self._default_driver_kwargs()
        kwargs.update(self._auth_to_driver_kwargs())
        if self.__adapter__ is not None:
            kwargs = self.__adapter__(self)
        return kwargs

    def to_connection_string(self) -> str:
        """Build from descriptor.connection_string_scheme + 1:1 params.
        Backends with non-standard shapes override this method on their subclass."""
        ...
```

At class-creation time (via a model-class decorator or `__init_subclass__`), the base reads `__descriptor__` and configures pydantic fields: one per `ParameterSpec`, plus the discriminated-union `auth` field assembled from `descriptor.auth_modes`. The result is a normal pydantic model — no runtime `create_model`, no dynamic attribute surgery — just a concise declarative path into the same machinery pydantic uses today.

---

## Adapter layer

**Default auth dispatch** — `settings/auth/dispatch.py` exports `AUTH_TO_DRIVER_KWARGS: dict[type[AuthSpec], Callable[[AuthSpec, BackendDescriptor], dict]]`. Example entries:

- `PasswordAuth` → `{"user": auth.username, "password": auth.password.get_secret_value()}`
- `TokenAuth` → `{"token": auth.token.get_secret_value()}`
- `OAuth2Auth` → `{"token": auth.token.get_secret_value()}` if `token` set, else `{"credential": f"{id}:{secret}"}`
- `IAMAuth` → `{"aws_access_key_id": ..., "aws_secret_access_key": ..., "aws_session_token": ...}` (skip None)
- `NoAuth` → `{}`

**Backend adapters override when the driver expects non-standard shapes:**

```python
# adapters/trino.py
from trino.auth import BasicAuthentication, JWTAuthentication, KerberosAuthentication

def build_driver_kwargs(profile):
    kwargs = profile._default_driver_kwargs()
    match profile.auth:
        case PasswordAuth(username=u, password=pw):
            kwargs["user"] = u
            kwargs["auth"] = BasicAuthentication(u, pw.get_secret_value())
        case JWTAuth(token=t):
            kwargs["auth"] = JWTAuthentication(t.get_secret_value())
        case KerberosAuth() as k:
            kwargs["auth"] = KerberosAuthentication(
                config=None, service_name=k.service_name,
                principal=k.principal, ...)
        case NoAuth():
            pass
    return kwargs
```

```python
# adapters/mysql.py
def build_driver_kwargs(profile):
    kwargs = profile._default_driver_kwargs()
    ssl = {k: v for k, v in {
        "ssl-key":    profile.SSL_KEY,
        "ssl-cert":   profile.SSL_CERT,
        "ssl-ca":     profile.SSL_CA,
        "ssl-capath": profile.SSL_CAPATH,
        "ssl-cipher": profile.SSL_CIPHER,
    }.items() if v is not None}
    if ssl:
        kwargs["ssl"] = ssl
    return kwargs
```

**Backends with no adapter:** `sqlite`, `duckdb`, `motherduck`, `postgresql` (the 1:1 libpq mapping is complete without one), `pyspark`.

**Backends with adapters:** `mysql` (`ssl={}`), `mssql` (Encrypt/TrustServerCertificate, `server\instance`), `snowflake` (session_parameters, authenticator mapping), `bigquery` (SA-info → `Credentials`), `redshift` (endpoint resolution from CLUSTER_IDENTIFIER/WORKGROUP_NAME), `trino` (auth wrapper objects), `pyiceberg_rest` (`s3.*` / `rest.sigv4-*` key prefixing).

---

## File layout

```
src/mountainash_data/core/settings/
├── __init__.py                  # re-exports the 11 *AuthSettings classes
├── profile.py                   # ConnectionProfile
├── descriptor.py                # BackendDescriptor, ParameterSpec, MISSING
├── registry.py                  # REGISTRY, @register, lookup helpers
├── auth/
│   ├── __init__.py              # exports all AuthSpec subclasses + union alias
│   ├── base.py                  # AuthSpec ABC
│   ├── password.py
│   ├── token.py                 # TokenAuth, JWTAuth
│   ├── oauth2.py
│   ├── iam.py
│   ├── azure.py                 # AzureADAuth, WindowsAuth
│   ├── cloud.py                 # ServiceAccountAuth
│   ├── kerberos.py
│   ├── certificate.py
│   ├── none.py
│   └── dispatch.py              # AUTH_TO_DRIVER_KWARGS default map
├── adapters/
│   ├── __init__.py
│   ├── trino.py
│   ├── mssql.py
│   ├── mysql.py
│   ├── snowflake.py
│   ├── bigquery.py
│   ├── redshift.py
│   └── pyiceberg_rest.py
├── sqlite.py                    # descriptor + shell class
├── duckdb.py
├── motherduck.py
├── postgresql.py
├── mysql.py
├── mssql.py
├── snowflake.py
├── bigquery.py
├── redshift.py
├── pyspark.py
├── trino.py
└── pyiceberg_rest.py
```

`base.py` and `exceptions.py` are **deleted**. `BaseDBAuthSettings` goes away; `DBAuthValidationError` is replaced by `ValueError` / pydantic's built-in `ValidationError`. If any external caller catches `DBAuthValidationError`, we add a one-line shim aliasing it to `ValueError`.

### Per-backend file template

```python
# postgresql.py
from enum import StrEnum
from pathlib import Path
from .descriptor import BackendDescriptor, ParameterSpec, MISSING
from .profile import ConnectionProfile
from .auth import PasswordAuth, NoAuth
from .registry import register
from ..constants import CONST_DB_PROVIDER_TYPE

class PostgresSSLMode(StrEnum):
    DISABLE = "disable"
    ALLOW = "allow"
    PREFER = "prefer"
    REQUIRE = "require"
    VERIFY_CA = "verify-ca"
    VERIFY_FULL = "verify-full"

# (other PG enums: target_session_attrs, require_auth methods, sslcertmode)

POSTGRESQL_DESCRIPTOR = BackendDescriptor(
    name="postgresql",
    provider_type=CONST_DB_PROVIDER_TYPE.POSTGRESQL,
    default_port=5432,
    connection_string_scheme="postgresql://",
    ibis_dialect="postgres",
    auth_modes=[PasswordAuth, NoAuth],
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", default=MISSING, driver_key="host"),
        ParameterSpec(name="DATABASE", type=str | None, tier="core", default=None, driver_key="database"),
        ParameterSpec(name="SCHEMA", type=str | None, tier="core", default=None, driver_key="schema"),
        ParameterSpec(name="SSL_MODE", type=PostgresSSLMode, tier="core",
                      default=PostgresSSLMode.PREFER, driver_key="sslmode"),
        ParameterSpec(name="SSL_CERT", type=Path | None, tier="advanced",
                      default=None, driver_key="sslcert"),
        ParameterSpec(name="CONNECT_TIMEOUT", type=int | None, tier="advanced",
                      default=None, driver_key="connect_timeout"),
        # ... rest of the libpq surface, widened per the audit ...
    ],
)

@register(POSTGRESQL_DESCRIPTOR)
class PostgreSQLAuthSettings(ConnectionProfile):
    __descriptor__ = POSTGRESQL_DESCRIPTOR
```

Two lines of actual class body. The rest is declarative data.

---

## Testing

1. **Descriptor-driven parametric tests** (`tests/test_unit/core/settings/test_descriptors.py`). Iterate over `REGISTRY`, assert invariants every backend must satisfy:
   - Parameter `name`s unique within descriptor.
   - `driver_key`s unique within descriptor (where non-None).
   - `auth_modes` contains only `AuthSpec` subclasses.
   - `provider_type` matches a known `CONST_DB_PROVIDER_TYPE` member.
   - `tier ∈ {"core", "advanced"}`.
   - `name == descriptor.name` matches the `@register` key.

   These would have caught the `db_provider_type returns BIGQUERY` copy-paste automatically.

2. **Round-trip tests per backend.** One test file per backend with:
   - Minimal construction (required fields only) succeeds.
   - `to_driver_kwargs()` returns the expected keys/types — secrets unwrapped to raw strings, enums serialized to values.
   - Each declared auth mode constructs and maps to driver kwargs correctly.
   - `to_connection_string()` produces a syntactically-correct URL for the scheme.

3. **Audit-regression tests.** For each bug fix the refactor carries in from the audit, one test that would have caught the original defect (Snowflake `"snowflake "` trailing whitespace rejection, Trino `auth=BasicAuthentication(...)` vs bare `password=` kwarg, PG `db_provider_type`, MSSQL `args["server"]` KeyError, etc.). Each lands with its backend's migration commit.

4. **Connection smoke tests.** SQLite + DuckDB in-memory already run in CI — they continue to run unchanged. Other backends stay mocked.

---

## Migration path

**Phase 1 — scaffolding:**
- `profile.py`, `descriptor.py`, `registry.py`.
- `auth/` with all `AuthSpec` members + `dispatch.py`.
- Parametric descriptor-invariant tests (no backend involved yet).

**Phase 2 — migrate backends one at a time,** in this order (cheapest → hardest):
- SQLite, DuckDB, PySpark, MotherDuck (no auth or token-only; small surface).
- PostgreSQL, MySQL, Trino.
- MSSQL, Snowflake.
- BigQuery, Redshift, PyIceberg REST.

Each backend migration is **one commit**: the new file (descriptor + shell class + any adapter) + its tests, delete the old file, update `__init__.py` re-exports, run the full test suite. The old and new class never coexist — we flip one at a time.

**Phase 3 — consumer update + cleanup:**
- Delete `base.py`, `exceptions.py`, now-dead helpers.
- Update `SettingsFactory`, `ConnectionFactory`, `DatabaseUtils` to call `to_driver_kwargs` / `to_connection_string`.
- Update `backends/ibis/connection.py` and `backends/iceberg/connection.py` to consume the new profile API.
- Update `README.md` and any user-facing docs.

**Phase 4 — audit-fix sweep:**
- For any audit findings not already handled during Phase 2 translation (e.g. parameter surface widening that would have bloated a translation commit), a dedicated per-backend pass. Each is a small commit.

---

## Out of scope

- Rewriting factories, `DatabaseUtils`, or `backends/` beyond call-site updates.
- Adding new backends.
- Adding new `AuthSpec` members beyond those required by the current 11 backends. (No speculative `SSHTunnelAuth`, no `DatabaseCredentialsAuth`.)
- Runtime introspection of driver modules (`RestCatalog.__init__`, `snowflake.connector.connect` signature walking). Separate initiative.
- Maintaining backward compatibility for the retired `get_connection_string_template`/`get_connection_string_params`/`get_connection_kwargs`/`get_post_connection_options` methods. They have no external consumers outside factories we also update.

## Risks and mitigations

- **Pydantic discriminated-union edge cases** with the `MountainAshBaseSettings` config-file loader. Mitigation: an early Phase-1 spike with a representative auth-multi-mode backend (MSSQL or Snowflake) confirms config-file YAML/ENV maps cleanly to tagged-union construction before we scale out.
- **Parameter surface drift.** The audit widens many backends' surfaces. A too-ambitious Phase-2 commit per backend could balloon. Mitigation: Phase 4 is explicitly reserved for widening; Phase 2 commits translate 1:1 and keep scope tight. Reviewers can compare old class vs. new descriptor parameter-by-parameter.
- **Secret handling regressions.** Several backends currently pass SecretStr objects directly (we flagged this across the audit). The new `ParameterSpec.secret=True` centralizes `.get_secret_value()` — but only at the `to_driver_kwargs` boundary. Mitigation: round-trip tests assert raw strings come out of `to_driver_kwargs`, not `SecretStr` objects.
- **`DBAuthValidationError` shim removal.** If external callers catch it, deleting the class breaks them. Mitigation: one-line `DBAuthValidationError = ValueError` alias in a deprecation shim module; delete in a later release.
