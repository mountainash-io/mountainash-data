# Design Spec: Migrate mountainash-data to mountainash-auth-client

**Date:** 2026-06-28
**Status:** Draft — for review
**Author:** Nathaniel Ramm (with Claude)
**Supersedes:** `2026-06-27-auth-client-migration-design.md` (v1–v4). That draft
routed auth translation through `Profile.register_adapter` (registering
data's adapters onto auth-client's classes). This rewrite drops that entirely:
**data owns its auth translation in its own code**, mirroring how
`mountainash-wearables` reads credentials directly. No dependency on the
settings `register_adapter` primitive.

---

## 1. Context & Problem

mountainash-data's settings layer still imports `mountainash_settings.auth`, which
was **deleted upstream** when auth was extracted into the standalone
`mountainash-auth-client` package (settings commit `3d0f4a4`). Against the live
`mountainash-settings` 26.5.0 the package is **broken**: the whole test suite fails
at collection — `conftest` → settings fixtures → `core/settings/__init__.py:21` →
`from mountainash_settings.auth import …` → `ModuleNotFoundError`. Top-level
`import mountainash_data` only survives because `__init__` does not eagerly load the
settings layer.

This is **not a rename**. Three pieces of upstream machinery mountainash-data leaned
on were also removed:

| Removed upstream | mountainash-data dependency | Failure |
|---|---|---|
| `auth_modes` field on `ProfileSpec` (settings `2d72318`) | all 20 backends call `BackendSpec(auth_modes=[…])` | `TypeError` at import — frozen dataclass, unknown kwarg |
| `_auth_kwargs()` on `Profile` (settings `297b587`) | the base profile's `to_driver_kwargs()` + `adapters/mysql.py` call it | `AttributeError` at runtime |
| auto-installed `.auth` discriminated-union field (driven by `auth_modes`) | per-backend adapters + `to_connection_string()` read `self.auth` | field no longer exists |
| `mountainash_settings.auth` module | `__init__.py`, the per-backend settings files, 9 adapters, the `core/settings/auth/` shim, ~28 tests | `ModuleNotFoundError` |

### The new auth model (`mountainash-auth-client`)

auth-client replaces the old pydantic `*Auth` classes with `*AuthProfile`
classes (subclasses of `mountainash_settings.Profile`):

- Names: `PasswordAuth` → `PasswordAuthProfile`, `NoAuth` → `NoAuthProfile`, etc.
  **No backward-compat aliases, no `AuthSpec` base** — an `AuthProfile` union type
  is exported instead.
- Fields are **UPPERCASE** `ParameterSpec` names: `auth.username` → `auth.USERNAME`,
  `auth.password` → `auth.PASSWORD`. Secret fields remain pydantic `SecretStr`
  (`.get_secret_value()` still works); path fields are `Path | None`.
- The auth profiles ship `emit()`/`__adapters__` adapters for auth-client's own
  SDK families (`HTTP`/`BOTO`/`PARAMIKO`). **These are reference implementations**
  — the shape a client copies, not a surface a client extends. mountainash-data's
  targets are ibis DB drivers, outside those families, so data does **not** use the
  auth profiles' `emit()`; it reads their fields directly (§3.4).

### Project constraints

mountainash-data is **pre-release with zero downstream consumers**. A **clean
break** is required; the goal is the best architecture for this infrastructure
package, **not** backward compatibility. No deprecation aliases, no compat shims.

---

## 2. Goals & Non-Goals

### Goals
1. Unbreak the package against `mountainash-settings` 26.5.0 + `mountainash-auth-client`.
2. Adopt `mountainash-transport`'s **three-layer separation** — declarative config
   profile, runtime connection, composing factory — with auth **decoupled** from the
   config profile and passed alongside it at connect time.
3. **Own the auth→driver-kwargs translation in mountainash-data** (the
   `mountainash-wearables` model: read the auth profile's fields directly). Driver
   imports (`trino.auth`, `google.oauth2`) stay in data. Nothing is registered onto
   auth-client's classes; data depends on no settings extension primitive.
4. Replace the deleted `auth_modes` / `_auth_kwargs` / `.auth`-field machinery.
5. Rename the misnamed `*AuthSettings` classes to `*BackendProfile` (base
   `ConnectionProfile` → `BackendProfile`), reserving **"Connection" for the runtime
   layer** (§3.1).
6. Make `mountainash-auth-client` a first-class core dependency.
7. All tests green under `hatch run test:test`; `mypy:check` + `ruff:check` clean.

### Non-Goals
- Interactive OAuth **acquisition**/persistence (`OAuth2TokenManager`,
  `PersistableAuthProfile`, `token_store`). Deferred — §10 Backlog.
- Reworking the Ibis `DialectSpec` registry, the inspection model, or the iceberg
  catalog registry beyond the auth threading.
- Adding new backends or auth types.
- Any use of `Profile.register_adapter` / cross-package adapter registration.

---

## 3. Architecture

### 3.1 Three layers — and "Connection" reserved for the runtime

mountainash-data mirrors `mountainash-transport`'s three roles. The config-layer
classes are named `*BackendProfile` so "Connection" belongs exclusively to the
runtime handles:

| Role | transport | mountainash-data |
|---|---|---|
| **config profile** — declarative, owns `emit()` for its own config | `settings/storage/profiles/*StorageProfile` | `core/settings/*BackendProfile` |
| **runtime connection** — consumes a finished kwargs dict, opens the handle | `connections/*Connection` | `backends/ibis` (`IbisBackend`/`IbisConnection`), `backends/iceberg` (`IcebergConnection`) |
| **composing factory** — bridges the two, layers auth onto config | `connections/__init__.py:create_connection` | `core/factories/ConnectionFactory` |

- Base class `ConnectionProfile` → `BackendProfile`.
- 20 leaves `*AuthSettings` → `*BackendProfile` (e.g. `PostgreSQLBackendProfile`).
- Runtime handles keep `IbisConnection` / `IcebergConnection` / `BaseDBConnection`.

### 3.2 Decouple auth from the backend profile

A `*BackendProfile` carries **only backend config** (host/port/database/
warehouse/role/…). Auth is a **separate, orthogonal** `AuthProfile | None` passed
alongside it at connect time — mirroring transport's
`create_connection(storage_profile, auth_profile)`, and reflecting reality: the same
server config is reusable with different credentials.

```python
backend = IbisBackend(dialect="postgres", host="db", database="app")
conn = backend.connect(
    auth_profile=PasswordAuthProfile(USERNAME="app", PASSWORD="s3cret"),
)
```

### 3.3 Config emission — `BackendProfile.emit()` (transport-style, data-owned)

The config side uses the `emit()` pipeline exactly as transport's `StorageProfile`
does — and data owns these classes, so the adapters are **class-body literals**, the
copy-on-write-safe idiom (no `register_adapter`):

- **Flat backends** (17 of 20): `ParameterSpec.driver_key` renames alone. The package
  already carries ~143 `driver_key` annotations; `profile.emit(target)` runs them via
  `_default_kwargs`.
- **Non-flat backends** (3): a class-literal `__adapters__` compose adapter builds the
  nested/combined config the driver wants — the direct analogue of transport's S3
  `botocore.Config`, SFTP `_post_connect`, HTTP `httpx.Timeout`:

  | Backend | Non-flat connection shaping | Transport precedent |
  |---|---|---|
  | mysql | nested `ssl={…}` from the 5 `SSL_*` fields | `HTTPStorageProfile` → `httpx.Timeout(...)` |
  | mssql | fold `HOST` + `INSTANCE_NAME` → `host\instance`; encryption flags | `SFTPStorageProfile` → `_post_connect` |
  | snowflake | `session_parameters={…}` from `QUERY_TAG`/`TIMEZONE` | `S3StorageProfile` → `botocore.Config(...)` |

The emit **target key is the backend's `provider_type`** (`CONST_DB_PROVIDER_TYPE`,
already on every spec) — no new target enum. A shaping backend keys its literal under
its own `provider_type`:

```python
@register
class MySQLBackendProfile(BackendProfile):
    __spec__ = MYSQL_SPEC                                    # provider_type=MYSQL
    __adapters__ = {CONST_DB_PROVIDER_TYPE.MYSQL: _mysql.ssl_compose}
```

`base = profile.emit(profile.__spec__.provider_type)` is then uniform across all 20:
flat backends just rename; the 3 shaping backends additionally run their compose
adapter (which receives the already-renamed dict as its second arg). The compose
functions live in `core/settings/adapters/<dialect>.py`.

### 3.4 Auth translation — data-owned, in the factory (the wearables model)

auth-client can't ship adapters for ibis drivers (they'd need `import trino.auth`),
and its HTTP/BOTO/PARAMIKO adapters are **reference implementations to copy, not an
extension surface**. So mountainash-data owns its auth→kwargs translation outright,
the way `mountainash-wearables` reads `profile.PASSWORD.get_secret_value()` directly
in its connections. **Data does not call `auth_profile.emit()` and registers nothing
onto auth-client's classes.**

The translation lives behind a **data-owned dispatch table** keyed by
`(provider_type, auth_class)` — declarative dispatch with no `isinstance` ladders,
entirely inside mountainash-data:

```python
# core/settings/adapters/registry.py  (data-owned; NOT auth-client)
from mountainash_auth_client import PasswordAuthProfile, JWTAuthProfile, \
    KerberosAuthProfile, ServiceAccountAuthProfile, IAMAuthProfile, TokenAuthProfile, \
    OAuth2AuthProfile, CertificateAuthProfile, WindowsAuthProfile, AzureADAuthProfile
from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE as P
from . import sql as _sql, trino as _trino, snowflake as _snow, bigquery as _bq, \
    databricks as _dbx, mssql as _mssql, redshift as _rs, pyiceberg_rest as _ice

# (provider_type, auth_class) -> (auth_profile, base) -> dict
_AUTH_ADAPTERS: dict[tuple, Callable] = {
    (P.TRINO,      PasswordAuthProfile):       _trino.password,
    (P.TRINO,      JWTAuthProfile):            _trino.jwt,
    (P.TRINO,      KerberosAuthProfile):       _trino.kerberos,
    (P.SNOWFLAKE,  PasswordAuthProfile):       _snow.password,
    (P.SNOWFLAKE,  TokenAuthProfile):          _snow.token,
    (P.SNOWFLAKE,  OAuth2AuthProfile):         _snow.oauth2,
    (P.SNOWFLAKE,  CertificateAuthProfile):    _snow.certificate,
    (P.BIGQUERY,   ServiceAccountAuthProfile): _bq.service_account,
    (P.DATABRICKS, TokenAuthProfile):          _dbx.token,
    (P.DATABRICKS, PasswordAuthProfile):       _dbx.password,
    (P.MSSQL,      PasswordAuthProfile):       _mssql.password,
    (P.MSSQL,      WindowsAuthProfile):        _mssql.windows,
    (P.MSSQL,      AzureADAuthProfile):        _mssql.azure_ad,
    (P.REDSHIFT,   PasswordAuthProfile):       _rs.password,
    (P.REDSHIFT,   IAMAuthProfile):            _rs.iam,
    (P.PYICEBERG_REST, TokenAuthProfile):      _ice.token,
    (P.PYICEBERG_REST, OAuth2AuthProfile):     _ice.oauth2,
}
# Flat user/password backends share one adapter:
for _p in (P.POSTGRES, P.MYSQL, P.CLICKHOUSE, P.MATERIALIZE, P.RISINGWAVE,
           P.DRUID, P.SINGLESTOREDB, P.IMPALA, P.EXASOL):
    _AUTH_ADAPTERS[(_p, PasswordAuthProfile)] = _sql.userpass

def auth_adapter(provider_type, auth_class):
    return _AUTH_ADAPTERS.get((provider_type, auth_class))
```

Each adapter is `(_auth_profile, base) -> dict`, reads UPPERCASE fields (`str(...)` on
`Path | None` where the driver wants a string), and builds the driver kwargs/objects.
Example:

```python
# core/settings/adapters/trino.py
def password(auth, base):
    from trino.auth import BasicAuthentication
    return {**base, "auth": BasicAuthentication(auth.USERNAME,
                                                auth.PASSWORD.get_secret_value())}
```

`NoAuthProfile` is never in the table — the factory short-circuits it (§3.5). The
driver imports are local to each adapter module, so importing the settings layer
never pulls in `trino`/`google` unless that backend is actually used.

### 3.5 The composing factory — `ConnectionFactory` (the `_emit_kwargs` analogue)

Composition lives in the factory, not on the profile (transport's
`create_connection`/`_emit_kwargs`). `BackendProfile` stays a pure config emitter.

```python
# core/factories/connection_factory.py
def build_driver_kwargs(profile: BackendProfile, auth_profile: AuthProfile | None = None) -> dict:
    auth = _normalize_and_validate_auth(profile, auth_profile)   # §6
    target = profile.__spec__.provider_type
    base = profile.emit(target)                                  # config only (§3.3)
    if isinstance(auth, NoAuthProfile):
        return base                                              # short-circuit (cf. transport)
    fn = auth_adapter(target, type(auth))
    if fn is None:                                               # fail-closed
        raise ValueError(
            f"{profile.backend}: no auth adapter for {type(auth).__name__}"
        )
    return fn(auth, base)                                        # data-owned translation
```

```
caller ── auth_profile (AuthProfile|None) ──▶ IbisBackend.connect(auth_profile)
                                              │  (also iceberg connect path)
                                              ▼
            ConnectionFactory.build_driver_kwargs(backend_profile, auth_profile)
                    auth = _normalize_and_validate_auth(profile, auth_profile)
                    target = profile.__spec__.provider_type
                    base = profile.emit(target)         # config (BackendProfile)
                              │
                    NoAuth? ──┴── yes ─▶ return base
                              │ no
                    auth_adapter(target, type(auth))(auth, base)   # data-owned
                              │  (builds BasicAuthentication / Credentials /
                              ▼   {user,password} / …; imports the driver)
                    dict ready for the ibis driver ──▶ runtime Connection opens it
```

---

## 4. Component Changes

### 4.1 `core/settings/__init__.py`
- Replace `from mountainash_settings.auth import (…)` with
  `from mountainash_auth_client import (NoAuthProfile, PasswordAuthProfile,
  TokenAuthProfile, JWTAuthProfile, OAuth2AuthProfile, IAMAuthProfile,
  WindowsAuthProfile, AzureADAuthProfile, KerberosAuthProfile,
  CertificateAuthProfile, ServiceAccountAuthProfile, AuthProfile)`.
- Update `__all__`: drop old `*Auth`/`AuthSpec` names; add the `*AuthProfile`
  names + `AuthProfile`; rename the backend re-exports to `*BackendProfile`.
- No registration import — auth dispatch is a plain table loaded lazily by the
  factory (§3.4); nothing must run at settings-import time.

### 4.2 Delete `core/settings/auth/`
Remove `__init__.py`, `base.py`, `dispatch.py` (pure shims over the deleted
`mountainash_settings.auth`). Verified the only consumer of
`auth_to_driver_kwargs`/`AUTH_TO_DRIVER_KWARGS` is the shim itself.

### 4.3 `core/settings/descriptor.py` (`BackendSpec`)
- Add a **required** `supported_auth: tuple[type, ...]` (no default; typed loosely as
  `type` to avoid importing the union at dataclass-definition time; values are
  `*AuthProfile` classes). Registry invariant: empty `supported_auth` fails at import.
- **Drop `auth_modes`** everywhere (gone with the upstream `ProfileSpec` path).
- No new `auth_target` field — the existing `provider_type` is the dispatch/emit key
  (§3.3–§3.4).

### 4.4 `core/settings/profile.py` (`BackendProfile`) — pure config emitter
- Rename the base class `ConnectionProfile` → `BackendProfile`.
- **Remove all auth coupling**: no `to_driver_kwargs`, no `to_connection_string`, no
  `_auth_kwargs`, no `__adapter__`. The class exposes only `emit(target)` (inherited)
  for its own config — as declarative as transport's `StorageProfile`.

### 4.5 `core/settings/adapters/` — data-owned adapter functions
- `adapters/<dialect>.py` — the auth-translation functions `(_auth_profile, base) -> dict`
  (driver imports local) **and** the 3 connection-shaping compose functions
  `(profile, base) -> dict` referenced by the `__adapters__` literals (§3.3).
- `adapters/sql.py` — the shared flat `userpass(auth, base)`.
- `adapters/registry.py` — the `_AUTH_ADAPTERS` table + `auth_adapter()` lookup (§3.4).
- The old per-backend `build_driver_kwargs` modules and the
  `__adapter__ = staticmethod(...)` lines are **removed**.

### 4.6 New: `core/factories/connection_factory.py`
- `_normalize_and_validate_auth(profile, auth_profile) -> AuthProfile`: `None` →
  `NoAuthProfile()`, then `isinstance`-validate against
  `profile.__spec__.supported_auth`; clear `ValueError` on miss (§6).
- `build_driver_kwargs(profile, auth_profile=None) -> dict` — the §3.5 body.
- `build_connection_string(profile, auth_profile=None) -> str`:
  - validate first;
  - password-style `scheme://user:pass@host:port/db` **only** for
    `PasswordAuthProfile` (`USERNAME` / `PASSWORD.get_secret_value()`, each
    `quote(..., safe="")`); `NoAuthProfile` → no creds;
  - token-in-URL backends (MotherDuck `md:<db>?motherduck_token=…`, and any future
    Snowflake/Databricks/Trino-JWT URL form) handled by a per-`provider_type` URL
    builder (keeps URL quirks out of the profile); any other auth type →
    `NotImplementedError`.
  - (URLs are not kwargs; this path does not use `emit()`.)

### 4.7 Rename `*AuthSettings` → `*BackendProfile` (20 backends)
Across all 20 modules, class definitions, `__init__.py` exports, and references. Drop
`auth_modes=[…]` from each `BackendSpec(...)`; add `supported_auth=(…AuthProfile, …)`.
mysql/mssql/snowflake additionally gain a class-literal `__adapters__` (§3.3).

| Old | New |
|---|---|
| `SQLiteAuthSettings` | `SQLiteBackendProfile` |
| `PostgreSQLAuthSettings` | `PostgreSQLBackendProfile` |
| … (all 20) | `*BackendProfile` |

### 4.8 Entry points
- `backends/ibis/backend.py`: `IbisBackend.connect(self, auth_profile=None)` is the
  single auth entry point. **Defer auth-dependent kwargs to `connect()`** — today
  `_init_from_settings` eagerly calls `to_driver_kwargs()` at `__init__`
  (backend.py:242), before any `auth_profile` exists. Restructure so
  `__init__`/`_init_from_settings` resolves only the dialect + spec and stores the
  `BackendProfile`; `connect(auth_profile)` calls
  `ConnectionFactory.build_driver_kwargs(profile, auth_profile)` and layers
  `self._config`. The direct-dialect path is unaffected.
- **URL creds vs explicit `auth_profile` precedence:** explicit
  `connect(auth_profile=…)` **always wins**. URL `user:pass@` is parsed into a
  `PasswordAuthProfile` **only when no explicit `auth_profile` is given**; supplying
  both is a `ValueError`. URL credentials are **stripped** before the URL reaches
  `ibis.connect` (creds travel via the auth profile).
- `backends/iceberg/connection.py`: `connect_default(self, *, auth_profile=None, **kwargs)`
  and `connect` thread `auth_profile` into
  `ConnectionFactory.build_driver_kwargs(profile, auth_profile)`. Precedence:
  **profile-derived kwargs < explicit `connection_kwargs`/`**kwargs`** (caller
  overrides win); document on the methods.

### 4.9 Dependency wiring
- `pyproject.toml`: add `mountainash-auth-client` to core `dependencies`.
- `hatch.toml`: add `mountainash_auth_client @ {root:uri}/../mountainash-auth-client`
  (local: `dev`, `test`) and `{root:uri}/temp/mountainash-auth-client` (CI:
  `test_github`, `build_github`), mirroring the existing settings/transport path deps.
  Remove the dead `mountainash_utils_ssh` path-dep line where present (the package is
  no longer a dependency of this layer).

---

## 5. Field Mapping (old → new), per auth type

Field names go UPPERCASE; secret-ness preserved. Path fields are `Path | None` —
adapters `str(...)` them where the driver wants a string. Verified against the old
adapter reads and the new `*AuthProfile` `ParameterSpec`s.

| Auth | Old field(s) | New field(s) | Secret / type notes |
|---|---|---|---|
| Password | `username`, `password` | `USERNAME`, `PASSWORD` | PASSWORD secret |
| Token | `token` | `TOKEN` | TOKEN secret |
| JWT | `token` | `TOKEN` | TOKEN secret |
| Kerberos | `service_name`, `principal` | `SERVICE_NAME`, `PRINCIPAL`, `KEYTAB` | `KEYTAB: Path \| None` (unused by the trino adapter) |
| Windows | `domain`, `username` | `DOMAIN`, `USERNAME` | — |
| AzureAD | `tenant_id`, `client_id`, `client_secret`, `managed_identity`, `msi_endpoint` | `TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET`, `MANAGED_IDENTITY`, `MSI_ENDPOINT` | CLIENT_SECRET secret |
| IAM | `role_arn`, `access_key_id`, `secret_access_key`, `session_token`, `profile_name` | `ROLE_ARN`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`, `SESSION_TOKEN`, `PROFILE_NAME` | SECRET_ACCESS_KEY, SESSION_TOKEN secret |
| ServiceAccount | `info`, `file` | `INFO`, `FILE` | `FILE: Path \| None`; `INFO: dict \| None` |
| OAuth2 | `client_id`, `client_secret`, `token`, `refresh_token`, `server_uri`, `scope` | `CLIENT_ID`, `CLIENT_SECRET`, `TOKEN`, `REFRESH_TOKEN`, `SERVER_URI`, `SCOPE` | CLIENT_SECRET, TOKEN, REFRESH_TOKEN secret |
| Certificate | `private_key`, `private_key_path`, `passphrase` | `PRIVATE_KEY`, `PRIVATE_KEY_PATH`, `PASSPHRASE` | PRIVATE_KEY, PASSPHRASE secret; `PRIVATE_KEY_PATH: Path \| None` |
| NoAuth | — | — | — |

> **Scope:** rows are exactly the auth types a backend consumes. `OAuth1AuthProfile`
> and `OAuth2AuthCodeAuthProfile` are union members **no backend consumes** (verified:
> zero references) — no mapping, not in any `supported_auth`, not in the table.
> `OAuth2AuthProfile.SERVER_URI`/`SCOPE` are `tier="advanced"`; pyiceberg reads
> `SERVER_URI`/`SCOPE`/`CLIENT_ID`/`CLIENT_SECRET`/`TOKEN`; snowflake reads `TOKEN`.

### 5.1 Per-backend `supported_auth`

| Backend | provider_type | supported_auth |
|---|---|---|
| sqlite, duckdb, pyspark | SQLITE/DUCKDB/PYSPARK | `(NoAuthProfile,)` |
| postgres, clickhouse, singlestoredb, druid, impala, materialize, risingwave | … | `(PasswordAuthProfile, NoAuthProfile)` |
| mysql, exasol | MYSQL/EXASOL | `(PasswordAuthProfile,)` |
| motherduck | MOTHERDUCK | `(TokenAuthProfile,)` |
| trino | TRINO | `(PasswordAuthProfile, JWTAuthProfile, KerberosAuthProfile, NoAuthProfile)` |
| snowflake | SNOWFLAKE | `(PasswordAuthProfile, OAuth2AuthProfile, CertificateAuthProfile, TokenAuthProfile)` |
| bigquery | BIGQUERY | `(ServiceAccountAuthProfile, NoAuthProfile)` |
| databricks | DATABRICKS | `(TokenAuthProfile, PasswordAuthProfile, NoAuthProfile)` |
| redshift | REDSHIFT | `(PasswordAuthProfile, IAMAuthProfile)` |
| mssql | MSSQL | `(PasswordAuthProfile, WindowsAuthProfile, AzureADAuthProfile)` |
| pyiceberg_rest | PYICEBERG_REST | `(TokenAuthProfile, OAuth2AuthProfile)` |

---

## 6. Validation & Error Handling

- Factory-level `_normalize_and_validate_auth(profile, auth_profile)` is called first
  by **both** `build_driver_kwargs` and `build_connection_string`: `None` →
  `NoAuthProfile()`, then `isinstance(auth, tuple(profile.__spec__.supported_auth))`;
  on miss raise `ValueError(f"{profile.backend} does not support auth:
  {type(auth).__name__}")`. `isinstance` (not exact `type()`) so subclasses are
  accepted.
- Empty `supported_auth` is impossible: the registry invariant (§4.3) rejects it.
- **Fail-closed dispatch:** if `auth` passed `supported_auth` but
  `auth_adapter(provider_type, type(auth))` is `None`, the factory raises (§3.5) —
  an auth type listed as supported but missing its adapter is caught loudly, never by
  emitting unauthenticated kwargs.
- A startup consistency check (test, §7) asserts every `(provider_type, auth_class)`
  in each backend's `supported_auth` (minus `NoAuthProfile`) has a table entry, and
  no table entry references an unsupported pair.

---

## 7. Testing Strategy

- Update ~28 test files: imports → `mountainash_auth_client` (or the `core/settings`
  re-exports); construction → UPPERCASE kwargs
  (`PasswordAuthProfile(USERNAME="u", PASSWORD="p")`); auth passed as a separate arg,
  not an `auth=` field. `tests/fixtures/settings_fixtures.py` yields
  `(backend_profile, auth_profile)` pairs.
- New focused tests:
  - **Golden per (dialect, auth type):** `build_driver_kwargs(profile, auth)` yields
    the exact driver-kwargs dict (trino → `auth=BasicAuthentication(...)`; bigquery →
    `credentials=…`; postgres → `{user, password}`; …). Mirrors transport's emission
    goldens.
  - **Config-only emit:** `profile.emit(provider_type)` golden for the 3 shaping
    backends (mysql `ssl={}`, mssql host-fold, snowflake `session_parameters`).
  - **`supported_auth` rejection:** out-of-`supported_auth` type → `ValueError` (one
    negative per backend).
  - **`None` normalization:** no auth → `NoAuthProfile` accepted for no-auth backends,
    rejected for credential-required backends.
  - **`isinstance` validation:** a subclass of an allowed `*AuthProfile` is accepted.
  - **Fail-closed dispatch:** a supported auth type with no table entry → `ValueError`.
  - **Table/`supported_auth` consistency** (§6).
  - **`build_connection_string`:** password backend → `user:pass@` (percent-encoded,
    secret unwrapped); MotherDuck → `…?motherduck_token=`; other type →
    `NotImplementedError`.
  - **Registry invariant:** empty `supported_auth` fails at import.
  - **URL-vs-explicit precedence:** both → `ValueError`; URL-only → creds stripped and
    carried via `PasswordAuthProfile`.
- Acceptance gate: `hatch run test:test` green; `mypy:check` clean; `ruff:check` clean.

---

## 8. Isolation & Interfaces

- **auth-client** — owns the credential schemas (`*AuthProfile`) and ships
  `emit()` adapters for its own HTTP/BOTO/PARAMIKO families as **reference
  implementations**. mountainash-data reads its profile fields directly and
  **registers nothing onto it**.
- **`*BackendProfile`** — owns backend config + its own `emit(provider_type)`; pure
  config, no auth methods (transport `StorageProfile` analogue).
- **auth adapters** (`core/settings/adapters/<dialect>.py` + `sql.py`) — data-owned
  `(auth_profile, base) -> dict`; the only place that knows a driver's auth-kwarg
  shape; import the DB drivers; independently unit-testable without a live DB.
- **`adapters/registry.py`** — the `(provider_type, auth_class) -> fn` dispatch table.
- **`ConnectionFactory`** — the composing bridge (`build_driver_kwargs` /
  `build_connection_string` / `_normalize_and_validate_auth`); the only layer aware
  of *both* a backend profile and an auth profile.
- **runtime** (`IbisBackend`/`IbisConnection`, `IcebergConnection`) — consume the
  finished kwargs dict.

This depends on **no settings extension primitive** — only the stable
`mountainash-settings` `Profile`/`ProfileSpec`/`emit()` surface and
`mountainash-auth-client`'s profile classes.

---

## 9. Rollout

A single feature branch off mountainash-data `develop` → PR to `develop`. Internally
atomic (the package does not import cleanly until the settings layer is migrated).
Suggested commit slices:
(a) deps + `__init__` import swap + delete `auth/` shim;
(b) descriptor (`supported_auth` + invariant, drop `auth_modes`) + `BackendProfile`
rename to a pure `emit` config class;
(c) the 20 renames `*AuthSettings`→`*BackendProfile` + `supported_auth` + the
mysql/mssql/snowflake connection-shaping `__adapters__` literals;
(d) the data-owned auth adapters + `adapters/registry.py`;
(e) `ConnectionFactory` (`_normalize_and_validate_auth`, `build_driver_kwargs`,
`build_connection_string`);
(f) entry points (deferred-auth `IbisBackend`, URL precedence, iceberg threading);
(g) tests.

**No dependency on the settings `Profile.register_adapter` primitive.** That primitive
(settings PR #47) has no consumer under this design; reverting it from
mountainash-settings is a separate, recommended cleanup tracked outside this spec.

---

## 10. Backlog (deferred, captured)

**Interactive OAuth acquisition & token persistence.** Snowflake (OAuth) and
PyIceberg-REST (OAuth2) currently consume an **already-obtained** token read
statically off the auth profile (`auth.TOKEN` / `auth.CLIENT_ID`). The decoupled
design already lets a caller hand in a fully-authorized `OAuth2AuthProfile`. A future
capability should integrate the wearables lifecycle so mountainash-data can **acquire
and refresh** tokens itself: `OAuth2TokenManager(provider, auth_profile, resolver=…)`;
`PersistableAuthProfile` (`SETTINGS_SOURCE_SECRETS_PROVIDER` + `persist_key()`) +
`token_store()`; a `SecretStoreResolver` + `mountainash-secrets` wiring; likely a
small per-OAuth-backend subclass (à la wearables' `WearableOAuth2Auth`). Tracked as a
follow-up issue after this migration merges.

---

## 11. Open Questions

None outstanding. Auth placement = decoupled, composed in the factory; compat = clean
break; rename = `*BackendProfile` ("Connection" reserved for runtime); auth
translation = **data-owned** direct-field adapters in a `(provider_type, auth_class)`
table (no `register_adapter`, no `emit()` on auth profiles, no cross-package
mutation); config = `BackendProfile.emit()` with class-literal `__adapters__` on the 3
non-flat backends; OAuth lifecycle = deferred (§10).
