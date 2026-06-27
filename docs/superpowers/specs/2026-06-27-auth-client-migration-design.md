# Design Spec: Migrate mountainash-data to mountainash-auth-client

**Date:** 2026-06-27
**Status:** Draft — for review
**Author:** Nathaniel Ramm (with Claude)
**Depends on:** mountainash-settings `Profile.register_adapter`
(`2026-06-27-profile-register-adapter-design.md`) — must land first.

---

## 1. Context & Problem

mountainash-data's settings layer still imports `mountainash_settings.auth`, which
was **deleted upstream** when auth was extracted into the standalone
`mountainash-auth-client` package (settings commit `3d0f4a4`). Against the live
`mountainash-settings` 26.5.0, the package is **currently broken**: the entire
test suite fails at collection because `conftest` → settings fixtures →
`core/settings/__init__.py:21` → `from mountainash_settings.auth import …` →
`ModuleNotFoundError`. Top-level `import mountainash_data` only survives because
`__init__` does not eagerly load the settings layer.

This is **not a rename**. Two pieces of machinery mountainash-data depends on
were also removed from `mountainash-settings`:

| Removed upstream | mountainash-data dependency | Failure |
|---|---|---|
| `auth_modes` field on `ProfileSpec` (settings `2d72318`) | all 20 backends call `BackendSpec(auth_modes=[…])` | `TypeError` at import — frozen dataclass, unknown kwarg |
| `_auth_kwargs()` on `Profile` (settings `297b587`) | `ConnectionProfile.to_driver_kwargs()` (profile.py:44) + `adapters/mysql.py:14` call it | `AttributeError` at runtime |
| auto-installed `.auth` discriminated-union field (driven by `auth_modes`) | adapters + `to_connection_string()` read `self.auth` | field no longer exists |
| `mountainash_settings.auth` module | `__init__.py`, 22 settings files, 7 adapters, the `core/settings/auth/` shim, ~25 tests | `ModuleNotFoundError` |

### The new auth model (`mountainash-auth-client`)

auth-client replaces the old pydantic `*Auth` classes with `*AuthProfile`
classes (subclasses of `mountainash_settings.Profile`):

- Names: `PasswordAuth` → `PasswordAuthProfile`, `NoAuth` → `NoAuthProfile`, etc.
  There are **no backward-compat aliases** and **no `AuthSpec` base** — instead an
  `AuthProfile` union type is exported.
- Fields are **UPPERCASE** `ParameterSpec` names: `auth.username` → `auth.USERNAME`,
  `auth.password` → `auth.PASSWORD`. Secret fields remain pydantic `SecretStr`
  (`.get_secret_value()` still works).
- `auth_to_driver_kwargs()` / `AUTH_TO_DRIVER_KWARGS` are gone; profiles expose the
  generic three-tier `emit(target, base=…)` (driver_key renames → per-target
  `__adapters__` → legacy `__adapter__`) over any `Hashable` target.

### Project constraints

mountainash-data is **pre-release with zero downstream consumers**. A **clean
break** is required; the goal is the best possible architecture for this
infrastructure package, **not** backward compatibility. No deprecation aliases,
no compat shims.

---

## 2. Goals & Non-Goals

### Goals
1. Unbreak the package against `mountainash-settings` 26.5.0 + `mountainash-auth-client`.
2. Adopt the **canonical `emit()` composition** (auth-client `INTEGRATION.md`
   Pattern 1): `auth_profile.emit(target, base=connection_profile.emit(target))`,
   with auth **decoupled** from the connection profile (per `mountainash-transport`).
3. Contribute mountainash-data's ibis-driver auth translation through the **sanctioned
   `Profile.register_adapter` extension point** — registered *from* mountainash-data
   *onto* the auth-client profile classes, so auth-client never imports a DB driver
   and no package hand-mutates another's class state.
4. Replace the deleted `auth_modes` / `_auth_kwargs` / `.auth`-field machinery.
5. Rename the misnamed `*AuthSettings` classes to `*ConnectionProfile`.
6. Make `mountainash-auth-client` a first-class core dependency.
7. All tests green under `hatch run test:test`.

### Non-Goals
- Interactive OAuth **acquisition**/persistence (`OAuth2TokenManager`,
  `PersistableAuthProfile`, `token_store`). Deferred — see §10 Backlog.
- Reworking the Ibis `DialectSpec` registry, inspection model, or iceberg catalog
  registry beyond the auth threading.
- Adding new backends or auth types.
- An MRO-merge for `emit()` (register on the exact leaf class — see the settings
  spec §3.3).

---

## 3. Architecture

### 3.1 Decouple auth from the connection profile

The connection profile (`*ConnectionProfile`) carries **only backend config**
(host/port/database/warehouse/role/…). Auth is a **separate, orthogonal**
`AuthProfile | None` passed alongside it at connect time. This mirrors
`mountainash-transport`'s `create_connection(storage_profile, auth_profile)` and
reflects reality: the same server config is reusable with different credentials.

```python
backend = IbisBackend(dialect="postgres", host="db", database="app")
conn = backend.connect(
    auth_profile=PasswordAuthProfile(USERNAME="app", PASSWORD="s3cret"),
)
```

### 3.2 Adopt the canonical `emit()` pattern via `register_adapter`

We use the ecosystem-blessed primitive directly. `auth_profile.emit(target, base)`
layers credentials onto a base config dict; `connection_profile.emit(target)`
produces that base. mountainash-data's only job is to **contribute the ibis-driver
adapters** for its targets.

**Why this is now clean (vs. the earlier "own a bespoke adapter layer" draft).**
auth-client's built-in `emit()` adapters only cover `HTTP`/`BOTO`/`PARAMIKO`, and
the ibis-driver credential shapes are per-dialect (trino wraps creds in
`trino.auth.BasicAuthentication`; bigquery in `google …Credentials`; postgres is
flat `user`/`password`). Two facts make `emit()` the right vehicle anyway:

1. **`emit()` targets are any `Hashable`** — settings stores adapters "under an
   opaque `Hashable` key" precisely so other domains plug in. mountainash-data
   defines its own **namespaced** target type (§3.3) — never bare strings.
2. **`Profile.register_adapter` (the settings primitive) is the sanctioned way to
   add an adapter to an existing profile class** — copy-on-write-safe, conflict-checked.
   mountainash-data registers its dialect adapters onto the auth-client profile
   classes at import. The adapter **functions live in mountainash-data** (they
   `import trino.auth`, `google.oauth2`, …), so **auth-client never depends on a DB
   driver**, and there is no hand-mutation of shared class state.

This keeps the layering honest — auth-client owns the credential schemas + the
`emit()`/registry mechanism; mountainash-data owns the dialect bindings — while
using the canonical primitive end to end.

**OAuth credential seam (forward-compatible).** Deferring the OAuth *lifecycle*
(§10) does not leave the credential path open-ended: the snowflake / pyiceberg-rest
adapters read an **already-resolved** token off the auth profile
(`OAuth2AuthProfile.TOKEN`, `.CLIENT_ID`, …). A future token manager produces a
populated `OAuth2AuthProfile`; the registered adapter is unchanged.

### 3.3 Emission targets — `IbisDialectTarget`

Define a **package-namespaced** target type in mountainash-data (a frozen dataclass
or `Enum`, e.g. `IbisDialectTarget`), never bare strings (settings spec §3.6):

- **`SQL_USERPASS`** — one shared target for the flat user/password backends that
  support only `{Password, NoAuth}`: postgres, mysql, clickhouse, materialize,
  risingwave, druid, singlestoredb, impala, exasol. `PasswordAuthProfile` registers
  **once** for this target → `{"user": …, "password": …}`.
- **Per-dialect targets** where the shape diverges or multiple auth types are
  supported: `TRINO`, `SNOWFLAKE`, `BIGQUERY`, `DATABRICKS`, `REDSHIFT`, `MSSQL`,
  `PYICEBERG_REST`, `MOTHERDUCK`. Each supported `*AuthProfile` registers an adapter
  for that target (e.g. `TRINO`: Password→`BasicAuthentication`, JWT→`JWTAuthentication`,
  Kerberos→`KerberosAuthentication`; `BIGQUERY`: ServiceAccount→`Credentials`;
  `REDSHIFT`: Password + IAM; `MSSQL`: Password + Windows + AzureAD).

Each `*ConnectionProfile` declares its `auth_target` on its `BackendSpec`
(default `SQL_USERPASS`). The no-auth-only backends (sqlite, duckdb, pyspark) use
`SQL_USERPASS` with `supported_auth=(NoAuthProfile,)` and never reach an auth adapter
(short-circuit, §3.5).

### 3.4 Registration

A single import-time module — `core/settings/adapters/register.py` — imports the
driver-binding adapter functions and registers them:

```python
PasswordAuthProfile.register_adapter(IbisDialectTarget.SQL_USERPASS, _sql_userpass)
PasswordAuthProfile.register_adapter(IbisDialectTarget.TRINO, _trino_password)
JWTAuthProfile.register_adapter(IbisDialectTarget.TRINO, _trino_jwt)
ServiceAccountAuthProfile.register_adapter(IbisDialectTarget.BIGQUERY, _bigquery_sa)
IAMAuthProfile.register_adapter(IbisDialectTarget.REDSHIFT, _redshift_iam)
# … one line per (auth-profile, dialect-target) the package supports
```

`core/settings/__init__.py` imports this module so registration happens when the
settings layer loads — before any `connect()`. Each adapter is a module-level
singleton function `(_auth_profile, base) -> dict` (settings spec §3.2 identity
contract), registered on the **concrete** auth-profile class (settings spec §3.3
leaf-registration guidance).

### 3.5 Uniform connect path — no per-backend `__adapter__`, no `_auth_kwargs`

Because the dialect logic now lives in registered `emit()` adapters, the
connection profile's emission is **uniform** across all backends:

```python
def to_driver_kwargs(self, auth_profile: AuthProfile | None = None) -> dict:
    auth = self._normalize_and_validate_auth(auth_profile)   # §6
    target = self.__spec__.auth_target
    base = self.emit(target)                                 # connection config (§3.5.1)
    if isinstance(auth, NoAuthProfile):
        return base                                          # short-circuit (cf. transport)
    return auth.emit(target, base=base)                      # credentials via the registered adapter
```

This **removes** the legacy `__adapter__` indirection, the per-backend
`adapters/*.py` `build_driver_kwargs` modules (their logic becomes the registered
adapters), and the `_auth_kwargs` base method. `emit()`'s fail-closed semantics
give a second guard: `auth.emit(target)` for an (auth-type, dialect) with no
registered adapter raises, complementing the explicit `supported_auth` check.

#### 3.5.1 Two-sided emission — connection shaping vs. credentials

`base = self.emit(target)` is **not** "driver_key only". `emit()` is the same
three-tier pipeline on both sides: driver_key renames → per-target `__adapters__`
2-arg compose → return. So the connection profile owns **all non-auth shaping**,
and the auth profile owns **only credentials** — exactly the
`mountainash-transport` split, where a storage profile emits the SDK config and a
separate `AuthProfile` layers creds onto it (`connections/__init__.py:_emit_kwargs`).

Most backends are pure driver_key renames, so `self.emit(target)` needs no adapter.
The three backends whose connection config is **not a flat rename** carry a
**connection-shaping compose adapter on their own `*ConnectionProfile` class**,
precisely mirroring transport's connection-side adapters:

| Backend | Non-flat connection shaping | Transport precedent |
|---|---|---|
| mysql | nested `ssl={...}` dict from the 5 `SSL_*` fields | `HTTPStorageProfile` → `httpx.Timeout(...)` object |
| mssql | fold `HOST` + `INSTANCE_NAME` → `host\instance`; encryption flags | `SFTPStorageProfile` → `_post_connect` sidecar |
| snowflake | `session_parameters={...}` from `QUERY_TAG`/`TIMEZONE` | `S3StorageProfile` → nested `botocore.Config(...)` |

Because the compose adapter receives the **already-driver_key-renamed dict** as its
second arg (settings spec §3.2), it layers the nested pieces on top of the flat
renames. pyiceberg-rest's dotted keys (`s3.region`, `rest.sigv4-enabled`, `header.*`)
and redshift's `readonly`/`sslmode` are **flat** — handled by `driver_key` alone
(string driver_key may itself contain a dot), no connection adapter.

Two distinct adapter homes, same `register_adapter` primitive:

- **Connection-shaping adapters** register onto mountainash-data's own
  `*ConnectionProfile` classes (data owns them; a class-literal `__adapters__ =
  {target: fn}` à la transport is equivalent). Keyed by the *same* `auth_target`.
- **Auth adapters** register onto auth-client's `*AuthProfile` classes (data does
  **not** own them — this is the case that *requires* the settings primitive).

The shared `SQL_USERPASS` target stays conflict-free: mysql's connection adapter
lives on `MySQLConnectionProfile` only, postgres has none, and both share the one
`PasswordAuthProfile`→`SQL_USERPASS` auth adapter. Different classes, same key.

**MotherDuck is the exception that registers no driver adapter at all:** its token
travels in the connection *string* (`duckdb://md:<db>?motherduck_token=…`, via
`rides_on="duckdb"`), not in driver kwargs. It declares
`supported_auth=(TokenAuthProfile,)` and overrides `to_connection_string` to inject
the token; `to_driver_kwargs` for it returns the flat duckdb base (no auth adapter,
so `auth.emit` is never reached for the token — handled in the URL path).

### 3.6 Auth flow

```
caller ── auth_profile (AuthProfile|None) ──▶ IbisBackend.connect(auth_profile)
                                              │  (also iceberg connect path)
                                              ▼
                       ConnectionProfile.to_driver_kwargs(auth_profile)
                                              │
                    auth = normalize_and_validate(auth_profile)
                    target = spec.auth_target
                    base = self.emit(target)            # config
                              │
                    NoAuth? ──┴── yes ─▶ return base
                              │ no
                    auth.emit(target, base=base)        # registered dialect adapter
                              │  (lives in mountainash-data; builds BasicAuthentication/
                              ▼   Credentials/flat user-password/…)
                    dict ready for the ibis driver
```

---

## 4. Component Changes

### 4.1 `core/settings/__init__.py`
- Replace the `from mountainash_settings.auth import (…)` block with
  `from mountainash_auth_client import (NoAuthProfile, PasswordAuthProfile,
  TokenAuthProfile, JWTAuthProfile, OAuth2AuthProfile, OAuth2AuthCodeAuthProfile,
  OAuth1AuthProfile, IAMAuthProfile, WindowsAuthProfile, AzureADAuthProfile,
  KerberosAuthProfile, CertificateAuthProfile, ServiceAccountAuthProfile,
  AuthProfile)`.
- Update `__all__`: drop old `*Auth`/`AuthSpec` names; add the `*AuthProfile`
  names + `AuthProfile` + `IbisDialectTarget`.
- Import `core/settings/adapters/register.py` so adapters register at load (§3.4).
- Update the `*AuthSettings` re-exports to the renamed `*ConnectionProfile` names (§4.6).

### 4.2 Delete `core/settings/auth/`
Remove `__init__.py`, `base.py`, `dispatch.py` entirely. Verified the only
consumer of `auth_to_driver_kwargs` / `AUTH_TO_DRIVER_KWARGS` is the shim itself
(no src/test references elsewhere), so deletion is safe.

### 4.3 `core/settings/descriptor.py` (`BackendSpec`)
- Add a **required** `supported_auth: tuple[type, ...]` field (no default; typed
  loosely as `type` to avoid importing the union at dataclass-definition time;
  values are `*AuthProfile` classes). Add a registry invariant so an empty
  `supported_auth` fails at import.
- Add an `auth_target: Hashable` field defaulting to `IbisDialectTarget.SQL_USERPASS`.
- No `auth_modes` anywhere (gone with the upstream `ProfileSpec` path).

### 4.4 New: `core/settings/targets.py`
Define `IbisDialectTarget` (frozen dataclass or `Enum`) — the namespaced target
type (§3.3). Exported from `core/settings`.

### 4.5 New: `core/settings/adapters/` becomes the registered-adapter home
- `core/settings/adapters/<dialect>.py` — module-level singleton functions
  `(_auth_profile, base) -> dict` that read UPPERCASE fields (calling `str(...)`
  on `Path | None` fields — `PRIVATE_KEY_PATH`, `FILE`, `KEYTAB` — where the driver
  wants a string) and build the driver kwargs / objects. These hold the same
  per-dialect knowledge as the old `build_driver_kwargs`, minus the `isinstance`
  ladder (one function per (auth-type, dialect)).
- `core/settings/adapters/register.py` — the import-time registration calls (§3.4).
- The old `__adapter__ = staticmethod(_adapter.build_driver_kwargs)` lines on the
  backend classes are **removed**.

### 4.6 `core/settings/profile.py` (`ConnectionProfile`)
- Add `_normalize_and_validate_auth(self, auth_profile) -> AuthProfile`: normalize
  `None` → `NoAuthProfile()`, then `isinstance`-validate against
  `self.__spec__.supported_auth`; raise a clear `ValueError` on miss.
- `to_driver_kwargs(self, auth_profile=None)` — the uniform body in §3.5. (No
  `__adapter__` lookup, no `_auth_kwargs`.)
- `to_connection_string(self, auth_profile=None)`:
  - call `_normalize_and_validate_auth` first;
  - base builds password-style `scheme://user:pass@host:port/db` **only** for
    `PasswordAuthProfile` (read `USERNAME` / `PASSWORD.get_secret_value()`, each
    `quote(..., safe="")`); `NoAuthProfile` → no creds in URL;
  - **any other auth type raises `NotImplementedError`** — token-in-URL backends
    (MotherDuck `md:<db>?motherduck_token=…`, Snowflake, Databricks, Trino-JWT)
    **override** `to_connection_string` in their own module if a URL form is needed.
  - (URLs are not kwargs, so this path does not use `emit()`.)

### 4.7 Rename `*AuthSettings` → `*ConnectionProfile` (20 backends)
Rename across all 20 backend modules, class definitions, `core/settings/__init__.py`
exports, and references. Drop `auth_modes=[…]` from each `BackendSpec(...)`; add
`supported_auth=(…AuthProfile, …)` and (where not `SQL_USERPASS`) `auth_target=…`.

| Old | New |
|---|---|
| `SQLiteAuthSettings` | `SQLiteConnectionProfile` |
| `PostgreSQLAuthSettings` | `PostgreSQLConnectionProfile` |
| … (all 20) | `*ConnectionProfile` |

### 4.8 Entry points
- `backends/ibis/backend.py`: `IbisBackend.connect(self, auth_profile=None)` is the
  single auth entry point. **The settings-backed path must defer auth-dependent
  kwargs assembly to `connect()`** — today `_init_from_settings` eagerly calls
  `to_driver_kwargs()` at `__init__` (backend.py:242), before any `auth_profile`
  exists. Restructure so `__init__`/`_init_from_settings` resolves only the dialect
  + spec and stores `obj_settings`; `connect(auth_profile)` then calls
  `obj_settings.to_driver_kwargs(auth_profile)` and layers `self._config`. The
  direct-dialect path is unaffected.
- **URL credentials vs explicit `auth_profile` precedence:** an explicit
  `connect(auth_profile=…)` **always wins**. URL `user:pass@` is parsed into a
  `PasswordAuthProfile` **only when no explicit `auth_profile` is given**; supplying
  both is a `ValueError`. URL credentials are **stripped** from the URL before it
  reaches `ibis.connect` (credentials travel via the auth profile).
- `backends/iceberg/connection.py`: `connect_default(self, *, auth_profile=None, **kwargs)`
  and `connect`/`get_or_connect` thread `auth_profile` into
  `to_driver_kwargs(auth_profile)`. Precedence: **profile-derived
  `to_driver_kwargs(auth_profile)` < explicit `connection_kwargs`/`**kwargs`**
  (caller overrides win); document on the methods.

### 4.9 Dependency wiring
- `pyproject.toml`: add `mountainash-auth-client` to core `dependencies` (every
  backend needs it). Requires a `mountainash-settings` build that includes
  `Profile.register_adapter` (the prerequisite spec) — ensure the env pins/paths
  resolve to that version.
- `hatch.toml`: add the path dep to all relevant envs (`default`, `dev`, `test`,
  `test_github`, `build_github`, `tower`), mirroring transport:
  `mountainash_auth_client @ {root:uri}/../mountainash-auth-client` (local) and
  `{root:uri}/temp/mountainash-auth-client` (the `*_github` envs).

---

## 5. Field Mapping (old → new), per auth type

Field names change to UPPERCASE; secret-ness preserved. Most are plain renames,
but **path fields are typed `Path | None`** — adapters must `str(...)` them where
the driver expects a string (the current adapters already do, e.g.
`str(auth.private_key_path)`, `str(auth.file)`). Confirmed against both the old
adapter reads and the new `*AuthProfile` `ParameterSpec`s.

| Auth | Old field(s) | New field(s) | Secret / type notes |
|---|---|---|---|
| Password | `username`, `password` | `USERNAME`, `PASSWORD` | PASSWORD secret |
| Token | `token` | `TOKEN` | TOKEN secret |
| JWT | `token` | `TOKEN` | TOKEN secret |
| Kerberos | `service_name`, `principal` | `SERVICE_NAME`, `PRINCIPAL`, `KEYTAB` | `KEYTAB: Path \| None` (new; unused by the trino adapter; for completeness) |
| Windows | `domain`, `username` | `DOMAIN`, `USERNAME` | — |
| AzureAD | `tenant_id`, `client_id`, `client_secret`, `managed_identity`, `msi_endpoint` | `TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET`, `MANAGED_IDENTITY`, `MSI_ENDPOINT` | CLIENT_SECRET secret |
| IAM | `role_arn`, `access_key_id`, `secret_access_key`, `session_token`, `profile_name` | `ROLE_ARN`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`, `SESSION_TOKEN`, `PROFILE_NAME` | SECRET_ACCESS_KEY, SESSION_TOKEN secret |
| ServiceAccount | `info`, `file` | `INFO`, `FILE` | `FILE: Path \| None`; `INFO: dict \| None` |
| OAuth2 | `client_id`, `client_secret`, `token`, `refresh_token`, `server_uri`, `scope` | `CLIENT_ID`, `CLIENT_SECRET`, `TOKEN`, `REFRESH_TOKEN`, `SERVER_URI`, `SCOPE` | CLIENT_SECRET, TOKEN, REFRESH_TOKEN secret |
| Certificate | `private_key`, `private_key_path`, `passphrase` | `PRIVATE_KEY`, `PRIVATE_KEY_PATH`, `PASSPHRASE` | PRIVATE_KEY, PASSPHRASE secret; `PRIVATE_KEY_PATH: Path \| None` |
| NoAuth | — | — | — |

> **Scope:** rows are exactly the auth types consumed by a backend.
> `OAuth1AuthProfile` and `OAuth2AuthCodeAuthProfile` are union members **not
> consumed by any backend** (verified: zero references) — no mapping, not in any
> `supported_auth`. `OAuth2AuthProfile.SERVER_URI`/`SCOPE` are `tier="advanced"`;
> pyiceberg reads `server_uri`/`scope`/`client_id`/`client_secret`/`token` (all
> present); snowflake reads only `token` (present).

---

## 6. Validation & Error Handling

- Shared `_normalize_and_validate_auth(auth_profile)` (§4.6) is called first by
  **both** `to_driver_kwargs` and `to_connection_string`: normalize `None` →
  `NoAuthProfile()`, then `isinstance(auth, tuple(supported_auth))`; on miss raise
  `ValueError(f"{backend} does not support auth: {type(auth).__name__}")`.
  `isinstance` (not exact `type()`) so `*AuthProfile` subclasses are accepted.
- Empty `supported_auth` is impossible: the registry invariant (§4.3) rejects it.
- `emit()` fail-closed gives a second guard: `auth.emit(target)` for an
  (auth-type, dialect) pair with no registered adapter raises — so an auth type
  listed in `supported_auth` but missing its registration is caught loudly, not by
  emitting unauthenticated kwargs.
- `register_adapter` conflict-checks at import (settings spec): a duplicate
  (profile, target) registration with a different function fails at load.

---

## 7. Testing Strategy

- Update ~25 test files: imports → `mountainash_auth_client` (or the `core/settings`
  re-exports); construction → UPPERCASE kwargs
  (`PasswordAuthProfile(USERNAME="u", PASSWORD="p")`); auth passed as a separate
  arg, not an `auth=` field.
- `tests/fixtures/settings_fixtures.py`: yield `(connection_profile, auth_profile)`
  pairs.
- Add focused tests:
  - **Registration:** at import, the expected `(auth-profile, IbisDialectTarget.*)`
    adapters are present (`registered_adapters()` introspection); no cross-pollution
    onto unrelated auth profiles.
  - **Golden per (dialect, auth type):** `auth.emit(target, base=conn.emit(target))`
    yields the exact driver-kwargs dict (trino → `auth=BasicAuthentication(...)`;
    bigquery → `credentials=…`; postgres → `{user, password}`; …). Mirrors
    transport's `test_emission_golden.py`.
  - **Fail-closed:** `auth.emit(target)` for an unsupported (auth, dialect) → raises.
  - `supported_auth` rejection: out-of-`supported_auth` type → `ValueError` (one
    negative test per backend).
  - `None` normalization: no auth → `NoAuthProfile` accepted for no-auth backends,
    rejected for credential-required backends.
  - `isinstance` validation: a subclass of an allowed `*AuthProfile` is accepted.
  - `to_connection_string`: password backend → `user:pass@` (percent-encoded, secret
    unwrapped); token/other type → `NotImplementedError` from the base.
  - Registry invariant: empty `supported_auth` fails at import.
  - URL-vs-explicit precedence: both supplied → `ValueError`; URL-only → creds
    stripped and carried via `PasswordAuthProfile`.
- Acceptance gate: `hatch run test:test` green; `mypy:check` clean; `ruff:check` clean.

---

## 8. Isolation & Interfaces

- **auth-client** — owns credential schemas (`*AuthProfile`) + the `emit()`/registry
  mechanism. Never imports a DB driver. mountainash-data registers adapters onto its
  profile classes via the sanctioned `Profile.register_adapter`.
- **`IbisDialectTarget`** — mountainash-data's namespaced target type; the key that
  ties a connection profile's `emit(target)` to the registered auth adapter.
- **registered adapters** (`core/settings/adapters/<dialect>.py`) — module-level
  singleton `(auth_profile, base) -> dict`; the only place that knows a driver's
  auth-kwarg shape; import the DB drivers; independently testable.
- **`*ConnectionProfile`** — owns backend config + the uniform `to_driver_kwargs` /
  `to_connection_string`; no per-backend auth branching.

---

## 9. Rollout

Depends on the settings `Profile.register_adapter` PR landing first. Then a single
feature branch off mountainash-data `develop` → PR to `develop`. Internally atomic
(the package does not import cleanly until the settings layer is migrated). Suggested
commit slices: (a) deps + re-exports + delete shim; (b) `IbisDialectTarget` +
descriptor (`supported_auth`/`auth_target` + invariant) + uniform `ConnectionProfile`
(`_normalize_and_validate_auth`, `to_driver_kwargs`, `to_connection_string`);
(c) the 20 renames + `supported_auth`/`auth_target`; (d) the dialect adapter
functions + `register.py`; (e) entry points (deferred-auth `IbisBackend`, URL
precedence, iceberg threading); (f) tests.

---

## 10. Backlog (deferred, in-scope to capture)

**Interactive OAuth acquisition & token persistence.** Snowflake (OAuth
authenticator), PyIceberg-REST (OAuth2), and any future OAuth backend currently
consume an **already-obtained** token read statically off the auth profile
(`auth.TOKEN` / `auth.CLIENT_ID`). The decoupled design already lets a caller hand
in a fully-authorized `OAuth2AuthProfile`.

A future capability should integrate the wearables lifecycle so mountainash-data can
**acquire and refresh** tokens itself:
- `OAuth2TokenManager(provider, auth_profile, resolver=…)` for authorize/refresh/revoke.
- `PersistableAuthProfile` (`SETTINGS_SOURCE_SECRETS_PROVIDER` + `persist_key()`)
  + `token_store()` for per-(provider, account) token persistence.
- A `SecretStoreResolver` + `mountainash-secrets` wiring and a named token store.
- Likely a small mountainash-data-side subclass per OAuth backend (à la wearables'
  `WearableOAuth2Auth`) binding the persist identity.

Tracked as a follow-up issue after this migration merges.

---

## 11. Open Questions

None outstanding. (Auth placement = decouple; compat = clean break; rename =
`*ConnectionProfile`; consumption = canonical `emit()` via `register_adapter` with
per-dialect `IbisDialectTarget`; OAuth lifecycle = deferred to §10.)

---

## 12. Revision history

- **v1** — initial design: keep a bespoke per-backend adapter layer, reject `emit()`.
- **v1 Codex review** — incorporated: deferred-auth `IbisBackend` lifecycle; URL/explicit
  precedence; `to_connection_string` token-backend restriction; required +
  `isinstance` `supported_auth`; shared validation helper; field-table gaps (KEYTAB,
  `Path` types, OAuth1/OAuth2AuthCode scope); MotherDuck token handling; per-adapter
  terminal raise; iceberg `connection_kwargs` precedence.
- **v2** — adopt the canonical `emit()` pattern via the new
  `Profile.register_adapter` settings primitive: per-dialect `IbisDialectTarget`,
  adapters registered from mountainash-data onto auth-client profiles, uniform
  `auth.emit(target, base=conn.emit(target))` connect path. Removes the legacy
  `__adapter__` indirection, the per-backend `build_driver_kwargs` modules, and the
  `_auth_kwargs` base method. (Supersedes the v1 §3.2 "reject emit()" decision.)
- **v3 (this revision)** — grounding the plan against the live code surfaced that
  the per-backend `build_driver_kwargs` modules mix auth with **non-flat connection
  shaping** (mysql `ssl={}`, mssql `host\instance` fold, snowflake
  `session_parameters={}`) that `base = self.emit(target)` as "driver_key only"
  cannot produce. Resolved per the established `mountainash-transport` pattern (new
  §3.5.1): connection shaping is a compose adapter on the `*ConnectionProfile` class
  (the SFTP/S3/HTTP precedent), auth stays a separate adapter on the `*AuthProfile`
  class — both via the same `register_adapter` primitive, same `auth_target` key.
  Flat cases (pyiceberg dotted keys, redshift) stay pure `driver_key`. MotherDuck
  registers no driver adapter (token via connection string).
