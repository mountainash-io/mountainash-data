# Design Spec: Migrate mountainash-data to mountainash-auth-client

**Date:** 2026-06-27
**Status:** Draft — for review
**Author:** Nathaniel Ramm (with Claude)

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
- `auth_to_driver_kwargs()` / `AUTH_TO_DRIVER_KWARGS` are gone; profiles expose
  `emit(target, base=…)` over `TargetFamily.{HTTP, BOTO, PARAMIKO}`.

### Project constraints

mountainash-data is **pre-release with zero downstream consumers**. A **clean
break** is required; the goal is the best possible architecture for this
infrastructure package, **not** backward compatibility. No deprecation aliases,
no compat shims.

---

## 2. Goals & Non-Goals

### Goals
1. Unbreak the package against `mountainash-settings` 26.5.0 + `mountainash-auth-client`.
2. Adopt the ecosystem-blessed auth composition pattern (per `mountainash-transport`
   / `mountainash-wearables`): **auth decoupled from the connection profile**.
3. Replace the deleted `auth_modes` / `_auth_kwargs` / `.auth`-field machinery
   with mountainash-data-owned equivalents.
4. Rename the misnamed `*AuthSettings` classes to `*ConnectionProfile`.
5. Make `mountainash-auth-client` a first-class core dependency.
6. All tests green under `hatch run test:test`.

### Non-Goals
- Interactive OAuth **acquisition**/persistence (`OAuth2TokenManager`,
  `PersistableAuthProfile`, `token_store`). Deferred — see §10 Backlog.
- Reworking the Ibis `DialectSpec` registry, inspection model, or iceberg catalog
  registry beyond the auth threading.
- Adding new backends or auth types.

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

`auth_profile` threads through every connect path into
`to_driver_kwargs(auth_profile)` and `to_connection_string(auth_profile)`.

### 3.2 mountainash-data owns the database-driver credential translation

auth-client's `emit()` only renders `HTTP`/`BOTO`/`PARAMIKO` SDK shapes.
mountainash-data's target is the **ibis database driver** — `trino.auth.BasicAuthentication`,
the snowflake connector's `user`/`password`/`token`/`private_key` kwargs,
postgres `user`/`password`, etc. These are **per-dialect** shapes auth-client
deliberately does not cover (there is no "DB" `TargetFamily`).

**Decision:** keep the per-backend adapter layer (`core/settings/adapters/*.py`)
as mountainash-data's database-family translation; re-point it to read the new
`*AuthProfile` UPPERCASE fields. We explicitly reject registering a custom target
into auth-client's `__adapters__`: DB shapes diverge per dialect, so that would
scatter the same adapter functions across upstream auth classes with worse
encapsulation. Keeping translation in mountainash-data preserves clean separation
— auth-client stays a pure credential carrier; mountainash-data is the DB translator.

### 3.3 Replace the deleted machinery

- **`supported_auth`** (replaces `auth_modes`): each backend declares a
  `supported_auth: tuple[type[AuthProfile], ...]` on its `BackendSpec` (the same
  object that carried `auth_modes` before it was passed upstream). Used to
  **validate** the passed `auth_profile` at
  `to_driver_kwargs()` time and reject unsupported types with a clear error
  (formalizing the adapters' existing `else: raise ValueError(...)`). It carries
  no pydantic-field semantics — it is plain backend metadata.
- **`ConnectionProfile._auth_kwargs(auth_profile)`** (replaces the removed
  upstream `Profile._auth_kwargs`): a mountainash-data base method that maps a
  generic credential auth profile (`USERNAME`/`PASSWORD`) onto generic ibis
  `user`/`password` kwargs, for the adapter-less backends (postgres, mysql,
  clickhouse, materialize, risingwave, druid, singlestoredb, impala, exasol,
  duckdb, sqlite, motherduck).

### 3.4 Auth flow

```
caller ── auth_profile (AuthProfile|None) ──▶ IbisBackend.connect(auth_profile)
                                              │  (also iceberg connect path)
                                              ▼
                            ConnectionProfile.to_driver_kwargs(auth_profile)
                                              │
                    ┌─────────────────────────┴──────────────────────────┐
            has __adapter__?                                     no adapter
                    │                                                     │
       adapters/<backend>.build_driver_kwargs(profile, auth_profile)      │
       (isinstance dispatch on *AuthProfile, reads UPPERCASE fields)      │
                    │                                          _default_kwargs()
                    │                                     + _auth_kwargs(auth_profile)
                    └─────────────────────────┬──────────────────────────┘
                                              ▼
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
  names + `AuthProfile`.
- Update the `*AuthSettings` re-exports to the renamed `*ConnectionProfile` names
  (§4.6).

### 4.2 Delete `core/settings/auth/`
Remove `__init__.py`, `base.py`, `dispatch.py` entirely. Verified the only
consumer of `auth_to_driver_kwargs` / `AUTH_TO_DRIVER_KWARGS` is the shim itself
(no src/test references elsewhere), so deletion is safe.

### 4.3 `core/settings/descriptor.py` (`BackendSpec`)
- Add `supported_auth: tuple[type, ...] = ()` field (typed loosely as
  `type` to avoid importing the union at dataclass-definition time; values are
  `*AuthProfile` classes).
- No `auth_modes` anywhere (it was never a `BackendSpec` field locally — it was
  passed through to the upstream `ProfileSpec`; that path is gone).

### 4.4 `core/settings/profile.py` (`ConnectionProfile`)
- `to_driver_kwargs(self, auth_profile: AuthProfile | None = None)`:
  - validate `auth_profile` against `self.__spec__.supported_auth` (raise a clear
    `ValueError` if unsupported);
  - if `__adapter__` is set, call `adapter(self, auth_profile)`;
  - else `kwargs = self._default_kwargs(); kwargs.update(self._auth_kwargs(auth_profile))`.
- Add `_auth_kwargs(self, auth_profile)` (the replacement base dispatch).
- `to_connection_string(self, auth_profile: AuthProfile | None = None)`: read
  `USERNAME`/`PASSWORD` (UPPERCASE) from `auth_profile` instead of
  lowercase `self.auth.username`/`.password`.

### 4.5 `core/settings/adapters/*.py` (7 adapters)
Change each `build_driver_kwargs(profile)` →
`build_driver_kwargs(profile, auth_profile)`; re-point imports to
`mountainash_auth_client`; re-point isinstance checks to `*AuthProfile`; read
UPPERCASE fields. The base `_auth_kwargs` is now called with the passed
`auth_profile` (mysql adapter).

### 4.6 Rename `*AuthSettings` → `*ConnectionProfile` (20 backends)
Now that auth is decoupled, "AuthSettings" is a misnomer. Rename across all 20
backend modules, their class definitions, `core/settings/__init__.py` exports,
and all references. Drop the `auth_modes=[…]` kwarg from each `BackendSpec(...)`
call and replace with `supported_auth=(…AuthProfile, …)`.

| Old | New |
|---|---|
| `SQLiteAuthSettings` | `SQLiteConnectionProfile` |
| `PostgreSQLAuthSettings` | `PostgreSQLConnectionProfile` |
| … (all 20) | `*ConnectionProfile` |

### 4.7 Entry points
- `backends/ibis/backend.py`: `IbisBackend.connect(self, auth_profile=None)`;
  thread `auth_profile` into `_init_from_settings` → `to_driver_kwargs(auth_profile)`.
  For the URL path, when the URL carries `user:pass@`, construct a
  `PasswordAuthProfile(USERNAME=…, PASSWORD=…)` as the auth profile.
- `backends/iceberg/connection.py`: `connect_default(self, *, auth_profile=None, **kwargs)`
  and `connect`/`get_or_connect` thread it into `to_driver_kwargs(auth_profile)`.

### 4.8 Dependency wiring
- `pyproject.toml`: add `mountainash-auth-client` to core `dependencies`
  (every backend, even `NoAuthProfile`, needs it).
- `hatch.toml`: add the path dep to all relevant envs (`default`, `dev`, `test`,
  `test_github`, `build_github`, `tower`), mirroring transport:
  `mountainash_auth_client @ {root:uri}/../mountainash-auth-client` (local) and
  `{root:uri}/temp/mountainash-auth-client` (the `*_github` envs).

---

## 5. Field Mapping (old → new), per auth type

All mappings are 1:1 casing changes; secret-ness preserved. Confirmed against
both the old adapter reads and the new `*AuthProfile` `ParameterSpec`s.

| Auth | Old field(s) | New field(s) | Secret |
|---|---|---|---|
| Password | `username`, `password` | `USERNAME`, `PASSWORD` | PASSWORD |
| Token | `token` | `TOKEN` | TOKEN |
| JWT | `token` | `TOKEN` | TOKEN |
| Kerberos | `service_name`, `principal` | `SERVICE_NAME`, `PRINCIPAL` | — |
| Windows | `domain`, `username` | `DOMAIN`, `USERNAME` | — |
| AzureAD | `tenant_id`, `client_id`, `client_secret`, `managed_identity`, `msi_endpoint` | `TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET`, `MANAGED_IDENTITY`, `MSI_ENDPOINT` | CLIENT_SECRET |
| IAM | `role_arn`, `access_key_id`, `secret_access_key`, `session_token`, `profile_name` | `ROLE_ARN`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`, `SESSION_TOKEN`, `PROFILE_NAME` | SECRET_ACCESS_KEY, SESSION_TOKEN |
| ServiceAccount | `info`, `file` | `INFO`, `FILE` | — |
| OAuth2 | `client_id`, `client_secret`, `token`, `refresh_token`, `server_uri`, `scope` | `CLIENT_ID`, `CLIENT_SECRET`, `TOKEN`, `REFRESH_TOKEN`, `SERVER_URI`, `SCOPE` | CLIENT_SECRET, TOKEN, REFRESH_TOKEN |
| Certificate | `private_key`, `private_key_path`, `passphrase` | `PRIVATE_KEY`, `PRIVATE_KEY_PATH`, `PASSPHRASE` | PRIVATE_KEY, PASSPHRASE |
| NoAuth | — | — | — |

> Note: `OAuth2AuthProfile.SERVER_URI`/`SCOPE` are `tier="advanced"`; pyiceberg's
> adapter reads `server_uri`/`scope`/`client_id`/`client_secret`/`token` — all
> present on `OAuth2AuthProfile`. Snowflake's adapter reads only `token` from
> OAuth2 — also present.

---

## 6. Validation & Error Handling

- `to_driver_kwargs` normalizes `auth_profile=None` to a `NoAuthProfile()`
  instance, then validates `type(auth_profile)` ∈ `supported_auth`; on miss,
  raise `ValueError(f"{backend} does not support auth: {type(auth_profile).__name__}")`.
  Thus a backend that lists `NoAuthProfile` in `supported_auth` accepts `None`;
  a backend that requires credentials (no `NoAuthProfile`) rejects `None` with
  that same clear error.
- Each adapter keeps its terminal `else: raise ValueError(...)` as defense in depth.

---

## 7. Testing Strategy

- Update ~25 test files: imports → `mountainash_auth_client` (or the
  `core/settings` re-exports); construction → UPPERCASE kwargs
  (`PasswordAuthProfile(USERNAME="u", PASSWORD="p")`); auth passed as a separate
  arg to the connect/`to_driver_kwargs` calls rather than an `auth=` field.
- `tests/fixtures/settings_fixtures.py`: rebuild fixtures to yield
  `(connection_profile, auth_profile)` pairs.
- Add focused tests:
  - `supported_auth` rejection path (unsupported auth → `ValueError`).
  - `_auth_kwargs` base dispatch for an adapter-less backend (e.g. postgres) →
    `{user, password}`.
  - One golden per adapter asserting the exact driver-kwargs dict for its
    supported auth types (mirrors transport's `test_emission_golden.py`).
- Acceptance gate: `hatch run test:test` green; `hatch run mypy:check` clean;
  `hatch run ruff:check` clean.

---

## 8. Isolation & Interfaces

- **auth-client** — owns credential schemas (`*AuthProfile`) + `emit()` for
  HTTP/BOTO/PARAMIKO. mountainash-data treats it as a black-box credential carrier.
- **`*ConnectionProfile`** — owns backend config + `to_driver_kwargs(auth_profile)`
  / `to_connection_string(auth_profile)`. Does not know auth internals beyond
  reading documented UPPERCASE fields via adapters.
- **adapters** — pure functions `(profile, auth_profile) -> dict`; the only place
  that knows a specific driver's auth-kwarg shape. Independently testable.

---

## 9. Rollout

Single feature branch off `develop` → PR to `develop` (three-tier flow). The
change is internally atomic (the package does not import cleanly until the whole
settings layer is migrated), so it lands as one reviewed PR. Suggested commit
slices for reviewability: (a) deps + re-exports + delete shim; (b) descriptor +
profile base (`supported_auth`, `_auth_kwargs`, decoupled signatures);
(c) the 20 backend renames + `supported_auth`; (d) the 7 adapters; (e) entry
points; (f) tests.

---

## 10. Backlog (deferred, in-scope to capture)

**Interactive OAuth acquisition & token persistence.** Snowflake (OAuth
authenticator), PyIceberg-REST (OAuth2), and any future OAuth backend currently
consume an **already-obtained** token read statically off the auth profile
(`auth.TOKEN` / `auth.CLIENT_ID`). The decoupled design already lets a caller
hand in a fully-authorized `OAuth2AuthProfile`.

A future capability should integrate the wearables lifecycle so mountainash-data
can **acquire and refresh** tokens itself:
- `OAuth2TokenManager(provider, auth_profile, resolver=…)` for authorize/refresh/revoke.
- `PersistableAuthProfile` (`SETTINGS_SOURCE_SECRETS_PROVIDER` + `persist_key()`)
  + `token_store()` for per-(provider, account) token persistence.
- A `SecretStoreResolver` + `mountainash-secrets` wiring and a named token store.
- Likely a small `mountainash-data`-side subclass per OAuth backend (à la
  wearables' `WearableOAuth2Auth`) binding the persist identity.

Tracked as a follow-up issue after this migration merges. Out of scope here to
keep the migration focused on unbreaking + the decoupled auth model.

---

## 11. Open Questions

None outstanding. (Auth placement = decouple; compat = clean break; rename =
`*ConnectionProfile`; OAuth lifecycle = deferred to §10 — all resolved.)
```
</content>
