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
`*AuthProfile` UPPERCASE fields.

We considered the `emit()` route and reject it deliberately. `Profile.emit()` is
generic over any `Hashable` target (not just `TargetFamily`), so mountainash-data
*could* define a `DBTarget` key family and call `auth_profile.emit(DBTarget.X, base=…)`
— transport does exactly this for storage with per-provider `__adapters__`. The
difference that makes it wrong here:

- In transport, the **auth** profile layers credentials onto a base via its *own*
  `__adapters__` (`HTTP`/`BOTO`/`PARAMIKO`) — auth-client owns those adapters. For
  the DB case the credential→driver shape is keyed on **auth-type × ibis-dialect**
  (trino `BasicAuthentication` vs snowflake connector kwargs vs postgres
  `user`/`password`). To route that through `auth_profile.emit(DBTarget.<dialect>)`,
  mountainash-data would have to **register dialect-specific adapters onto
  upstream auth-client classes' `__adapters__`** — action-at-a-distance mutation
  of another package's shared classes. That is worse layering, not better.
- The connection profile's own `emit()` cannot layer credentials either, because
  auth is **decoupled** (§3.1) — the connection profile does not hold the auth.

So the per-backend adapter, reading documented UPPERCASE fields off the passed
`auth_profile`, is the correct seam: auth-client stays a pure credential carrier,
mountainash-data owns the dialect translation, and no package reaches into
another's class internals.

**OAuth credential seam (forward-compatible).** Deferring the OAuth *lifecycle*
(§10) does not leave the credential path open-ended: the snowflake / pyiceberg-rest
adapters read an **already-resolved** token off the auth profile
(`OAuth2AuthProfile.TOKEN`, `.CLIENT_ID`, `.SERVER_URI`, …). A future token manager
produces a populated `OAuth2AuthProfile`; the adapter contract does not change.

### 3.3 Replace the deleted machinery

- **`supported_auth`** (replaces `auth_modes`): each backend declares a
  `supported_auth: tuple[type[AuthProfile], ...]` on its `BackendSpec` (the same
  object that carried `auth_modes` before it was passed upstream). It is
  **required** — `BackendSpec` gives it no default; a registry invariant
  (`spec_invariants_for`, run at import/registration) rejects any backend whose
  `supported_auth` is empty, so a forgotten declaration fails loudly at import,
  never silently at connect. It carries no pydantic-field semantics — it is plain
  backend metadata. Validation is by **`isinstance`** (not exact `type()`) against
  `tuple(supported_auth)`, so legitimate `*AuthProfile` subclasses (e.g. a future
  wearables-style persistence subclass) are accepted.
- **`ConnectionProfile._auth_kwargs(auth_profile)`** (replaces the removed
  upstream `Profile._auth_kwargs`): a mountainash-data base method for the
  **adapter-less, password-or-none** backends (postgres, mysql, clickhouse,
  materialize, risingwave, druid, singlestoredb, impala, exasol, duckdb, sqlite).
  It handles exactly two cases — `NoAuthProfile` → `{}`; `PasswordAuthProfile` →
  generic ibis `{"user": …, "password": …}` — and **raises** for anything else.
  Backends whose adapter-less default does not fit get a **minimal dedicated
  adapter** instead of leaning on the base — notably **MotherDuck**, which is
  token-only (no adapter today) and must not be routed through `_auth_kwargs`
  (§4.6).

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
- Add a **required** `supported_auth: tuple[type, ...]` field (no default; typed
  loosely as `type` to avoid importing the union at dataclass-definition time;
  values are `*AuthProfile` classes). Add a registry invariant so an empty
  `supported_auth` fails at import (§3.3).
- No `auth_modes` anywhere (it was never a `BackendSpec` field locally — it was
  passed through to the upstream `ProfileSpec`; that path is gone).

### 4.4 `core/settings/profile.py` (`ConnectionProfile`)
- Add a shared `_normalize_and_validate_auth(self, auth_profile) -> AuthProfile`
  helper: normalize `None` → `NoAuthProfile()`, then `isinstance`-validate against
  `self.__spec__.supported_auth`; raise a clear `ValueError` on miss
  (`f"{backend} does not support auth: {type(auth_profile).__name__}"`). **Both**
  `to_driver_kwargs` and `to_connection_string` call it first (§6).
- `to_driver_kwargs(self, auth_profile: AuthProfile | None = None)`:
  - `auth = self._normalize_and_validate_auth(auth_profile)`;
  - if `__adapter__` is set, call `adapter(self, auth)`;
  - else `kwargs = self._default_kwargs(); kwargs.update(self._auth_kwargs(auth))`.
- Add `_auth_kwargs(self, auth)` — the restricted base dispatch (NoAuth → `{}`,
  Password → `{"user", "password"}`, else raise) per §3.3.
- `to_connection_string(self, auth_profile: AuthProfile | None = None)`:
  - call `_normalize_and_validate_auth` first;
  - the base builds a password-style `scheme://user:pass@host:port/db` URL **only**
    for `PasswordAuthProfile` (read `USERNAME` / `PASSWORD.get_secret_value()`,
    each wrapped in `quote(..., safe="")`); `NoAuthProfile` → no credentials in URL;
  - **any other auth type raises `NotImplementedError`** — token-in-URL / query-param
    backends (MotherDuck `md:<db>?motherduck_token=…`, Snowflake, Databricks,
    Trino-JWT) are **not** expressible as `user:pass@host` and must **override**
    `to_connection_string` in their own module if a URL form is needed.

### 4.5 `core/settings/adapters/*.py` (7 → 8 adapters)
For each adapter: change `build_driver_kwargs(profile)` →
`build_driver_kwargs(profile, auth)`; re-point imports to `mountainash_auth_client`;
re-point `isinstance` checks to `*AuthProfile`; read UPPERCASE fields (calling
`str(...)` on `Path | None` fields — `PRIVATE_KEY_PATH`, `FILE`, `KEYTAB` — where
the driver wants a string, as the current code already does). **Every** adapter
gains a terminal `else: raise ValueError(f"{backend} adapter does not support auth: …")`
— today only `trino.py` has one; the other 7 (bigquery, databricks, mssql,
mysql, pyiceberg_rest, pyspark, redshift, snowflake) must add it. This is
defense-in-depth behind the central `supported_auth` validation (§6). The mysql
adapter's `profile._auth_kwargs()` call passes the normalized `auth`.

> The central validation makes unauthenticated-kwargs-on-unsupported-auth
> impossible; the per-adapter terminal raise guarantees it even if an adapter is
> reached with an in-`supported_auth` type it doesn't branch on.

### 4.6 Rename `*AuthSettings` → `*ConnectionProfile` (20 backends) + MotherDuck adapter
Now that auth is decoupled, "AuthSettings" is a misnomer. Rename across all 20
backend modules, their class definitions, `core/settings/__init__.py` exports,
and all references. Drop the `auth_modes=[…]` kwarg from each `BackendSpec(...)`
call and replace with `supported_auth=(…AuthProfile, …)`.

**MotherDuck** is token-only (`supported_auth=(TokenAuthProfile,)`) and currently
has no adapter, so it cannot use the password-or-none base `_auth_kwargs`. Add a
**minimal `adapters/motherduck.py`** that reads `auth.TOKEN.get_secret_value()`
into the MotherDuck driver's token kwarg / connection-string token param. (This is
the one new adapter; "7 adapters" elsewhere becomes 8.)

| Old | New |
|---|---|
| `SQLiteAuthSettings` | `SQLiteConnectionProfile` |
| `PostgreSQLAuthSettings` | `PostgreSQLConnectionProfile` |
| … (all 20) | `*ConnectionProfile` |

### 4.7 Entry points
- `backends/ibis/backend.py`: `IbisBackend.connect(self, auth_profile=None)` is the
  single auth entry point. **The settings-backed path must defer auth-dependent
  kwargs assembly to `connect()`** — today `_init_from_settings` eagerly calls
  `to_driver_kwargs()` at `__init__` (backend.py:242), before any `auth_profile`
  exists. Restructure so `__init__`/`_init_from_settings` resolves only the
  dialect + spec and stores `obj_settings`; `connect(auth_profile)` then calls
  `obj_settings.to_driver_kwargs(auth_profile)` and layers `self._config`. The
  direct-dialect path is unaffected (no settings auth).
- **URL credentials vs explicit `auth_profile` precedence:** an explicit
  `connect(auth_profile=…)` **always wins**. URL `user:pass@` is parsed into a
  `PasswordAuthProfile` **only when no explicit `auth_profile` is given**; supplying
  both is a `ValueError`. When credentials come from the URL they are **stripped**
  from the URL before it reaches `ibis.connect` (credentials travel via the auth
  profile, not the URL).
- `backends/iceberg/connection.py`: `connect_default(self, *, auth_profile=None, **kwargs)`
  and `connect`/`get_or_connect` thread `auth_profile` into
  `to_driver_kwargs(auth_profile)`. Define kwargs precedence explicitly:
  **profile-derived `to_driver_kwargs(auth_profile)` < explicit `connection_kwargs`/`**kwargs`**
  (caller overrides win), and document it on the methods.

### 4.8 Dependency wiring
- `pyproject.toml`: add `mountainash-auth-client` to core `dependencies`
  (every backend, even `NoAuthProfile`, needs it).
- `hatch.toml`: add the path dep to all relevant envs (`default`, `dev`, `test`,
  `test_github`, `build_github`, `tower`), mirroring transport:
  `mountainash_auth_client @ {root:uri}/../mountainash-auth-client` (local) and
  `{root:uri}/temp/mountainash-auth-client` (the `*_github` envs).

---

## 5. Field Mapping (old → new), per auth type

Field names change to UPPERCASE; secret-ness preserved. Most are plain renames,
but **path fields are typed `Path | None`** (not strings) — adapters must
`str(...)` them where the driver expects a string (the current adapters already
do, e.g. `str(auth.private_key_path)`, `str(auth.file)`). Confirmed against both
the old adapter reads and the new `*AuthProfile` `ParameterSpec`s.

| Auth | Old field(s) | New field(s) | Secret / type notes |
|---|---|---|---|
| Password | `username`, `password` | `USERNAME`, `PASSWORD` | PASSWORD secret |
| Token | `token` | `TOKEN` | TOKEN secret |
| JWT | `token` | `TOKEN` | TOKEN secret |
| Kerberos | `service_name`, `principal` | `SERVICE_NAME`, `PRINCIPAL`, `KEYTAB` | `KEYTAB: Path \| None` (new field, unused by the trino adapter; listed for completeness) |
| Windows | `domain`, `username` | `DOMAIN`, `USERNAME` | — |
| AzureAD | `tenant_id`, `client_id`, `client_secret`, `managed_identity`, `msi_endpoint` | `TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET`, `MANAGED_IDENTITY`, `MSI_ENDPOINT` | CLIENT_SECRET secret |
| IAM | `role_arn`, `access_key_id`, `secret_access_key`, `session_token`, `profile_name` | `ROLE_ARN`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`, `SESSION_TOKEN`, `PROFILE_NAME` | SECRET_ACCESS_KEY, SESSION_TOKEN secret |
| ServiceAccount | `info`, `file` | `INFO`, `FILE` | `FILE: Path \| None`; `INFO: dict \| None` |
| OAuth2 | `client_id`, `client_secret`, `token`, `refresh_token`, `server_uri`, `scope` | `CLIENT_ID`, `CLIENT_SECRET`, `TOKEN`, `REFRESH_TOKEN`, `SERVER_URI`, `SCOPE` | CLIENT_SECRET, TOKEN, REFRESH_TOKEN secret |
| Certificate | `private_key`, `private_key_path`, `passphrase` | `PRIVATE_KEY`, `PRIVATE_KEY_PATH`, `PASSPHRASE` | PRIVATE_KEY, PASSPHRASE secret; `PRIVATE_KEY_PATH: Path \| None` |
| NoAuth | — | — | — |

> **Scope of this table:** the rows are exactly the auth types consumed by a
> current backend adapter. `OAuth1AuthProfile` and `OAuth2AuthCodeAuthProfile` are
> members of the `AuthProfile` union but are **not consumed by any backend**
> (verified: zero references) — they have no old equivalent and need no mapping.
> They are re-exported as part of the union for completeness, not declared in any
> backend's `supported_auth`.
>
> `OAuth2AuthProfile.SERVER_URI`/`SCOPE` are `tier="advanced"`; pyiceberg's adapter
> reads `server_uri`/`scope`/`client_id`/`client_secret`/`token` — all present.
> Snowflake's adapter reads only `token` from OAuth2 — also present.

---

## 6. Validation & Error Handling

- A single shared helper `_normalize_and_validate_auth(auth_profile)` (§4.4) is
  called first by **both** `to_driver_kwargs` and `to_connection_string`:
  normalize `None` → `NoAuthProfile()`, then `isinstance(auth, tuple(supported_auth))`;
  on miss raise `ValueError(f"{backend} does not support auth: {type(auth).__name__}")`.
  `isinstance` (not exact `type()`) so `*AuthProfile` subclasses are accepted.
  Thus a backend listing `NoAuthProfile` in `supported_auth` accepts `None`; a
  backend requiring credentials (no `NoAuthProfile`) rejects `None` with the same
  clear error.
- Empty `supported_auth` is impossible: the registry invariant (§3.3) rejects it
  at import.
- Defense-in-depth: **every** adapter (all 8) ends with a terminal
  `else: raise ValueError(...)` for an auth type it does not branch on — added in
  this migration (only `trino.py` had one before).

---

## 7. Testing Strategy

- Update ~25 test files: imports → `mountainash_auth_client` (or the
  `core/settings` re-exports); construction → UPPERCASE kwargs
  (`PasswordAuthProfile(USERNAME="u", PASSWORD="p")`); auth passed as a separate
  arg to the connect/`to_driver_kwargs` calls rather than an `auth=` field.
- `tests/fixtures/settings_fixtures.py`: rebuild fixtures to yield
  `(connection_profile, auth_profile)` pairs.
- Add focused tests:
  - `supported_auth` rejection path: **one negative test per backend/adapter**
    feeding an out-of-`supported_auth` auth type → `ValueError` (covers both the
    central validation and each adapter's terminal raise).
  - `None` normalization: `connect()`/`to_driver_kwargs()` with no auth →
    `NoAuthProfile` accepted for no-auth backends; rejected for credential-required
    backends.
  - `isinstance` validation: a subclass of an allowed `*AuthProfile` is accepted.
  - `_auth_kwargs` base dispatch for an adapter-less backend (e.g. postgres) →
    `{user, password}`; and that a non-(NoAuth|Password) type raises there.
  - MotherDuck token adapter: `TokenAuthProfile` → correct token kwarg/URL param.
  - `to_connection_string`: password backend → `user:pass@` (percent-encoded,
    secret unwrapped); token/other type → `NotImplementedError` from the base.
  - Registry invariant: a backend spec with empty `supported_auth` fails at import.
  - URL-vs-explicit precedence: both supplied → `ValueError`; URL-only → creds
    stripped from URL and carried via `PasswordAuthProfile`.
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
slices for reviewability: (a) deps + re-exports + delete shim; (b) descriptor
(`supported_auth` + registry invariant) + profile base
(`_normalize_and_validate_auth`, restricted `_auth_kwargs`, auth-threaded
`to_driver_kwargs`/`to_connection_string`); (c) the 20 backend renames +
`supported_auth`; (d) the 8 adapters (re-point + terminal raise + new MotherDuck
adapter); (e) entry points (deferred-auth restructure of `IbisBackend`, URL
precedence, iceberg threading); (f) tests.

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

---

## 12. Adversarial review (Codex) — incorporated

A Codex design review (2026-06-27) raised, and this spec now resolves:
- **Lifecycle seam** — `IbisBackend` resolved settings (and called `to_driver_kwargs`)
  eagerly at `__init__`, before any `auth_profile`. Fixed: settings path defers
  auth-dependent kwargs to `connect()` (§4.7).
- **URL vs explicit auth precedence** — now defined: explicit wins, both = error,
  URL creds stripped before `ibis.connect` (§4.7).
- **`to_connection_string` for token backends** — base restricted to
  password/none; other types raise `NotImplementedError`, backends override (§4.4).
- **`supported_auth=()` default + exact `type()` check** — now required (registry
  invariant) and validated by `isinstance` (§3.3, §6).
- **Validation only on `to_driver_kwargs`** — shared `_normalize_and_validate_auth`
  used by both entry methods (§4.4, §6).
- **Field table gaps** — Kerberos `KEYTAB`, `Path | None` typing, and the
  OAuth1/OAuth2AuthCode "union-but-unconsumed" scope note added (§5).
- **`_auth_kwargs` mis-applied to token-only MotherDuck** — base restricted to
  NoAuth/Password; MotherDuck gets a dedicated token adapter (§3.3, §4.6).
- **"Adapters keep terminal else" was false** — only trino had one; all 8 adapters
  now add it, with a negative test each (§4.5, §6, §7).
- **Iceberg `connection_kwargs` precedence** — defined: explicit kwargs override
  profile-derived (§4.7).
```
</content>
