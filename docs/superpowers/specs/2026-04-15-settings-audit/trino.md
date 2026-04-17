# Trino Settings Audit

## Header

- **Backend:** trino
- **Date checked:** 2026-04-15
- **Our settings class:** `src/mountainash_data/core/settings/trino.py` (`TrinoAuthSettings`)
- **Ibis backend file:** `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/trino/__init__.py`
- **Spec URLs (precedence-tagged):**
  - **Driver (authoritative):** https://github.com/trinodb/trino-python-client/blob/master/trino/dbapi.py (`trino.dbapi.Connection.__init__`)
  - **Driver user guide (supplementary):** https://github.com/trinodb/trino-python-client

## Stale-link check

| URL | Status |
|---|---|
| https://github.com/trinodb/trino-python-client/blob/master/trino/dbapi.py | OK |
| https://github.com/trinodb/trino-python-client | OK |

## Summary counts

- Core missing: **0**
- Core mismatch: **5** (`AUTH` type; `HTTP_SCHEME` default; `PORT` default; `VERIFY` type narrowed; `PASSWORD` wiring broken)
- Advanced missing: **1** (`encoding`)
- Advanced mismatch: **8** (`SESSION_PROPERTIES`, `HTTP_HEADERS`, `HTTP_SESSION`, `EXTRA_CREDENTIAL`, `CLIENT_TAGS`, `ROLES`, `ISOLATION_LEVEL`, `LEGACY_PREPARED_STATEMENTS` — all typed `Optional[str]` instead of native types)
- Advanced (other): **all advanced fields silently dropped by `get_connection_kwargs()` — only SOURCE, HTTP_SCHEME, PASSWORD are plumbed**
- Extra: **1** (`AUTH_METHOD`, local selector not a driver kwarg)
- Total audited: **~23**
- Stale links: **0**

## Parameter table

Legend — Status: `present` / `missing` / `mismatch` / `extra`. Tier: `core` / `advanced`. Type/Default: ✓ / ✗ / N/A. Ibis passthrough: ✓ / ✗ / unknown.

| Spec name | Our field | Status | Tier | Type ✓ | Default ✓ | Ibis passthrough | Notes |
|---|---|---|---|---|---|---|---|
| `host` (str) | `HOST` (base, Optional[str]) | present | core | ✓ | N/A | ✓ | Driver requires it; Ibis positional. |
| `port` (int) | `PORT` (base, Optional[int]) | mismatch | core | ✓ | ✗ | ✓ | Ibis default `8080`; ours has no Trino-specific default. Users forgetting to set PORT get None routed through connection string. |
| `user` (str) | `USERNAME` (base) | present | core | ✓ | ✓ | ✓ | Ibis default `"user"`; ours None. |
| `catalog` (str) | `CATALOG` | present | core | ✓ | ✓ | ✓ | |
| `schema` (str) | `SCHEMA` (override of base) | present | core | ✓ | ✓ | ✓ | |
| `http_scheme` (str, None) | `HTTP_SCHEME` (default `"https"`) | mismatch | core | ✓ | ✗ | ✓ | Opinionated default. Fine in practice — most production Trino is HTTPS — but document it. |
| `auth` (trino.auth.Authentication) | `AUTH` (Optional[str]) | mismatch | core | ✗ | ✓ | ✓ | **Type wrong**: driver expects a `trino.auth.*` instance (e.g. `BasicAuthentication`, `JWTAuthentication`, `OAuth2Authentication`, `KerberosAuthentication`). Our `Optional[str]` cannot carry one. Settings layer needs a factory that maps `AUTH_METHOD` + credentials → auth instance. |
| `verify` (bool \| str path) | `VERIFY` (Optional[bool], default True) | mismatch | core | ✗ | ✓ | ✓ | Driver accepts `bool` OR a path string to a CA bundle. Ours narrows to bool only; users with custom CA bundles are stuck. |
| `source` (str) | `SOURCE` | present | advanced | ✓ | ✓ | ✓ | Driver default `DEFAULT_SOURCE`; ours None. |
| `session_properties` (dict) | `SESSION_PROPERTIES` (Optional[str]) | mismatch | advanced | ✗ | ✓ | ✓ | Driver expects a dict; string won't deserialize. Also **not plumbed** by `get_connection_kwargs()`. |
| `http_headers` (dict) | `HTTP_HEADERS` (Optional[str]) | mismatch | advanced | ✗ | ✓ | ✓ | Driver expects a dict. **Not plumbed**. |
| `http_session` (requests.Session) | `HTTP_SESSION` (Optional[str]) | mismatch | advanced | ✗ | ✓ | ✓ | Driver expects a `requests.Session` instance. **Not plumbed**. |
| `extra_credential` (List[Tuple[str,str]]) | `EXTRA_CREDENTIAL` (Optional[str]) | mismatch | advanced | ✗ | ✓ | ✓ | **Not plumbed**. |
| `max_attempts` (int) | `MAX_ATTEMPTS` (Optional[int]) | present | advanced | ✓ | ✓ | ✓ | **Not plumbed** by `get_connection_kwargs()`. |
| `request_timeout` (float) | `REQUEST_TIMEOUT` (Optional[int]) | mismatch | advanced | ✗ | ✓ | ✓ | Driver accepts float seconds; int likely OK but narrowing is suboptimal. **Not plumbed**. |
| `isolation_level` (IsolationLevel enum) | `ISOLATION_LEVEL` (Optional[str]) | mismatch | advanced | ✗ | ✓ | ✓ | Driver expects enum. **Not plumbed**. |
| `client_tags` (List[str]) | `CLIENT_TAGS` (Optional[str]) | mismatch | advanced | ✗ | ✓ | ✓ | Driver expects list. **Not plumbed**. |
| `legacy_primitive_types` (bool) | `LEGACY_PRIMITIVE_TYPES` (Optional[bool]) | present | advanced | ✓ | ✓ | ✓ | **Not plumbed**. |
| `legacy_prepared_statements` (Optional[bool]) | `LEGACY_PREPARED_STATEMENTS` (Optional[str]) | mismatch | advanced | ✗ | ✓ | ✓ | Driver expects Optional[bool]; ours is Optional[str]. **Not plumbed**. |
| `roles` (dict \| str) | `ROLES` (Optional[str]) | mismatch | advanced | ✗ | ✓ | ✓ | Driver accepts dict (per-catalog) or str. **Not plumbed**. |
| `timezone` (str) | `TIMEZONE` (Optional[str]) | present | advanced | ✓ | ✓ | ✓ | **Not plumbed**. |
| `encoding` (str \| List[str]) | — | missing | advanced | N/A | N/A | ✓ | Spooling protocol parameter; not modelled. |
| — | `AUTH_METHOD` (base override, default `None`) | extra | core | N/A | N/A | ✗ | Local selector used only to decide whether to pack `password` into kwargs; not a driver param. |
| — | `PASSWORD` (base) | extra/broken | core | ✗ | N/A | ✗ | `get_connection_kwargs()` sets `kwargs["password"] = self.PASSWORD` when `AUTH_METHOD == "password"`. **Driver has no `password=` kwarg.** Must be wrapped in `trino.auth.BasicAuthentication(user, password)` and passed via `auth=`. **Current wiring will fail at connect time.** |

## Findings narrative

**Core gaps:** None (all essential connection parameters are declared as Fields, though not all are plumbed).

**Core mismatches:**

1. **`PASSWORD` wiring is broken.** `get_connection_kwargs()` passes `password` as a bare kwarg, but `trino.dbapi.Connection.__init__` has no `password` parameter. The driver's documented pattern is `auth=BasicAuthentication(user, password)`. This path has likely never been exercised successfully.
2. **`AUTH` typed as `Optional[str]`** but must be a `trino.auth.*` instance. The settings layer needs a mapping from `AUTH_METHOD` (+ credentials / keytab / keystore paths) to an auth object.
3. **`HTTP_SCHEME` defaults to `"https"`** rather than None. Opinionated but reasonable — document it.
4. **`PORT` has no Trino default** (Ibis uses 8080).
5. **`VERIFY` narrows** `bool | str` to `Optional[bool]` — users with a custom CA bundle can't configure it.

**Advanced mismatches:** Eight fields are typed `Optional[str]` where the driver expects dicts, lists, or enum values. On top of that, **`get_connection_kwargs()` only forwards `SOURCE`, `HTTP_SCHEME`, and `PASSWORD`** — every other advanced field (MAX_ATTEMPTS, REQUEST_TIMEOUT, TIMEZONE, SESSION_PROPERTIES, HTTP_HEADERS, HTTP_SESSION, EXTRA_CREDENTIAL, CLIENT_TAGS, ROLES, ISOLATION_LEVEL, LEGACY_PRIMITIVE_TYPES, LEGACY_PREPARED_STATEMENTS, VERIFY) is declared, validated, and then silently dropped.

**Advanced gaps:** `encoding` (spooling protocol) is unmodelled.

**Stale links:** None. Note the user guide link points to the repo root rather than a dedicated "Connection parameters" page — consider referencing `dbapi.py` directly as primary.

## Recommended follow-ups

- **Fix password auth wiring first** — this is blocking real use. Build `trino.auth.BasicAuthentication(USERNAME, PASSWORD)` when `AUTH_METHOD == "password"` and pass as `auth=`.
- **Introduce an auth adapter**: retype `AUTH` to an auth-instance union (or carry it through as a computed property) and add per-method fields (e.g. `KERBEROS_SERVICE_NAME`, `JWT_TOKEN`, `OAUTH_TOKEN`) that the adapter composes.
- **Plumb every advanced Field through `get_connection_kwargs()`** — the current class declares many options but forwards only three.
- **Retype structured fields**: `SESSION_PROPERTIES` → `Dict[str, str]`; `HTTP_HEADERS` → `Dict[str, str]`; `EXTRA_CREDENTIAL` → `List[Tuple[str, str]]`; `CLIENT_TAGS` → `List[str]`; `ROLES` → `Dict[str, str] | str`; `ISOLATION_LEVEL` → enum; `LEGACY_PREPARED_STATEMENTS` → `Optional[bool]`.
- **Widen `VERIFY`** to `Optional[bool | str]`.
- **Add `ENCODING`** field (`Optional[str | List[str]]`).
- **Set `PORT` default** to 8080 for Trino or document why it's None.
- **Document `HTTP_SCHEME="https"` opinion** in the class docstring.
- Replace the user guide URL in the docstring with `https://github.com/trinodb/trino-python-client/blob/master/trino/dbapi.py` as the authoritative reference.
