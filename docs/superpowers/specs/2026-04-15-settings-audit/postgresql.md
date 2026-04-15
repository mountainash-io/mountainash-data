# PostgreSQL Settings Audit

## Header

- **Backend:** postgresql
- **Date checked:** 2026-04-15
- **Our settings class:** `src/mountainash_data/core/settings/postgresql.py` (`PostgreSQLAuthSettings`)
- **Ibis backend file:** `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/postgres/__init__.py` — `do_connect(host=None, user=None, password=None, port=5432, database=None, schema=None, autocommit=True, **kwargs)`
- **Spec URLs (precedence-tagged):**
  - **Driver (authoritative):** libpq connection keyword parameters — https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
  - **Ibis passthrough:** Ibis postgres backend (file above); Ibis uses psycopg (v3); `**kwargs` flows to `psycopg.connect()` which forwards to libpq.

## Stale-link check

| URL | Status |
|---|---|
| https://www.postgresql.org/docs/current/libpq-connect.html | OK |

## Summary counts

- Core missing: **0**
- Core mismatch: **5** (`SSL_MODE` typed `str` not enum; `SSL_*` family typed `bool` when libpq expects strings/paths; `REQUIRE_AUTH` typed `bool` when libpq expects method list; `db_provider_type` returns BIGQUERY; SSL/keepalive/timeout parameters declared but never plumbed)
- Advanced missing: **~20** (documented but commented out: ISOLATION_LEVEL, READONLY, DEFERABLE, AUTOCOMMIT, STATEMENT_TIMEOUT, LOCK_TIMEOUT, IDLE_IN_TRANSACTION_SESSION_TIMEOUT, TARGET_SESSION_ATTRS, LOAD_BALANCE_HOSTS, CLIENT_ENCODING, DATESTYLE, TIMEZONE, GSS_ENCMODE, KRBSRVNAME, SSL_MIN_PROTOCOL_VERSION, SSL_MAX_PROTOCOL_VERSION, HOSTADDR, CONNECT_TIMEOUT, FALLBACK_APPLICATION_NAME, SERVICE)
- Advanced mismatch: **~12** (every field in the class is declared but **not forwarded by `get_connection_kwargs()`** — it only forwards `SCHEMA`)
- Enum coverage: **4 enums defined but unused** — `PostgresTargetSessionAttrs`, `PostgresRequireAuthMethods`, `PostgresSSLCertNegotiation`, `PostgresSSLCertMode`. None referenced by any field.
- Extra: **1** (`ASYNC_MODE` — psycopg uses `autocommit` + async connections differently; no direct libpq keyword)
- Total audited: **~40**
- Stale links: **0**

## Parameter table

Legend — Status: `present` / `missing` / `mismatch` / `extra`. Tier: `core` / `advanced`. Type/Default: ✓ / ✗ / N/A. Ibis passthrough: ✓ / ✗ / unknown.

| Spec name | Our field | Status | Tier | Type ✓ | Default ✓ | Ibis passthrough | Notes |
|---|---|---|---|---|---|---|---|
| `host` | `HOST` (base) | present | core | ✓ | ✓ | ✓ | Plumbed via connection string only. |
| `hostaddr` | — | missing | advanced | N/A | N/A | ✓ (via **kwargs) | Bypasses DNS; useful in high-traffic setups. |
| `port` | `PORT` (override, default `5432`) | present | core | ✓ | ✓ | ✓ | Matches Ibis default. |
| `dbname` | `DATABASE` (base) | present | core | ✓ | ✓ | ✓ | |
| `user` | `USERNAME` (base) | present | core | ✓ | ✓ | ✓ | |
| `password` | `PASSWORD` (base, SecretStr) | present | core | ✓ | ✓ | ✓ | Plumbed via template; SecretStr not unwrapped. |
| `passfile` | `PASSFILE` (Optional[str]) | present | advanced | ✓ | ✓ | ✓ (via **kwargs) | **Not plumbed** — declared but never forwarded. |
| `require_auth` (list of methods) | `REQUIRE_AUTH` (bool, default `True`) | mismatch | core | ✗ | ✗ | ✓ (via **kwargs) | libpq expects a comma/`!`-separated method list (`scram-sha-256`, `md5`, `!password`, etc.); our bool can't express it. The `PostgresRequireAuthMethods` enum exists but isn't used. |
| `channel_binding` (str: `prefer`/`require`/`disable`) | `CHANNEL_BINDING` (Optional[str]) | present | advanced | ✓ | ✓ | ✓ (via **kwargs) | **Not plumbed**. Should be a 3-value enum. |
| `connect_timeout` | — | missing | core | N/A | N/A | ✓ | Common operational knob. |
| `client_encoding` | — (commented out) | missing | advanced | N/A | N/A | ✓ | |
| `options` | `OPTIONS` (Optional[str]) | present | advanced | ✓ | ✓ | ✓ | **Not plumbed**. |
| `application_name` | `APPLICATION_NAME` (Optional[str]) | present | advanced | ✓ | ✓ | ✓ | **Not plumbed**. |
| `fallback_application_name` | — | missing | advanced | N/A | N/A | ✓ | |
| `keepalives` (0/1) | `KEEPALIVES` (bool, default `True`) | mismatch | advanced | ✓ | ✓ | ✓ | **Not plumbed**. libpq expects 0/1 int, pydantic bool will coerce. |
| `keepalives_idle` | `KEEPALIVES_IDLE` (Optional[int]) | present | advanced | ✓ | ✓ | ✓ | **Not plumbed**. |
| `keepalives_interval` | `KEEPALIVES_INTERVAL` (Optional[int]) | present | advanced | ✓ | ✓ | ✓ | **Not plumbed**. |
| `keepalives_count` | `KEEPALIVES_COUNT` (Optional[int]) | present | advanced | ✓ | ✓ | ✓ | **Not plumbed**. |
| `tcp_user_timeout` | `TCP_USER_TIMEOUT` (Optional[int]) | present | advanced | ✓ | ✓ | ✓ | **Not plumbed**. |
| `sslmode` (`disable`/`allow`/`prefer`/`require`/`verify-ca`/`verify-full`) | `SSL_MODE` (str, default `PREFER`) | mismatch | core | ✗ | ✓ | ✓ | Stringly-typed; should be enum (`CONST_DB_SSL_MODE_POSTGRES` exists but isn't a StrEnum constraint). **Not plumbed**. |
| `sslnegotiation` (`postgres`/`direct`) | `SSL_NEGOTIATION` (bool, default `None`) | mismatch | advanced | ✗ | ✗ | ✓ | **Wrong type**: libpq expects string enum; the `PostgresSSLCertNegotiation` enum exists but is unused. **Not plumbed**. |
| `sslcompression` (0/1) | `SSL_COMPRESSION` (bool, default `None`) | present | advanced | ✓ | ✓ | ✓ | **Not plumbed**. |
| `sslcert` (path) | `SSL_CERT` (bool, default `None`) | mismatch | advanced | ✗ | ✗ | ✓ | **Wrong type**: libpq expects a file path string, not bool. **Not plumbed**. |
| `sslkey` (path) | `SSL_KEY` (bool) | mismatch | advanced | ✗ | ✗ | ✓ | Same as above. **Not plumbed**. |
| `sslpassword` (str) | `SSL_PASSWORD` (bool) | mismatch | advanced | ✗ | ✗ | ✓ | Should be SecretStr. **Not plumbed**. |
| `sslcertmode` (`disable`/`allow`/`require`) | `SSL_CERTMODE` (bool) | mismatch | advanced | ✗ | ✗ | ✓ | `PostgresSSLCertMode` enum exists but unused. **Not plumbed**. |
| `sslrootcert` (path) | `SSL_ROOTCERT` (bool) | mismatch | advanced | ✗ | ✗ | ✓ | Path string expected. **Not plumbed**. |
| `sslcrl` (path) | `SSL_CRL` (bool) | mismatch | advanced | ✗ | ✗ | ✓ | Path string expected. **Not plumbed**. |
| `sslcrldir` (path) | `SSL_CRLDIR` (bool) | mismatch | advanced | ✗ | ✗ | ✓ | Path string expected. **Not plumbed**. |
| `sslsni` (0/1) | `SSL_SNI` (bool) | present | advanced | ✓ | ✓ | ✓ | **Not plumbed**. |
| `ssl_min_protocol_version` | — (commented out) | missing | advanced | N/A | N/A | ✓ | |
| `ssl_max_protocol_version` | — (commented out) | missing | advanced | N/A | N/A | ✓ | |
| `gssencmode` | — (commented out) | missing | advanced | N/A | N/A | ✓ | |
| `krbsrvname` | — (commented out) | missing | advanced | N/A | N/A | ✓ | |
| `target_session_attrs` | — (commented out; enum defined) | missing | advanced | N/A | N/A | ✓ | `PostgresTargetSessionAttrs` enum exists with all six libpq values but no field uses it. |
| `load_balance_hosts` | — (commented out) | missing | advanced | N/A | N/A | ✓ | |
| `service` | — | missing | advanced | N/A | N/A | ✓ | pg_service.conf lookup. |
| (session-level SQL, not libpq) | `SEARCH_PATH` (Optional[str]) | extra/advanced | advanced | ✓ | ✓ | ✓ (via `options=-c search_path=...`) | **Not plumbed**. Goes via `options` not as its own keyword. |
| (psycopg-specific) | `ASYNC_MODE` (bool, default `False`) | extra | advanced | N/A | N/A | ✗ | Not a libpq keyword. **Not plumbed**. |
| `autocommit` (Ibis kwarg, default `True`) | — | missing | core | N/A | N/A | ✓ (direct Ibis kwarg) | Ibis `do_connect()` exposes this as a top-level kwarg; we don't. |

### Enum coverage

| Enum | Field that uses it | Intended libpq keyword | Status |
|---|---|---|---|
| `PostgresTargetSessionAttrs` | — | `target_session_attrs` | **Unused** — field is commented out |
| `PostgresRequireAuthMethods` | — | `require_auth` | **Unused** — `REQUIRE_AUTH` is a bool |
| `PostgresSSLCertNegotiation` | — | `sslnegotiation` | **Unused** — `SSL_NEGOTIATION` is a bool |
| `PostgresSSLCertMode` | — | `sslcertmode` | **Unused** — `SSL_CERTMODE` is a bool |

## Findings narrative

**Core gaps:** `connect_timeout` and `autocommit` are both missing from the settings layer; both are operationally important.

**Core mismatches:**

1. **`db_provider_type` returns `CONST_DB_PROVIDER_TYPE.BIGQUERY`** (line 132). Copy-paste bug — should be `POSTGRESQL`. This is a real defect: any code that branches on `db_provider_type` will route PostgreSQL instances through BigQuery paths.
2. **`REQUIRE_AUTH` is a bool**, but libpq's `require_auth` parameter accepts a comma-separated list of acceptable authentication methods (e.g., `"scram-sha-256,md5"` or `"!password"`). A bool cannot express this. The `PostgresRequireAuthMethods` enum already exists with the correct value set — use it.
3. **SSL field types are wrong.** `SSL_CERT`, `SSL_KEY`, `SSL_PASSWORD`, `SSL_CERTMODE`, `SSL_ROOTCERT`, `SSL_CRL`, `SSL_CRLDIR`, `SSL_NEGOTIATION` are all typed `bool`, but libpq expects path strings (for the cert/key/crl ones), enum strings (for `sslnegotiation`, `sslcertmode`), or a password string (for `sslpassword`). Bool here is silently-invalid.
4. **`SSL_MODE` is stringly-typed** rather than enum-constrained. The `CONST_DB_SSL_MODE_POSTGRES` import exists but isn't enforced as a pydantic enum type.
5. **Nothing is plumbed.** `get_connection_kwargs()` forwards only `SCHEMA`. Every other declared field (30+ of them) is validated by pydantic and then silently dropped. The entire PG-specific surface of this class is currently non-functional.

**Advanced gaps:** Roughly 20 commented-out libpq parameters. Several are important (`connect_timeout`, `target_session_attrs`, `hostaddr`, `ssl_min_protocol_version`, `gssencmode`, `client_encoding`). The file contains mostly-completed but commented-out plumbing in `get_connection_kwargs()` — restoring and correcting it would close most of these gaps simultaneously.

**Advanced mismatches:** The un-plumbed fields are the dominant issue, overlapping with the "not plumbed" notes above. Additionally, `SEARCH_PATH` would need to be mapped into `options=-c search_path=...` rather than a bare keyword.

**Extras:** `ASYNC_MODE` has no direct libpq counterpart; psycopg handles async via `async_`/`AsyncConnection` at the client level, not as a connection keyword.

**Stale links:** None.

## Recommended follow-ups

- **Fix `db_provider_type`** to return `CONST_DB_PROVIDER_TYPE.POSTGRESQL`. This is a one-line bug.
- **Retype the four bool fields that should be enums**: `REQUIRE_AUTH` → `List[PostgresRequireAuthMethods]` (or a comma-joined string from them); `SSL_NEGOTIATION` → `PostgresSSLCertNegotiation`; `SSL_CERTMODE` → `PostgresSSLCertMode`; `SSL_MODE` → enum-constrained.
- **Retype the bool-that-should-be-path fields**: `SSL_CERT`, `SSL_KEY`, `SSL_ROOTCERT`, `SSL_CRL`, `SSL_CRLDIR` → `Optional[str]` (or `Optional[UPath]`). `SSL_PASSWORD` → `Optional[SecretStr]`.
- **Plumb all declared fields through `get_connection_kwargs()`** — the method already has mostly-complete commented scaffolding; restore it, but emit libpq keyword names (`sslmode`, `sslcert`, etc.) and normalize bool→0/1 where libpq expects integers.
- **Restore the commented-out fields** for `TARGET_SESSION_ATTRS`, `CONNECT_TIMEOUT`, `CLIENT_ENCODING`, `STATEMENT_TIMEOUT`, etc., or explicitly decide to drop them.
- **Add `AUTOCOMMIT`** as a first-class field (Ibis `do_connect()` takes it directly).
- **Map `SEARCH_PATH`** into `options=-c search_path=...` in `get_connection_kwargs()`.
- **Reconsider `ASYNC_MODE`**: either plumb it to psycopg's `AsyncConnection` path (if ever used) or remove.
- **Secret handling**: ensure `PASSWORD.get_secret_value()` is called at the connection-kwargs boundary.
- **Drop bespoke `ASYNC_MODE` field** unless there's a live consumer; document otherwise.
- **Validate `SSL_MODE` against the 6-value set** using the existing `CONST_DB_SSL_MODE_POSTGRES` enum.
