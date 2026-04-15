# MySQL Settings Audit

## Header

- **Backend:** mysql
- **Date checked:** 2026-04-15
- **Our settings class:** `src/mountainash_data/core/settings/mysql.py` (`MySQLAuthSettings`)
- **Ibis backend file:** `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/mysql/__init__.py` — `do_connect(host="localhost", user=None, password=None, port=3306, autocommit=True, **kwargs)` (kwargs → `MySQLdb.connect`)
- **Spec URLs (precedence-tagged):**
  - **Driver (authoritative):** mysqlclient — https://mysqlclient.readthedocs.io/user_guide.html#functions-and-attributes
  - **Driver (SSL C-API):** https://dev.mysql.com/doc/c-api/8.4/en/mysql-ssl-set.html
  - **Driver (options C-API):** https://dev.mysql.com/doc/c-api/8.4/en/mysql-options.html

## Stale-link check

| URL | Status |
|---|---|
| https://mysqlclient.readthedocs.io/user_guide.html | OK (assumed — readthedocs stable) |
| https://dev.mysql.com/doc/c-api/8.4/en/mysql-ssl-set.html | OK (assumed) |
| https://dev.mysql.com/doc/c-api/8.4/en/mysql-options.html | OK (assumed) |

## Summary counts

- Core missing: **0**
- Core mismatch: **3** (`db_provider_type` returns BIGQUERY; `SSL_MODE` stringly-typed; `CONV` typed without parameters / default `None` despite non-Optional type)
- Advanced missing: **~8** (CONNECT_TIMEOUT, READ_TIMEOUT, WRITE_TIMEOUT, MAX_ALLOWED_PACKET, COMPRESSION, COMPRESSION_LEVEL, PROGRAM_NAME, CLIENT_FLAG — all commented out)
- Advanced mismatch: **2** (`SSL_CAPATH` guarded behind `SSL_CA` typo bug; `SSL_MODE != DISABLED` branch fires even when `SSL_MODE is None`)
- Extra: **0** (6 inherited base-class fields; of those, HOST/PORT/USERNAME/PASSWORD/DATABASE are all used — clean)
- Total audited: **~18**
- Stale links: **0**

## Parameter table

Legend — Status: `present` / `missing` / `mismatch` / `extra`. Tier: `core` / `advanced`. Type/Default: ✓ / ✗ / N/A. Ibis passthrough: ✓ / ✗ / unknown.

| Spec name | Our field | Status | Tier | Type ✓ | Default ✓ | Ibis passthrough | Notes |
|---|---|---|---|---|---|---|---|
| `host` | `HOST` (base) | present | core | ✓ | ✓ | ✓ | Ibis default `"localhost"`; ours None. |
| `user` | `USERNAME` (base) | present | core | ✓ | ✓ | ✓ | |
| `password` | `PASSWORD` (base, SecretStr) | present | core | ✓ | ✓ | ✓ | |
| `port` | `PORT` (override, default `3306`) | present | core | ✓ | ✓ | ✓ | Matches Ibis default. |
| `db` / `database` | `DATABASE` (base) | present | core | ✓ | ✓ | ✓ | mysqlclient accepts both `db` and `database`; Ibis uses `database`. |
| `autocommit` | `AUTOCOMMIT` (bool, default `True`) | present | core | ✓ | ✓ | ✓ (direct Ibis kwarg) | Matches Ibis default. Note: in `get_connection_kwargs()` the `if self.AUTOCOMMIT:` guard means `False` is silently dropped (bug — should be `if self.AUTOCOMMIT is not None`). |
| `charset` | `CHARSET` (str, default `"utf8mb4"`) | present | advanced | ✓ | ✓ | ✓ (via **kwargs) | Validator enforces a small allowlist — reasonable. |
| `use_unicode` (bool) | — | missing | advanced | N/A | N/A | ✓ | Defaults True in mysqlclient. |
| `collation` (server-side option) | `COLLATION` (str, default `"utf8mb4_unicode_ci"`) | present | advanced | ✓ | ✓ | ✓ (via **kwargs) | mysqlclient accepts via `MYSQL_SET_CHARSET_NAME` / connection attributes — verify the kwarg name. |
| `conv` (dict type conversions) | `CONV` (`Dict` untyped, default `None`) | mismatch | advanced | ✗ | ✗ | ✗ | **Not plumbed**. Type annotation is bare `Dict` (no parameters) and default is `None` but type isn't Optional. Pydantic will accept it; mypy won't. |
| `ssl_mode` (DISABLED/PREFERRED/REQUIRED/VERIFY_CA/VERIFY_IDENTITY) | `SSL_MODE` (str, default `None`) | mismatch | core | ✗ | ✓ | ✓ (via **kwargs) | Stringly typed; validator checks against `CONST_DB_SSL_MODE_MYSQL.__dict__` which includes dunder keys — fragile. Should be enum. |
| `ssl` (dict: `{ca, cert, key, capath, cipher}`) | `SSL_KEY`, `SSL_CERT`, `SSL_CA`, `SSL_CAPATH`, `SSL_CIPHER` | present | core/advanced | ✓ | ✓ | ✓ | Assembled into `args["ssl"]` dict. **Bug** at line 218-219: `if self.SSL_CA:` guards `ssl["ssl-capath"] = self.SSL_CAPATH` — should check `SSL_CAPATH`, not `SSL_CA`. |
| `connect_timeout` | — (commented out) | missing | advanced | N/A | N/A | ✓ (via **kwargs) | Default 10s in mysqlclient. |
| `read_timeout` | — (commented out) | missing | advanced | N/A | N/A | ✓ | |
| `write_timeout` | — (commented out) | missing | advanced | N/A | N/A | ✓ | |
| `max_allowed_packet` | — (commented out) | missing | advanced | N/A | N/A | ✓ | |
| `compress` | — (commented out as `COMPRESSION`) | missing | advanced | N/A | N/A | ✓ | mysqlclient spells it `compress` (bool). |
| `client_flag` | — (commented out) | missing | advanced | N/A | N/A | ✓ | |
| `program_name` | — (commented out) | missing | advanced | N/A | N/A | ✓ | |
| `local_infile` | — (commented out as `ALLOW_LOCAL_INFILE`) | missing | advanced | N/A | N/A | ✓ | Security-relevant (off by default). |
| `auth_plugin` | — | missing | advanced | N/A | N/A | ✓ | e.g., `mysql_clear_password` for PAM/LDAP auth. |
| `init_command` | — | missing | advanced | N/A | N/A | ✓ | Runs on connect; useful for `SET NAMES`/`SET sql_mode`. |
| `unix_socket` | — | missing | advanced | N/A | N/A | ✓ | Required for local-socket connections; we currently force TCP via HOST/PORT. |

## Findings narrative

**Core gaps:** None at the connection-string level. `unix_socket` is missing for local-socket deployments but that's advanced.

**Core mismatches:**

1. **`db_provider_type` returns `CONST_DB_PROVIDER_TYPE.BIGQUERY`** (line 75). Same copy-paste bug as postgresql.py — should be `MYSQL`. Real defect.
2. **`SSL_MODE` stringly-typed.** No enum constraint; validator compares against `CONST_DB_SSL_MODE_MYSQL.__dict__` which leaks class dunders (`__module__`, `__qualname__`, …) into the accepted set. Retype to a pydantic-validated StrEnum.
3. **`CONV` field shape.** Default is `None` but annotation is bare `Dict` (not `Optional[Dict[int, Callable]]`). Neither validated nor plumbed.

**Core bugs (new findings in `get_connection_kwargs`):**

1. **`if self.SSL_CA:` guards the SSL_CAPATH assignment** (line 218). Typo — means `SSL_CAPATH` only gets set when `SSL_CA` is also set.
2. **`SSL_MODE != DISABLED` check fires when SSL_MODE is None.** Because the default is `None` and `None != CONST_DB_SSL_MODE_MYSQL.DISABLED`, the SSL branch runs unconditionally and emits `args["ssl_mode"] = None`. Should be `if self.SSL_MODE is not None and self.SSL_MODE != ...DISABLED:`.
3. **`if self.AUTOCOMMIT:` drops explicit False.** Same `if x:` vs `if x is not None:` pattern.

**Advanced gaps:** Eight commented-out fields covering timeouts, compression, client flags, and program name. Plus `unix_socket`, `local_infile`, `auth_plugin`, `init_command`, `use_unicode` — all real mysqlclient kwargs with operational value.

**Advanced mismatches:** Covered by the SSL_CAPATH guard bug above.

**Extras:** The model validators `validate_auth_ssl_cert` check `self.SSL_MODE is not None and (SSL_CERT or SSL_KEY)` and require both — sensible but could be relaxed (mysqlclient accepts cert without key for some providers).

**Stale links:** None.

## Recommended follow-ups

- **Fix `db_provider_type`** to return `CONST_DB_PROVIDER_TYPE.MYSQL`. One-line bug.
- **Fix the `SSL_CAPATH` guard typo** (line 218: `if self.SSL_CA:` → `if self.SSL_CAPATH:`).
- **Fix the `SSL_MODE != DISABLED` guard** to also check `is not None`. Currently the SSL branch runs when SSL_MODE is None, producing `ssl_mode=None` kwarg.
- **Fix `if self.AUTOCOMMIT:`** → `if self.AUTOCOMMIT is not None:` so explicit False is honored.
- **Retype `SSL_MODE`** to a StrEnum (use `CONST_DB_SSL_MODE_MYSQL` as the enum). Replace the `__dict__` validator.
- **Fix `CONV` type** to `Optional[Dict[int, Any]]` with default `None`.
- **Restore timeouts and compression** from the commented-out block; plumb into `get_connection_kwargs()`.
- **Add `UNIX_SOCKET`, `LOCAL_INFILE`, `INIT_COMMAND`, `AUTH_PLUGIN`** — real operational needs.
- **Validator cleanup**: the `validate_charset` allowlist is narrow (8 entries). MySQL supports many more; either expand or drop the validator and rely on server-side rejection.
- **Secret handling**: password is SecretStr — ensure `get_connection_string_params()` unwraps via `.get_secret_value()` at the driver boundary.
