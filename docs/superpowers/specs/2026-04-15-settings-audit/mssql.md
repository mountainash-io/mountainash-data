# MSSQL Settings Audit

## Header

- **Backend:** mssql
- **Date checked:** 2026-04-15
- **Our settings class:** `src/mountainash_data/core/settings/mssql.py` (`MSSQLAuthSettings`)
- **Ibis backend file:** `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/mssql/__init__.py` — `do_connect(host="localhost", user=None, password=None, port=1433, database=None, driver=None, **kwargs)` (kwargs → PyODBC)
- **Spec URLs (precedence-tagged):**
  - **Driver (authoritative):** PyODBC connect — https://github.com/mkleehammer/pyodbc/wiki/The-pyodbc-Module#connect
  - **ODBC connection string ref (authoritative for kwargs):** https://learn.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute
  - **Driver install (context):** https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server

## Stale-link check

| URL | Status |
|---|---|
| https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server | OK (assumed — MS Learn stable) |
| https://learn.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute | OK (assumed) |

## Summary counts

- Core missing: **1** (ENCRYPTION — commented out but mandatory for ODBC Driver 18, which is the default)
- Core mismatch: **4** (`get_connection_string_params()` references `AZURE_MANAGED_IDENTITY`/`AZURE_MSI_ENDPOINT` fields that don't exist; `args["server"] += ...` on missing key — NameError; `PASSWORD` returned as SecretStr not unwrapped; `DRIVER`/`PROTOCOL` typed `str` with enum-coerce validator — OK but confusing)
- Advanced missing: **~15** (all commented out: TRUST_SERVER_CERTIFICATE, COLUMN_ENCRYPTION, KEY_STORE_*, LOGIN_TIMEOUT, CONNECTION_TIMEOUT, QUERY_TIMEOUT, POOL_*, PACKET_SIZE, AUTOCOMMIT, ANSI_NULLS, QUOTED_IDENTIFIER, ISOLATION_LEVEL, AZURE_MANAGED_IDENTITY, AZURE_MSI_ENDPOINT)
- Advanced mismatch: **1** (`MARS_ENABLED` declared but not plumbed)
- Enum coverage: **4 enums defined, 2 used (DRIVER, PROTOCOL), 2 unused (ENCRYPTION — commented out; MSSQLAuthMethod — exists but `AUTH_METHOD` uses `CONST_DB_AUTH_METHOD` instead)**
- Extra: **1** (`PROTOCOL` — PyODBC infers from driver, not a standalone kwarg)
- Total audited: **~30**
- Stale links: **0**

## Parameter table

Legend — Status: `present` / `missing` / `mismatch` / `extra`. Tier: `core` / `advanced`. Type/Default: ✓ / ✗ / N/A. Ibis passthrough: ✓ / ✗ / unknown.

| Spec name | Our field | Status | Tier | Type ✓ | Default ✓ | Ibis passthrough | Notes |
|---|---|---|---|---|---|---|---|
| `host` / `server` | `HOST` (base) | present | core | ✓ | ✓ | ✓ | |
| `port` | `PORT` (override, default `1433`) | present | core | ✓ | ✓ | ✓ | Matches Ibis default. |
| `user` / `uid` | `USERNAME` (base) | present | core | ✓ | ✓ | ✓ | |
| `password` / `pwd` | `PASSWORD` (base, SecretStr) | present | core | ✓ | ✓ | ✓ | Passed as SecretStr object; PyODBC stringifies unpredictably. |
| `database` | `DATABASE` (base) | present | core | ✓ | ✓ | ✓ | |
| `driver` | `DRIVER` (str, default `MSSQLDriverType.ODBC` = ODBC Driver 18) | present | core | ✓ | ✓ | ✓ | Validator coerces to enum. ODBC 18 default changes SSL behavior (Encrypt=yes by default) — worth documenting. |
| `encrypt` (yes/no/strict) | — (ENCRYPTION commented out) | missing | core | N/A | N/A | ✓ | **Critical** for ODBC Driver 18 which defaults to `Encrypt=Yes` and fails handshake if server cert isn't trusted. Need ENCRYPTION + TRUST_SERVER_CERTIFICATE. |
| `trustservercertificate` | — (commented out) | missing | core | N/A | N/A | ✓ | Goes hand-in-hand with Encrypt. |
| `trusted_connection` | (derived from `AUTH_METHOD=="windows"`) | present | core | ✓ | ✓ | ✓ | Set to "yes" when Windows auth. |
| `authentication` (for Azure AD) | (derived) | present | core | ✓ | ✓ | ✓ | Set to `ActiveDirectoryMsi` / `ActiveDirectoryServicePrincipal`. |
| `application_name` / `app` | `APP_NAME` (str, default `"MountainAsh"`) | present | advanced | ✓ | ✓ | ✓ | **Not plumbed** (commented out in kwargs). |
| `loginTimeout` | — (commented out) | missing | advanced | N/A | N/A | ✓ | |
| `connectionTimeout` / `timeout` | — (commented out) | missing | advanced | N/A | N/A | ✓ | |
| `MARS_Connection` | `MARS_ENABLED` (bool, default `False`) | present | advanced | ✓ | ✓ | ✓ | **Not plumbed**. |
| `packet_size` | — (commented out) | missing | advanced | N/A | N/A | ✓ | |
| `isolation_level` | — (commented out) | missing | advanced | N/A | N/A | ✓ | |
| `ColumnEncryption` | — (commented out) | missing | advanced | N/A | N/A | ✓ | Enterprise Always Encrypted feature. |
| `KeyStoreAuthentication` / `KeyStorePrincipalId` / `KeyStoreSecret` | — (commented out) | missing | advanced | N/A | N/A | ✓ | |
| Instance name (e.g. `host\SQLEXPRESS`) | `INSTANCE_NAME` (Optional[str]) | present | advanced | ✓ | ✓ | ✓ | **Bug** — see below. |
| — (Azure AD fields) | `WINDOWS_DOMAIN`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` | present | advanced | ✓ | ✓ | ✓ | Used by AUTH_METHOD branches. |
| — | `AZURE_MANAGED_IDENTITY`, `AZURE_MSI_ENDPOINT` | **referenced but undefined** | core | N/A | N/A | N/A | Referenced in `get_connection_string_params()` (lines 334, 336) but **not declared as Fields**. Runtime `AttributeError` when `AUTH_METHOD == "azure_active_directory"`. |
| — | `PROTOCOL` (str, default `MSSQLAuthProtocol.TCP`) | extra | advanced | N/A | N/A | ✗ | Not a PyODBC connection kwarg — protocol is encoded in driver string / server address (e.g. `np:...`, `lpc:...`). **Not plumbed**. |

### Enum coverage

| Enum | Field that uses it | Intended ODBC keyword | Status |
|---|---|---|---|
| `MSSQLDriverType` | `DRIVER` | `DRIVER` | **Used** via `validate_driver` |
| `MSSQLAuthProtocol` | `PROTOCOL` | (no direct ODBC kwarg) | **Used** but field is orphan — not plumbed |
| `MSSQLAuthEncryption` | — (commented out) | `Encrypt` | **Unused** |
| `MSSQLAuthMethod` | — | (internal selector) | **Unused** — the class uses `CONST_DB_AUTH_METHOD` strings instead, and hardcodes `"windows"`/`"azure_active_directory"` literals. Two parallel enum definitions for auth method. |

## Findings narrative

**Core gaps:** `ENCRYPTION` and `TRUST_SERVER_CERTIFICATE` are missing. This matters because the default `DRIVER` is "ODBC Driver 18 for SQL Server", which flipped the `Encrypt` default to `Yes` (strict). Without exposing either encryption mode or trust-server-cert, users connecting to self-signed SQL Servers via Driver 18 will get handshake failures with no settings-layer knob to turn.

**Core mismatches / bugs:**

1. **`AZURE_MANAGED_IDENTITY` and `AZURE_MSI_ENDPOINT` are referenced but not declared.** `get_connection_string_params()` at lines 334 and 336 reads `self.AZURE_MANAGED_IDENTITY` and `self.AZURE_MSI_ENDPOINT`. Neither is a declared Field on this class. Under pydantic v2 `model_config.extra = "forbid"` this raises; otherwise accessing an unset attribute raises `AttributeError`. **Path is untested.**
2. **`args["server"] += f"\\{self.INSTANCE_NAME}"` at line 353.** The dict has no `"server"` key — only `"host"`. When `INSTANCE_NAME` is set, this raises `KeyError`. Either the key should be `"host"`, or `"server"` needs to be initialized first. Either way this code path is broken.
3. **Secret handling inconsistent.** `args["password"] = self.PASSWORD if self.PASSWORD else None` passes the SecretStr object as-is; depending on the ODBC driver's string coercion, this may pass the literal `"**********"` placeholder. Should be `.get_secret_value()`.
4. **Two auth-method enums.** `MSSQLAuthMethod` is defined but unused; the class uses string literals (`"windows"`, `"azure_active_directory"`) and `CONST_DB_AUTH_METHOD.PASSWORD`. Pick one.

**Advanced gaps:** ~15 commented-out fields covering encryption, pool, timeouts, packet size, isolation, and Azure managed identity. Most have mostly-complete commented scaffolding in both `get_connection_string()` and `get_connection_string_params()` — restoring is straightforward.

**Advanced mismatches:** `MARS_ENABLED` is declared but not plumbed (the `MARS_Connection=yes` emission is commented out).

**Extras:** `PROTOCOL` doesn't correspond to any ODBC kwarg — SQL Server protocol is selected via the driver string or by prefixing the server (`np:host`, `lpc:host`). Remove or convert to a prefix-emitter.

**Stale links:** None.

## Recommended follow-ups

- **Declare `AZURE_MANAGED_IDENTITY` and `AZURE_MSI_ENDPOINT` Fields** — the Azure AD code path references them but they don't exist. Blocking for any Azure MSI user.
- **Fix `args["server"] += ...` KeyError** (line 353). Change to `args["host"] += ...` or change the whole class to use `server` consistently (ODBC keyword is `Server`).
- **Add `ENCRYPTION` + `TRUST_SERVER_CERTIFICATE` Fields** and plumb to `Encrypt` / `TrustServerCertificate` ODBC keywords. Required for default Driver 18 connections to non-public-CA servers.
- **Plumb `APP_NAME` and `MARS_ENABLED`** — currently declared but dropped.
- **Restore timeouts** (`LOGIN_TIMEOUT`, `CONNECTION_TIMEOUT`, `QUERY_TIMEOUT`) — basic operational needs.
- **Unwrap SecretStr**: `.get_secret_value()` for `PASSWORD` and `AZURE_CLIENT_SECRET` at the kwargs boundary.
- **Decide on auth-method enum**: either adopt `MSSQLAuthMethod` throughout or delete it in favour of `CONST_DB_AUTH_METHOD` + string literals (which is what the code actually uses).
- **Remove or reimplement `PROTOCOL`** — not a direct ODBC kwarg. If keeping, emit via `Server=np:host,port` style prefix.
- **Implement `get_connection_kwargs()`** — currently returns `{}`. Everything flows via `get_connection_string_params()`, so `get_connection_kwargs()` is presumably dead or reserved. Clarify contract or remove the method.
- **Validator annotations**: `validate_driver`/`validate_protocol` lack `@classmethod`. Works under pydantic v2 but emits a warning.
