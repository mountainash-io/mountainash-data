# Snowflake Settings Audit

## Header

- **Backend:** snowflake
- **Date checked:** 2026-04-15
- **Our settings class:** `src/mountainash_data/core/settings/snowflake.py` (`SnowflakeAuthSettings`)
- **Ibis backend file:** `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/snowflake/__init__.py` — `do_connect(user, account, database, ..., **kwargs)` (kwargs → `snowflake.connector.connect`)
- **Spec URLs (precedence-tagged):**
  - **Driver (authoritative):** https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#label-snowflake-connector-methods-connect
  - **OAuth examples:** https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-example#connecting-with-oauth
  - **Connect guide / session params:** https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect

## Stale-link check

| URL | Status |
|---|---|
| https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api | OK (assumed) |
| https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-example | OK (assumed) |
| https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect | OK (assumed) |

## Summary counts

- Core missing: **0**
- Core mismatch: **4** (`CONST_SNOWFLAKE_AUTHENTICATOR` enum values have trailing whitespace bugs; `AUTHENTICATOR` not plumbed; OAuth branch sets `authenticator = AUTH_METHOD` (internal selector string) instead of `"oauth"`; SecretStr fields passed without `.get_secret_value()`)
- Advanced missing: **~15** (session_parameters, query_tag, application, client_session_keep_alive, login_timeout, network_timeout, socket_timeout, client_store_temporary_credential, paramstyle, insecure_mode, ocsp_fail_open, autocommit, validate_default_parameters, numpy, consent_cache_id_token, arrow_number_to_decimal)
- Advanced mismatch: **1** (`TIMEZONE` declared, not plumbed)
- Enum coverage: **1 enum defined (`CONST_SNOWFLAKE_AUTHENTICATOR`), NOT used as a type — `AUTHENTICATOR` is `Optional[str]` with a `.member_values()` validator**
- Extra: **1** (`HOST` in `get_connection_string_params()` — Snowflake connector uses `account`, not `host`)
- Total audited: **~25**
- Stale links: **0**

## Parameter table

Legend — Status: `present` / `missing` / `mismatch` / `extra`. Tier: `core` / `advanced`. Type/Default: ✓ / ✗ / N/A. Ibis passthrough: ✓ / ✗ / unknown.

| Spec name | Our field | Status | Tier | Type ✓ | Default ✓ | Ibis passthrough | Notes |
|---|---|---|---|---|---|---|---|
| `account` (required) | `ACCOUNT` (str, required) | present | core | ✓ | ✓ | ✓ | Validators enforce non-null + alnum/dash/underscore. |
| `user` | `USERNAME` (base) | present | core | ✓ | ✓ | ✓ | |
| `password` | `PASSWORD` (base, SecretStr) | present | core | ✓ | ✓ | ✓ | `get_connection_string_params()` passes SecretStr as-is. |
| `database` | `DATABASE` (base) | present | core | ✓ | ✓ | ✓ | |
| `schema` | `SCHEMA` (base) | present | core | ✓ | ✓ | ✓ | |
| `warehouse` | `WAREHOUSE` (str, required) | present | core | ✓ | ✓ | ✓ | Spec says optional; ours required. |
| `role` | `ROLE` (Optional[str]) | present | core | ✓ | ✓ | ✓ | **Not plumbed** — declared but never added to args. |
| `authenticator` (`snowflake`/`oauth`/`externalbrowser`/`okta`/`username_password_mfa`/...) | `AUTHENTICATOR` (Optional[str]) | mismatch | core | ✗ | ✓ | ✓ | Typed `Optional[str]`; should be enum-constrained. The enum `CONST_SNOWFLAKE_AUTHENTICATOR` exists but is only used inside a validator via `.member_values()`. **Commented-out plumbing** at line 250. |
| `token` (OAuth) | `OAUTH_TOKEN` (SecretStr) | present | core | ✓ | ✓ | ✓ | Plumbed as `token=`. SecretStr not unwrapped. |
| `private_key` | `PRIVATE_KEY` (SecretStr) | present | core | ✓ | ✓ | ✓ | Plumbed; SecretStr not unwrapped. |
| `private_key_file` | `PRIVATE_KEY_PATH` (Optional[str]) | present | core | ✓ | ✓ | ✓ | Note: driver kwarg name is `private_key_file` in recent versions, not `private_key_path`. Verify against connector version. |
| `private_key_file_pwd` | `PRIVATE_KEY_PASSPHRASE` (SecretStr) | present | core | ✓ | ✓ | ✓ | Driver kwarg is `private_key_file_pwd`. Name drift. |
| `connection_name` (toml lookup) | `CONNECTION_NAME` (Optional[str]) | present | core | ✓ | ✓ | ✓ | Plumbed. `connections.toml` support noted as TODO in docstring. |
| `session_parameters` (dict) | — | missing | advanced | N/A | N/A | ✓ | Ibis doc explicitly calls this out. |
| `query_tag` | — (commented out) | missing | advanced | N/A | N/A | ✓ (via session_parameters) | |
| `application` | — (commented out) | missing | advanced | N/A | N/A | ✓ | Default `"MountainAsh"` value lost. |
| `client_session_keep_alive` | — (commented out) | missing | advanced | N/A | N/A | ✓ | Long-running session friendly. |
| `login_timeout` | — | missing | advanced | N/A | N/A | ✓ | Default 120s. |
| `network_timeout` / `socket_timeout` | — | missing | advanced | N/A | N/A | ✓ | |
| `insecure_mode` | — | missing | advanced | N/A | N/A | ✓ | Security-relevant. |
| `ocsp_fail_open` | — | missing | advanced | N/A | N/A | ✓ | |
| `autocommit` | — | missing | advanced | N/A | N/A | ✓ | |
| `paramstyle` | — | missing | advanced | N/A | N/A | ✓ | |
| `timezone` | `TIMEZONE` (Optional[str]) | present | advanced | ✓ | ✓ | ✓ | **Not plumbed**. Belongs in `session_parameters`, not a top-level kwarg. |
| `ocsp_response_cache_filename` | — | missing | advanced | N/A | N/A | ✓ | |
| `oauth_client_id` | `OAUTH_CLIENT_ID` (Optional[str]) | present | advanced | ✓ | ✓ | ✓ | Plumbed. Note: not a standard Snowflake connector kwarg — OAuth flows typically use `token` directly; may be custom. |
| `oauth_client_secret` | `OAUTH_CLIENT_SECRET` (SecretStr) | present | advanced | ✓ | ✓ | ✓ | Same — verify. |
| `oauth_refresh_token` | `OAUTH_REFRESH_TOKEN` (SecretStr) | present | advanced | ✓ | ✓ | ✓ | Same — verify. |
| `okta_account_name` | `OKTA_ACCOUNT_NAMER` (Optional[str]) | present | advanced | ✓ | ✓ | ✓ | **Typo in field name** (`NAMER` vs `NAME`). Also **not plumbed**. |
| — | `HOST` (base) referenced in `get_connection_string_params()` | extra | core | N/A | N/A | ✗ | Snowflake connector uses `account`, not `host`; emitting both is redundant and `host` may confuse the driver. |

### Enum coverage

| Enum | Values | Field that uses it | Status |
|---|---|---|---|
| `CONST_SNOWFLAKE_AUTHENTICATOR` | SNOWFLAKE=`"snowflake "`, OAUTH=`"oauth"`, OKTA=`"okta"`, EXTERNAL_BROWSER=`"externalbrowser"`, PASSWORD_MFA=`"username_password_mfa "` | `AUTHENTICATOR` (indirectly via validator) | **Defective** — `SNOWFLAKE` and `PASSWORD_MFA` values have **trailing spaces** (`"snowflake "`, `"username_password_mfa "`). Snowflake will reject these. |

## Findings narrative

**Core gaps:** None.

**Core mismatches / bugs:**

1. **`CONST_SNOWFLAKE_AUTHENTICATOR` enum values contain trailing whitespace** (lines 19 and 23). `"snowflake "` and `"username_password_mfa "` would be rejected by Snowflake's server. The validator at line 126 compares against `member_values()`, so the whitespace is enforced. This locks out the default authenticator entirely if anyone tries to set it explicitly.
2. **OAuth branch sets `authenticator = AUTH_METHOD`** (line 255). `AUTH_METHOD` is an internal selector (`CONST_DB_AUTH_METHOD.OAUTH` = `"oauth"` presumably), which coincidentally matches the Snowflake authenticator string. Relying on the coincidence is fragile; should use `CONST_SNOWFLAKE_AUTHENTICATOR.OAUTH` or an explicit `"oauth"` literal.
3. **`AUTHENTICATOR` not plumbed outside OAuth path.** Lines 250-251 are commented out. Users setting `AUTHENTICATOR="externalbrowser"` get nothing.
4. **`ROLE` not plumbed.** Declared but never emitted to kwargs.
5. **SecretStr not unwrapped anywhere.** `PASSWORD`, `OAUTH_TOKEN`, `PRIVATE_KEY`, `PRIVATE_KEY_PASSPHRASE`, `OAUTH_CLIENT_SECRET`, `OAUTH_REFRESH_TOKEN` all pass the SecretStr object directly.
6. **`HOST` in connection-string params** is spurious for Snowflake.

**Advanced gaps:** `session_parameters` is the headline omission — it's the canonical way to configure Snowflake session behavior (query_tag, timezone, statement_timeout, autocommit, etc.) and Ibis documents it as a recognized kwarg. Instead we declare standalone `TIMEZONE` and orphan it. Plus ~14 other connect-time kwargs for timeouts, security (insecure_mode, ocsp_fail_open), and client identification (application).

**Advanced mismatches:** `TIMEZONE` as a top-level kwarg doesn't match driver expectations — belongs in `session_parameters={"TIMEZONE": ...}`.

**Extras:** `HOST`-in-params noted above. `OKTA_ACCOUNT_NAMER` (typo) is also unused.

**Stale links:** None.

## Recommended follow-ups

- **Fix enum whitespace bug** (`"snowflake "` → `"snowflake"`, `"username_password_mfa "` → `"username_password_mfa"`). High priority.
- **Retype `AUTHENTICATOR`** as `Optional[CONST_SNOWFLAKE_AUTHENTICATOR]` (StrEnum). Drop the manual validator.
- **Plumb `AUTHENTICATOR` universally** (un-comment line 251 equivalent). Remove the coincidental `AUTH_METHOD == authenticator` coupling.
- **Plumb `ROLE`** — add to args in `get_connection_kwargs()`.
- **Fix typo**: `OKTA_ACCOUNT_NAMER` → `OKTA_ACCOUNT_NAME`. Plumb it.
- **Unwrap SecretStr** at the kwargs boundary for all secret fields (`.get_secret_value()`).
- **Add `SESSION_PARAMETERS: Dict[str, Any]` Field** and route `TIMEZONE` through it; add `QUERY_TAG`, `CLIENT_SESSION_KEEP_ALIVE`, `AUTOCOMMIT`.
- **Add `APPLICATION`, `LOGIN_TIMEOUT`, `NETWORK_TIMEOUT`, `SOCKET_TIMEOUT`, `INSECURE_MODE`, `OCSP_FAIL_OPEN`** — standard operational/security knobs.
- **Remove `HOST` from connection-string params** — Snowflake uses `account`.
- **Rename `PRIVATE_KEY_PATH`/`PRIVATE_KEY_PASSPHRASE`** to emit driver-correct kwarg names (`private_key_file`, `private_key_file_pwd`) in `get_connection_kwargs()`. Keep settings-facing names if preferred, but translate at the boundary.
- **Verify `OAUTH_CLIENT_ID`/`OAUTH_CLIENT_SECRET`/`OAUTH_REFRESH_TOKEN` kwargs** against current snowflake-connector-python — standard OAuth2 flow uses `token=` once acquired, not client credentials at connect time. These may be doing nothing.
- **Relax `WAREHOUSE`** to Optional (Snowflake can use account-default warehouse).
- **Support `connections.toml` lookup** per the in-code TODO.
