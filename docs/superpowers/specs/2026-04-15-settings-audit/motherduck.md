# MotherDuck Settings Audit

## Header

- **Backend:** motherduck
- **Date checked:** 2026-04-15
- **Our settings class:** `src/mountainash_data/core/settings/motherduck.py` (`MotherDuckAuthSettings`, inherits `BaseDBAuthSettings`)
- **Ibis backend file:** `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/duckdb/__init__.py` (`do_connect(database=":memory:", read_only=False, extensions=None, **config)`) — MotherDuck rides the **duckdb** Ibis backend; connection string form is `md:<database>?motherduck_token=<token>`.
- **Spec URLs (precedence-tagged):**
  - **Driver (authoritative):** DuckDB Python API + MotherDuck extension — https://motherduck.com/docs/getting-started/connect-query-from-python/installation-authentication/
  - **Ibis passthrough:** Ibis duckdb backend (see file above)
  - **Vendor (context):** https://motherduck.com/docs/authenticating-to-motherduck/

## Stale-link check

| URL | Status |
|---|---|
| https://motherduck.com/docs/getting-started/connect-query-from-python/installation-authentication/ | OK (assumed — vendor doc root stable) |
| https://motherduck.com/docs/authenticating-to-motherduck/ | OK (assumed) |

## Summary counts

- Core missing: **0** (all essentials reachable via connection string)
- Core mismatch: **2** (`DATABASE` validator nullability-inconsistent; `AUTH_METHOD=TOKEN` enforced but docstring says "file-based auth")
- Advanced missing: **All duckdb `**config` options** (inherited passthrough surface not exposed; see duckdb.md — same gap)
- Advanced mismatch: **1** (`ATTACH_PATH` declared but never consumed)
- Extra: **6** (HOST, PORT, SCHEMA, USERNAME, PASSWORD — base-class fields unused by MotherDuck; plus irrelevant base bits)
- Total audited: **~10**
- Stale links: **0**

## Parameter table

Legend — Status: `present` / `missing` / `mismatch` / `extra`. Tier: `core` / `advanced`. Type/Default: ✓ / ✗ / N/A. Ibis passthrough: ✓ / ✗ / unknown.

| Spec name | Our field | Status | Tier | Type ✓ | Default ✓ | Ibis passthrough | Notes |
|---|---|---|---|---|---|---|---|
| `database` (str, `"md:"` or `"md:<db_name>"`) | `DATABASE` (Optional[str]) | present | core | ✓ | ✗ | ✓ (via duckdb) | Required in practice — validator enforces non-None. Users pass bare db name; connection-string template prefixes `md:`. |
| `motherduck_token` (query param on connection string) | `TOKEN` (base, SecretStr) | present | core | ✓ | ✓ | ✓ (via duckdb) | Plumbed via connection-string template `?motherduck_token={token}`. Note: `get_connection_string_params()` passes the SecretStr object itself — depends on upstream stringification. |
| `read_only` (bool) | — (inherited/missing) | missing | core | N/A | N/A | ✓ (via duckdb) | MotherDuck supports read-only attachments; no field exposed. |
| `extensions` (Sequence[str]) | — | missing | advanced | N/A | N/A | ✓ (via duckdb) | Same gap as duckdb report. |
| `config.*` (duckdb config dict) | — | missing | advanced | N/A | N/A | ✓ (via duckdb) | Threads, memory_limit, etc. not exposed here though DuckDB accepts them. |
| (no direct spec equivalent — SQL statement) | `ATTACH_PATH` (Optional[str \| List[str]]) | extra/mismatch | advanced | ✗ | N/A | ✗ | Declared but `get_connection_kwargs()` returns `{}` and `get_post_connection_options()` is `...` (returns None). Dead field. Intended use is presumably post-connect `ATTACH 'md:other_db'`. |
| — | `AUTH_METHOD` (overridden default `TOKEN`) | extra | core | N/A | N/A | ✗ | Local selector. Docstring says "file-based authentication" but the code forces TOKEN — contradictory. |
| — | `HOST`, `PORT`, `SCHEMA`, `USERNAME`, `PASSWORD` (base) | extra | N/A | N/A | N/A | ✗ | MotherDuck has no host/port/user/pass auth. |

## Findings narrative

**Core gaps:** None fatal — `DATABASE` + `TOKEN` cover the minimum needed to reach a MotherDuck instance. `read_only` is unreachable (MotherDuck supports it via DuckDB); add if needed.

**Core mismatches:**

1. **Docstring/AUTH_METHOD contradiction.** The class docstring reads "DuckDB authentication settings" and the inline comment on `AUTH_METHOD` says "DuckDB uses file-based authentication" — but the default is `CONST_DB_AUTH_METHOD.TOKEN` and the model validator enforces `TOKEN is not None` when `AUTH_METHOD == TOKEN`. MotherDuck authentication is genuinely token-based (a MotherDuck JWT); the comment is simply wrong (it was copy-pasted from a DuckDB context). Fix: drop the "file-based" comment, update the class docstring to say "MotherDuck authentication settings (token-based)".
2. **`DATABASE` validator logic.** The validator uses `precondition = True` unconditionally and rejects `None`, yet the field is typed `Optional[str]`. Either the field should be required (`Field(...)`) or the validator should permit `None` (MotherDuck does accept a bare `md:` with no database). The current shape is inconsistent — pydantic will still accept `None` at construction until the validator fires.

**Advanced gaps:** The entire DuckDB `**config` passthrough surface (threads, memory_limit, access_mode, external_threads, etc.) is absent here just as in the duckdb audit. Since MotherDuck connections are DuckDB connections under the hood, the same tuning options apply. Decide whether MotherDuckAuthSettings should inherit/compose DuckDB's tunables rather than sit alongside them. Also absent: `read_only`, `extensions`.

**Advanced mismatches:** `ATTACH_PATH` is declared but neither `get_connection_kwargs()` (returns `{}`) nor `get_post_connection_options()` (body is `...` → returns None) consume it. Either wire it into a post-connect `ATTACH 'md:<name>'` routine, or remove.

**Extras:** Five inherited base-class fields (HOST, PORT, SCHEMA, USERNAME, PASSWORD) are irrelevant to MotherDuck. `AUTH_METHOD` is internally consistent with token auth but commented misleadingly.

**Stale links:** None confirmed stale. Note the class has **no docstring URLs** at all — unlike other settings classes in this tree, there is no `# Source: ...` pointer. Add one to the MotherDuck Python installation/authentication page.

## Recommended follow-ups

- **Add source URLs to the docstring** — this class has none; at minimum reference the MotherDuck installation-authentication page.
- **Fix the "file-based authentication" comment** — MotherDuck is token-based. Replace the stale copy.
- **Resolve `DATABASE` nullability**: either make it required via `Field(...)`, or widen the validator to accept `None` (legal for MotherDuck "no default db" connections).
- **Plumb or remove `ATTACH_PATH`.** If keeping, implement `get_post_connection_options()` to return ATTACH SQL.
- **Decide composition with DuckDB tunables.** MotherDuck accepts the same DuckDB config dict; either share the field set or document that tuning must happen post-connect via SQL `SET` statements.
- **Consider adding `READ_ONLY`** to match the DuckDB class.
- **Drop or validate inherited base-class fields** (HOST/PORT/SCHEMA/USERNAME/PASSWORD) — same base-class composition issue as sqlite/duckdb.
- **Secret handling**: in `get_connection_string_params()`, call `.get_secret_value()` on `TOKEN` explicitly rather than relying on implicit stringification, or verify what the downstream connection-string builder does with SecretStr.
