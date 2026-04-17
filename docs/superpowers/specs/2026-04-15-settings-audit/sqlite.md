# SQLite Settings Audit

## Header

- **Backend:** sqlite
- **Date checked:** 2026-04-15
- **Our settings class:** `src/mountainash_data/core/settings/sqlite.py` (`SQLiteAuthSettings`, inherits `BaseDBAuthSettings`)
- **Ibis backend file:** `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/sqlite/__init__.py` (`do_connect(database, type_map)`)
- **Spec URLs (precedence-tagged):**
  - **Driver (authoritative):** https://docs.python.org/3/library/sqlite3.html#sqlite3.connect
  - **Ibis passthrough (authoritative for what flows through):** local file above
  - **Vendor (context only — PRAGMAs are post-connect SQL, not connect kwargs):** https://www.sqlite.org/pragma.html

## Stale-link check

| URL | Status |
|---|---|
| https://docs.python.org/3/library/sqlite3.html#sqlite3.connect | OK |
| https://www.sqlite.org/pragma.html | OK |

## Summary counts

- Core missing: **0**
- Core mismatch: **0**
- Advanced missing: **8** (stdlib kwargs not exposed; all are pass-through via Ibis `type_map` is the only advanced we expose)
- Advanced mismatch: **0**
- Extra (ours, not in spec): **7** (base-class fields irrelevant to SQLite: HOST, PORT, SCHEMA, USERNAME, PASSWORD, TOKEN, AUTH_METHOD)
- Total audited: **16**
- Stale links: **0**

## Parameter table

Legend — Status: `present` / `missing` / `mismatch` / `extra`. Tier: `core` / `advanced`. Type/Default: ✓ / ✗ / N/A. Ibis passthrough: ✓ / ✗ / unknown.

| Spec name | Our field | Status | Tier | Type ✓ | Default ✓ | Ibis passthrough | Notes |
|---|---|---|---|---|---|---|---|
| `database` (str \| Path \| None) | `DATABASE` (Optional[str]) | present | core | ✓ | ✓ (both default `None`) | ✓ | Our `get_connection_string_params()` returns `database=UPath(...).expanduser()`. Ibis accepts `str \| Path`. |
| `timeout` (float, 5.0) | — | missing | advanced | N/A | N/A | ✗ | Not in Ibis `do_connect()`; would have to be set via `sqlite3.connect()` directly. Low priority for our workloads. |
| `detect_types` (int, 0) | — | missing | advanced | N/A | N/A | ✗ | Not in Ibis passthrough. Ibis handles type inference itself; adding this would bypass Ibis type handling. |
| `isolation_level` (str \| None, "DEFERRED") | — | missing | advanced | N/A | N/A | ✗ | Ibis manages transactions internally. |
| `check_same_thread` (bool, True) | — | missing | advanced | N/A | N/A | ✗ | Multi-thread use would route via Ibis connection cloning, not this flag. |
| `factory` (Connection subclass) | — | missing | advanced | N/A | N/A | ✗ | Power-user knob; unlikely to need. |
| `cached_statements` (int, 128) | — | missing | advanced | N/A | N/A | ✗ | Performance tuning; no current need. |
| `uri` (bool, False) | — | missing | advanced | N/A | N/A | ✗ | Would enable `file:...?mode=...` connection strings. Potentially useful for read-only/shared-cache DBs. |
| `autocommit` (bool, LEGACY_TRANSACTION_CONTROL) | — | missing | advanced | N/A | N/A | ✗ | Modern transaction control; Ibis does not expose it. |
| `type_map` (dict[str, str \| dt.DataType] \| None) | `TYPE_MAP` (Optional[Dict[str, Any]]) | mismatch | advanced | ✗ | ✓ (both `None`) | ✓ | **Type drift:** ours is `Dict[str, Any]`; Ibis expects values of `str \| ibis.expr.datatypes.DataType`. Passing arbitrary `Any` silently accepts invalid type specs until runtime. |
| — | `HOST` (base) | extra | N/A | N/A | N/A | ✗ | SQLite is file-based; HOST is meaningless here. Not passed through. |
| — | `PORT` (base) | extra | N/A | N/A | N/A | ✗ | SQLite has no network port. |
| — | `SCHEMA` (base) | extra | N/A | N/A | N/A | ✗ | SQLite has no schema concept (single `main` schema + ATTACH). |
| — | `USERNAME` (base) | extra | N/A | N/A | N/A | ✗ | SQLite has no user auth. |
| — | `PASSWORD` (base) | extra | N/A | N/A | N/A | ✗ | SQLite has no user auth. |
| — | `TOKEN` (base) | extra | N/A | N/A | N/A | ✗ | SQLite has no token auth. |
| — | `AUTH_METHOD` (base, default `"none"` overridden in subclass) | extra | N/A | N/A | N/A | ✗ | Subclass correctly sets `"none"`, but the field is still inherited — no runtime harm. |

## Findings narrative

**Core gaps:** None. The single core parameter (`database`) is present with the correct type and default, and Ibis passes it through.

**Core mismatches:** None.

**Advanced gaps:** All eight non-`database` stdlib kwargs (`timeout`, `detect_types`, `isolation_level`, `check_same_thread`, `factory`, `cached_statements`, `uri`, `autocommit`) are absent. None of them are passthroughs via Ibis `do_connect()`, so adding them to our settings class would have no effect without also bypassing Ibis — not worthwhile. The only one that plausibly could matter operationally is **`uri=True`** (to allow `file:...?mode=ro` read-only SQLite attachments); this would require either bypassing Ibis or upstreaming to Ibis.

**Advanced mismatches:** `TYPE_MAP` type drift — ours is `Dict[str, Any]`, Ibis expects `dict[str, str | dt.DataType]`. The `Any` is permissive; fixing it would surface misconfiguration at construction time.

**Extras:** Seven base-class fields (HOST, PORT, SCHEMA, USERNAME, PASSWORD, TOKEN, AUTH_METHOD) bleed into this class by inheritance. None are used by SQLite. They don't break anything, but they clutter config files and let users set meaningless values without warning. This is a base-class composition issue, not a sqlite-specific bug.

**Stale links:** None — both spec URLs return 200.

## Recommended follow-ups

- Tighten `TYPE_MAP` field type from `Dict[str, Any]` to `Dict[str, str | ibis.expr.datatypes.DataType]` (or a narrower union of primitive dtype names).
- Consider a validator on `SQLiteAuthSettings` that warns if any of HOST/PORT/SCHEMA/USERNAME/PASSWORD/TOKEN are set, since SQLite ignores them. (Alternatively, tackle the broader base-class shape in a separate design.)
- If operational need emerges for read-only attachment of SQLite files, investigate upstreaming a `uri=True` passthrough into Ibis, or expose it via a post-connect `ATTACH DATABASE 'file:...?mode=ro'` helper rather than through `sqlite3.connect()` kwargs.
- No changes needed for `DATABASE`.
