# DuckDB Settings Audit

## Header

- **Backend:** duckdb
- **Date checked:** 2026-04-15
- **Our settings class:** `src/mountainash_data/core/settings/duckdb.py` (`DuckDBAuthSettings`, inherits `BaseDBAuthSettings`)
- **Ibis backend file:** `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/duckdb/__init__.py` — `do_connect(database=":memory:", read_only=False, extensions=None, **config)`
- **Spec URLs (precedence-tagged):**
  - **Driver (authoritative):** https://duckdb.org/docs/current/configuration/overview.html (redirect from `/docs/configuration/overview.html`)
  - **Ibis backend (context):** https://ibis-project.org/backends/duckdb
  - **Vendor (context):** https://duckdb.org/docs/extensions/spatial.html

## Stale-link check

| URL | Status |
|---|---|
| https://duckdb.org/docs/configuration/overview.html | redirect → https://duckdb.org/docs/current/configuration/overview.html |
| https://duckdb.org/docs/current/configuration/overview.html | OK |
| https://ibis-project.org/backends/duckdb | OK (assumed — Ibis docs site stable) |
| https://duckdb.org/docs/extensions/spatial.html | OK (assumed — DuckDB docs root stable) |

## Summary counts

- Core missing: **0**
- Core mismatch: **1** (`read_only` default drift)
- Advanced missing: **~12** DuckDB config options unexposed (acceptable via `**config` — see narrative)
- Advanced mismatch: **2** (`MEMORY_LIMIT` regex too narrow; `ATTACH_PATH` not plumbed)
- Extra: **7** (base-class fields irrelevant to embedded DuckDB)
- Total audited: **~22**
- Stale links: **0** (one redirect, not stale)

## Parameter table

Legend — Status: `present` / `missing` / `mismatch` / `extra`. Tier: `core` / `advanced`. Type/Default: ✓ / ✗ / N/A. Ibis passthrough: ✓ / ✗ / unknown.

| Spec name | Our field | Status | Tier | Type ✓ | Default ✓ | Ibis passthrough | Notes |
|---|---|---|---|---|---|---|---|
| `database` (str \| Path, `":memory:"`) | `DATABASE` (Optional[str]) | present | core | ✓ | ✓ | ✓ | `get_connection_string_params()` falls back to `":memory:"` — matches Ibis default. |
| `read_only` (bool, `False`) | `READ_ONLY` (bool, `True`) | mismatch | core | ✓ | ✗ | ✓ | **Default drift**: ours defaults to `True`, Ibis/DuckDB default to `False`. Deliberate opinion or bug? Silent divergence is dangerous — users who expect to write will silently fail with a read-only error. |
| `extensions` (Sequence[str] \| None) | `EXTENSIONS` (List[str], default `[]`) | present | core | ✓ | ✓ | ✓ | Passed via `config["extensions"]` in our `get_connection_kwargs()` — **mismatch**: Ibis accepts `extensions` as a top-level kwarg, not inside `config`. See narrative. |
| `config.threads` (BIGINT, #cores) | `THREADS` (Optional[int]) | present | advanced | ✓ | ✓ | ✓ (via `**config`) | Routed through `config["threads"]`. |
| `config.memory_limit` (VARCHAR, "80% RAM") | `MEMORY_LIMIT` (Optional[str]) | present | advanced | ✓ | ✓ | ✓ (via `**config`) | Regex `^\d+[KMG]B$` rejects legal DuckDB values like `"500MB"` (fine), `"1.5GB"` (rejected — no decimals), `"1024KiB"` (rejected), and the string `"80%"` (percentage form). |
| `config.access_mode` (VARCHAR, "automatic") | — | missing | advanced | N/A | N/A | ✓ (via `**config`) | Overlaps with `read_only`; exposing both invites conflicts. |
| `config.temp_directory` (VARCHAR) | — (commented out in source) | missing | advanced | N/A | N/A | ✓ (via `**config`) | Explicitly commented out. OK to leave — Ibis/DuckDB default is sensible. |
| `config.max_memory` (VARCHAR) | — | missing | advanced | N/A | N/A | ✓ (via `**config`) | Alias of `memory_limit`. |
| `config.external_threads` (UBIGINT, 1) | — | missing | advanced | N/A | N/A | ✓ (via `**config`) | |
| `config.allow_unsigned_extensions` (BOOLEAN, false) | — (commented out) | missing | advanced | N/A | N/A | ✓ (via `**config`) | Security-relevant flag. Consider restoring the field. |
| `config.autoload_known_extensions` (BOOLEAN, true) | — | missing | advanced | N/A | N/A | ✓ (via `**config`) | |
| `config.autoinstall_known_extensions` (BOOLEAN, true) | — | missing | advanced | N/A | N/A | ✓ (via `**config`) | |
| `config.preserve_insertion_order` (BOOLEAN, true) | — | missing | advanced | N/A | N/A | ✓ (via `**config`) | |
| `config.enable_external_access` (BOOLEAN, true) | — | missing | advanced | N/A | N/A | ✓ (via `**config`) | Security-relevant. |
| `config.max_temp_directory_size` (VARCHAR) | — | missing | advanced | N/A | N/A | ✓ (via `**config`) | |
| `config.default_order` (VARCHAR, "ASCENDING") | — | missing | advanced | N/A | N/A | ✓ (via `**config`) | |
| `config.default_null_order` (VARCHAR, "NULLS_LAST") | — | missing | advanced | N/A | N/A | ✓ (via `**config`) | |
| `config.enable_progress_bar` (BOOLEAN, true) | — | missing | advanced | N/A | N/A | ✓ (via `**config`) | Annoying in CI; consider exposing. |
| `config.default_collation` (VARCHAR) | — | missing | advanced | N/A | N/A | ✓ (via `**config`) | |
| — (no spec counterpart) | `ATTACH_PATH` (Optional[str \| List[str]]) | extra/mismatch | advanced | ✗ | N/A | ✗ | **Not plumbed** — declared as a Field but never consumed by `get_connection_kwargs()` or `get_post_connection_options()`. Dead field, or incomplete implementation? |
| — | `HOST` (base) | extra | N/A | N/A | N/A | ✗ | Embedded DuckDB has no host. |
| — | `PORT` (base) | extra | N/A | N/A | N/A | ✗ | No port. |
| — | `SCHEMA` (base) | extra | N/A | N/A | N/A | ✗ | DuckDB uses `catalog.database.schema`; SCHEMA not plumbed. |
| — | `USERNAME` (base) | extra | N/A | N/A | N/A | ✗ | No auth. |
| — | `PASSWORD` (base) | extra | N/A | N/A | N/A | ✗ | No auth. |
| — | `TOKEN` (base) | extra | N/A | N/A | N/A | ✗ | No auth (use `MotherDuckAuthSettings` for tokens). |
| — | `AUTH_METHOD` (base, overridden `"none"`) | extra | N/A | N/A | N/A | ✗ | Correctly set to `"none"`. |

## Findings narrative

**Core gaps:** None.

**Core mismatches:**

1. **`READ_ONLY` default is `True`** (ours) vs `False` (Ibis/DuckDB). This is the single most important finding. Either intentional policy (lock down by default) — in which case it should be documented as such — or a copy-paste bug. Users constructing settings and expecting to write will get silent failures.

2. **`EXTENSIONS` routing**: we pack `extensions` inside `config["extensions"]`, but Ibis `do_connect(extensions=...)` accepts it as a top-level kwarg and passes `config` separately to `duckdb.connect()`. DuckDB itself doesn't document `extensions` as a config key — Ibis handles extension install/load in `_post_connect`. Current wiring likely causes extensions to be silently ignored (DuckDB ignores unknown config keys) or to error. **Needs verification with a live test** before classifying as a bug.

**Advanced gaps:** The `**config` splat in Ibis means any DuckDB config option flows through — so "missing" here means "not exposed as a typed Field", not "unreachable". Twelve commonly-tuned options (access_mode, temp_directory, max_memory, allow_unsigned_extensions, autoload/autoinstall_known_extensions, preserve_insertion_order, enable_external_access, max_temp_directory_size, default_order, default_null_order, enable_progress_bar, default_collation) are unexposed. Priority candidates to add: `allow_unsigned_extensions` (security), `enable_external_access` (security), `temp_directory` (ops), `enable_progress_bar` (CI ergonomics).

**Advanced mismatches:**

1. **`MEMORY_LIMIT` regex** `^\d+[KMG]B$` rejects legal values: decimal quantities (`"1.5GB"`), KiB/MiB/GiB forms, and percentage forms (`"80%"`). DuckDB accepts these; users would hit pydantic validation errors on valid input.
2. **`ATTACH_PATH`** is declared but orphaned — never consumed. Either plumb into a post-connect `ATTACH DATABASE` routine (which `get_post_connection_options()` is set up for but returns `None`), or remove the field.

**Extras:** Seven base-class fields irrelevant to embedded DuckDB. Same composition issue as SQLite.

**Stale links:** None. `/docs/configuration/overview.html` redirects to `/docs/current/configuration/overview.html`; the docstring URL should be updated to the canonical one.

## Recommended follow-ups

- **Investigate `READ_ONLY=True` default.** Decide: keep as opinionated default (document why), or change to `False` to match Ibis/DuckDB. Either way, make the choice explicit in a docstring.
- **Verify extension loading.** Write a test that passes `EXTENSIONS=["httpfs"]` and confirms the extension is actually loaded. If broken, fix by moving `extensions` out of `config` and passing it as a top-level kwarg to Ibis `do_connect()`.
- **Plumb or remove `ATTACH_PATH`.** If keeping, implement `get_post_connection_options()` to return `ATTACH DATABASE` SQL statements.
- **Relax `MEMORY_LIMIT` regex** to `^\d+(\.\d+)?\s*[KMG]i?B$|^\d+%$` (or similar) — test against the DuckDB config docs examples.
- **Expose security-relevant config** as typed Fields: `ALLOW_UNSIGNED_EXTENSIONS`, `ENABLE_EXTERNAL_ACCESS`.
- **Update docstring URL** from `duckdb.org/docs/configuration/overview.html` to `duckdb.org/docs/current/configuration/overview.html`.
- Consider a validator that warns if HOST/PORT/USERNAME/PASSWORD/TOKEN are set on embedded DuckDB.
