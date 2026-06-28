# Dialect-Aware `add_columns` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a dialect-agnostic `IbisBackend.add_columns(name, source)` operation for additive schema evolution, so consumers never hand-roll `ALTER TABLE … ADD COLUMN` DDL or polars→backend type maps.

**Architecture:** A single generic implementation renders DDL off the *live connection's own* Ibis compiler (`compiler.type_mapper` for types — identical to `create_table`; `compiler.dialect` for sqlglot identifier quoting), so it covers SQL backends that expose a sqlglot compiler + `raw_sql` and support `ALTER TABLE … ADD COLUMN` — with no per-dialect code. Verified on duckdb/sqlite; the registry's other SQL dialects (postgres, snowflake, trino, …) are covered by construction but unverified until a consumer exercises them. Dispatch mirrors the existing hook pattern (`upsert_hook` etc.) and adds an `add_columns_hook` *override* seam so a dialect that genuinely cannot `ADD COLUMN` (or needs a quirk) can override the generic default.

**Tech Stack:** Python 3.12, Ibis (SQL backends), sqlglot (transitive via Ibis), polars, hatch + uv test env, pytest, ruff, mypy.

**Spec:** `docs/superpowers/specs/2026-06-27-dialect-aware-add-columns-design.md`

## Global Constraints

- **Additive only.** Adds columns; never drops/renames/re-types existing ones.
- **Idempotent (single-process preflight).** Missing columns are computed against the live table schema once, then one `ALTER` is issued per column. A call that adds nothing is a no-op. NOT concurrency-safe: two writers racing the same new column will collide, and a multi-column add is not atomic on engines without transactional DDL. Acceptable for the single-writer consumer (wearables store); documented as a limitation, not handled here.
- **Identifier contract.** `name` and `database` must each be a *simple* (non-dotted) identifier. Each is quoted as one part; dotted/multi-part qualified names (e.g. `project.dataset`) are out of scope this iteration.
- **Type parity with `create_table`.** Types render via `ibis_conn.compiler.type_mapper.to_string(dtype)` — the exact mapper Ibis uses for `CREATE TABLE`. Never a hand-written type map.
- **Null-typed columns → dialect string.** A candidate column whose inferred dtype is Ibis `null` coerces to `ibis.dtype("string")` before rendering.
- **One ALTER per column.** SQLite allows only one `ADD COLUMN` per statement.
- **Generic default + optional override.** `DialectSpec.add_columns_hook` defaults `None`; when `None`, the generic path runs. No dialect registers a hook initially — the seam exists for dialects that later prove to need an override.
- **Run everything in the hatch test env:** `hatch run test:test-target-quick <path>` (quick, no coverage) for iteration. Never use the stale `.venv`.
- **Targeted test backends:** in-memory `duckdb` and `sqlite` only (no external deps).

---

### Task 1: Add `add_columns_hook` to `DialectSpec`

**Files:**
- Modify: `src/mountainash_data/backends/ibis/dialects/_registry.py` (type aliases ~line 25-32; `DialectSpec` fields ~line 44-47)
- Test: `tests/test_unit/backends/ibis/test_add_columns.py` (create)

**Interfaces:**
- Produces: `AddColumnsHook = t.Callable[..., None]`; `DialectSpec.add_columns_hook: t.Optional[AddColumnsHook] = None`

- [ ] **Step 1: Write the failing test**

Create `tests/test_unit/backends/ibis/test_add_columns.py`. **Import only what each task uses** — later tasks append their own imports — so every intermediate commit stays ruff-clean (no `F401`):

```python
"""Tests for dialect-agnostic add_columns (schema evolution)."""

from mountainash_data.backends.ibis.dialects._registry import DIALECTS, DialectSpec


class TestDialectSpecField:
    def test_add_columns_hook_defaults_none(self):
        spec = DialectSpec(
            ibis_backend_name="duckdb",
            connection_mode="connection_string",
            connection_string_scheme="duckdb://",
        )
        assert spec.add_columns_hook is None

    def test_registered_dialects_have_no_hook_initially(self):
        # The generic path covers every dialect; none registers an override.
        assert DIALECTS["duckdb"].add_columns_hook is None
        assert DIALECTS["sqlite"].add_columns_hook is None
        assert DIALECTS["postgres"].add_columns_hook is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_add_columns.py::TestDialectSpecField -v`
Expected: FAIL — `TypeError: ... unexpected keyword 'add_columns_hook'` or `AttributeError: ... 'add_columns_hook'`.

- [ ] **Step 3: Write minimal implementation**

In `_registry.py`, add the alias next to the other hook aliases (after `RenameTableHook`):

```python
RenameTableHook = t.Callable[..., None]
AddColumnsHook = t.Callable[..., None]
```

And the field in `DialectSpec`, after `rename_table_hook`:

```python
    rename_table_hook: t.Optional[RenameTableHook] = None
    add_columns_hook: t.Optional[AddColumnsHook] = None
    extras: t.Mapping[str, t.Any] = field(default_factory=dict)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_add_columns.py::TestDialectSpecField -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Lint, then commit**

Run `hatch run ruff:check tests/test_unit/backends/ibis/test_add_columns.py` first and fix any finding in the files you touched (every intermediate commit must be ruff-clean). The `ruff:check` script is hardcoded to `./src`, so the test path is appended explicitly — otherwise the new test file is never linted. Then:

```bash
git add src/mountainash_data/backends/ibis/dialects/_registry.py tests/test_unit/backends/ibis/test_add_columns.py
git commit -m "feat(ibis): add add_columns_hook override seam to DialectSpec"
```

---

### Task 2: Source normalization helpers

**Files:**
- Modify: `src/mountainash_data/backends/ibis/operations.py` (imports at top ~line 10-20; new functions appended to the MODULE-LEVEL HELPER FUNCTIONS section)
- Test: `tests/test_unit/backends/ibis/test_add_columns.py`

**Interfaces:**
- Consumes: nothing from prior tasks.
- Produces:
  - `_coerce_dtype(v: t.Any) -> ibis.DataType` — ibis DataType passthrough; ibis type string via `ibis.dtype`; `MountainashDtype` via the canonical bridge; raises `ValueError` for parametric MountainashDtype members.
  - `_normalize_to_schema(source: t.Any) -> ibis.Schema` — `Mapping` → `ibis.schema` of coerced dtypes; otherwise frame → `ibis.memtable(source).schema()`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_unit/backends/ibis/test_add_columns.py` (add these imports just under the existing import at the top of the file, then append the test classes):

```python
# add to the import block at the top of the file:
import ibis
import polars as pl
import pytest

from mountainash.core.dtypes.canonical import MountainashDtype
from mountainash_data.backends.ibis.operations import (
    _coerce_dtype,
    _normalize_to_schema,
)
```

```python
class TestCoerceDtype:
    def test_passes_through_ibis_datatype(self):
        dt = ibis.dtype("float64")
        assert _coerce_dtype(dt) is dt

    def test_from_type_string(self):
        assert _coerce_dtype("float64") == ibis.dtype("float64")

    def test_from_mountainash_scalar_dtype(self):
        assert _coerce_dtype(MountainashDtype.FP64) == ibis.dtype("float64")
        assert _coerce_dtype(MountainashDtype.U8) == ibis.dtype("uint8")

    def test_parametric_mountainash_dtype_raises_valueerror(self):
        with pytest.raises(ValueError, match="parametric"):
            _coerce_dtype(MountainashDtype.LIST)


class TestNormalizeToSchema:
    def test_mapping_of_mixed_dtype_specs(self):
        sch = _normalize_to_schema({"a": "float64", "b": ibis.dtype("int64")})
        assert dict(sch.items()) == dict(
            ibis.schema({"a": "float64", "b": "int64"}).items()
        )

    def test_frame_inference(self):
        sch = _normalize_to_schema(pl.DataFrame({"a": [1], "b": ["x"]}))
        assert set(sch.names) == {"a", "b"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run test:test-target-quick "tests/test_unit/backends/ibis/test_add_columns.py::TestCoerceDtype" "tests/test_unit/backends/ibis/test_add_columns.py::TestNormalizeToSchema" -v`
Expected: FAIL — `ImportError: cannot import name '_coerce_dtype'`.

- [ ] **Step 3: Write minimal implementation**

In `operations.py`, add to the top imports (after the existing `import` lines) — only `ibis` here; `sqlglot` is added in Task 3 where it is first used, keeping this commit ruff-clean:

```python
import ibis
```

Then append to the MODULE-LEVEL HELPER FUNCTIONS section:

```python
def _coerce_dtype(v: t.Any) -> ibis.DataType:
    """Normalize a dtype spec to an ibis DataType.

    Accepts an ibis DataType (passthrough), an ibis type string, or a
    MountainashDtype (resolved via the canonical ibis bridge). Parametric
    MountainashDtype members (LIST/STRUCT) carry no element type and raise.
    """
    if isinstance(v, ibis.DataType):
        return v

    mountainash_dtype = None
    target_ibis = None
    try:
        from mountainash.core.dtypes.canonical import MountainashDtype as _MD
        from mountainash.core.dtypes import target_ibis as _ti

        mountainash_dtype, target_ibis = _MD, _ti
    except Exception:  # mountainash build without the canonical dtypes bridge
        pass

    if mountainash_dtype is not None and isinstance(v, mountainash_dtype):
        # Gate parametric members explicitly via the canonical bridge's own
        # CAST_UNSUPPORTED set (currently {LIST, STRUCT}) rather than relying
        # on ibis.dtype() to reject a bare "array"/"struct" string.
        if v in target_ibis.CAST_UNSUPPORTED:
            raise ValueError(
                f"MountainashDtype.{v.name} is a parametric type with no "
                f"element types; pass an ibis DataType or use the frame form "
                f"for nested columns."
            )
        return ibis.dtype(target_ibis.SCHEMA_TYPES[v])

    return ibis.dtype(v)


def _normalize_to_schema(source: t.Any) -> ibis.Schema:
    """Resolve `source` to a candidate ibis Schema.

    A Mapping of ``{name: dtype}`` is coerced per-value; any other object is
    treated as a frame and run through Ibis's native inference (identical to
    what ``create_table`` applies).
    """
    if isinstance(source, t.Mapping):
        return ibis.schema({k: _coerce_dtype(v) for k, v in source.items()})
    return ibis.memtable(source).schema()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `hatch run test:test-target-quick "tests/test_unit/backends/ibis/test_add_columns.py::TestCoerceDtype" "tests/test_unit/backends/ibis/test_add_columns.py::TestNormalizeToSchema" -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Lint, then commit**

Run `hatch run ruff:check tests/test_unit/backends/ibis/test_add_columns.py` and fix any finding in the files you touched. Then:

```bash
git add src/mountainash_data/backends/ibis/operations.py tests/test_unit/backends/ibis/test_add_columns.py
git commit -m "feat(ibis): add dtype/schema normalization helpers for add_columns"
```

---

### Task 3: Generic `_generic_add_columns` implementation

**Files:**
- Modify: `src/mountainash_data/backends/ibis/operations.py` (append after the normalization helpers)
- Test: `tests/test_unit/backends/ibis/test_add_columns.py`

**Interfaces:**
- Consumes: `_normalize_to_schema` (Task 2).
- Produces: `_generic_add_columns(ibis_conn, table_name, source, *, database=None) -> None` — additive, idempotent column adder operating on a *raw ibis connection* (`ibis_conn`, i.e. the `IbisConnection._ibis_conn`).

- [ ] **Step 1: Write the failing test**

Append to `tests/test_unit/backends/ibis/test_add_columns.py`:

```python
from mountainash_data.backends.ibis.operations import _generic_add_columns


class TestGenericAddColumns:
    def test_adds_missing_column_from_frame_duckdb(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1], "name": ["a"]}))
        _generic_add_columns(
            con, "t", pl.DataFrame({"id": [1], "name": ["a"], "score": [1.5]})
        )
        assert "score" in con.table("t").schema().names

    def test_idempotent_second_call_is_noop(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1]}))
        _generic_add_columns(con, "t", {"x": "float64"})
        _generic_add_columns(con, "t", {"x": "float64"})
        assert list(con.table("t").schema().names).count("x") == 1

    def test_null_typed_column_becomes_string(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1]}))
        _generic_add_columns(
            con, "t",
            pl.DataFrame({"id": [1], "note": pl.Series([None], dtype=pl.Null)}),
        )
        assert str(con.table("t").schema()["note"]) == "string"

    def test_quotes_identifiers_needing_quoting(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1]}))
        _generic_add_columns(con, "t", {"new col": "float64"})
        assert "new col" in con.table("t").schema().names

    def test_works_on_sqlite(self):
        con = ibis.sqlite.connect()
        con.create_table("t", pl.DataFrame({"id": [1]}))
        _generic_add_columns(con, "t", {"score": "float64"})
        assert "score" in con.table("t").schema().names

    def test_rejects_dotted_table_name(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1]}))
        with pytest.raises(ValueError, match="simple"):
            _generic_add_columns(con, "schema.t", {"x": "float64"})

    def test_rejects_dotted_database(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1]}))
        with pytest.raises(ValueError, match="simple"):
            _generic_add_columns(con, "t", {"x": "float64"}, database="a.b")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run test:test-target-quick "tests/test_unit/backends/ibis/test_add_columns.py::TestGenericAddColumns" -v`
Expected: FAIL — `ImportError: cannot import name '_generic_add_columns'`.

- [ ] **Step 3: Write minimal implementation**

First add the `sqlglot` import to `operations.py`'s top imports (first use is here, so it lands in this commit ruff-clean):

```python
from sqlglot import exp
```

Then append to `operations.py` (the validator first, then the main function):

```python
def _validate_simple_identifier(value: str, *, kind: str) -> None:
    """Reject dotted/multi-part names — only simple identifiers are supported.

    A dotted ``table_name``/``database`` would otherwise be quoted as a single
    literal identifier (``"a.b"``) rather than a namespace, silently violating
    the documented contract. Fail loudly instead.
    """
    if value is not None and "." in value:
        raise ValueError(
            f"{kind} {value!r} must be a simple (non-dotted) identifier; "
            f"multi-part qualified names are out of scope."
        )


def _generic_add_columns(
    ibis_conn: t.Any,
    table_name: str,
    source: t.Any,
    *,
    database: str | None = None,
) -> None:
    """Add columns present in `source` but missing from `table_name`.

    Additive and idempotent (single-process preflight: missing columns are
    computed once, then one ALTER is issued per column — not concurrency-safe
    and not atomic across columns on engines without transactional DDL).
    Column types render through the connection's own compiler type-mapper
    (identical to ``create_table``); a null-typed column coerces to the
    dialect string type; identifiers are quoted per dialect. One ``ALTER
    TABLE … ADD COLUMN`` is issued per new column (SQLite permits only one per
    statement).

    `table_name` and `database` must each be a simple (non-dotted) identifier;
    each is quoted as a single part. Dotted/multi-part qualified names are out
    of scope.
    """
    _validate_simple_identifier(table_name, kind="table_name")
    if database is not None:
        _validate_simple_identifier(database, kind="database")
    candidate = _normalize_to_schema(source)
    existing = set(ibis_conn.table(table_name, database=database).schema().names)
    type_mapper = ibis_conn.compiler.type_mapper
    dialect = ibis_conn.compiler.dialect

    def _quote(identifier: str) -> str:
        return exp.to_identifier(identifier, quoted=True).sql(dialect=dialect)

    table_parts = [database, table_name] if database else [table_name]
    qualified = ".".join(_quote(part) for part in table_parts)

    for col_name, dtype in candidate.items():
        if col_name in existing:
            continue
        if dtype.is_null():
            dtype = ibis.dtype("string")
        type_sql = type_mapper.to_string(dtype)
        ibis_conn.raw_sql(
            f"ALTER TABLE {qualified} ADD COLUMN {_quote(col_name)} {type_sql}"
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `hatch run test:test-target-quick "tests/test_unit/backends/ibis/test_add_columns.py::TestGenericAddColumns" -v`
Expected: PASS (7 tests).

- [ ] **Step 5: Lint, then commit**

Run `hatch run ruff:check tests/test_unit/backends/ibis/test_add_columns.py` and fix any finding in the files you touched. Then:

```bash
git add src/mountainash_data/backends/ibis/operations.py tests/test_unit/backends/ibis/test_add_columns.py
git commit -m "feat(ibis): generic dialect-agnostic add_columns implementation"
```

---

### Task 4: `IbisBackend.add_columns` method (dispatch + integration)

**Files:**
- Modify: `src/mountainash_data/backends/ibis/backend.py` (import from operations near top ~line 13; new method in the "Hook-dispatched operations" section ~line 516, after `upsert`)
- Test: `tests/test_unit/backends/ibis/test_add_columns.py`

**Interfaces:**
- Consumes: `_generic_add_columns` (Task 3); `DialectSpec.add_columns_hook` (Task 1); existing `self._require_connected()`, `conn._ibis_conn`, `self._spec`.
- Produces: `IbisBackend.add_columns(self, name: str, source: t.Any, *, database: str | None = None) -> IbisBackend` — fluent (returns `self`); calls `add_columns_hook` when set, else `_generic_add_columns`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_unit/backends/ibis/test_add_columns.py` (add `import dataclasses` to the top import block — `IbisBackend` is imported here, used immediately):

```python
# add to the import block at the top of the file:
import dataclasses

from mountainash_data import IbisBackend
```

```python
class TestIbisBackendAddColumns:
    def test_frame_form_returns_self_and_adds_column(self):
        with IbisBackend(dialect="duckdb", database=":memory:") as be:
            be.create_table("t", pl.DataFrame({"id": [1], "name": ["a"]}))
            ret = be.add_columns(
                "t", pl.DataFrame({"id": [1], "name": ["a"], "score": [1.5]})
            )
            assert ret is be
            cols = {c.name for c in be.inspect_table("t").columns}
            assert "score" in cols

    def test_explicit_mountainash_dtype(self):
        with IbisBackend(dialect="duckdb", database=":memory:") as be:
            be.create_table("t", {"id": [1]})
            be.add_columns("t", {"hrv": MountainashDtype.FP64})
            cols = {c.name: c.type_name for c in be.inspect_table("t").columns}
            assert cols["hrv"] == "float64"

    def test_create_evolve_type_parity_sqlite(self):
        """The core invariant: an evolved column types like a created one."""
        with IbisBackend(dialect="sqlite", database=":memory:") as be:
            be.create_table(
                "fresh", pl.DataFrame({"cnt": pl.Series([3], dtype=pl.UInt8)})
            )
            be.create_table("evo", pl.DataFrame({"id": [1]}))
            be.add_columns(
                "evo",
                pl.DataFrame({"id": [1], "cnt": pl.Series([3], dtype=pl.UInt8)}),
            )
            fresh = {c.name: c.type_name for c in be.inspect_table("fresh").columns}
            evolved = {c.name: c.type_name for c in be.inspect_table("evo").columns}
            assert evolved["cnt"] == fresh["cnt"]

    def test_hook_override_wins_over_generic(self):
        calls = []

        def fake_hook(ibis_conn, name, source, *, database=None):
            calls.append((name, source))

        with IbisBackend(dialect="duckdb", database=":memory:") as be:
            be.create_table("t", {"id": [1]})
            be._spec = dataclasses.replace(be._spec, add_columns_hook=fake_hook)
            be.add_columns("t", {"x": "float64"})
            assert calls == [("t", {"x": "float64"})]
            # generic path did NOT run -> column absent
            cols = {c.name for c in be.inspect_table("t").columns}
            assert "x" not in cols
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run test:test-target-quick "tests/test_unit/backends/ibis/test_add_columns.py::TestIbisBackendAddColumns" -v`
Expected: FAIL — `AttributeError: 'IbisBackend' object has no attribute 'add_columns'`.

- [ ] **Step 3: Write minimal implementation**

In `backend.py`, add the import near the other backend-internal imports (after the `_registry` import line):

```python
from mountainash_data.backends.ibis.operations import _generic_add_columns
```

In the "Hook-dispatched operations (fluent — return self)" section, after the `upsert` method, add:

```python
    def add_columns(
        self,
        name: str,
        source: t.Any,
        *,
        database: str | None = None,
    ) -> IbisBackend:
        """Additively evolve `name`: add columns present in `source` but
        missing from the table. `source` is a frame (types inferred) or a
        ``{column: dtype}`` mapping. Additive, idempotent, dialect-agnostic.
        """
        conn = self._require_connected()
        hook = self._spec.add_columns_hook
        if hook is not None:
            hook(conn._ibis_conn, name, source, database=database)
        else:
            _generic_add_columns(
                conn._ibis_conn, name, source, database=database
            )
        return self
```

- [ ] **Step 4: Run test to verify it passes**

Run: `hatch run test:test-target-quick "tests/test_unit/backends/ibis/test_add_columns.py::TestIbisBackendAddColumns" -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Run the full new test file + lint + types**

Run:
```bash
hatch run test:test-target-quick tests/test_unit/backends/ibis/test_add_columns.py -v
hatch run ruff:check tests/test_unit/backends/ibis/test_add_columns.py
hatch run mypy:check
```
Expected: all add_columns tests PASS; ruff clean; mypy clean (resolve any new findings in the files you touched before committing).

- [ ] **Step 6: Commit**

```bash
git add src/mountainash_data/backends/ibis/backend.py tests/test_unit/backends/ibis/test_add_columns.py
git commit -m "feat(ibis): IbisBackend.add_columns with hook-or-generic dispatch"
```

---

### Task 5: Full-suite regression run

**Files:** none (verification only).

**Interfaces:** none.

> The spec's stale `backend.py:NNN` line citation was already removed during
> planning (replaced with a method/section reference), so there is no
> citation-refresh step here — the spec carries no line numbers to drift.

- [ ] **Step 1: Run the full backend test suite to confirm no regressions**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/ -v`
Expected: PASS — the new `test_add_columns.py` (16 tests across its classes) plus all pre-existing ibis backend tests, no regressions.

- [ ] **Step 2: Run lint + types across the touched source**

Run:
```bash
hatch run ruff:check tests/test_unit/backends/ibis/test_add_columns.py
hatch run mypy:check
```
Expected: both clean.

- [ ] **Step 3: (If anything failed) fix and re-run** — do not proceed to the branch/PR until Steps 1-2 are green.

---

## Out of Scope (tracked elsewhere)

- **Consumer migration** (mountainash-wearables `WearableStore`/`BronzeStore` deleting `_evolve_schema` + `_POLARS_TO_DUCKDB` + `_cast_null_columns`) happens in the wearables repo after this ships. Note carried from the spec: wearables-on-postgres also needs portable `upsert`, tracked in `generic-default-dialect-operations.md`.
- **`upsert`/`rename_table` generic defaults** — sibling backlog item `generic-default-dialect-operations.md`.

## Self-Review

**Spec coverage:**
- API surface (`add_columns(name, source, *, database)`, fluent) → Task 4. ✓
- Frame + explicit-map (`MountainashDtype`/string/ibis dtype) source forms → Tasks 2, 4. ✓
- Additive + idempotent semantics → Task 3 (`test_idempotent...`). ✓
- Type parity via `compiler.type_mapper` → Task 3 impl + Task 4 (`test_create_evolve_type_parity_sqlite`). ✓
- Null → dialect string → Task 3 (`test_null_typed_column_becomes_string`). ✓
- One ALTER per column → Task 3 impl. ✓
- Generic default + `add_columns_hook` override → Task 1 (field) + Task 4 (`test_hook_override_wins_over_generic`). ✓
- Identifier quoting for simple identifiers → Task 3 impl + (`test_quotes_identifiers_needing_quoting`). Dotted/multi-part namespaces are out of scope and now *enforced*: `_validate_simple_identifier` raises `ValueError` for dotted `table_name`/`database` (Task 3 `test_rejects_dotted_table_name`/`test_rejects_dotted_database`). ✓
- Idempotency is single-process preflight only (concurrency caveat) → documented in Global Constraints + `_generic_add_columns` docstring; matches the spec's Known Limitations. ✓
- Parametric `MountainashDtype` raises → Task 2 (`test_parametric_mountainash_dtype_raises_valueerror`). ✓
- Known limitation (unsigned ints non-round-tripping) → asserted as *parity-preserving* in Task 4 parity test (both sides equal), matching the spec. ✓

**Type consistency:** `_coerce_dtype`/`_normalize_to_schema`/`_generic_add_columns` signatures are identical across the task that defines each and the tasks that consume them. `add_columns_hook` arg order (`ibis_conn, name, source, *, database`) matches between the field's intended call (Task 4) and the override test. ✓

**Placeholder scan:** no TBD/TODO; every code step contains complete code; every run step has an exact command and expected outcome. ✓
