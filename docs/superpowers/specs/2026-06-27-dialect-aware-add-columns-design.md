# Dialect-Aware Schema Evolution (`add_columns`)

> **Date:** 2026-06-27
> **Status:** Draft
> **Backlog ref:** `mountainash-central/01.principles/mountainash-data/f.backlog/dialect-aware-schema-evolution.md`
> **Sibling:** `mountainash-central/01.principles/mountainash-data/f.backlog/generic-default-dialect-operations.md` (applies this pattern to `upsert`/`rename_table`)
> **Builds on:** `docs/superpowers/specs/2026-04-27-settings-aware-ibis-backend-design.md`

## Goal

Add a dialect-agnostic `IbisBackend.add_columns(name, source)` operation that
performs **additive** schema evolution — adding columns present in an incoming
frame (or an explicit `{name: dtype}` map) but missing from a target table.
Consumers must never hand-roll `ALTER TABLE … ADD COLUMN` DDL or maintain
their own polars→backend type maps.

This removes the last non-portable seam in mountainash-wearables'
`WearableStore`/`BronzeStore` (`_evolve_schema` + `_POLARS_TO_DUCKDB` +
`_cast_null_columns`), which today is DuckDB-only DDL bypassing Ibis.

## Investigation Corrections (read before designing)

Two assumptions in the backlog item do **not** hold in mountainash-data and
shaped this design:

1. **There is no "type bridge" in `create_table` to reuse.**
   `IbisBackend.create_table` (the "Thin wrapper operations" section of
   `backend.py`) is a pure passthrough to `conn._ibis_conn.create_table(...)`;
   Ibis infers all column types natively.
   "Reuse whatever type bridge `create_table` applies" therefore means **let
   Ibis render the types** — specifically via the connection's own
   `compiler.type_mapper`, which is exactly what Ibis uses to emit `CREATE
   TABLE` DDL. This guarantees an evolved column and a freshly-created column
   get **byte-identical** types (verified — see Parity Invariant).

2. **`_cast_null_columns` is a consumer convention, not an internal one.**
   It exists only in wearables. Absorbing "null-typed column → dialect string
   type" into `add_columns` is a *new* hoisted behaviour, implemented against
   the Ibis `null` dtype rather than against polars.

## API Surface

```python
def add_columns(
    self,
    name: str,
    source: t.Any,                       # frame OR Mapping[str, dtype]
    *,
    database: str | None = None,
) -> IbisBackend:                        # fluent — returns self
```

`source` is one of:

- **A frame** — any object `ibis.memtable(...)` accepts (polars/pandas/pyarrow).
  Candidate column types are inferred via `ibis.memtable(source).schema()`,
  the same inference `create_table` relies on.
- **A `Mapping[str, dtype]`** — explicit column→type. Each value may be an
  `ibis.DataType`, an ibis type **string** (`"float64"`), or a
  `MountainashDtype` (resolved through the canonical ibis bridge — see Source
  Normalization).

```python
# Infer from the frame, then upsert — the consumer pattern.
backend.add_columns("readings", df)                  # idempotent, additive
backend.upsert("readings", df, conflict_columns=keys)

# Explicit types.
from mountainash.core.dtypes.canonical import MountainashDtype
backend.add_columns("readings", {"hrv": MountainashDtype.FP64})   # NB: FP64
backend.add_columns("readings", {"hrv": "float64"})               # equivalent
```

> Note: the canonical member is `MountainashDtype.FP64`, **not** `FLOAT64` as
> the backlog example wrote. There is no `FLOAT64` member.

## Semantics

- **Additive only.** Adds columns. Never drops, renames, or re-types existing
  columns. Out of scope by design (matches the consumer need:
  passthrough-column accretion).
- **Idempotent / introspective (single-process preflight).** Missing columns
  are computed against the live table schema
  (`conn._ibis_conn.table(name).schema().names`) once, then one `ALTER` is
  issued per missing column. A call that adds nothing is a no-op (verified: a
  repeated call adds `[]`), so it is safe to call unconditionally before every
  write *within a single writer*. It is **not** concurrency-safe — two writers
  racing the same new column will collide — and a multi-column add is **not
  atomic** on engines without transactional DDL. See Known Limitations; this
  matches the single-writer consumer (wearables store) and is documented, not
  handled.
- **Type parity with `create_table`.** Types render through the connection's
  own `compiler.type_mapper.to_string(dtype)` — the identical mapper Ibis uses
  for `CREATE TABLE`. An evolved column is typed exactly as a freshly-created
  one would be.
- **Null-typed columns → dialect string.** A candidate column whose inferred
  dtype is Ibis `null` (an all-null incoming column) is coerced to
  `ibis.dtype("string")` before rendering, so it is creatable on every
  dialect. Replaces the wearables `_cast_null_columns` hack.
- **One column per statement.** SQLite permits only a single `ADD COLUMN` per
  `ALTER TABLE`; the implementation issues one statement per new column for
  universal portability.

## Design

### Dispatch shape — generic default with override seam

Unlike `upsert`/`create_index` (hook-or-`NotImplementedError`),
`add_columns` is a **uniform-SQL** operation: `ALTER TABLE … ADD COLUMN …` is
standard across the registry; only type rendering and identifier quoting vary,
and both are already encapsulated by the connection's compiler. So the default
is a single generic implementation that **covers SQL backends exposing a
sqlglot compiler + `raw_sql` and supporting `ALTER TABLE … ADD COLUMN`, for
single-part `database`/`table` contexts** — verified on duckdb/sqlite; the
registry's other SQL dialects (postgres, snowflake, trino, bigquery, …) are
covered by construction but unverified until a consumer exercises them.
**Multi-part qualification (e.g. BigQuery `project.dataset.table`) is not
supported** by the generic path — it would need a dialect hook; dotted
`name`/`database` are rejected with `ValueError` (see Semantics). An optional
per-dialect override also handles genuine capability gaps (e.g. a backend with
no `ADD COLUMN`).

```python
# backend.py — thin method, mirrors the existing hook-dispatch wiring
def add_columns(self, name, source, *, database=None):
    conn = self._require_connected()
    hook = self._spec.add_columns_hook
    if hook is not None:
        hook(conn._ibis_conn, name, source, database=database)   # override wins
    else:
        _generic_add_columns(conn._ibis_conn, name, source, database=database)
    return self
```

```python
# _registry.py — new optional field on DialectSpec (default None)
add_columns_hook: t.Optional[AddColumnsHook] = None
```

No dialect registers a hook initially; the generic path covers every SQL
dialect that supports `ALTER TABLE … ADD COLUMN`. The field exists so a dialect
that genuinely cannot `ADD COLUMN`, or needs a quirk, can override later —
consistent with the established extensibility pattern.

### Generic implementation (`operations.py`)

Verified end-to-end on duckdb and sqlite in the test env:

```python
from sqlglot import exp

def _generic_add_columns(ibis_conn, table_name, source, *, database=None):
    candidate = _normalize_to_schema(source)            # -> ibis.Schema
    existing = set(ibis_conn.table(table_name, database=database).schema().names)
    tm = ibis_conn.compiler.type_mapper                  # exact create_table mapper
    dialect = ibis_conn.compiler.dialect                 # sqlglot dialect for quoting

    def _quote(name):                                    # quote each part separately
        return exp.to_identifier(name, quoted=True).sql(dialect=dialect)

    # table_name / database must each be a SIMPLE identifier; each is quoted
    # as one part. Dotted/multi-part namespaces are out of scope and rejected
    # up front (a dotted value would otherwise be quoted as one literal).
    _validate_simple_identifier(table_name, kind="table_name")
    if database is not None:
        _validate_simple_identifier(database, kind="database")
    table_parts = [database, table_name] if database else [table_name]
    ident_t = ".".join(_quote(p) for p in table_parts)   # never quote "db.t" as one

    for col_name, dtype in candidate.items():
        if col_name in existing:
            continue
        if dtype.is_null():                              # all-null col -> string
            dtype = ibis.dtype("string")
        type_sql = tm.to_string(dtype)
        ibis_conn.raw_sql(
            f"ALTER TABLE {ident_t} ADD COLUMN {_quote(col_name)} {type_sql}"
        )
```

Rendering primitives are read off the **live connection** — no dialect name→
class lookup, no hardcoded type knowledge. `compiler.type_mapper` and
`compiler.dialect` are present on every Ibis SQL backend (verified on the
test env's Ibis; confirm against the pinned Ibis during implementation).

### Source normalization

```python
def _normalize_to_schema(source) -> ibis.Schema:
    if isinstance(source, t.Mapping):
        return ibis.schema({k: _coerce_dtype(v) for k, v in source.items()})
    return ibis.memtable(source).schema()                # frame inference

def _coerce_dtype(v) -> ibis.DataType:
    if isinstance(v, ibis.DataType):
        return v
    if isinstance(v, MountainashDtype):
        from mountainash.core.dtypes import target_ibis
        # Gate parametric members via the bridge's own CAST_UNSUPPORTED set
        # (currently {LIST, STRUCT}) rather than relying on ibis.dtype() to
        # reject a bare "array"/"struct".
        if v in target_ibis.CAST_UNSUPPORTED:
            raise ValueError(f"MountainashDtype.{v.name} is parametric; "
                             "pass an ibis.DataType or use the frame form")
        return ibis.dtype(target_ibis.SCHEMA_TYPES[v])   # canonical bridge
    return ibis.dtype(v)                                  # str or polars/pyarrow dtype
```

`target_ibis.SCHEMA_TYPES` maps each `MountainashDtype` to an ibis-castable
type string (`FP64`→`"float64"`, `U8`→`"uint8"`, …); `CAST_UNSUPPORTED` is the
bridge's own `frozenset` of parametric members. **Limitation:** parametric
members (`LIST`/`STRUCT`) are not expressible via the bare enum (they need
element types) and raise `ValueError`; use an explicit `ibis.DataType` or the
frame form for nested columns.

## Parity Invariant (verified)

A freshly-`create_table`d column and an `add_columns`-evolved column produce
identical schemas because both flow through the same `type_mapper`. Confirmed
even for an edge type — `uint8` on SQLite, which has no native unsigned type:

```
fresh-created uint8 : unknown(DataType(this=DType.USERDEFINED, kind=utinyint))
evolved     uint8 : unknown(DataType(this=DType.USERDEFINED, kind=utinyint))
PARITY HOLDS      : True
```

## Known Limitations

- **Unsigned integers on dialects without them** (SQLite affinity, PostgreSQL
  has no unsigned types) render to engine-specific spellings that may not
  round-trip cleanly. This is an upstream Ibis behaviour shared by
  `create_table` — parity holds, so `add_columns` introduces no new
  divergence. Document, don't work around.
- **Parametric types via bare `MountainashDtype`** (LIST/STRUCT) are
  unsupported in the explicit-map form; supply an `ibis.DataType` or use the
  frame form.
- **Not concurrency-safe; non-atomic multi-column adds.** Idempotency is
  single-process preflight (compute-missing-then-ALTER). Concurrent writers can
  collide on the same new column, and a partial failure mid-add leaves earlier
  columns applied. Acceptable for the single-writer consumer; a transactional
  wrapper (where the engine supports DDL transactions) is a future enhancement,
  not in this iteration.
- **Simple identifiers only (enforced).** `name`/`database` must each be a
  single, non-dotted identifier; multi-part qualified names
  (`project.dataset.table`) are out of scope and rejected with `ValueError`
  via `_validate_simple_identifier`, not silently mis-quoted.
- **Additive only** — re-typing/dropping/renaming are explicitly out of scope.

## Files Changed

| File | Change |
|------|--------|
| `src/mountainash_data/backends/ibis/operations.py` | `_generic_add_columns`, `_normalize_to_schema`, `_coerce_dtype`, `_validate_simple_identifier` |
| `src/mountainash_data/backends/ibis/backend.py` | `IbisBackend.add_columns` thin method (hook dispatch + generic fallback) |
| `src/mountainash_data/backends/ibis/dialects/_registry.py` | `add_columns_hook` optional field on `DialectSpec`; `AddColumnsHook` type alias |
| `tests/test_unit/backends/ibis/test_add_columns.py` | **new file** — all add_columns tests (helpers + integration; see Testing) |

## Files NOT Changed

- `DialectSpec` per-dialect entries — no hooks registered; generic path covers all.
- `create_table` / `insert` / `upsert` — untouched.
- `core/protocol.py` — `add_columns` is an `IbisBackend` capability, not part
  of the minimal `Connection` protocol (consistent with `upsert`/`create_index`).
- One new file: `tests/test_unit/backends/ibis/test_add_columns.py` (no new
  source modules — all production code lands in existing files).

## Testing

All tests use in-memory SQLite and DuckDB (no external deps), matching the
existing suite. Cases mirror the verified prototype:

```python
def test_add_columns_infers_from_frame_duckdb():
    with IbisBackend(dialect="duckdb", database=":memory:") as be:
        be.create_table("t", pl.DataFrame({"id": [1], "name": ["a"]}))
        df = pl.DataFrame({"id": [1], "name": ["a"], "score": [1.5]})
        be.add_columns("t", df)
        cols = {c.name: c.type_name for c in be.inspect_table("t").columns}
        assert "score" in cols

def test_add_columns_is_idempotent():
    with IbisBackend(dialect="sqlite", database=":memory:") as be:
        be.create_table("t", {"id": [1]})
        be.add_columns("t", {"x": "float64"})
        be.add_columns("t", {"x": "float64"})            # no-op, no error
        names = [c.name for c in be.inspect_table("t").columns]
        assert names.count("x") == 1

def test_add_columns_null_column_becomes_string():
    with IbisBackend(dialect="duckdb", database=":memory:") as be:
        be.create_table("t", {"id": [1]})
        df = pl.DataFrame({"id": [1], "note": pl.Series([None], dtype=pl.Null)})
        be.add_columns("t", df)
        cols = {c.name: c.type_name for c in be.inspect_table("t").columns}
        assert cols["note"] == "string"

def test_add_columns_explicit_mountainash_dtype():
    from mountainash.core.dtypes.canonical import MountainashDtype
    with IbisBackend(dialect="duckdb", database=":memory:") as be:
        be.create_table("t", {"id": [1]})
        be.add_columns("t", {"hrv": MountainashDtype.FP64})
        cols = {c.name: c.type_name for c in be.inspect_table("t").columns}
        assert cols["hrv"] == "float64"

def test_add_columns_create_evolve_parity_sqlite():
    """Evolved column types match freshly-created ones (the core invariant)."""
    # create uint8 fresh vs evolve uint8; assert identical schema repr

def test_add_columns_quotes_identifiers():
    """A column name needing quoting (space/keyword) is added correctly."""
    with IbisBackend(dialect="duckdb", database=":memory:") as be:
        be.create_table("t", {"id": [1]})
        be.add_columns("t", {"new col": "float64"})
```

## Consumer Migration (mountainash-wearables, after ship)

- `WearableStore._evolve_schema` + `_POLARS_TO_DUCKDB` → **delete**; the
  `upsert` path becomes `self._backend.add_columns(table, df)` then
  `self._backend.upsert(...)`.
- `WearableStore._cast_null_columns` / `BronzeStore._cast_null_columns` →
  **delete**; null coercion now lives in `add_columns`. (Confirm no remaining
  caller relies on the frame itself being cast before `create_table` — if
  `full_replace`/initial `create_table` still need it, keep a thin local cast
  only there, or rely on Ibis inference.)
- `BronzeStore` evolution → identical replacement.

> Caveat carried from the sibling backlog item: `add_columns` makes
> *evolution* portable, but wearables also calls `upsert`, which currently has
> a hook only for the duckdb/sqlite family. Swapping wearables to PostgreSQL
> needs **both** this item and the `upsert` generalization.

## Commit Strategy

Single feature branch targeting `develop`. Suggested commits:

1. `feat(ibis): add dialect-agnostic add_columns with generic-default dispatch`
   — operations + backend method + `DialectSpec.add_columns_hook` field + tests.
2. `chore(hatch): drop deprecated mountainash-utils-ssh from test env` — the
   stale path dependency removed to unblock the test env (see note below).

> **Env note (out-of-band):** the `[envs.test]` dependency list referenced
> `../mountainash-utils-ssh`, which has been moved to `deprecated/`. It is only
> a commented-out import in `core/connection.py` and not a runtime dependency,
> so it was removed from the test env to allow a clean rebuild. Flag for the
> maintainer in case other envs (`dev`, `tower`) need the same cleanup.
