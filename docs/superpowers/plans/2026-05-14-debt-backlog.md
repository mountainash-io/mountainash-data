# Technical debt backlog — mountainash-data

Discovered during initial package documentation profile run (2026-05-14).
Source hash at time of discovery: `1254928c55c9b0c5932707a6255c0325dd96f3c9`

---

## DEBT-1 — `mountainash` meta-package is an undeclared hard dependency

**Priority:** High
**Severity:** Runtime import failure if `mountainash` is not installed

### What is broken

`backends/ibis/operations.py:15` contains a module-level import:

```python
import mountainash as ma
```

This file is imported at package load time by `dialects/_registry.py`, which is imported by
`backends/ibis/backend.py`, which is imported by `__init__.py`. If `mountainash` is not
installed, the entire package fails to import with `ModuleNotFoundError`.

Two additional lazy imports exist in `backend.py` at lines 559 and 578 inside
`index_exists()` and `list_indexes()` — those fail at call time, not import time.

`mountainash` does not appear anywhere in `pyproject.toml` — not in core dependencies
and not in any optional extra.

### Affected files

- `src/mountainash_data/backends/ibis/operations.py:15`
- `src/mountainash_data/backends/ibis/backend.py:559,578`
- `pyproject.toml` (missing declaration)

### What `mountainash` is used for

`operations.py` uses `ma.relation(result).to_dict()` and `ma.relation(result).to_dicts()`
to read the result sets of index-introspection SQL queries. It is not used for anything else.

### Options

**Option A — declare as core dependency:**
Add `mountainash` to `[project].dependencies` in `pyproject.toml`. Clean if `mountainash`
is always a reasonable peer dep for users of this package.

**Option B — move to optional:**
Gate `index_exists()` and `list_indexes()` behind a try/import with a clear error message
if `mountainash` is missing. Appropriate if most users don't need index operations.

**Option C — remove the dependency:**
Replace `ma.relation(result).to_dict()` with direct ibis `.execute().to_dict()` calls,
eliminating the `mountainash` import entirely. The ibis connection object already has
`.sql(query)` returning an ibis relation — `.execute()` converts it to pandas, from which
`to_dict()` works natively.

**Recommended:** Option C for the module-level import in `operations.py` (zero new deps,
moves this forward). Option A for the lazy imports in `backend.py` if `mountainash` is
otherwise a declared peer.

---

## DEBT-2 — `IcebergBackend.inspect_*` return types are `t.Any`

**Priority:** Medium
**Severity:** Type-checker cannot infer result types for callers

### What is wrong

`backends/iceberg/backend.py` declares three inspection methods with `-> t.Any` return types:

```python
def inspect_table(self, name: str, namespace: str | None = None) -> t.Any: ...
def inspect_namespace(self, name: str) -> t.Any: ...
def inspect_catalog(self) -> t.Any: ...
```

The underlying `IcebergConnectionBase` methods return `TableInfo`, `NamespaceInfo`, and
`CatalogInfo` respectively (from `core/inspection.py`). The `t.Any` annotations lose this
information, breaking type inference for any caller that uses `IcebergBackend`.

### Fix

```python
from mountainash_data.core.inspection import CatalogInfo, NamespaceInfo, TableInfo

def inspect_table(self, name: str, namespace: str | None = None) -> TableInfo: ...
def inspect_namespace(self, name: str) -> NamespaceInfo: ...
def inspect_catalog(self) -> CatalogInfo: ...
```

Three-line change. Also brings `IcebergBackend` into formal conformance with the
`Backend` protocol (which uses these concrete types via `core/protocol.py`).

### Affected file

- `src/mountainash_data/backends/iceberg/backend.py:71–78`

---

## DEBT-3 — Oracle dialect is half-registered

**Priority:** Medium
**Severity:** `IbisBackend(dialect="oracle")` constructs without error but
has no settings class and no declared ibis driver extra

### What exists

- `DIALECTS["oracle"]` entry in `dialects/_registry.py` with a working
  `_build_oracle_connection(**config)` function
- `CONST_DB_BACKEND.ORACLE` and `CONST_DB_BACKEND_IBIS_PREFIX.ORACLE` in `constants.py`
- Oracle listed in `test_dialect_spec.py:39` (registry presence test passes)

### What is missing

- `src/mountainash_data/core/settings/oracle.py` (no `OracleAuthSettings` class)
- `[oracle]` optional extra in `pyproject.toml` (no ibis oracle driver declared)
- No per-backend settings test file

The settings path (`IbisBackend(settings_params)`) cannot be used for oracle because
`OracleAuthSettings` does not exist. The direct kwargs path (`IbisBackend(dialect="oracle",
host=..., ...)`) may work at runtime but is untested and has no driver dep guarantee.

### Options

**Option A — complete oracle support:**
Add `settings/oracle.py`, add `[oracle]` extra to `pyproject.toml`, add
`tests/test_unit/core/settings/backends/test_oracle.py`.

**Option B — remove oracle from DIALECTS:**
Delete the `_build_oracle_connection` function and the `"oracle"` DIALECTS entry,
remove oracle from `CONST_DB_BACKEND` and `CONST_DB_BACKEND_IBIS_PREFIX`, remove
from `test_dialect_spec.py`. Clean public contract: only declare what is actually supported.

**Recommended:** Decide explicitly. The current state (in registry, no settings class) is
misleading — it implies support that does not exist end-to-end.

---

## DEBT-4 — `IcebergConnectionBase.connect_default()` hardcodes `RestCatalog`

**Priority:** Medium
**Severity:** Architectural — future non-REST catalog implementations must override the entire base method

### What is wrong

```python
# connection.py:113
self._catalog_backend: RestCatalog = RestCatalog(**connection_kwargs)
```

This is in the abstract base class `IcebergConnectionBase`. The type annotation and the
instantiation are both locked to `RestCatalog`, meaning:

1. The `_catalog_backend` attribute is typed `RestCatalog` even in subclasses using
   different catalog backends.
2. A `HiveConnectionBase` subclass can't call `super().connect_default()` — it must
   re-implement the entire method.
3. `catalog_backend` abstract property is typed `Catalog | t.Any | None` which doesn't
   match the stored concrete type.

### Recommended fix

Introduce a factory method for the catalog type, or accept a catalog class in the
constructor:

```python
class IcebergConnectionBase(BaseDBConnection):
    catalog_class: type[Catalog] = RestCatalog  # override in subclasses

    def connect_default(self, **kwargs: t.Any) -> Catalog:
        if self.catalog_backend is None:
            obj_settings = ...
            connection_kwargs = obj_settings.to_driver_kwargs()
            self._catalog_backend = self.catalog_class(**connection_kwargs)
        return self.catalog_backend
```

This keeps the base method intact while letting subclasses override only `catalog_class`.

### Affected file

- `src/mountainash_data/backends/iceberg/connection.py:105–114`

---

## DEBT-5 — `BaseDBConnection.init_ssh()` is dead code with a latent crash

**Priority:** Low
**Severity:** `AttributeError` if called; never called anywhere

### What is wrong

`core/connection.py:115`:

```python
def init_ssh(self):
    if self.ssh_required:
        self.ssh_client.connect_ssh()
```

`self.ssh_required` is never set — the three lines that would set it are commented out in
`__init__()`. `init_ssh()` is not called anywhere in the codebase (confirmed by grep).
The SSH tunnel feature was scaffolded and then commented out, leaving a method that would
raise `AttributeError` at the first line if anyone called it.

### Fix

Delete `init_ssh()` and the commented-out SSH blocks in `__init__()`. If SSH tunnel
support is a future requirement, implement it cleanly when needed rather than maintaining
broken scaffolding.

### Affected file

- `src/mountainash_data/core/connection.py:33–40, 115–117`

---

## DEBT-6 — `core/registry.py` is an empty placeholder

**Priority:** Low
**Severity:** Misleading API — `get('ibis', ...)` raises `KeyError` at runtime

### What exists

`core/registry.py` provides `register(name, factory)` and `get(name, **config)` but its
docstring says "intentionally a placeholder for now." No backends self-register and the
`_REGISTRY` dict is always empty at runtime.

### Context

This module appears to be the intended future unified factory interface — a single
`get("ibis", dialect="duckdb", ...)` call instead of separate `IbisBackend` / `IcebergBackend`
entry points. The design intent exists; the wiring does not.

### Options

**Option A — wire it up:** Register `IbisBackend` and `IcebergBackend` factories here
on import, making `core.registry.get("ibis", ...)` a real third entry point.

**Option B — remove it:** Delete the file if the unified factory design is not being
pursued. Keeping an empty public module with a doc lie is worse than not having it.

**Option C — mark it explicitly experimental:** Rename to `_registry.py` or add a clear
`NotImplementedError` to `get()` so accidental callers get a meaningful error.

**Recommended:** Decide intent. If the unified factory is in scope for the next major
refactor, add Option C guard and a TODO. If not, delete.

### Affected file

- `src/mountainash_data/core/registry.py`

---

## Summary table

| ID | Issue | Priority | Effort | Breaking if unaddressed |
|----|-------|----------|--------|------------------------|
| DEBT-1 | `mountainash` undeclared dep — module-level import crash | High | Small | Yes — full package import failure |
| DEBT-2 | `IcebergBackend.inspect_*` typed `t.Any` | Medium | Trivial | No — runtime works, types mislead |
| DEBT-3 | Oracle half-registered (no settings class, no extra) | Medium | Medium | No — misleads but doesn't crash |
| DEBT-4 | `connect_default()` hardcodes `RestCatalog` in abstract base | Medium | Medium | No — only breaks future catalog impls |
| DEBT-5 | `init_ssh()` dead code + latent `AttributeError` | Low | Trivial | No — never called |
| DEBT-6 | `core/registry.py` empty placeholder | Low | Decision | No — nobody calls it |
