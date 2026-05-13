# mountainash-data: profile-spec migration plan

**Goal:** Migrate `mountainash-data` to the new `ProfileSpec` / `Profile` vocabulary introduced in `mountainash-settings 26.5.0`. Follow the [migration guide](../../../../mountainash-settings/docs/superpowers/specs/2026-05-13-profile-spec-rename-design.md#migration-guide-for-downstream-consumers) verbatim.

**Architecture:** Mechanical search-and-replace across 21 backend files plus updates to `descriptor.py`, `registry.py`, `profile.py`, and test files. Add a PEP 562 `__getattr__` shim in `mountainash-data`'s own `descriptor.py` so any downstream consumer of `mountainash-data` gets the same one-release deprecation window.

**Tech Stack:** Python 3.10+, pydantic 2.x, mountainash-settings ≥26.5.0 (via path-based dep), pytest, hatch.

**Upstream spec:** `../../../../mountainash-settings/docs/superpowers/specs/2026-05-13-profile-spec-rename-design.md`

**Out of scope:**
- Renaming `*AuthSettings` concrete class names (e.g. `PostgreSQLAuthSettings`) — explicitly deferred in the upstream spec.
- Adding a `mountainash-settings` version pin — `mountainash-data` uses a path-based dependency (`{root:uri}/../mountainash-settings`), so version coordination is implicit.

---

## File survey (from `grep -ln`)

**Source files using old names (21):**

```
src/mountainash_data/core/settings/registry.py
src/mountainash_data/core/settings/profile.py
src/mountainash_data/core/settings/descriptor.py
src/mountainash_data/core/settings/sqlite.py
src/mountainash_data/core/settings/duckdb.py        (likely)
src/mountainash_data/core/settings/postgresql.py
src/mountainash_data/core/settings/mysql.py
src/mountainash_data/core/settings/mssql.py
src/mountainash_data/core/settings/snowflake.py
src/mountainash_data/core/settings/redshift.py
src/mountainash_data/core/settings/bigquery.py      (likely)
src/mountainash_data/core/settings/databricks.py    (likely)
src/mountainash_data/core/settings/motherduck.py
src/mountainash_data/core/settings/clickhouse.py    (likely)
src/mountainash_data/core/settings/trino.py
src/mountainash_data/core/settings/singlestoredb.py
src/mountainash_data/core/settings/exasol.py
src/mountainash_data/core/settings/impala.py
src/mountainash_data/core/settings/materialize.py
src/mountainash_data/core/settings/risingwave.py
src/mountainash_data/core/settings/druid.py         (likely)
src/mountainash_data/core/settings/pyspark.py
src/mountainash_data/core/settings/pyiceberg_rest.py
```

**Test files using old names (3):**

```
tests/test_unit/core/settings/test_descriptor.py
tests/test_unit/core/settings/test_profile.py
tests/test_unit/core/settings/test_descriptors_invariants.py
```

---

## Rename table

Apply to every file touched:

| Old | New |
|---|---|
| `from mountainash_settings.profiles import ProfileDescriptor` | `from mountainash_settings.profiles import ProfileSpec` |
| `from mountainash_settings.profiles.descriptor import _Missing` | `from mountainash_settings.profiles import Missing` |
| `class BackendDescriptor(ProfileDescriptor)` | `class BackendSpec(ProfileSpec)` |
| Any `BackendDescriptor` reference | `BackendSpec` |
| `*_DESCRIPTOR = BackendDescriptor(...)` | `*_SPEC = BackendSpec(...)` |
| Every reference to `POSTGRESQL_DESCRIPTOR` etc. | `POSTGRESQL_SPEC` etc. |
| `@register(POSTGRESQL_DESCRIPTOR)` | `@register` (argument-free) |
| `__descriptor__ = POSTGRESQL_DESCRIPTOR` | `__spec__ = POSTGRESQL_SPEC` |
| `Registry("databases")` | `Registry("databases", spec_type=BackendSpec, profile_type=ConnectionProfile)` |
| Local MRO walk in `to_driver_kwargs` | `lookup_class_var` import from `mountainash_settings` |
| `descriptor_invariants_for` | `spec_invariants_for` |
| `TestDescriptorInvariants_*` | `TestSpecInvariants_*` (in expected pytest output assertions) |

---

## Tasks

### Task A: `descriptor.py` rename + shim

**Files:**
- Modify: `src/mountainash_data/core/settings/descriptor.py`
- Modify: `src/mountainash_data/core/settings/__init__.py` (if `BackendDescriptor` is re-exported there)

**Required changes:**

1. Rename `class BackendDescriptor` → `class BackendSpec`.
2. Replace `from mountainash_settings.profiles.descriptor import _Missing` with `from mountainash_settings.profiles import Missing`.
3. Replace `from mountainash_settings.profiles import ProfileDescriptor` with `from mountainash_settings.profiles import ProfileSpec`. Update `class BackendSpec(ProfileSpec)`.
4. Update `__all__` to use `BackendSpec` and `Missing`.
5. Add PEP 562 `__getattr__` shim at the bottom of `descriptor.py`:

```python
import warnings


_DEPRECATED = {
    "BackendDescriptor": ("BackendSpec", BackendSpec),
    "_Missing":          ("Missing", Missing),
}


def __getattr__(name):
    if name in _DEPRECATED:
        new_name, obj = _DEPRECATED[name]
        warnings.warn(
            f"{name!r} is renamed to {new_name!r} in mountainash-data. "
            f"Update imports to use the new name.",
            DeprecationWarning, stacklevel=2,
        )
        return obj
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
```

6. Update `__init__.py` re-exports if `BackendDescriptor` was previously exported — change to `BackendSpec` and add a top-level shim if downstream consumers may import directly from the package root.

### Task B: `registry.py` constraints + `profile.py` lookup helper

**Files:**
- Modify: `src/mountainash_data/core/settings/registry.py`
- Modify: `src/mountainash_data/core/settings/profile.py`

**registry.py changes:**

1. Update the `DATABASES_REGISTRY` construction:

```python
# Before
DATABASES_REGISTRY = Registry("databases")

# After
DATABASES_REGISTRY = Registry(
    "databases",
    spec_type=BackendSpec,
    profile_type=ConnectionProfile,
)
```

2. Update any `descriptor_invariants_for` references to `spec_invariants_for`.

**profile.py changes:**

3. Replace local MRO walk in `to_driver_kwargs()`:

```python
# Before — local MRO walk
adapter = type(self).__dict__.get("__adapter__")
if adapter is None:
    for base in type(self).__mro__[1:]:
        candidate = base.__dict__.get("__adapter__")
        if candidate is not None:
            adapter = candidate
            break

# After — public helper
from mountainash_settings import lookup_class_var
adapter = lookup_class_var(type(self), "__adapter__")
```

The import can go at the top of the file rather than inline.

### Task C: 21 backend file sweep

For each file in `src/mountainash_data/core/settings/` matching `^(?!__init__|descriptor|profile|registry).*\.py$`:

**Replace:**
- `BackendDescriptor` → `BackendSpec` (imports and constructor calls)
- `<UPPER>_DESCRIPTOR = BackendDescriptor(` → `<UPPER>_SPEC = BackendSpec(`
- Every reference to `<UPPER>_DESCRIPTOR` → `<UPPER>_SPEC`
- `@register(<UPPER>_DESCRIPTOR)` → `@register` (drop the argument)
- `__descriptor__ = <UPPER>_DESCRIPTOR` (or whatever spec it points at) → `__spec__ = <UPPER>_SPEC`

Each file is independent. Apply the same mechanical pattern. Verify after each that imports resolve.

### Task D: Tests

**Files:**
- Modify: `tests/test_unit/core/settings/test_profile.py`
- Modify: `tests/test_unit/core/settings/test_descriptor.py` (or rename to `test_spec.py` if desired — optional)
- Modify: `tests/test_unit/core/settings/test_descriptors_invariants.py` (consider renaming to `test_spec_invariants.py`)

Apply the same rename table. Update any references to the old API names.

### Task E: Version bump + verification

**Files:**
- Modify: `src/mountainash_data/__version__.py`

**Steps:**

1. Bump version. Current is `2026.04.2`. Following `mountainash-data`'s CalVer pattern (`YYYY.MM.MICRO`), the next release is `2026.05.0` (since we're in May).

2. Run full test suite:
   ```bash
   hatch run test:test
   ```

3. Run with deprecation warnings escalated to errors:
   ```bash
   hatch run test:test -W "error::DeprecationWarning" -W "default::DeprecationWarning:mountainash_data.core.settings.descriptor"
   ```
   The `-W default` filter explicitly allows warnings from `mountainash-data`'s own descriptor shim (which exists by design). Any warning from elsewhere fails the run — that's the migration completion check.

4. Run lint:
   ```bash
   hatch run ruff:check
   ```

5. Build:
   ```bash
   hatch build
   ```

### Task F: Push and PR

1. Push the feature branch.
2. Open PR targeting `develop` with the same level of detail as the upstream PR (description, test plan, removal commitment).

### Restore stashed working tree

After the PR is open:

```bash
git stash pop  # restore hatch.toml reorder + .claude/worktrees/settings-registry
```

(Or leave for the user to handle.)

---

## Execution strategy

The work is mechanical. Single-subagent dispatch can handle Tasks A-D as one batch since they're all in the same package and the transformations are templated. Task E and F can be done manually.

This plan does not list individual TDD steps because the upstream contract guarantees behaviour: the new names resolve to the same objects as the old names (via deprecation aliases). Running the existing test suite is the verification. New tests are not added in this PR — the upstream PR added all the deprecation tests; this PR is purely a consumer migration.
