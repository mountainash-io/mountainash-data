# Legacy Cleanup — Final Two Items Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the last two legacy artefacts from the April 2026 settings-registry refactor — the deprecated bridge methods on `BaseDBConnection` and the redundant `SecretStr` guard in `ConnectionProfile.to_connection_string()`.

**Architecture:** Two independent deletions. Item 1 removes four methods from the abstract base class and the legacy fallback branches in Ibis/Iceberg `connect_default()`. Item 2 replaces an `isinstance(pw, SecretStr)` guard with a simpler `pw is not None` check. Both are safe because all downstream settings classes now use `ConnectionProfile`.

**Tech Stack:** Python, pydantic, mountainash-settings, mountainash-data

**Spec:** `docs/superpowers/specs/2026-04-26-legacy-cleanup-final.md`

---

## File Map

| File | Action | What changes |
|------|--------|--------------|
| `src/mountainash_data/core/connection.py` | Modify | Delete 4 bridge methods + commented-out block |
| `src/mountainash_data/backends/ibis/connection.py` | Modify | Delete legacy fallback branch in `connect_default()`, remove `isinstance` guard |
| `src/mountainash_data/backends/iceberg/connection.py` | Modify | Remove `isinstance` dispatch in `connect_default()` |
| `src/mountainash_data/core/settings/profile.py` | Modify | Replace `isinstance(pw, SecretStr)` with `pw is not None`, drop `SecretStr` import |

---

### Task 1: Remove bridge methods from BaseDBConnection

**Files:**
- Modify: `src/mountainash_data/core/connection.py:135-248`

- [ ] **Step 1: Delete the four bridge methods and commented-out block**

In `src/mountainash_data/core/connection.py`, delete everything from line 122 (`# def prepare_connection_parameters`) through line 248 (end of file). This removes:

- Commented-out `prepare_connection_parameters` block (lines 122-132)
- `get_connection_string_template()` (lines 135-156)
- `get_connection_string_params()` (lines 158-175)
- `get_connection_kwargs()` (lines 177-194)
- `format_connection_string()` (lines 196-216)
- Second commented-out `format_connection_string` block (lines 220-248)

The file should end after `init_ssh()` (line 120) with a clean trailing newline.

Also remove the now-unused import of `ConnectionProfile` from line 8:
```python
# DELETE this line:
from mountainash_data.core.settings import ConnectionProfile
```

And clean up the `Dict` import from line 1 if no longer used (it won't be — the bridge methods were the only consumers):
```python
# BEFORE:
from typing import Optional, Any, Type, Dict
# AFTER:
from typing import Optional, Any, Type
```

- [ ] **Step 2: Run tests to verify nothing breaks**

Run: `hatch run test:test-quick`
Expected: All tests pass. No tests reference the bridge methods (confirmed by grep).

- [ ] **Step 3: Delete the legacy fallback branch in Ibis connect_default()**

In `src/mountainash_data/backends/ibis/connection.py`, modify `connect_default()` to:

1. Remove the `isinstance(obj_settings, ConnectionProfile)` guard — call `to_driver_kwargs()` unconditionally.
2. Remove the local import of `ConnectionProfile` (line 112).
3. Delete the entire legacy branch (lines 142-164):
   ```python
   # DELETE everything from here...
               # Legacy path for BaseDBAuthSettings subclasses
               connection_string_template = self.get_connection_string_template(...)
               ...
               elif self.ibis_connection_mode == IBIS_DB_CONNECTION_MODE.HYBRID:
                   self._connect(...)

           #TODO: Add check and logging
           if self.ibis_backend is None:
               raise Exception(...)

           return self.ibis_backend
   # ...to here
   ```

The resulting `connect_default()` method should be:

```python
def connect_default(self, **kwargs) -> SQLBackend:
    """Connect using default configuration"""

    if self.ibis_backend is None:

        settings_class = self.db_auth_settings_parameters.settings_class
        if settings_class is not None:
            obj_settings = settings_class.get_settings(
                settings_parameters=self.db_auth_settings_parameters
            )
            driver_kwargs = obj_settings.to_driver_kwargs()
            # Filter out empty lists/sequences that some drivers reject.
            # (e.g. ibis.duckdb.connect() does not accept extensions=[])
            driver_kwargs = {k: v for k, v in driver_kwargs.items()
                             if not (isinstance(v, (list, tuple)) and len(v) == 0)}
            driver_kwargs.update(kwargs)
            # Use the ibis dialect string to call ibis.<dialect>.connect(**driver_kwargs)
            descriptor = getattr(obj_settings, "__descriptor__", None)
            ibis_dialect = descriptor.ibis_dialect if descriptor else None
            if ibis_dialect:
                dialect_backend = getattr(ibis, ibis_dialect, None)
                if dialect_backend is not None:
                    self._ibis_backend = dialect_backend.connect(**driver_kwargs)
                    if self.ibis_backend is None:
                        raise Exception(f"Unable to establish default connection to {self.db_backend_name}")
                    return self.ibis_backend
            # Fallback: build KWARGS-mode connection
            self._connect(
                connection_string=self.connection_string_scheme,
                connection_kwargs=driver_kwargs if driver_kwargs else None,
            )
            if self.ibis_backend is None:
                raise Exception(f"Unable to establish default connection to {self.db_backend_name}")
            return self.ibis_backend

    return self.ibis_backend
```

Also remove the now-unused `IBIS_DB_CONNECTION_MODE` import from line 12:
```python
# BEFORE:
from mountainash_data.core.constants import (
    IBIS_DB_CONNECTION_MODE,
    CONST_DB_ABSTRACTION_LAYER,
    CONST_DB_PROVIDER_TYPE,
    CONST_DB_BACKEND as _CONST_DB_BACKEND,
)
# AFTER:
from mountainash_data.core.constants import (
    CONST_DB_ABSTRACTION_LAYER,
    CONST_DB_PROVIDER_TYPE,
    CONST_DB_BACKEND as _CONST_DB_BACKEND,
)
```

**Wait** — `IBIS_DB_CONNECTION_MODE` is still used by all concrete subclasses' `ibis_connection_mode` property defaults (e.g. line 233, 272, etc.). Keep the import. The abstract property `ibis_connection_mode` and its concrete implementations are now dead code, but removing them from 12+ subclasses is a separate cleanup — note for follow-up.

- [ ] **Step 4: Delete the isinstance dispatch in Iceberg connect_default()**

In `src/mountainash_data/backends/iceberg/connection.py`, modify `connect_default()`.

Replace lines 107-118:
```python
    def connect_default(self, **kwargs: t.Any) -> Catalog:
        """Connect using credentials from the configured settings class."""
        if self.catalog_backend is None:
            settings_class = self.db_auth_settings_parameters.settings_class
            if settings_class is None:
                raise ValueError("Settings class is required for the database connection")
            obj_settings = settings_class.get_settings(settings_parameters=self.db_auth_settings_parameters)
            from mountainash_data.core.settings import ConnectionProfile
            if isinstance(obj_settings, ConnectionProfile):
                connection_kwargs = obj_settings.to_driver_kwargs()
            else:
                connection_kwargs = obj_settings.get_connection_kwargs()
            self._catalog_backend: RestCatalog = RestCatalog(**connection_kwargs)
        return self.catalog_backend
```

With:
```python
    def connect_default(self, **kwargs: t.Any) -> Catalog:
        """Connect using credentials from the configured settings class."""
        if self.catalog_backend is None:
            settings_class = self.db_auth_settings_parameters.settings_class
            if settings_class is None:
                raise ValueError("Settings class is required for the database connection")
            obj_settings = settings_class.get_settings(settings_parameters=self.db_auth_settings_parameters)
            connection_kwargs = obj_settings.to_driver_kwargs()
            self._catalog_backend: RestCatalog = RestCatalog(**connection_kwargs)
        return self.catalog_backend
```

- [ ] **Step 5: Run tests**

Run: `hatch run test:test-quick`
Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/mountainash_data/core/connection.py src/mountainash_data/backends/ibis/connection.py src/mountainash_data/backends/iceberg/connection.py
git commit -m "chore(connection): remove deprecated bridge methods and legacy fallback branches"
```

---

### Task 2: Remove redundant SecretStr guard

**Files:**
- Modify: `src/mountainash_data/core/settings/profile.py:13,78-80`

- [ ] **Step 1: Replace isinstance guard with None check**

In `src/mountainash_data/core/settings/profile.py`, replace lines 78-80:

```python
                pw = getattr(auth, "password", None)
                if isinstance(pw, SecretStr):
                    url += ":" + quote(pw.get_secret_value(), safe="")
```

With:
```python
                pw = getattr(auth, "password", None)
                if pw is not None:
                    url += ":" + quote(pw.get_secret_value(), safe="")
```

- [ ] **Step 2: Remove the SecretStr import**

Delete line 13:
```python
from pydantic import SecretStr
```

- [ ] **Step 3: Run tests**

Run: `hatch run test:test-quick`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/mountainash_data/core/settings/profile.py
git commit -m "chore(settings): replace SecretStr isinstance guard with None check"
```

---

## Follow-up (not in scope)

After the legacy branch in `connect_default()` is removed, the following become dead code:

- `BaseIbisConnection.ibis_connection_mode` abstract property
- `BaseIbisConnection.connection_string_scheme` abstract property (partially — still used on line 135 as kwargs-mode fallback)
- All 12 concrete subclass implementations of `ibis_connection_mode`
- `IBIS_DB_CONNECTION_MODE` enum values `CONNECTION_STRING`, `KWARGS`, `HYBRID` (still imported by subclasses)

These should be tracked as a separate backlog item — removing abstract properties from 12+ subclasses warrants its own PR.

## Backlog update

After merge, update `mountainash-central/01.principles/mountainash-data/f.backlog/legacy-cleanup.md`:
- `BaseDBConnection deprecated bridge methods` → **RESOLVED** with PR reference
- `ConnectionProfile._default_driver_kwargs SecretStr guard` → **RESOLVED** with PR reference
