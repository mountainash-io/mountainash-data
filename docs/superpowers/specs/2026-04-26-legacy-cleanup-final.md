# Legacy Cleanup — Final Two Items

> **Date:** 2026-04-26
> **Status:** Approved
> **Backlog ref:** `mountainash-central/01.principles/mountainash-data/f.backlog/legacy-cleanup.md`

## Context

The April 2026 settings-registry refactor migrated all backends to
`ConnectionProfile` + `BackendDescriptor`. Two legacy artefacts remain:

1. Four deprecated bridge methods on `BaseDBConnection` and their callers
2. A redundant `isinstance(pw, SecretStr)` guard in `ConnectionProfile.to_connection_string()`

All downstream consumers now use `ConnectionProfile`. No `BaseDBAuthSettings`
subclasses remain in production.

---

## Item 1 — Remove bridge methods and legacy connection branches

### What to delete

**`core/connection.py`** — remove these four methods from `BaseDBConnection`:

| Method | Lines (approx) |
|--------|----------------|
| `get_connection_string_template()` | 135-156 |
| `get_connection_string_params()` | 158-175 |
| `get_connection_kwargs()` | 177-194 |
| `format_connection_string()` | 196-216 |

Also remove the commented-out `format_connection_string` block below (lines 220+).

**`backends/ibis/connection.py`** — in `BaseIbisConnection.connect_default()`:

- Delete the legacy fallback branch (lines 142-164) that calls the bridge
  methods. The `ConnectionProfile` path (lines 116-140) already returns before
  reaching this code.
- The `if isinstance(obj_settings, ConnectionProfile)` guard on line 116 can
  become unconditional — just call `to_driver_kwargs()` directly on `obj_settings`.

**`backends/iceberg/connection.py`** — in `IcebergConnectionBase.connect_default()`:

- Delete the `isinstance` dispatch (lines 113-116). Call
  `obj_settings.to_driver_kwargs()` unconditionally.

### What to keep

- `BaseDBConnection` itself — still the abstract base for Ibis/Iceberg connections.
- The `connect_default()` methods — just cleaned of the legacy branches.

### Tests

- Existing unit tests for Ibis and Iceberg connections should continue to pass
  (they use `ConnectionProfile`-based settings).
- Grep for any test that calls the bridge methods directly and remove/update.

---

## Item 2 — Remove redundant SecretStr guard

### What to change

**`core/settings/profile.py`** — in `ConnectionProfile.to_connection_string()`:

Replace:
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

### Why this is safe

`PasswordAuth.password` is typed as `SecretStr` in upstream `mountainash-settings`.
With `validate_assignment=True` enabled (since v26.4.1), pydantic enforces the
type — a raw string can never sneak into the field. The `isinstance` guard was
protecting against a `__setattr__` bypass path that no longer exists.

### Cleanup

Remove `from pydantic import SecretStr` from `profile.py` if it becomes the
only consumer.

### Tests

- Existing `to_connection_string()` tests should pass unchanged.
- No new tests needed — behaviour is identical for all valid inputs.

---

## Commit strategy

Two separate commits, one per item:

1. `chore(connection): remove deprecated bridge methods and legacy fallback branches`
2. `chore(settings): replace SecretStr isinstance guard with None check`

Single branch, single PR targeting `develop`.

## Backlog update

After merge, update `legacy-cleanup.md`:
- Item 1 → **RESOLVED** with PR reference
- Item 2 → **RESOLVED** with PR reference
