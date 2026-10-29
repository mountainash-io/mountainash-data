---
title: Extending mountainash-data
description: Add a database dialect, a settings backend or a new kind of backend.
date: 2026-10-29
---

# Extending mountainash-data

mountainash-data has three extension points. A store that Ibis does not reach gets its own class that satisfies the `Backend` protocol. A relational engine that Ibis does reach becomes a new dialect of `IbisBackend`, declared as data rather than as a new class, and its tables are then reachable from mountainash relations like any other dialect's ([Running mountainash expressions against databases](../07-running-mountainash-expressions-against-databases/index.md)). A new way of configuring a connection becomes a settings backend: a `BackendSpec` plus a registered `BackendProfile` subclass, which mountainash-settings sources populate like the built-in profiles ([Configuring connections with mountainash-settings](../04-configuring-connections-with-mountainash-settings/index.md)). The first and third can live in your own code; adding a dialect is a change to this repository.

Modules whose names start with an underscore, such as the dialect registry module and the transaction machinery, are private. Their contents can change between releases without notice, so extensions outside this repository should depend only on the public imports used below.

<!-- concept:149 -->
## Protocol conformance testing

`Backend` (in `mountainash_data.core.protocol`, re-exported from `mountainash_data`) is a `typing.Protocol` marked `runtime_checkable`. A class conforms by having the right members; it never inherits from anything in this package. The runtime check that `isinstance(obj, Backend)` performs is shallow: it confirms that every protocol member is present and nothing more. It does not check signatures, return types or behaviour.

A class that has the lifecycle, discovery and inspection members but none of the transaction members fails the check, and the missing names are easy to list:

```python
from mountainash_data import Backend


class ReadOnlyCatalog:
    name = "read-only-catalog"

    def connect(self): return self
    def close(self): return self
    def __enter__(self): return self
    def __exit__(self, *args): return None
    def list_tables(self, namespace=None): return []
    def list_namespaces(self, catalog=None): return []
    def list_catalogs(self): return []
    def inspect_table(self, name, namespace=None): ...
    def inspect_namespace(self, name): ...
    def inspect_catalog(self, catalog=None): ...


print(isinstance(ReadOnlyCatalog(), Backend))  # → False
print([m for m in ("raw_driver_connection", "supports_transactions", "transaction",
                   "native_transaction_open", "in_transaction")
       if not hasattr(ReadOnlyCatalog, m)])
# → ['raw_driver_connection', 'supports_transactions', 'transaction', 'native_transaction_open', 'in_transaction']
```

Because the runtime check is only a presence test, a conformance suite for a backend has three parts: the `isinstance` assertion, behavioural assertions for the contract the protocol's docstrings state (shared inspection types, the transaction rules, `raw_driver_connection()` raising rather than returning `None`), and a static check, where mypy verifies signatures for an annotated assignment such as `backend: Backend = MemoryBackend(...)`. The package keeps its own guard of this kind: `test_non_ibis_stub_satisfies_backend_protocol` in `tests/test_unit/core/test_protocol.py` checks that a minimal non-Ibis class still satisfies the protocol, so the contract cannot quietly narrow to whatever `IbisBackend` happens to do.

<!-- concept:152 -->
## Implementing a Backend

A backend for a non-Ibis store implements every member of the protocol and reports metadata through the shared inspection model (`TableInfo`, `ColumnInfo`, `NamespaceInfo`, `CatalogInfo` and `Namespace`, described in [Inspecting databases](../05-inspecting-databases/index.md)). The members fall into four groups:

| Group | Members | Contract |
|---|---|---|
| Lifecycle | `name`, `connect()`, `close()`, `__enter__`, `__exit__` | `connect()` and `close()` return the backend itself; the context manager connects and closes. |
| Discovery | `list_catalogs()`, `list_namespaces(catalog=None)`, `list_tables(namespace=None)` | Return lists of names. `namespace` accepts anything `Namespace.coerce()` accepts. |
| Inspection | `inspect_table()`, `inspect_namespace()`, `inspect_catalog()` | Return the shared dataclasses, with `location` filled in. |
| Driver and transactions | `raw_driver_connection()`, `supports_transactions`, `transaction(*, required=True)`, `native_transaction_open()`, `in_transaction()` | Raise when there is no handle. A store without transactions raises `TransactionUnsupportedError` when `required=True`, warns and runs a no-op scope when `required=False`, and returns `False` from both probes. |

The transaction rules are the ones [Transactions](../10-transactions/index.md) describes for `IbisBackend`; a backend with real transactions should follow the same ownership semantics. The class below serves table schemas held in a nested dictionary:

```python
# memory_backend.py
from __future__ import annotations

import contextlib
import warnings

from mountainash_data import CatalogInfo, ColumnInfo, Namespace, NamespaceInfo, TableInfo
from mountainash_data.core.errors import TransactionUnsupportedError


class MemoryBackend:
    """A Backend over a nested dict: {namespace: {table: {column: type}}}."""

    name = "memory"

    def __init__(self, schemas: dict[str, dict[str, dict[str, str]]], catalog: str = "mem"):
        self._schemas = schemas
        self._catalog = catalog
        self._open = False

    def connect(self) -> MemoryBackend:
        self._open = True
        return self

    def close(self) -> MemoryBackend:
        self._open = False
        return self

    def __enter__(self) -> MemoryBackend:
        return self.connect()

    def __exit__(self, *args) -> None:
        self.close()

    def list_catalogs(self) -> list[str]:
        return [self._catalog]

    def list_namespaces(self, catalog: str | None = None) -> list[str]:
        return sorted(self._schemas)

    def list_tables(self, namespace=None) -> list[str]:
        ns = Namespace.coerce(namespace)
        return sorted(self._schemas.get(ns.path[-1] if ns.path else "main", {}))

    def inspect_table(self, name: str, namespace=None) -> TableInfo:
        ns = Namespace.coerce(namespace)
        key = ns.path[-1] if ns.path else "main"
        columns = [ColumnInfo(name=c, type_name=t, nullable=True)
                   for c, t in self._schemas[key][name].items()]
        return TableInfo(name=name, columns=columns,
                         location=Namespace(path=(key,), catalog=self._catalog))

    def inspect_namespace(self, name: str) -> NamespaceInfo:
        return NamespaceInfo(location=Namespace(path=(name,), catalog=self._catalog),
                             tables=self.list_tables(name))

    def inspect_catalog(self, catalog: str | None = None) -> CatalogInfo:
        return CatalogInfo(name=self._catalog,
                           namespaces=[self.inspect_namespace(n) for n in self.list_namespaces()])

    def raw_driver_connection(self):
        if not self._open:
            raise RuntimeError("MemoryBackend is not connected")
        return self._schemas

    @property
    def supports_transactions(self) -> bool:
        return False

    def transaction(self, *, required: bool = True):
        if required:
            raise TransactionUnsupportedError("memory backend has no transactions")
        warnings.warn("memory backend has no transactions; running without one")
        return contextlib.nullcontext()

    def native_transaction_open(self) -> bool | None:
        return False

    def in_transaction(self) -> bool:
        return False
```

Code written against `Backend` then works with it unchanged:

```python
from mountainash_data import Backend
from memory_backend import MemoryBackend

backend = MemoryBackend({"main": {"users": {"id": "int64", "email": "string"}},
                         "audit": {"events": {"at": "timestamp"}}})
print(isinstance(backend, Backend))  # → True

with backend as be:
    print(be.list_namespaces())                    # → ['audit', 'main']
    info = be.inspect_table("users")
    print(info.qualified_name, info.column_names)  # → mem.main.users ['id', 'email']
```

### A conformance test for the backend

The pytest module below makes the runtime and behavioural checks for `MemoryBackend`; running mypy over code that annotates a `MemoryBackend` as `Backend` adds the static check. Running `pytest test_memory_backend.py` reports `4 passed`.

```python
# test_memory_backend.py
import pytest
from mountainash_data import Backend, TableInfo
from mountainash_data.core.errors import TransactionUnsupportedError
from memory_backend import MemoryBackend


@pytest.fixture
def backend():
    with MemoryBackend({"main": {"users": {"id": "int64"}}}) as be:
        yield be


def test_satisfies_backend_protocol(backend):
    assert isinstance(backend, Backend)


def test_inspection_returns_shared_model(backend):
    info = backend.inspect_table("users")
    assert isinstance(info, TableInfo)
    assert info.location.path == ("main",)


def test_transaction_contract(backend):
    assert backend.supports_transactions is False
    assert backend.in_transaction() is False
    with pytest.raises(TransactionUnsupportedError):
        backend.transaction()


def test_raw_handle_raises_when_closed():
    with pytest.raises(RuntimeError):
        MemoryBackend({}).raw_driver_connection()
```

<!-- concept:155 -->
## The backend name registry

`mountainash_data.core.registry` is a process-wide map from a name to a factory that returns a `Backend`. It has three functions: `register(name, factory)`, `get(name, **config)`, which calls the factory with the config, and `names()`. The registry starts empty. `IbisBackend` does not register itself, nothing in the package reads the registry, and no test exercises it. It was kept by backlog decision DEBT-6 as backend-neutral infrastructure; adding tests and a statement of its public status is tracked as DEBT-25. It is not exported from `mountainash_data`, so treat it as provisional until that work lands.

```python
from mountainash_data.core import registry
from memory_backend import MemoryBackend

registry.register("memory", MemoryBackend)
print(registry.names())  # → ['memory']

backend = registry.get("memory", schemas={"main": {"users": {"id": "int64"}}})
print(type(backend).__name__, backend.connect().list_tables())  # → MemoryBackend ['users']

try:
    registry.get("ibis")
except KeyError as exc:
    print(exc)  # → "No backend registered as 'ibis'. Available: ['memory']"
```

`register()` overwrites an existing name without warning, and the registry has no function for removing an entry.

<!-- concept:150 -->
## Adding a dialect

A dialect of `IbisBackend` is one `DialectSpec` entry in the `DIALECTS` mapping in `src/mountainash_data/backends/ibis/dialects/_registry.py`, keyed by the dialect name that users pass as `IbisBackend(dialect=...)`. [How one backend serves 20 dialects](../14-how-one-backend-serves-20-dialects/index.md) explains how `IbisBackend` reads these fields at run time. The new engine must already have an Ibis backend; see the [Ibis backend list](https://ibis-project.org/backends/).

Every entry sets the connection fields. The capability fields declare what the generic operations may do; leaving one at its default declines that capability, and the matching operation raises `NotImplementedError`.

| Field | Default | Set it to |
|---|---|---|
| `ibis_backend_name` | required | The `ibis.backends.<name>` module the dialect uses. Several dialects can share one (`redshift` uses `postgres`). |
| `connection_mode` | required | `"connection_string"`, `"kwargs"` or `"hybrid"`: how the dialect's connection is opened through Ibis. |
| `connection_string_scheme` | required | The URL scheme, for example `"mysql://"`. URL detection takes the first dialect registered for a scheme. |
| `connection_builder` | `None` | A callable that takes driver kwargs and returns an Ibis connection. Without it, `connect()` raises `NotImplementedError`. |
| `upsert_style` | `None` | `ON_CONFLICT`, `MERGE` or `ON_DUPLICATE_KEY`, to use the generic upsert renderer. |
| `index_caps` | `None` | An `IndexCapability` (drop scope, partial-index support, native `IF [NOT] EXISTS`, permitted `USING` types) to enable generic index DDL. |
| `get_index_exists_sql`, `get_list_indexes_sql`, `list_indexes_hook` | `None` | Index introspection. A dialect with `index_caps` needs `get_index_exists_sql` and exactly one of the two list routes. |
| `transaction_support` | `NONE` | `FULL` or `LIMITED` when the engine has transactions. |
| `begin_statement` | `"BEGIN"` | The statement that opens a unit, or `None`. Must be `None` when support is `NONE`. |
| `in_transaction_probe`, `precommit_probe`, `begin_hook`, `raw_execute_hook` | `None` | Native transaction state and control, where the generic statements are not enough. |
| `raw_handle_attr` | `"con"` | The attribute on the Ibis backend that holds the native driver handle. |
| `adoption_mutations` | `()` | `SessionOption` entries for session settings Ibis changes when it adopts a connection, so they can be restored. |
| `raw_adoption_verified` | `False` | `True` once raw-connection adoption has been verified against a live server. |

Engines whose SQL the generic renderers cannot express get hooks instead: `upsert_hook`, `create_index_hook`, `drop_index_hook`, `rename_table_hook`, `truncate_table_hook`, `add_columns_hook`, and `ibis_connection_adapter` for patching the Ibis connection after it is built. A hook takes precedence over the generic path for its operation.

Joining a caller's already-open transaction, rather than always beginning one, is limited to the dialects listed in `OWNERSHIP_DIALECTS` in the private transaction module (`sqlite`, `duckdb`, `postgres` at this revision); [Transaction and adoption internals](../16-transaction-and-adoption-internals/index.md) explains why. A new dialect also needs an extra in `pyproject.toml` with the same name, because the missing-driver `ImportError` tells users to install `mountainash-data[<dialect>]`.

The unit tests that pin each dialect's declarations are matrices keyed by dialect name, so a new entry must be added to them explicitly:

| Test file (under `tests/test_unit/backends/ibis/`) | What to add |
|---|---|
| `test_dialect_spec.py` | The name in the registry-membership set; any transaction or adoption assignments worth pinning. Its invariants (non-empty `raw_handle_attr`, `NONE` implies no `begin_statement`, well-formed `SessionOption`s) apply automatically. |
| `test_upsert_style_registry.py` | An explicit entry in `EXPECTED_STYLE`, even when the style is `None`. |
| `test_index_capability.py` | The expected `IndexCapability`, or the name in the unsupported set; the list-index route matrix. |
| `test_missing_extra.py` | A case if the dialect shares a driver with another (as `motherduck` and `redshift` do). |
| `test_upsert_render.py`, `test_index_render.py`, `test_rename_table_render.py` | Golden SQL for any generic rendering the dialect takes. |

Behaviour against a real server belongs in the live tier: a backend entry in `tests/config/live-db.toml` and, if it can run locally, a Compose service. [Contributing and testing](../18-contributing-and-testing/index.md) covers that harness. Once the dialect is in, update the support matrix in [Supported databases and limits](../13-supported-databases-and-limits/index.md).

<!-- concept:151 -->
## Adding a settings backend

A settings backend is a `BackendSpec` and a `BackendProfile` subclass decorated with `@register`, all importable from `mountainash_data.core.settings`. The spec names the parameters, their driver keys, the supported auth profile types and the Ibis dialect the profile connects through. [From settings to driver arguments](../15-from-settings-to-driver-arguments/index.md) describes the pipeline the profile feeds, and [mountainash-settings](https://github.com/mountainash-io/mountainash-settings) owns the `ParameterSpec` and registry machinery.

This profile wraps DuckDB with read-only as the default:

```python
# settings_backend.py
import typing as t
from enum import Enum, auto

from mountainash_data.core.settings import (
    BackendProfile, BackendSpec, NoAuthProfile, ParameterSpec,
    get_descriptor, get_settings_class, register,
)


class WarehouseProvider(Enum):
    ANALYTICS_DUCKDB = auto()


ANALYTICS_SPEC = BackendSpec(
    name="analytics_duckdb",
    provider_type=WarehouseProvider.ANALYTICS_DUCKDB,
    connection_string_scheme="duckdb://",
    ibis_dialect="duckdb",
    supported_auth=(NoAuthProfile,),
    parameters=[
        ParameterSpec(name="DATABASE", type=t.Optional[str], tier="core",
                      default=None, driver_key="database"),
        ParameterSpec(name="READ_ONLY", type=bool, tier="core",
                      default=True, driver_key="read_only"),
    ],
)


@register
class AnalyticsDuckDBProfile(BackendProfile):
    __spec__ = ANALYTICS_SPEC


if __name__ == "__main__":
    print(get_settings_class("analytics_duckdb") is AnalyticsDuckDBProfile)  # → True
    print(get_descriptor("analytics_duckdb").ibis_dialect)                   # → duckdb
```

`provider_type` should be an enum member: the connection factory compares `provider_type.name` when several specs share one Ibis dialect. Once registered, the profile emits driver kwargs and connects like any built-in profile:

```python
from mountainash_data import IbisBackend
from mountainash_data.core.factories.connection_factory import build_driver_kwargs
from mountainash_settings import SettingsParameters
from settings_backend import AnalyticsDuckDBProfile

profile = AnalyticsDuckDBProfile(DATABASE=":memory:", READ_ONLY=False)
print(build_driver_kwargs(profile))  # → {'database': ':memory:', 'read_only': False}

params = SettingsParameters.create(settings_class=AnalyticsDuckDBProfile,
                                   DATABASE=":memory:", READ_ONLY=False)
backend = IbisBackend(params).connect()
print(backend.dialect, backend.list_tables())  # → duckdb []
backend.close()
```

Registration lasts for the life of the process, and registering a second class under the same name raises `ValueError: Profile 'analytics_duckdb' is already registered in databases registry by settings_backend.AnalyticsDuckDBProfile`. There is no public unregister. The package's own tests isolate registrations with the `_snapshot_for_tests()` and `_reset_for_tests()` seams in `mountainash_data.core.settings.registry`.

### Auth adapters

Every non-`NoAuthProfile` entry in `supported_auth` needs an auth adapter: a function, keyed by `(provider_type, auth profile class)`, that merges the credentials into the driver kwargs. The adapter table lives in `src/mountainash_data/core/settings/adapters/registry.py` and has no public registration function, so an out-of-tree profile that declares password auth fails at connect time:

```python
import typing as t
from enum import Enum, auto

from mountainash_data.core.factories.connection_factory import build_driver_kwargs
from mountainash_data.core.settings import (
    BackendProfile, BackendSpec, ParameterSpec, PasswordAuthProfile, register,
)


class EdgeProvider(Enum):
    EDGE_PG = auto()


@register
class EdgePostgresProfile(BackendProfile):
    __spec__ = BackendSpec(
        name="edge_pg", provider_type=EdgeProvider.EDGE_PG,
        connection_string_scheme="postgres://", ibis_dialect="postgres",
        supported_auth=(PasswordAuthProfile,),
        parameters=[ParameterSpec(name="HOST", type=str, tier="core", driver_key="host")],
    )


try:
    build_driver_kwargs(EdgePostgresProfile(HOST="edge.internal"),
                        PasswordAuthProfile(USERNAME="app", PASSWORD="s3cret"))
except ValueError as exc:
    print(exc)  # → edge_pg: no auth adapter for PasswordAuthProfile
```

Out-of-tree settings backends therefore use `NoAuthProfile`, or are contributed to this repository. Auth profile classes themselves belong to [mountainash-auth-client](https://github.com/mountainash-io/mountainash-auth-client).

### Contributing a settings backend

A settings backend added to the package takes these steps:

1. Add a member to `CONST_DB_PROVIDER_TYPE` in `src/mountainash_data/core/constants.py`.
2. Add `src/mountainash_data/core/settings/<name>.py` with the spec and the registered `<Name>BackendProfile`, and import it in `core/settings/__init__.py`. The import is what registers it.
3. Add one adapter row per supported auth class to the table in `core/settings/adapters/registry.py`. Flat user and password backends reuse the shared `sql.userpass` adapter. A backend-specific adapter goes in `adapters/<name>.py`.
4. When driver kwargs need composing (MySQL assembles its SSL options into one `ssl` dict), declare `__adapters__ = {provider_type: fn}` on the profile class.
5. Add `tests/test_unit/core/settings/backends/test_<name>.py` for the parameters and emitted kwargs, and adapter tests under `tests/test_unit/core/settings/adapters/`.

Two tests then cover the new backend without any edits. `test_descriptors_invariants.py` runs the shared spec invariants from mountainash-settings (unique upper-case parameter names, unique driver keys, name matching the registry key) for every registered spec. `test_supported_auth_consistency.py` fails if any non-`NoAuthProfile` entry in a spec's `supported_auth` has no adapter, which catches a forgotten row from step 3.
