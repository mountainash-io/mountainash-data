# Databricks Dialect Design

**Date:** 2026-05-07
**Issue:** mountainash-io/mountainash#108 (follow-on from ClickHouse dialect)
**Scope:** Add Databricks as the 14th supported dialect in `IbisBackend`

## Context

Databricks is a cloud-based SQL analytics platform. Its ibis backend
(`ibis.databricks.connect()`) uses kwargs-only connection with no connection
string or port — instead it takes `server_hostname` + `http_path` to identify
a SQL warehouse or cluster, plus auth credentials.

This follows the same `DialectSpec` + `BackendDescriptor` pattern used by
ClickHouse (PR #81) and the 12 original dialects.

## Connection Model

Databricks connects via `ibis.databricks.connect(**kwargs)`:

- `server_hostname` — workspace URL (e.g. `adb-123.12.azuredatabricks.net`)
- `http_path` — SQL warehouse path (e.g. `/sql/1.0/warehouses/abc123`)
- `access_token` — PAT token
- `catalog` + `schema` — three-level namespace (catalog.schema.table)
- `use_cloud_fetch` — performance optimisation for large result sets

No `host`/`port`/`database` — this is the key difference from standard dialects.

## Auth Modes

| Mode | Mapping | Use case |
|------|---------|----------|
| `TokenAuth` | `token` → `access_token` | Primary — PAT tokens |
| `PasswordAuth` | `username` + `password` | Rare — basic auth |
| `NoAuth` | nothing | Env-var-driven (`DATABRICKS_TOKEN`) |

Custom `credentials_provider` (OAuth M2M) is a callable, not a credential
pair — better handled as a passthrough kwarg than a first-class auth mode.

## Changes

### 1. Constants (`core/constants.py`)

Add `DATABRICKS = auto()` to `CONST_DB_PROVIDER_TYPE`. The `CONST_DB_BACKEND`
and `CONST_DB_BACKEND_IBIS_PREFIX` entries already exist.

### 2. Settings class (`core/settings/databricks.py`)

```python
DATABRICKS_DESCRIPTOR = BackendDescriptor(
    name="databricks",
    provider_type=CONST_DB_PROVIDER_TYPE.DATABRICKS,
    ibis_dialect="databricks",
    auth_modes=[TokenAuth, PasswordAuth, NoAuth],
    parameters=[
        ParameterSpec(name="SERVER_HOSTNAME", type=str, tier="core",
                      driver_key="server_hostname"),
        ParameterSpec(name="HTTP_PATH", type=str, tier="core",
                      driver_key="http_path"),
        ParameterSpec(name="CATALOG", type=Optional[str], tier="core",
                      default=None, driver_key="catalog"),
        ParameterSpec(name="SCHEMA", type=str, tier="core",
                      default="default", driver_key="schema"),
        ParameterSpec(name="USE_CLOUD_FETCH", type=bool, tier="advanced",
                      default=False, driver_key="use_cloud_fetch"),
    ],
)
```

No `default_port` or `connection_string_scheme` — Databricks uses neither.

### 3. Adapter (`core/settings/adapters/databricks.py`)

Custom adapter following Snowflake's pattern. Handles auth dispatch:

- `TokenAuth` → `access_token = token.get_secret_value()`
- `PasswordAuth` → `username`, `password`
- `NoAuth` → no auth kwargs (driver falls back to env vars)

### 4. Dialect registration (`backends/ibis/dialects/_registry.py`)

- `_build_databricks_connection(**config)` — extracts known params, calls
  `ibis.databricks.connect(**kwargs)`
- `DialectSpec` entry: `connection_mode=KWARGS`, `connection_string_scheme=""`

### 5. Optional dependency (`pyproject.toml`)

```toml
databricks = ["databricks-sql-connector>=4", "ibis-framework[databricks]>=11.0.0"]
```

### 6. Exports (`core/settings/__init__.py`)

Import and export `DatabricksAuthSettings`.

### 7. Tests

- `test_unit/core/settings/backends/test_databricks.py` — provider type,
  defaults, token auth kwargs plumbing, password auth kwargs, no-auth, schema
  default
- `test_dialect_spec.py` — registry count 13 → 14, add `"databricks"` to
  expected set
