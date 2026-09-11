---
title: Settings and Configuration
description: ConnectionProfile, BackendSpec, ParameterSpec, DATABASES registry, register decorator, and all provider-specific AuthSettings classes
generated_by: claude skill chapter-content-generator
date: 2026-06-03
version: 0.08
---

# Settings and Configuration

## Summary

This chapter covers the typed configuration system for mountainash-data. It begins with the ConnectionProfile base class and its to_driver_kwargs method for converting settings into connection parameters. BackendSpec and ParameterSpec define typed parameter specifications with tiered complexity levels. The DATABASES registry and @register decorator enable auto-discovery of new backend settings at import time. The chapter then presents all provider-specific AuthSettings implementations: SQLite, DuckDB, PostgreSQL, BigQuery, Snowflake, Redshift, Clickhouse, Databricks, MSSQL, and Trino.

---

<!-- concept:67 -->
## ConnectionProfile Base

The **ConnectionProfile base class** (implemented as `BaseDBAuthSettings` in the codebase) is the abstract foundation for all database authentication and connection settings in mountainash-data. It extends `MountainAshBaseSettings` (itself a Pydantic `BaseSettings` subclass) and declares the common fields shared across all database providers.

The base class defines the universal connection fields that most database engines require: `HOST`, `PORT`, `DATABASE`, `SCHEMA`, `USERNAME`, `PASSWORD`, and `TOKEN`. It also declares an `AUTH_METHOD` field that controls which validation rules apply (password authentication requires both username and password; token authentication requires a token).

```python
class BaseDBAuthSettings(MountainAshBaseSettings, ABC):
    AUTH_METHOD: str = Field(default=CONST_DB_AUTH_METHOD.PASSWORD)

    # Connection Settings
    HOST: Optional[str] = Field(default=None)
    PORT: Optional[int] = Field(default=None)
    DATABASE: Optional[str] = Field(default=None)
    SCHEMA: Optional[str] = Field(default=None)

    # Password Authentication
    USERNAME: Optional[str] = Field(default=None)
    PASSWORD: Optional[SecretStr] = Field(default=None)

    # Token Authentication
    TOKEN: Optional[SecretStr] = Field(default=None)
```

The class uses Pydantic's `SecretStr` type for sensitive fields (`PASSWORD`, `TOKEN`), which prevents accidental exposure in logs and string representations. When printed, a `SecretStr` value displays as `"**********"` rather than the actual credential.

The constructor accepts three optional initialization paths: configuration files (`config_files`), a settings parameters object (`settings_parameters`), or direct keyword arguments (`**kwargs`). This flexibility allows settings to be loaded from YAML/TOML configuration files, from environment variables, or from direct programmatic construction.

#### Diagram: Settings Class Hierarchy
<iframe src="../../sims/settings-class-hierarchy/main.html" width="100%" height="500px" scrolling="no"></iframe>
<details markdown="1">
<summary>Settings Class Hierarchy</summary>
Type: diagram
**sim-id:** settings-class-hierarchy<br/>
**Library:** vis-network<br/>
**Status:** Specified

A UML-style class hierarchy diagram with MountainAshBaseSettings at the top, BaseDBAuthSettings in the middle, and all 10 provider-specific AuthSettings classes at the bottom. Each class box shows its unique fields (not inherited ones). Clicking a provider class highlights the fields it inherits from the base class versus the fields it adds. Color-coded groupings distinguish embedded databases (SQLite, DuckDB), server databases (PostgreSQL, MySQL, MSSQL), and cloud warehouses (Snowflake, BigQuery, Redshift, Databricks, Trino). Learning objective: Analyze the relationship between base and provider-specific settings (Bloom: Analyze). Controls: click class to show inheritance details, hover for field types. Colors: SteelBlue for base classes, DarkGreen for embedded, Teal for server, MediumPurple for cloud.
</details>

<!-- concept:68 -->
## To Driver Kwargs Method

The **to_driver_kwargs method** pattern encompasses three abstract methods on the base class that convert a settings object into the parameters needed by the underlying database driver. Every provider-specific settings class must implement these three methods.

- `get_connection_string_template(scheme)`: Returns a template string with placeholders (e.g., `"{scheme}{user}:{password}@{account}/{database}"`) that defines the connection URI structure for this provider.
- `get_connection_string_params()`: Returns a dictionary mapping template placeholders to their actual values, extracted from the settings fields.
- `get_connection_kwargs()`: Returns additional keyword arguments that must be passed alongside the connection string (for HYBRID mode backends) or as the sole connection parameters (for KWARGS mode backends).

Additionally, `get_post_connection_options()` returns session-level settings that should be applied after the connection is established (e.g., setting the timezone or query tag).

This multi-method design accommodates the three connection modes described in Chapter 5.

| Connection Mode | Uses Template + Params | Uses Kwargs |
|---|---|---|
| CONNECTION_STRING | Yes | No (empty dict) |
| KWARGS | No (unused) | Yes (all params) |
| HYBRID | Yes | Yes (extra params) |

<!-- concept:69 -->
## BackendSpec Class

The **BackendSpec class** is a typed specification that describes a database backend's capabilities and parameter requirements. It serves as a metadata container that the settings system uses to validate configuration at a higher level than individual field validation.

A BackendSpec declares the backend's name, its supported authentication methods, the connection mode it uses, and the list of `ParameterSpec` objects that define its configurable parameters. This metadata enables auto-generated documentation, configuration UI forms, and validation error messages that reference the specific backend context.

<!-- concept:70 -->
## ParameterSpec Class

The **ParameterSpec class** defines the specification for a single configurable parameter on a database backend. Each ParameterSpec captures the parameter's name, its Python type, a description, a default value (if any), whether it is required, and its parameter tier.

```python
# Conceptual structure of ParameterSpec
class ParameterSpec:
    name: str           # e.g., "HOST"
    type: type          # e.g., str
    description: str    # e.g., "Database server hostname"
    default: Any        # e.g., "localhost"
    required: bool      # e.g., False
    tier: str           # e.g., "basic"
```

ParameterSpec objects are used by tooling and configuration UIs to present users with the appropriate set of configuration options for their chosen backend. The tier system (described next) controls which parameters are shown at each level of configuration complexity.

<!-- concept:71 -->
## Parameter Tiers

**Parameter tiers** categorize configuration parameters by their complexity and frequency of use. This tiered system ensures that casual users see only the essential parameters while advanced users can access the full set of tuning options.

The three tiers are:

- **Basic**: Parameters required for a minimal working connection (e.g., HOST, PORT, DATABASE, USERNAME, PASSWORD). These are shown by default in configuration interfaces.
- **Standard**: Parameters used in typical production configurations but not strictly required (e.g., SCHEMA, WAREHOUSE, ROLE). These are shown when the user requests more options.
- **Advanced**: Rarely-changed parameters for fine-tuning or specialized use cases (e.g., SSL certificates, OAuth token endpoints, session parameters). These are hidden by default.

The following list shows how common PostgreSQL parameters map to tiers:

- **Basic**: HOST, PORT, DATABASE, USERNAME, PASSWORD
- **Standard**: SCHEMA, SSL_MODE
- **Advanced**: SSL_CERT, SSL_KEY, CONNECTION_TIMEOUT, APPLICATION_NAME

<!-- concept:72 -->
## DATABASES REGISTRY

The **DATABASES registry** is a module-level dictionary that maps backend names to their settings class references. It is the settings-layer counterpart to the `DIALECTS` registry in the dialect system (Chapter 5). While `DIALECTS` maps dialect names to `DialectSpec` objects for connection building, `DATABASES` maps backend names to settings classes for configuration validation.

The registry is populated at import time through the `@register` decorator (described next). When a settings module is imported, its `@register` decorator fires and adds the class to the registry. Consumer code can then look up settings classes by name without knowing which module they are defined in.

```python
# In core.registry
_REGISTRY: dict[str, t.Callable[..., Backend]] = {}

def register(name: str, factory: t.Callable[..., Backend]) -> None:
    _REGISTRY[name] = factory

def get(name: str, **config: t.Any) -> Backend:
    if name not in _REGISTRY:
        raise KeyError(f"No backend registered as {name!r}. Available: {sorted(_REGISTRY)}")
    return _REGISTRY[name](**config)
```

<!-- concept:73 -->
## Register Decorator

The **register decorator** is the mechanism that connects settings classes to the DATABASES registry. Applied to a settings class definition, it fires at import time and adds the class to the registry under the specified name.

The decorator is a higher-order function: `@register("sqlite")` returns a decorator that wraps the class. The inner decorator adds the class to the registry and returns it unchanged, so the class itself is not modified.

```python
def register(name):
    def decorator(cls):
        DATABASES[name] = cls
        return cls
    return decorator

@register("sqlite")
class SQLiteAuthSettings(BaseDBAuthSettings):
    ...
```

This pattern is a concrete application of the registry pattern introduced in Chapter 1. The key insight is that registration happens as a side effect of importing the module, which enables automatic discovery without explicit registration calls in application startup code.

<!-- concept:74 -->
## Auto Registration

**Auto registration** is the system-level behavior that ensures all backend settings classes are registered by the time consumer code needs them. It works through Python's import system: when the mountainash-data package is imported, its `__init__.py` imports the settings submodules, which triggers the `@register` decorators on each settings class.

The auto-registration sequence follows these steps:

1. Consumer code imports `mountainash_data`.
2. The package `__init__.py` imports `core.settings`.
3. `core.settings.__init__.py` imports each provider module (sqlite, duckdb, postgresql, etc.).
4. Each provider module's `@register` decorator fires, adding the class to the registry.
5. By the time consumer code calls `registry.get("sqlite")`, the class is already registered.

This approach means that consumers never need to explicitly register backends. The simple act of importing mountainash-data makes all supported backends available through the registry.

#### Diagram: Auto Registration Sequence
<iframe src="../../sims/auto-registration-sequence/main.html" width="100%" height="450px" scrolling="no"></iframe>
<details markdown="1">
<summary>Auto Registration Sequence</summary>
Type: workflow
**sim-id:** auto-registration-sequence<br/>
**Library:** vis-network<br/>
**Status:** Specified

A sequence diagram showing the import chain that triggers auto-registration. Five vertical swim lanes represent: Consumer Code, mountainash_data.__init__, core.settings.__init__, Provider Module (e.g., sqlite.py), and DATABASES Registry. Arrows flow left to right showing the import chain, with the final arrow being the @register decorator adding the class to the registry. An animated replay button allows stepping through the sequence. At the end, the registry node shows all registered entries. Learning objective: Understand how Python's import system enables automatic backend discovery (Bloom: Understand). Controls: step-through animation, click lanes for detail. Colors: SteelBlue for consumer, DarkSlateBlue for package init, Teal for provider module, Gold for registry.
</details>

<!-- concept:75 -->
## SQLiteAuthSettings

**SQLiteAuthSettings** is the simplest provider-specific settings class. SQLite uses file-based authentication (no credentials required), so its `AUTH_METHOD` defaults to `"none"`. The only additional field beyond the base class is `TYPE_MAP`, an optional dictionary for custom type mappings.

```python
class SQLiteAuthSettings(BaseDBAuthSettings):
    AUTH_METHOD: str = Field(default="none")
    TYPE_MAP: Optional[Dict[str, Any]] = Field(default=None)
```

The connection string template is simply the URI scheme followed by an optional database path: `sqlite://{database}`. Connection kwargs pass through the `TYPE_MAP` if provided.

<!-- concept:76 -->
## DuckDBAuthSettings

**DuckDBAuthSettings** extends the base class with DuckDB-specific parameters. Like SQLite, DuckDB does not require network credentials for local usage, but it adds a `read_only` parameter to control database access mode. When connecting to cloud-hosted MotherDuck, a token is required.

<!-- concept:77 -->
## PostgreSQLAuthSettings

**PostgreSQLAuthSettings** represents the standard server-based authentication model. It requires HOST, PORT, USERNAME, PASSWORD, and DATABASE for a minimal connection. The connection string follows the standard PostgreSQL URI format: `postgres://{user}:{password}@{host}:{port}/{database}`.

The class includes a `PORT` validator that ensures the value falls within the valid range (1-65535) and the standard `validate_auth_method_password` model validator that ensures both USERNAME and PASSWORD are provided when `AUTH_METHOD` is `"password"`.

<!-- concept:78 -->
## BigQueryAuthSettings

**BigQueryAuthSettings** handles Google BigQuery's unique authentication model. BigQuery uses Google Cloud IAM for authentication, supporting either service account credentials (a JSON credentials dictionary) or Application Default Credentials (ADC). The class adds fields for `PROJECT_ID`, `DATASET_ID`, and `CREDENTIALS_INFO`.

Unlike server-based databases, BigQuery does not use HOST/PORT/USERNAME/PASSWORD. Its connection kwargs mode passes parameters directly to the BigQuery Ibis backend rather than constructing a connection string.

<!-- concept:79 -->
## SnowflakeAuthSettings

**SnowflakeAuthSettings** is the most complex settings class, reflecting Snowflake's rich authentication ecosystem. Beyond the standard connection fields, it adds Snowflake-specific parameters: `ACCOUNT` (required), `WAREHOUSE` (required), `ROLE`, `AUTHENTICATOR`, and multiple OAuth/certificate-related fields.

The class implements several validators.

- `validate_account_not_null`: Ensures the account identifier is provided.
- `validate_account_formatted`: Validates the account identifier format using a regex pattern.
- `validate_authenticator`: Ensures the authenticator value is one of the supported Snowflake authenticator types.
- `validate_authentication_mode`: Ensures password is provided for password authentication.
- `validate_certificate_set`: Ensures private key or key path is provided for certificate authentication.
- `validate_ouath_set`: Ensures OAuth token or client credentials are provided for OAuth authentication.

```python
class SnowflakeAuthSettings(BaseDBAuthSettings):
    ACCOUNT: str = Field(...)
    WAREHOUSE: str = Field(...)
    ROLE: Optional[str] = Field(default=None)
    AUTHENTICATOR: Optional[str] = Field(default="snowflake")
    PRIVATE_KEY: Optional[SecretStr] = Field(default=None)
    OAUTH_TOKEN: Optional[SecretStr] = Field(default=None)
    # ... additional OAuth and certificate fields
```

!!! tip "Snowflake supports multiple auth methods"
    Snowflake's settings class demonstrates the power of Pydantic model validators for conditional validation. The `AUTH_METHOD` field determines which validators fire: password auth validates username/password, OAuth validates tokens/client credentials, and certificate auth validates private keys. This pattern is reusable for any backend with multiple authentication strategies.

<!-- concept:80 -->
## RedshiftAuthSettings

**RedshiftAuthSettings** reuses the PostgreSQL connection model because Amazon Redshift speaks the PostgreSQL wire protocol. The connection builder for Redshift is literally a pass-through to the PostgreSQL builder. The settings class may add Redshift-specific fields for IAM authentication and cluster identifier configuration.

<!-- concept:81 -->
## ClickhouseAuthSettings

**ClickhouseAuthSettings** configures connections to ClickHouse, a columnar OLAP database. ClickHouse uses standard host/port/user/password authentication with an HTTP or native protocol interface.

<!-- concept:82 -->
## DatabricksAuthSettings

**DatabricksAuthSettings** configures connections to Databricks SQL Warehouse endpoints. Databricks typically uses token-based authentication with a workspace URL and HTTP path for the SQL endpoint.

<!-- concept:83 -->
## MSSQLAuthSettings

**MSSQLAuthSettings** configures Microsoft SQL Server connections. MSSQL requires host, port (default 1433), username, password, and database. The connection string format follows the `mssql://` URI scheme. Note that MSSQL connections on Linux require the UnixODBC driver packages (`unixodbc` and `unixodbc-dev`).

<!-- concept:84 -->
## TrinoAuthSettings

**TrinoAuthSettings** configures Trino (formerly PrestoSQL) connections. Trino uses the HYBRID connection mode, supporting both a connection string and additional keyword arguments for catalog and schema selection. Trino connections typically require host, port (default 8080), and optionally a user, catalog, and schema.

The following table summarizes all ten provider-specific settings classes and their key characteristics.

| Settings Class | Auth Method | Connection Mode | Required Fields | Unique Features |
|---|---|---|---|---|
| SQLiteAuthSettings | None (file) | CONNECTION_STRING | DATABASE | TYPE_MAP for custom types |
| DuckDBAuthSettings | None (file) | CONNECTION_STRING | DATABASE | read_only mode |
| PostgreSQLAuthSettings | Password | CONNECTION_STRING | HOST, PORT, USER, PASS, DB | Port validation |
| BigQueryAuthSettings | IAM/ADC | KWARGS | PROJECT_ID | Service account JSON |
| SnowflakeAuthSettings | Multiple | HYBRID | ACCOUNT, WAREHOUSE | OAuth, certificate, MFA |
| RedshiftAuthSettings | Password | CONNECTION_STRING | HOST, PORT, USER, PASS, DB | Reuses PostgreSQL builder |
| ClickhouseAuthSettings | Password | CONNECTION_STRING | HOST, PORT, USER, PASS | HTTP/native protocol |
| DatabricksAuthSettings | Token | HYBRID | HOST, TOKEN, HTTP_PATH | Workspace URL |
| MSSQLAuthSettings | Password | CONNECTION_STRING | HOST, PORT, USER, PASS, DB | Requires UnixODBC on Linux |
| TrinoAuthSettings | Optional | HYBRID | HOST, PORT | Catalog + schema selection |

#### Diagram: Authentication Methods Landscape
<iframe src="../../sims/auth-methods-landscape/main.html" width="100%" height="500px" scrolling="no"></iframe>
<details markdown="1">
<summary>Authentication Methods Landscape</summary>
Type: chart
**sim-id:** auth-methods-landscape<br/>
**Library:** Chart.js<br/>
**Status:** Specified

A grouped bar chart showing which authentication methods each database provider supports. X axis: provider names (SQLite through Trino). Y axis: authentication methods stacked as colored segments (None/file, Password, Token, OAuth, Certificate, IAM). Each bar is clickable to show the validation rules for that provider's auth methods. A legend explains each auth method type. Learning objective: Evaluate which authentication strategies are available for each provider (Bloom: Evaluate). Controls: click bars for validation details, hover for auth method descriptions. Colors: DarkGreen for None/file, SteelBlue for Password, Gold for Token, MediumPurple for OAuth, Orange for Certificate, Crimson for IAM.
</details>

## Key Takeaways

- **ConnectionProfile (BaseDBAuthSettings)** provides the common field set (HOST, PORT, DATABASE, credentials) and validation framework for all provider settings.
- The **to_driver_kwargs pattern** (three abstract methods) converts typed settings into the connection string templates, parameter dictionaries, and keyword arguments needed by each driver.
- **BackendSpec** and **ParameterSpec** provide typed metadata about backend capabilities and configurable parameters, enabling auto-generated documentation and configuration UIs.
- **Parameter tiers** (basic, standard, advanced) categorize settings by complexity, ensuring casual users see only essential parameters.
- The **DATABASES registry** and **@register decorator** enable auto-discovery of settings classes at import time, with no explicit registration required.
- **Auto registration** works through Python's import chain: importing mountainash-data triggers all settings modules to register their classes.
- Provider-specific settings classes range from minimal (**SQLiteAuthSettings** with no credentials) to complex (**SnowflakeAuthSettings** with OAuth, certificates, and MFA).
- Pydantic's field and model validators ensure that configuration errors are caught at construction time with descriptive error messages.
