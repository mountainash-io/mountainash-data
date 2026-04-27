"""Data-driven dialect registry. Replaces the 13 per-backend connection
classes from databases/connections/ibis/.

Each entry is a DialectSpec containing the connection-builder callable,
ibis backend name, connection mode, and any backend-specific capability
hooks (e.g. dialect-specific index introspection SQL).

Connection modes:
- CONNECTION_STRING: ibis.connect(connection_string, **kwargs)
- KWARGS: ibis.connect(scheme, **{params + kwargs})
- HYBRID: ibis.connect(connection_string, **kwargs_extra)

D3 resolution: HYBRID is a general per-dialect setting. Both snowflake
and trino default to HYBRID. All other backends default to
CONNECTION_STRING or KWARGS (bigquery).
"""

from __future__ import annotations

from dataclasses import dataclass, field
import typing as t


# Capability hook signatures
GetIndexExistsSql = t.Callable[[str, str, t.Optional[str]], str]  # (index_name, table_name, database) -> SQL
GetListIndexesSql = t.Callable[[str, t.Optional[str]], str]  # (table_name, database) -> SQL
ConnectionBuilder = t.Callable[..., t.Any]  # (**config) -> ibis backend connection
UpsertHook = t.Callable[..., None]
CreateIndexHook = t.Callable[..., None]
DropIndexHook = t.Callable[..., None]
RenameTableHook = t.Callable[..., None]


@dataclass(frozen=True)
class DialectSpec:
    """Per-dialect configuration and capability hooks."""

    ibis_backend_name: str
    connection_mode: str
    connection_string_scheme: str
    connection_builder: t.Optional[ConnectionBuilder] = None
    get_index_exists_sql: t.Optional[GetIndexExistsSql] = None
    get_list_indexes_sql: t.Optional[GetListIndexesSql] = None
    upsert_hook: t.Optional[UpsertHook] = None
    create_index_hook: t.Optional[CreateIndexHook] = None
    drop_index_hook: t.Optional[DropIndexHook] = None
    rename_table_hook: t.Optional[RenameTableHook] = None
    extras: t.Mapping[str, t.Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Connection builder functions — salvaged from per-backend connection files
# ---------------------------------------------------------------------------


def _build_sqlite_connection(**config: t.Any) -> t.Any:
    """Build a sqlite ibis connection.

    Salvaged from databases/connections/ibis/sqlite_ibis_connection.py.
    Connection scheme was 'sqlite://' — uses ibis.sqlite.connect().
    Accepts 'database' kwarg for the file path (or ':memory:' for in-memory).
    """
    import ibis

    database = config.get("database", ":memory:")
    # ibis.sqlite.connect accepts file path or ':memory:' directly
    return ibis.sqlite.connect(database)


def _build_duckdb_connection(**config: t.Any) -> t.Any:
    """Build a duckdb ibis connection.

    Salvaged from databases/connections/ibis/duckdb_ibis_connection.py.
    Connection scheme was 'duckdb://' — uses ibis.connect.
    Preserves the read_only logic: defaults False for in-memory, configurable otherwise.
    Also preserves DuckDB-specific disconnect logic (handled on the connection object).
    """
    import ibis

    database = config.get("database", None)
    read_only = config.get("read_only", False)

    if database is None:
        # In-memory database
        connection_string = "duckdb://"
        kwargs = {"read_only": False}
    else:
        connection_string = f"duckdb://{database}" if not database.startswith("duckdb://") else database
        kwargs = {"read_only": read_only}

    # Pass through any extra kwargs except 'database' and 'read_only'
    extra = {k: v for k, v in config.items() if k not in ("database", "read_only")}
    kwargs.update(extra)

    return ibis.connect(connection_string, **kwargs)


def _build_motherduck_connection(**config: t.Any) -> t.Any:
    """Build a MotherDuck ibis connection.

    Salvaged from databases/connections/ibis/motherduck_ibis_connection.py.
    Connection scheme was 'duckdb://md:' — MotherDuck uses duckdb protocol.
    Accepts 'token' kwarg for authentication and 'database' for the MD database name.
    """
    import ibis

    token = config.get("token", None)
    database = config.get("database", "")

    # MotherDuck connection string format: duckdb://md:<database>?motherduck_token=<token>
    if token:
        connection_string = f"duckdb://md:{database}?motherduck_token={token}"
    else:
        connection_string = f"duckdb://md:{database}"

    extra = {k: v for k, v in config.items() if k not in ("token", "database")}
    return ibis.connect(connection_string, **extra)


def _build_postgres_connection(**config: t.Any) -> t.Any:
    """Build a postgres ibis connection.

    Salvaged from databases/connections/ibis/postgres_ibis_connection.py.
    Connection scheme was 'postgres://' — uses ibis.connect with a connection string.
    Also had set_post_connection_options for session-level option setting.
    Accepts standard postgres kwargs: host, port, user, password, database, schema.
    """
    import ibis

    # Support both a full connection_string or individual components
    connection_string = config.get("connection_string", None)
    if connection_string is not None:
        return ibis.connect(connection_string)

    host = config.get("host", "localhost")
    port = config.get("port", 5432)
    user = config.get("user", config.get("username", None))
    password = config.get("password", None)
    database = config.get("database", None)

    conn_str = "postgres://"
    if user and password:
        conn_str += f"{user}:{password}@"
    elif user:
        conn_str += f"{user}@"
    conn_str += f"{host}:{port}"
    if database:
        conn_str += f"/{database}"

    extra = {k: v for k, v in config.items()
             if k not in ("host", "port", "user", "username", "password", "database", "connection_string")}
    return ibis.connect(conn_str, **extra)


def _build_mysql_connection(**config: t.Any) -> t.Any:
    """Build a MySQL ibis connection.

    Salvaged from databases/connections/ibis/mysql_ibis_connection.py.
    Connection scheme was 'mysql://' — uses ibis.connect with a connection string.
    """
    import ibis

    connection_string = config.get("connection_string", None)
    if connection_string is not None:
        return ibis.connect(connection_string)

    host = config.get("host", "localhost")
    port = config.get("port", 3306)
    user = config.get("user", config.get("username", None))
    password = config.get("password", None)
    database = config.get("database", None)

    conn_str = "mysql://"
    if user and password:
        conn_str += f"{user}:{password}@"
    elif user:
        conn_str += f"{user}@"
    conn_str += f"{host}:{port}"
    if database:
        conn_str += f"/{database}"

    extra = {k: v for k, v in config.items()
             if k not in ("host", "port", "user", "username", "password", "database", "connection_string")}
    return ibis.connect(conn_str, **extra)


def _build_mssql_connection(**config: t.Any) -> t.Any:
    """Build an MSSQL ibis connection.

    Salvaged from databases/connections/ibis/mssql_ibis_connection.py.
    Connection scheme was 'mssql://' — uses ibis.connect with a connection string.
    Note: Requires sudo apt-get install unixodbc unixodbc-dev.
    """
    import ibis

    connection_string = config.get("connection_string", None)
    if connection_string is not None:
        return ibis.connect(connection_string)

    host = config.get("host", "localhost")
    port = config.get("port", 1433)
    user = config.get("user", config.get("username", None))
    password = config.get("password", None)
    database = config.get("database", None)

    conn_str = "mssql://"
    if user and password:
        conn_str += f"{user}:{password}@"
    elif user:
        conn_str += f"{user}@"
    conn_str += f"{host}:{port}"
    if database:
        conn_str += f"/{database}"

    extra = {k: v for k, v in config.items()
             if k not in ("host", "port", "user", "username", "password", "database", "connection_string")}
    return ibis.connect(conn_str, **extra)


def _build_oracle_connection(**config: t.Any) -> t.Any:
    """Build an Oracle ibis connection.

    Salvaged from databases/connections/ibis/oracle_ibis_connection.py.
    Connection scheme was 'oracle://' — uses ibis.connect with a connection string.
    """
    import ibis

    connection_string = config.get("connection_string", None)
    if connection_string is not None:
        return ibis.connect(connection_string)

    host = config.get("host", "localhost")
    port = config.get("port", 1521)
    user = config.get("user", config.get("username", None))
    password = config.get("password", None)
    database = config.get("database", None)

    conn_str = "oracle://"
    if user and password:
        conn_str += f"{user}:{password}@"
    elif user:
        conn_str += f"{user}@"
    conn_str += f"{host}:{port}"
    if database:
        conn_str += f"/{database}"

    extra = {k: v for k, v in config.items()
             if k not in ("host", "port", "user", "username", "password", "database", "connection_string")}
    return ibis.connect(conn_str, **extra)


def _build_snowflake_connection(**config: t.Any) -> t.Any:
    """Build a Snowflake ibis connection.

    Salvaged from databases/connections/ibis/snowflake_ibis_connection.py.
    Connection scheme was 'snowflake://' — uses HYBRID mode (both connection string + kwargs).
    Accepts: user, password, account, database, schema, warehouse, role.
    """
    import ibis

    connection_string = config.get("connection_string", None)
    if connection_string is not None:
        # HYBRID mode: combine connection string with kwargs
        extra = {k: v for k, v in config.items() if k != "connection_string"}
        return ibis.connect(connection_string, **extra)

    user = config.get("user", config.get("username", None))
    password = config.get("password", None)
    account = config.get("account", None)
    database = config.get("database", None)
    schema = config.get("schema", None)
    warehouse = config.get("warehouse", None)
    role = config.get("role", None)

    conn_str = "snowflake://"
    if user and password and account:
        conn_str += f"{user}:{password}@{account}"
    elif user and account:
        conn_str += f"{user}@{account}"

    if database:
        conn_str += f"/{database}"
        if schema:
            conn_str += f"/{schema}"

    kwargs = {}
    if warehouse:
        kwargs["warehouse"] = warehouse
    if role:
        kwargs["role"] = role

    extra = {k: v for k, v in config.items()
             if k not in ("user", "username", "password", "account", "database",
                          "schema", "warehouse", "role", "connection_string")}
    kwargs.update(extra)
    return ibis.connect(conn_str, **kwargs)


def _build_bigquery_connection(**config: t.Any) -> t.Any:
    """Build a BigQuery ibis connection.

    Salvaged from databases/connections/ibis/bigquery_ibis_connection.py.
    Connection scheme was 'bigquery://' — uses KWARGS mode.
    Auth methods: service account credentials (credentials_info dict) or ADC (project_id only).
    """
    import ibis.backends.bigquery as ir_backend

    credentials_info = config.get("credentials_info", None)
    dataset_id = config.get("dataset_id", "")
    project_id = config.get("project_id", None)

    if credentials_info:
        from google.oauth2 import service_account
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        return ir_backend.connect(dataset_id=dataset_id, credentials=credentials)
    else:
        return ir_backend.connect(project_id=project_id, dataset_id=dataset_id)


def _build_redshift_connection(**config: t.Any) -> t.Any:
    """Build a Redshift ibis connection.

    Salvaged from databases/connections/ibis/redshift_ibis_connection.py.
    Redshift uses postgres protocol under the hood — connection scheme was 'postgres://'.
    Also had set_post_connection_options for session-level option setting.
    """
    # Redshift uses the postgres ibis protocol
    return _build_postgres_connection(**config)


def _build_trino_connection(**config: t.Any) -> t.Any:
    """Build a Trino ibis connection.

    Salvaged from databases/connections/ibis/trino_ibis_connection.py.
    Connection scheme was 'trino://' — uses HYBRID mode (connection string + kwargs).
    Accepts: host, port, user, catalog, schema.
    """
    import ibis

    connection_string = config.get("connection_string", None)
    if connection_string is not None:
        extra = {k: v for k, v in config.items() if k != "connection_string"}
        return ibis.connect(connection_string, **extra)

    host = config.get("host", "localhost")
    port = config.get("port", 8080)
    user = config.get("user", config.get("username", None))
    catalog = config.get("catalog", None)
    schema = config.get("schema", None)

    conn_str = "trino://"
    if user:
        conn_str += f"{user}@"
    conn_str += f"{host}:{port}"
    if catalog:
        conn_str += f"/{catalog}"
        if schema:
            conn_str += f"/{schema}"

    extra = {k: v for k, v in config.items()
             if k not in ("host", "port", "user", "username", "catalog", "schema", "connection_string")}
    return ibis.connect(conn_str, **extra)


def _build_pyspark_connection(**config: t.Any) -> t.Any:
    """Build a PySpark ibis connection.

    Salvaged from databases/connections/ibis/pyspark_ibis_connection.py.
    Connection scheme was 'pyspark://' — uses ibis.connect.
    """
    import ibis

    connection_string = config.get("connection_string", None)
    if connection_string is not None:
        return ibis.connect(connection_string)

    # PySpark connect typically needs a running Spark session
    extra = {k: v for k, v in config.items() if k != "connection_string"}
    return ibis.connect("pyspark://", **extra)


# ---------------------------------------------------------------------------
# Dialect registry
# ---------------------------------------------------------------------------

# Connection mode constants (mirroring IBIS_DB_CONNECTION_MODE from core.constants)
_CONNECTION_STRING = "connection_string"
_KWARGS = "kwargs"
_HYBRID = "hybrid"


# Import capability hook functions from operations module.
# These are the per-dialect index SQL functions. No circular imports since
# operations.py does not import from _registry.py.
from mountainash_data.backends.ibis.operations import (  # noqa: E402
    duckdb_get_index_exists_sql,
    duckdb_get_list_indexes_sql,
    sqlite_get_index_exists_sql,
    sqlite_get_list_indexes_sql,
    motherduck_get_index_exists_sql,
    motherduck_get_list_indexes_sql,
    duckdb_family_upsert,
    duckdb_family_create_index,
    duckdb_family_drop_index,
)


DIALECTS: dict[str, DialectSpec] = {
    "sqlite": DialectSpec(
        ibis_backend_name="sqlite",
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="sqlite://",
        connection_builder=_build_sqlite_connection,
        get_index_exists_sql=sqlite_get_index_exists_sql,
        get_list_indexes_sql=sqlite_get_list_indexes_sql,
        upsert_hook=duckdb_family_upsert,
        create_index_hook=duckdb_family_create_index,
        drop_index_hook=duckdb_family_drop_index,
    ),
    "duckdb": DialectSpec(
        ibis_backend_name="duckdb",
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="duckdb://",
        connection_builder=_build_duckdb_connection,
        get_index_exists_sql=duckdb_get_index_exists_sql,
        get_list_indexes_sql=duckdb_get_list_indexes_sql,
        upsert_hook=duckdb_family_upsert,
        create_index_hook=duckdb_family_create_index,
        drop_index_hook=duckdb_family_drop_index,
    ),
    "motherduck": DialectSpec(
        ibis_backend_name="duckdb",
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="duckdb://md:",
        connection_builder=_build_motherduck_connection,
        get_index_exists_sql=motherduck_get_index_exists_sql,
        get_list_indexes_sql=motherduck_get_list_indexes_sql,
        upsert_hook=duckdb_family_upsert,
        create_index_hook=duckdb_family_create_index,
        drop_index_hook=duckdb_family_drop_index,
    ),
    "postgres": DialectSpec(
        ibis_backend_name="postgres",
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="postgres://",
        connection_builder=_build_postgres_connection,
    ),
    "mysql": DialectSpec(
        ibis_backend_name="mysql",
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="mysql://",
        connection_builder=_build_mysql_connection,
    ),
    "mssql": DialectSpec(
        ibis_backend_name="mssql",
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="mssql://",
        connection_builder=_build_mssql_connection,
    ),
    "oracle": DialectSpec(
        ibis_backend_name="oracle",
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="oracle://",
        connection_builder=_build_oracle_connection,
    ),
    "snowflake": DialectSpec(
        ibis_backend_name="snowflake",
        connection_mode=_HYBRID,  # confirmed: snowflake defaults to HYBRID
        connection_string_scheme="snowflake://",
        connection_builder=_build_snowflake_connection,
    ),
    "bigquery": DialectSpec(
        ibis_backend_name="bigquery",
        connection_mode=_KWARGS,  # confirmed: bigquery defaults to KWARGS
        connection_string_scheme="bigquery://",
        connection_builder=_build_bigquery_connection,
    ),
    "redshift": DialectSpec(
        ibis_backend_name="postgres",  # Redshift uses postgres protocol
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="postgres://",  # confirmed: redshift uses postgres://
        connection_builder=_build_redshift_connection,
    ),
    "trino": DialectSpec(
        ibis_backend_name="trino",
        connection_mode=_HYBRID,  # confirmed: trino defaults to HYBRID
        connection_string_scheme="trino://",
        connection_builder=_build_trino_connection,
    ),
    "pyspark": DialectSpec(
        ibis_backend_name="pyspark",
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="pyspark://",
        connection_builder=_build_pyspark_connection,
    ),
}
