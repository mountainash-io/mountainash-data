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

import enum
from dataclasses import dataclass, field
import typing as t

from mountainash_data.core.inspection import IndexInfo
from mountainash_data.backends.ibis._exasol_compat import patch_exasol_connection

class UpsertStyle(str, enum.Enum):
    ON_CONFLICT = "on_conflict"
    MERGE = "merge"
    ON_DUPLICATE_KEY = "on_duplicate_key"


class TransactionSupport(str, enum.Enum):
    FULL = "full"
    LIMITED = "limited"
    NONE = "none"


class DropScope(str, enum.Enum):
    SCHEMA_GLOBAL = "schema_global"   # DROP INDEX name
    TABLE_SCOPED = "table_scoped"     # DROP INDEX name ON tbl


@dataclass(frozen=True)
class IndexCapability:
    """Per-dialect conventional-B-tree index capability (spec §3).

    None on DialectSpec.index_caps means the dialect has no conventional
    secondary index -> create/drop raise NotImplementedError.
    """

    drop_scope: DropScope
    partial: bool                     # supports a WHERE filter (partial/filtered index)
    native_if_not_exists: bool        # engine has CREATE INDEX IF NOT EXISTS
    native_if_exists: bool            # engine has DROP INDEX IF EXISTS
    index_types: frozenset[str]       # valid USING <type> values; empty = no USING clause


GetIndexExistsSql = t.Callable[[str, str, t.Optional[str]], str]  # (index_name, table_name, database) -> SQL
GetListIndexesSql = t.Callable[[str, t.Optional[str]], str]  # (table_name, database) -> SQL
ListIndexesHook = t.Callable[[t.Any, str, t.Optional[str]], list[IndexInfo]]
ConnectionBuilder = t.Callable[..., t.Any]  # (**config) -> ibis backend connection
IbisConnectionAdapter = t.Callable[[t.Any], t.Any]
UpsertHook = t.Callable[..., None]
CreateIndexHook = t.Callable[..., None]
DropIndexHook = t.Callable[..., None]
RenameTableHook = t.Callable[..., None]
AddColumnsHook = t.Callable[..., None]


@dataclass(frozen=True)
class SessionOption:
    """A session option ibis mutates on adoption (Gap 1).

    read_sql returns the current scalar value (None if unreadable); render_set
    maps a value to the SQL statement that sets it.
    """

    name: str
    read_sql: t.Optional[str]
    render_set: t.Callable[[t.Any], str]


@dataclass(frozen=True)
class DialectSpec:
    """Per-dialect configuration and capability hooks."""

    ibis_backend_name: str
    connection_mode: str
    connection_string_scheme: str
    connection_builder: t.Optional[ConnectionBuilder] = None
    ibis_connection_adapter: t.Optional[IbisConnectionAdapter] = None
    get_index_exists_sql: t.Optional[GetIndexExistsSql] = None
    get_list_indexes_sql: t.Optional[GetListIndexesSql] = None
    list_indexes_hook: t.Optional[ListIndexesHook] = None
    upsert_hook: t.Optional[UpsertHook] = None
    # None = upsert not supported (no hook + no style -> NotImplementedError).
    upsert_style: t.Optional[UpsertStyle] = None
    index_caps: t.Optional[IndexCapability] = None
    # None = no conventional index support -> NotImplementedError.
    create_index_hook: t.Optional[CreateIndexHook] = None
    drop_index_hook: t.Optional[DropIndexHook] = None
    rename_table_hook: t.Optional[RenameTableHook] = None
    add_columns_hook: t.Optional[AddColumnsHook] = None
    raw_handle_attr: str = "con"
    # attribute on the ibis backend holding the native driver handle (Gap 2).
    raw_adoption_verified: bool = False
    # True once Gap 1's from_ibis_connection() adoption path has been live-verified
    # for this dialect (assigned by the Gap 1 plan; declared here to avoid a
    # second addition to this dataclass if Gap 1 lands after Gap 3).
    transaction_support: "TransactionSupport" = TransactionSupport.NONE
    begin_statement: t.Optional[str] = "BEGIN"
    autocommit_probe: t.Optional[t.Callable[[t.Any], t.Optional[bool]]] = None
    in_transaction_probe: t.Optional[t.Callable[[t.Any], t.Optional[bool]]] = None
    raw_execute_hook: t.Optional[t.Callable[[t.Any, str], None]] = None
    adoption_mutations: tuple["SessionOption", ...] = ()
    # session options ibis stomps on adoption; () = none (Gap 1).
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
    from .._oracle_compat import patch_oracle_connection

    connection_string = config.get("connection_string", None)
    if connection_string is not None:
        return patch_oracle_connection(ibis.connect(connection_string))

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
    return patch_oracle_connection(ibis.connect(conn_str, **extra))


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


def _build_clickhouse_connection(**config: t.Any) -> t.Any:
    """Build a ClickHouse ibis connection.

    Uses ibis.clickhouse.connect() with kwargs: host, port, user, password,
    database, secure.
    """
    import ibis

    host = config.get("host", "localhost")
    port = config.get("port", 9000)
    user = config.get("user", config.get("username", "default"))
    password = config.get("password", "")
    database = config.get("database", "default")
    secure = config.get("secure", False)

    extra = {k: v for k, v in config.items()
             if k not in ("host", "port", "user", "username", "password",
                          "database", "secure", "connection_string")}

    return ibis.clickhouse.connect(
        host=host, port=port, user=user, password=password,
        database=database, secure=secure, **extra,
    )


def _build_databricks_connection(**config: t.Any) -> t.Any:
    """Build a Databricks ibis connection.

    Uses ibis.databricks.connect() with kwargs: server_hostname, http_path,
    access_token, catalog, schema, use_cloud_fetch.
    """
    import ibis

    server_hostname = config.get("server_hostname", None)
    http_path = config.get("http_path", None)
    access_token = config.get("access_token", None)
    catalog = config.get("catalog", None)
    schema = config.get("schema", "default")
    use_cloud_fetch = config.get("use_cloud_fetch", False)

    known = {"server_hostname", "http_path", "access_token", "catalog",
             "schema", "use_cloud_fetch", "username", "password",
             "connection_string"}
    extra = {k: v for k, v in config.items() if k not in known}

    kwargs: dict[str, t.Any] = {}
    if server_hostname is not None:
        kwargs["server_hostname"] = server_hostname
    if http_path is not None:
        kwargs["http_path"] = http_path
    if access_token is not None:
        kwargs["access_token"] = access_token
    if catalog is not None:
        kwargs["catalog"] = catalog
    kwargs["schema"] = schema
    kwargs["use_cloud_fetch"] = use_cloud_fetch

    username = config.get("username", None)
    password = config.get("password", None)
    if username is not None:
        kwargs["username"] = username
    if password is not None:
        kwargs["password"] = password

    kwargs.update(extra)
    return ibis.databricks.connect(**kwargs)


def _build_singlestoredb_connection(**config: t.Any) -> t.Any:
    """Build a SingleStoreDB ibis connection.

    Uses ibis.singlestoredb.connect() with kwargs: host, port, user, password,
    database, driver, autocommit, local_infile.
    """
    import ibis

    host = config.get("host", "localhost")
    port = config.get("port", 3306)
    user = config.get("user", config.get("username", None))
    password = config.get("password", None)
    database = config.get("database", None)
    driver = config.get("driver", None)
    autocommit = config.get("autocommit", True)
    local_infile = config.get("local_infile", True)

    known = {"host", "port", "user", "username", "password", "database",
             "driver", "autocommit", "local_infile", "connection_string"}
    extra = {k: v for k, v in config.items() if k not in known}

    kwargs: dict[str, t.Any] = {
        "host": host,
        "port": port,
        "autocommit": autocommit,
        "local_infile": local_infile,
    }
    if user is not None:
        kwargs["user"] = user
    if password is not None:
        kwargs["password"] = password
    if database is not None:
        kwargs["database"] = database
    if driver is not None:
        kwargs["driver"] = driver

    kwargs.update(extra)
    return ibis.singlestoredb.connect(**kwargs)


def _build_exasol_connection(**config: t.Any) -> t.Any:
    """Build an Exasol ibis connection."""
    import ibis

    user = config.get("user", config.get("username", None))
    password = config.get("password", None)
    host = config.get("host", "localhost")
    port = config.get("port", 8563)
    timezone = config.get("timezone", "UTC")

    known = {"host", "port", "user", "username", "password", "timezone",
             "connection_string"}
    extra = {k: v for k, v in config.items() if k not in known}

    kwargs: dict[str, t.Any] = {"host": host, "port": port, "timezone": timezone}
    if user is not None:
        kwargs["user"] = user
    if password is not None:
        kwargs["password"] = password
    kwargs.update(extra)
    return ibis.exasol.connect(**kwargs)


def _build_impala_connection(**config: t.Any) -> t.Any:
    """Build an Impala ibis connection."""
    import ibis

    host = config.get("host", "localhost")
    port = config.get("port", 21050)
    database = config.get("database", "default")
    timeout = config.get("timeout", 45)
    use_ssl = config.get("use_ssl", False)
    ca_cert = config.get("ca_cert", None)
    user = config.get("user", config.get("username", None))
    password = config.get("password", None)
    auth_mechanism = config.get("auth_mechanism", "NOSASL")
    kerberos_service_name = config.get("kerberos_service_name", "impala")

    known = {"host", "port", "database", "timeout", "use_ssl", "ca_cert",
             "user", "username", "password", "auth_mechanism",
             "kerberos_service_name", "connection_string"}
    extra = {k: v for k, v in config.items() if k not in known}

    kwargs: dict[str, t.Any] = {
        "host": host, "port": port, "database": database,
        "timeout": timeout, "use_ssl": use_ssl,
        "auth_mechanism": auth_mechanism,
        "kerberos_service_name": kerberos_service_name,
    }
    if ca_cert is not None:
        kwargs["ca_cert"] = ca_cert
    if user is not None:
        kwargs["user"] = user
    if password is not None:
        kwargs["password"] = password
    kwargs.update(extra)
    return ibis.impala.connect(**kwargs)


def _build_materialize_connection(**config: t.Any) -> t.Any:
    """Build a Materialize ibis connection."""
    import ibis

    host = config.get("host", None)
    port = config.get("port", 6875)
    user = config.get("user", config.get("username", None))
    password = config.get("password", None)
    database = config.get("database", None)
    schema = config.get("schema", None)
    autocommit = config.get("autocommit", True)
    cluster = config.get("cluster", None)

    known = {"host", "port", "user", "username", "password", "database",
             "schema", "autocommit", "cluster", "connection_string"}
    extra = {k: v for k, v in config.items() if k not in known}

    kwargs: dict[str, t.Any] = {"port": port, "autocommit": autocommit}
    if host is not None:
        kwargs["host"] = host
    if user is not None:
        kwargs["user"] = user
    if password is not None:
        kwargs["password"] = password
    if database is not None:
        kwargs["database"] = database
    if schema is not None:
        kwargs["schema"] = schema
    if cluster is not None:
        kwargs["cluster"] = cluster
    kwargs.update(extra)
    return ibis.materialize.connect(**kwargs)


def _build_risingwave_connection(**config: t.Any) -> t.Any:
    """Build a RisingWave ibis connection."""
    import ibis

    host = config.get("host", None)
    port = config.get("port", 5432)
    user = config.get("user", config.get("username", None))
    password = config.get("password", None)
    database = config.get("database", None)
    schema = config.get("schema", None)

    known = {"host", "port", "user", "username", "password", "database",
             "schema", "connection_string"}
    extra = {k: v for k, v in config.items() if k not in known}

    kwargs: dict[str, t.Any] = {"port": port}
    if host is not None:
        kwargs["host"] = host
    if user is not None:
        kwargs["user"] = user
    if password is not None:
        kwargs["password"] = password
    if database is not None:
        kwargs["database"] = database
    if schema is not None:
        kwargs["schema"] = schema
    kwargs.update(extra)
    return ibis.risingwave.connect(**kwargs)


def _build_druid_connection(**config: t.Any) -> t.Any:
    """Build a Druid ibis connection."""
    import ibis

    extra = {k: v for k, v in config.items() if k != "connection_string"}
    return ibis.druid.connect(**extra)


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
from mountainash_data.backends.ibis._index_inspection import (  # noqa: E402
    duckdb_list_indexes_hook,
    mssql_get_list_indexes_sql,
    mysql_get_list_indexes_sql,
    oracle_get_list_indexes_sql,
    postgres_get_list_indexes_sql,
    singlestore_get_list_indexes_sql,
    sqlite_get_list_indexes_sql,
)
from mountainash_data.backends.ibis.operations import (  # noqa: E402
    duckdb_get_index_exists_sql,
    sqlite_get_index_exists_sql,
    motherduck_get_index_exists_sql,
    postgres_get_index_exists_sql,
    mysql_get_index_exists_sql,
    mssql_get_index_exists_sql,
    oracle_get_index_exists_sql,
    singlestore_get_index_exists_sql,
)


def _postgres_autocommit_probe(con: t.Any) -> t.Optional[bool]:
    """psycopg Connection.autocommit — True when ibis's connect default is in force."""
    return bool(con.autocommit)


def _postgres_in_transaction_probe(con: t.Any) -> t.Optional[bool]:
    """False when no server-side transaction is open (psycopg transaction_status IDLE == 0)."""
    return con.info.transaction_status != 0


def _sql_str_literal(v: t.Any) -> str:
    """Escape a value as a single-quoted SQL string literal (injection-safe).

    Uses sqlglot so embedded quotes/backslashes are escaped, not raw-interpolated
    (Codex review — apply_session_options values are caller-supplied).
    """
    import sqlglot.expressions as exp
    return exp.Literal.string(str(v)).sql()


def _duckdb_render_replacements(v: t.Any) -> str:
    # boolean -> fixed token, never interpolated
    return f"SET python_enable_replacements={'true' if v else 'false'}"


def _duckdb_render_timezone(v: t.Any) -> str:
    return f"SET TimeZone={_sql_str_literal(v)}"


_DUCKDB_ADOPTION = (
    SessionOption("python_enable_replacements",
                  "SELECT current_setting('python_enable_replacements')",
                  _duckdb_render_replacements),
    SessionOption("timezone",
                  "SELECT current_setting('TimeZone')",
                  _duckdb_render_timezone),
)


DIALECTS: dict[str, DialectSpec] = {
    "sqlite": DialectSpec(
        ibis_backend_name="sqlite",
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="sqlite://",
        connection_builder=_build_sqlite_connection,
        get_index_exists_sql=sqlite_get_index_exists_sql,
        get_list_indexes_sql=sqlite_get_list_indexes_sql,
        upsert_style=UpsertStyle.ON_CONFLICT,
        index_caps=IndexCapability(
            drop_scope=DropScope.SCHEMA_GLOBAL, partial=True,
            native_if_not_exists=True, native_if_exists=True,
            index_types=frozenset(),
        ),
        transaction_support=TransactionSupport.FULL,
        begin_statement="BEGIN",
    ),
    "duckdb": DialectSpec(
        ibis_backend_name="duckdb",
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="duckdb://",
        connection_builder=_build_duckdb_connection,
        get_index_exists_sql=duckdb_get_index_exists_sql,
        list_indexes_hook=duckdb_list_indexes_hook,
        upsert_style=UpsertStyle.ON_CONFLICT,
        index_caps=IndexCapability(
            drop_scope=DropScope.SCHEMA_GLOBAL, partial=False,
            native_if_not_exists=True, native_if_exists=True,
            index_types=frozenset(),
        ),
        transaction_support=TransactionSupport.FULL,
        begin_statement="BEGIN",
        adoption_mutations=_DUCKDB_ADOPTION,
        raw_adoption_verified=True,
    ),
    "motherduck": DialectSpec(
        ibis_backend_name="duckdb",
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="duckdb://md:",
        connection_builder=_build_motherduck_connection,
        get_index_exists_sql=motherduck_get_index_exists_sql,
        list_indexes_hook=duckdb_list_indexes_hook,
        upsert_style=UpsertStyle.ON_CONFLICT,
        index_caps=IndexCapability(
            drop_scope=DropScope.SCHEMA_GLOBAL, partial=False,
            native_if_not_exists=True, native_if_exists=True,
            index_types=frozenset(),
        ),
        transaction_support=TransactionSupport.FULL,
        begin_statement="BEGIN",
        adoption_mutations=_DUCKDB_ADOPTION,
        raw_adoption_verified=True,
    ),
    "postgres": DialectSpec(
        ibis_backend_name="postgres",
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="postgres://",
        connection_builder=_build_postgres_connection,
        upsert_style=UpsertStyle.ON_CONFLICT,
        get_list_indexes_sql=postgres_get_list_indexes_sql,
        get_index_exists_sql=postgres_get_index_exists_sql,
        index_caps=IndexCapability(
            drop_scope=DropScope.SCHEMA_GLOBAL, partial=True,
            native_if_not_exists=True, native_if_exists=True,
            index_types=frozenset({"btree", "hash", "gist", "gin", "brin", "spgist"}),
        ),
        transaction_support=TransactionSupport.FULL,
        begin_statement="BEGIN",
        autocommit_probe=_postgres_autocommit_probe,
        in_transaction_probe=_postgres_in_transaction_probe,
    ),
    "mysql": DialectSpec(
        ibis_backend_name="mysql",
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="mysql://",
        connection_builder=_build_mysql_connection,
        upsert_style=UpsertStyle.ON_DUPLICATE_KEY,
        get_list_indexes_sql=mysql_get_list_indexes_sql,
        get_index_exists_sql=mysql_get_index_exists_sql,
        index_caps=IndexCapability(
            drop_scope=DropScope.TABLE_SCOPED, partial=False,
            native_if_not_exists=False, native_if_exists=False,
            index_types=frozenset({"btree"}),
        ),
        transaction_support=TransactionSupport.FULL,
        begin_statement="BEGIN",
    ),
    "mssql": DialectSpec(
        ibis_backend_name="mssql",
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="mssql://",
        connection_builder=_build_mssql_connection,
        upsert_style=UpsertStyle.MERGE,
        get_list_indexes_sql=mssql_get_list_indexes_sql,
        get_index_exists_sql=mssql_get_index_exists_sql,
        index_caps=IndexCapability(
            drop_scope=DropScope.TABLE_SCOPED, partial=True,
            native_if_not_exists=False, native_if_exists=True,
            index_types=frozenset(),
        ),
        transaction_support=TransactionSupport.FULL,
        begin_statement="BEGIN TRANSACTION",
    ),
    "oracle": DialectSpec(
        ibis_backend_name="oracle",
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="oracle://",
        connection_builder=_build_oracle_connection,
        upsert_style=UpsertStyle.MERGE,
        get_list_indexes_sql=oracle_get_list_indexes_sql,
        get_index_exists_sql=oracle_get_index_exists_sql,
        index_caps=IndexCapability(
            drop_scope=DropScope.SCHEMA_GLOBAL, partial=False,
            native_if_not_exists=False, native_if_exists=False,
            index_types=frozenset(),
        ),
        transaction_support=TransactionSupport.FULL,
        begin_statement=None,
    ),
    "snowflake": DialectSpec(
        ibis_backend_name="snowflake",
        connection_mode=_HYBRID,  # confirmed: snowflake defaults to HYBRID
        connection_string_scheme="snowflake://",
        connection_builder=_build_snowflake_connection,
        upsert_style=UpsertStyle.MERGE,
        transaction_support=TransactionSupport.FULL,
        begin_statement="BEGIN",
    ),
    "bigquery": DialectSpec(
        ibis_backend_name="bigquery",
        connection_mode=_KWARGS,  # confirmed: bigquery defaults to KWARGS
        connection_string_scheme="bigquery://",
        connection_builder=_build_bigquery_connection,
        upsert_style=UpsertStyle.MERGE,
        raw_handle_attr="client",
        transaction_support=TransactionSupport.NONE,
        begin_statement=None,
    ),
    "redshift": DialectSpec(
        ibis_backend_name="postgres",  # Redshift uses postgres protocol
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="postgres://",  # confirmed: redshift uses postgres://
        connection_builder=_build_redshift_connection,
        upsert_style=UpsertStyle.MERGE,
        transaction_support=TransactionSupport.FULL,
        begin_statement="BEGIN",
    ),
    "trino": DialectSpec(
        ibis_backend_name="trino",
        connection_mode=_HYBRID,  # confirmed: trino defaults to HYBRID
        connection_string_scheme="trino://",
        connection_builder=_build_trino_connection,
        upsert_style=UpsertStyle.MERGE,
        transaction_support=TransactionSupport.LIMITED,
        begin_statement="START TRANSACTION",
    ),
    "clickhouse": DialectSpec(
        ibis_backend_name="clickhouse",
        connection_mode=_KWARGS,
        connection_string_scheme="clickhouse://",
        connection_builder=_build_clickhouse_connection,
        transaction_support=TransactionSupport.NONE,
        begin_statement=None,
    ),
    "databricks": DialectSpec(
        ibis_backend_name="databricks",
        connection_mode=_KWARGS,
        connection_string_scheme="",
        connection_builder=_build_databricks_connection,
        upsert_style=UpsertStyle.MERGE,
        transaction_support=TransactionSupport.NONE,
        begin_statement=None,
    ),
    "singlestoredb": DialectSpec(
        ibis_backend_name="singlestoredb",
        connection_mode=_KWARGS,
        connection_string_scheme="singlestoredb://",
        connection_builder=_build_singlestoredb_connection,
        upsert_style=UpsertStyle.ON_DUPLICATE_KEY,
        get_list_indexes_sql=singlestore_get_list_indexes_sql,
        get_index_exists_sql=singlestore_get_index_exists_sql,
        index_caps=IndexCapability(
            drop_scope=DropScope.TABLE_SCOPED, partial=False,
            native_if_not_exists=False, native_if_exists=False,
            index_types=frozenset({"btree", "hash"}),
        ),
        transaction_support=TransactionSupport.LIMITED,
        begin_statement="BEGIN",
    ),
    "exasol": DialectSpec(
        ibis_backend_name="exasol",
        connection_mode=_KWARGS,
        connection_string_scheme="exasol://",
        connection_builder=_build_exasol_connection,
        ibis_connection_adapter=patch_exasol_connection,
        upsert_style=UpsertStyle.MERGE,
        transaction_support=TransactionSupport.FULL,
        begin_statement=None,
    ),
    "impala": DialectSpec(
        ibis_backend_name="impala",
        connection_mode=_KWARGS,
        connection_string_scheme="impala://",
        connection_builder=_build_impala_connection,
        transaction_support=TransactionSupport.NONE,
        begin_statement=None,
    ),
    "materialize": DialectSpec(
        ibis_backend_name="materialize",
        connection_mode=_KWARGS,
        connection_string_scheme="materialize://",
        connection_builder=_build_materialize_connection,
        transaction_support=TransactionSupport.FULL,
        begin_statement="BEGIN",
    ),
    "risingwave": DialectSpec(
        ibis_backend_name="risingwave",
        connection_mode=_KWARGS,
        connection_string_scheme="risingwave://",
        connection_builder=_build_risingwave_connection,
        upsert_style=UpsertStyle.ON_CONFLICT,
        transaction_support=TransactionSupport.LIMITED,
        begin_statement="BEGIN",
    ),
    "druid": DialectSpec(
        ibis_backend_name="druid",
        connection_mode=_KWARGS,
        connection_string_scheme="druid://",
        connection_builder=_build_druid_connection,
        transaction_support=TransactionSupport.NONE,
        begin_statement=None,
    ),
    "pyspark": DialectSpec(
        ibis_backend_name="pyspark",
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="pyspark://",
        connection_builder=_build_pyspark_connection,
        raw_handle_attr="_session",
        transaction_support=TransactionSupport.NONE,
        begin_statement=None,
    ),
}
