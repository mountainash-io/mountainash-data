"""Database-related fixtures for testing."""

import os
import pytest
import tempfile
import sqlite3
from pathlib import Path
from typing import Generator
import ibis

from mountainash_data import IbisBackend

_PG = dict(
    host=os.environ.get("IBIS_TEST_POSTGRES_HOST", os.environ.get("PGHOST", "localhost")),
    port=int(os.environ.get("IBIS_TEST_POSTGRES_PORT", os.environ.get("PGPORT", "5432"))),
    user=os.environ.get("IBIS_TEST_POSTGRES_USER", os.environ.get("PGUSER", "postgres")),
    password=os.environ.get("IBIS_TEST_POSTGRES_PASSWORD", os.environ.get("PGPASSWORD", "postgres")),
    database=os.environ.get("IBIS_TEST_POSTGRES_DATABASE", os.environ.get("PGDATABASE", "ibis_testing")),
)
_MY = dict(
    host=os.environ.get("IBIS_TEST_MYSQL_HOST", "localhost"),
    port=int(os.environ.get("IBIS_TEST_MYSQL_PORT", "3306")),
    user=os.environ.get("IBIS_TEST_MYSQL_USER", "ibis"),
    password=os.environ.get("IBIS_TEST_MYSQL_PASSWORD", "ibis"),
    database=os.environ.get("IBIS_TEST_MYSQL_DATABASE", "ibis_testing"),
)
_ORA = dict(
    host=os.environ.get("IBIS_TEST_ORACLE_HOST", "localhost"),
    port=int(os.environ.get("IBIS_TEST_ORACLE_PORT", "1521")),
    user=os.environ.get("IBIS_TEST_ORACLE_USER", "app"),
    password=os.environ.get("IBIS_TEST_ORACLE_PASSWORD", "app"),
    database=os.environ.get("IBIS_TEST_ORACLE_DATABASE", "XEPDB1"),
)


def _live_or_skip(dialect: str, params: dict):
    require = os.environ.get("MOUNTAINASH_REQUIRE_LIVE_DB") == "1"
    try:
        be = IbisBackend(dialect=dialect, **params)
        be.connect()
        return be
    except Exception as exc:  # noqa: BLE001 - service availability gate
        msg = f"{dialect} service unreachable: {exc}"
        if require:
            pytest.fail(msg)
        pytest.skip(msg)


@pytest.fixture
def postgres_backend():
    be = _live_or_skip("postgres", _PG)
    try:
        yield be
    finally:
        be.close()


@pytest.fixture
def mysql_backend():
    be = _live_or_skip("mysql", _MY)
    try:
        yield be
    finally:
        be.close()


@pytest.fixture
def oracle_backend():
    be = _live_or_skip("oracle", _ORA)
    try:
        yield be
    finally:
        be.close()


@pytest.fixture(scope="session")
def temp_sqlite_db() -> Generator[Path, None, None]:
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_file:
        db_path = Path(tmp_file.name)

    # Create test tables
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        INSERT INTO test_table (id, name, value) VALUES
        (1, 'Alice', 100.5),
        (2, 'Bob', 200.7),
        (3, 'Charlie', 300.9)
    """)

    conn.commit()
    conn.close()

    yield db_path

    # Cleanup
    if db_path.exists():
        db_path.unlink()


@pytest.fixture(scope="session")
def temp_duckdb_db() -> Generator[Path, None, None]:
    """Create a temporary DuckDB database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as tmp_file:
        db_path = Path(tmp_file.name)

    # Create test data with DuckDB
    import duckdb
    conn = duckdb.connect(str(db_path))

    conn.execute("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            value DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        INSERT INTO test_table (id, name, value) VALUES
        (1, 'Alice', 100.5),
        (2, 'Bob', 200.7),
        (3, 'Charlie', 300.9)
    """)

    conn.close()

    yield db_path

    # Cleanup
    if db_path.exists():
        db_path.unlink()


@pytest.fixture(scope="session")
def ibis_sqlite_backend(temp_sqlite_db):
    """Create an Ibis SQLite backend for testing."""
    backend = ibis.sqlite.connect(str(temp_sqlite_db))
    yield backend
    # No cleanup needed - backend manages connection


@pytest.fixture(scope="session")
def ibis_duckdb_backend():
    """Create an Ibis DuckDB in-memory backend for testing."""
    backend = ibis.duckdb.connect(":memory:")
    yield backend
    # No cleanup needed


@pytest.fixture
def ibis_polars_backend():
    """Create an Ibis Polars backend for testing."""
    backend = ibis.polars.connect()
    return backend


@pytest.fixture
def sample_table_data():
    """Provide sample table data for various tests."""
    return {
        "simple": {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [100.5, 200.7, 300.9]
        },
        "complex": {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "category": ["A", "B", "A", "C", "B"],
            "value": [100.5, 200.7, 300.9, 400.2, 500.8],
            "active": [True, False, True, True, False]
        },
        "numeric_types": {
            "integers": [1, 2, 3, 4, 5],
            "floats": [1.1, 2.2, 3.3, 4.4, 5.5],
            "decimals": [10.01, 20.02, 30.03, 40.04, 50.05]
        },
        "string_types": {
            "id": [1, 2, 3],
            "short_text": ["a", "b", "c"],
            "long_text": ["Lorem ipsum", "dolor sit amet", "consectetur adipiscing"]
        },
        "with_nulls": {
            "id": [1, 2, 3, 4, 5],
            "nullable_int": [1, None, 3, None, 5],
            "nullable_string": ["a", "b", None, "d", None]
        }
    }


@pytest.fixture
def sample_table_schemas():
    """Provide sample Ibis table schemas for testing."""
    import ibis.expr.schema as sch

    return {
        "simple": sch.Schema({
            "id": "int64",
            "name": "string",
            "value": "float64"
        }),
        "complex": sch.Schema({
            "id": "int64",
            "name": "string",
            "category": "string",
            "value": "float64",
            "active": "bool"
        })
    }
