"""Database-related fixtures for testing."""

import sqlite3
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import ibis
import pytest

from .live_db_fixtures import (
    exasol_backend,
    mssql_backend,
    mysql_backend,
    oracle_backend,
    postgres_backend,
    singlestore_backend,
    trino_backend,
)


@contextmanager
def cleanup_test_objects(*cleanups: Callable[[], None]) -> Iterator[None]:
    """Run all cleanups, preserving body and control-flow failures."""
    body_error: BaseException | None = None
    try:
        yield
    except BaseException as exc:
        body_error = exc
        raise
    finally:
        cleanup_errors: list[BaseException] = []
        for cleanup in cleanups:
            try:
                cleanup()
            except BaseException as exc:
                cleanup_errors.append(exc)

        control_flow_index = next(
            (
                index
                for index, error in enumerate(cleanup_errors)
                if not isinstance(error, Exception)
            ),
            None,
        )
        if control_flow_index is not None:
            control_flow_error = cleanup_errors[control_flow_index]
            cause = body_error
            if cause is None and control_flow_index:
                cause = cleanup_errors[control_flow_index - 1]
            if cause is None:
                raise control_flow_error
            raise control_flow_error from cause
        if body_error is None and cleanup_errors:
            raise cleanup_errors[0]




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
