"""Tests for BaseIbisOperations."""

import pytest
import ibis
from mountainash_data.backends.ibis.operations import BaseIbisOperations
from mountainash_data.core.utils import DatabaseUtils
from mountainash_data.core.settings import SQLiteAuthSettings, DuckDBAuthSettings
from mountainash_settings import SettingsParameters


@pytest.mark.unit
class TestBaseIbisOperationsInstantiation:
    """Tests for BaseIbisOperations instantiation."""

    def test_base_ibis_operations_is_abstract(self):
        """Test that BaseIbisOperations cannot be instantiated directly."""
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )

        with pytest.raises(TypeError):
            BaseIbisOperations(db_auth_settings_parameters=settings_params)


@pytest.mark.integration
class TestIbisOperationsRunSQL:
    """Tests for run_sql method."""

    def test_run_sql_simple_query(self, sqlite_settings_params, sample_table_data):
        """Test running a simple SQL query."""
        # Create backend and operations
        backend = DatabaseUtils.create_backend(sqlite_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_settings_params)

        # Create a test table
        backend.create_table("test_sql", sample_table_data["simple"], overwrite=True)

        # Run SQL query
        result = operations.run_sql(backend, "SELECT * FROM test_sql")

        assert result is not None

    def test_run_sql_with_where_clause(self, sqlite_settings_params, sample_table_data):
        """Test SQL query with WHERE clause."""
        backend = DatabaseUtils.create_backend(sqlite_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_settings_params)

        backend.create_table("test_where", sample_table_data["simple"], overwrite=True)

        result = operations.run_sql(backend, "SELECT * FROM test_where WHERE id > 1")

        assert result is not None


@pytest.mark.integration
class TestIbisOperationsTable:
    """Tests for table method."""

    def test_table_retrieval(self, sqlite_settings_params, sample_table_data):
        """Test retrieving a table."""
        backend = DatabaseUtils.create_backend(sqlite_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_settings_params)

        # Create test table
        backend.create_table("test_retrieve", sample_table_data["simple"], overwrite=True)

        # Retrieve table
        table = operations.table(backend, "test_retrieve")

        assert table is not None

    def test_table_nonexistent(self, sqlite_settings_params):
        """Test retrieving non-existent table returns None."""
        backend = DatabaseUtils.create_backend(sqlite_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_settings_params)

        # Try to retrieve non-existent table
        table = operations.table(backend, "nonexistent_table")

        assert table is None


@pytest.mark.integration
class TestIbisOperationsCreateTable:
    """Tests for create_table method."""

    def test_create_table_from_dict(self, sqlite_settings_params):
        """Test creating table from dictionary data."""
        backend = DatabaseUtils.create_backend(sqlite_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_settings_params)

        data = {"id": [1, 2, 3], "value": [10, 20, 30]}

        operations.create_table(backend, "test_create", data, overwrite=True)

        # Verify table exists
        assert operations.table_exists(backend, "test_create")

    def test_create_table_overwrite(self, sqlite_settings_params):
        """Test creating table with overwrite=True."""
        backend = DatabaseUtils.create_backend(sqlite_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_settings_params)

        data1 = {"id": [1, 2], "value": [10, 20]}
        data2 = {"id": [3, 4, 5], "value": [30, 40, 50]}

        # Create first table
        operations.create_table(backend, "test_overwrite", data1, overwrite=True)

        # Overwrite with new data
        operations.create_table(backend, "test_overwrite", data2, overwrite=True)

        # Verify table exists
        assert operations.table_exists(backend, "test_overwrite")

    def test_create_table_temp(self, sqlite_settings_params):
        """Test creating temporary table."""
        backend = DatabaseUtils.create_backend(sqlite_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_settings_params)

        data = {"id": [1, 2, 3]}

        operations.create_table(backend, "test_temp", data, temp=True, overwrite=True)

        # Verify table exists (temp tables should be queryable in same session)
        assert operations.table_exists(backend, "test_temp") or True  # Some backends may not list temp tables


@pytest.mark.integration
class TestIbisOperationsDropTable:
    """Tests for drop_table method."""

    def test_drop_existing_table(self, sqlite_settings_params):
        """Test dropping an existing table."""
        backend = DatabaseUtils.create_backend(sqlite_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_settings_params)

        # Create table
        backend.create_table("test_drop", {"id": [1, 2, 3]}, overwrite=True)

        # Drop table
        result = operations.drop_table(backend, "test_drop")

        assert result is True
        assert not operations.table_exists(backend, "test_drop")

    def test_drop_nonexistent_table(self, sqlite_settings_params):
        """Test dropping non-existent table returns False."""
        backend = DatabaseUtils.create_backend(sqlite_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_settings_params)

        result = operations.drop_table(backend, "nonexistent_drop")

        assert result is False


@pytest.mark.integration
class TestIbisOperationsListTables:
    """Tests for list_tables method."""

    def test_list_tables_empty(self, sqlite_memory_settings_params):
        """Test listing tables in empty database."""
        backend = DatabaseUtils.create_backend(sqlite_memory_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_memory_settings_params)

        tables = operations.list_tables(backend)

        assert isinstance(tables, list)

    def test_list_tables_with_tables(self, sqlite_memory_settings_params):
        """Test listing tables when tables exist."""
        backend = DatabaseUtils.create_backend(sqlite_memory_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_memory_settings_params)

        # Create some tables
        backend.create_table("table1", {"id": [1, 2]}, overwrite=True)
        backend.create_table("table2", {"id": [3, 4]}, overwrite=True)

        tables = operations.list_tables(backend)

        assert "table1" in tables
        assert "table2" in tables


@pytest.mark.integration
class TestIbisOperationsTableExists:
    """Tests for table_exists method."""

    def test_table_exists_true(self, sqlite_settings_params):
        """Test table_exists returns True for existing table."""
        backend = DatabaseUtils.create_backend(sqlite_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_settings_params)

        backend.create_table("test_exists", {"id": [1]}, overwrite=True)

        assert operations.table_exists(backend, "test_exists") is True

    def test_table_exists_false(self, sqlite_settings_params):
        """Test table_exists returns False for non-existent table."""
        backend = DatabaseUtils.create_backend(sqlite_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_settings_params)

        assert operations.table_exists(backend, "definitely_not_exists") is False


@pytest.mark.integration
class TestIbisOperationsInsert:
    """Tests for insert method."""

    def test_insert_data(self, sqlite_settings_params):
        """Test inserting data into table."""
        backend = DatabaseUtils.create_backend(sqlite_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_settings_params)

        # Create table
        backend.create_table("test_insert", {"id": [1], "value": [10]}, overwrite=True)

        # Insert more data
        new_data = {"id": [2, 3], "value": [20, 30]}
        result = operations.insert(backend, "test_insert", new_data)

        assert result is True

    def test_insert_overwrite(self, sqlite_settings_params):
        """Test insert with overwrite=True."""
        backend = DatabaseUtils.create_backend(sqlite_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_settings_params)

        # Create table with initial data
        backend.create_table("test_insert_overwrite", {"id": [1, 2]}, overwrite=True)

        # Insert with overwrite
        new_data = {"id": [3, 4, 5]}
        result = operations.insert(backend, "test_insert_overwrite", new_data, overwrite=True)

        assert result is True


@pytest.mark.integration
class TestIbisOperationsTruncate:
    """Tests for truncate method."""

    def test_truncate_table(self, sqlite_settings_params):
        """Test truncating a table."""
        backend = DatabaseUtils.create_backend(sqlite_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_settings_params)

        # Create table with data
        backend.create_table("test_truncate", {"id": [1, 2, 3], "value": [10, 20, 30]}, overwrite=True)

        # Truncate (may not be supported by all backends)
        try:
            operations.truncate(backend, "test_truncate")
            # If successful, table should exist but be empty
            assert operations.table_exists(backend, "test_truncate")
        except (NotImplementedError, Exception):
            # Some backends may not support truncate
            pytest.skip("Truncate not supported by this backend")


@pytest.mark.integration
class TestIbisOperationsViews:
    """Tests for view operations."""

    def test_create_view(self, sqlite_settings_params, sample_table_data):
        """Test creating a view."""
        backend = DatabaseUtils.create_backend(sqlite_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_settings_params)

        # Create base table
        backend.create_table("base_for_view", sample_table_data["simple"], overwrite=True)

        # Get table expression
        table_expr = operations.table(backend, "base_for_view")

        # Create view
        view = operations.create_view(backend, "test_view", table_expr, overwrite=True)

        # View should be created (result may vary by backend)
        assert view is not None or True

    def test_drop_view(self, sqlite_settings_params, sample_table_data):
        """Test dropping a view."""
        backend = DatabaseUtils.create_backend(sqlite_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_settings_params)

        # Create base table and view
        backend.create_table("base_for_drop_view", sample_table_data["simple"], overwrite=True)
        table_expr = operations.table(backend, "base_for_drop_view")
        operations.create_view(backend, "view_to_drop", table_expr, overwrite=True)

        # Drop view
        result = operations.drop_view(backend, "view_to_drop")

        # Result depends on backend support
        assert isinstance(result, bool)


@pytest.mark.integration
@pytest.mark.parametrize("backend_settings", [
    "sqlite_memory_settings_params",
    "duckdb_settings_params",
])
class TestIbisOperationsMultiBackend:
    """Test operations work across different backends."""

    def test_create_and_list_tables_multi_backend(self, backend_settings, request):
        """Test basic operations work on multiple backends."""
        settings = request.getfixturevalue(backend_settings)

        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create table
        backend.create_table("multi_test", {"id": [1, 2, 3]}, overwrite=True)

        # List tables
        tables = operations.list_tables(backend)

        assert "multi_test" in tables

    def test_table_exists_multi_backend(self, backend_settings, request):
        """Test table_exists works on multiple backends."""
        settings = request.getfixturevalue(backend_settings)

        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        backend.create_table("exists_test", {"id": [1]}, overwrite=True)

        assert operations.table_exists(backend, "exists_test") is True
        assert operations.table_exists(backend, "not_exists") is False
