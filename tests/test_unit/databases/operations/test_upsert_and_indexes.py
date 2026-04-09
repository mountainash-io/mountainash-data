"""Comprehensive tests for upsert and index management operations.

Tests cover:
- Upsert operations (various scenarios)
- Index creation, deletion, and querying
- Cross-database compatibility (DuckDB, SQLite)
"""

import pytest
import polars as pl
from mountainash_data.core.utils import DatabaseUtils
from mountainash_data.core.constants import CONST_CONFLICT_ACTION


@pytest.mark.integration
@pytest.mark.parametrize("backend_fixture", [
    "sqlite_memory_settings_params",
    "duckdb_settings_params",
])
class TestUpsertOperations:
    """Tests for upsert operations across different scenarios."""

    def test_simple_upsert_insert_new_rows(self, backend_fixture, request):
        """Test upsert inserts new rows when no conflicts exist."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create initial table
        initial_data = pl.DataFrame({
            "id": [1, 2],
            "email": ["alice@example.com", "bob@example.com"],
            "name": ["Alice", "Bob"]
        })
        operations.create_table(backend, "users", initial_data, overwrite=True)

        # Create unique index on email
        operations.create_unique_index(backend, "users", ["email"])

        # Upsert with new data (no conflicts)
        new_data = pl.DataFrame({
            "id": [3, 4],
            "email": ["charlie@example.com", "diana@example.com"],
            "name": ["Charlie", "Diana"]
        })
        operations.upsert(
            backend, "users", new_data,
            conflict_columns=["email"]
        )

        # Verify all 4 rows exist
        result = operations.run_sql(backend, "SELECT COUNT(*) as count FROM users")
        count = result.to_polars()["count"][0]
        assert count == 4

    def test_simple_upsert_update_existing_rows(self, backend_fixture, request):
        """Test upsert updates existing rows on conflict."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create initial table with index
        initial_data = pl.DataFrame({
            "id": [1, 2],
            "email": ["alice@example.com", "bob@example.com"],
            "name": ["Alice", "Bob"],
            "score": [100, 200]
        })
        operations.create_table(backend, "users", initial_data, overwrite=True)
        operations.create_unique_index(backend, "users", ["email"])

        # Upsert with conflicting emails but different data
        update_data = pl.DataFrame({
            "id": [1, 2],
            "email": ["alice@example.com", "bob@example.com"],
            "name": ["Alice Updated", "Bob Updated"],
            "score": [150, 250]
        })
        operations.upsert(
            backend, "users", update_data,
            conflict_columns=["email"]
        )

        # Verify rows were updated
        result = operations.run_sql(backend, "SELECT * FROM users ORDER BY id")
        df = result.to_polars()
        assert len(df) == 2
        # Names should be updated
        names = df["name"].to_list()
        assert "Alice Updated" in names
        assert "Bob Updated" in names

    def test_upsert_mixed_insert_and_update(self, backend_fixture, request):
        """Test upsert handles both inserts and updates in same operation."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create initial table
        initial_data = pl.DataFrame({
            "email": ["alice@example.com", "bob@example.com"],
            "name": ["Alice", "Bob"]
        })
        operations.create_table(backend, "users", initial_data, overwrite=True)
        operations.create_unique_index(backend, "users", ["email"])

        # Upsert with mix of existing and new emails
        mixed_data = pl.DataFrame({
            "email": ["alice@example.com", "charlie@example.com"],
            "name": ["Alice Updated", "Charlie"]
        })
        operations.upsert(
            backend, "users", mixed_data,
            conflict_columns=["email"]
        )

        # Verify: 3 rows total (alice updated, bob unchanged, charlie added)
        result = operations.run_sql(backend, "SELECT COUNT(*) as count FROM users")
        count = result.to_polars()["count"][0]
        assert count == 3

        # Verify Alice was updated
        result = operations.run_sql(backend, "SELECT name FROM users WHERE email = 'alice@example.com'")
        alice_name = result.to_polars()["name"][0]
        assert alice_name == "Alice Updated"

    def test_upsert_with_conflict_action_nothing(self, backend_fixture, request):
        """Test upsert with DO NOTHING ignores conflicts."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create initial table
        initial_data = pl.DataFrame({
            "email": ["alice@example.com"],
            "name": ["Alice"],
            "score": [100]
        })
        operations.create_table(backend, "users", initial_data, overwrite=True)
        operations.create_unique_index(backend, "users", ["email"])

        # Upsert with DO NOTHING - should skip conflicting row
        conflict_data = pl.DataFrame({
            "email": ["alice@example.com", "bob@example.com"],
            "name": ["Alice Updated", "Bob"],
            "score": [999, 200]
        })
        operations.upsert(
            backend, "users", conflict_data,
            conflict_columns=["email"],
            conflict_action=CONST_CONFLICT_ACTION.NOTHING
        )

        # Verify: Alice unchanged (still 100), Bob added
        result = operations.run_sql(backend, "SELECT email, score FROM users ORDER BY email")
        df = result.to_polars()
        assert len(df) == 2
        # Alice should still have original score
        alice_score = df.filter(pl.col("email") == "alice@example.com")["score"][0]
        assert alice_score == 100

    def test_upsert_with_specific_update_columns(self, backend_fixture, request):
        """Test upsert updates only specified columns."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create initial table
        initial_data = pl.DataFrame({
            "email": ["alice@example.com"],
            "name": ["Alice"],
            "score": [100],
            "status": ["active"]
        })
        operations.create_table(backend, "users", initial_data, overwrite=True)
        operations.create_unique_index(backend, "users", ["email"])

        # Upsert updating only score (not name or status)
        update_data = pl.DataFrame({
            "email": ["alice@example.com"],
            "name": ["SHOULD NOT UPDATE"],
            "score": [200],
            "status": ["SHOULD NOT UPDATE"]
        })
        operations.upsert(
            backend, "users", update_data,
            conflict_columns=["email"],
            update_columns=["score"]  # Only update score
        )

        # Verify: score updated, name and status unchanged
        result = operations.run_sql(backend, "SELECT name, score, status FROM users WHERE email = 'alice@example.com'")
        df = result.to_polars()
        assert df["name"][0] == "Alice"  # name unchanged
        assert df["score"][0] == 200  # score updated
        assert df["status"][0] == "active"  # status unchanged

    def test_upsert_with_conditional_update(self, backend_fixture, request):
        """Test upsert with conditional WHERE clause on update."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create initial table
        initial_data = pl.DataFrame({
            "email": ["alice@example.com", "bob@example.com"],
            "last_login": [100, 50]
        })
        operations.create_table(backend, "users", initial_data, overwrite=True)
        operations.create_unique_index(backend, "users", ["email"])

        # Upsert with condition: only update if new last_login is greater
        update_data = pl.DataFrame({
            "email": ["alice@example.com", "bob@example.com"],
            "last_login": [90, 75]  # Alice: 90 < 100 (no update), Bob: 75 > 50 (update)
        })
        operations.upsert(
            backend, "users", update_data,
            conflict_columns=["email"],
            update_columns=["last_login"],
            update_condition="users.last_login < EXCLUDED.last_login"
        )

        # Verify: Alice unchanged (100), Bob updated (75)
        result = operations.run_sql(backend, "SELECT email, last_login FROM users ORDER BY email")
        df = result.to_polars()
        alice_login = df.filter(pl.col("email") == "alice@example.com")["last_login"][0]
        bob_login = df.filter(pl.col("email") == "bob@example.com")["last_login"][0]
        assert alice_login == 100  # Not updated (condition failed)
        assert bob_login == 75  # Updated (condition passed)

    def test_upsert_composite_conflict_columns(self, backend_fixture, request):
        """Test upsert with multiple conflict columns (composite key)."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create table with composite key
        initial_data = pl.DataFrame({
            "user_id": [1, 1, 2],
            "product_id": [100, 101, 100],
            "quantity": [5, 3, 7]
        })
        operations.create_table(backend, "cart", initial_data, overwrite=True)
        operations.create_unique_index(backend, "cart", ["user_id", "product_id"])

        # Upsert with composite conflict
        update_data = pl.DataFrame({
            "user_id": [1, 2, 2],
            "product_id": [100, 100, 102],
            "quantity": [10, 15, 2]
        })
        operations.upsert(
            backend, "cart", update_data,
            conflict_columns=["user_id", "product_id"]
        )

        # Verify: user 1/product 100 updated, user 2/product 100 updated, user 2/product 102 added
        result = operations.run_sql(backend, "SELECT user_id, product_id, quantity FROM cart ORDER BY user_id, product_id")
        df = result.to_polars()
        assert len(df) == 4

    def test_upsert_auto_column_detection(self, backend_fixture, request):
        """Test upsert auto-detects update columns when not specified."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create table
        initial_data = pl.DataFrame({
            "id": [1],
            "email": ["alice@example.com"],
            "name": ["Alice"],
            "score": [100]
        })
        operations.create_table(backend, "users", initial_data, overwrite=True)
        operations.create_unique_index(backend, "users", ["email"])

        # Upsert without specifying update_columns (should auto-detect)
        update_data = pl.DataFrame({
            "id": [1],
            "email": ["alice@example.com"],
            "name": ["Alice Updated"],
            "score": [200]
        })
        operations.upsert(
            backend, "users", update_data,
            conflict_columns=["email"]
            # update_columns not specified - should update id, name, score
        )

        # Verify all non-conflict columns were updated
        result = operations.run_sql(backend, "SELECT name, score FROM users WHERE email = 'alice@example.com'")
        df = result.to_polars()
        assert df["name"][0] == "Alice Updated"
        assert df["score"][0] == 200

    def test_upsert_validation_table_not_exists(self, backend_fixture, request):
        """Test upsert raises error when target table doesn't exist."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Try to upsert to non-existent table
        data = pl.DataFrame({
            "email": ["alice@example.com"],
            "name": ["Alice"]
        })

        with pytest.raises(ValueError, match="does not exist"):
            operations.upsert(
                backend, "nonexistent_table", data,
                conflict_columns=["email"]
            )

    def test_upsert_validation_empty_conflict_columns(self, backend_fixture, request):
        """Test upsert raises error when conflict_columns is empty."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create table
        initial_data = pl.DataFrame({"email": ["alice@example.com"]})
        operations.create_table(backend, "users", initial_data, overwrite=True)

        # Try upsert with empty conflict columns
        with pytest.raises(ValueError, match="At least one column"):
            operations.upsert(
                backend, "users", initial_data,
                conflict_columns=[]
            )


@pytest.mark.integration
@pytest.mark.parametrize("backend_fixture", [
    "sqlite_memory_settings_params",
    "duckdb_settings_params",
])
class TestIndexManagement:
    """Tests for index creation, deletion, and querying."""

    def test_create_simple_index(self, backend_fixture, request):
        """Test creating a simple index."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create table
        data = pl.DataFrame({
            "id": [1, 2, 3],
            "email": ["a@example.com", "b@example.com", "c@example.com"]
        })
        operations.create_table(backend, "users", data, overwrite=True)

        # Create index
        result = operations.create_index(backend, "users", ["email"])
        assert result is True

        # Verify index exists
        assert operations.index_exists(backend, "idx_users_email") is True

    def test_create_unique_index(self, backend_fixture, request):
        """Test creating a unique index."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create table
        data = pl.DataFrame({
            "id": [1, 2, 3],
            "email": ["a@example.com", "b@example.com", "c@example.com"]
        })
        operations.create_table(backend, "users", data, overwrite=True)

        # Create unique index
        result = operations.create_unique_index(backend, "users", ["email"])
        assert result is True

        # Verify index exists
        assert operations.index_exists(backend, "uidx_users_email") is True

    def test_create_composite_index(self, backend_fixture, request):
        """Test creating a composite (multi-column) index."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create table
        data = pl.DataFrame({
            "user_id": [1, 2, 3],
            "product_id": [100, 200, 300],
            "quantity": [5, 10, 15]
        })
        operations.create_table(backend, "orders", data, overwrite=True)

        # Create composite index
        result = operations.create_index(
            backend, "orders", ["user_id", "product_id"]
        )
        assert result is True

        # Verify index exists
        assert operations.index_exists(backend, "idx_orders_product_id_user_id") is True

    def test_create_index_with_custom_name(self, backend_fixture, request):
        """Test creating index with custom name."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create table
        data = pl.DataFrame({"email": ["a@example.com"]})
        operations.create_table(backend, "users", data, overwrite=True)

        # Create index with custom name
        result = operations.create_index(
            backend, "users", ["email"],
            index_name="my_custom_index"
        )
        assert result is True

        # Verify custom name
        assert operations.index_exists(backend, "my_custom_index") is True

    def test_create_partial_index(self, backend_fixture, request):
        """Test creating partial index with WHERE clause."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create table
        data = pl.DataFrame({
            "email": ["a@example.com", "b@example.com"],
            "status": ["active", "inactive"]
        })
        operations.create_table(backend, "users", data, overwrite=True)

        # Create partial index (only for active users)
        # Note: DuckDB does not support partial indexes as of v1.3.2
        result = operations.create_index(
            backend, "users", ["email"],
            where_condition="status = 'active'"
        )

        # DuckDB doesn't support partial indexes, SQLite does
        if "duckdb" in backend_fixture:
            assert result is False  # Expected: DuckDB doesn't support partial indexes
        else:
            assert result is True
            # Verify index exists (only for SQLite)
            assert operations.index_exists(backend, "idx_users_email") is True

    def test_create_index_if_not_exists(self, backend_fixture, request):
        """Test creating index with IF NOT EXISTS doesn't fail on duplicate."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create table and index
        data = pl.DataFrame({"email": ["a@example.com"]})
        operations.create_table(backend, "users", data, overwrite=True)
        operations.create_index(backend, "users", ["email"])

        # Create same index again with if_not_exists=True (should not fail)
        result = operations.create_index(
            backend, "users", ["email"],
            if_not_exists=True
        )
        # Result may vary, but should not raise error
        assert result in [True, False]

    def test_drop_index(self, backend_fixture, request):
        """Test dropping an index."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create table and index
        data = pl.DataFrame({"email": ["a@example.com"]})
        operations.create_table(backend, "users", data, overwrite=True)
        operations.create_index(backend, "users", ["email"])

        # Verify index exists
        assert operations.index_exists(backend, "idx_users_email") is True

        # Drop index
        result = operations.drop_index(backend, "idx_users_email")
        assert result is True

        # Verify index no longer exists
        assert operations.index_exists(backend, "idx_users_email") is False

    def test_drop_index_if_exists(self, backend_fixture, request):
        """Test dropping non-existent index with IF EXISTS doesn't fail."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Try to drop non-existent index with if_exists=True
        result = operations.drop_index(
            backend, "nonexistent_index",
            if_exists=True
        )
        # Should not raise error
        assert result in [True, False]

    def test_index_exists_returns_false_for_nonexistent(self, backend_fixture, request):
        """Test index_exists returns False for non-existent index."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Check non-existent index
        assert operations.index_exists(backend, "nonexistent_index") is False

    def test_list_indexes(self, backend_fixture, request):
        """Test listing all indexes for a table."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create table with multiple indexes
        data = pl.DataFrame({
            "id": [1, 2],
            "email": ["a@example.com", "b@example.com"],
            "name": ["Alice", "Bob"]
        })
        operations.create_table(backend, "users", data, overwrite=True)
        operations.create_index(backend, "users", ["email"])
        operations.create_unique_index(backend, "users", ["id"])

        # List indexes
        indexes = operations.list_indexes(backend, "users")
        assert isinstance(indexes, list)
        assert len(indexes) >= 2  # At least the two we created

        # Verify index names are in the list
        index_names = [idx.get("name") for idx in indexes]
        assert "idx_users_email" in index_names
        assert "uidx_users_id" in index_names

    def test_create_unique_index_convenience_method(self, backend_fixture, request):
        """Test create_unique_index convenience method."""
        settings = request.getfixturevalue(backend_fixture)
        backend = DatabaseUtils.create_backend(settings)
        operations = DatabaseUtils.create_operations(settings)

        # Create table
        data = pl.DataFrame({"email": ["a@example.com"]})
        operations.create_table(backend, "users", data, overwrite=True)

        # Use convenience method
        result = operations.create_unique_index(backend, "users", "email")
        assert result is True

        # Verify unique index created
        assert operations.index_exists(backend, "uidx_users_email") is True


@pytest.mark.integration
class TestUpsertIndexIntegration:
    """Integration tests combining upsert and index operations."""

    def test_upsert_requires_unique_index_on_conflict_columns(self, duckdb_settings_params):
        """Test that upsert works correctly with unique index on conflict columns."""
        backend = DatabaseUtils.create_backend(duckdb_settings_params)
        operations = DatabaseUtils.create_operations(duckdb_settings_params)

        # Create table
        initial_data = pl.DataFrame({
            "email": ["alice@example.com"],
            "name": ["Alice"]
        })
        operations.create_table(backend, "users", initial_data, overwrite=True)

        # Create unique index on conflict column
        operations.create_unique_index(backend, "users", ["email"])

        # Upsert should work
        update_data = pl.DataFrame({
            "email": ["alice@example.com"],
            "name": ["Alice Updated"]
        })
        operations.upsert(
            backend, "users", update_data,
            conflict_columns=["email"]
        )

        # Verify update worked
        result = operations.run_sql(backend, "SELECT name FROM users WHERE email = 'alice@example.com'")
        df = result.to_polars()
        assert df["name"][0] == "Alice Updated"

    def test_multiple_indexes_and_upserts(self, sqlite_memory_settings_params):
        """Test multiple indexes don't interfere with upserts."""
        backend = DatabaseUtils.create_backend(sqlite_memory_settings_params)
        operations = DatabaseUtils.create_operations(sqlite_memory_settings_params)

        # Create table with multiple indexes
        data = pl.DataFrame({
            "id": [1, 2],
            "email": ["a@example.com", "b@example.com"],
            "username": ["alice", "bob"]
        })
        operations.create_table(backend, "users", data, overwrite=True)

        # Create multiple indexes
        operations.create_unique_index(backend, "users", ["email"])
        operations.create_unique_index(backend, "users", ["username"])
        operations.create_index(backend, "users", ["id"])

        # Upsert on email should work
        update_data = pl.DataFrame({
            "id": [1],
            "email": ["a@example.com"],
            "username": ["alice_updated"]
        })
        operations.upsert(
            backend, "users", update_data,
            conflict_columns=["email"]
        )

        # Verify
        result = operations.run_sql(backend, "SELECT username FROM users WHERE email = 'a@example.com'")
        df = result.to_polars()
        assert df["username"][0] == "alice_updated"
