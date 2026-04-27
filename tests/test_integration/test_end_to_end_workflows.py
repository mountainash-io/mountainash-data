"""End-to-end integration tests using IbisBackend."""

import pytest
import polars as pl
from mountainash_data.backends.ibis.backend import IbisBackend
from mountainash_data.core.settings import DuckDBAuthSettings
from mountainash_settings import SettingsParameters


@pytest.mark.integration
class TestIbisBackendWorkflow:
    """Test complete workflows through IbisBackend."""

    def test_sqlite_dialect_workflow(self):
        """Full workflow with dialect= keyword."""
        with IbisBackend(dialect="sqlite", database=":memory:") as backend:
            backend.create_table("users", {"id": [1, 2], "name": ["a", "b"]})
            assert "users" in backend.list_tables()
            tbl = backend.table("users")
            assert tbl is not None

    def test_sqlite_url_workflow(self, tmp_path):
        """Full workflow from URL."""
        db_file = tmp_path / "test.db"
        with IbisBackend(f"sqlite:///{db_file}") as backend:
            backend.create_table("t", {"id": [1, 2, 3]})
            assert "t" in backend.list_tables()
        assert db_file.exists()

    def test_duckdb_settings_workflow(self):
        """Full workflow from SettingsParameters."""
        from mountainash_data.core.settings import NoAuth
        params = SettingsParameters.create(
            settings_class=DuckDBAuthSettings,
            DATABASE=":memory:",
            auth=NoAuth(),
        )
        with IbisBackend(params) as backend:
            backend.create_table("t", {"id": [1, 2]})
            assert "t" in backend.list_tables()

    def test_fluent_chaining_workflow(self):
        """Fluent API chaining across multiple operations."""
        with IbisBackend(dialect="sqlite", database=":memory:") as backend:
            (
                backend
                .create_table("users", {"id": [1], "email": ["a@b.com"]})
                .create_index("users", ["email"], unique=True)
                .create_table("orders", {"id": [1], "user_id": [1]})
            )
            assert sorted(backend.list_tables()) == ["orders", "users"]

    def test_inspect_workflow(self):
        """Inspection methods work through the backend."""
        with IbisBackend(dialect="sqlite", database=":memory:") as backend:
            backend.create_table("t", {"id": [1], "name": ["a"]})
            info = backend.inspect_table("t")
            assert info.name == "t"
            assert len(info.columns) == 2

    def test_ibis_connection_seam(self):
        """ibis_connection() provides the seam to mountainash-expressions."""
        with IbisBackend(dialect="sqlite", database=":memory:") as backend:
            backend.create_table("t", {"id": [1, 2]})
            raw = backend.ibis_connection()
            tbl = raw.table("t")
            assert tbl is not None


@pytest.mark.integration
class TestDuckDBOperationsWorkflow:
    """Test DuckDB-specific operations (upsert, indexes) through IbisBackend."""

    def test_upsert_insert_new_rows(self):
        """Upsert inserts new rows when no conflicts exist."""
        with IbisBackend(dialect="duckdb", database=":memory:") as backend:
            initial = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
            backend.create_table("users", initial)
            backend.create_unique_index("users", ["id"])

            new_data = pl.DataFrame({"id": [3, 4], "name": ["Charlie", "Diana"]})
            backend.upsert("users", new_data, conflict_columns=["id"])

            count = backend.run_sql("SELECT COUNT(*) as cnt FROM users").to_polars()["cnt"][0]
            assert count == 4

    def test_upsert_update_existing_rows(self):
        """Upsert updates existing rows on conflict."""
        with IbisBackend(dialect="duckdb", database=":memory:") as backend:
            initial = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "score": [100, 200]})
            backend.create_table("users", initial)
            backend.create_unique_index("users", ["id"])

            update = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "score": [150, 250]})
            backend.upsert("users", update, conflict_columns=["id"])

            result = backend.run_sql("SELECT score FROM users ORDER BY id").to_polars()
            assert list(result["score"]) == [150, 250]

    def test_index_lifecycle(self):
        """Create, check, list, drop index."""
        with IbisBackend(dialect="duckdb", database=":memory:") as backend:
            backend.create_table("t", {"id": [1, 2], "name": ["a", "b"]})

            backend.create_index("t", ["name"], index_name="idx_name")
            assert backend.index_exists("idx_name") is True

            indexes = backend.list_indexes("t")
            assert any(idx.get("name") == "idx_name" for idx in indexes)

            backend.drop_index("idx_name")
            assert backend.index_exists("idx_name") is False
