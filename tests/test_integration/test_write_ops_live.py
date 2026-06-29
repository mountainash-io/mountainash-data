"""Live round-trip tests for generic write ops (postgres + mysql)."""

import polars as pl
import pytest


@pytest.mark.integration
def test_rename_table_live_postgres(postgres_backend):
    be = postgres_backend
    be.create_table("ren_old", pl.DataFrame({"id": [1]}), overwrite=True)
    be.rename_table("ren_old", "ren_new")
    assert "ren_new" in be.list_tables()
    be.drop_table("ren_new", force=True)


@pytest.mark.integration
def test_rename_table_live_mysql(mysql_backend):
    be = mysql_backend
    be.create_table("ren_old", pl.DataFrame({"id": [1]}), overwrite=True)
    be.rename_table("ren_old", "ren_new")
    assert "ren_new" in be.list_tables()
    be.drop_table("ren_new", force=True)
