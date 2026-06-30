"""rename_table works via the sqlglot generic default on every dialect."""

import ibis
import polars as pl
import pytest

from mountainash_data import IbisBackend
from mountainash_data.backends.ibis.operations import _generic_rename_table, build_rename_sql
from mountainash_data.backends.ibis.dialects._registry import DIALECTS

# ibis backend name -> sqlglot dialect name (identity unless listed).
# Pinned against sqlglot 30.12.0 by probe: impala/pyspark are unknown to
# sqlglot, mapped to their wire-compatible Hive/Spark base; mssql/singlestoredb
# differ by name. (motherduck/redshift already carry ibis_backend_name
# duckdb/postgres, so they need no entry.)
_IBIS_TO_SQLGLOT = {
    "mssql": "tsql",
    "motherduck": "duckdb",
    "singlestoredb": "singlestore",
    "impala": "hive",
    "pyspark": "spark",
}


class TestGenericRenameTable:
    def test_renames_on_duckdb(self):
        con = ibis.duckdb.connect()
        con.create_table("old", pl.DataFrame({"id": [1]}))
        _generic_rename_table(con, "old", "new")
        names = con.list_tables()
        assert "new" in names and "old" not in names

    def test_renames_on_sqlite(self):
        con = ibis.sqlite.connect()
        con.create_table("old", pl.DataFrame({"id": [1]}))
        _generic_rename_table(con, "old", "new")
        assert "new" in con.list_tables()

    def test_rejects_dotted_names(self):
        con = ibis.duckdb.connect()
        con.create_table("old", pl.DataFrame({"id": [1]}))
        with pytest.raises(ValueError, match="simple"):
            _generic_rename_table(con, "a.old", "new")

    def test_backend_rename_table_returns_self(self):
        with IbisBackend(dialect="duckdb", database=":memory:") as be:
            be.create_table("old", pl.DataFrame({"id": [1]}))
            assert be.rename_table("old", "new") is be
            assert "new" in be.list_tables()


class TestRenameGoldenPerDialect:
    """Registry-iterating render assertion — every dialect renders a rename."""

    @pytest.mark.parametrize("name", list(DIALECTS))
    def test_every_dialect_renders_rename(self, name):
        d = _IBIS_TO_SQLGLOT.get(
            DIALECTS[name].ibis_backend_name,
            DIALECTS[name].ibis_backend_name,
        )
        sql = build_rename_sql("old", "new", dialect=d)
        # tsql renders sp_rename; everyone else an ALTER ... RENAME
        assert ("sp_rename" in sql.lower()) or ("rename" in sql.lower())
