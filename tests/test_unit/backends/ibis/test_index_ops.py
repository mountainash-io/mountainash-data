"""Generic index dispatchers, exercised on in-memory sqlite/duckdb."""

import ibis
import polars as pl
import pytest

from mountainash_data.backends.ibis._index import (
    _generic_create_index,
    _generic_drop_index,
    _generic_index_exists,
)
from mountainash_data.backends.ibis.dialects._registry import DIALECTS

_SQLITE = DIALECTS["sqlite"].index_caps
_SQLITE_FN = DIALECTS["sqlite"].get_index_exists_sql
_DUCKDB = DIALECTS["duckdb"].index_caps
_DUCKDB_FN = DIALECTS["duckdb"].get_index_exists_sql


def _seed_sqlite():
    con = ibis.sqlite.connect()
    con.create_table("t", pl.DataFrame({"id": [1, 2], "active": [True, False]}))
    return con


class TestCreateDropExistsRoundtrip:
    def test_create_then_exists_then_drop(self):
        con = _seed_sqlite()
        _generic_create_index(
            con, "t", ["id"], index_name="idx_t_id", caps=_SQLITE,
            exists_sql_fn=_SQLITE_FN,
        )
        assert _generic_index_exists(con, "idx_t_id", table_name="t",
                                     exists_sql_fn=_SQLITE_FN) is True
        _generic_drop_index(con, "idx_t_id", table_name="t", caps=_SQLITE,
                            exists_sql_fn=_SQLITE_FN)
        assert _generic_index_exists(con, "idx_t_id", table_name="t",
                                     exists_sql_fn=_SQLITE_FN) is False

    def test_create_if_not_exists_is_idempotent_native(self):
        con = _seed_sqlite()
        for _ in range(2):
            _generic_create_index(
                con, "t", ["id"], index_name="idx_t_id", if_not_exists=True,
                caps=_SQLITE, exists_sql_fn=_SQLITE_FN,
            )  # second call must not raise (native IF NOT EXISTS)

    def test_default_index_name_generated(self):
        con = _seed_sqlite()
        _generic_create_index(con, "t", ["id"], caps=_SQLITE, exists_sql_fn=_SQLITE_FN)
        assert _generic_index_exists(con, "idx_t_id", table_name="t",
                                     exists_sql_fn=_SQLITE_FN) is True


class TestPartialIndex:
    def test_partial_where_on_sqlite(self):
        con = _seed_sqlite()
        _generic_create_index(
            con, "t", ["id"], index_name="idx_active",
            where=lambda r: r.active == True, caps=_SQLITE,  # noqa: E712
            exists_sql_fn=_SQLITE_FN,
        )
        assert _generic_index_exists(con, "idx_active", table_name="t",
                                     exists_sql_fn=_SQLITE_FN) is True

    def test_where_on_non_partial_dialect_raises(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1], "active": [True]}))
        with pytest.raises(ValueError, match="partial"):
            _generic_create_index(
                con, "t", ["id"], where=lambda r: r.active, caps=_DUCKDB,
                exists_sql_fn=_DUCKDB_FN,
            )


class TestValidationErrors:
    def test_unsupported_index_type_raises(self):
        con = _seed_sqlite()
        with pytest.raises(ValueError, match="index_type"):
            _generic_create_index(
                con, "t", ["id"], index_type="hash", caps=_SQLITE,
                exists_sql_fn=_SQLITE_FN,
            )

    def test_table_scoped_drop_requires_table_name(self):
        con = _seed_sqlite()
        mysql_caps = DIALECTS["mysql"].index_caps
        with pytest.raises(ValueError, match="table_name"):
            _generic_drop_index(con, "idx", table_name=None, caps=mysql_caps,
                                exists_sql_fn=DIALECTS["mysql"].get_index_exists_sql)

    def test_bad_identifier_rejected(self):
        con = _seed_sqlite()
        with pytest.raises(ValueError, match="simple identifier"):
            _generic_create_index(con, "t", ["id"], index_name="x; DROP",
                                  caps=_SQLITE, exists_sql_fn=_SQLITE_FN)

    def test_drop_if_exists_absent_is_noop_native(self):
        con = _seed_sqlite()
        _generic_drop_index(con, "nope", table_name="t", if_exists=True,
                            caps=_SQLITE, exists_sql_fn=_SQLITE_FN)  # no raise


class TestIndexExistsReadsColumnByPosition:
    """Guards DEBT-1: _generic_index_exists must read the COUNT column BY
    POSITION, not by its alias name. Oracle upper-cases the unquoted `count`
    alias to `COUNT`, so keying by the literal string "count" would KeyError
    on that dialect. Simulate a dialect whose introspection SQL uses an
    unrelated, differently-cased alias to prove the dispatcher never keys
    off the alias."""

    @staticmethod
    def _odd_alias_fn(index_name, table_name, namespace):
        tbl_clause = f" AND tbl_name = '{table_name}'" if table_name else ""
        return (
            "SELECT COUNT(*) AS \"TOTAL_MATCHES\" FROM sqlite_master "
            f"WHERE type = 'index' AND name = '{index_name}'{tbl_clause}"
        )

    def test_true_when_index_present_under_odd_alias(self):
        con = _seed_sqlite()
        _generic_create_index(con, "t", ["id"], index_name="idx_t_id",
                              caps=_SQLITE, exists_sql_fn=_SQLITE_FN)
        assert _generic_index_exists(
            con, "idx_t_id", table_name="t",
            exists_sql_fn=self._odd_alias_fn,
        ) is True

    def test_false_when_index_absent_under_odd_alias(self):
        con = _seed_sqlite()
        assert _generic_index_exists(
            con, "no_such_idx", table_name="t",
            exists_sql_fn=self._odd_alias_fn,
        ) is False


from mountainash_data import IbisBackend  # noqa: E402


class TestBackendDispatch:
    def test_create_exists_drop_via_backend(self):
        be = IbisBackend(dialect="sqlite", database=":memory:")
        be.connect()
        try:
            be.create_table("t", pl.DataFrame({"id": [1], "active": [True]}),
                            overwrite=True)
            assert be.create_index("t", ["id"], index_name="ix") is be
            assert be.index_exists("ix", table_name="t") is True
            assert be.drop_index("ix", table_name="t") is be
            assert be.index_exists("ix", table_name="t") is False
        finally:
            be.close()

    def test_where_predicate_via_backend(self):
        be = IbisBackend(dialect="sqlite", database=":memory:")
        be.connect()
        try:
            be.create_table("t", pl.DataFrame({"id": [1], "active": [True]}),
                            overwrite=True)
            be.create_index("t", ["id"], index_name="ixp",
                            where=lambda r: r.active == True)  # noqa: E712
            assert be.index_exists("ixp", table_name="t") is True
        finally:
            be.close()

    def test_unsupported_dialect_raises_notimplemented(self):
        from mountainash_data.backends.ibis.dialects._registry import DialectSpec
        be = IbisBackend(dialect="sqlite", database=":memory:")
        be.connect()
        try:
            # Rebind the INSTANCE's _spec to a fresh no-index spec (index_caps and
            # create_index_hook default to None). Never mutate the shared frozen
            # singleton in DIALECTS — that would corrupt other tests.
            be._spec = DialectSpec(
                ibis_backend_name="sqlite",
                connection_mode="connection_string",
                connection_string_scheme="sqlite://",
            )
            with pytest.raises(NotImplementedError):
                be.create_index("t", ["id"])
        finally:
            be.close()


def test_create_index_accepts_namespace_kwarg():
    from mountainash_data import IbisBackend

    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        backend.ibis_connection().raw_sql("CREATE SCHEMA idx_ns")
        backend.create_table("t", {"id": [1], "name": ["a"]}, namespace="idx_ns")
        backend.create_index("t", ["name"], namespace="idx_ns", index_name="idx_t_name")
        assert backend.index_exists("idx_t_name", table_name="t", namespace="idx_ns") is True


def test_index_ops_reject_database_kwarg():
    import pytest
    from mountainash_data import IbisBackend

    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1]})
        with pytest.raises(TypeError):
            backend.create_index("t", ["id"], database="x")


def test_create_index_rejects_catalog_qualified_namespace():
    """Index DDL builds engine-native SQL; catalog-qualified must raise (not reach SQL)."""
    import pytest
    from mountainash_data import IbisBackend
    from mountainash_data.core.namespace import Namespace

    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        backend.create_table("t", {"id": [1]})
        with pytest.raises(ValueError, match="does not support catalog-qualified"):
            backend.create_index("t", ["id"], namespace=Namespace(catalog="wh", path=("s",)))
