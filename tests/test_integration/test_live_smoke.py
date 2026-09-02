"""Smoke test that the live-db fixtures connect or skip correctly."""

import pytest


@pytest.mark.integration
def test_postgres_smoke(postgres_backend):
    assert isinstance(postgres_backend.list_tables(), list)


@pytest.mark.integration
def test_mysql_smoke(mysql_backend):
    assert isinstance(mysql_backend.list_tables(), list)


@pytest.mark.integration
def test_oracle_smoke(oracle_backend):
    assert isinstance(oracle_backend.list_tables(), list)


@pytest.mark.integration
def test_singlestoredb_smoke(singlestore_backend):
    assert isinstance(singlestore_backend.list_tables(), list)


@pytest.mark.integration
def test_mssql_smoke(mssql_backend):
    assert isinstance(mssql_backend.list_tables(), list)


@pytest.mark.integration
def test_trino_smoke(trino_backend):
    assert isinstance(trino_backend.list_tables(), list)
