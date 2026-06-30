"""Smoke test that the live-db fixtures connect or skip correctly."""

import pytest


@pytest.mark.integration
def test_postgres_smoke(postgres_backend):
    assert isinstance(postgres_backend.list_tables(), list)


@pytest.mark.integration
def test_mysql_smoke(mysql_backend):
    assert isinstance(mysql_backend.list_tables(), list)
