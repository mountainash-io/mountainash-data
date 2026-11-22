"""URL handling: profile -> URL -> IbisBackend -> outbound ibis.connect (recorder).

Configuration-flow evidence only; no live connectivity.
"""
from __future__ import annotations

import ibis
import pytest
from mountainash_auth_client import NoAuthProfile, PasswordAuthProfile, TokenAuthProfile

from mountainash_data.backends.ibis import backend as backend_mod
from mountainash_data.backends.ibis.backend import _SCHEME_TO_DIALECT, IbisBackend
from mountainash_data.core.factories.connection_factory import (
    build_connection_string,
    provider_for_scheme,
)
from mountainash_data.core.settings import (
    BigQueryBackendProfile,
    DuckDBBackendProfile,
    MotherDuckBackendProfile,
    PostgreSQLBackendProfile,
    RedshiftBackendProfile,
    SnowflakeBackendProfile,
    SQLiteBackendProfile,
)

PW = PasswordAuthProfile(USERNAME="u", PASSWORD="SENTINEL-pw")


@pytest.fixture
def recorder(monkeypatch):
    calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(ibis, "connect", lambda url, **kw: calls.append((url, kw)) or object())
    monkeypatch.setattr(backend_mod, "_import_ibis_backend", lambda spec, dialect: None)
    return calls


CASES = [
    ("sqlite", lambda: SQLiteBackendProfile(DATABASE="x.db"), NoAuthProfile(),
     "sqlite", "sqlite:x.db", {}),
    ("duckdb", lambda: DuckDBBackendProfile(DATABASE="x.db"), NoAuthProfile(),
     "duckdb", "duckdb:x.db", {}),
    ("postgres", lambda: PostgreSQLBackendProfile(HOST="h", PORT=5433, DATABASE="app"), PW,
     "postgres", "postgresql://h:5433/app", {"user": "u", "password": "SENTINEL-pw"}),
    ("redshift", lambda: RedshiftBackendProfile(HOST="h", DATABASE="d", REGION="us-east-1"), PW,
     "postgres", "postgresql://h:5439/d", {"user": "u", "password": "SENTINEL-pw"}),
    ("motherduck", lambda: MotherDuckBackendProfile(DATABASE="mydb"), TokenAuthProfile(TOKEN="T"),
     "motherduck", "duckdb://md:mydb?motherduck_token=T", {}),
    ("bigquery", lambda: BigQueryBackendProfile(PROJECT_ID="my-project-1", DATASET_ID="ds"), NoAuthProfile(),
     "bigquery", "bigquery://my-project-1/ds", {}),
    ("snowflake",
     lambda: SnowflakeBackendProfile(ACCOUNT="acct", DATABASE="db", SCHEMA="sch", WAREHOUSE="w", ROLE="r"), PW,
     "snowflake", "snowflake://acct/db/sch?warehouse=w&role=r", {"user": "u", "password": "SENTINEL-pw"}),
]


@pytest.mark.parametrize("name, make, auth, dialect, outbound, kwargs", CASES, ids=[c[0] for c in CASES])
def test_round_trip_reaches_driver_with_destination_and_credentials(recorder, name, make, auth, dialect, outbound, kwargs):
    url = build_connection_string(make(), auth)
    be = IbisBackend(url)
    assert be.dialect == dialect
    be.connect()
    (sent_url, sent_kwargs), = recorder
    assert sent_url == outbound
    assert sent_kwargs == kwargs
    assert "SENTINEL" not in sent_url


@pytest.mark.parametrize("scheme", sorted(s for s in _SCHEME_TO_DIALECT if s != "pyspark"))
def test_credential_path_every_scheme(recorder, scheme, monkeypatch):
    import sys
    import types
    from unittest import mock

    # exasol's connect shim needs a usable handle; trino's adapter imports its driver
    monkeypatch.setattr(ibis, "connect", lambda url, **kw: recorder.append((url, kw)) or mock.MagicMock())
    trino_auth = types.ModuleType("trino.auth")
    trino_auth.BasicAuthentication = lambda user, pw: ("basic", user, pw)
    monkeypatch.setitem(sys.modules, "trino", types.ModuleType("trino"))
    monkeypatch.setitem(sys.modules, "trino.auth", trino_auth)
    be = IbisBackend(f"{scheme}://u:SENTINEL@h/db")
    assert be.dialect == _SCHEME_TO_DIALECT[scheme]
    provider_for_scheme(scheme)  # resolves through the single scheme table
    try:
        be.connect()
    except ValueError as exc:  # scheme without a password adapter fails closed
        assert "no auth adapter" in str(exc)
        return
    (sent_url, sent_kwargs), = recorder
    assert "SENTINEL" not in sent_url
    assert sent_kwargs.get("password") == "SENTINEL" or "SENTINEL" in sent_kwargs.get("auth", ())


def test_postgres_scheme_delivers_credentials(recorder):
    IbisBackend("postgres://u:SENTINEL@h:5432/db").connect()
    (url, kwargs), = recorder
    assert url == "postgres://h:5432/db"
    assert kwargs == {"user": "u", "password": "SENTINEL"}


@pytest.mark.parametrize(
    "bad", ["redshift://u:SENTINEL@h/db", "md:mydb", "C:\\db.sqlite", "nosuch://u:SENTINEL@h/db",
            "user:SENTINEL@host/db"],
)
def test_unsupported_strings_rejected_without_echo(bad):
    with pytest.raises(ValueError, match="schemes") as ei:
        IbisBackend(bad)
    assert "SENTINEL" not in str(ei.value) and bad not in str(ei.value)
    assert "postgres" in str(ei.value)


def test_authority_less_forms_route_as_urls():
    assert IbisBackend("sqlite:x.db").dialect == "sqlite"
    assert IbisBackend("duckdb:").dialect == "duckdb"
    assert IbisBackend("duckdb://md:x").dialect == "motherduck"
