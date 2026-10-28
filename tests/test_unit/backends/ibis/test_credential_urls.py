"""F-4 (W4): credentials in connection URLs are never mis-parsed and never leak.

Layers exercised: Mountainash dialect builders / URL factory / IbisBackend
(ibis.connect is stubbed where a driver would be needed; the "unknown scheme"
failure test runs real ibis).
"""
from __future__ import annotations

import ibis
import pytest
from urllib.parse import parse_qs, urlsplit

from mountainash_auth_client import PasswordAuthProfile, TokenAuthProfile
from mountainash_data.backends.ibis.backend import IbisBackend
from mountainash_data.backends.ibis.dialects import _registry
from mountainash_data.core.factories.connection_factory import build_connection_string
from mountainash_data.core.settings import MotherDuckBackendProfile, PostgreSQLBackendProfile

# Every URL-significant character: '@' ':' '/' '#' '%' plus '?', '&', '+', ' '.
NASTY_PW = "SENTINEL-p@ss:w/o#r%d?x&y+z q"
NASTY_USER = "SENTINEL-us@r:na/me"
RAW_PW = NASTY_PW.replace(" ", "")   # unencoded password inside a caller-supplied URL string
NASTY_TOKEN = "SENTINEL-tok@en:/#%?&+=x"


def _assert_clean(exc: BaseException, *secrets: str) -> None:
    blobs = [str(exc), repr(exc), repr(exc.__dict__), repr(exc.args)]
    for secret in secrets:
        for blob in blobs:
            assert secret not in blob
    assert "SENTINEL" not in "".join(blobs)
    assert exc.__cause__ is None and exc.__context__ is None


@pytest.fixture
def captured(monkeypatch):
    calls: list[tuple[str, dict]] = []

    def fake_connect(url, **kwargs):
        calls.append((url, kwargs))
        return object()

    monkeypatch.setattr(ibis, "connect", fake_connect)
    return calls


@pytest.mark.parametrize(
    "builder, scheme, host_cfg",
    [
        (_registry._build_postgres_connection, "postgres", {"host": "dbhost", "port": 5432}),
        (_registry._build_mysql_connection, "mysql", {"host": "dbhost", "port": 3306}),
        (_registry._build_mssql_connection, "mssql", {"host": "dbhost", "port": 1433}),
    ],
)
def test_host_port_builders_keep_credentials_out_of_the_url(captured, builder, scheme, host_cfg):
    builder(user=NASTY_USER, password=NASTY_PW, database="app", **host_cfg)

    (url, kwargs), = captured
    parts = urlsplit(url)
    assert (parts.scheme, parts.hostname, parts.port, parts.path) == (scheme, "dbhost", host_cfg["port"], "/app")
    assert parts.username is None and parts.password is None
    assert "SENTINEL" not in url
    # ibis does not unquote usernames, so credentials travel as exact driver kwargs.
    assert kwargs["user"] == NASTY_USER and kwargs["password"] == NASTY_PW


def test_oracle_builder_keeps_credentials_out_of_the_url(captured, monkeypatch):
    import mountainash_data.backends.ibis._oracle_compat as compat

    monkeypatch.setattr(compat, "patch_oracle_connection", lambda conn: conn)
    _registry._build_oracle_connection(host="dbhost", port=1521, user=NASTY_USER, password=NASTY_PW, database="svc")

    (url, kwargs), = captured
    assert urlsplit(url).hostname == "dbhost" and "SENTINEL" not in url
    assert kwargs["user"] == NASTY_USER and kwargs["password"] == NASTY_PW


def test_snowflake_builder_keeps_credentials_out_of_the_url(captured):
    _registry._build_snowflake_connection(
        user=NASTY_USER, password=NASTY_PW, account="acct", database="db", schema="sch"
    )

    (url, kwargs), = captured
    parts = urlsplit(url)
    assert (parts.hostname, parts.path) == ("acct", "/db/sch")
    assert "SENTINEL" not in url
    assert kwargs["user"] == NASTY_USER and kwargs["password"] == NASTY_PW


def test_trino_builder_keeps_user_out_of_the_url(captured):
    _registry._build_trino_connection(host="dbhost", port=8080, user=NASTY_USER, catalog="c")

    (url, kwargs), = captured
    assert urlsplit(url).hostname == "dbhost" and "SENTINEL" not in url
    assert kwargs["user"] == NASTY_USER


def test_motherduck_builder_percent_encodes_the_token(captured):
    _registry._build_motherduck_connection(database="mydb", token=NASTY_TOKEN)

    (url, _), = captured
    parts = urlsplit(url)
    assert parts.fragment == ""                       # '#' did not start a fragment
    assert parse_qs(parts.query) == {"motherduck_token": [NASTY_TOKEN]}   # ibis.connect parses with parse_qsl


def test_motherduck_url_factory_percent_encodes_the_token():
    url = build_connection_string(MotherDuckBackendProfile(DATABASE="mydb"), TokenAuthProfile(TOKEN=NASTY_TOKEN))

    parts = urlsplit(url)
    assert parts.fragment == ""
    assert parse_qs(parts.query) == {"motherduck_token": [NASTY_TOKEN]}


def test_password_url_round_trips_through_ibis_backend():
    profile = PostgreSQLBackendProfile(HOST="db", PORT=5432, DATABASE="app")
    url = build_connection_string(profile, PasswordAuthProfile(USERNAME=NASTY_USER, PASSWORD=NASTY_PW))

    config, clean_url = IbisBackend(url)._resolve_url_auth(url, None)

    assert config["user"] == NASTY_USER and config["password"] == NASTY_PW
    assert clean_url == "postgresql://db:5432/app"


def test_credential_less_username_url_never_reaches_ibis_with_password(captured):
    # ':secret@host' has an empty username; the password must still be stripped.
    be = IbisBackend("postgresql://:SENTINEL-pw@dbhost:5432/app")
    config, clean_url = be._resolve_url_auth("postgresql://:SENTINEL-pw@dbhost:5432/app", None)

    assert "SENTINEL" not in clean_url
    assert config["password"] == "SENTINEL-pw"


def test_unencoded_slash_in_url_password_error_is_value_free():
    # 'pa/ss' ends the authority early, so urllib reports a "port" fragment of the password.
    with pytest.raises(ValueError) as info:
        IbisBackend("postgres://user:SENTINEL/ss@dbhost/app")
    _assert_clean(info.value, "SENTINEL")
    assert "percent-encode" in str(info.value)


def test_ibis_unknown_scheme_error_does_not_carry_url_userinfo():
    # Real ibis echoes the whole connection string in its ValueError.
    with pytest.raises(ValueError) as info:
        _registry._build_postgres_connection(connection_string=f"bogus://user:{RAW_PW}@host/db")
    _assert_clean(info.value, RAW_PW)
    assert "bogus://" in str(info.value)               # still useful for diagnosis


def test_url_path_connect_failure_does_not_carry_motherduck_token(monkeypatch):
    def boom(url, **kwargs):
        raise ValueError(f"Don't know how to connect to {url!r}")

    monkeypatch.setattr(ibis, "connect", boom)
    be = IbisBackend("duckdb://md:mydb?motherduck_token=SENTINEL-tok")
    with pytest.raises(ValueError) as info:
        be.connect()
    _assert_clean(info.value, "SENTINEL-tok")


def test_driver_exception_types_are_wrapped_not_leaked(monkeypatch):
    class DriverError(Exception):
        pass

    def boom(url, **kwargs):
        raise DriverError(f"could not connect: {url}")

    monkeypatch.setattr(ibis, "connect", boom)
    with pytest.raises(Exception) as info:
        _registry._build_mysql_connection(connection_string=f"mysql://u:{RAW_PW}@h/db")
    _assert_clean(info.value, RAW_PW)
    assert type(info.value).__name__ == "BackendConnectionError"


def test_clean_exceptions_propagate_unchanged(monkeypatch):
    original = RuntimeError("network unreachable")
    monkeypatch.setattr(ibis, "connect", lambda url, **kw: (_ for _ in ()).throw(original))
    with pytest.raises(RuntimeError) as info:
        _registry._build_postgres_connection(host="h", user="u", password="p")
    assert info.value is original


def test_userinfo_redaction_runs_to_the_last_at_sign(monkeypatch):
    # the in-password '@' must not end the userinfo early and leave a tail behind
    def boom(url, **kwargs):
        raise ValueError(f"Don't know how to connect to {url!r}")

    monkeypatch.setattr(ibis, "connect", boom)
    with pytest.raises(ValueError) as info:
        _registry._build_postgres_connection(connection_string="postgres://u:a@SENTINEL-tail:x@h/db")
    _assert_clean(info.value)
    assert str(info.value).endswith("postgres://***@h/db'")


@pytest.mark.parametrize("url", ["duckdb://:memory:", "sqlite://:memory:", "duckdb://md:mydb"])
def test_in_process_urls_are_not_rejected_by_port_validation(url):
    IbisBackend(url)


def test_driver_kwargs_cannot_collide_with_connect_helper_parameters(captured):
    # kwargs travel as a mapping, so option names like these bind to ibis, not the helper
    _registry._build_postgres_connection(
        host="h", user="u", password="p", secrets="x", kwargs="y", connect="z", connection_string=None, _secrets="w"
    )
    (_, passed), = captured
    assert passed == {"user": "u", "password": "p", "secrets": "x", "kwargs": "y", "connect": "z", "_secrets": "w"}
