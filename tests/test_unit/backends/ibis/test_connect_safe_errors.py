"""W8 sentinel walk: no secret on any error surface of connect().

Failures are injected at each boundary of the connect composition.
"""
from __future__ import annotations

import dataclasses
import importlib

import ibis
import pytest
from mountainash_auth_client import PasswordAuthProfile
from mountainash_settings import SettingsParameters

from mountainash_data.backends.ibis import backend as backend_mod
from mountainash_data.backends.ibis.backend import IbisBackend
from mountainash_data.core.errors import BackendConnectionError
from mountainash_data.core.factories import connection_factory
from mountainash_data.core.settings import PostgreSQLBackendProfile
from tests.helpers.error_surfaces import exception_surfaces

S = "SENTINEL"
URL = f"postgresql://u:{S}PW@127.0.0.1:1/db?token={S}Q#{S}F"
AUTH = PasswordAuthProfile(USERNAME=f"{S}U", PASSWORD=f"{S}P")


class NativeError(Exception):
    """A driver error carrying the sentinel in message, attribute and cause."""

    def __init__(self) -> None:
        super().__init__(f"could not connect to {URL} {S}MSG")
        self.dsn = f"{URL} {S}ATTR"
        self.__cause__ = ValueError(f"{S}CAUSE")
        self.__context__ = KeyError(f"{S}CONTEXT")


def _boom(*_args, **_kwargs):
    raise NativeError()


@pytest.fixture(autouse=True)
def _no_driver_check(monkeypatch):
    monkeypatch.setattr(backend_mod, "_import_ibis_backend", lambda spec, dialect: None)


def _builder_backends(monkeypatch):
    params = SettingsParameters.create(settings_class=PostgreSQLBackendProfile, HOST="h", DATABASE="d")
    settings = IbisBackend(params)
    settings._spec = dataclasses.replace(settings._spec, connection_builder=_boom)
    direct = IbisBackend(dialect="postgres", host="h")
    direct._spec = dataclasses.replace(direct._spec, connection_builder=_boom)
    return settings, direct


def _assert_clean(err: BaseException, capsys) -> None:
    assert isinstance(err, BackendConnectionError)
    assert S not in exception_surfaces(err)
    out = capsys.readouterr()
    assert S not in out.out + out.err


def test_driver_connect_failure_url_path(monkeypatch, capsys):
    monkeypatch.setattr(ibis, "connect", _boom)
    with pytest.raises(BackendConnectionError) as info:
        IbisBackend(URL).connect()
    _assert_clean(info.value, capsys)
    assert str(info.value) == "postgresql://127.0.0.1:1/db: connection failed (NativeError)"


def test_builder_failure_settings_and_dialect_paths(monkeypatch, capsys):
    settings, direct = _builder_backends(monkeypatch)
    with pytest.raises(BackendConnectionError) as info:
        settings.connect(auth_profile=AUTH)
    _assert_clean(info.value, capsys)
    assert str(info.value).startswith("postgres: connection failed")
    with pytest.raises(BackendConnectionError) as info:
        direct.connect(auth_profile=AUTH)
    _assert_clean(info.value, capsys)


def test_settings_kwargs_build_failure(monkeypatch, capsys):
    monkeypatch.setattr(backend_mod, "build_driver_kwargs", _boom)
    params = SettingsParameters.create(settings_class=PostgreSQLBackendProfile, HOST="h", DATABASE="d")
    with pytest.raises(BackendConnectionError) as info:
        IbisBackend(params).connect(auth_profile=AUTH)
    _assert_clean(info.value, capsys)


def test_adapter_failure_in_public_factory_and_connect(monkeypatch, capsys):
    from mountainash_data.core.settings.adapters import registry as adapters

    monkeypatch.setitem(adapters._AUTH_ADAPTERS, (connection_factory.P.POSTGRESQL, PasswordAuthProfile), _boom)
    profile = PostgreSQLBackendProfile(HOST="h", DATABASE="d")
    with pytest.raises(BackendConnectionError) as info:
        connection_factory.build_driver_kwargs(profile, AUTH)
    _assert_clean(info.value, capsys)
    monkeypatch.setattr(PostgreSQLBackendProfile, "to_url_parts", _boom)
    with pytest.raises(BackendConnectionError) as info:
        connection_factory.build_connection_string(profile, AUTH)
    _assert_clean(info.value, capsys)
    with pytest.raises(BackendConnectionError) as info:
        IbisBackend(URL).connect()
    _assert_clean(info.value, capsys)


def test_scheme_resolution_failure(monkeypatch, capsys):
    monkeypatch.setattr(backend_mod, "provider_for_scheme", lambda scheme: (_ for _ in ()).throw(KeyError(URL)))
    with pytest.raises(BackendConnectionError) as info:
        IbisBackend(URL).connect()
    _assert_clean(info.value, capsys)


def test_malformed_url_and_short_password(monkeypatch, capsys):
    monkeypatch.setattr(ibis, "connect", _boom)
    for url in (f"postgresql://u:{S}/x@h/db", "postgresql://u:a@h/db"):
        try:
            be = IbisBackend(url)
        except ValueError as exc:
            assert S not in exception_surfaces(exc)
            continue
        with pytest.raises(BackendConnectionError) as info:
            be.connect()
        _assert_clean(info.value, capsys)


def test_missing_driver_inside_ambient_handler(monkeypatch):
    monkeypatch.undo()  # use the real _import_ibis_backend
    real = importlib.import_module

    def fake(name, *a, **kw):
        if name == "ibis.backends.duckdb":
            raise ModuleNotFoundError(f"No module named '{S}'", name=S)
        return real(name, *a, **kw)

    monkeypatch.setattr(backend_mod.importlib, "import_module", fake)
    backend = IbisBackend(dialect="duckdb", database=":memory:")
    with pytest.raises(ImportError) as info:
        try:
            raise ValueError(f"{S}-ambient")
        except ValueError:
            backend.connect()
    assert "mountainash-data[duckdb]" in str(info.value)
    assert S not in exception_surfaces(info.value)


@pytest.mark.parametrize("bad", ["user:SENTINEL@host/db", f"{S}:x", "redshift://u:SENTINEL@h/db"])
def test_unknown_scheme_or_a1_input_not_echoed(bad):
    with pytest.raises(ValueError) as info:
        IbisBackend(bad)
    assert S not in exception_surfaces(info.value)


def test_unknown_dialect_echoes_plain_identifiers_only():
    with pytest.raises(KeyError) as info:
        IbisBackend("nosuchdialect")
    assert "nosuchdialect" in str(info.value)
    with pytest.raises(KeyError) as info:
        IbisBackend(f"{S}@host/db")
    assert S not in exception_surfaces(info.value)


def test_listing_print_uses_safe_detail(capsys):
    from mountainash_data.core._safe_errors import safe_detail

    assert safe_detail(NativeError()) == "NativeError"
