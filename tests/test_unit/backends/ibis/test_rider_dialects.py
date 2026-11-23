"""Rider profiles (motherduck, redshift) resolve to their own DialectSpec."""
from __future__ import annotations

import ibis
import pytest
from mountainash_auth_client import PasswordAuthProfile, TokenAuthProfile
from mountainash_settings import SettingsParameters

from mountainash_data.backends.ibis.backend import IbisBackend
from mountainash_data.core.settings import MotherDuckBackendProfile, RedshiftBackendProfile


@pytest.fixture
def recorder(monkeypatch):
    calls: list[tuple[str, dict]] = []

    def fake_connect(url, **kwargs):
        calls.append((url, kwargs))
        return object()

    monkeypatch.setattr(ibis, "connect", fake_connect)
    return calls


def test_motherduck_settings_resolve_to_motherduck_and_token_reaches_driver(recorder):
    params = SettingsParameters.create(settings_class=MotherDuckBackendProfile, DATABASE="mydb")
    backend = IbisBackend(params)
    assert backend.dialect == "motherduck"
    backend.connect(auth_profile=TokenAuthProfile(TOKEN="tok"))
    (url, _), = recorder
    assert url == "duckdb://md:mydb?motherduck_token=tok"


def test_redshift_settings_resolve_to_redshift():
    params = SettingsParameters.create(
        settings_class=RedshiftBackendProfile, HOST="h", DATABASE="d", REGION="us-east-1"
    )
    assert IbisBackend(params).dialect == "redshift"
