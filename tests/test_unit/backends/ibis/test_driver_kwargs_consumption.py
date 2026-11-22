"""Settings -> driver kwargs -> builder -> final connect (recorder).

Configuration-flow evidence: the final Ibis connect function is replaced by a
recorder, proving each emitted value is consumed by the builder.
"""
from __future__ import annotations

import sys
import types

import ibis
import pytest
from mountainash_auth_client import NoAuthProfile, PasswordAuthProfile

from mountainash_data.backends.ibis.dialects import _registry
from mountainash_data.core.factories.connection_factory import build_driver_kwargs
from mountainash_data.core.settings import (
    BigQueryBackendProfile,
    DuckDBBackendProfile,
    RedshiftBackendProfile,
    SQLiteBackendProfile,
)


@pytest.fixture
def url_recorder(monkeypatch):
    calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(ibis, "connect", lambda url, **kw: calls.append((url, kw)) or object())
    return calls


def test_bigquery_settings_reach_connect(monkeypatch):
    calls: list[dict] = []
    fake = types.ModuleType("ibis.backends.bigquery")
    fake.connect = lambda **kw: calls.append(kw) or object()
    monkeypatch.setitem(sys.modules, "ibis.backends.bigquery", fake)
    monkeypatch.setattr("ibis.backends.bigquery", fake, raising=False)

    profile = BigQueryBackendProfile(
        PROJECT_ID="my-project-1", DATASET_ID="ds", LOCATION="EU", APPLICATION_NAME="app",
        PARTITION_COLUMN="pc", AUTH_LOCAL_WEBSERVER=False, AUTH_EXTERNAL_DATA=True, AUTH_CACHE="reauth",
    )
    cfg = build_driver_kwargs(profile, NoAuthProfile())
    _registry._build_bigquery_connection(**cfg)
    assert calls == [{
        "project_id": "my-project-1", "dataset_id": "ds", "location": "EU",
        "application_name": "app", "partition_column": "pc",
        "auth_local_webserver": False, "auth_external_data": True, "auth_cache": "reauth",
    }]


def test_bigquery_credentials_info_keeps_project_id(monkeypatch):
    calls: list[dict] = []
    fake = types.ModuleType("ibis.backends.bigquery")
    fake.connect = lambda **kw: calls.append(kw) or object()
    monkeypatch.setitem(sys.modules, "ibis.backends.bigquery", fake)
    monkeypatch.setattr("ibis.backends.bigquery", fake, raising=False)
    sentinel_cred = object()
    sa = types.SimpleNamespace(
        Credentials=types.SimpleNamespace(from_service_account_info=lambda info: sentinel_cred)
    )
    oauth2 = types.ModuleType("google.oauth2")
    oauth2.service_account = sa
    monkeypatch.setitem(sys.modules, "google", types.ModuleType("google"))
    monkeypatch.setitem(sys.modules, "google.oauth2", oauth2)
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", sa)

    _registry._build_bigquery_connection(project_id="p-123456", dataset_id="d", credentials_info={"k": "v"})
    assert calls == [{"project_id": "p-123456", "dataset_id": "d", "credentials": sentinel_cred}]


def test_duckdb_threads_and_memory_limit_reach_connect(url_recorder):
    profile = DuckDBBackendProfile(DATABASE=":memory:", THREADS=2, MEMORY_LIMIT="1GB")
    cfg = build_driver_kwargs(profile, NoAuthProfile())
    assert cfg["threads"] == 2 and cfg["memory_limit"] == "1GB"
    _registry._build_duckdb_connection(**cfg)
    (_, kwargs), = url_recorder
    assert kwargs["threads"] == 2 and kwargs["memory_limit"] == "1GB"


def test_sqlite_type_map_reaches_connect(monkeypatch):
    calls: list[tuple] = []
    monkeypatch.setattr(ibis.sqlite, "connect", lambda *a, **kw: calls.append((a, kw)) or object())
    tm = {"MYTYPE": "int64"}
    _registry._build_sqlite_connection(database=":memory:", type_map=tm)
    assert calls == [((":memory:",), {"type_map": tm})]


def _redshift(**kw):
    return RedshiftBackendProfile(HOST="h", DATABASE="d", REGION="us-east-1", **kw)


def test_redshift_read_only_emits_libpq_options():
    cfg = build_driver_kwargs(_redshift(CLUSTER_READ_ONLY=True), PasswordAuthProfile(USERNAME="u", PASSWORD="p"))
    assert cfg["options"] == "-c default_transaction_read_only=on"
    assert "readonly" not in cfg
    assert not any(k.startswith(("iam", "aws")) for k in cfg)
    psycopg = pytest.importorskip("psycopg", reason="driver-level assertion needs psycopg")
    kw = {k: v for k, v in cfg.items() if k in ("host", "port", "user", "password", "options", "sslmode")}
    kw["dbname"] = cfg["database"]
    assert "default_transaction_read_only=on" in psycopg.conninfo.make_conninfo(**kw)


def test_redshift_not_read_only_emits_nothing():
    cfg = build_driver_kwargs(_redshift(CLUSTER_READ_ONLY=False), PasswordAuthProfile(USERNAME="u", PASSWORD="p"))
    assert "options" not in cfg and "readonly" not in cfg


def test_redshift_rejects_iam_auth():
    from mountainash_auth_client import IAMAuthProfile

    with pytest.raises(ValueError, match="does not support auth"):
        build_driver_kwargs(_redshift(), IAMAuthProfile(ROLE_ARN="arn:aws:iam::123456789012:role/x"))


def test_redshift_caller_options_override_generated_options():
    from mountainash_settings import SettingsParameters
    from mountainash_data.backends.ibis.backend import IbisBackend

    params = SettingsParameters.create(
        settings_class=RedshiftBackendProfile, HOST="h", DATABASE="d", REGION="us-east-1",
        CLUSTER_READ_ONLY=True,
    )
    backend = IbisBackend(params, options="-c statement_timeout=1")
    cfg = build_driver_kwargs(backend._profile, PasswordAuthProfile(USERNAME="u", PASSWORD="p"))
    cfg.update(backend._extra_config)
    assert cfg["options"] == "-c statement_timeout=1"
