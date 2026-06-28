import pytest
from mountainash_auth_client import PasswordAuthProfile, NoAuthProfile
from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE as P
from mountainash_data.core.settings import (
    BackendProfile, PostgreSQLBackendProfile, MotherDuckBackendProfile,
)
from mountainash_data.core.settings.descriptor import BackendSpec


def test_supported_auth_present():
    assert PostgreSQLBackendProfile.__spec__.supported_auth == (PasswordAuthProfile, NoAuthProfile)


def test_flat_emit_is_config_only():
    out = PostgreSQLBackendProfile(HOST="db", PORT=5432, DATABASE="app").emit(P.POSTGRESQL)
    assert out["host"] == "db" and out["port"] == 5432 and out["database"] == "app"
    assert "user" not in out and "password" not in out


def test_to_url_parts_standard():
    parts = PostgreSQLBackendProfile(HOST="db", PORT=5432, DATABASE="app").to_url_parts()
    assert (parts.scheme, parts.host, parts.port, parts.database) == ("postgresql", "db", 5432, "app")


def test_motherduck_url_parts_authority_less():
    parts = MotherDuckBackendProfile(DATABASE="mydb").to_url_parts()
    assert parts.scheme == "md" and parts.host is None and parts.database == "mydb"


def test_empty_supported_auth_invariant():
    with pytest.raises(ValueError, match="supported_auth"):
        BackendSpec(name="x", provider_type=P.SQLITE, parameters=[], supported_auth=())
