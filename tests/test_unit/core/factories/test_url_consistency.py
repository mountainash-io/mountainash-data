"""URL applier coverage goldens."""

import pytest
from mountainash_auth_client import PasswordAuthProfile, TokenAuthProfile
from mountainash_data.core.settings import PostgreSQLBackendProfile, MotherDuckBackendProfile
from mountainash_data.core.factories.connection_factory import build_connection_string


def test_postgres_password_url():
    s = PostgreSQLBackendProfile(HOST="db", PORT=5432, DATABASE="app")
    assert build_connection_string(s, PasswordAuthProfile(USERNAME="u", PASSWORD="p@s")) == "postgresql://u:p%40s@db:5432/app"


def test_motherduck_token_url():
    assert build_connection_string(MotherDuckBackendProfile(DATABASE="mydb"), TokenAuthProfile(TOKEN="T")) == "md:mydb?motherduck_token=T"


def test_snowflake_token_url_not_implemented():
    # snowflake supports TokenAuthProfile for kwargs but has no URL form → fail-closed
    from mountainash_data.core.settings import SnowflakeBackendProfile
    with pytest.raises(NotImplementedError):
        build_connection_string(SnowflakeBackendProfile(ACCOUNT="a"), TokenAuthProfile(TOKEN="T"))
