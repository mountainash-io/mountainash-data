from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE as P
from mountainash_data.core.settings import (
    MySQLBackendProfile, MSSQLBackendProfile, SnowflakeBackendProfile,
)


def test_mysql_ssl_compose_full_dict():
    out = MySQLBackendProfile(HOST="h", PORT=3306, SSL_CA="/ca.pem", SSL_CIPHER="HIGH").emit(P.MYSQL)
    # full equality: nested ssl ADDED, no flat ssl_* leaked, config unchanged
    assert out == {
        "host": "h", "port": 3306, "charset": "utf8mb4",
        "collation": "utf8mb4_unicode_ci", "autocommit": True,
        "ssl": {"ssl-ca": "/ca.pem", "ssl-cipher": "HIGH"},
    }


def test_mssql_host_fold_full_dict():
    out = MSSQLBackendProfile(HOST="srv", PORT=1433, INSTANCE_NAME="INST").emit(P.MSSQL)
    assert out["host"] == "srv\\INST" and "instance_name" not in out


def test_snowflake_session_parameters_added_only():
    out = SnowflakeBackendProfile(ACCOUNT="acct", QUERY_TAG="etl", TIMEZONE="UTC").emit(P.SNOWFLAKE)
    assert out["session_parameters"] == {"QUERY_TAG": "etl", "TIMEZONE": "UTC"}
    assert "query_tag" not in out and "timezone" not in out

