from mountainash_auth_client import (
    PasswordAuthProfile, TokenAuthProfile, OAuth2AuthProfile,
    CertificateAuthProfile, WindowsAuthProfile, AzureADAuthProfile,
    IAMAuthProfile,
)
from mountainash_data.core.settings.adapters import (
    sql as _sql, snowflake as _snow, mssql as _mssql,
    redshift as _rs, databricks as _dbx,
)


def test_sql_userpass():
    assert _sql.userpass(PasswordAuthProfile(USERNAME="u", PASSWORD="p"), {"host": "h"}) == {
        "host": "h", "user": "u", "password": "p"}


def test_userpass_no_mutate():
    base = {"host": "h"}
    _sql.userpass(PasswordAuthProfile(USERNAME="u", PASSWORD="p"), base)
    assert base == {"host": "h"}


def test_snowflake_token_oauth():
    assert _snow.token(TokenAuthProfile(TOKEN="t"), {}) == {"authenticator": "oauth", "token": "t"}


def test_snowflake_oauth2_token_only():
    assert _snow.oauth2(OAuth2AuthProfile(ACCESS_TOKEN="t"), {}) == {"authenticator": "oauth", "token": "t"}


def test_snowflake_password():
    assert _snow.password(PasswordAuthProfile(USERNAME="u", PASSWORD="p"), {}) == {"user": "u", "password": "p"}


def test_snowflake_certificate():
    assert _snow.certificate(CertificateAuthProfile(PRIVATE_KEY="KEY", PASSPHRASE="ph"), {}) == {
        "private_key": "KEY", "private_key_file_pwd": "ph"}


def test_mssql_password():
    assert _mssql.password(PasswordAuthProfile(USERNAME="u", PASSWORD="p"), {}) == {"user": "u", "password": "p"}


def test_mssql_windows():
    assert _mssql.windows(WindowsAuthProfile(USERNAME="u", DOMAIN="D"), {}) == {
        "trusted_connection": "yes", "user": "D\\u"}


def test_mssql_azure_ad_sp():
    assert _mssql.azure_ad(AzureADAuthProfile(CLIENT_ID="cid", CLIENT_SECRET="sec", TENANT_ID="t"), {}) == {
        "authentication": "ActiveDirectoryServicePrincipal", "user_id": "cid",
        "password": "sec", "tenant_id": "t"}


def test_redshift_iam():
    assert _rs.iam(IAMAuthProfile(ROLE_ARN="arn", ACCESS_KEY_ID="ak"), {}) == {
        "iam": True, "iam_role_arn": "arn", "aws_access_key_id": "ak"}


def test_databricks_token():
    assert _dbx.token(TokenAuthProfile(TOKEN="tok"), {}) == {"access_token": "tok"}
