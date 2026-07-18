"""Data-owned auth dispatch: (provider_type, auth_class) -> adapter fn."""
from __future__ import annotations
import typing as t

from mountainash_auth_client import (
    PasswordAuthProfile, JWTAuthProfile, KerberosAuthProfile,
    ServiceAccountAuthProfile, IAMAuthProfile, TokenAuthProfile,
    OAuth2AuthProfile, CertificateAuthProfile, WindowsAuthProfile, AzureADAuthProfile,
)
from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE as P
from . import (sql as _sql, trino as _trino, snowflake as _snow, bigquery as _bq,
               databricks as _dbx, mssql as _mssql, redshift as _rs,
               motherduck as _md)

_AUTH_ADAPTERS: dict[tuple[t.Any, type], t.Callable[[t.Any, dict], dict]] = {
    (P.MOTHERDUCK,     TokenAuthProfile):          _md.token,
    (P.TRINO,          PasswordAuthProfile):       _trino.password,
    (P.TRINO,          JWTAuthProfile):            _trino.jwt,
    (P.TRINO,          KerberosAuthProfile):       _trino.kerberos,
    (P.SNOWFLAKE,      PasswordAuthProfile):       _snow.password,
    (P.SNOWFLAKE,      TokenAuthProfile):          _snow.token,
    (P.SNOWFLAKE,      OAuth2AuthProfile):         _snow.oauth2,
    (P.SNOWFLAKE,      CertificateAuthProfile):    _snow.certificate,
    (P.BIGQUERY,       ServiceAccountAuthProfile): _bq.service_account,
    (P.DATABRICKS,     TokenAuthProfile):          _dbx.token,
    (P.DATABRICKS,     PasswordAuthProfile):       _dbx.password,
    (P.MSSQL,          PasswordAuthProfile):       _mssql.password,
    (P.MSSQL,          WindowsAuthProfile):        _mssql.windows,
    (P.MSSQL,          AzureADAuthProfile):        _mssql.azure_ad,
    (P.REDSHIFT,       PasswordAuthProfile):       _rs.password,
    (P.REDSHIFT,       IAMAuthProfile):            _rs.iam,
}
for _p in (P.POSTGRESQL, P.MYSQL, P.CLICKHOUSE, P.MATERIALIZE, P.RISINGWAVE,
           P.DRUID, P.SINGLESTOREDB, P.IMPALA, P.EXASOL):
    _AUTH_ADAPTERS[(_p, PasswordAuthProfile)] = _sql.userpass


def auth_adapter(provider_type: t.Any, auth_class: type[t.Any]) -> t.Callable[[t.Any, dict], dict] | None:
    matches = [
        k for k in auth_class.__mro__
        if k is not object and (provider_type, k) in _AUTH_ADAPTERS
    ]
    if not matches:
        return None
    winner = matches[0]
    ambiguous = [k for k in matches[1:] if not issubclass(winner, k)]
    if ambiguous:
        raise TypeError(
            f"ambiguous auth adapter for {auth_class.__name__} on {provider_type}: "
            f"{winner.__name__} vs {[k.__name__ for k in ambiguous]} "
            f"(multiply-inherits unrelated registered auth types)"
        )
    return _AUTH_ADAPTERS[(provider_type, winner)]
