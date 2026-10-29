---
title: From settings to driver arguments
description: How profiles and credentials become driver arguments and connection URLs.
date: 2026-10-29
---

# From settings to driver arguments

When an `IbisBackend` is built from settings, `connect(auth_profile=...)` turns a `*BackendProfile` holding connection configuration and a `*AuthProfile` holding credentials into one set of driver keyword arguments. The profile's values usually come from mountainash-settings sources such as configuration files, environment variables and secret stores ([Configuring connections with mountainash-settings](../04-configuring-connections-with-mountainash-settings/index.md)); the credentials come from mountainash-auth-client and stay apart from configuration until connect time, so the same configuration can be used with different credentials and credentials never sit in a configuration file by accident. A typical user only writes the profile and the auth object, as shown in [Connection profiles and authentication](../03-connection-profiles-and-authentication/index.md). The composition step underneath is a small set of functions and a lookup table, which can also be called directly to see exactly what a driver will receive or to build a connection URL.

## ConnectionFactory

<!-- concept:107 -->
"ConnectionFactory" names the module `mountainash_data.core.factories.connection_factory`, not a class. It holds five functions, and `IbisBackend.connect()` calls a different one depending on how the backend was constructed (`src/mountainash_data/backends/ibis/backend.py`).

| Function | Input | Output | Used by |
|---|---|---|---|
| `build_driver_kwargs(profile, auth_profile=None)` | A backend profile and an optional auth profile | Driver keyword arguments | The settings path: `IbisBackend(SettingsParameters...)` |
| `build_connection_string(profile, auth_profile=None)` | The same | A URL string | Callers who need a URL for another tool |
| `apply_auth_adapter(provider_type, base, auth_profile)` | A provider type, a kwargs dict, an auth profile | The kwargs with credentials merged in | The dialect-keyword and URL paths, which have no profile |
| `provider_for_dialect(dialect)` | An Ibis dialect key, such as `"postgres"` | The settings provider type | The dialect-keyword path |
| `provider_for_scheme(scheme)` | A URL scheme, such as `"postgresql"` | The settings provider type | The URL path |

The two lookup functions bridge naming between layers. The dialect registry in [How one backend serves 20 dialects](../14-how-one-backend-serves-20-dialects/index.md) is keyed by Ibis dialect names, while auth adapters are keyed by the settings layer's `CONST_DB_PROVIDER_TYPE` values. Where several providers share one Ibis dialect (`postgresql` and `redshift` both use `postgres`; `duckdb` and `motherduck` both use `duckdb`), `provider_for_dialect` resolves deterministically to the provider whose name matches the dialect.

The project's `CLAUDE.md` once also described `OperationsFactory`, `SettingsFactory` and a `DatabaseUtils` facade; none of them exist in the source (profile record `core.factories`, `coverage.md` documentation-drift list).

## The auth adapter registry

<!-- concept:86 -->
Credential transformation is the step that turns an auth profile's fields into the argument names and types a particular driver accepts. Auth profiles come from [mountainash-auth-client](https://github.com/mountainash-io/mountainash-auth-client), which owns their fields and any flows that obtain tokens; this package only consumes them. Their fields use the auth client's names (`USERNAME`, `PASSWORD`, `TOKEN`, `ACCESS_TOKEN`) and hold secrets as pydantic `SecretStr`. Drivers want something else: psycopg wants `user` and `password`, Databricks wants `access_token`, Snowflake wants `authenticator="oauth"` plus `token`, and Trino wants an `auth=` object from its own `trino.auth` module.

An auth adapter is a function `(auth_profile, base_kwargs) -> kwargs`. It unwraps each secret with `get_secret_value()` at this point and no earlier, returns a new dict rather than mutating `base`, and adds only the keys its driver understands. The adapters live one module per backend family under `src/mountainash_data/core/settings/adapters/`, and a shared `sql.userpass` adapter covers the ten backends whose drivers take a flat `user`/`password` pair.

<!-- concept:148 -->
The registry is a dict keyed by `(provider_type, auth profile class)` in `src/mountainash_data/core/settings/adapters/registry.py`, and `auth_adapter(provider_type, auth_class)` looks an adapter up. The lookup walks the auth class's MRO, so a caller's subclass of a registered auth type resolves to its parent's adapter, and an unregistered pair returns `None`:

```python
from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE as P
from mountainash_data.core.settings import OAuth2AuthProfile, PasswordAuthProfile
from mountainash_data.core.settings.adapters.registry import auth_adapter


class VaultPasswordAuthProfile(PasswordAuthProfile):
    """A caller's own auth subclass, e.g. one populated from a vault."""


for provider, auth_cls in [(P.SNOWFLAKE, OAuth2AuthProfile),
                           (P.POSTGRESQL, PasswordAuthProfile),
                           (P.POSTGRESQL, VaultPasswordAuthProfile),
                           (P.POSTGRESQL, OAuth2AuthProfile)]:
    fn = auth_adapter(provider, auth_cls)
    print(f"{provider.name:<11}{auth_cls.__name__:<26}{fn and f'{fn.__module__}.{fn.__name__}'}")
```

```text
SNOWFLAKE  OAuth2AuthProfile         mountainash_data.core.settings.adapters.snowflake.oauth2
POSTGRESQL PasswordAuthProfile       mountainash_data.core.settings.adapters.sql.userpass
POSTGRESQL VaultPasswordAuthProfile  mountainash_data.core.settings.adapters.sql.userpass
POSTGRESQL OAuth2AuthProfile         None
```

If a class inherits from two unrelated registered auth types, the lookup raises `TypeError` naming both rather than picking one. Each backend's `supported_auth` tuple and this table must agree; `tests/test_unit/core/settings/test_supported_auth_consistency.py` asserts that every non-`NoAuthProfile` entry in every `supported_auth` has an adapter. Registering adapters for a new backend is covered in [Extending mountainash-data](../17-extending-mountainash-data/index.md).

## The adapter pipeline and build_driver_kwargs

<!-- concept:85 -->
Two kinds of adapter run in sequence. Configuration adapters reshape a profile's own fields and are attached to the profile class through `__adapters__`, a mechanism owned by [mountainash-settings](https://github.com/mountainash-io/mountainash-settings): `Profile.emit(target)` first renames each field to its `driver_key` (applying any `transform`, and skipping fields that are `None` or have no driver key), then calls the profile's adapter for that target. Three backends have one: `mssql.host_fold` folds `INSTANCE_NAME` into `host` and maps encryption options, `mysql.ssl_compose` assembles the SSL fields into one `ssl` dict, and `snowflake.session_params` groups `QUERY_TAG` and `TIMEZONE` into `session_parameters`. Auth adapters run after that, on the emitted dict.

```mermaid
flowchart LR
  P["BackendProfile<br/>(HOST, PORT, ...)"] --> E["profile.emit(provider_type)<br/>driver_key renames + transforms"]
  E --> C["config adapter<br/>(__adapters__, if any)"]
  C --> K["base kwargs"]
  A["AuthProfile<br/>(USERNAME, TOKEN, ...)"] --> V{"in supported_auth?"}
  V -->|no| X["raise ValueError"]
  V -->|yes| R["auth_adapter(provider_type, type(auth))"]
  K --> M["auth adapter merges credentials"]
  R --> M
  M --> D["driver kwargs → connection builder"]
```

<!-- concept:68 -->
`build_driver_kwargs(profile, auth_profile=None)` is that pipeline as one call. It substitutes `NoAuthProfile()` for `None`, checks the auth type against the profile's `supported_auth`, emits the profile for its own provider type, and returns the emitted dict unchanged for `NoAuthProfile` or passes it through the registered auth adapter otherwise. On the settings path, `IbisBackend.connect()` then layers any extra keyword arguments given to the `IbisBackend` constructor on top and hands the result to the dialect's connection builder.

```python
from mountainash_data.core.factories.connection_factory import build_driver_kwargs
from mountainash_data.core.settings import PasswordAuthProfile, PostgreSQLBackendProfile

profile = PostgreSQLBackendProfile(HOST="db.example", DATABASE="app")
print(build_driver_kwargs(profile))
print(build_driver_kwargs(profile, PasswordAuthProfile(USERNAME="app", PASSWORD="s3cret")))
```

```text
{'host': 'db.example', 'port': 5432, 'database': 'app', 'autocommit': True, 'keepalives': 1, 'sslmode': <PostgresSSLMode.PREFER: 'prefer'>}
{'host': 'db.example', 'port': 5432, 'database': 'app', 'autocommit': True, 'keepalives': 1, 'sslmode': <PostgresSSLMode.PREFER: 'prefer'>, 'user': 'app', 'password': 's3cret'}
```

The returned dict contains plain secrets. It is meant to be passed straight to a driver, not logged.

On the settings path, the connection builder that receives these kwargs is chosen by the profile's `BackendSpec.ibis_dialect`, while the auth adapter is chosen by its `provider_type`. For most backends the two agree. For the rides-on backends they do not: `RedshiftBackendProfile` resolves to the `postgres` dialect entry and `MotherDuckBackendProfile` to `duckdb`, so the MotherDuck token reaches the plain DuckDB builder, which opens an in-memory database and passes `token` to DuckDB as a configuration option:

```python
from mountainash_data import IbisBackend
from mountainash_data.core.settings import MotherDuckBackendProfile, TokenAuthProfile
from mountainash_settings import SettingsParameters

params = SettingsParameters.create(settings_class=MotherDuckBackendProfile, DATABASE="analytics")
backend = IbisBackend(params)
print(backend.dialect)
try:
    backend.connect(auth_profile=TokenAuthProfile(TOKEN="tok-123"))
except Exception as exc:
    print(f"{type(exc).__name__}: {exc}")
```

```text
duckdb
InvalidInputException: Invalid Input Error: The following options were not recognized: token
```

No test builds an `IbisBackend` from MotherDuck or Redshift settings. Until the settings path honours these backends, connect to MotherDuck with `IbisBackend(dialect="motherduck", database=..., token=...)` or a `duckdb://md:` URL, which use the MotherDuck builder.

## Adapter families

The registered adapters fall into four families by the shape of credential they produce. One run shows three of them, plus a configuration adapter; each is discussed below.

```python
from mountainash_data.core.factories.connection_factory import build_driver_kwargs
from mountainash_data.core.settings import (
    AzureADAuthProfile, IAMAuthProfile, MSSQLBackendProfile, MySQLBackendProfile,
    OAuth2AuthProfile, PasswordAuthProfile, RedshiftBackendProfile, SnowflakeBackendProfile,
)

snowflake = SnowflakeBackendProfile(ACCOUNT="acme-xy123", QUERY_TAG="nightly")
oauth = OAuth2AuthProfile(ACCESS_TOKEN="eyJ-access", CLIENT_ID="app", CLIENT_SECRET="cs", SCOPE="session:role:etl")
print(build_driver_kwargs(snowflake, oauth))

redshift = RedshiftBackendProfile(HOST="wg.example", DATABASE="dev", REGION="ap-southeast-2")
iam = IAMAuthProfile(ROLE_ARN="arn:aws:iam::123456789012:role/etl")
print(build_driver_kwargs(redshift, iam))

mssql = MSSQLBackendProfile(HOST="sql.example", DATABASE="app", INSTANCE_NAME="PROD")
print(build_driver_kwargs(mssql, AzureADAuthProfile(MANAGED_IDENTITY=True)))

mysql = MySQLBackendProfile(HOST="mysql.example", SSL_MODE="VERIFY_CA",
                            SSL_CA="/etc/ssl/ca.pem", SSL_CERT="/etc/ssl/client.pem")
print(build_driver_kwargs(mysql, PasswordAuthProfile(USERNAME="app", PASSWORD="pw")))
```

```text
{'account': 'acme-xy123', 'session_parameters': {'QUERY_TAG': 'nightly'}, 'authenticator': 'oauth', 'token': 'eyJ-access'}
{'host': 'wg.example', 'port': 5439, 'database': 'dev', 'sslmode': <RedshiftSSLMode.VERIFY_FULL: 'verify-full'>, 'readonly': False, 'iam': True, 'iam_role_arn': 'arn:aws:iam::123456789012:role/etl'}
{'host': 'sql.example\\PROD', 'port': 1433, 'database': 'app', 'driver': <MSSQLDriver.ODBC_18: 'ODBC Driver 18 for SQL Server'>, 'application_name': 'MountainAsh', 'encrypt': 'yes', 'authentication': 'ActiveDirectoryMsi'}
{'host': 'mysql.example', 'port': 3306, 'charset': 'utf8mb4', 'collation': 'utf8mb4_unicode_ci', 'autocommit': True, 'ssl_mode': 'VERIFY_CA', 'ssl': {'ssl-cert': '/etc/ssl/client.pem', 'ssl-ca': '/etc/ssl/ca.pem'}, 'user': 'app', 'password': 'pw'}
```

<!-- concept:87 -->
The OAuth family is Snowflake's. Both `TokenAuthProfile` and `OAuth2AuthProfile` map to `authenticator="oauth"` and `token=<access token>`, which the Snowflake connector accepts as a pre-obtained OAuth access token (`src/mountainash_data/core/settings/adapters/snowflake.py`). The `OAuth2AuthProfile` adapter reads only `ACCESS_TOKEN`: `CLIENT_ID`, `CLIENT_SECRET`, `SCOPE` and `REFRESH_TOKEN` are ignored, as the output above shows, so the package performs no token exchange or refresh. A source comment marks this as a known smell, and the profile record `core.settings.adapters` lists it as a risk. Obtaining or refreshing the token is the job of [mountainash-auth-client](https://github.com/mountainash-io/mountainash-auth-client). Databricks's token adapter is the simpler case of the same idea, mapping `TOKEN` to `access_token`.

<!-- concept:88 -->
The JWT family is Trino's. Trino's driver takes authentication as an object, so the adapters construct one from the driver's own module: `JWTAuthProfile` becomes `auth=trino.auth.JWTAuthentication(token)`, `PasswordAuthProfile` becomes `user` plus `auth=BasicAuthentication(user, password)`, and `KerberosAuthProfile` becomes `auth=KerberosAuthentication(service_name=..., principal=...)` (`src/mountainash_data/core/settings/adapters/trino.py`). Because these adapters import `trino.auth`, they need the `trino` extra installed. That is why `IbisBackend.connect()` imports the dialect's Ibis backend before it resolves auth: a missing driver surfaces as the `ImportError` naming the extra, not as a failure inside an adapter. The Kerberos adapter does not pass `KEYTAB`, and these adapters are not run in this book because the Trino driver is not part of the local install.

<!-- concept:89 -->
Cloud-native adapters map a cloud identity rather than a database password. `IAMAuthProfile` on Redshift sets `iam=True` and copies `ROLE_ARN`, access keys, session token and AWS profile name to `iam_role_arn`, `aws_access_key_id` and related keys. `AzureADAuthProfile` on SQL Server sets `authentication="ActiveDirectoryMsi"` for a managed identity, or `ActiveDirectoryServicePrincipal` with `user_id`, `password` and `tenant_id` for a service principal; `WindowsAuthProfile` sets `trusted_connection="yes"` and an optional `DOMAIN\user`. `ServiceAccountAuthProfile` on BigQuery parses `INFO` (a JSON string) or reads `FILE` into a `google.oauth2` credentials object under `credentials`; an `INFO` that is not valid JSON raises a `ValueError` whose message omits the document.

Two of these do not currently reach a working connection through `IbisBackend`, and no test connects either one. The Redshift dialect connects through the PostgreSQL builder and Ibis's psycopg backend, and psycopg rejects options it does not know: `psycopg.conninfo.make_conninfo(iam=True)` raises `ProgrammingError: invalid connection option "iam"`, and the same applies to the `readonly` key the Redshift profile emits by default (shown in the output above). The BigQuery adapter emits `credentials`, but the BigQuery connection builder reads only `credentials_info`, `project_id` and `dataset_id` (`src/mountainash_data/backends/ibis/dialects/_registry.py`), so the service-account credentials are dropped and the builder falls back to application default credentials.

<!-- concept:90 -->
The SSL bundle adapter is MySQL's configuration adapter, `mysql.ssl_compose`. The MySQL profile exposes `SSL_MODE`, `SSL_KEY`, `SSL_CERT`, `SSL_CA`, `SSL_CAPATH` and `SSL_CIPHER` as separate typed fields with no driver key of their own. The adapter emits `ssl_mode` as a string and bundles whichever file and cipher fields are set into a single `ssl` dict with the driver's hyphenated key names (`ssl-ca`, `ssl-cert`, and so on), omitting the dict entirely when none are set. It runs during `emit`, before any auth adapter, so it applies whatever credentials are supplied. SQL Server's `host_fold` does the equivalent for its TLS options, turning `ENCRYPTION`, `TRUST_SERVER_CERTIFICATE` and `MARS_ENABLED` into `encrypt`, `trust_server_certificate` and `mars_connection`.

## Supported-auth validation

<!-- concept:109 -->
Every `BackendSpec` declares a non-empty `supported_auth` tuple; an empty one is rejected with `ValueError` when the spec is defined. `build_driver_kwargs` and `build_connection_string` check the supplied auth against it with `isinstance`, after replacing `None` with `NoAuthProfile()`. A backend whose tuple omits `NoAuthProfile` (MySQL, Oracle, SQL Server, Snowflake, Redshift, Exasol and MotherDuck) therefore requires credentials. The dialect-keyword and URL paths have no profile and skip this gate; they go straight to `apply_auth_adapter`, which fails with a different message when no adapter is registered.

```python
from mountainash_data import IbisBackend
from mountainash_data.core.factories.connection_factory import build_driver_kwargs
from mountainash_data.core.settings import (
    MySQLBackendProfile, PasswordAuthProfile, SQLiteBackendProfile,
)

password = PasswordAuthProfile(USERNAME="u", PASSWORD="p")
attempts = [
    lambda: build_driver_kwargs(SQLiteBackendProfile(DATABASE=":memory:"), password),
    lambda: build_driver_kwargs(MySQLBackendProfile(HOST="mysql.example")),
    lambda: IbisBackend(dialect="sqlite").connect(auth_profile=password),
]
for attempt in attempts:
    try:
        attempt()
    except ValueError as exc:
        print(f"ValueError: {exc}")
```

```text
ValueError: sqlite does not support auth: PasswordAuthProfile
ValueError: mysql does not support auth: NoAuthProfile
ValueError: CONST_DB_PROVIDER_TYPE.SQLITE: no auth adapter for PasswordAuthProfile
```

Which auth types each backend accepts is tabulated in [Connection profiles and authentication](../03-connection-profiles-and-authentication/index.md).

## build_connection_string

<!-- concept:108 -->
`build_connection_string(profile, auth_profile=None)` produces a URL from the same two objects. The profile supplies a credential-free skeleton through `to_url_parts()` (scheme, host, port, database), and the auth is applied in one of three ways: `NoAuthProfile` adds nothing, `PasswordAuthProfile` adds `user:password@` with both parts percent-encoded, and a per-provider applier handles anything else. Only MotherDuck has such an applier (`TokenAuthProfile` becomes `?motherduck_token=`). Other combinations raise `NotImplementedError`, and backends without a host use an authority-less form.

```python
from mountainash_data.core.factories.connection_factory import build_connection_string
from mountainash_data.core.settings import (
    MotherDuckBackendProfile, PasswordAuthProfile, PostgreSQLBackendProfile,
    SnowflakeBackendProfile, SQLiteBackendProfile, TokenAuthProfile,
)

pg = PostgreSQLBackendProfile(HOST="db.example", DATABASE="app")
print(build_connection_string(pg))
print(build_connection_string(pg, PasswordAuthProfile(USERNAME="app@corp", PASSWORD="p/ss:w@rd")))
print(build_connection_string(SQLiteBackendProfile(DATABASE="/data/app.db")))
print(build_connection_string(MotherDuckBackendProfile(DATABASE="analytics"),
                              TokenAuthProfile(TOKEN="md tok")))
try:
    build_connection_string(SnowflakeBackendProfile(ACCOUNT="acme-xy123"),
                            TokenAuthProfile(TOKEN="t"))
except NotImplementedError as exc:
    print(f"NotImplementedError: {exc}")
```

```text
postgresql://db.example:5432/app
postgresql://app%40corp:p%2Fss%3Aw%40rd@db.example:5432/app
sqlite:/data/app.db
md:analytics?motherduck_token=md%20tok
NotImplementedError: snowflake: no URL form for TokenAuthProfile
```

The URL carries only the skeleton fields; driver options such as `sslmode` are not included. It is also not guaranteed to be accepted by `IbisBackend`'s URL form, which requires `scheme://` and a scheme known to the dialect registry. A password URL for PostgreSQL round-trips (the URL path decodes the credentials back into a `PasswordAuthProfile`; `tests/test_unit/backends/ibis/test_credential_urls.py`), but the authority-less SQLite and MotherDuck forms are read as dialect names, and a Redshift URL fails:

```text
redshift://etl:pw@wg.example:5439/dev
ValueError: Cannot detect ibis dialect from URL scheme: 'redshift'
```

That output comes from passing `build_connection_string(RedshiftBackendProfile(...), PasswordAuthProfile(...))` to `IbisBackend`. For these backends, construct the `IbisBackend` from settings instead.

## Credential redaction

<!-- concept:143 -->
Connection URLs can reach exception messages: Ibis echoes the whole connection string in errors such as "Don't know how to connect to ...", and drivers often include it too. Every `ibis.connect` call the package makes, on the URL path and in the URL-style connection builders, runs inside `connect_redacted` (`src/mountainash_data/core/_redact.py`). If the raised exception's text contains credentials, it is replaced: URL userinfo becomes `***@`, values of secret-looking query keys (`token`, `motherduck_token`, `access_token`, `password`, `pwd`, `secret`, `api_key` and similar) become `***`, and any explicitly supplied secret is removed in raw, percent-encoded and plus-encoded forms. The builders pass the values of `password`, `passwd`, `token` and `access_token` keyword arguments, and the MotherDuck token, as those explicit secrets. Builders that call a backend's own `connect()` directly (ClickHouse, Databricks, SingleStoreDB, Exasol, Impala, Materialize, RisingWave, Druid, BigQuery) are not wrapped, because they never build a URL. A builtin exception type is rebuilt as the same type, so `except ValueError` still works; a driver exception becomes `mountainash_data.core.errors.BackendConnectionError`. The replacement is raised without the original attached as its cause or context, so the credential-bearing text is not reachable through the traceback. An exception that carried no credentials propagates unchanged.

Redaction is paired with keeping credentials out of URLs in the first place. On the URL path, `IbisBackend` strips `user:password@` from the URL before Ibis sees it and supplies the decoded values as driver keyword arguments. A URL that cannot be parsed fails with a message that contains none of the URL:

```python
from mountainash_data import IbisBackend

try:
    IbisBackend("postgres://app:pa/ss@db.example/app")   # unencoded "/" in the password
except ValueError as exc:
    print(f"ValueError: {exc}")
```

```text
ValueError: Invalid connection URL (value redacted): percent-encode credentials with urllib.parse.quote(value, safe='') or pass an auth profile
```

The redaction is pattern-based. A secret inside a non-URL driver message, or under a query key outside the pattern list, is removed only if it was passed in as an explicit secret (profile record `core._redact`). The module is covered only indirectly, through the credential-URL tests (`coverage.md`).
