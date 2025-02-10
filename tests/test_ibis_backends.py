import pytest
import ibis
import os
from unittest import mock
# from mountainash_data.databases.connections.ibis import Postgres_IbisConnection, SQLite_IbisConnection, DuckDB_IbisConnection, MSSQL_IbisConnection, MySQL_IbisConnection, Snowflake_IbisConnection
from mountainash_data.databases.ibis import SQLite_IbisConnection, DuckDB_IbisConnection
from mountainash_settings import SettingsParameters
from mountainash_auth_settings.auth_settings import AuthSettings

@pytest.fixture
def mock_settings_parameters_1():
    return SettingsParameters.create(settings_class = AuthSettings, namespace="mock", )

@pytest.fixture
def mock_settings_parameters_2():
    kwargs = {"USERNAME": "ngods", "PASSWORD": "ngods", "HOST": "host", "PORT": "5432"}
    return SettingsParameters.create(settings_class = AuthSettings, namespace="mock_pg", kwargs=kwargs)

@pytest.fixture
def mock_settings_parameters_3():
    kwargs = {"USERNAME": "ngods", "PASSWORD": "ngods", "HOST": "host", "PORT": "5432", "DATABASE_NAME": "database_name"}
    return SettingsParameters.create(settings_class = AuthSettings, namespace="mock_mssql", kwargs=kwargs)

################
# Connections

# @pytest.fixture
# def mock_sqlite_connection(mock_settings_parameters_1):
#     with mock.patch('mountainash_data.databases.connections.ibis.SQLite_IbisConnection.connect') as mock_connect:
#         mock_connect.return_value = mock.MagicMock()
#         yield SQLite_IbisConnection(db_auth_settings_parameters=mock_settings_parameters_1)

# @pytest.fixture
# def mock_duckdb_connection(mock_settings_parameters_1):
#     with mock.patch('mountainash_data.databases.connections.ibis.DuckDB_IbisConnection.connect') as mock_connect:
#         mock_connect.return_value = mock.MagicMock()
#         yield DuckDB_IbisConnection(db_auth_settings_parameters=mock_settings_parameters_1)

@pytest.fixture
def sqlite_connection(mock_settings_parameters_1):
    return SQLite_IbisConnection(db_auth_settings_parameters=mock_settings_parameters_1)

@pytest.fixture
def duckdb_connection(mock_settings_parameters_1):
    return DuckDB_IbisConnection(db_auth_settings_parameters=mock_settings_parameters_1)



# @pytest.fixture
# def mock_postgres_connection(mock_settings_parameters_2):
#     with mock.patch('mountainash_data.databases.connections.ibis.Postgres_IbisConnection.connect') as mock_connect:
#         mock_connect.return_value = mock.MagicMock()
#         yield Postgres_IbisConnection(db_auth_settings_parameters=mock_settings_parameters_2)

# @pytest.fixture
# def mock_mssql_connection(mock_settings_parameters_3):
#     with mock.patch('mountainash_data.databases.connections.ibis.MSSQL_IbisConnection.connect') as mock_connect:
#         mock_connect.return_value = mock.MagicMock()
#         yield MSSQL_IbisConnection(db_auth_settings_parameters=mock_settings_parameters_3)

# @pytest.fixture
# def mock_snowflake_connection(mock_settings_parameters_3):
#     with mock.patch('mountainash_data.databases.connections.ibis.Snowflake_IbisConnection.connect') as mock_connect:
#         mock_connect.return_value = mock.MagicMock()
#         yield Snowflake_IbisConnection(db_auth_settings_parameters=mock_settings_parameters_3)


# @pytest.fixture
# def mock_mysql_connection(mock_settings_parameters_3):
#     with mock.patch('mountainash_data.databases.connections.ibis.MySQL_IbisConnection.connect') as mock_connect:
#         mock_connect.return_value = mock.MagicMock()
#         yield MySQL_IbisConnection(db_auth_settings_parameters=mock_settings_parameters_3)




#################
# # Tests

# def test_sqlite_connection(mock_sqlite_connection):
#     mock_sqlite_connection.connect()
#     assert mock_sqlite_connection.ibis_backend is not None

# def test_duckdb_connection(mock_duckdb_connection):
#     mock_duckdb_connection.connect()
#     assert mock_duckdb_connection.ibis_backend is not None

def test_sqlite_connection(sqlite_connection):
    sqlite_connection.connect()
    assert sqlite_connection.ibis_backend is not None

def test_duckdb_connection(duckdb_connection):
    duckdb_connection.connect()
    assert duckdb_connection.ibis_backend is not None


# def test_postgres_connection(mock_postgres_connection):
#     mock_postgres_connection.connect()
#     assert mock_postgres_connection.ibis_backend is not None

# def test_mysql_connection(mock_mysql_connection):
#     mock_mysql_connection.connect()
#     assert mock_mysql_connection.ibis_backend is not None

# def test_mssql_connection(mock_mssql_connection):
#     mock_mssql_connection.connect()
#     assert mock_mssql_connection.ibis_backend is not None


# def test_snowflake_connection(mock_snowflake_connection):
#     mock_snowflake_connection.connect()
#     assert mock_snowflake_connection.ibis_backend is not None




# @pytest.fixture(scope="module")
# def docker_compose_file(pytestconfig):
#     return os.path.join(str(pytestconfig.rootdir), "docker-compose.yml")

# @pytest.fixture(scope="module")
# def docker_services(docker_ip, docker_services):
#     docker_services.start("postgres")
#     docker_services.wait_for_service("postgres", 5432)
#     yield
#     docker_services.stop("postgres")

# def test_postgres_docker_connection(docker_services):
#     connection = Postgres_IbisConnection(db_auth_settings_parameters=mock.MagicMock())
#     connection.connect()
#     assert connection.ibis_backend is not None
