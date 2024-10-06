import pytest
import ibis
import os
from unittest import mock
from mountainash_data import Postgres_IbisConnection, SQLite_IbisConnection, DuckDB_IbisConnection

@pytest.fixture
def mock_postgres_connection():
    with mock.patch('mountainash_data.Postgres_IbisConnection.connect_ibis') as mock_connect:
        mock_connect.return_value = mock.MagicMock()
        yield Postgres_IbisConnection(db_auth_settings_parameters=mock.MagicMock())

@pytest.fixture
def mock_sqlite_connection():
    with mock.patch('mountainash_data.SQLite_IbisConnection.connect_ibis') as mock_connect:
        mock_connect.return_value = mock.MagicMock()
        yield SQLite_IbisConnection(db_auth_settings_parameters=mock.MagicMock())

@pytest.fixture
def mock_duckdb_connection():
    with mock.patch('mountainash_data.DuckDB_IbisConnection.connect_ibis') as mock_connect:
        mock_connect.return_value = mock.MagicMock()
        yield DuckDB_IbisConnection(db_auth_settings_parameters=mock.MagicMock())

def test_postgres_connection(mock_postgres_connection):
    mock_postgres_connection.connect()
    assert mock_postgres_connection.ibis_backend is not None

def test_sqlite_connection(mock_sqlite_connection):
    mock_sqlite_connection.connect()
    assert mock_sqlite_connection.ibis_backend is not None

def test_duckdb_connection(mock_duckdb_connection):
    mock_duckdb_connection.connect()
    assert mock_duckdb_connection.ibis_backend is not None

@pytest.fixture(scope="module")
def docker_compose_file(pytestconfig):
    return os.path.join(str(pytestconfig.rootdir), "docker-compose.yml")

@pytest.fixture(scope="module")
def docker_services(docker_ip, docker_services):
    docker_services.start("postgres")
    docker_services.wait_for_service("postgres", 5432)
    yield
    docker_services.stop("postgres")

def test_postgres_docker_connection(docker_services):
    connection = Postgres_IbisConnection(db_auth_settings_parameters=mock.MagicMock())
    connection.connect()
    assert connection.ibis_backend is not None
