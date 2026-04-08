import typing as t # import t.Any, t.Dict, t.Optional

import ibis
from ibis.backends.sql import SQLBackend
from abc import abstractmethod

# from abc import abstractmethod
from mountainash_settings import SettingsParameters

from mountainash_data.core.connection import BaseDBConnection
from mountainash_data.core.constants import IBIS_DB_CONNECTION_MODE, CONST_DB_ABSTRACTION_LAYER, CONST_DB_PROVIDER_TYPE
# from mountainash_dataframes.utils.dataframe_utils import DataFrameUtils


class BaseIbisConnection(BaseDBConnection):

    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 ):

        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters,
                         )


    @property
    def db_abstraction_layer(self) -> CONST_DB_ABSTRACTION_LAYER:
        return CONST_DB_ABSTRACTION_LAYER.IBIS

    @property
    @abstractmethod
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        """Database provider identifier."""
        ...


    @property
    @abstractmethod
    def ibis_backend(self) -> t.Optional[SQLBackend|t.Any]:
        """Concrete Ibis backend connection object."""
        pass

    @property
    @abstractmethod
    def ibis_connection_mode(self) -> str:
        """Connect via a connection string, kwargs or both."""
        pass

    @property
    @abstractmethod
    def connection_string_scheme(self) -> str:
        """Template string for database connection."""
        pass

    # @property
    # @abstractmethod
    # def supports_upsert(self) -> str:
    #     """Whether upserts are supported"""
    #     pass




    ###########################
    # Core Functions


    def connect(self,
                *,
                connection_string: t.Optional[str] = None,
                connection_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
                **kwargs) -> SQLBackend:

        """Connect with explicitly provided connection parameters"""


        if self.ibis_backend is None:

            if connection_string is not None:
                if connection_kwargs is not None:
                    self._connect(connection_string=connection_string, connection_kwargs=connection_kwargs, **kwargs)
                else:
                    self._connect(connection_string=connection_string, **kwargs)

            else:
                if connection_kwargs is not None:
                    self._connect(connection_string = self.connection_string_scheme, connection_kwargs=connection_kwargs, **kwargs)
                else:
                    self.connect_default(**kwargs)

        if self.ibis_backend is None:
            raise Exception(f"Unable to establish connection to {self.db_backend_name}")


        return self.ibis_backend


    def connect_default(self, **kwargs) -> SQLBackend:
        """Connect using default configuration"""
        # Implementation for connecting with default parameters

        if self.ibis_backend is None:

            connection_string_template = self.get_connection_string_template(scheme=self.connection_string_scheme)
            connectionstring_params = self.get_connection_string_params()

            connection_string = self.format_connection_string(template=connection_string_template, params=connectionstring_params)
            connection_kwargs = self.get_connection_kwargs()

            if self.ibis_connection_mode == IBIS_DB_CONNECTION_MODE.CONNECTION_STRING:
                self._connect(connection_string=connection_string, **kwargs)

            elif self.ibis_connection_mode == IBIS_DB_CONNECTION_MODE.KWARGS:
                #combine connectionstring_params and connection_kwargs
                connection_kwargs = {**connectionstring_params, **connection_kwargs}
                self._connect(connection_string=self.connection_string_scheme, connection_kwargs=connection_kwargs, **kwargs)

            elif self.ibis_connection_mode == IBIS_DB_CONNECTION_MODE.HYBRID:
                self._connect(connection_string=connection_string, connection_kwargs=connection_kwargs, **kwargs)

        #TODO: Add check and logging
        if self.ibis_backend is None:
            raise Exception(f"Unable to establish default connection to {self.db_backend_name}")

        return self.ibis_backend



    def _connect(self,
                 connection_string: t.Optional[str] = None,
                 connection_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
                 **kwargs
                 ) -> SQLBackend:
        """
        Default Implementation to connect to the database using the provided connection string.
        By default this relies on the connection_string_scheme to determine the backend dynamically.
        Over-ride in subclasses if a different implementation is necessary, such as using the custom backend directly.
        """

        if connection_kwargs is None:
            connection_kwargs = {}

        #combine connection_kwargs and kwargs
        connection_kwargs = {**connection_kwargs, **kwargs}

        if connection_string is None:
            raise ValueError(f"{self.db_backend_name}: Connection string is required to establish connection")

        self._ibis_backend : t.Any = ibis.connect(connection_string, **connection_kwargs)

        if self.ibis_backend is None:
            raise Exception(f"Unable to establish connection to {self.db_backend_name}")


        return self.ibis_backend



    def close(self):
        """Close the connection to the database."""

        self.disconnect()

    def disconnect(self):
        """Close the connection to the database."""

        if self.ibis_backend is not None:
            self.ibis_backend.disconnect()
            self._ibis_backend  = None

    def is_connected(self) -> bool:
        """ Is the connection open?"""
        if self.ibis_backend is None:
            return False
        else:
            return True


# ===========================================================================
# Concrete per-dialect connection classes
# These preserve the per-backend concrete subclasses needed by the legacy
# ConnectionFactory and existing tests. Each implements the abstract properties
# that BaseIbisConnection requires from BaseDBConnection.
# ===========================================================================

from pydantic_settings import BaseSettings as _BaseSettings
from mountainash_data.core.constants import CONST_DB_BACKEND as _CONST_DB_BACKEND


class SQLite_IbisConnection(BaseIbisConnection):
    """SQLite ibis connection — concrete subclass for factory compatibility."""

    def __init__(self, db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None):
        self._ibis_backend: t.Optional[t.Any] = None
        self._ibis_connection_mode: str = (
            connection_mode if connection_mode is not None
            else IBIS_DB_CONNECTION_MODE.CONNECTION_STRING
        )
        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)

    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        return CONST_DB_PROVIDER_TYPE.SQLITE

    @property
    def ibis_backend(self) -> t.Optional[t.Any]:
        return self._ibis_backend

    @property
    def ibis_connection_mode(self) -> str:
        return self._ibis_connection_mode

    @property
    def db_backend_name(self) -> str:
        return _CONST_DB_BACKEND.SQLITE

    @property
    def connection_string_scheme(self) -> str:
        return "sqlite://"

    @property
    def settings_class(self) -> t.Type[_BaseSettings]:
        from mountainash_data.core.settings import SQLiteAuthSettings
        return SQLiteAuthSettings


class DuckDB_IbisConnection(BaseIbisConnection):
    """DuckDB ibis connection — concrete subclass for factory compatibility."""

    def __init__(self, db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None,
                 read_only: t.Optional[bool] = None):
        self._ibis_backend: t.Optional[t.Any] = None
        self._ibis_connection_mode: str = (
            connection_mode if connection_mode is not None
            else IBIS_DB_CONNECTION_MODE.CONNECTION_STRING
        )
        self._read_only = read_only
        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)

    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        return CONST_DB_PROVIDER_TYPE.DUCKDB

    @property
    def ibis_backend(self) -> t.Optional[t.Any]:
        return self._ibis_backend

    @property
    def ibis_connection_mode(self) -> str:
        return self._ibis_connection_mode

    @property
    def db_backend_name(self) -> str:
        return _CONST_DB_BACKEND.DUCKDB

    @property
    def connection_string_scheme(self) -> str:
        return "duckdb://"

    @property
    def settings_class(self) -> t.Type[_BaseSettings]:
        from mountainash_data.core.settings import DuckDBAuthSettings
        return DuckDBAuthSettings

    def _connect(self, connection_string: t.Optional[str] = None,
                 connection_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
                 **kwargs) -> t.Any:
        import contextlib
        import warnings
        from mountainash_data.core.settings import DuckDBAuthSettings

        if connection_kwargs is None:
            connection_kwargs = {}
        if kwargs is None:
            kwargs = {}

        settings: DuckDBAuthSettings = DuckDBAuthSettings.get_settings(self.db_auth_settings_parameters)

        if settings.DATABASE is None:
            kwargs["read_only"] = False
        else:
            if connection_kwargs.get("read_only") is not None and self._read_only is not None:
                kwargs["read_only"] = connection_kwargs["read_only"] or self._read_only
            elif self._read_only is not None:
                kwargs["read_only"] = self._read_only
            elif connection_kwargs.get("read_only") is None:
                kwargs["read_only"] = False

        connection_kwargs = {**connection_kwargs, **kwargs}
        return super()._connect(connection_string, connection_kwargs)

    def disconnect(self):
        """Close the DuckDB connection, ensuring the underlying connection is properly closed."""
        if self.ibis_backend is not None:
            try:
                if hasattr(self.ibis_backend, 'con'):
                    try:
                        if hasattr(self.ibis_backend.con, '_cursors'):
                            for cursor in list(self.ibis_backend.con._cursors):
                                try:
                                    cursor.close()
                                except Exception:
                                    pass
                        self.ibis_backend.con.close()
                    except Exception as e:
                        print(f"Warning: Error closing DuckDB connection: {str(e)}")
            except Exception as e:
                print(f"Warning: Error during DuckDB disconnect: {str(e)}")
            finally:
                try:
                    super().disconnect()
                except Exception as e:
                    print(f"Warning: Error during Ibis backend disconnect: {str(e)}")
                    self._ibis_backend = None


class MotherDuck_IbisConnection(BaseIbisConnection):
    """MotherDuck ibis connection — concrete subclass for factory compatibility."""

    def __init__(self, db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None):
        self._ibis_backend: t.Optional[t.Any] = None
        self._ibis_connection_mode: str = (
            connection_mode if connection_mode is not None
            else IBIS_DB_CONNECTION_MODE.CONNECTION_STRING
        )
        self.supports_upsert = True
        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)

    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        return CONST_DB_PROVIDER_TYPE.MOTHERDUCK

    @property
    def ibis_backend(self) -> t.Optional[t.Any]:
        return self._ibis_backend

    @property
    def ibis_connection_mode(self) -> str:
        return self._ibis_connection_mode

    @property
    def db_backend_name(self) -> str:
        return _CONST_DB_BACKEND.MOTHERDUCK

    @property
    def connection_string_scheme(self) -> str:
        return "duckdb://md:"

    @property
    def settings_class(self) -> t.Type[_BaseSettings]:
        from mountainash_data.core.settings import MotherDuckAuthSettings
        return MotherDuckAuthSettings


class Postgres_IbisConnection(BaseIbisConnection):
    """PostgreSQL ibis connection — concrete subclass for factory compatibility."""

    def __init__(self, db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None):
        self._ibis_backend: t.Optional[t.Any] = None
        self._ibis_connection_mode: str = (
            connection_mode if connection_mode is not None
            else IBIS_DB_CONNECTION_MODE.CONNECTION_STRING
        )
        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)

    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        return CONST_DB_PROVIDER_TYPE.POSTGRESQL

    @property
    def ibis_backend(self) -> t.Optional[t.Any]:
        return self._ibis_backend

    @property
    def ibis_connection_mode(self) -> str:
        return self._ibis_connection_mode

    @property
    def db_backend_name(self) -> str:
        return _CONST_DB_BACKEND.POSTGRES

    @property
    def connection_string_scheme(self) -> str:
        return "postgres://"

    @property
    def settings_class(self) -> t.Type[_BaseSettings]:
        from mountainash_data.core.settings import PostgreSQLAuthSettings
        return PostgreSQLAuthSettings


class MySQL_IbisConnection(BaseIbisConnection):
    """MySQL ibis connection."""

    def __init__(self, db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None):
        self._ibis_backend: t.Optional[t.Any] = None
        self._ibis_connection_mode: str = (
            connection_mode if connection_mode is not None
            else IBIS_DB_CONNECTION_MODE.CONNECTION_STRING
        )
        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)

    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        return CONST_DB_PROVIDER_TYPE.MYSQL

    @property
    def ibis_backend(self) -> t.Optional[t.Any]:
        return self._ibis_backend

    @property
    def ibis_connection_mode(self) -> str:
        return self._ibis_connection_mode

    @property
    def db_backend_name(self) -> str:
        return _CONST_DB_BACKEND.MYSQL

    @property
    def connection_string_scheme(self) -> str:
        return "mysql://"

    @property
    def settings_class(self) -> t.Type[_BaseSettings]:
        from mountainash_data.core.settings import MySQLAuthSettings
        return MySQLAuthSettings


class MSSQL_IbisConnection(BaseIbisConnection):
    """MSSQL ibis connection."""

    def __init__(self, db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None):
        self._ibis_backend: t.Optional[t.Any] = None
        self._ibis_connection_mode: str = (
            connection_mode if connection_mode is not None
            else IBIS_DB_CONNECTION_MODE.CONNECTION_STRING
        )
        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)

    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        return CONST_DB_PROVIDER_TYPE.MSSQL

    @property
    def ibis_backend(self) -> t.Optional[t.Any]:
        return self._ibis_backend

    @property
    def ibis_connection_mode(self) -> str:
        return self._ibis_connection_mode

    @property
    def db_backend_name(self) -> str:
        return _CONST_DB_BACKEND.MSSQL

    @property
    def connection_string_scheme(self) -> str:
        return "mssql://"

    @property
    def settings_class(self) -> t.Type[_BaseSettings]:
        from mountainash_data.core.settings import MSSQLAuthSettings
        return MSSQLAuthSettings


class Oracle_IbisConnection(BaseIbisConnection):
    """Oracle ibis connection."""

    def __init__(self, db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None):
        self._ibis_backend: t.Optional[t.Any] = None
        self._ibis_connection_mode: str = (
            connection_mode if connection_mode is not None
            else IBIS_DB_CONNECTION_MODE.CONNECTION_STRING
        )
        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)

    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        return CONST_DB_PROVIDER_TYPE.ORACLE

    @property
    def ibis_backend(self) -> t.Optional[t.Any]:
        return self._ibis_backend

    @property
    def ibis_connection_mode(self) -> str:
        return self._ibis_connection_mode

    @property
    def db_backend_name(self) -> str:
        return _CONST_DB_BACKEND.ORACLE

    @property
    def connection_string_scheme(self) -> str:
        return "oracle://"

    @property
    def settings_class(self) -> t.Type[_BaseSettings]:
        return None  # Oracle settings not yet implemented


class Snowflake_IbisConnection(BaseIbisConnection):
    """Snowflake ibis connection."""

    def __init__(self, db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None):
        self._ibis_backend: t.Optional[t.Any] = None
        self._ibis_connection_mode: str = (
            connection_mode if connection_mode is not None
            else IBIS_DB_CONNECTION_MODE.HYBRID
        )
        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)

    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        return CONST_DB_PROVIDER_TYPE.SNOWFLAKE

    @property
    def ibis_backend(self) -> t.Optional[t.Any]:
        return self._ibis_backend

    @property
    def ibis_connection_mode(self) -> str:
        return self._ibis_connection_mode

    @property
    def db_backend_name(self) -> str:
        return _CONST_DB_BACKEND.SNOWFLAKE

    @property
    def connection_string_scheme(self) -> str:
        return "snowflake://"

    @property
    def settings_class(self) -> t.Type[_BaseSettings]:
        from mountainash_data.core.settings import SnowflakeAuthSettings
        return SnowflakeAuthSettings


class BigQuery_IbisConnection(BaseIbisConnection):
    """BigQuery ibis connection."""

    def __init__(self, db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None):
        self._ibis_backend: t.Optional[t.Any] = None
        self._ibis_connection_mode: str = (
            connection_mode if connection_mode is not None
            else IBIS_DB_CONNECTION_MODE.KWARGS
        )
        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)

    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        return CONST_DB_PROVIDER_TYPE.BIGQUERY

    @property
    def ibis_backend(self) -> t.Optional[t.Any]:
        return self._ibis_backend

    @property
    def ibis_connection_mode(self) -> str:
        return self._ibis_connection_mode

    @property
    def db_backend_name(self) -> str:
        return _CONST_DB_BACKEND.BIGQUERY

    @property
    def connection_string_scheme(self) -> str:
        return "bigquery://"

    @property
    def settings_class(self) -> t.Type[_BaseSettings]:
        from mountainash_data.core.settings import BigQueryAuthSettings
        return BigQueryAuthSettings

    def _connect(self, connection_string: t.Optional[str] = None,
                 connection_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
                 **kwargs) -> t.Any:
        import ibis.backends.bigquery as ir_backend

        credentials_info = connection_kwargs.get('credentials_info', None) if connection_kwargs else None
        dataset_id = connection_kwargs.get('dataset_id', "") if connection_kwargs else ""
        project_id = connection_kwargs.get('project_id', None) if connection_kwargs else None

        if credentials_info:
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            self._ibis_backend = ir_backend.connect(dataset_id=dataset_id, credentials=credentials)
        else:
            self._ibis_backend = ir_backend.connect(project_id=project_id, dataset_id=dataset_id)

        return self.ibis_backend


class Redshift_IbisConnection(BaseIbisConnection):
    """Redshift ibis connection (uses postgres protocol)."""

    def __init__(self, db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None):
        self._ibis_backend: t.Optional[t.Any] = None
        self._ibis_connection_mode: str = (
            connection_mode if connection_mode is not None
            else IBIS_DB_CONNECTION_MODE.CONNECTION_STRING
        )
        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)

    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        return CONST_DB_PROVIDER_TYPE.REDSHIFT

    @property
    def ibis_backend(self) -> t.Optional[t.Any]:
        return self._ibis_backend

    @property
    def ibis_connection_mode(self) -> str:
        return self._ibis_connection_mode

    @property
    def db_backend_name(self) -> str:
        return _CONST_DB_BACKEND.REDSHIFT

    @property
    def connection_string_scheme(self) -> str:
        return "postgres://"

    @property
    def settings_class(self) -> t.Type[_BaseSettings]:
        from mountainash_data.core.settings import RedshiftAuthSettings
        return RedshiftAuthSettings


class Trino_IbisConnection(BaseIbisConnection):
    """Trino ibis connection."""

    def __init__(self, db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None):
        self._ibis_backend: t.Optional[t.Any] = None
        self._ibis_connection_mode: str = (
            connection_mode if connection_mode is not None
            else IBIS_DB_CONNECTION_MODE.HYBRID
        )
        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)

    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        return CONST_DB_PROVIDER_TYPE.TRINO

    @property
    def ibis_backend(self) -> t.Optional[t.Any]:
        return self._ibis_backend

    @property
    def ibis_connection_mode(self) -> str:
        return self._ibis_connection_mode

    @property
    def db_backend_name(self) -> str:
        return _CONST_DB_BACKEND.TRINO

    @property
    def connection_string_scheme(self) -> str:
        return "trino://"

    @property
    def settings_class(self) -> t.Type[_BaseSettings]:
        from mountainash_data.core.settings import TrinoAuthSettings
        return TrinoAuthSettings


class PySpark_IbisConnection(BaseIbisConnection):
    """PySpark ibis connection."""

    def __init__(self, db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None):
        self._ibis_backend: t.Optional[t.Any] = None
        self._ibis_connection_mode: str = (
            connection_mode if connection_mode is not None
            else IBIS_DB_CONNECTION_MODE.CONNECTION_STRING
        )
        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)

    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        return CONST_DB_PROVIDER_TYPE.PYSPARK

    @property
    def ibis_backend(self) -> t.Optional[t.Any]:
        return self._ibis_backend

    @property
    def ibis_connection_mode(self) -> str:
        return self._ibis_connection_mode

    @property
    def db_backend_name(self) -> str:
        return _CONST_DB_BACKEND.PYSPARK

    @property
    def connection_string_scheme(self) -> str:
        return "pyspark://"

    @property
    def settings_class(self) -> t.Type[_BaseSettings]:
        from mountainash_data.core.settings import PySparkAuthSettings
        return PySparkAuthSettings
