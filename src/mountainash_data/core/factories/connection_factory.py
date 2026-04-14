"""
Connection Factory for settings-driven database connection creation.

Uses SettingsParameters to auto-detect backend and create appropriate connection.
"""

import logging
from typing import Type

from mountainash_settings import SettingsParameters

from ..connection import BaseDBConnection
from ..constants import CONST_DB_PROVIDER_TYPE
from .base_strategy_factory import BaseStrategyFactory
from .settings_type_factory_mixin import SettingsTypeFactoryMixin

logger = logging.getLogger(__name__)


class ConnectionFactory(
    SettingsTypeFactoryMixin,
    BaseStrategyFactory[SettingsParameters, Type[BaseDBConnection]],
):
    """
    Settings-driven factory for database connections.

    Detects backend type from SettingsParameters.settings_class and returns
    appropriate connection class with lazy loading.

    Example:
        settings_params = SettingsParameters.create(
            settings_class=PostgreSQLAuthSettings,
            config_files=["postgres.env"]
        )

        factory = ConnectionFactory()
        connection_class = factory.get_strategy(settings_params)
        connection = connection_class(db_auth_settings_parameters=settings_params)
        backend = connection.connect()
    """

    # Each subclass needs its own dictionaries to avoid sharing with other factories
    _strategy_cache = {}
    _strategy_modules = {}
    _strategy_classes = {}

    @classmethod
    def _configure_strategy_mapping(cls) -> None:
        """
        Configure strategy mappings using ONLY strings (no imports).

        Maps backend types to connection module paths and class names.
        """
        # Ibis-based connections — point directly at backends.ibis.connection
        # (bypasses the databases.connections.ibis shim chain from Phase 4)
        cls._strategy_modules = {
            CONST_DB_PROVIDER_TYPE.POSTGRESQL: "mountainash_data.backends.ibis.connection",
            CONST_DB_PROVIDER_TYPE.SQLITE: "mountainash_data.backends.ibis.connection",
            CONST_DB_PROVIDER_TYPE.DUCKDB: "mountainash_data.backends.ibis.connection",
            CONST_DB_PROVIDER_TYPE.MOTHERDUCK: "mountainash_data.backends.ibis.connection",
            CONST_DB_PROVIDER_TYPE.SNOWFLAKE: "mountainash_data.backends.ibis.connection",
            CONST_DB_PROVIDER_TYPE.BIGQUERY: "mountainash_data.backends.ibis.connection",
            CONST_DB_PROVIDER_TYPE.MSSQL: "mountainash_data.backends.ibis.connection",
            CONST_DB_PROVIDER_TYPE.MYSQL: "mountainash_data.backends.ibis.connection",
            CONST_DB_PROVIDER_TYPE.TRINO: "mountainash_data.backends.ibis.connection",
            CONST_DB_PROVIDER_TYPE.PYSPARK: "mountainash_data.backends.ibis.connection",
            CONST_DB_PROVIDER_TYPE.REDSHIFT: "mountainash_data.backends.ibis.connection",
            CONST_DB_PROVIDER_TYPE.ORACLE: "mountainash_data.backends.ibis.connection",
            # Iceberg connections — point directly at backends.iceberg
            CONST_DB_PROVIDER_TYPE.PYICEBERG_REST: "mountainash_data.backends.iceberg.catalogs.rest",
        }

        cls._strategy_classes = {
            CONST_DB_PROVIDER_TYPE.POSTGRESQL: "Postgres_IbisConnection",
            CONST_DB_PROVIDER_TYPE.SQLITE: "SQLite_IbisConnection",
            CONST_DB_PROVIDER_TYPE.DUCKDB: "DuckDB_IbisConnection",
            CONST_DB_PROVIDER_TYPE.MOTHERDUCK: "MotherDuck_IbisConnection",
            CONST_DB_PROVIDER_TYPE.SNOWFLAKE: "Snowflake_IbisConnection",
            CONST_DB_PROVIDER_TYPE.BIGQUERY: "BigQuery_IbisConnection",
            CONST_DB_PROVIDER_TYPE.MSSQL: "MSSQL_IbisConnection",
            CONST_DB_PROVIDER_TYPE.MYSQL: "MySQL_IbisConnection",
            CONST_DB_PROVIDER_TYPE.TRINO: "Trino_IbisConnection",
            CONST_DB_PROVIDER_TYPE.PYSPARK: "PySpark_IbisConnection",
            CONST_DB_PROVIDER_TYPE.REDSHIFT: "Redshift_IbisConnection",
            CONST_DB_PROVIDER_TYPE.ORACLE: "Oracle_IbisConnection",
            CONST_DB_PROVIDER_TYPE.PYICEBERG_REST: "IcebergRestConnection",
        }

    @classmethod
    def get_connection(
        cls, settings_parameters: SettingsParameters
    ) -> BaseDBConnection:
        """
        Convenience method to get connection instance directly.

        Args:
            settings_parameters: SettingsParameters with settings_class

        Returns:
            Connection instance ready to use

        Example:
            factory = ConnectionFactory()
            connection = factory.get_connection(settings_params)
            backend = connection.connect()
        """
        connection_class = cls.get_strategy(settings_parameters)
        return connection_class(db_auth_settings_parameters=settings_parameters)
