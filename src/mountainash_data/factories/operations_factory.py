"""
Operations Factory for settings-driven database operations creation.

Uses SettingsParameters to auto-detect backend and create appropriate operations instance.
"""

import logging
from typing import Type

from mountainash_settings import SettingsParameters

from ..databases.operations.ibis.base_ibis_operations import BaseIbisOperations
from ..databases.constants import CONST_DB_PROVIDER_TYPE
from .base_strategy_factory import BaseStrategyFactory
from .settings_type_factory_mixin import SettingsTypeFactoryMixin

logger = logging.getLogger(__name__)


class OperationsFactory(
    SettingsTypeFactoryMixin,
    BaseStrategyFactory[SettingsParameters, Type[BaseIbisOperations]],
):
    """
    Settings-driven factory for database operations.

    Detects backend type from SettingsParameters.settings_class and returns
    appropriate operations class with lazy loading.

    Example:
        settings_params = SettingsParameters.create(
            settings_class=PostgreSQLAuthSettings,
            config_files=["postgres.env"]
        )

        factory = OperationsFactory()
        operations_class = factory.get_strategy(settings_params)
        operations = operations_class(db_auth_settings_parameters=settings_params)
        operations.create_table(backend, "my_table", dataframe)
    """

    @classmethod
    def _configure_strategy_mapping(cls) -> None:
        """
        Configure strategy mappings using ONLY strings (no imports).

        Maps backend types to operations module paths and class names.
        """
        cls._strategy_modules = {
            CONST_DB_PROVIDER_TYPE.POSTGRESQL: "mountainash_data.databases.operations.ibis",
            CONST_DB_PROVIDER_TYPE.SQLITE: "mountainash_data.databases.operations.ibis",
            CONST_DB_PROVIDER_TYPE.DUCKDB: "mountainash_data.databases.operations.ibis",
            CONST_DB_PROVIDER_TYPE.MOTHERDUCK: "mountainash_data.databases.operations.ibis",
            CONST_DB_PROVIDER_TYPE.SNOWFLAKE: "mountainash_data.databases.operations.ibis",
            CONST_DB_PROVIDER_TYPE.BIGQUERY: "mountainash_data.databases.operations.ibis",
            CONST_DB_PROVIDER_TYPE.MSSQL: "mountainash_data.databases.operations.ibis",
            CONST_DB_PROVIDER_TYPE.MYSQL: "mountainash_data.databases.operations.ibis",
            CONST_DB_PROVIDER_TYPE.TRINO: "mountainash_data.databases.operations.ibis",
            CONST_DB_PROVIDER_TYPE.PYSPARK: "mountainash_data.databases.operations.ibis",
            CONST_DB_PROVIDER_TYPE.REDSHIFT: "mountainash_data.databases.operations.ibis",
            CONST_DB_PROVIDER_TYPE.ORACLE: "mountainash_data.databases.operations.ibis",
            # PyIceberg operations (if they exist)
            # CONST_DB_PROVIDER_TYPE.PYICEBERG_REST: "mountainash_data.databases.operations.pyiceberg",
        }

        cls._strategy_classes = {
            CONST_DB_PROVIDER_TYPE.POSTGRESQL: "PostgresIbisOperations",
            CONST_DB_PROVIDER_TYPE.SQLITE: "SQLiteIbisOperations",
            CONST_DB_PROVIDER_TYPE.DUCKDB: "DuckDBIbisOperations",
            CONST_DB_PROVIDER_TYPE.MOTHERDUCK: "MotherDuckIbisOperations",
            CONST_DB_PROVIDER_TYPE.SNOWFLAKE: "SnowflakeIbisOperations",
            CONST_DB_PROVIDER_TYPE.BIGQUERY: "BigQueryIbisOperations",
            CONST_DB_PROVIDER_TYPE.MSSQL: "MSSQLIbisOperations",
            CONST_DB_PROVIDER_TYPE.MYSQL: "MySQLIbisOperations",
            CONST_DB_PROVIDER_TYPE.TRINO: "TrinoIbisOperations",
            CONST_DB_PROVIDER_TYPE.PYSPARK: "PySparkIbisOperations",
            CONST_DB_PROVIDER_TYPE.REDSHIFT: "RedshiftIbisOperations",
            CONST_DB_PROVIDER_TYPE.ORACLE: "OracleIbisOperations",
        }

    @classmethod
    def get_operations(
        cls, settings_parameters: SettingsParameters
    ) -> BaseIbisOperations:
        """
        Convenience method to get operations instance directly.

        Args:
            settings_parameters: SettingsParameters with settings_class

        Returns:
            Operations instance ready to use

        Example:
            factory = OperationsFactory()
            operations = factory.get_operations(settings_params)
            operations.create_table(backend, "my_table", dataframe)
        """
        operations_class = cls.get_strategy(settings_parameters)
        return operations_class(db_auth_settings_parameters=settings_parameters)
