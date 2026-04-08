"""
Operations Factory for settings-driven database operations creation.

Uses SettingsParameters to auto-detect backend and create appropriate operations instance.
"""

import logging
from typing import Type

from mountainash_settings import SettingsParameters

from mountainash_data.backends.ibis.operations import BaseIbisOperations
from ..constants import CONST_DB_PROVIDER_TYPE
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

    # Each subclass needs its own dictionaries to avoid sharing with other factories
    _strategy_cache = {}
    _strategy_modules = {}
    _strategy_classes = {}

    @classmethod
    def _configure_strategy_mapping(cls) -> None:
        """
        Configure strategy mappings using ONLY strings (no imports).

        Maps backend types to operations module paths and class names.
        """
        # All ibis backends point directly at backends.ibis.operations
        # (bypasses the databases.operations.ibis shim chain from Phase 4).
        # Per-backend ops classes (SQLite_IbisOperations, etc.) are in the
        # same module; POSTGRESQL/SNOWFLAKE/etc. fall back to BaseIbisOperations.
        cls._strategy_modules = {
            CONST_DB_PROVIDER_TYPE.POSTGRESQL: "mountainash_data.backends.ibis.operations",
            CONST_DB_PROVIDER_TYPE.SQLITE: "mountainash_data.backends.ibis.operations",
            CONST_DB_PROVIDER_TYPE.DUCKDB: "mountainash_data.backends.ibis.operations",
            CONST_DB_PROVIDER_TYPE.MOTHERDUCK: "mountainash_data.backends.ibis.operations",
            CONST_DB_PROVIDER_TYPE.SNOWFLAKE: "mountainash_data.backends.ibis.operations",
            CONST_DB_PROVIDER_TYPE.BIGQUERY: "mountainash_data.backends.ibis.operations",
            CONST_DB_PROVIDER_TYPE.MSSQL: "mountainash_data.backends.ibis.operations",
            CONST_DB_PROVIDER_TYPE.MYSQL: "mountainash_data.backends.ibis.operations",
            CONST_DB_PROVIDER_TYPE.TRINO: "mountainash_data.backends.ibis.operations",
            CONST_DB_PROVIDER_TYPE.PYSPARK: "mountainash_data.backends.ibis.operations",
            CONST_DB_PROVIDER_TYPE.REDSHIFT: "mountainash_data.backends.ibis.operations",
            CONST_DB_PROVIDER_TYPE.ORACLE: "mountainash_data.backends.ibis.operations",
            # Iceberg REST — operations are merged into IcebergRestConnection
            CONST_DB_PROVIDER_TYPE.PYICEBERG_REST: "mountainash_data.backends.iceberg.catalogs.rest",
        }

        cls._strategy_classes = {
            CONST_DB_PROVIDER_TYPE.POSTGRESQL: "BaseIbisOperations",
            CONST_DB_PROVIDER_TYPE.SQLITE: "SQLite_IbisOperations",
            CONST_DB_PROVIDER_TYPE.DUCKDB: "DuckDB_IbisOperations",
            CONST_DB_PROVIDER_TYPE.MOTHERDUCK: "MotherDuck_IbisOperations",
            CONST_DB_PROVIDER_TYPE.SNOWFLAKE: "BaseIbisOperations",
            CONST_DB_PROVIDER_TYPE.BIGQUERY: "BaseIbisOperations",
            CONST_DB_PROVIDER_TYPE.MSSQL: "BaseIbisOperations",
            CONST_DB_PROVIDER_TYPE.MYSQL: "BaseIbisOperations",
            CONST_DB_PROVIDER_TYPE.TRINO: "Trino_IbisOperations",
            CONST_DB_PROVIDER_TYPE.PYSPARK: "BaseIbisOperations",
            CONST_DB_PROVIDER_TYPE.REDSHIFT: "BaseIbisOperations",
            CONST_DB_PROVIDER_TYPE.ORACLE: "BaseIbisOperations",
            CONST_DB_PROVIDER_TYPE.PYICEBERG_REST: "IcebergRestConnection",
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
