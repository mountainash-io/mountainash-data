"""
High-level database utilities for settings-driven connection and operations management.

Provides a unified API for creating database connections and operations
with automatic backend detection and lazy loading.
"""

import logging
from typing import Any, Optional

from mountainash_settings import SettingsParameters, MountainAshBaseSettings

from .databases import BaseDBConnection
from .databases.operations.ibis.base_ibis_operations import BaseIbisOperations
from .databases.constants import CONST_DB_PROVIDER_TYPE
from .factories import ConnectionFactory, OperationsFactory, SettingsFactory

logger = logging.getLogger(__name__)


class DatabaseUtils:
    """
    High-level API for settings-driven database operations.

    All operations driven by SettingsParameters, with automatic backend
    detection and lazy loading of backend-specific implementations.

    Example:
        # Create settings parameters
        settings_params = SettingsParameters.create(
            settings_class=PostgreSQLAuthSettings,
            config_files=["postgres.env"]
        )

        # Auto-detect and create connection
        connection = DatabaseUtils.create_connection(settings_params)
        backend = connection.connect()

        # Auto-detect and create operations
        operations = DatabaseUtils.create_operations(settings_params)
        operations.create_table(backend, "my_table", dataframe)
    """

    @classmethod
    def create_connection(
        cls, settings_parameters: SettingsParameters
    ) -> BaseDBConnection:
        """
        Create database connection from settings parameters.

        Auto-detects backend from settings_class and returns appropriate connection.

        Args:
            settings_parameters: SettingsParameters with settings_class

        Returns:
            Connection instance ready to use

        Example:
            settings_params = SettingsParameters.create(
                settings_class=PostgreSQLAuthSettings,
                config_files=["postgres.env"]
            )
            connection = DatabaseUtils.create_connection(settings_params)
            backend = connection.connect()
        """
        factory = ConnectionFactory()
        return factory.get_connection(settings_parameters)

    @classmethod
    def create_operations(
        cls, settings_parameters: SettingsParameters
    ) -> BaseIbisOperations:
        """
        Create database operations from settings parameters.

        Auto-detects backend from settings_class and returns appropriate operations.

        Args:
            settings_parameters: SettingsParameters with settings_class

        Returns:
            Operations instance ready to use

        Example:
            settings_params = SettingsParameters.create(
                settings_class=PostgreSQLAuthSettings,
                config_files=["postgres.env"]
            )
            operations = DatabaseUtils.create_operations(settings_params)
            operations.upsert(backend, "my_table", df, natural_key_columns=["id"])
        """
        factory = OperationsFactory()
        return factory.get_operations(settings_parameters)

    @classmethod
    def create_backend(
        cls, settings_parameters: SettingsParameters, **connect_kwargs
    ) -> Any:
        """
        Create connection and connect to backend in one step.

        Convenience method combining connection creation and connection.

        Args:
            settings_parameters: SettingsParameters with settings_class
            **connect_kwargs: Additional arguments passed to connect()

        Returns:
            Connected backend (Ibis backend or PyIceberg catalog)

        Example:
            settings_params = SettingsParameters.create(
                settings_class=PostgreSQLAuthSettings,
                config_files=["postgres.env"]
            )
            backend = DatabaseUtils.create_backend(settings_params)
            tables = backend.list_tables()
        """
        connection = cls.create_connection(settings_parameters)
        return connection.connect(**connect_kwargs)

    @classmethod
    def create_settings_from_url(
        cls, connection_url: str, **kwargs
    ) -> MountainAshBaseSettings:
        """
        Auto-detect backend from URL and create appropriate settings.

        Args:
            connection_url: Database connection URL
            **kwargs: Arguments passed to settings constructor

        Returns:
            Settings instance for detected backend

        Example:
            settings = DatabaseUtils.create_settings_from_url(
                "postgresql://user:pass@localhost:5432/db",
                config_files=["postgres.env"]
            )
        """
        return SettingsFactory.from_connection_string(connection_url, **kwargs)

    @classmethod
    def create_settings_from_backend_type(
        cls, backend_type: CONST_DB_PROVIDER_TYPE, **kwargs
    ) -> MountainAshBaseSettings:
        """
        Create settings for specific backend type.

        Args:
            backend_type: Database backend type enum
            **kwargs: Arguments passed to settings constructor

        Returns:
            Settings instance for the backend

        Example:
            settings = DatabaseUtils.create_settings_from_backend_type(
                CONST_DB_PROVIDER_TYPE.POSTGRESQL,
                config_files=["postgres.env"]
            )
        """
        return SettingsFactory.from_backend_type(backend_type, **kwargs)

    @classmethod
    def detect_backend_from_url(cls, connection_url: str) -> CONST_DB_PROVIDER_TYPE:
        """
        Detect backend type from connection URL.

        Args:
            connection_url: Database connection URL

        Returns:
            Detected backend type

        Example:
            backend_type = DatabaseUtils.detect_backend_from_url(
                "postgresql://localhost/db"
            )
        """
        return SettingsFactory.detect_backend_from_url(connection_url)

    @classmethod
    def create_from_url(
        cls,
        connection_url: str,
        config_files: Optional[list] = None,
        **settings_kwargs,
    ) -> tuple[BaseDBConnection, Any]:
        """
        Complete workflow: URL → settings → connection → backend.

        Convenience method for quick setup from connection URL.

        Args:
            connection_url: Database connection URL
            config_files: Optional configuration files
            **settings_kwargs: Additional settings constructor arguments

        Returns:
            Tuple of (connection, connected_backend)

        Example:
            connection, backend = DatabaseUtils.create_from_url(
                "postgresql://user:pass@localhost:5432/db",
                config_files=["postgres.env"]
            )
            tables = backend.list_tables()
        """
        # Create settings from URL
        settings = cls.create_settings_from_url(
            connection_url, config_files=config_files, **settings_kwargs
        )

        # Create settings parameters
        settings_params = settings.extract_settings_parameters()

        # Create connection and connect
        connection = cls.create_connection(settings_params)
        backend = connection.connect()

        return connection, backend
