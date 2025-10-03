"""
Settings Factory for auto-detecting and creating appropriate settings classes.

Provides utilities to auto-detect database backend from connection strings
and create the corresponding settings class.
"""

import logging
import re
from typing import Dict, Type
from urllib.parse import urlparse

from mountainash_settings import MountainAshBaseSettings

from ..databases.constants import CONST_DB_PROVIDER_TYPE

logger = logging.getLogger(__name__)


class SettingsFactory:
    """
    Factory for auto-detecting and loading appropriate settings classes.

    Provides intelligent backend detection from connection strings and
    automatic settings class selection.
    """

    # Mapping of URL schemes to backend types
    SCHEME_MAP: Dict[str, CONST_DB_PROVIDER_TYPE] = {
        "postgresql": CONST_DB_PROVIDER_TYPE.POSTGRESQL,
        "postgres": CONST_DB_PROVIDER_TYPE.POSTGRESQL,
        "sqlite": CONST_DB_PROVIDER_TYPE.SQLITE,
        "duckdb": CONST_DB_PROVIDER_TYPE.DUCKDB,
        "motherduck": CONST_DB_PROVIDER_TYPE.MOTHERDUCK,
        "md": CONST_DB_PROVIDER_TYPE.MOTHERDUCK,
        "snowflake": CONST_DB_PROVIDER_TYPE.SNOWFLAKE,
        "bigquery": CONST_DB_PROVIDER_TYPE.BIGQUERY,
        "mssql": CONST_DB_PROVIDER_TYPE.MSSQL,
        "mysql": CONST_DB_PROVIDER_TYPE.MYSQL,
        "trino": CONST_DB_PROVIDER_TYPE.TRINO,
        "pyspark": CONST_DB_PROVIDER_TYPE.PYSPARK,
        "redshift": CONST_DB_PROVIDER_TYPE.REDSHIFT,
        "oracle": CONST_DB_PROVIDER_TYPE.ORACLE,
    }

    # Mapping of backend types to settings classes (lazy loaded)
    SETTINGS_CLASS_MAP: Dict[CONST_DB_PROVIDER_TYPE, Type[MountainAshBaseSettings]] = {}

    @classmethod
    def _ensure_settings_classes_loaded(cls) -> None:
        """
        Lazy load settings class mappings.

        This avoids circular imports and reduces initial load time.
        """
        if cls.SETTINGS_CLASS_MAP:
            return  # Already loaded

        # Lazy import settings classes only when needed
        from ..databases.settings import (
            PostgreSQLAuthSettings,
            SQLiteAuthSettings,
            DuckDBAuthSettings,
            MotherDuckAuthSettings,
            SnowflakeAuthSettings,
            BigQueryAuthSettings,
            MSSQLAuthSettings,
            MySQLAuthSettings,
            TrinoAuthSettings,
            PySparkAuthSettings,
            RedshiftAuthSettings,
            PyIcebergRestAuthSettings,
        )

        cls.SETTINGS_CLASS_MAP = {
            CONST_DB_PROVIDER_TYPE.POSTGRESQL: PostgreSQLAuthSettings,
            CONST_DB_PROVIDER_TYPE.SQLITE: SQLiteAuthSettings,
            CONST_DB_PROVIDER_TYPE.DUCKDB: DuckDBAuthSettings,
            CONST_DB_PROVIDER_TYPE.MOTHERDUCK: MotherDuckAuthSettings,
            CONST_DB_PROVIDER_TYPE.SNOWFLAKE: SnowflakeAuthSettings,
            CONST_DB_PROVIDER_TYPE.BIGQUERY: BigQueryAuthSettings,
            CONST_DB_PROVIDER_TYPE.MSSQL: MSSQLAuthSettings,
            CONST_DB_PROVIDER_TYPE.MYSQL: MySQLAuthSettings,
            CONST_DB_PROVIDER_TYPE.TRINO: TrinoAuthSettings,
            CONST_DB_PROVIDER_TYPE.PYSPARK: PySparkAuthSettings,
            CONST_DB_PROVIDER_TYPE.REDSHIFT: RedshiftAuthSettings,
            CONST_DB_PROVIDER_TYPE.PYICEBERG_REST: PyIcebergRestAuthSettings,
        }

        logger.debug("Settings class mappings loaded")

    @classmethod
    def from_backend_type(
        cls, backend_type: CONST_DB_PROVIDER_TYPE, **kwargs
    ) -> MountainAshBaseSettings:
        """
        Create settings instance from backend type.

        Args:
            backend_type: Database backend type enum
            **kwargs: Arguments passed to settings constructor

        Returns:
            Settings instance for the backend

        Raises:
            KeyError: If backend type not supported
        """
        cls._ensure_settings_classes_loaded()

        if backend_type not in cls.SETTINGS_CLASS_MAP:
            raise KeyError(
                f"No settings class for {backend_type}. "
                f"Available: {list(cls.SETTINGS_CLASS_MAP.keys())}"
            )

        settings_class = cls.SETTINGS_CLASS_MAP[backend_type]
        return settings_class(**kwargs)

    @classmethod
    def from_connection_string(
        cls, connection_url: str, **kwargs
    ) -> MountainAshBaseSettings:
        """
        Auto-detect backend from connection URL and create settings.

        Args:
            connection_url: Database connection URL
            **kwargs: Arguments passed to settings constructor

        Returns:
            Settings instance for detected backend

        Raises:
            ValueError: If backend cannot be detected from URL

        Example:
            settings = SettingsFactory.from_connection_string(
                "postgresql://user:pass@localhost:5432/db",
                config_files=["postgres.env"]
            )
        """
        backend_type = cls.detect_backend_from_url(connection_url)
        return cls.from_backend_type(backend_type, **kwargs)

    @classmethod
    def detect_backend_from_url(cls, connection_url: str) -> CONST_DB_PROVIDER_TYPE:
        """
        Detect backend type from connection URL.

        Args:
            connection_url: Database connection URL

        Returns:
            Detected backend type

        Raises:
            ValueError: If backend cannot be detected

        Example:
            backend = SettingsFactory.detect_backend_from_url(
                "postgresql://localhost/db"
            )
            # Returns: CONST_DB_PROVIDER_TYPE.POSTGRESQL
        """
        parsed = urlparse(connection_url)
        scheme = parsed.scheme.lower()

        # Check exact scheme match
        if scheme in cls.SCHEME_MAP:
            return cls.SCHEME_MAP[scheme]

        # Check for special patterns (e.g., duckdb://md: for MotherDuck)
        if scheme == "duckdb" and connection_url.startswith("duckdb://md:"):
            return CONST_DB_PROVIDER_TYPE.MOTHERDUCK

        # Pattern matching for complex URLs
        for pattern, backend_type in cls.SCHEME_MAP.items():
            if re.match(pattern, scheme):
                logger.debug(
                    f"Pattern matched: {scheme} → {backend_type} (pattern: {pattern})"
                )
                return backend_type

        raise ValueError(
            f"Cannot detect backend from URL scheme: {scheme}. "
            f"URL: {connection_url}. "
            f"Supported schemes: {list(cls.SCHEME_MAP.keys())}"
        )
