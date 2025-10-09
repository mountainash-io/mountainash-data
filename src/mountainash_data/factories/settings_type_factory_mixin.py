"""
Settings Type Detection Mixin for Factory Pattern.

Detects database backend type from SettingsParameters.settings_class,
similar to DataFrame type detection in mountainash-dataframes.
"""

import logging
import re
from typing import Optional, Type

from mountainash_settings import SettingsParameters, MountainAshBaseSettings

from ..databases.constants import CONST_DB_PROVIDER_TYPE

logger = logging.getLogger(__name__)


class SettingsTypeFactoryMixin:
    """
    Mixin for detecting database backend type from settings class.

    Uses three-tier detection system:
        1. Exact Match (fast path): Direct mapping of known settings classes
        2. Pattern Matching (flexible): Regex-based class name matching
        3. Logging: Track unmapped types for future registration
    """

    # Layer 1: Exact Match - Direct settings class mapping (O(1) lookup)
    TYPE_MAP = {
        # Map settings class → backend type
        # These will be populated when settings classes are imported
    }

    # Layer 2: Pattern Matching - Class name patterns for flexible detection
    PATTERN_MAP = {
        # Regex pattern → backend type
        r"PostgreSQL.*Settings": CONST_DB_PROVIDER_TYPE.POSTGRESQL,
        r"SQLite.*Settings": CONST_DB_PROVIDER_TYPE.SQLITE,
        r"DuckDB.*Settings": CONST_DB_PROVIDER_TYPE.DUCKDB,
        r"MotherDuck.*Settings": CONST_DB_PROVIDER_TYPE.MOTHERDUCK,
        r"Snowflake.*Settings": CONST_DB_PROVIDER_TYPE.SNOWFLAKE,
        r"BigQuery.*Settings": CONST_DB_PROVIDER_TYPE.BIGQUERY,
        r"MSSQL.*Settings": CONST_DB_PROVIDER_TYPE.MSSQL,
        r"MySQL.*Settings": CONST_DB_PROVIDER_TYPE.MYSQL,
        r"Trino.*Settings": CONST_DB_PROVIDER_TYPE.TRINO,
        r"PySpark.*Settings": CONST_DB_PROVIDER_TYPE.PYSPARK,
        r"Redshift.*Settings": CONST_DB_PROVIDER_TYPE.REDSHIFT,
        r"Oracle.*Settings": CONST_DB_PROVIDER_TYPE.ORACLE,
        r"PyIceberg.*Settings": CONST_DB_PROVIDER_TYPE.PYICEBERG_REST,
    }

    @classmethod
    def _get_strategy_key(
        cls, settings_parameters: SettingsParameters, **kwargs
    ) -> Optional[CONST_DB_PROVIDER_TYPE]:
        """
        Detect backend type from SettingsParameters.

        Args:
            settings_parameters: SettingsParameters with settings_class
            **kwargs: Additional context (unused)

        Returns:
            Backend type enum or None if detection fails
        """
        if settings_parameters is None:
            logger.warning("SettingsParameters is None, cannot detect backend")
            return None

        settings_class = settings_parameters.settings_class

        if settings_class is None:
            logger.warning("settings_class is None in SettingsParameters")
            return None

        # Layer 1: Exact Match (Fast Path)
        backend_type = cls._detect_from_exact_match(settings_class)
        if backend_type:
            return backend_type

        # Layer 2: Pattern Matching (Flexible Fallback)
        backend_type = cls._detect_from_pattern_match(settings_class)
        if backend_type:
            # Auto-register for future fast-path lookup
            cls._register_settings_class(settings_class, backend_type)
            return backend_type

        # Layer 3: Logging (Detection Failed)
        cls._log_unmapped_settings_class(settings_class)
        return None

    @classmethod
    def _detect_from_exact_match(
        cls, settings_class: Type[MountainAshBaseSettings]
    ) -> Optional[CONST_DB_PROVIDER_TYPE]:
        """
        Fast path: Direct lookup in TYPE_MAP.

        Args:
            settings_class: Settings class to detect

        Returns:
            Backend type or None
        """
        return cls.TYPE_MAP.get(settings_class)

    @classmethod
    def _detect_from_pattern_match(
        cls, settings_class: Type[MountainAshBaseSettings]
    ) -> Optional[CONST_DB_PROVIDER_TYPE]:
        """
        Flexible fallback: Regex pattern matching on class name.

        Args:
            settings_class: Settings class to detect

        Returns:
            Backend type or None
        """
        class_name = settings_class.__name__

        for pattern, backend_type in cls.PATTERN_MAP.items():
            if re.match(pattern, class_name):
                logger.debug(
                    f"Pattern matched: {class_name} → {backend_type} (pattern: {pattern})"
                )
                return backend_type

        return None

    @classmethod
    def _register_settings_class(
        cls,
        settings_class: Type[MountainAshBaseSettings],
        backend_type: CONST_DB_PROVIDER_TYPE,
    ) -> None:
        """
        Register settings class in TYPE_MAP for future fast-path lookup.

        Args:
            settings_class: Settings class to register
            backend_type: Detected backend type
        """
        cls.TYPE_MAP[settings_class] = backend_type
        logger.debug(
            f"Auto-registered {settings_class.__name__} → {backend_type} for fast lookup"
        )

    @classmethod
    def _log_unmapped_settings_class(
        cls, settings_class: Type[MountainAshBaseSettings]
    ) -> None:
        """
        Log unmapped settings class for debugging.

        Args:
            settings_class: Unmapped settings class
        """
        class_name = settings_class.__name__
        module_name = settings_class.__module__

        logger.warning(
            f"Unmapped settings class: {class_name} (module: {module_name}). "
            f"Consider adding to TYPE_MAP or PATTERN_MAP."
        )

    @classmethod
    def register_settings_class_mapping(
        cls,
        settings_class: Type[MountainAshBaseSettings],
        backend_type: CONST_DB_PROVIDER_TYPE,
    ) -> None:
        """
        Manually register a settings class mapping.

        Useful for custom settings classes or explicit registration.

        Args:
            settings_class: Settings class to register
            backend_type: Backend type to map to
        """
        cls.TYPE_MAP[settings_class] = backend_type
        logger.info(f"Manually registered {settings_class.__name__} → {backend_type}")
