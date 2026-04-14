#path: mountainash_settings/auth/database/providers/file/duckdb.py

from typing import Optional, List, Any, Dict, Tuple
from upath import UPath
import re

from pydantic import Field, field_validator

from mountainash_settings import SettingsParameters

from .base import BaseDBAuthSettings
from ..constants import CONST_DB_PROVIDER_TYPE


class DuckDBAuthSettings(BaseDBAuthSettings):
    """DuckDB authentication settings

    Ibis DuckDB: https://ibis-project.org/backends/duckdb
    https://duckdb.org/docs/configuration/overview.html

    Geospatial: https://duckdb.org/docs/extensions/spatial.html#st_read—read-spatial-data-from-files

    """

    # PROVIDER_TYPE: str = Field(default=CONST_DB_PROVIDER_TYPE.DUCKDB)
    AUTH_METHOD: str = Field(default="none")  # DuckDB uses file-based authentication

    # File Settings
    READ_ONLY: bool = Field(default=True)

    # # Configuration Settings
    THREADS: Optional[int] = Field(default=None)
    MEMORY_LIMIT: Optional[str] = Field(default=None)  # e.g., "4GB"
    # TEMP_DIRECTORY: Optional[str] = Field(default=None)

    # # Extension Settings
    EXTENSIONS: List[str] = Field(default_factory=list)
    # ALLOW_UNSIGNED_EXTENSIONS: bool = Field(default=False)

    # # Performance Settings
    # PAGE_SIZE: Optional[int] = Field(default=None)  # in bytes
    # COMPRESSION: Optional[str] = Field(default="auto")
    # ACCESS_MODE: Optional[str] = Field(default=None)  # "AUTOMATIC", "DIRECT_IO"

    #Attach external database(s)
    ATTACH_PATH: Optional[str|List[str]] = Field(default=None)

    def __init__(self,
                 config_files: Optional[str|UPath|List[str|UPath]|Tuple[str|UPath]] = None,
                 settings_parameters:   Optional[SettingsParameters] = None,
                #  _dummy: Optional[bool] = False,
                 **kwargs) -> None:


        super().__init__(config_files=config_files,
                         settings_parameters=settings_parameters,
                        #  _dummy=_dummy,
                         **kwargs)

    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        """Database provider identifier."""
        return CONST_DB_PROVIDER_TYPE.DUCKDB


    @field_validator("MEMORY_LIMIT")
    @classmethod
    def validate_memory_limit(cls, value: Optional[str]) -> Optional[str]:
        """Validate validate_memory_limit"""

        regex: str = r'^\d+[KMG]B$'
        precondition: bool = value is not None
        test: bool = bool(re.match(regex, value)) if precondition else True
        valid: bool = (not precondition) | test

        if not valid:
            raise ValueError("Memory limit must match the format: number + unit (KB, MB, GB).")

        return value


    def _post_init(self, reinitialise: bool) -> None:
        """Initialize provider-specific settings"""
        ...


    def get_connection_string_template(self, scheme: Optional[str] = None) -> str:
        """Generate DuckDB connection string"""

        template = f"{scheme}"

        if self.DATABASE:
            template += "{database}"

        return template

    def get_connection_string_params(self) -> Dict[str, Any]:
        """Get connection arguments for DuckDB"""
        args = {}
        # args["scheme"] = scheme if scheme else "duckdb://"

        if self.DATABASE is not None:
            args["database"] = UPath(self.DATABASE).expanduser()
        else:
            args["database"] = ":memory:"

        return {k: v for k, v in args.items() if v is not None}

    def get_connection_kwargs(self) -> Dict[str, Any]:
        """Get connection arguments for DuckDB"""
        args =  {}

        if self.DATABASE:
            args["database"] = self.DATABASE
        if self.READ_ONLY:
            args["read_only"] = self.READ_ONLY

        # values for config parameter
        config = {}
        if self.THREADS:
            config["threads"] = self.THREADS
        if self.MEMORY_LIMIT:
            config["memory_limit"] = self.MEMORY_LIMIT
        if self.EXTENSIONS:
            config["extensions"] = self.EXTENSIONS

        if config:
            args["config"] = config

        return args

    def get_post_connection_options(self) -> Dict[str, Any]:

        """Get connection arguments as dictionary"""
        ...
