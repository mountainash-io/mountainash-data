#path: mountainash_settings/auth/storage/providers/cloud/r2.py

from typing import Optional, Dict, Any, List, Tuple

from upath import UPath
from pydantic import Field

from mountainash_settings import SettingsParameters

from .base import BaseDBAuthSettings
from ..constants import (
    # CONST_STORAGE_PROVIDER_TYPE,
    CONST_DB_AUTH_METHOD, CONST_DB_PROVIDER_TYPE
)

class PyIcebergRestAuthSettings(BaseDBAuthSettings):
    """
    Cloudflare R2 storage authentication settings.

    Handles authentication configuration for Cloudflare R2 storage.
    Does not perform actual authentication or connection.
    """
    # PROVIDER_TYPE: str = Field(default="PYICEBERG_REST")  # Need to add PYICEBERG_REST to CONST_STORAGE_PROVIDER_TYPE

    # R2 Settings
    WAREHOUSE: str = Field(...)  # Required - R2 bucket name
    CATALOG_NAME: str = Field(...)  # Required - R2 bucket name
    CATALOG_URI: str = Field(...)  # Required - R2 bucket name

    # Authentication Settings
    AUTH_METHOD: str = Field(default=CONST_DB_AUTH_METHOD.TOKEN )

    # Connection Settings
    USE_SSL: bool = Field(default=False)
    VERIFY_SSL: bool = Field(default=True)

    def __init__(self,
                 config_files: Optional[str|UPath|List[str|UPath]|Tuple[str|UPath]] = None,
                 settings_parameters: Optional[SettingsParameters] = None,
                 **kwargs) -> None:

        super().__init__(config_files=config_files,
                         settings_parameters=settings_parameters,
                         **kwargs)


    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        """Database provider identifier."""
        return CONST_DB_PROVIDER_TYPE.PYICEBERG_REST


    def _init_provider_specific(self, reinitialise: bool) -> None:
        """Initialize provider-specific settings"""
        # Validate authentication requirements
        pass


    def get_connection_url(self) -> Dict[str, Any]:
        return None

    def get_connection_kwargs(self, db_abstraction_layer: Optional[str] = None) -> Dict[str, Any]:
        """Get connection arguments as dictionary"""
        args = {}#  super().get_connection_kwargs()

        # Add R2-specific arguments
        args.update({
            "name": self.CATALOG_NAME,
            "warehouse": self.WAREHOUSE,
            "uri": self.CATALOG_URI,
            "token": self.TOKEN,
        })

        return {k: v for k, v in args.items() if v is not None}


    ########################
    # Abstract Methods
    def _post_init(self, reinitialise: bool) -> None:
        """Initialize provider-specific settings"""
        pass

    # @abstractmethod
    # def get_connection_string(self, variant: Optional[str]) -> str:
    #     """Generate connection string from settings"""
    #     pass

    def get_connection_string_template(self, scheme: Optional[str] = None) -> str:
        """Get connection arguments as dictionary"""
        ...


    def get_connection_string_params(self) -> Dict[str, Any]:
        """Get connection string params as a dictionary"""
        ...


    def get_post_connection_options(self, db_abstraction_layer: Optional[str] = None) -> Dict[str, Any]:

        """Get connection arguments as dictionary"""
        ...
