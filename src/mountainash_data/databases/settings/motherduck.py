#path: mountainash_settings/auth/database/providers/file/duckdb.py

from typing import Optional, List, Any, Dict, Tuple, Self
from upath import UPath

from pydantic import Field, model_validator, field_validator

from mountainash_settings import SettingsParameters

from .base import BaseDBAuthSettings
from ..constants import CONST_DB_PROVIDER_TYPE, CONST_DB_AUTH_METHOD


class MotherDuckAuthSettings(BaseDBAuthSettings):
    """DuckDB authentication settings"""

    # PROVIDER_TYPE: str = Field(default=CONST_DB_PROVIDER_TYPE.MOTHERDUCK)
    AUTH_METHOD: str = Field(default=CONST_DB_AUTH_METHOD.TOKEN)  # DuckDB uses file-based authentication

    # File Settings
    # TOKEN: Optional[SecretStr] = Field(default=None)

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
        return CONST_DB_PROVIDER_TYPE.MOTHERDUCK

    @field_validator("DATABASE")
    @classmethod
    def validate_database(cls, value: Optional[int]) -> Optional[int]:
        """Validate validate_memory_limit"""

        precondition: bool = True
        test: bool = value is not None
        valid: bool = (not precondition) | test

        if not valid:
            raise ValueError("DATABASE must be set")

        return value

    #Multi Field Validators
    @model_validator(mode='after')
    def validate_token_set(self) -> Self:

        precondition: bool = self.AUTH_METHOD == CONST_DB_AUTH_METHOD.TOKEN
        test: bool =  self.TOKEN is not None
        valid: bool = (not precondition) | test

        if not valid:
            raise ValueError("Username and password required for password authentication")

        return self



    def _post_init(self, reinitialise: bool) -> None:
        """Initialize provider-specific settings"""
        ...


    def get_connection_string_template(self, scheme: Optional[str] = None) -> str:

        template = f"{scheme}"

        # template +=  "{database}"
        if self.DATABASE is not None:
            template += "{database}"

        if self.TOKEN is not None:
            template += "?motherduck_token={token}"

        return template

    def get_connection_string_params(self) -> Dict[str, Any]:

        params = {}
        # params["scheme"] = scheme if scheme else "duckdb://md:"
        params['database'] = self.DATABASE

        if self.TOKEN is not None:
            params['token'] = self.TOKEN

        return params


    def get_connection_kwargs(self) -> Dict[str, Any]:
        """Get connection arguments for DuckDB"""
        return {}

    def get_post_connection_options(self) -> Dict[str, Any]:

        """Get connection arguments as dictionary"""
        ...
