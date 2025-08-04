from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List, Tuple, Self
from upath import UPath
from pydantic import Field, SecretStr, field_validator, model_validator


from mountainash_settings import SettingsParameters, MountainAshBaseSettings
from ..constants import CONST_DB_AUTH_METHOD

class BaseDBAuthSettings(MountainAshBaseSettings, ABC):
    """Base class for database authentication settings"""

    # Provider Configuration
    PROVIDER_TYPE: str = Field(...)
    AUTH_METHOD: str = Field(default=CONST_DB_AUTH_METHOD.PASSWORD)

    # Connection Settings
    HOST: Optional[str] = Field(default=None)
    PORT: Optional[int] = Field(default=None)
    DATABASE: Optional[str] = Field(default=None)
    SCHEMA: Optional[str] = Field(default=None)

    # Password Authentication
    USERNAME: Optional[str] = Field(default=None)
    PASSWORD: Optional[SecretStr] = Field(default=None)

    # Token Authentication
    TOKEN: Optional[SecretStr] = Field(default=None)


    def __init__(self,
                 config_files: Optional[str|UPath|List[str|UPath]|Tuple[str|UPath]] = None,
                 settings_parameters:   Optional[SettingsParameters] = None,
                #  _dummy: Optional[bool] = False,
                 **kwargs) -> None:


        super().__init__(config_files=config_files,
                         settings_parameters=settings_parameters,
                        #  _dummy=_dummy,
                         **kwargs)



    @field_validator("PORT")
    @classmethod
    def validate_port(cls, value: Optional[int|str]) -> Optional[int|str]:
        """Validate port number"""

        precondition: bool = value is not None
        test: bool = (1 <= int(value) <= 65535) if precondition else False
        valid: bool = (not precondition) | test

        print(f"precondition: {precondition}, test: {test}, valid: {valid}")


        if not valid:
            raise ValueError(f"Invalid port number: {value}")

        return value

    ########################
    # Multi Field Validators
    @model_validator(mode='after')
    def validate_auth_method_password(self) -> Self:

        precondition: bool = self.AUTH_METHOD == CONST_DB_AUTH_METHOD.PASSWORD and self.SETTINGS_NAMESPACE != "DUMMY"
        test: bool =  self.USERNAME is not None and self.PASSWORD is not None
        valid: bool = (not precondition) | test


        if not valid:
            raise ValueError("USERNAME and PASSWORD required for password authentication")

        return self

    @model_validator(mode='after')
    def validate_auth_method_token(self) -> Self:

        precondition: bool = self.AUTH_METHOD == CONST_DB_AUTH_METHOD.TOKEN and self.SETTINGS_NAMESPACE != "DUMMY"
        test: bool =  self.TOKEN is not None
        valid: bool = (not precondition) | test

        if not valid:
            raise ValueError("TOKEN required for token authentication")

        return self




    ########################
    # Post init template parameters


    def post_init(self, reinitialise: bool = False) -> None:
        """Post-initialization validation and setup"""
        self._post_init(reinitialise)


    ########################
    # Abstract Methods
    @abstractmethod
    def _post_init(self, reinitialise: bool) -> None:
        """Initialize provider-specific settings"""
        pass

    # @abstractmethod
    # def get_connection_string(self, variant: Optional[str]) -> str:
    #     """Generate connection string from settings"""
    #     pass

    @abstractmethod
    def get_connection_string_template(self, scheme: Optional[str] = None) -> str:
        """Get connection arguments as dictionary"""
        ...


    @abstractmethod
    def get_connection_string_params(self) -> Dict[str, Any]:
        """Get connection string params as a dictionary"""
        ...

    @abstractmethod
    def get_connection_kwargs(self, db_abstraction_layer: Optional[str] = None) -> Dict[str, Any]:

        """Get connection arguments as dictionary"""
        ...

    @abstractmethod
    def get_post_connection_options(self, db_abstraction_layer: Optional[str] = None) -> Dict[str, Any]:

        """Get connection arguments as dictionary"""
        ...
