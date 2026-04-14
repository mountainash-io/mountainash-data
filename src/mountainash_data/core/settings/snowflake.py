#path: mountainash_settings/auth/database/providers/cloud/snowflake.py

from typing import Optional, List, Any, Dict, Tuple, Self
from upath import UPath

from pydantic import Field, SecretStr, field_validator, model_validator
import re

from enum import StrEnum

from mountainash_settings import SettingsParameters

from .base import BaseDBAuthSettings
from ..constants import CONST_DB_PROVIDER_TYPE, CONST_DB_AUTH_METHOD


# @value_enum_helpers
class CONST_SNOWFLAKE_AUTHENTICATOR(StrEnum):
    SNOWFLAKE = "snowflake " #The Default
    OAUTH = "oauth"
    OKTA = "okta"
    EXTERNAL_BROWSER = "externalbrowser"
    PASSWORD_MFA = "username_password_mfa "



class SnowflakeAuthSettings(BaseDBAuthSettings):
    """Snowflake authentication settings

    https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-example#connecting-with-oauth

    extra kwargs:
    https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#label-snowflake-connector-methods-connect

    #session parameters
    https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect

    #TODO: Support connection_name from a ~/.snowflake/connections.toml file

    """

    # PROVIDER_TYPE: str = Field(default=CONST_DB_PROVIDER_TYPE.SNOWFLAKE)
    AUTH_METHOD: str = Field(default=CONST_DB_AUTH_METHOD.PASSWORD)
    CONNECTION_NAME: Optional[str] = Field(default=None)

    # Snowflake-specific Settings
    ACCOUNT:            str = Field(...)
    WAREHOUSE:          str = Field(...)
    ROLE:               Optional[str] = Field(default=None)

    # Authentication Settings
    AUTHENTICATOR:      Optional[str] = Field(default="snowflake")
    OKTA_ACCOUNT_NAMER:      Optional[str] = Field(default=None)

    PRIVATE_KEY:        Optional[SecretStr] = Field(default=None)
    PRIVATE_KEY_PATH:   Optional[str] = Field(default=None)
    PRIVATE_KEY_PASSPHRASE: Optional[SecretStr] = Field(default=None)

    # OAuth Settings
    OAUTH_TOKEN:        Optional[SecretStr] = Field(default=None)
    OAUTH_CLIENT_ID:    Optional[str] = Field(default=None)
    OAUTH_CLIENT_SECRET: Optional[SecretStr] = Field(default=None)
    OAUTH_REFRESH_TOKEN: Optional[SecretStr] = Field(default=None)

    # Connection Settings
    TIMEZONE: Optional[str] = Field(default=None)

    # Session Settings
    # QUERY_TAG: Optional[str] = Field(default=None)
    # APPLICATION: Optional[str] = Field(default="MountainAsh")
    # CLIENT_SESSION_KEEP_ALIVE: bool = Field(default=True)

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
        return CONST_DB_PROVIDER_TYPE.SNOWFLAKE

    #Single Field Validators
    @field_validator("ACCOUNT")
    @classmethod
    def validate_account_not_null(cls, value: Optional[str]) -> Optional[str]:
        """Validate validate_account_not_null"""

        valid: bool = value is not None

        if not valid:
            raise ValueError("Account identifier is required.")

        return value

    @field_validator("ACCOUNT")
    @classmethod
    def validate_account_formatted(cls, value: Optional[str]) -> Optional[str]:
        """Validate validate_account_formatted"""

        regex: str = r'^[a-zA-Z0-9-_]+$'
        precondition: bool = value is not None
        test: bool = bool(re.match(regex, value)) if precondition else False
        valid: bool = (not precondition) | test

        if not valid:
            raise ValueError("Account identifier is required.")

        return value


    @field_validator("AUTHENTICATOR")
    @classmethod
    def validate_authenticator(cls, value: Optional[str]) -> Optional[str]:
        """Validate validate_account_formatted"""

        precondition: bool = value is not None
        test: bool = value in CONST_SNOWFLAKE_AUTHENTICATOR.member_values()
        valid: bool = (not precondition) | test

        if not valid:
            raise ValueError("Account identifier is required.")

        return value


    #======================
    # Model Validators
    #======================

    @model_validator(mode='after')
    def validate_authentication_mode(self) -> Self:

        precondition: bool = self.AUTH_METHOD == CONST_DB_AUTH_METHOD.PASSWORD
        test: bool =  self.PASSWORD is not None
        valid: bool = (not precondition) | test

        if not valid:
            raise ValueError("Password required for password authentication")

        return self


    @model_validator(mode='after')
    def validate_certificate_set(self) -> Self:

        precondition: bool = self.AUTH_METHOD == CONST_DB_AUTH_METHOD.CERTIFICATE
        test: bool =  self.PRIVATE_KEY is not None or self.PRIVATE_KEY_PATH is not None
        valid: bool = (not precondition) | test

        if not valid:
            raise ValueError("Private key or key path required for certificate authentication")

        return self

    @model_validator(mode='after')
    def validate_ouath_set(self) -> Self:

        precondition: bool = self.AUTH_METHOD == CONST_DB_AUTH_METHOD.OAUTH
        test: bool =  self.OAUTH_TOKEN is not None  or (self.OAUTH_CLIENT_ID is not None and self.OAUTH_CLIENT_SECRET is not None)
        valid: bool = (not precondition) | test

        if not valid:
            raise ValueError("OAuth token or client credentials required for OAuth authentication")

        return self




    def _post_init(self, reinitialise: bool) -> None:
        pass

    def get_connection_string_template(self,scheme: Optional[str] = None) -> str:
        """Generate Snowflake connection string"""

        # template = "{scheme}{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}"

        template = f"{scheme}"
        # template += "{user}@{account}"

        if self.USERNAME is not None:
            template += "{user}"

        if self.PASSWORD is not None:
            template += ":{password}"

        if self.ACCOUNT is not None:
            template += "@{account}"

        if self.DATABASE is not None:
            template += "/{database}"
        if self.SCHEMA is not None:
            template += "/{schema}"

        if self.WAREHOUSE is not None:
            template += "?warehouse={warehouse}"

        return template

    def get_connection_string_params(self) -> Dict[str, Any]:

        """Get connection arguments for Snowflake"""
        args = {}

        if self.USERNAME is not None:
            args['user'] = self.USERNAME
        if self.HOST is not None:
            args['host'] = self.HOST
        if self.ACCOUNT is not None:
            args['account'] = self.ACCOUNT
        if self.DATABASE is not None:
            args['database'] = self.DATABASE
        if self.SCHEMA is not None:
            args['schema'] = self.SCHEMA
        if self.WAREHOUSE is not None:
            args['warehouse'] = self.WAREHOUSE

        if self.AUTH_METHOD == CONST_DB_AUTH_METHOD.PASSWORD:
            if self.PASSWORD:
                args["password"] =  self.PASSWORD



        return {k: v for k, v in args.items() if v is not None}

    def get_connection_kwargs(self) -> Dict[str, Any]:
        """Get connection arguments for Snowflake"""


        #It seems ibis recognises 'session_parameters' as a valid argument for snowflake
        #https://ibis-project.org/docs/backends/snowflake/

        #Also, how to handle snowflake config files?

        args = {}

        if self.CONNECTION_NAME is not None:
            args['connection_name'] = self.CONNECTION_NAME

        # if self.AUTH_METHOD == CONST_DB_AUTH_METHOD.PASSWORD:
            # if self.AUTHENTICATOR:
            #     args['authenticator'] = self.AUTHENTICATOR

        if self.AUTH_METHOD == CONST_DB_AUTH_METHOD.OAUTH:
            if self.AUTH_METHOD:
                args['authenticator'] = self.AUTH_METHOD
            if self.OAUTH_TOKEN:
                args['token'] =     self.OAUTH_TOKEN

            if self.OAUTH_CLIENT_ID:
                args["oauth_client_id"] = self.OAUTH_CLIENT_ID
            if self.OAUTH_CLIENT_SECRET:
                args["oauth_client_secret"] = self.OAUTH_CLIENT_SECRET
            if self.OAUTH_REFRESH_TOKEN:
                args["oauth_refresh_token"] = self.OAUTH_REFRESH_TOKEN

        if self.AUTH_METHOD == CONST_DB_AUTH_METHOD.CERTIFICATE:
            if self.PRIVATE_KEY:
                args["private_key"] =            self.PRIVATE_KEY
            if self.PRIVATE_KEY_PATH:
                args["private_key_path"] =       self.PRIVATE_KEY_PATH
            if self.PRIVATE_KEY_PASSPHRASE:
                args["private_key_passphrase"] = self.PRIVATE_KEY_PASSPHRASE

        return {k: v for k, v in args.items() if v is not None}

    def get_post_connection_options(self) -> Dict[str, Any]:

        """Get connection arguments as dictionary"""
        ...
