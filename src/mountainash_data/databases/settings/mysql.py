#path: mountainash_settings/auth/database/providers/sql/mysql.py


from typing import Optional, List, Any, Dict, Tuple, Self
from upath import UPath
from pydantic import Field, field_validator, model_validator

from mountainash_settings import SettingsParameters

from .base import BaseDBAuthSettings
from ..constants import CONST_DB_PROVIDER_TYPE, CONST_DB_AUTH_METHOD, CONST_DB_SSL_MODE_MYSQL


class MySQLAuthSettings(BaseDBAuthSettings):
    """MySQL authentication settings

    All parameters supported are here: https://mysqlclient.readthedocs.io/user_guide.html#functions-and-attributes
    former SSL parameters defined here: https://dev.mysql.com/doc/c-api/8.4/en/mysql-ssl-set.html

    New options are defined here https://dev.mysql.com/doc/c-api/8.4/en/mysql-options.html
    """

    # PROVIDER_TYPE: str = Field(default=CONST_DB_PROVIDER_TYPE.MYSQL)
    PORT: Optional[int] = Field(default=3306)

    # MySQL-specific Settings
    CHARSET: str = Field(default="utf8mb4")
    COLLATION: str = Field(default="utf8mb4_unicode_ci")
    AUTOCOMMIT: bool = Field(default=True)

    #Type Conversions
    CONV: Dict  = Field(default=None)

    # Connection Security Settings
    # ALLOW_LOCAL_INFILE: bool = Field(default=False)
    SSL_MODE: str = Field(default=None)
    SSL_KEY: Optional[str] = Field(default=None)
    SSL_CERT: Optional[str] = Field(default=None)
    SSL_CA: Optional[str] = Field(default=None)
    SSL_CAPATH: Optional[str] = Field(default=None)
    SSL_CIPHER: Optional[str] = Field(default=None)

    # SSL_CIPHER: Optional[str] = Field(default=None)
    # TLS_VERSION: Optional[List[str]] = Field(default=["TLSv1.2", "TLSv1.3"])

    # # Connection Settings
    # CONNECT_TIMEOUT: int = Field(default=10)
    # READ_TIMEOUT: Optional[int] = Field(default=None)
    # WRITE_TIMEOUT: Optional[int] = Field(default=None)
    # MAX_ALLOWED_PACKET: Optional[int] = Field(default=None)

    # # Compression Settings
    # COMPRESSION: bool = Field(default=False)
    # COMPRESSION_LEVEL: Optional[int] = Field(default=None)

    # # Client Settings
    # PROGRAM_NAME: Optional[str] = Field(default="MountainAsh")
    # CLIENT_FLAG: Optional[int] = Field(default=None)

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
        return CONST_DB_PROVIDER_TYPE.BIGQUERY

    @field_validator("CHARSET")
    @classmethod
    def validate_charset(cls, value: Optional[str]) -> Optional[str]:
        """Validate CHARSET"""

        valid_charsets = {
            "utf8mb4", "utf8mb3", "utf8", "latin1",
            "ascii", "binary", "cp1251", "latin2"
        }

        precondition: bool = value is not None
        test: bool = value in valid_charsets
        valid: bool = (not precondition) | test

        if not valid:
            raise ValueError(f"Invalid charset. Must be one of: {valid_charsets}")

        return value


    @field_validator("SSL_MODE")
    @classmethod
    def validate_ssl_mode(cls, value: Optional[str]) -> Optional[str]:
        """Validate CHARSET"""

        valid_values = CONST_DB_SSL_MODE_MYSQL.__dict__

        precondition: bool = value is not None
        test: bool = value in CONST_DB_SSL_MODE_MYSQL.__dict__
        valid: bool = (not precondition) | test

        if not valid:
            raise ValueError(f"Invalid SSL_MODE. Must be one of: {valid_values}")

        return value

    #Multi Field Validators
    @model_validator(mode='after')
    def validate_token_set(self) -> Self:

        precondition: bool = self.SSL_MODE in {CONST_DB_SSL_MODE_MYSQL.VERIFY_CA, CONST_DB_SSL_MODE_MYSQL.VERIFY_FULL}
        test: bool =  self.SSL_CA is not None
        valid: bool = (not precondition) | test

        if not valid:
            raise ValueError(f"SSL_CA required if SSL_MODE in {CONST_DB_SSL_MODE_MYSQL.VERIFY_CA, CONST_DB_SSL_MODE_MYSQL.VERIFY_FULL}")

        return self


    # @model_validator(mode='after')
    # def validate_auth_ssl_ca(self) -> Self:

    #     precondition: bool = self.SSL_MODE is not None and (self.SSL_VERIFY is not None or self.SSL_CA is not None)
    #     test: bool =  self.SSL_VERIFY is not None and self.SSL_CA is not None
    #     valid: bool = (not precondition) | test

    #     if not valid:
    #         raise ValueError(f"SSL_VERIFY both SSL_CA required if SSL_ENABLED for CA")

    #     return self

    @model_validator(mode='after')
    def validate_auth_ssl_cert(self) -> Self:

        precondition: bool = self.SSL_MODE is not None and (self.SSL_CERT is not None or self.SSL_KEY is not None)
        test: bool =  self.SSL_CERT is not None and self.SSL_KEY is not None
        valid: bool = (not precondition) | test

        if not valid:
            raise ValueError("SSL_CERT both SSL_KEY required if SSL_ENABLED for certificate and key")

        return self


    def _post_init(self, reinitialise: bool) -> None:
        """Initialize provider-specific settings"""
        ...

    def get_connection_string_template(self, scheme: Optional[str] = None) -> str:

        template = f"{scheme}"

        if self.AUTH_METHOD == CONST_DB_AUTH_METHOD.PASSWORD:

            template += "{user}"

            if self.PASSWORD is not None:
                template += ":{password}"

            template += "@{host}:{port}"

            if self.DATABASE is not None:
                template += "/{database}"

        return template

    def get_connection_string_params(self) -> Dict[str, Any]:

        params = {}

        if self.AUTH_METHOD == CONST_DB_AUTH_METHOD.PASSWORD:

            if self.USERNAME is not None:
                params['user'] =     self.USERNAME
            if self.PASSWORD is not None:
                params['password'] = self.PASSWORD
            if self.HOST is not None:
                params['host'] =     self.HOST
            if self.PORT is not None:
                params['port'] =     self.PORT
            if self.DATABASE is not None:
                params['database'] = self.DATABASE

        return params



    def get_connection_kwargs(self) -> Dict[str, Any]:
        """Get connection arguments for MySQL"""

        args = {}
        if self.CHARSET:
            args["charset"] =  self.CHARSET
        if self.COLLATION:
            args["collation"] =  self.COLLATION
        if self.AUTOCOMMIT:
            args["autocommit"] =  self.AUTOCOMMIT

        if self.SSL_MODE != CONST_DB_SSL_MODE_MYSQL.DISABLED:

            args["ssl_mode"] =  self.SSL_MODE

            ssl = {}

            if self.SSL_KEY:
                ssl["ssl-key"] = self.SSL_KEY
            if self.SSL_CERT:
                ssl["ssl-cert"] = self.SSL_CERT
            if self.SSL_CA:
                ssl["ssl-ca"] = self.SSL_CA
            if self.SSL_CA:
                ssl["ssl-capath"] = self.SSL_CAPATH
            if self.SSL_CIPHER:
                ssl["ssl-cipher"] = self.SSL_CIPHER
            if ssl:
                args["ssl"] = ssl


        # Add MySQL-specific arguments
        # args.update({
        #     "charset": self.CHARSET,
        #     "autocommit": self.AUTOCOMMIT,
        #     # "connect_timeout": self.CONNECT_TIMEOUT,
        #     # "program_name": self.PROGRAM_NAME
        # })

        # # Add optional arguments
        # if self.READ_TIMEOUT:
        #     args["read_timeout"] = self.READ_TIMEOUT
        # if self.WRITE_TIMEOUT:
        #     args["write_timeout"] = self.WRITE_TIMEOUT
        # if self.MAX_ALLOWED_PACKET:
        #     args["max_allowed_packet"] = self.MAX_ALLOWED_PACKET
        # if self.CLIENT_FLAG:
        #     args["client_flag"] = self.CLIENT_FLAG
        # if self.COMPRESSION:
        #     args["compression"] = True
        #     if self.COMPRESSION_LEVEL:
        #         args["compression_level"] = self.COMPRESSION_LEVEL



        return args


    def get_post_connection_options(self) -> Dict[str, Any]:

        """Get connection arguments as dictionary"""
        options = {}

        return options
