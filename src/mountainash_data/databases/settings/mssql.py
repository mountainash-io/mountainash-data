#path: mountainash_settings/auth/database/providers/sql/mssql.py


from typing import Optional, List, Any, Dict, Tuple
from upath import UPath
from pydantic import Field, SecretStr, field_validator
from enum import Enum

from mountainash_settings import SettingsParameters

from .base import BaseDBAuthSettings
from ..constants import CONST_DB_PROVIDER_TYPE, CONST_DB_AUTH_METHOD
from .exceptions import DBAuthValidationError

class MSSQLAuthMethod(str, Enum):
    """MSSQL connection encryption settings"""
    WINDOWS = "windows"
    AZURE_AD = "azure_active_directory"
    PASSWORD = "password"


class MSSQLAuthEncryption(str, Enum):
    """MSSQL connection encryption settings"""
    DISABLED = "disabled"
    MANDATORY = "mandatory"
    STRICT = "strict"

class MSSQLAuthProtocol(str, Enum):
    """MSSQL connection protocol"""
    TCP = "tcp"
    NP = "np"  # Named Pipes
    SHARED_MEMORY = "sm"

class MSSQLDriverType(str, Enum):
    """MSSQL driver types"""
    ODBC = "ODBC Driver 18 for SQL Server"
    ODBC_17 = "ODBC Driver 17 for SQL Server"
    LEGACY = "SQL Server"

class MSSQLAuthSettings(BaseDBAuthSettings):
    """Microsoft SQL Server authentication settings

    https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15&tabs=alpine18-install%2Calpine17-install%2Cdebian8-install%2Credhat7-13-install%2Crhel7-offline
    """

    PROVIDER_TYPE: str = Field(default=CONST_DB_PROVIDER_TYPE.MSSQL)
    PORT: int = Field(default=1433)

    # Authentication Settings
    AUTH_METHOD: str = Field(default=CONST_DB_AUTH_METHOD.PASSWORD)  # password, windows, azure_active_directory
    WINDOWS_DOMAIN: Optional[str] = Field(default=None)
    AZURE_TENANT_ID: Optional[str] = Field(default=None)
    AZURE_CLIENT_ID: Optional[str] = Field(default=None)
    AZURE_CLIENT_SECRET: Optional[SecretStr] = Field(default=None)

    # Connection Settings
    DRIVER: str = Field(default=MSSQLDriverType.ODBC)
    PROTOCOL: str = Field(default=MSSQLAuthProtocol.TCP)
    APP_NAME: str = Field(default="MountainAsh")
    INSTANCE_NAME: Optional[str] = Field(default=None)
    MARS_ENABLED: bool = Field(default=False)

    # # Security Settings
    # ENCRYPTION: str = Field(default=MSSQLAuthEncryption.MANDATORY)
    # TRUST_SERVER_CERTIFICATE: bool = Field(default=False)
    # COLUMN_ENCRYPTION: bool = Field(default=False)
    # KEY_STORE_AUTHENTICATION: Optional[str] = Field(default=None)
    # KEY_STORE_PRINCIPAL_ID: Optional[str] = Field(default=None)
    # KEY_STORE_SECRET: Optional[SecretStr] = Field(default=None)

    # # Timeout Settings
    # LOGIN_TIMEOUT: int = Field(default=15)
    # CONNECTION_TIMEOUT: int = Field(default=30)
    # QUERY_TIMEOUT: Optional[int] = Field(default=None)

    # # Connection Pool Settings
    # POOL_SIZE: int = Field(default=5)
    # MIN_POOL_SIZE: Optional[int] = Field(default=None)
    # MAX_POOL_SIZE: Optional[int] = Field(default=None)
    # POOL_TIMEOUT: int = Field(default=30)

    # # Advanced Settings
    # PACKET_SIZE: Optional[int] = Field(default=4096)
    # AUTOCOMMIT: bool = Field(default=True)
    # ANSI_NULLS: bool = Field(default=True)
    # QUOTED_IDENTIFIER: bool = Field(default=True)
    # ISOLATION_LEVEL: Optional[str] = Field(default=None)

    # # Azure Settings
    # AZURE_MANAGED_IDENTITY: bool = Field(default=False)
    # AZURE_MSI_ENDPOINT: Optional[str] = Field(default=None)

    def __init__(self,
                 config_files: Optional[str|UPath|List[str|UPath]|Tuple[str|UPath]] = None,
                 settings_parameters:   Optional[SettingsParameters] = None,
                #  _dummy: Optional[bool] = False,
                 **kwargs) -> None:


        super().__init__(config_files=config_files,
                         settings_parameters=settings_parameters,
                        #  _dummy=_dummy,
                         **kwargs)



    ## Field Validators ##
    @field_validator("DRIVER")
    def validate_driver(cls, v: str) -> str:
        """Validate SQL Server driver"""
        try:
            return MSSQLDriverType(v)
        except ValueError:
            raise DBAuthValidationError(
                f"Invalid driver. Must be one of: {[e for e in MSSQLDriverType]}",
                provider=CONST_DB_PROVIDER_TYPE.MSSQL,
                validation_type="driver"
            )

    @field_validator("PROTOCOL")
    def validate_protocol(cls, v: str) -> str:
        """Validate connection protocol"""
        try:
            return MSSQLAuthProtocol(v)
        except ValueError:
            raise DBAuthValidationError(
                f"Invalid protocol. Must be one of: {[e for e in MSSQLAuthProtocol]}",
                provider=CONST_DB_PROVIDER_TYPE.MSSQL,
                validation_type="protocol"
            )

    # @field_validator("ENCRYPTION")
    # def validate_encryption(cls, v: str) -> str:
    #     """Validate encryption setting"""
    #     try:
    #         return MSSQLAuthEncryption(v)
    #     except ValueError:
    #         raise DBAuthValidationError(
    #             f"Invalid encryption setting. Must be one of: {[e for e in MSSQLAuthEncryption]}",
    #             provider=CONST_DB_PROVIDER_TYPE.MSSQL,
    #             validation_type="encryption"
    #         )

    # @field_validator("ISOLATION_LEVEL")
    # def validate_isolation_level(cls, v: Optional[str]) -> Optional[str]:
    #     """Validate isolation level"""
    #     if v is not None:
    #         valid_levels = {
    #             "READ UNCOMMITTED",
    #             "READ COMMITTED",
    #             "REPEATABLE READ",
    #             "SERIALIZABLE",
    #             "SNAPSHOT"
    #         }
    #         if v.upper() not in valid_levels:
    #             raise DBAuthValidationError(
    #                 f"Invalid isolation level. Must be one of: {valid_levels}",
    #                 provider=CONST_DB_PROVIDER_TYPE.MSSQL,
    #                 validation_type="isolation_level"
    #             )
    #     return v

    def _post_init(self, reinitialise: bool) -> None:
        pass
        """Initialize provider-specific settings"""
        # super()._init_provider_specific(reinitialise)

        # # Validate Windows Authentication
        # if self.AUTH_METHOD == "windows":
        #     if not self.WINDOWS_DOMAIN and not self.USERNAME:
        #         raise DBAuthConfigError(
        #             "Windows domain or username required for Windows authentication",
        #             provider=self.PROVIDER_TYPE
        #         )

        # # Validate Azure AD Authentication
        # elif self.AUTH_METHOD == "azure_active_directory":
        #     if self.AZURE_MANAGED_IDENTITY:
        #         if not self.AZURE_MSI_ENDPOINT:
        #             raise DBAuthConfigError(
        #                 "Azure MSI endpoint required for managed identity authentication",
        #                 provider=self.PROVIDER_TYPE
        #             )
        #     elif not (self.AZURE_CLIENT_ID and self.AZURE_CLIENT_SECRET and self.AZURE_TENANT_ID):
        #         raise DBAuthConfigError(
        #             "Azure client credentials required for Azure AD authentication",
        #             provider=self.PROVIDER_TYPE
        #         )

        # # Validate Column Encryption
        # if self.COLUMN_ENCRYPTION:
        #     if not self.KEY_STORE_AUTHENTICATION:
        #         raise DBAuthConfigError(
        #             "Key store authentication required for column encryption",
        #             provider=self.PROVIDER_TYPE
        #         )
        #     if self.KEY_STORE_AUTHENTICATION == "KeyVault" and not (
        #         self.KEY_STORE_PRINCIPAL_ID and self.KEY_STORE_SECRET
        #     ):
        #         raise DBAuthConfigError(
        #             "Key store principal ID and secret required for Azure Key Vault",
        #             provider=self.PROVIDER_TYPE
        #         )

    def get_connection_string_template(self) -> str:

        template =  "mssql://"

        # Add authentication
        if self.AUTH_METHOD == "windows":
            if self.WINDOWS_DOMAIN:
                template += "{windows_domain}\\{username}@{host}"
            else:
                template += "{username}@{host}"

        elif self.AUTH_METHOD == "azure_active_directory":
            template += "{username}@{host}"
        else:
            template += "{username}:{password}@{host}"

        # Add port and database
        if self.INSTANCE_NAME:
            template += "\\{instance_name}"
        else:
            template += ":{port}"

        template += "/{database}"


        return template



    # def get_connection_string_params(self) -> Dict:

    #     params = {}
    #     params['database'] = self.DATABASE

    #     if self.TOKEN is not None:
    #         params['token'] = self.TOKEN

    #     # Add driver and parameters
    #     # params = ["driver={driver}"]


    #     return params



    def get_connection_string(self, scheme: str) -> str:
        """Generate MSSQL connection string"""
        # Base connection string
        # template = "mssql://"
        template = f"{scheme}"

        # Add authentication
        if self.AUTH_METHOD == "windows":
            if self.WINDOWS_DOMAIN:
                template += "{windows_domain}\\{username}@{host}"
            else:
                template += "{username}@{host}"
        elif self.AUTH_METHOD == "azure_active_directory":
            template += "{username}@{host}"
        else:
            template += "{username}:{password}@{host}"

        # Add port and database
        if self.INSTANCE_NAME:
            template += "\\{instance_name}"
        else:
            template += ":{port}"
        template += "/{database}"

        # Add driver and parameters
        # params = [f"driver={self.DRIVER}"]

        # Add encryption settings
        # if self.ENCRYPTION != MSSQLAuthEncryption.DISABLED:
        #     params.append(f"encrypt={self.ENCRYPTION}")
        #     if self.TRUST_SERVER_CERTIFICATE:
        #         params.append("TrustServerCertificate=yes")

        # Add connection settings
        # params.extend([
        #     # f"application_name={self.APP_NAME}",
        #     # f"login_timeout={self.LOGIN_TIMEOUT}",
        #     # f"connection_timeout={self.CONNECTION_TIMEOUT}"
        # ])

        # if self.MARS_ENABLED:
        #     params.append("MARS_Connection=yes")

        # # Add column encryption
        # if self.COLUMN_ENCRYPTION:
        #     params.append("ColumnEncryption=Enabled")
        #     if self.KEY_STORE_AUTHENTICATION:
        #         params.append(f"KeyStoreAuthentication={self.KEY_STORE_AUTHENTICATION}")
        #     if self.KEY_STORE_PRINCIPAL_ID:
        #         params.append(f"KeyStorePrincipalId={self.KEY_STORE_PRINCIPAL_ID}")

        # # Add other settings
        # if self.PACKET_SIZE:
        #     params.append(f"packet_size={self.PACKET_SIZE}")
        # if self.ISOLATION_LEVEL:
        #     params.append(f"isolation_level={self.ISOLATION_LEVEL}")

        # template += "?" + "&".join(params)
        return self.format_connection_string(template)

    def get_connection_string_params(self) -> Dict[str, Any]:
        """Get connection arguments for MSSQL"""
        args = {
            "driver": self.DRIVER,
            "host": self.HOST,
            "database": self.DATABASE,
            "port": self.PORT,
            # "schema": self.SCHEMA,
            # "application_name": self.APP_NAME,
            # "autocommit": self.AUTOCOMMIT,
            # "login_timeout": self.LOGIN_TIMEOUT,
            # "timeout": self.CONNECTION_TIMEOUT,
        }

        # Add authentication
        if self.AUTH_METHOD == "windows":
            args["trusted_connection"] = "yes"
            if self.WINDOWS_DOMAIN:
                args["username"] = f"{self.WINDOWS_DOMAIN}\\{self.USERNAME}"
            else:
                args["username"] = self.USERNAME
        elif self.AUTH_METHOD == "azure_active_directory":
            if self.AZURE_MANAGED_IDENTITY:
                args["authentication"] = "ActiveDirectoryMsi"
                if self.AZURE_MSI_ENDPOINT:
                    args["msi_endpoint"] = self.AZURE_MSI_ENDPOINT
            else:
                args.update({
                    "authentication": "ActiveDirectoryServicePrincipal",
                    "user_id": self.AZURE_CLIENT_ID,
                    "password": self.AZURE_CLIENT_SECRET if self.AZURE_CLIENT_SECRET else None,
                    "tenant_id": self.AZURE_TENANT_ID
                })
        else:
            args.update({
                "username": self.USERNAME,
                "password": self.PASSWORD if self.PASSWORD else None
            })

        # Add instance/port
        if self.INSTANCE_NAME:
            args["server"] += f"\\{self.INSTANCE_NAME}"
        else:
            args["port"] = self.PORT

        # # Add encryption settings
        # if self.ENCRYPTION != MSSQLAuthEncryption.DISABLED:
        #     args["encrypt"] = self.ENCRYPTION
        #     args["trust_server_certificate"] = self.TRUST_SERVER_CERTIFICATE

        # # Add column encryption
        # if self.COLUMN_ENCRYPTION:
        #     args.update({
        #         "column_encryption": "enabled",
        #         "key_store_authentication": self.KEY_STORE_AUTHENTICATION,
        #         "key_store_principal_id": self.KEY_STORE_PRINCIPAL_ID,
        #         "key_store_secret": (
        #             self.KEY_STORE_SECRET
        #             if self.KEY_STORE_SECRET else None
        #         )
        #     })

        # # Add other settings
        # if self.MARS_ENABLED:
        #     args["mars_connection"] = "yes"
        # if self.PACKET_SIZE:
        #     args["packet_size"] = self.PACKET_SIZE
        # if self.ISOLATION_LEVEL:
        #     args["isolation_level"] = self.ISOLATION_LEVEL
        # if self.QUERY_TIMEOUT:
        #     args["query_timeout"] = self.QUERY_TIMEOUT

        return {k: v for k, v in args.items() if v is not None}

    def get_connection_kwargs(self, db_abstraction_layer: Optional[str] = None) -> Dict[str, Any]:
        """Get connection arguments for MSSQL"""
        return {}

    def get_post_connection_options(self, db_abstraction_layer: Optional[str] = None) -> Dict[str, Any]:

        """Get connection arguments as dictionary"""
        ...
