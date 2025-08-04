#path: mountainash_settings/auth/database/providers/file/sqlite.py

from typing import Optional, List, Any, Dict, Tuple
from upath import UPath

from pydantic import Field

from mountainash_settings import SettingsParameters

from .base import BaseDBAuthSettings
from ..constants import CONST_DB_PROVIDER_TYPE


class TrinoAuthSettings(BaseDBAuthSettings):
    """ Trino authentication settings

    Extra connection settings: https://github.com/trinodb/trino-python-client/blob/master/trino/dbapi.py

    """

    PROVIDER_TYPE: str = Field(default=CONST_DB_PROVIDER_TYPE.TRINO)
    AUTH_METHOD: str = Field(default=None)  # Trino supports "password" or None

    SOURCE: Optional[str] =             Field(default=None, alias="source")
    CATALOG: Optional[str] =            Field(default=None, alias="catalog")
    SCHEMA: Optional[str] =             Field(default=None, alias="schema")
    SESSION_PROPERTIES: Optional[str] = Field(default=None, alias="session_properties")

    #Client Session Params
    HTTP_HEADERS: Optional[str] =       Field(default=None, alias="http_headers")
    HTTP_SCHEME: Optional[str] =        Field(default="https", alias="http_scheme")
    HTTP_SESSION: Optional[str] =       Field(default=None, alias="http_session")
    AUTH: Optional[str] =               Field(default=None, alias="auth")
    EXTRA_CREDENTIAL: Optional[str] =   Field(default=None, alias="extra_credential")
    MAX_ATTEMPTS: Optional[int] =       Field(default=None, alias="max_attempts")
    REQUEST_TIMEOUT: Optional[int] =    Field(default=None, alias="request_timeout")
    ISOLATION_LEVEL: Optional[str] =    Field(default=None, alias="isolation_level")
    VERIFY: Optional[bool] =            Field(default=True, alias="verify")
    CLIENT_TAGS: Optional[str] =        Field(default=None, alias="client_tags")
    LEGACY_PRIMITIVE_TYPES: Optional[bool] =    Field(default=False, alias="legacy_primitive_types")
    LEGACY_PREPARED_STATEMENTS: Optional[str] = Field(default=None, alias="legacy_prepared_statements")
    ROLES: Optional[str] =              Field(default=None, alias="roles")
    TIMEZONE: Optional[str] =           Field(default=None, alias="timezone")




    def __init__(self,
                 config_files: Optional[str|UPath|List[str|UPath]|Tuple[str|UPath]] = None,
                 settings_parameters:   Optional[SettingsParameters] = None,
                #  _dummy: Optional[bool] = False,
                 **kwargs) -> None:


        super().__init__(config_files=config_files,
                         settings_parameters=settings_parameters,
                        #  _dummy=_dummy,
                         **kwargs)


    def _post_init(self, reinitialise: bool) -> None:
        pass

    def get_connection_string_template(self, scheme: Optional[str] = None) -> str:

        #ibis.connect(f"trino://user@localhost:8080/{catalog}/{schema}")

        """Generate Trino connection string"""
        template =  f"{scheme}"

        if self.USERNAME is not None:
            template += "{user}"
        if self.HOST is not None:
            template += "@{host}"
        if self.PORT is not None:
            template += ":{port}"
        if self.CATALOG is not None:
            template += "/{catalog}"
        if self.SCHEMA is not None:
            template += "/{schema}"

     #   "trino://user@localhost:8080/{catalog}/{schema}"

        return template

    def get_connection_string_params(self) -> Dict[str, Any]:
        """Get connection arguments for Trino"""

        args = {}
        if self.USERNAME is not None:
            args["user"] =      self.USERNAME
        if self.HOST is not None:
            args["host"] =      self.HOST
        if self.PORT is not None:
            args["port"] =      str(self.PORT)
        if self.CATALOG is not None:
            args["catalog"] =  self.CATALOG
        if self.SCHEMA is not None:
            args["schema"] =  self.SCHEMA

        return args


    def get_connection_kwargs(self, db_abstraction_layer: Optional[str] = None) -> Dict[str, Any]:
        """Get connection arguments for SQLite"""

        kwargs = {}

        if self.SOURCE:
            kwargs["source"] =  self.SOURCE
        if self.HTTP_SCHEME:
            kwargs["http_scheme"] =  self.HTTP_SCHEME
        if self.AUTH_METHOD == "password" and self.PASSWORD:
            kwargs["password"] = self.PASSWORD

        return kwargs

    def get_post_connection_options(self, db_abstraction_layer: Optional[str] = None) -> Dict[str, Any]:

        """Get connection arguments as dictionary"""
        ...
