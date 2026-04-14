#path: mountainash_settings/auth/database/providers/file/sqlite.py

from typing import Optional, List, Any, Dict, Tuple
from upath import UPath

from pydantic import Field

from mountainash_settings import SettingsParameters

from .base import BaseDBAuthSettings
from ..constants import CONST_DB_PROVIDER_TYPE


class SQLiteAuthSettings(BaseDBAuthSettings):
    """ SQLite authentication settings

        SQLite Prgamas: https://www.sqlite.org/pragma.html
    """

    # PROVIDER_TYPE: str = Field(default=CONST_DB_PROVIDER_TYPE.SQLITE)
    AUTH_METHOD: str = Field(default="none")  # SQLite uses file-based authentication

    # File Settings
    TYPE_MAP: Optional[Dict[str, Any]] = Field(default=None)  # Custom type mapping

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

    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        """Database provider identifier."""
        return CONST_DB_PROVIDER_TYPE.SQLITE


    def get_connection_string_template(self, scheme: Optional[str] = None) -> str:

        """Generate SQLite connection string"""
        template =  f"{scheme}"

        if self.DATABASE is not None:
            template += "{database}"

        return template

    def get_connection_string_params(self) -> Dict[str, Any]:
        """Get connection arguments for SQLite"""

        args = {}

        if self.DATABASE is not None:
            args["database"] = UPath(self.DATABASE).expanduser()

        return args


    def get_connection_kwargs(self) -> Dict[str, Any]:
        """Get connection arguments for SQLite"""

        kwargs = {}

        if self.TYPE_MAP:
            kwargs["type_map"] = self.TYPE_MAP

        return kwargs

    def get_post_connection_options(self) -> Dict[str, Any]:

        """Get connection arguments as dictionary"""
        ...
