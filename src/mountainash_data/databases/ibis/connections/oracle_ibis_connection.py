import typing as t
import ibis.backends.oracle as ir_backend
from pydantic_settings import BaseSettings


from mountainash_settings import SettingsParameters

from ..base_ibis_connection import BaseIbisConnection
from ...constants import IBIS_DB_CONNECTION_MODE, CONST_DB_BACKEND


class Oracle_IbisConnection(BaseIbisConnection):


    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None
                 ):

        self._ibis_backend: t.Optional[ir_backend.Backend] = None
        self._ibis_connection_mode: str = connection_mode if connection_mode is not None else IBIS_DB_CONNECTION_MODE.CONNECTION_STRING

        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)

    #From BaseIbisConnection
    @property
    def ibis_backend(self) -> t.Optional[ir_backend.Backend]:
        return self._ibis_backend

    @property
    def ibis_connection_mode(self) -> str:
        return self._ibis_connection_mode

    #From BaseDBConnection
    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.ORACLE

    @property
    def connection_string_scheme(self) -> str:
        return "oracle://"

    @property
    def settings_class(self) -> t.Type[BaseSettings]:
        return None #OracleAuthSettings
