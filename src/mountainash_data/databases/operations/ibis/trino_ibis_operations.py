import typing as t
import contextlib
import warnings


import ibis as ibis
import ibis.backends.trino as ir_backend
from pydantic_settings import BaseSettings


from mountainash_settings import SettingsParameters

from .base_ibis_operations import BaseIbisOperations
from ...constants import IBIS_DB_CONNECTION_MODE, CONST_DB_BACKEND
from ...settings import TrinoAuthSettings


class Trino_IbisOperations(BaseIbisOperations):



    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None
                 ):


        #From BaseIbisConnection
        self._ibis_backend: t.Optional[ir_backend.Backend] = None
        self._ibis_connection_mode: str = connection_mode if connection_mode is not None else IBIS_DB_CONNECTION_MODE.HYBRID

        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters,
                         )

    #From BaseDBConnection
    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.TRINO
