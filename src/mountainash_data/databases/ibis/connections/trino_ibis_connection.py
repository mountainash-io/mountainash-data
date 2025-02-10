import typing as t
import ibis as ibis
from ibis.backends.sql import SQLBackend
import ibis.backends.trino as ir_backend
import contextlib
import warnings
from ..constants import IBIS_DB_connection_mode
from ..base_ibis_connection import BaseIbisConnection
from mountainash_constants import CONST_DB_ABSTRACTION_LAYER, CONST_DB_BACKEND
from mountainash_settings import SettingsParameters
from mountainash_settings.settings.auth.database import TrinoAuthSettings
from pydantic_settings import BaseSettings

class Trino_IbisConnection(BaseIbisConnection):

 

    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None
                 ):


        #From BaseIbisConnection
        self._ibis_backend: t.Optional[ir_backend.Backend] = None
        self._ibis_connection_mode: str = connection_mode if connection_mode is not None else IBIS_DB_connection_mode.HYBRID

        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters, 
                         )
   
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
        return CONST_DB_BACKEND.TRINO.value

    @property
    def connection_string_scheme(self) -> str:
        return "trino://"

    @property
    def settings_class(self) -> t.Type[BaseSettings]:
        return TrinoAuthSettings


    # def _connect(self, connection_string: t.Optional[str] = None, **kwargs) -> SQLBackend:
    #     """
    #     Default Implementation to connect to the database using the provided connection string.
    #     Over-ride in subclasses if a different implementation is necessary."""

    #     if self.ibis_connection_mode == IBIS_DB_connection_mode.CONNECTION_STRING:
    #         self.ibis_backend: SQLBackend = ibis.trino.connect(connection_string)

    #     elif self.ibis_connection_mode == IBIS_DB_connection_mode.KWARGS:
    #         self.ibis_backend: SQLBackend = ibis.trino.connect(**kwargs)
    
    #     return self.ibis_backend

    def _list_tables(self,                
                like: str | None = None,
                database: tuple[str, str] | str | None = None,
                schema: str | None = None
                    ) -> t.List[str]:

        return self.ibis_backend.list_tables(like=like, database=database, schema=schema) if self.ibis_backend is not None else []
    

    def set_post_connection_options(self, post_connection_options: t.Dict[str, t.Any]):

        if self.ibis_backend is not None:
            with contextlib.closing(self.ibis_backend.con.cursor()) as cur:
                for option_key, option_value in post_connection_options.items():
                    try:
                        cur.execute(f"SET @@session.{option_key} = '{option_value}'")
                    except Exception as e:
                        warnings.warn(f"Unable to set session {option_key} to UTC: {e}")
