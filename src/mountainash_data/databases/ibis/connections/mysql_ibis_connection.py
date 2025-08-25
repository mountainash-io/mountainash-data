import typing  as t
import contextlib
import warnings

from pydantic_settings import BaseSettings
import ibis.backends.mysql as ir_backend

from mountainash_settings import SettingsParameters

from ..base_ibis_connection import BaseIbisConnection
from ...constants import IBIS_DB_CONNECTION_MODE, CONST_DB_BACKEND
from ...settings import MySQLAuthSettings



class MySQL_IbisConnection(BaseIbisConnection):

    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None
                 ):

        self._ibis_backend: t.Optional[ir_backend.Backend] = None
        self._ibis_connection_mode =               connection_mode if connection_mode is not None else IBIS_DB_CONNECTION_MODE.CONNECTION_STRING


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
        return CONST_DB_BACKEND.MYSQL

    @property
    def connection_string_scheme(self) -> str:
        return "mysql://"

    @property
    def settings_class(self) -> t.Type[BaseSettings]:
        return MySQLAuthSettings


    # def connect(self,
    #             connection_string: t.Optional[str] = None,
    #             connection_args: t.Optional[t.Dict] = None,
    #             connection_mode: t.Optional[str] = None,
    #             post_connection_options: t.Optional[t.Dict] = None,
    #             ):

    #     super().connect(connection_string=connection_string,
    #                     connection_args=connection_args,
    #                     connection_mode=connection_mode
    #                     )
    #     #Run post connection settings, if set
    #     if not post_connection_options:
    #         post_connection_options = self.get_post_connection_options()

    #     self.set_post_connection_options(post_connection_options)

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
