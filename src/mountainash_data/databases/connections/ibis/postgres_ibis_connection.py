import typing as t
import contextlib
import warnings


import ibis.backends.postgres as ir_backend
from pydantic_settings import BaseSettings

from mountainash_settings import SettingsParameters

from .base_ibis_connection import BaseIbisConnection
from ...constants import IBIS_DB_CONNECTION_MODE, CONST_DB_BACKEND, CONST_DB_PROVIDER_TYPE
from ...settings import PostgreSQLAuthSettings

class Postgres_IbisConnection(BaseIbisConnection):


    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None
                #  ssh_auth_settings_parameters: Optional[SettingsParameters] = None,
                #  connection_string: Optional[str] = None
                 ):



        self._ibis_backend: t.Optional[ir_backend.Backend] = None
        self._ibis_connection_mode: str =  connection_mode if connection_mode is not None else IBIS_DB_CONNECTION_MODE.CONNECTION_STRING

        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)

    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        """Database provider identifier."""
        return CONST_DB_PROVIDER_TYPE.POSTGRESQL

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
        return CONST_DB_BACKEND.POSTGRES

    @property
    def connection_string_scheme(self) -> str:
        return "postgres://"

    @property
    def settings_class(self) -> t.Type[BaseSettings]:
        return PostgreSQLAuthSettings




    # def _connect(self, connection_string: t.Optional[str] = None, **kwargs) -> SQLBackend:
    #     """
    #     Default Implementation to connect to the database using the provided connection string.
    #     Over-ride in subclasses if a different implementation is necessary."""

    #     if self.connection_mode == IBIS_DB_CONNECTION_MODE.CONNECTION_STRING:
    #         ibis_backend: SQLBackend = ibis.connect(connection_string)

    #     if self.connection_mode == IBIS_DB_CONNECTION_MODE.KWARGS:
    #         ibis_backend: SQLBackend = ibis.postgres.connect(**kwargs)

    #     return ibis_backend




    # def _list_tables(self,
    #             like: str | None = None,
    #             database: tuple[str, str] | str | None = None,
    #             schema: str | None = None
    #                 ) -> t.List[str]:

    #     return self.ibis_backend.list_tables(like=like, database=database, schema=schema) if self.ibis_backend is not None else []

    def set_post_connection_options(self, post_connection_options: t.Dict[str, t.Any]):

        if self.ibis_backend is not None:
            with contextlib.closing(self.ibis_backend.con.cursor()) as cur:
                for option_key, option_value in post_connection_options.items():
                    try:
                        cur.execute(f"SET @@session.{option_key} = '{option_value}'")
                    except Exception as e:
                        warnings.warn(f"Unable to set session {option_key} to UTC: {e}")
