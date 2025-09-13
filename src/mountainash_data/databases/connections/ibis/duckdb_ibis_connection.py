import typing as t
import contextlib
import warnings

import ibis.backends.duckdb as ir_backend
from pydantic_settings import BaseSettings


from .base_ibis_connection import BaseIbisConnection
from ...constants import IBIS_DB_CONNECTION_MODE, CONST_DB_BACKEND, CONST_DB_PROVIDER_TYPE
from ...settings import DuckDBAuthSettings

from mountainash_settings import SettingsParameters, get_settings



class DuckDB_IbisConnection(BaseIbisConnection):

    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None,
                 read_only: t.Optional[bool] = None
                 ):


        self._ibis_backend: t.Optional[ir_backend.Backend] = None
        self._ibis_connection_mode: str = connection_mode if connection_mode is not None else IBIS_DB_CONNECTION_MODE.CONNECTION_STRING
        self._read_only = read_only

        super().__init__( db_auth_settings_parameters=db_auth_settings_parameters )


    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        """Database provider identifier."""
        return CONST_DB_PROVIDER_TYPE.MOTHERDUCK



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
        return CONST_DB_BACKEND.DUCKDB

    @property
    def connection_string_scheme(self) -> str:
        return "duckdb://"

    @property
    def settings_class(self) -> t.Type[BaseSettings]:
        return DuckDBAuthSettings

    # ==============
    #
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


    def _connect(self,
                 connection_string: t.Optional[str] = None,
                 connection_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
                 **kwargs
                 ) -> ir_backend.Backend:

        if connection_kwargs is None:
            connection_kwargs = {}

        if kwargs is None:
            kwargs = {}

        settings: DuckDBAuthSettings = DuckDBAuthSettings.get_settings(self.db_auth_settings_parameters)

        if settings.DATABASE is None:
            kwargs["read_only"] = False
        else:

            # Resolve Read Only flag, defaulting to the least permissive (True) if both set
            if connection_kwargs.get("read_only") is not None and self._read_only is not None:
                # If both are set, use the most restrictive (True) value
                kwargs["read_only"] = connection_kwargs["read_only"] or self._read_only
            elif self._read_only is not None:
                # Use instance setting if only it exists
                kwargs["read_only"] = self._read_only
            elif connection_kwargs.get("read_only") is None:
                # Default to False if neither is set
                kwargs["read_only"] = False

        #combine connection_kwargs and kwargs
        connection_kwargs = {**connection_kwargs, **kwargs}


        return super()._connect(connection_string, connection_kwargs)

    def disconnect(self):
        """Close the DuckDB connection, ensuring the underlying connection is properly closed."""
        if self.ibis_backend is not None:
            try:
                # Close the underlying DuckDB connection first
                if hasattr(self.ibis_backend, 'con'):
                    try:
                        # Force close any open cursors
                        if hasattr(self.ibis_backend.con, '_cursors'):
                            for cursor in list(self.ibis_backend.con._cursors):
                                try:
                                    cursor.close()
                                except Exception:
                                    pass
                        # Close the connection
                        self.ibis_backend.con.close()
                    except Exception as e:
                        print(f"Warning: Error closing DuckDB connection: {str(e)}")
            except Exception as e:
                print(f"Warning: Error during DuckDB disconnect: {str(e)}")
            finally:
                # Then handle the ibis cleanup
                try:
                    super().disconnect()
                except Exception as e:
                    print(f"Warning: Error during Ibis backend disconnect: {str(e)}")
                    # Force cleanup
                    self._ibis_backend = None
