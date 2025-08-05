import typing as t

import ibis.backends.bigquery as ir_backend

from mountainash_settings import SettingsParameters
# from mountainash_data.databases.settings import BigQueryAuthSettings


from ..base_ibis_connection import BaseIbisConnection
from ...constants import IBIS_DB_connection_mode, CONST_DB_BACKEND
from ...settings import BigQueryAuthSettings


from google.oauth2 import service_account
import contextlib
import warnings
from pydantic_settings import BaseSettings


class BigQuery_IbisConnection(BaseIbisConnection):


    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None
                 ):

        self._ibis_backend: t.Optional[ir_backend.Backend] = None
        self._ibis_connection_mode: str = connection_mode if connection_mode is not None else IBIS_DB_connection_mode.KWARGS

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
        return CONST_DB_BACKEND.BIGQUERY

    @property
    def connection_string_scheme(self) -> str:
        return "bigquery://"

    @property
    def settings_class(self) -> t.Type[BaseSettings]:
        return BigQueryAuthSettings



    def _connect(self,
                    connection_string: t.Optional[str] = None,
                    **kwargs
                    ):

        # credentials = kwargs.get('credentials', None)
        credentials_info = kwargs.get('credentials_info', None)
        dataset_id = kwargs.get('dataset_id', None)
        project_id = kwargs.get('project_id', None)

        if credentials_info:
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            ibis_backend = ir_backend.connect(dataset_id=dataset_id, credentials=credentials)
        else:
            ibis_backend = ir_backend.connect(project_id=project_id, dataset_id=dataset_id)

        return ibis_backend


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
