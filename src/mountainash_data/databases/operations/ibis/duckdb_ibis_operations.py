import typing as t
import contextlib
import warnings

import ibis.backends.duckdb as ir_backend
from pydantic_settings import BaseSettings

from .base_ibis_operations import BaseIbisOperations

from ...constants import IBIS_DB_CONNECTION_MODE, CONST_DB_BACKEND
from ...settings import DuckDBAuthSettings

from mountainash_settings import SettingsParameters, get_settings



class DuckDB_IbisOperations(BaseIbisOperations):

    #From BaseDBConnection
    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.DUCKDB
