import typing as t
import contextlib
import warnings

from pydantic_settings import BaseSettings
import ibis.backends.mssql as ir_backend


from mountainash_settings import SettingsParameters

from .base_ibis_operations import BaseIbisOperations

from ...constants import IBIS_DB_CONNECTION_MODE, CONST_DB_BACKEND
from ...settings import MSSQLAuthSettings


"""
Note: Requires sudo apt-get install unixodbc unixodbc-dev
"""

class MSSQL_IbisOperations(BaseIbisOperations):

    #From BaseDBConnection
    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.MSSQL
