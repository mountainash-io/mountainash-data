import typing as t
import ibis.backends.pyspark as ir_backend
import contextlib
import warnings
from pydantic_settings import BaseSettings


from mountainash_settings import SettingsParameters

from .base_ibis_operations import BaseIbisOperations
from ...constants import IBIS_DB_CONNECTION_MODE, CONST_DB_BACKEND
from ...settings import PySparkAuthSettings


class PySpark_IbisOperations(BaseIbisOperations):


    #From BaseDBConnection
    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.PYSPARK
