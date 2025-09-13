import typing as t
import contextlib
import warnings
import ibis.backends.postgres as ir_backend
from pydantic_settings import BaseSettings


from mountainash_settings import SettingsParameters

from .base_ibis_operations import BaseIbisOperations
from ...constants import IBIS_DB_CONNECTION_MODE, CONST_DB_BACKEND
from ...settings import RedshiftAuthSettings


class Redshift_IbisOperations(BaseIbisOperations):


    #From BaseDBConnection
    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.REDSHIFT
