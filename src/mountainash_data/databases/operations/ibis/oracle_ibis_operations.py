

from .base_ibis_operations import BaseIbisOperations
from ...constants import CONST_DB_BACKEND


class Oracle_IbisOperations(BaseIbisOperations):


    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.ORACLE
