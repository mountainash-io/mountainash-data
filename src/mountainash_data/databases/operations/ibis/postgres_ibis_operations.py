



from .base_ibis_operations import BaseIbisOperations
from ...constants import CONST_DB_BACKEND

class Postgres_IbisOperations(BaseIbisOperations):


    #From BaseDBConnection
    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.POSTGRES
