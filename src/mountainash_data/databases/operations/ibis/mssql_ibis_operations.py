



from .base_ibis_operations import BaseIbisOperations

from ...constants import CONST_DB_BACKEND


"""
Note: Requires sudo apt-get install unixodbc unixodbc-dev
"""

class MSSQL_IbisOperations(BaseIbisOperations):

    #From BaseDBConnection
    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.MSSQL
