

# from mountainash_data.databases.settings import BigQueryAuthSettings


from .base_ibis_operations import BaseIbisOperations
from ...constants import CONST_DB_BACKEND




class BigQuery_IbisConnection(BaseIbisOperations):

    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.BIGQUERY
