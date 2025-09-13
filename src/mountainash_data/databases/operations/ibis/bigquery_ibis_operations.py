import typing as t

import ibis.backends.bigquery as ir_backend
import ibis

from mountainash_settings import SettingsParameters
# from mountainash_data.databases.settings import BigQueryAuthSettings


from .base_ibis_operations import BaseIbisOperations
from ...constants import IBIS_DB_CONNECTION_MODE, CONST_DB_BACKEND


from google.oauth2 import service_account
import contextlib
import warnings
from pydantic_settings import BaseSettings


class BigQuery_IbisConnection(BaseIbisOperations):

    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.BIGQUERY
