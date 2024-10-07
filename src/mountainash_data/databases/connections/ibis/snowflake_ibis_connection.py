from typing import Optional
import ibis.backends.snowflake as ir_backend

from .base_ibis_connection import BaseIbisConnection
from mountainash_constants import CONST_DB_ABSTRACTION_LAYER, CONST_DB_BACKEND
from mountainash_settings import SettingsParameters

class Snowflake_IbisConnection(BaseIbisConnection):

    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 ssh_auth_settings_parameters: Optional[SettingsParameters] = None,
                 connection_string: Optional[str] = None):

        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters, 
                         ssh_auth_settings_parameters=ssh_auth_settings_parameters,
                         connection_string=connection_string)

        self.ibis_backend:                      Optional[ir_backend.Backend] = None

        self.database_backend: str =            CONST_DB_BACKEND.SNOWFLAKE.value
        self.database_abstraction_layer: str =  CONST_DB_ABSTRACTION_LAYER.IBIS.value

        self.template_connection_string:        Optional[str] = "snowflake://{USERNAME}:{PASSWORD}@{HOST}/{DATABASE_NAME}"




