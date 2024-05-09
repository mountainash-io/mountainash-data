from typing import Optional
import ibis

from .base_ibis_connection import BaseIbisConnection
from mountainash_constants import CONST_DB_ABSTRACTION_LAYER, CONST_DB_BACKEND
from mountainash_settings import SettingsParameters

class MySQL_IbisConnection(BaseIbisConnection):


    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 ssh_auth_settings_parameters: Optional[SettingsParameters] = None,
                 connection_string: Optional[str] = None):

        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters, 
                         ssh_auth_settings_parameters=ssh_auth_settings_parameters,
                         connection_string=connection_string)


        self.database_backend: str =             CONST_DB_BACKEND.MYSQL.value
        self.database_abstraction_layer: str =   CONST_DB_ABSTRACTION_LAYER.IBIS.value

        

        self.template_connection_string:    Optional[str] = "mysql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE_NAME}"


    def connect_ibis(self, connection_string: str) -> ibis.BaseBackend:
        """Connect to the database using the provided connection string."""

        ibis_backend = ibis.mysql.connect(connection_string)

        if ibis_backend is None:
            raise ValueError("MySQL_IbisConnection: Connection could not be established")
    
        return ibis_backend
