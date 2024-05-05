from typing import Optional
import ibis as ibis

from .base_ibis_connection import BaseIbisConnection
from mountainash_constants import CONST_DB_ABSTRACTION_LAYER, CONST_DB_BACKEND
from mountainash_settings import SettingsParameters

class Trino_IbisConnection(BaseIbisConnection):



    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 ssh_auth_settings_parameters: Optional[SettingsParameters] = None,
                 connection_string: Optional[str] = None):

        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters, 
                         ssh_auth_settings_parameters=ssh_auth_settings_parameters,
                         connection_string=connection_string)

        self.database_backend: str =             CONST_DB_BACKEND.ORACLE.value
        self.database_abstraction_layer: str =   CONST_DB_ABSTRACTION_LAYER.IBIS.value

        self.template_connection_string:    Optional[str] = "trino://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE_NAME}/{DATABASE_SCHEMA}"


    def connect_ibis(self, connection_string: str) -> ibis.BaseBackend:
        """Connect to the database using the provided connection string."""

        db_connection = ibis.trino.connect(connection_string)

        if db_connection is None:
            raise ValueError("Trino_IbisConnection: Connection could not be established")
    
        return db_connection
    
