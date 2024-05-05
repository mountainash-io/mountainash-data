from typing import Optional
import ibis

from .base_ibis_connection import BaseIbisConnection
from mountainash_constants import CONST_DB_ABSTRACTION_LAYER, CONST_DB_BACKEND
from mountainash_settings import SettingsParameters

class Polars_IbisConnection(BaseIbisConnection):


    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 ssh_auth_settings_parameters: Optional[SettingsParameters] = None,
                 connection_string: Optional[str] = None):

        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters, 
                         ssh_auth_settings_parameters=ssh_auth_settings_parameters,
                         connection_string=connection_string)


        self.database_backend: str =             CONST_DB_BACKEND.POLARS.value
        self.database_abstraction_layer: str =   CONST_DB_ABSTRACTION_LAYER.IBIS.value

        

        self.template_connection_string:    Optional[str] = "polars://"


    def connect_ibis(self, connection_string: str) -> ibis.BaseBackend:
        """Connect to the database using the provided connection string."""

        db_connection = ibis.polars.connect(connection_string)

        if db_connection is None:
            raise ValueError("Polars_IbisConnection: Connection could not be established")
    
        return db_connection
