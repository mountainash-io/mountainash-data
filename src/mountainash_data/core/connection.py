from typing import Optional, Any, Type
from abc import ABC, abstractmethod

from ibis.backends.sql import SQLBackend
# from pydantic_settings import BaseSettings

from mountainash_settings import SettingsParameters, MountainAshBaseSettings

from mountainash_data.core.constants import CONST_DB_ABSTRACTION_LAYER, CONST_DB_PROVIDER_TYPE

# from mountainash_utils_ssh import SSH_Helper
# from mountainash_constants import CONST_DB_ABSTRACTION_LAYER, CONST_DB_BACKEND

class BaseDBConnection(ABC):


    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 read_only: Optional[bool] = True
                 ):

        self.db_auth_settings_parameters: SettingsParameters = db_auth_settings_parameters

        if db_auth_settings_parameters.settings_class is None:
            raise ValueError("Settings class is required for the database connection")

        if not issubclass(db_auth_settings_parameters.settings_class, self.settings_class):
            raise Exception(f"Settings must be for the correct class for the database connection. Expected {self.settings_class}. Received {db_auth_settings_parameters.settings_class}")


        # self.connection_string:     Optional[str] = connection_string

        # self.ssh_required: bool = bool(get_settings(settings_parameters=db_auth_settings_parameters).SSH_REQUIRED)

        # if self.ssh_required:
        #     if ssh_auth_settings_parameters is None:
        #         raise ValueError("SSH Auth settings are required for SSH connection")

        #     #Just create the object, do not connect
        #     self.ssh_client = SSH_Helper(auth_settings_parameters=ssh_auth_settings_parameters)


    # =================
    # Abstract Properties



    @property
    @abstractmethod
    def db_abstraction_layer(self) -> CONST_DB_ABSTRACTION_LAYER:
        """Database abstraction layer identifier."""
        pass

    @property
    @abstractmethod
    def provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        """Database provider identifier."""
        pass

    @property
    @abstractmethod
    def db_backend_name(self) -> Any:
        """Database connection object name."""
        pass


    @property
    @abstractmethod
    def settings_class(self) -> Type[MountainAshBaseSettings]:
        """Settings class for database configuration."""
        pass


    # =================
    # Abstract Methods

    @abstractmethod
    def connect(self) -> Optional[SQLBackend]:
        """Connect to the database using the provided connection string."""
        pass

    @abstractmethod
    def disconnect(self):
        """Close the connection to the database."""
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """ Is the connection open?"""
        pass


    # @abstractmethod
    # def run_sql(self,
    #             sql: str):
    #     """Execute the given SQL statement."""
    #     pass

    # @abstractmethod
    # def call_procedure(self,
    #                    procedure_name: str,
    #                    params: Optional[Dict[str, Any]] = None):
    #     """Call the specified stored procedure with the given parameters."""
    #     pass

    # @abstractmethod
    # def get_dataframe_from_sql(self,
    #                             sql: str):
    #     """Execute the given SQL statement."""
    #     pass

    # =================
    # Concrete Functions

    def init_ssh(self):

        if self.ssh_required:
            self.ssh_client.connect_ssh()
