from typing import Any, Optional
from abc import ABC, abstractmethod
from urllib.parse import urlparse
from mountainash_settings import SettingsParameters
from mountainash_auth_settings import get_auth_settings
 
from mountainash_utils_ssh import SSH_Helper
# from mountainash_constants import CONST_DB_ABSTRACTION_LAYER, CONST_DB_BACKEND

class BaseDBConnection(ABC):


    def __init__(self, 
                 db_auth_settings_parameters: SettingsParameters,
                 ssh_auth_settings_parameters: Optional[SettingsParameters] = None,
                 connection_string: Optional[str] = None):

        self.db_abstraction_layer:          Optional[str] = None
        self.db_connection:                 Optional[Any] = None
        self.template_connection_string:    Optional[str] = None


        self.db_auth_settings_parameters: SettingsParameters = db_auth_settings_parameters
        self.connection_string:     Optional[str] = connection_string
        self.ssh_required: bool = bool(get_auth_settings(auth_settings_parameters=db_auth_settings_parameters).SSH_REQUIRED)

        if self.ssh_required:
            if ssh_auth_settings_parameters is None:
                raise ValueError("SSH Auth settings are required for SSH connection")  

            #Just create the object, do not connect 
            self.ssh_client = SSH_Helper(auth_settings_parameters=ssh_auth_settings_parameters)


    @abstractmethod
    def connect(self):
        """Connect to the database using the provided connection string."""
        pass

    @abstractmethod
    def close(self):
        """Close the connection to the database."""
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
            self.ssh_client.connect()


    def prepare_connection_parameters(self) -> str:
        """Connect to the database using the provided connection string."""
        

        if not self.connection_string:
            self.connection_string = get_auth_settings(auth_settings_parameters=self.db_auth_settings_parameters).CONNECTION_STRING

        if not self.connection_string:
            self.connection_string = self.template_connection_string

        return  self.format_connection_string(connection_string=self.connection_string)
    
    




    def format_connection_string(self, 
                                 connection_string: Optional[str] = None) -> str:
        """Connect to the database using the provided connection string."""

        if connection_string is None:
            raise ValueError("BaseDBConnection: Connection string or a template is not provided. Use the CONNECTION_STRING field in the auth settings, provide it via the constructor, or override the template_connection_string value.")

        # Apply settings parameters to the template
        formatted_connection_string = get_auth_settings(auth_settings_parameters=self.db_auth_settings_parameters).format_template_from_settings(template_str=connection_string)

        # encoded_connection_string = quote_plus(string=formatted_connection_string)
        url = urlparse(formatted_connection_string)
        print(url)

        database, *schema = url.path[1:].split("/", 1)
        # query_params = parse_qs(url.query)
        connect_args = {
            "user": url.username,
            "password": url.password or "",
            "host": url.hostname,
            "database": database or "",
            "schema": schema[0] if schema else "",
            "port": url.port,
        }
        print(connect_args)


        return formatted_connection_string
    