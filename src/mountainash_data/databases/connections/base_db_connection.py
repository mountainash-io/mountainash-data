from typing import Optional, Any, Type, Dict
from abc import ABC, abstractmethod

from ibis.backends.sql import SQLBackend
from pydantic_settings import BaseSettings

from mountainash_settings import SettingsParameters, get_settings
from mountainash_data.databases.settings import BaseDBAuthSettings

from ..constants import CONST_DB_ABSTRACTION_LAYER, CONST_DB_PROVIDER_TYPE

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
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        """Database provider identifier."""
        pass

    @property
    @abstractmethod
    def db_backend_name(self) -> Any:
        """Database connection object name."""
        pass


    @property
    @abstractmethod
    def settings_class(self) -> Type[BaseSettings]:
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


    # def prepare_connection_parameters(self) -> str:
    #     """Connect to the database using the provided connection string."""


    #     if not self.connection_string:
    #         self.connection_string = get_settings(settings_parameters=self.db_auth_settings_parameters).CONNECTION_STRING

    #     if not self.connection_string:
    #         self.connection_string = self.template_connection_string

    #     return  self.format_connection_string(connection_string=self.connection_string)


    def get_connection_string_template(self,
                                     scheme: Optional[str] = None) -> str:

        if scheme is None:
            scheme = self.connection_string_scheme

        obj_settings: BaseDBAuthSettings = BaseDBAuthSettings.get_settings(settings_parameters=self.db_auth_settings_parameters)

        if not isinstance(obj_settings, BaseDBAuthSettings):
            raise ValueError(f"Expected BaseDBAuthSettings but got {type(obj_settings)}")

        return obj_settings.get_connection_string_template(scheme=scheme)

    def get_connection_string_params(self) -> Dict[str, Any]:


        obj_settings: BaseDBAuthSettings = BaseDBAuthSettings.get_settings(settings_parameters=self.db_auth_settings_parameters)

        if not isinstance(obj_settings, BaseDBAuthSettings):
            raise ValueError(f"Expected BaseDBAuthSettings but got {type(obj_settings)}")

        return obj_settings.get_connection_string_params()


    def get_connection_kwargs(self) -> Dict[str, Any]:

        obj_settings: BaseDBAuthSettings  = BaseDBAuthSettings.get_settings(settings_parameters=self.db_auth_settings_parameters)

        if not isinstance(obj_settings, BaseDBAuthSettings):
            raise ValueError(f"Expected BaseDBAuthSettings but got {type(obj_settings)}")

        return obj_settings.get_connection_kwargs()


    def format_connection_string(self,
                                 template: Optional[str] = None,
                                 params: Optional[Dict] = None
                                 ) -> str:

        """Format connection string using template"""

        if not template:
            template = self.get_connection_string_template()

        if not params:
            params = self.get_connection_string_params()

        # escaped_params = {k: quote_plus(str(v)) for k, v in params.items()}
        # safe_params = defaultdict(str, escaped_params)

        try:
            return template.format_map(params)
        except Exception as e:
            raise ValueError(f"Failed to format connection string: {str(e)}")



    # def format_connection_string(self,
    #                              connection_string: Optional[str] = None) -> str:
    #     """Connect to the database using the provided connection string."""

    #     if connection_string is None:
    #         raise ValueError("BaseDBConnection: Connection string or a template is not provided. Use the CONNECTION_STRING field in the auth settings, provide it via the constructor, or override the template_connection_string value.")

    #     # Apply settings parameters to the template
    #     formatted_connection_string = get_settings(settings_parameters=self.db_auth_settings_parameters).format_template_from_settings(template_str=connection_string)

    #     # encoded_connection_string = quote_plus(string=formatted_connection_string)
    #     url = urlparse(formatted_connection_string)
    #     print(url)

    #     database, *schema = url.path[1:].split("/", 1)
    #     # query_params = parse_qs(url.query)
    #     connect_args = {
    #         "user": url.username,
    #         "password": url.password or "",
    #         "host": url.hostname,
    #         "database": database or "",
    #         "schema": schema[0] if schema else "",
    #         "port": url.port,
    #     }
    #     print(connect_args)


    #     return formatted_connection_string
