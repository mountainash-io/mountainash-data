import typing as t # import t.Any, t.Dict, t.Optional

import ibis
import ibis.expr.types.relations as ir
from ibis.expr.schema import SchemaLike
from ibis.backends.sql import SQLBackend
from abc import abstractmethod

# from abc import abstractmethod
from mountainash_settings import SettingsParameters
from mountainash_dataframes.constants import CONST_DATAFRAME_FRAMEWORK

from ..base_db_connection import BaseDBConnection
from ...constants import IBIS_DB_CONNECTION_MODE,  CONST_DB_ABSTRACTION_LAYER, CONST_DB_PROVIDER_TYPE
# from mountainash_dataframes import BaseDataFrame, IbisDataFrame
# from mountainash_dataframes.utils.dataframe_utils import DataFrameUtils


class BaseIbisConnection(BaseDBConnection):

    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 ):

        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters,
                         )


    @property
    def db_abstraction_layer(self) -> CONST_DB_ABSTRACTION_LAYER:
        return CONST_DB_ABSTRACTION_LAYER.IBIS

    @property
    @abstractmethod
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        """Database provider identifier."""
        ...


    @property
    @abstractmethod
    def ibis_backend(self) -> t.Optional[SQLBackend|t.Any]:
        """Concrete Ibis backend connection object."""
        pass

    @property
    @abstractmethod
    def ibis_connection_mode(self) -> str:
        """Connect via a connection string, kwargs or both."""
        pass

    @property
    @abstractmethod
    def connection_string_scheme(self) -> str:
        """Template string for database connection."""
        pass

    # @property
    # @abstractmethod
    # def supports_upsert(self) -> str:
    #     """Whether upserts are supported"""
    #     pass




    ###########################
    # Core Functions


    def connect(self,
                *,
                connection_string: t.Optional[str] = None,
                connection_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
                **kwargs) -> SQLBackend:

        """Connect with explicitly provided connection parameters"""


        if self.ibis_backend is None:

            if connection_string is not None:
                if connection_kwargs is not None:
                    self._connect(connection_string=connection_string, connection_kwargs=connection_kwargs, **kwargs)
                else:
                    self._connect(connection_string=connection_string, **kwargs)

            else:
                if connection_kwargs is not None:
                    self._connect(connection_string = self.connection_string_scheme, connection_kwargs=connection_kwargs, **kwargs)
                else:
                    self.connect_default(**kwargs)

        if self.ibis_backend is None:
            raise Exception(f"Unable to establish connection to {self.db_backend_name}")


        return self.ibis_backend


    def connect_default(self, **kwargs) -> SQLBackend:
        """Connect using default configuration"""
        # Implementation for connecting with default parameters

        if self.ibis_backend is None:

            connection_string_template = self.get_connection_string_template(scheme=self.connection_string_scheme)
            connectionstring_params = self.get_connection_string_params()

            connection_string = self.format_connection_string(template=connection_string_template, params=connectionstring_params)
            connection_kwargs = self.get_connection_kwargs()

            if self.ibis_connection_mode == IBIS_DB_CONNECTION_MODE.CONNECTION_STRING:
                self._connect(connection_string=connection_string, **kwargs)

            elif self.ibis_connection_mode == IBIS_DB_CONNECTION_MODE.KWARGS:
                #combine connectionstring_params and connection_kwargs
                connection_kwargs = {**connectionstring_params, **connection_kwargs}
                self._connect(connection_string=self.connection_string_scheme, connection_kwargs=connection_kwargs, **kwargs)

            elif self.ibis_connection_mode == IBIS_DB_CONNECTION_MODE.HYBRID:
                self._connect(connection_string=connection_string, connection_kwargs=connection_kwargs, **kwargs)

        #TODO: Add check and logging
        if self.ibis_backend is None:
            raise Exception(f"Unable to establish default connection to {self.db_backend_name}")

        return self.ibis_backend



    def _connect(self,
                 connection_string: t.Optional[str] = None,
                 connection_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
                 **kwargs
                 ) -> SQLBackend:
        """
        Default Implementation to connect to the database using the provided connection string.
        By default this relies on the connection_string_scheme to determine the backend dynamically.
        Over-ride in subclasses if a different implementation is necessary, such as using the custom backend directly.
        """

        if connection_kwargs is None:
            connection_kwargs = {}

        #combine connection_kwargs and kwargs
        connection_kwargs = {**connection_kwargs, **kwargs}

        if connection_string is None:
            raise ValueError(f"{self.db_backend_name}: Connection string is required to establish connection")

        self._ibis_backend : t.Any = ibis.connect(connection_string, **connection_kwargs)

        if self.ibis_backend is None:
            raise Exception(f"Unable to establish connection to {self.db_backend_name}")


        return self.ibis_backend



    def close(self):
        """Close the connection to the database."""

        self.disconnect()

    def disconnect(self):
        """Close the connection to the database."""

        if self.ibis_backend is not None:
            self.ibis_backend.disconnect()
            self._ibis_backend  = None

    def is_connected(self) -> bool:
        """ Is the connection open?"""
        if self.ibis_backend is None:
            return False
        else:
            return True
