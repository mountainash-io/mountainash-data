
# from mountainash_constants import CONST_DB_ABSTRACTION_LAYER
# from mountainash_settings import SettingsParameters
# from .base_db_connection import BaseDBConnection

# class DBConnectionFactory:

#     @staticmethod
#     def create_connection(auth_settings_parameters: SettingsParameters,
#                           database_abstraction_layer: str,
#                           ) -> BaseDBConnection:

#         if CONST_DB_ABSTRACTION_LAYER.IBIS == database_abstraction_layer:
#             raise NotImplementedError            
#             # return IbisConnectionFactory.init_db_connection(auth_settings_parameters)

#         elif CONST_DB_ABSTRACTION_LAYER.FUGUE == database_abstraction_layer:
#             raise NotImplementedError
#             #return FugueConnectionFactory.create_db_connection(connection_string)

#         elif CONST_DB_ABSTRACTION_LAYER.SQLALCHEMY == database_abstraction_layer:
#             raise NotImplementedError
#             # return SQLAlchemyConnectionFactory.create_db_connection(connection_string)

#         else:
#             raise ValueError(f"DBConnectionFactory: Unsupported database abstraction layer: {database_abstraction_layer}")
