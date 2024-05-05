
# from ..db_connection_factory import DBConnectionFactory

# class IbisConnectionFactory(DBConnectionFactory):


#     def init_db_connection(self, 
                           
#                            database_abstraction_layer: str,

#                                    **kwargs):

#         #TODO: resolve whether using a connection string (with prefix), or individual connection parameters
#         db_connections_string = get_auth_settings(self.auth_settings_parameters).CONNECTION_STRING

#         if db_connections_string is not None:
#             self.init_db_connection_from_string(db_connections_string)



#         if db_backend not in DataclassUtils.get_enum_values_set(enumclass=CONST_DB_BACKEND):
#             print(f"Unsupported DB Backend: {db_backend}")
#             raise NotImplementedError

#         if db_backend == CONST_DB_BACKEND.DUCKDB.value:
#             raise NotImplementedError
#             # self.init_duckdb_connection(**kwargs)
#         elif db_backend == CONST_DB_BACKEND.SQLITE.value:
#             raise NotImplementedError
#             # self.init_sqlite_connection(, **kwargs)
#         elif db_backend == CONST_DB_BACKEND.POSTGRES.value:
#             raise NotImplementedError
#         elif db_backend == CONST_DB_BACKEND.TRINO.value:
#             raise NotImplementedError
#         elif db_backend == CONST_DB_BACKEND.DATABRICKS.value:
#             raise NotImplementedError
#         elif db_backend == CONST_DB_BACKEND.SNOWFLAKE.value:
#             raise NotImplementedError
#         elif db_backend == CONST_DB_BACKEND.ORACLE.value:
#             raise NotImplementedError
#         elif db_backend == CONST_DB_BACKEND.REDSHIFT.value:
#             raise NotImplementedError
#         elif db_backend == CONST_DB_BACKEND.MSSQL.value:
#             raise NotImplementedError
#         elif db_backend == CONST_DB_BACKEND.MYSQL.value:
#             raise NotImplementedError
        