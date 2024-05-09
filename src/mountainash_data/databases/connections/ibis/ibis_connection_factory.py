
# from ..ibis_backend_factory import DBConnectionFactory

# class IbisConnectionFactory(DBConnectionFactory):


#     def init_ibis_backend(self, 
                           
#                            database_abstraction_layer: str,

#                                    **kwargs):

#         #TODO: resolve whether using a connection string (with prefix), or individual connection parameters
#         ibis_backends_string = get_auth_settings(self.auth_settings_parameters).CONNECTION_STRING

#         if ibis_backends_string is not None:
#             self.init_ibis_backend_from_string(ibis_backends_string)



#         if db_backend not in DataclassUtils.get_enum_values_set(enumclass=CONST_DB_BACKEND):
#             print(f"Unsupported DB Backend: {db_backend}")
#             raise NotImplementedError

#         if db_backend == CONST_DB_BACKEND.DUCKDB.value:
#             raise NotImplementedError
#             # self.init_duckibis_backend(**kwargs)
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
        