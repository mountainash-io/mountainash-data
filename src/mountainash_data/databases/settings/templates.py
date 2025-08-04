#path: mountainash_settings/auth/database/templates.py

from pydantic import Field
from pydantic_settings import BaseSettings
from functools import lru_cache

class DBAuthTemplates(BaseSettings):
    """Templates for database connection strings"""
    
    # SQL Database Templates
    MYSQL_TEMPLATE: str = Field(
        default="mysql://{username}:{password}@{host}:{port}/{database}"
    )
    
    POSTGRESQL_TEMPLATE: str = Field(
        default="postgresql://{username}:{password}@{host}:{port}/{database}"
    )
    
    MSSQL_TEMPLATE: str = Field(
        default="mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    )
    
    # Cloud Database Templates
    SNOWFLAKE_TEMPLATE: str = Field(
        default="snowflake://{username}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}"
    )
    
    BIGQUERY_TEMPLATE: str = Field(
        default="bigquery://{project_id}/{dataset_id}"
    )
    
    REDSHIFT_TEMPLATE: str = Field(
        default="redshift+psycopg2://{username}:{password}@{host}:{port}/{database}"
    )
    
    # File Database Templates
    SQLITE_TEMPLATE: str = Field(
        default="sqlite:///{database}"
    )
    
    DUCKDB_TEMPLATE: str = Field(
        default="duckdb:///{database}"
    )
    
    # Generic Template Parts
    SSL_PARAMS_TEMPLATE: str = Field(
        default="?ssl_ca={ssl_ca}&ssl_cert={ssl_cert}&ssl_key={ssl_key}&ssl_verify={ssl_verify}"
    )
    
    POOL_PARAMS_TEMPLATE: str = Field(
        default="&pool_size={pool_size}&pool_timeout={pool_timeout}&max_overflow={max_overflow}"
    )

@lru_cache()
def get_db_auth_templates() -> DBAuthTemplates:
    """Get cached instance of database authentication templates"""
    return DBAuthTemplates()