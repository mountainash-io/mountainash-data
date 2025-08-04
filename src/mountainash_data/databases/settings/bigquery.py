#path: mountainash_settings/auth/database/providers/cloud/bigquery.py

from typing import Optional, List, Any, Dict, Tuple
from upath import UPath

from pydantic import Field, field_validator

from mountainash_settings import SettingsParameters

from .base import BaseDBAuthSettings
from ..constants import CONST_DB_PROVIDER_TYPE



class BigQueryAuthSettings(BaseDBAuthSettings):
    """BigQuery authentication settings

    Ibis BigQuery: https://ibis-project.org/backends/bigquery
    Auth Optiopns: https://cloud.google.com/sdk/docs/authorizing
    External data souyrces: https://cloud.google.com/bigquery/external-data-sources

    """

    PROVIDER_TYPE: str = Field(default=CONST_DB_PROVIDER_TYPE.BIGQUERY)

    # Project Settings
    PROJECT_ID: str = Field(...)
    DATASET_ID: Optional[str] = Field(default=None)

    LOCATION: Optional[str] = Field(default=None)
    APPLICATION_NAME: Optional[str] = Field(default=None)
    PARTITION_COLUMN: Optional[str] = Field(default=None)

    # # Authentication Settings
    SERVICE_ACCOUNT_INFO: Optional[Dict[str, Any]] = Field(default=None)
    # SERVICE_ACCOUNT_FILE: Optional[str] = Field(default=None)

    # # Client Settings
    # DEFAULT_QUERY_JOB_CONFIG: Optional[Dict[str, Any]] = Field(default=None)
    # MAXIMUM_BYTES_BILLED: Optional[int] = Field(default=None)
    # API_ENDPOINT: Optional[str] = Field(default=None)

    # # Performance Settings
    # NUM_RETRIES: int = Field(default=3)
    # RETRIES_WITH_LOGGING: Optional[List[int]] = Field(default=[1, 5, 10])

    def __init__(self,
                 config_files: Optional[str|UPath|List[str|UPath]|Tuple[str|UPath]] = None,
                 settings_parameters:   Optional[SettingsParameters] = None,
                #  _dummy: Optional[bool] = False,
                 **kwargs) -> None:


        super().__init__(config_files=config_files,
                         settings_parameters=settings_parameters,
                        #  _dummy=_dummy,
                         **kwargs)


    @field_validator("PROJECT_ID")
    @classmethod
    def validate_project_id(cls, value: Optional[str]) -> Optional[str]:
        """Validate validate_auth_method"""

        precondition: bool = value is not None
        test: bool = (6 <= len(value) <= 30) if value else False
        valid: bool = (not precondition) | test

        if not valid:
            raise ValueError("PROJECT_ID must be between 6 and 30 characters.")

        return value


    def _post_init(self, reinitialise: bool) -> None:
        pass

    def get_connection_string_template(self) -> str:

        # "bigquery://{project_id}/{dataset_id}"

        template = "{scheme}{project_id}/{dataset_id}"

        return template

    def get_connection_string_params(self, scheme: Optional[str] = None) -> Dict[str, Any]:

        args = {}
        args["scheme"] = scheme if scheme else "bigquery://"

        if self.PROJECT_ID:
            args["project_id"] =  self.PROJECT_ID
        if self.DATASET_ID:
            args["dataset_id"] =  self.DATASET_ID

        return args


    def get_connection_kwargs(self, db_abstraction_layer: Optional[str] = None) -> Dict[str, Any]:
        """Get connection arguments for BigQuery"""

        args = {}
        if self.SERVICE_ACCOUNT_INFO:
            args["credentials"] =  self.SERVICE_ACCOUNT_INFO

        if self.APPLICATION_NAME:
            args["application_name"] =  self.APPLICATION_NAME

        if self.LOCATION:
            args["location"] =  self.LOCATION

        if self.PARTITION_COLUMN:
            args["partition_column"] =  self.PARTITION_COLUMN



        return {k: v for k, v in args.items() if v is not None}

    def get_post_connection_options(self, db_abstraction_layer: Optional[str] = None) -> Dict[str, Any]:

        """Get connection arguments as dictionary"""
        ...
