#path: mountainash_settings/auth/database/providers/cloud/redshift.py

from typing import Optional, List, Any, Dict,  Tuple
from upath import UPath

from pydantic import Field, SecretStr, field_validator
import re

from mountainash_settings import SettingsParameters

from .base import BaseDBAuthSettings
from ..constants import CONST_DB_PROVIDER_TYPE, CONST_DB_AUTH_METHOD
from .exceptions import DBAuthValidationError


class RedshiftAuthSettings(BaseDBAuthSettings):
    """Amazon Redshift authentication settings"""

    # PROVIDER_TYPE: str = Field(default=CONST_DB_PROVIDER_TYPE.REDSHIFT)

    # AWS Settings
    REGION: str = Field(...)
    CLUSTER_IDENTIFIER: Optional[str] = Field(default=None)
    IAM_ROLE_ARN: Optional[str] = Field(default=None)

    # Redshift-specific Settings
    # DATABASE: Optional[str] = Field(...)
    PORT: Optional[int] = Field(default=5439)
    SCHEMA: Optional[str] = Field(default=None)

    # Authentication Settings
    AUTH_METHOD: str = Field(default=CONST_DB_AUTH_METHOD.PASSWORD)
    ACCESS_KEY_ID: Optional[str] = Field(default=None)
    SECRET_ACCESS_KEY: Optional[SecretStr] = Field(default=None)
    SESSION_TOKEN: Optional[SecretStr] = Field(default=None)

    # # Connection Settings
    SSL: bool = Field(default=True)
    SERVERLESS: bool = Field(default=False)
    WORKGROUP_NAME: Optional[str] = Field(default=None)
    AUTO_CREATE: bool = Field(default=False)

    # # Additional Settings
    ENDPOINT_URL: Optional[str] = Field(default=None)
    FORCE_IAM: bool = Field(default=False)
    CLUSTER_READ_ONLY: bool = Field(default=False)
    PROFILE_NAME: Optional[str] = Field(default=None)

    def __init__(self,
                 config_files: Optional[str|UPath|List[str|UPath]|Tuple[str|UPath]] = None,
                 settings_parameters:   Optional[SettingsParameters] = None,
                #  _dummy: Optional[bool] = False,
                 **kwargs) -> None:


        super().__init__(config_files=config_files,
                         settings_parameters=settings_parameters,
                        #  _dummy=_dummy,
                         **kwargs)

    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        """Database provider identifier."""
        return CONST_DB_PROVIDER_TYPE.REDSHIFT


    ## Field Validators ##
    @field_validator("REGION")
    def validate_region(cls, v: str) -> str:
        """Validate AWS region format"""
        if not v:
            raise DBAuthValidationError(
                "Region is required",
                provider=CONST_DB_PROVIDER_TYPE.REDSHIFT,
                validation_type="region"
            )

        if not re.match(r'^[a-z]{2}-[a-z]+-\d{1}$', v):
            raise DBAuthValidationError(
                "Invalid AWS region format",
                provider=CONST_DB_PROVIDER_TYPE.REDSHIFT,
                validation_type="region"
            )
        return v

    @field_validator("IAM_ROLE_ARN")
    def validate_role_arn(cls, v: Optional[str]) -> Optional[str]:
        """Validate IAM role ARN format"""
        if v and not v.startswith("arn:aws:iam::"):
            raise DBAuthValidationError(
                "Invalid IAM role ARN format",
                provider=CONST_DB_PROVIDER_TYPE.REDSHIFT,
                validation_type="iam_role"
            )
        return v

    def _init_provider_specific(self, reinitialise: bool) -> None:
        """Initialize provider-specific settings"""
        # Validate authentication configuration
        if self.AUTH_METHOD == CONST_DB_AUTH_METHOD.IAM:
            if not self.IAM_ROLE_ARN and not (self.ACCESS_KEY_ID and self.SECRET_ACCESS_KEY):
                raise DBAuthValidationError(
                    "IAM role ARN or access keys required for IAM authentication",
                    provider=self.PROVIDER_TYPE,
                    validation_type="auth_method"
                )

        # Validate serverless configuration
        if self.SERVERLESS and not self.WORKGROUP_NAME:
            raise DBAuthValidationError(
                "Workgroup name required for serverless mode",
                provider=self.PROVIDER_TYPE,
                validation_type="serverless"
            )

        # Validate cluster configuration
        if not self.SERVERLESS and not self.CLUSTER_IDENTIFIER:
            raise DBAuthValidationError(
                "Cluster identifier required for provisioned mode",
                provider=self.PROVIDER_TYPE,
                validation_type="cluster"
            )

    def get_connection_string_template(self, scheme: Optional[str] = None) -> str:
        """Generate Redshift connection string"""

        # if self.SERVERLESS:
        #     host = self._get_serverless_endpoint()
        # else:][p9]
        #     host = self._get_cluster_endpoint()

        # Base connection string
        template = "{scheme}{username}@{host}:{port}/{database}"

        # Add schema if specified
        if self.SCHEMA:
            template += "/{schema}"

        # Add SSL parameter if enabled
        params = []
        if self.SSL:
            params.append("sslmode=verify-full")

        # Add IAM authentication parameter if using IAM
        if self.AUTH_METHOD == CONST_DB_AUTH_METHOD.IAM or self.FORCE_IAM:
            params.append("iam=true")

        if self.CLUSTER_READ_ONLY:
            params.append("readonly=true")

        if params:
            template += "?" + "&".join(params)

        return self.format_connection_string(template)


    def get_connection_string_params(self, scheme: Optional[str] = None) -> Dict[str, Any]:
        """Get connection arguments for Redshift"""

        args = {'scheme': scheme if scheme else 'redshift://'}

        # Add AWS credentials if using IAM
        if self.AUTH_METHOD == CONST_DB_AUTH_METHOD.IAM or self.FORCE_IAM:
            if self.ACCESS_KEY_ID and self.SECRET_ACCESS_KEY:
                args.update({
                    "aws_access_key_id": self.ACCESS_KEY_ID,
                    "aws_secret_access_key": self.SECRET_ACCESS_KEY,
                })
                if self.SESSION_TOKEN:
                    args["aws_session_token"] = self.SESSION_TOKEN

        # Add Redshift-specific arguments
        # args.update({
        #     "database": self.DATABASE,
        #     "port": self.PORT,
        #     "ssl": self.SSL
        # })

        if self.SCHEMA:
            args["schema"] = self.SCHEMA

        if self.IAM_ROLE_ARN:
            args["iam_role_arn"] = self.IAM_ROLE_ARN

        # if self.CLUSTER_READ_ONLY:
        #     args["readonly"] = True

        return {k: v for k, v in args.items() if v is not None}

    # def _get_cluster_endpoint(self) -> str:
    #     """Get Redshift cluster endpoint"""
    #     try:
    #         session_kwargs = {}
    #         if self.ACCESS_KEY_ID and self.SECRET_ACCESS_KEY:
    #             session_kwargs.update({
    #                 "aws_access_key_id": self.ACCESS_KEY_ID,
    #                 "aws_secret_access_key": self.SECRET_ACCESS_KEY,
    #             })
    #             if self.SESSION_TOKEN:
    #                 session_kwargs["aws_session_token"] = self.SESSION_TOKEN

    #         # if self.PROFILE_NAME:
    #         #     session_kwargs["profile_name"] = self.PROFILE_NAME

    #         session = boto3.Session(**session_kwargs)
    #         client = session.client(
    #             'redshift',
    #             region_name=self.REGION,
    #             endpoint_url=self.ENDPOINT_URL
    #         )

    #         response = client.describe_clusters(
    #             ClusterIdentifier=self.CLUSTER_IDENTIFIER
    #         )

    #         if not response['Clusters']:
    #             raise DBAuthConfigError(
    #                 f"Cluster not found: {self.CLUSTER_IDENTIFIER}",
    #                 provider=self.PROVIDER_TYPE
    #             )

    #         return response['Clusters'][0]['Endpoint']['Address']

    #     except Exception as e:
    #         raise DBAuthConfigError(
    #             f"Failed to get cluster endpoint: {str(e)}",
    #             provider=self.PROVIDER_TYPE
    #         )

    # def _get_serverless_endpoint(self) -> str:
    #     """Get Redshift serverless endpoint"""
    #     try:
    #         session_kwargs = {}
    #         if self.ACCESS_KEY_ID and self.SECRET_ACCESS_KEY:
    #             session_kwargs.update({
    #                 "aws_access_key_id": self.ACCESS_KEY_ID,
    #                 "aws_secret_access_key": self.SECRET_ACCESS_KEY,
    #             })
    #             if self.SESSION_TOKEN:
    #                 session_kwargs["aws_session_token"] = self.SESSION_TOKEN

    #         if self.PROFILE_NAME:
    #             session_kwargs["profile_name"] = self.PROFILE_NAME

    #         # session = boto3.Session(**session_kwargs)
    #         # # client = session.client(
    #         # #     'redshift-serverless',
    #         # #     region_name=self.REGION,
    #         # #     endpoint_url=self.ENDPOINT_URL
    #         # # )

    #         response = client.get_workgroup(
    #             workgroupName=self.WORKGROUP_NAME
    #         )

    #         return response['workgroup']['endpoint']['address']

        # except Exception as e:
        #     raise DBAuthConfigError(
        #         f"Failed to get serverless endpoint: {str(e)}",
        #         provider=self.PROVIDER_TYPE
        #     )

    def get_connection_kwargs(self, db_abstraction_layer: Optional[str] = None) -> Dict[str, Any]:
        """Get connection arguments for Redshift"""
        return {}

    def get_post_connection_options(self, db_abstraction_layer: Optional[str] = None) -> Dict[str, Any]:

        """Get connection arguments as dictionary"""
        ...
