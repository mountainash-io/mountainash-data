#path: mountainash_settings/auth/database/providers/file/sqlite.py

from typing import Optional, List, Any, Dict, Tuple
from upath import UPath

from pydantic import Field

from mountainash_settings import SettingsParameters

from .base import BaseDBAuthSettings
from ..constants import CONST_DB_PROVIDER_TYPE

class PySparkMode():
    BATCH = "batch"
    STREAMING = "streaming"

class PySparkAuthSettings(BaseDBAuthSettings):
    """ SQLite authentication settings
        Databricks options: https://docs.databricks.com/en/spark/conf.html
        Too many options to set. Configure your spark instanmce directly! https://spark.apache.org/docs/3.5.1/configuration.html#available-properties
    """

    # PROVIDER_TYPE: str = Field(default=CONST_DB_PROVIDER_TYPE.PYSPARK)
    AUTH_METHOD: str = Field(default="none")

    # File Settings
    MODE: str = Field(default=None) #batch or streaming

    SPARK_MASTER: str = Field(default=None)
    APPLICATION_NAME: str = Field(default=None)
    WAREHOUSE_DIR: str = Field(default=None)


    # Databricks options
    PARTITIONS: int = Field(default={})

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
        return CONST_DB_PROVIDER_TYPE.PYSPARK

    def _post_init(self, reinitialise: bool) -> None:
        """Initialize provider-specific settings"""
        pass

    def get_connection_string_template(self, scheme: Optional[str] = None) -> str:

        """Generate PySpark connection string"""
        #"pyspark://{warehouse-dir}?spark.app.name=CountingSheep&spark.master=local[2]""
        template = f"{scheme}"

        if self.WAREHOUSE_DIR:
            template += "{warehouse_dir}"

        if self.APPLICATION_NAME:
            template += "{spark_app_name}"

        if self.SPARK_MASTER:
            template += "{spark_master}"

        return template

    def get_connection_string_params(self) -> Dict[str, Any]:
        """Get connection arguments for PySpark"""
        args = {}


        if self.SPARK_MASTER:
            args["spark_master"] = self.SPARK_MASTER

        if self.APPLICATION_NAME:
            args["spark_app_name"] = self.APPLICATION_NAME

        if self.WAREHOUSE_DIR:
            args["warehouse_dir"] = self.WAREHOUSE_DIR


        return args

    def get_connection_kwargs(self, db_abstraction_layer: Optional[str] = None) -> Dict[str, Any]:
        """Get connection arguments for PySpark"""
        kwargs =  {}

        if self.MODE:
            kwargs["mode"] = self.MODE



        return kwargs

    def get_post_connection_options(self, db_abstraction_layer: Optional[str] = None) -> Dict[str, Any]:

        """Get post connection arguments as dictionary"""
        options = {}

        if self.PARTITIONS:
            options["spark.sql.shuffle.partitions"] = self.PARTITIONS

        return options
