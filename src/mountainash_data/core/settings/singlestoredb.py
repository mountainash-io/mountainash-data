"""SingleStoreDB backend settings.

Driver: https://singlestoredb-python.labs.singlestore.com/
Ibis: ``ibis.singlestoredb.connect(host, user, password, port, database,
       driver, autocommit, local_infile, **kwargs)``
"""

from __future__ import annotations

import typing as t
from enum import StrEnum

from ..constants import CONST_DB_PROVIDER_TYPE
from mountainash_settings.auth import NoAuth, PasswordAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


class SingleStoreDriver(StrEnum):
    MYSQL = "mysql"
    HTTP = "http"
    HTTPS = "https"


SINGLESTOREDB_DESCRIPTOR = BackendDescriptor(
    name="singlestoredb",
    provider_type=CONST_DB_PROVIDER_TYPE.SINGLESTOREDB,
    default_port=3306,
    connection_string_scheme="singlestoredb://",
    ibis_dialect="singlestoredb",
    auth_modes=[PasswordAuth, NoAuth],
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=3306,
                      driver_key="port"),
        ParameterSpec(name="DATABASE", type=t.Optional[str], tier="core",
                      default=None, driver_key="database"),
        ParameterSpec(name="DRIVER", type=t.Optional[SingleStoreDriver],
                      tier="core", default=None, driver_key="driver"),
        ParameterSpec(name="AUTOCOMMIT", type=bool, tier="core",
                      default=True, driver_key="autocommit"),
        ParameterSpec(name="LOCAL_INFILE", type=bool, tier="advanced",
                      default=True, driver_key="local_infile"),
    ],
)


@register(SINGLESTOREDB_DESCRIPTOR)
class SingleStoreDBAuthSettings(ConnectionProfile):
    __descriptor__ = SINGLESTOREDB_DESCRIPTOR
