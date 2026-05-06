"""ClickHouse backend settings.

Driver: https://clickhouse.com/docs/en/integrations/python
Ibis: ``ibis.clickhouse.connect(host='localhost', port=9000, database='default',
       user='default', password='', **kwargs)``
"""

from __future__ import annotations

import typing as t

from ..constants import CONST_DB_PROVIDER_TYPE
from mountainash_settings.auth import NoAuth, PasswordAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


CLICKHOUSE_DESCRIPTOR = BackendDescriptor(
    name="clickhouse",
    provider_type=CONST_DB_PROVIDER_TYPE.CLICKHOUSE,
    default_port=9000,
    connection_string_scheme="clickhouse://",
    ibis_dialect="clickhouse",
    auth_modes=[PasswordAuth, NoAuth],
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=9000,
                      driver_key="port"),
        ParameterSpec(name="DATABASE", type=t.Optional[str], tier="core",
                      default=None, driver_key="database"),
        ParameterSpec(name="SECURE", type=bool, tier="core",
                      default=False, driver_key="secure"),
        ParameterSpec(name="CONNECT_TIMEOUT", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="connect_timeout"),
        ParameterSpec(name="SEND_RECEIVE_TIMEOUT", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="send_receive_timeout"),
        ParameterSpec(name="SYNC_REQUEST_TIMEOUT", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="sync_request_timeout"),
    ],
)


@register(CLICKHOUSE_DESCRIPTOR)
class ClickHouseAuthSettings(ConnectionProfile):
    __descriptor__ = CLICKHOUSE_DESCRIPTOR
