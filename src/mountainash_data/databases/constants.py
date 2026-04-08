"""DEPRECATED: import from mountainash_data.core.constants instead.

This shim exists during the Phase 1–6 refactor and will be removed in
Phase 6.
"""

from mountainash_data.core.constants import *  # noqa: F401,F403
from mountainash_data.core.constants import (  # noqa: F401  # explicit re-exports
    IBIS_DB_CONNECTION_MODE,
    CONST_DB_PROVIDER_TYPE,
    CONST_DB_AUTH_METHOD,
    CONST_DB_SSL_MODE_MYSQL,
    CONST_DB_SSL_MODE_POSTGRES,
    CONST_DB_CONNECTION_STATUS,
    CONST_DB_POOL_MODE,
    CONST_DB_ABSTRACTION_LAYER,
    CONST_DB_BACKEND,
    CONST_DB_BACKEND_IBIS_PREFIX,
    CONST_DB_BACKEND_CAPABILITIES,
    CONST_INDEX_TYPE,
    CONST_CONFLICT_ACTION,
)
