from typing import Optional
import ibis

from functools import lru_cache

@lru_cache(maxsize=None)
def init_ibis_connection(ibis_schema: Optional[str] = None) -> ibis.BaseBackend:

    if ibis_schema is not None:
        return ibis.connect(f"{ibis_schema}://")
    else:
        "Connecting ibis to polars backend"
        return ibis.polars.connect()

def get_default_ibis_backend_schema():
    return "polars"