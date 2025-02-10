from typing import Optional
import ibis

from functools import lru_cache

@lru_cache(maxsize=None)
def init_ibis_connection(ibis_schema: Optional[str] = None) -> ibis.BaseBackend:
    return ibis.connect(resource=f"{ibis_schema}://")


def get_default_ibis_backend_schema():
    return "polars"