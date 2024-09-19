import ibis
from functools import lru_cache
from typing import Optional

@lru_cache(maxsize=None)
def init_ibis_connection(ibis_schema: Optional[str] = None) -> ibis.BaseBackend:
    return ibis.connect(resource=f"{ibis_schema}://")