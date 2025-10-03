from typing import TYPE_CHECKING
import lazy_loader

# Base PyIceberg connection (always available)
from .base_pyiceberg_connection import BasePyIcebergConnection

# Type hints for PyIceberg connections (zero runtime cost)
if TYPE_CHECKING:
    from .pyiceberg_rest_connection import PyIcebergRestConnection

# Lazy loading for PyIceberg connections (imported only when used)
__getattr__, __dir__, __all__ = lazy_loader.attach(
    __name__,
    submodules=[],
    submod_attrs={
        'pyiceberg_rest_connection': ['PyIcebergRestConnection'],
    }
)

# Manually extend __all__ to include base export
__all__ = [
    "BasePyIcebergConnection",
] + list(__all__)
