from .base_pyiceberg_connection import BasePyIcebergConnection
from .connections import PyIcebergRestConnection
# import importlib.util

__all__ = (

    "BasePyIcebergConnection",
    "PyIcebergRestConnection"

    )


# # Helper function to conditionally import and add to __all__
# def _conditional_import(module_path, class_name):
#     """Import a class only if its module is available."""
#     try:
#         module = importlib.import_module(module_path)
#         if hasattr(module, class_name):
#             globals()[class_name] = getattr(module, class_name)
#             __all__.append(class_name)
#     except ImportError:
#         pass

# Import optional connections if available
# _conditional_import(".base_pyiceberg_connection", "BasePyIcebergConnection")
# _conditional_import(".connections", "PyIcebergRestConnection")
