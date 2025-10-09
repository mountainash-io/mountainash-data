"""Tests for BaseDBConnection abstract base class."""

import pytest
from mountainash_data.databases import BaseDBConnection


@pytest.mark.unit
class TestBaseDBConnection:
    """Tests for BaseDBConnection abstract base class."""

    def test_base_db_connection_is_abstract(self):
        """Test that BaseDBConnection cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            BaseDBConnection()

    def test_base_db_connection_has_required_abstract_methods(self):
        """Test that BaseDBConnection defines required abstract methods."""
        assert hasattr(BaseDBConnection, 'connect')
        assert hasattr(BaseDBConnection, 'disconnect')
        assert hasattr(BaseDBConnection, 'is_connected')

    def test_base_db_connection_has_db_backend_property(self):
        """Test that BaseDBConnection has db_backend_name property."""
        assert hasattr(BaseDBConnection, 'db_backend_name')

    def test_base_db_connection_has_settings_parameter(self):
        """Test that BaseDBConnection expects db_auth_settings_parameters."""
        # This is verified by the abstract __init__ signature
        import inspect
        sig = inspect.signature(BaseDBConnection.__init__)
        assert 'db_auth_settings_parameters' in sig.parameters
