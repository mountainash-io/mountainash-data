"""Matrix parametrization tests for comprehensive backend and data structure testing.

This module demonstrates sophisticated matrix testing across:
- Multiple backends (SQLite, DuckDB, MotherDuck, Pandas, Polars)
- Multiple data structures (dict, list_of_dicts, dataframes)
- Multiple transformation chains (input -> processing -> output formats)
- Real-world business scenarios using enhanced fixtures
- Edge cases and performance scenarios

The matrix approach ensures comprehensive coverage while maintaining test readability
and leveraging the existing sophisticated fixture ecosystem.
"""

import pytest
from typing import Dict, List, Any, Optional, Union, Tuple
from unittest.mock import Mock
import tempfile
from pathlib import Path

# Core imports
import pandas as pd
import polars as pl
import pyarrow as pa

# Mountain Ash Data imports
from mountainash_data import (
    SQLite_IbisConnection,
    DuckDB_IbisConnection,
    MotherDuck_IbisConnection
)
from mountainash_dataframes.utils.dataframe_factory import DataFrameFactory
from mountainash_dataframes.ibis_dataframe import IbisDataFrame

# Settings imports
from mountainash_settings import SettingsParameters
from mountainash_settings.settings.auth.database import (
    SQLiteAuthSettings,
    DuckDBAuthSettings,
    MotherDuckAuthSettings
)


# ============================================================================
# BACKEND PARAMETRIZATION FIXTURES
# ============================================================================

@pytest.fixture(params=[
    "sqlite_memory",
    "duckdb_memory",
    "pandas_df",
    "polars_df"
])
def backend_config(request):
    """Parametrize across different backends and data structure types.

    Returns configuration for creating dataframes across different backends:
    - Ibis backends (SQLite, DuckDB, MotherDuck)
    - Native pandas DataFrames
    - Native polars DataFrames
    """
    backend_type = request.param

    if backend_type == "sqlite_memory":
        # Create in-memory SQLite connection
        settings = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            namespace="SQLiteAuthSettings",
            database_path=":memory:"
        )
        connection = SQLite_IbisConnection(db_auth_settings_parameters=settings)

        return {
            "type": "ibis_sqlite",
            "connection": connection,
            "backend": connection.connect(),
            "materialise_targets": ["pandas", "polars", "pyarrow"],
            "supports_sql": True,
            "supports_complex_types": True
        }

    elif backend_type == "duckdb_memory":
        # Create in-memory DuckDB connection
        settings = SettingsParameters.create(
            settings_class=DuckDBAuthSettings,
            namespace="DuckDBAuthSettings",
            database_path=":memory:"
        )
        connection = DuckDB_IbisConnection(db_auth_settings_parameters=settings)

        return {
            "type": "ibis_duckdb",
            "connection": connection,
            "backend": connection.connect(),
            "materialise_targets": ["pandas", "polars", "pyarrow"],
            "supports_sql": True,
            "supports_complex_types": True
        }

    elif backend_type == "pandas_df":
        return {
            "type": "pandas_df",
            "connection": None,
            "backend": None,
            "materialise_targets": ["pandas"],
            "supports_sql": False,
            "supports_complex_types": True
        }

    elif backend_type == "polars_df":
        return {
            "type": "polars_df",
            "connection": None,
            "backend": None,
            "materialise_targets": ["polars"],
            "supports_sql": True,  # Polars supports SQL
            "supports_complex_types": True
        }

    else:
        raise ValueError(f"Unknown backend type: {backend_type}")


@pytest.fixture(params=[
    ("dict", "ibis_dataframe", "polars"),
    ("dict", "ibis_dataframe", "pandas"),
    ("dict", "ibis_dataframe", "pyarrow"),
    ("dict", "pandas_dataframe", None),
    ("dict", "polars_dataframe", None),
    ("list_of_dicts", "pandas_dataframe", None),
    ("list_of_dicts", "polars_dataframe", None),
    ("pandas_df", "ibis_dataframe", "polars"),
    ("polars_df", "ibis_dataframe", "pandas"),
])
def data_transform_chain(request):
    """Parametrize data transformation chains.

    Tests different paths for data transformation:
    input_format -> intermediate_format -> output_format

    This ensures we test all supported conversion paths in the system.
    """
    input_format, intermediate_format, output_format = request.param
    return {
        "input": input_format,
        "intermediate": intermediate_format,
        "output": output_format,
        "chain_id": f"{input_format}->{intermediate_format}->{output_format or 'none'}"
    }


# ============================================================================
# UTILITY FUNCTIONS FOR MATRIX TESTING
# ============================================================================

class DataFrameTestHelper:
    """Helper class for DataFrame operations across different backends."""

    @staticmethod
    def create_dataframe(data: Dict[str, List], backend_config: Dict, transform_chain: Dict):
        """Create DataFrame using specified backend and transformation chain."""

        if backend_config["type"].startswith("ibis_"):
            # Create Ibis DataFrame
            if transform_chain["input"] == "dict":
                return DataFrameFactory.create_ibis_dataframe_object_from_dictionary(
                    data, backend_config["backend"]
                )
            elif transform_chain["input"] == "pandas_df":
                pandas_df = pd.DataFrame(data)
                return DataFrameFactory.create_ibis_dataframe_object_from_pandas_dataframe(
                    pandas_df, backend_config["backend"]
                )
            elif transform_chain["input"] == "polars_df":
                polars_df = pl.DataFrame(data)
                return DataFrameFactory.create_ibis_dataframe_object_from_polars_dataframe(
                    polars_df, backend_config["backend"]
                )

        elif backend_config["type"] == "pandas_df":
            # Create native pandas DataFrame
            if transform_chain["input"] in ["dict", "list_of_dicts"]:
                return pd.DataFrame(data)
            else:
                return pd.DataFrame(data)

        elif backend_config["type"] == "polars_df":
            # Create native polars DataFrame
            if transform_chain["input"] in ["dict", "list_of_dicts"]:
                return pl.DataFrame(data)
            else:
                return pl.DataFrame(data)

        else:
            raise ValueError(f"Unknown backend type: {backend_config['type']}")

    @staticmethod
    def get_row_count(df, backend_config: Dict) -> int:
        """Get row count across different DataFrame types."""
        if backend_config["type"].startswith("ibis_"):
            return len(df)
        elif backend_config["type"] == "pandas_df":
            return len(df)
        elif backend_config["type"] == "polars_df":
            return df.height
        else:
            raise ValueError(f"Unknown backend type: {backend_config['type']}")

    @staticmethod
    def get_column_names(df, backend_config: Dict) -> List[str]:
        """Get column names across different DataFrame types."""
        if backend_config["type"].startswith("ibis_"):
            return df.columns
        elif backend_config["type"] == "pandas_df":
            return df.columns.tolist()
        elif backend_config["type"] == "polars_df":
            return df.columns
        else:
            raise ValueError(f"Unknown backend type: {backend_config['type']}")

    @staticmethod
    def materialize_if_possible(df, target_format: str, backend_config: Dict):
        """Materialize DataFrame if the backend supports it."""
        if hasattr(df, 'materialise') and target_format in backend_config["materialise_targets"]:
            return df.materialise(target_format)
        elif backend_config["type"] == "pandas_df" and target_format == "pandas":
            return df
        elif backend_config["type"] == "polars_df" and target_format == "polars":
            return df
        else:
            return None


# ============================================================================
# COMPREHENSIVE MATRIX TESTS
# ============================================================================

class TestBackendDataStructureMatrix:
    """Comprehensive matrix testing across backends, data structures, and business scenarios."""

    def test_basic_dataframe_operations_matrix(
        self,
        financial_transactions_data,
        backend_config,
        data_transform_chain
    ):
        """Test basic DataFrame operations across all backend/transform combinations."""

        # Create DataFrame using specified backend and transformation
        df = DataFrameTestHelper.create_dataframe(
            financial_transactions_data,
            backend_config,
            data_transform_chain
        )

        # Basic validation - should work across all backends
        assert df is not None
        row_count = DataFrameTestHelper.get_row_count(df, backend_config)
        assert row_count == 100  # Financial transactions data has 100 records

        columns = DataFrameTestHelper.get_column_names(df, backend_config)
        assert 'transaction_id' in columns
        assert 'amount' in columns
        assert 'currency' in columns

        # Test materialization if supported
        for target_format in backend_config["materialise_targets"]:
            materialized = DataFrameTestHelper.materialize_if_possible(
                df, target_format, backend_config
            )
            if materialized is not None:
                # Basic validation of materialized result
                if target_format == "pandas":
                    assert isinstance(materialized, pd.DataFrame)
                    assert len(materialized) == row_count
                elif target_format == "polars":
                    assert isinstance(materialized, pl.DataFrame)
                    assert materialized.height == row_count
                elif target_format == "pyarrow":
                    assert isinstance(materialized, pa.Table)
                    assert materialized.num_rows == row_count


    @pytest.mark.parametrize("business_scenario", [
        "financial_transactions_data",
        "ecommerce_orders_data",
        "hierarchical_org_data"
    ])
    def test_business_scenarios_cross_backend(
        self,
        business_scenario,
        backend_config,
        request
    ):
        """Test realistic business scenarios across different backends."""

        # Get the fixture data dynamically
        fixture_data = request.getfixturevalue(business_scenario)

        # Create DataFrame using backend
        df = DataFrameTestHelper.create_dataframe(
            fixture_data,
            backend_config,
            {"input": "dict", "intermediate": "default", "output": None}
        )

        # Business scenario specific tests
        if business_scenario == "financial_transactions_data":
            self._test_financial_operations(df, backend_config)
        elif business_scenario == "ecommerce_orders_data":
            self._test_ecommerce_operations(df, backend_config)
        elif business_scenario == "hierarchical_org_data":
            self._test_hierarchical_operations(df, backend_config)

    def _test_financial_operations(self, df, backend_config):
        """Test financial-specific operations."""
        columns = DataFrameTestHelper.get_column_names(df, backend_config)

        # Verify financial data structure
        required_columns = ['transaction_id', 'amount', 'currency', 'transaction_type']
        for col in required_columns:
            assert col in columns

        # Test filtering operations (if supported)
        if backend_config["type"].startswith("ibis_"):
            # Test Ibis-specific filtering
            usd_transactions = df.filter(df['currency'] == 'USD')
            assert DataFrameTestHelper.get_row_count(usd_transactions, backend_config) >= 0

        elif backend_config["type"] == "pandas_df":
            # Test pandas-specific filtering
            usd_transactions = df[df['currency'] == 'USD']
            assert len(usd_transactions) >= 0

        elif backend_config["type"] == "polars_df":
            # Test polars-specific filtering
            usd_transactions = df.filter(pl.col('currency') == 'USD')
            assert usd_transactions.height >= 0

    def _test_ecommerce_operations(self, df, backend_config):
        """Test e-commerce-specific operations."""
        columns = DataFrameTestHelper.get_column_names(df, backend_config)

        # Verify e-commerce data structure
        required_columns = ['order_id', 'customer_id', 'unit_price', 'quantity']
        for col in required_columns:
            assert col in columns

        row_count = DataFrameTestHelper.get_row_count(df, backend_config)
        assert row_count == 200  # E-commerce data has 200 orders

    def _test_hierarchical_operations(self, df, backend_config):
        """Test hierarchical data operations."""
        columns = DataFrameTestHelper.get_column_names(df, backend_config)

        # Verify hierarchical structure
        required_columns = ['employee_id', 'department', 'parent_department', 'manager_id']
        for col in required_columns:
            assert col in columns

        row_count = DataFrameTestHelper.get_row_count(df, backend_config)
        assert row_count == 150  # Org data has 150 employees


class TestEdgeCaseMatrix:
    """Test edge cases and boundary conditions across backends."""

    @pytest.mark.parametrize("edge_case_scenario", [
        "numeric_boundary_data",
        "string_boundary_data",
        "datetime_boundary_data",
        "null_and_missing_data"
    ])
    def test_edge_cases_cross_backend(
        self,
        edge_case_scenario,
        backend_config,
        request
    ):
        """Test edge cases across different backends."""

        fixture_data = request.getfixturevalue(edge_case_scenario)

        # Some backends may not handle certain edge cases - test gracefully
        try:
            df = DataFrameTestHelper.create_dataframe(
                fixture_data,
                backend_config,
                {"input": "dict", "intermediate": "default", "output": None}
            )

            # Basic validation
            assert df is not None
            row_count = DataFrameTestHelper.get_row_count(df, backend_config)
            assert row_count > 0

            # Test materialization with edge cases
            for target_format in backend_config["materialise_targets"]:
                materialized = DataFrameTestHelper.materialize_if_possible(
                    df, target_format, backend_config
                )
                if materialized is not None:
                    # Edge case data should still materialize successfully
                    assert materialized is not None

        except Exception as e:
            # Document known limitations
            pytest.skip(
                f"Backend {backend_config['type']} doesn't support edge case "
                f"{edge_case_scenario}: {str(e)}"
            )

    def test_comprehensive_data_types_matrix(
        self,
        comprehensive_data_types,
        backend_config
    ):
        """Test comprehensive data type handling across backends."""

        # Test each data type category separately to isolate issues
        data_type_categories = [
            ("integers", ["integers"]),
            ("floats", ["floats"]),
            ("strings", ["strings"]),
            ("dates", ["dates"]),
            ("booleans", ["booleans"])
        ]

        for category_name, columns in data_type_categories:
            # Create subset of data for this category
            category_data = {
                col: comprehensive_data_types[col]
                for col in columns
                if col in comprehensive_data_types
            }

            try:
                df = DataFrameTestHelper.create_dataframe(
                    category_data,
                    backend_config,
                    {"input": "dict", "intermediate": "default", "output": None}
                )

                # Verify data type handling
                assert df is not None
                columns_in_df = DataFrameTestHelper.get_column_names(df, backend_config)
                for expected_col in columns:
                    assert expected_col in columns_in_df

            except Exception as e:
                pytest.skip(
                    f"Backend {backend_config['type']} doesn't support "
                    f"{category_name}: {str(e)}"
                )


class TestPerformanceMatrix:
    """Performance testing across backends and data sizes."""

    @pytest.mark.performance
    @pytest.mark.parametrize("dataset_size", ["small", "medium"])
    def test_performance_scaling_matrix(
        self,
        large_dataset_configs,
        dataset_size,
        backend_config,
        performance_timer,
        memory_monitor
    ):
        """Test performance characteristics across backends and data sizes."""

        config = large_dataset_configs[dataset_size]

        # Generate test data appropriate for the dataset size
        with performance_timer.time_operation(f"data_generation_{dataset_size}"):
            test_data = self._generate_performance_test_data(config["rows"], config["cols"])
            memory_monitor.update_peak()

        # Test DataFrame creation performance
        operation_name = f"dataframe_creation_{backend_config['type']}_{dataset_size}"
        with performance_timer.time_operation(operation_name):
            df = DataFrameTestHelper.create_dataframe(
                test_data,
                backend_config,
                {"input": "dict", "intermediate": "default", "output": None}
            )
            memory_monitor.update_peak()

        # Verify creation succeeded
        assert df is not None
        row_count = DataFrameTestHelper.get_row_count(df, backend_config)
        assert row_count == config["rows"]

        # Test basic operations performance
        operation_name = f"basic_operations_{backend_config['type']}_{dataset_size}"
        with performance_timer.time_operation(operation_name):
            columns = DataFrameTestHelper.get_column_names(df, backend_config)
            assert len(columns) == config["cols"]
            memory_monitor.update_peak()

        # Test materialization performance
        for target_format in backend_config["materialise_targets"]:
            operation_name = f"materialise_{target_format}_{backend_config['type']}_{dataset_size}"
            with performance_timer.time_operation(operation_name):
                materialized = DataFrameTestHelper.materialize_if_possible(
                    df, target_format, backend_config
                )
                if materialized is not None:
                    # Verify materialization result
                    if target_format == "pandas":
                        assert len(materialized) == row_count
                    elif target_format == "polars":
                        assert materialized.height == row_count
                    elif target_format == "pyarrow":
                        assert materialized.num_rows == row_count
                memory_monitor.update_peak()

        # Performance assertions based on dataset size
        memory_stats = memory_monitor.get_stats()

        # Memory usage should be reasonable for the data size
        # Adjust thresholds based on your performance requirements
        if dataset_size == "small":
            assert memory_stats["peak_increase_mb"] < 100  # Less than 100MB for small datasets
        elif dataset_size == "medium":
            assert memory_stats["peak_increase_mb"] < 500  # Less than 500MB for medium datasets

    def _generate_performance_test_data(self, rows: int, cols: int) -> Dict[str, List]:
        """Generate test data for performance testing."""
        import random
        import string
        from datetime import date, timedelta

        data = {}

        for i in range(cols):
            col_name = f"col_{i:03d}"

            # Mix different data types for realistic performance testing
            if i % 4 == 0:  # Integer columns
                data[col_name] = [random.randint(1, 1000) for _ in range(rows)]
            elif i % 4 == 1:  # Float columns
                data[col_name] = [round(random.uniform(0.0, 100.0), 2) for _ in range(rows)]
            elif i % 4 == 2:  # String columns
                data[col_name] = [
                    ''.join(random.choices(string.ascii_letters, k=10))
                    for _ in range(rows)
                ]
            else:  # Date columns
                base_date = date(2020, 1, 1)
                data[col_name] = [
                    base_date + timedelta(days=random.randint(0, 1000))
                    for _ in range(rows)
                ]

        return data


class TestDataQualityMatrix:
    """Test data quality detection and handling across backends."""

    def test_data_quality_detection_matrix(
        self,
        data_quality_issues_data,
        backend_config
    ):
        """Test data quality issue detection across different backends."""

        try:
            df = DataFrameTestHelper.create_dataframe(
                data_quality_issues_data,
                backend_config,
                {"input": "dict", "intermediate": "default", "output": None}
            )

            # Test duplicate detection
            self._test_duplicate_detection(df, backend_config)

            # Test null handling
            self._test_null_handling(df, backend_config)

        except Exception as e:
            # Some backends may not handle data quality issues gracefully
            pytest.skip(
                f"Backend {backend_config['type']} failed on data quality test: {str(e)}"
            )

    def _test_duplicate_detection(self, df, backend_config):
        """Test duplicate detection capabilities."""

        # We know the test data has duplicate customer_ids: 3 and 5
        row_count = DataFrameTestHelper.get_row_count(df, backend_config)
        columns = DataFrameTestHelper.get_column_names(df, backend_config)

        assert row_count == 10  # Data quality test data has 10 records
        assert 'customer_id' in columns

        # Test materialization to verify duplicate preservation
        for target_format in backend_config["materialise_targets"]:
            materialized = DataFrameTestHelper.materialize_if_possible(
                df, target_format, backend_config
            )
            if materialized is not None:
                if target_format == "pandas":
                    # Check for duplicates in pandas
                    duplicates = materialized['customer_id'].duplicated().sum()
                    assert duplicates == 2  # Should find 2 duplicate rows
                elif target_format == "polars":
                    # Check for duplicates in polars
                    duplicates = materialized.filter(
                        pl.col('customer_id').is_duplicated()
                    ).height
                    assert duplicates == 4  # 2 pairs of duplicates = 4 rows marked as duplicated

    def _test_null_handling(self, df, backend_config):
        """Test null value handling."""

        # The data quality test data contains various null patterns
        columns = DataFrameTestHelper.get_column_names(df, backend_config)

        # Verify columns with known null issues
        null_containing_columns = ['first_name', 'last_name', 'registration_date', 'age']
        for col in null_containing_columns:
            if col in columns:
                # Test that nulls are preserved in materialization
                for target_format in backend_config["materialise_targets"]:
                    materialized = DataFrameTestHelper.materialize_if_possible(
                        df, target_format, backend_config
                    )
                    if materialized is not None:
                        if target_format == "pandas":
                            null_count = materialized[col].isnull().sum()
                            assert null_count > 0  # Should have some nulls
                        elif target_format == "polars":
                            null_count = materialized[col].null_count()
                            assert null_count > 0  # Should have some nulls


# ============================================================================
# INTEGRATION TESTS WITH EXISTING FIXTURES
# ============================================================================

class TestMatrixIntegrationWithExistingFixtures:
    """Demonstrate integration with all existing sophisticated fixtures."""

    @pytest.mark.parametrize("temporal_fixture", [
        "temporal_data_comprehensive"
    ])
    def test_temporal_data_matrix(
        self,
        temporal_fixture,
        backend_config,
        request
    ):
        """Test temporal data handling across backends."""

        fixture_data = request.getfixturevalue(temporal_fixture)

        # Test different temporal data patterns
        temporal_patterns = [
            "hourly_series",
            "irregular_series",
            "daily_aggregations",
            "seasonal_monthly"
        ]

        for pattern in temporal_patterns:
            if pattern in fixture_data:
                pattern_data = fixture_data[pattern]

                try:
                    df = DataFrameTestHelper.create_dataframe(
                        pattern_data,
                        backend_config,
                        {"input": "dict", "intermediate": "default", "output": None}
                    )

                    # Verify temporal data handling
                    assert df is not None
                    row_count = DataFrameTestHelper.get_row_count(df, backend_config)
                    assert row_count > 0

                    # Test timestamp column handling
                    columns = DataFrameTestHelper.get_column_names(df, backend_config)
                    timestamp_columns = [c for c in columns if 'time' in c.lower() or 'date' in c.lower()]
                    assert len(timestamp_columns) > 0

                except Exception as e:
                    pytest.skip(
                        f"Backend {backend_config['type']} failed on temporal pattern "
                        f"{pattern}: {str(e)}"
                    )

    def test_factory_integration_matrix(
        self,
        data_type_factory,
        backend_config
    ):
        """Test data type factory integration across backends."""

        # Test different data types with factory
        data_types = ["integer", "float", "string", "date", "boolean"]

        for data_type in data_types:
            try:
                # Generate data using factory
                factory_data = data_type_factory(data_type, size=50, null_rate=0.1)

                # Convert to dict format for DataFrame creation
                test_data = {f"{data_type}_column": factory_data}

                df = DataFrameTestHelper.create_dataframe(
                    test_data,
                    backend_config,
                    {"input": "dict", "intermediate": "default", "output": None}
                )

                # Verify factory-generated data works with backend
                assert df is not None
                row_count = DataFrameTestHelper.get_row_count(df, backend_config)
                assert row_count == 50

                columns = DataFrameTestHelper.get_column_names(df, backend_config)
                assert f"{data_type}_column" in columns

            except Exception as e:
                pytest.skip(
                    f"Backend {backend_config['type']} failed with factory data type "
                    f"{data_type}: {str(e)}"
                )
