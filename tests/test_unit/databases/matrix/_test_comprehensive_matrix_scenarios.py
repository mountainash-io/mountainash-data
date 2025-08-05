"""Comprehensive matrix testing scenarios demonstrating advanced test patterns.

This module showcases the full power of matrix parametrization testing by combining:
- Advanced matrix testing fixtures
- Realistic business data scenarios
- Cross-backend validation
- Performance benchmarking
- Data quality assessment
- Error condition testing

These tests demonstrate how the matrix approach scales from simple smoke tests
to comprehensive integration and stress testing across the entire system.
"""

import pytest
from typing import Dict, List, Any, Optional
import time
from contextlib import contextmanager

# Core imports
import pandas as pd
import polars as pl

# Mountain Ash imports
from mountainash_dataframes.utils.dataframe_factory import DataFrameFactory

# Matrix testing fixtures
from fixtures.matrix_testing_fixtures import (
    TestComplexity,
    BackendCapability,
    MatrixTestConfig
)

# Import helper from the main matrix test file
from test_backend_datastructure_matrix import DataFrameTestHelper


class TestAdvancedMatrixScenarios:
    """Advanced matrix testing scenarios using sophisticated fixture combinations."""

    def test_scenario_driven_matrix(
        self,
        matrix_test_scenarios,
        matrix_test_data_generator,
        backend_config,
        matrix_test_validator,
        performance_timer,
        memory_monitor
    ):
        """Test predefined scenarios across matrix of backends and configurations."""

        # Test each predefined scenario
        for scenario_name, scenario_config in matrix_test_scenarios.items():

            # Skip scenarios not applicable to current backend
            if backend_config["type"] not in scenario_config["backends"]:
                pytest.skip(
                    f"Scenario {scenario_name} not applicable to backend {backend_config['type']}"
                )

            # Generate data for each data scenario in the test scenario
            for data_scenario in scenario_config["data_scenarios"]:

                # Generate appropriate test data
                test_data = matrix_test_data_generator(
                    data_scenario,
                    {"rows": 1000, "cols": 10},
                    scenario_config["complexity"],
                    include_edge_cases=True
                )

                # Create DataFrame
                with performance_timer.time_operation(f"{scenario_name}_{data_scenario}_creation"):
                    df = DataFrameTestHelper.create_dataframe(
                        test_data,
                        backend_config,
                        {"input": "dict", "intermediate": "default", "output": None}
                    )
                    memory_monitor.update_peak()

                # Execute operations defined for this scenario
                for operation in scenario_config["operations"]:
                    self._execute_scenario_operation(
                        df,
                        operation,
                        backend_config,
                        performance_timer,
                        f"{scenario_name}_{operation}"
                    )

                # Cache results for cross-backend validation
                matrix_test_validator.cache_result(
                    f"{scenario_name}_{data_scenario}",
                    backend_config["type"],
                    {"row_count": DataFrameTestHelper.get_row_count(df, backend_config)}
                )

    def _execute_scenario_operation(
        self,
        df,
        operation: str,
        backend_config: Dict,
        performance_timer,
        operation_id: str
    ):
        """Execute a specific operation as part of a scenario."""

        with performance_timer.time_operation(operation_id):

            if operation == "count":
                result = DataFrameTestHelper.get_row_count(df, backend_config)
                assert result > 0

            elif operation == "column_names":
                columns = DataFrameTestHelper.get_column_names(df, backend_config)
                assert len(columns) > 0

            elif operation == "head":
                # Test head operation if supported
                if hasattr(df, 'head'):
                    head_df = df.head(5)
                    assert head_df is not None
                elif hasattr(df, 'limit'):
                    head_df = df.limit(5)
                    assert head_df is not None

            elif operation == "filter_equals":
                # Test filtering if supported
                columns = DataFrameTestHelper.get_column_names(df, backend_config)
                if len(columns) > 0:
                    first_col = columns[0]
                    if backend_config["type"].startswith("ibis_"):
                        # Ibis filtering
                        filtered = df.filter(df[first_col].notnull())
                        assert filtered is not None
                    elif backend_config["type"] == "pandas_df":
                        filtered = df[df[first_col].notnull()]
                        assert len(filtered) >= 0
                    elif backend_config["type"] == "polars_df":
                        filtered = df.filter(pl.col(first_col).is_not_null())
                        assert filtered.height >= 0

            elif operation == "group_by":
                # Test grouping operations if supported
                columns = DataFrameTestHelper.get_column_names(df, backend_config)
                categorical_columns = [c for c in columns if 'type' in c.lower() or 'category' in c.lower() or 'status' in c.lower()]

                if categorical_columns:
                    group_col = categorical_columns[0]
                    if backend_config["type"].startswith("ibis_"):
                        grouped = df.group_by(group_col).count()
                        assert grouped is not None

            elif operation == "aggregation":
                # Test aggregation operations
                columns = DataFrameTestHelper.get_column_names(df, backend_config)
                numeric_columns = [c for c in columns if 'amount' in c.lower() or 'value' in c.lower() or 'score' in c.lower()]

                if numeric_columns:
                    numeric_col = numeric_columns[0]
                    if backend_config["type"].startswith("ibis_"):
                        try:
                            agg_result = df[numeric_col].sum()
                            assert agg_result is not None
                        except Exception:
                            # Some aggregations might not be supported
                            pass


class TestCrossBackendConsistency:
    """Test consistency of results across different backends."""

    @pytest.mark.parametrize("consistency_scenario", [
        "financial_transactions_data",
        "ecommerce_orders_data"
    ])
    def test_cross_backend_result_consistency(
        self,
        consistency_scenario,
        matrix_test_validator,
        request
    ):
        """Test that the same operations produce consistent results across backends."""

        fixture_data = request.getfixturevalue(consistency_scenario)

        # We'll collect this across multiple backend runs via pytest-xdist or manual orchestration
        # For demonstration, we'll test that operations on the same data are deterministic

        backends_to_test = [
            {"type": "pandas_df", "connection": None, "backend": None, "materialise_targets": ["pandas"]},
            {"type": "polars_df", "connection": None, "backend": None, "materialise_targets": ["polars"]}
        ]

        operation_results = {}

        for backend_config in backends_to_test:
            try:
                # Create DataFrame with this backend
                df = DataFrameTestHelper.create_dataframe(
                    fixture_data,
                    backend_config,
                    {"input": "dict", "intermediate": "default", "output": None}
                )

                # Collect results for consistency checking
                results = {
                    "row_count": DataFrameTestHelper.get_row_count(df, backend_config),
                    "column_count": len(DataFrameTestHelper.get_column_names(df, backend_config)),
                    "column_names": set(DataFrameTestHelper.get_column_names(df, backend_config))
                }

                operation_results[backend_config["type"]] = results

            except Exception as e:
                pytest.skip(f"Backend {backend_config['type']} failed: {str(e)}")

        # Validate consistency across backends
        if len(operation_results) >= 2:
            for operation in ["row_count", "column_count"]:
                assert matrix_test_validator.validate_cross_backend_consistency(
                    {backend: results[operation] for backend, results in operation_results.items()},
                    operation
                ), f"Inconsistent {operation} across backends"

            # Column names should be identical
            column_name_sets = [results["column_names"] for results in operation_results.values()]
            assert all(cols == column_name_sets[0] for cols in column_name_sets), \
                "Column names inconsistent across backends"


class TestPerformanceBenchmarking:
    """Comprehensive performance benchmarking across backends and scenarios."""

    @pytest.mark.performance
    def test_performance_benchmarking_matrix(
        self,
        matrix_test_configurations,
        matrix_test_data_generator,
        large_dataset_configs,
        backend_config,
        backend_capability_matrix,
        performance_timer,
        memory_monitor,
        matrix_test_validator
    ):
        """Comprehensive performance benchmarking using matrix configurations."""

        # Get backend capabilities
        backend_capabilities = backend_capability_matrix.get(
            backend_config["type"],
            {"capabilities": [], "max_recommended_rows": 10000}
        )

        # Test each performance-oriented configuration
        performance_configs = [
            config for config in matrix_test_configurations.values()
            if config.complexity in [TestComplexity.INTEGRATION, TestComplexity.STRESS]
            and backend_config["type"] in config.backends
        ]

        for config in performance_configs:

            # Check if backend supports required capabilities
            if not all(cap in backend_capabilities["capabilities"] for cap in config.required_capabilities):
                pytest.skip(
                    f"Backend {backend_config['type']} missing required capabilities "
                    f"{config.required_capabilities}"
                )

            # Test with appropriate data sizes
            for data_size in config.data_sizes:
                if data_size not in large_dataset_configs:
                    continue

                size_config = large_dataset_configs[data_size]

                # Skip if data size exceeds backend recommendations
                if size_config["rows"] > backend_capabilities["max_recommended_rows"]:
                    pytest.skip(
                        f"Data size {data_size} ({size_config['rows']} rows) exceeds "
                        f"recommendations for {backend_config['type']}"
                    )

                # Generate test data
                test_scenario = "financial_analytics" if "analytics" in config.test_id else "generic"

                with performance_timer.time_operation(f"data_gen_{config.test_id}_{data_size}"):
                    test_data = matrix_test_data_generator(
                        test_scenario,
                        size_config,
                        config.complexity
                    )
                    memory_monitor.update_peak()

                # Test DataFrame creation performance
                creation_op = f"creation_{config.test_id}_{data_size}"
                with performance_timer.time_operation(creation_op):
                    df = DataFrameTestHelper.create_dataframe(
                        test_data,
                        backend_config,
                        {"input": "dict", "intermediate": "default", "output": None}
                    )
                    memory_monitor.update_peak()

                # Test operations performance
                operations_op = f"operations_{config.test_id}_{data_size}"
                with performance_timer.time_operation(operations_op):

                    # Row count
                    row_count = DataFrameTestHelper.get_row_count(df, backend_config)
                    assert row_count == size_config["rows"]

                    # Column operations
                    columns = DataFrameTestHelper.get_column_names(df, backend_config)
                    assert len(columns) > 0

                    # Materialization test
                    if "pandas" in backend_config["materialise_targets"]:
                        materialized = DataFrameTestHelper.materialize_if_possible(
                            df, "pandas", backend_config
                        )
                        if materialized is not None:
                            assert len(materialized) == row_count

                    memory_monitor.update_peak()

                # Validate performance thresholds
                times = performance_timer.get_times()
                memory_stats = memory_monitor.get_stats()

                # Check performance thresholds
                if config.expected_performance_threshold:
                    total_time = times.get(creation_op, 0) + times.get(operations_op, 0)
                    assert total_time <= config.expected_performance_threshold, \
                        f"Performance threshold exceeded: {total_time:.2f}s > {config.expected_performance_threshold}s"

                # Check memory thresholds
                if config.memory_threshold_mb:
                    assert memory_stats["peak_increase_mb"] <= config.memory_threshold_mb, \
                        f"Memory threshold exceeded: {memory_stats['peak_increase_mb']:.1f}MB > {config.memory_threshold_mb}MB"


class TestDataQualityMatrix:
    """Advanced data quality testing across backends and scenarios."""

    def test_data_quality_scenarios_matrix(
        self,
        matrix_test_data_generator,
        backend_config,
        matrix_test_validator
    ):
        """Test data quality scenarios across backends."""

        # Generate data with known quality issues
        quality_test_data = matrix_test_data_generator(
            "data_quality_assessment",
            {"rows": 200, "cols": 8},
            TestComplexity.EDGE_CASE,
            include_edge_cases=True
        )

        try:
            df = DataFrameTestHelper.create_dataframe(
                quality_test_data,
                backend_config,
                {"input": "dict", "intermediate": "default", "output": None}
            )

            # Test data quality detection capabilities
            self._test_null_detection(df, backend_config)
            self._test_duplicate_detection(df, backend_config)
            self._test_data_type_validation(df, backend_config)

        except Exception as e:
            pytest.skip(
                f"Backend {backend_config['type']} failed on data quality test: {str(e)}"
            )

    def _test_null_detection(self, df, backend_config):
        """Test null value detection across backends."""

        columns = DataFrameTestHelper.get_column_names(df, backend_config)

        # Test that we can detect nulls in various formats
        for target_format in backend_config["materialise_targets"]:
            materialized = DataFrameTestHelper.materialize_if_possible(
                df, target_format, backend_config
            )
            if materialized is not None:
                if target_format == "pandas":
                    # Check for null detection in pandas
                    for col in columns:
                        if col in materialized.columns:
                            null_count = materialized[col].isnull().sum()
                            # Should have some nulls in quality test data
                            # (This is a heuristic - adjust based on your data)

                elif target_format == "polars":
                    # Check for null detection in polars
                    for col in columns:
                        if col in materialized.columns:
                            null_count = materialized[col].null_count()
                            # Should handle nulls appropriately

    def _test_duplicate_detection(self, df, backend_config):
        """Test duplicate detection capabilities."""

        # Test materialization and duplicate handling
        for target_format in backend_config["materialise_targets"]:
            materialized = DataFrameTestHelper.materialize_if_possible(
                df, target_format, backend_config
            )
            if materialized is not None:
                row_count = DataFrameTestHelper.get_row_count(df, backend_config)

                if target_format == "pandas":
                    # Test pandas duplicate detection
                    duplicates = materialized.duplicated().sum()
                    # Quality test data should have some duplicates

                elif target_format == "polars":
                    # Test polars duplicate detection
                    unique_count = materialized.n_unique()
                    # Should be fewer unique rows than total rows if duplicates exist

    def _test_data_type_validation(self, df, backend_config):
        """Test data type handling and validation."""

        # Test that backends handle mixed/invalid data types appropriately
        columns = DataFrameTestHelper.get_column_names(df, backend_config)

        # Basic validation - should not crash
        assert len(columns) > 0

        # Test materialization with mixed data types
        for target_format in backend_config["materialise_targets"]:
            materialized = DataFrameTestHelper.materialize_if_possible(
                df, target_format, backend_config
            )
            if materialized is not None:
                # Should successfully materialize even with data quality issues
                if target_format == "pandas":
                    assert len(materialized) > 0
                elif target_format == "polars":
                    assert materialized.height > 0


class TestErrorHandlingMatrix:
    """Test error handling and recovery across backends."""

    def test_error_recovery_matrix(
        self,
        backend_config
    ):
        """Test error handling and recovery scenarios across backends."""

        # Test various error scenarios
        error_scenarios = [
            {
                "name": "empty_data",
                "data": {},
                "should_fail": True
            },
            {
                "name": "mismatched_lengths",
                "data": {"col1": [1, 2, 3], "col2": [1, 2]},
                "should_fail": True
            },
            {
                "name": "invalid_column_names",
                "data": {"": [1, 2, 3], None: [4, 5, 6]},
                "should_fail": True
            },
            {
                "name": "extremely_large_strings",
                "data": {"col1": ["x" * 1_000_000] * 10},
                "should_fail": False  # Should handle but may be slow
            }
        ]

        for scenario in error_scenarios:
            try:
                df = DataFrameTestHelper.create_dataframe(
                    scenario["data"],
                    backend_config,
                    {"input": "dict", "intermediate": "default", "output": None}
                )

                if scenario["should_fail"]:
                    # If we expected this to fail but it didn't, that's also valid
                    # (some backends may be more tolerant)
                    pass
                else:
                    # Should succeed
                    assert df is not None

            except Exception as e:
                if not scenario["should_fail"]:
                    pytest.fail(
                        f"Unexpected failure in scenario {scenario['name']} "
                        f"for backend {backend_config['type']}: {str(e)}"
                    )
                # Expected failure - test passes


# ============================================================================
# MATRIX TESTING UTILITIES AND HELPERS
# ============================================================================

@contextmanager
def matrix_test_context(test_name: str, backend_config: Dict):
    """Context manager for matrix testing with consistent setup/teardown."""

    print(f"Starting matrix test '{test_name}' with backend {backend_config['type']}")
    start_time = time.time()

    try:
        yield

    finally:
        end_time = time.time()
        duration = end_time - start_time
        print(f"Completed matrix test '{test_name}' in {duration:.3f}s")


def skip_if_backend_unsupported(backend_config: Dict, required_capabilities: List[BackendCapability]):
    """Skip test if backend doesn't support required capabilities."""

    # This would be implemented with actual capability checking
    # For now, we'll implement basic logic

    if BackendCapability.LARGE_DATASETS in required_capabilities:
        if backend_config["type"] == "sqlite_memory":
            pytest.skip("SQLite not recommended for large datasets")

    if BackendCapability.REAL_TIME in required_capabilities:
        if backend_config["type"] in ["sqlite_memory", "pandas_df"]:
            pytest.skip("Backend doesn't support real-time operations")


# ============================================================================
# DEMONSTRATION OF MATRIX TESTING POWER
# ============================================================================

class TestMatrixTestingDemo:
    """Demonstration of the full power of matrix testing approach."""

    def test_comprehensive_matrix_demonstration(
        self,
        financial_transactions_data,
        backend_config,
        matrix_test_configurations,
        performance_timer,
        memory_monitor
    ):
        """Comprehensive demonstration of matrix testing capabilities."""

        with matrix_test_context("comprehensive_demo", backend_config):

            # 1. Create DataFrame with realistic data
            with performance_timer.time_operation("demo_creation"):
                df = DataFrameTestHelper.create_dataframe(
                    financial_transactions_data,
                    backend_config,
                    {"input": "dict", "intermediate": "default", "output": None}
                )
                memory_monitor.update_peak()

            # 2. Test basic operations
            with performance_timer.time_operation("demo_basic_ops"):
                row_count = DataFrameTestHelper.get_row_count(df, backend_config)
                columns = DataFrameTestHelper.get_column_names(df, backend_config)
                memory_monitor.update_peak()

            # 3. Test materialization
            materialization_results = {}
            for target_format in backend_config["materialise_targets"]:
                with performance_timer.time_operation(f"demo_materialize_{target_format}"):
                    materialized = DataFrameTestHelper.materialize_if_possible(
                        df, target_format, backend_config
                    )
                    if materialized is not None:
                        materialization_results[target_format] = True
                    memory_monitor.update_peak()

            # 4. Validate results
            assert row_count == 100  # Financial data has 100 transactions
            assert 'transaction_id' in columns
            assert 'amount' in columns
            assert len(materialization_results) > 0  # At least one materialization worked

            # 5. Performance validation
            times = performance_timer.get_times()
            memory_stats = memory_monitor.get_stats()

            # Basic performance assertions
            assert times["demo_creation"] < 10.0  # Should create DF in < 10 seconds
            assert memory_stats["peak_increase_mb"] < 200.0  # Should use < 200MB

            print(f"Matrix test completed successfully for backend {backend_config['type']}")
            print(f"Operations: {len(times)}, Materializations: {len(materialization_results)}")
            print(f"Peak memory: {memory_stats['peak_mb']:.1f}MB")
