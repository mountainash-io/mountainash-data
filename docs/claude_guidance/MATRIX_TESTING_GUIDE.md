# Matrix Testing Guide for Mountain Ash Data

This guide demonstrates how to use the sophisticated matrix parametrization testing framework for comprehensive backend and datastructure validation in the `mountainash-data` package.

## Overview

Matrix testing allows systematic validation of the same operations across multiple backends (SQLite, DuckDB, Pandas, Polars), data structures (dictionaries, DataFrames, lists), and transformation chains. This ensures consistency, performance, and reliability across the entire ecosystem.

## Architecture

### Core Components

```
tests/
├── test_backend_datastructure_matrix.py      # Main matrix tests
├── test_comprehensive_matrix_scenarios.py    # Advanced scenarios
├── test_enhanced_fixtures_demo.py           # Fixture demonstrations
├── fixtures/
│   ├── matrix_testing_fixtures.py           # Matrix-specific fixtures
│   ├── realistic_data_fixtures.py           # Business data fixtures
│   ├── edge_case_fixtures.py               # Edge case fixtures
│   ├── data_type_fixtures.py               # Data type fixtures
│   └── performance_fixtures.py             # Performance fixtures
└── conftest.py                              # Shared fixtures
```

## Quick Start

### 1. Basic Matrix Test

```python
def test_basic_operations_across_backends(
    financial_transactions_data,    # Realistic business data
    backend_config,                 # Parametrized across all backends
    data_transform_chain           # Parametrized transformation paths
):
    """Test basic operations across all backend combinations."""
    
    # Create DataFrame using matrix configuration
    df = DataFrameTestHelper.create_dataframe(
        financial_transactions_data, 
        backend_config, 
        data_transform_chain
    )
    
    # Test operations that should work across all backends
    row_count = DataFrameTestHelper.get_row_count(df, backend_config)
    columns = DataFrameTestHelper.get_column_names(df, backend_config)
    
    # Validate results
    assert row_count == 100
    assert 'transaction_id' in columns
    
    # Test materialization if supported
    for target_format in backend_config["materialise_targets"]:
        materialized = DataFrameTestHelper.materialize_if_possible(
            df, target_format, backend_config
        )
        if materialized is not None:
            assert len(materialized) == row_count
```

This single test automatically runs across:
- **4 backends**: SQLite, DuckDB, Pandas, Polars
- **9 transformation chains**: All supported input→intermediate→output combinations
- **Total combinations**: 36 test executions from one test function

### 2. Business Scenario Matrix

```python
@pytest.mark.parametrize("business_scenario", [
    "financial_transactions_data",
    "ecommerce_orders_data", 
    "hierarchical_org_data"
])
def test_business_scenarios_cross_backend(
    business_scenario,
    backend_config,
    request
):
    """Test realistic business scenarios across different backends."""
    
    fixture_data = request.getfixturevalue(business_scenario)
    
    df = DataFrameTestHelper.create_dataframe(
        fixture_data, backend_config, transform_config
    )
    
    if business_scenario == "financial_transactions_data":
        # Test financial-specific operations
        assert 'amount' in df.columns
        usd_transactions = df.filter(df['currency'] == 'USD')
        assert len(usd_transactions) >= 0
```

This creates:
- **3 business scenarios** × **4 backends** = **12 combinations**
- Each with realistic, business-relevant test data
- Scenario-specific validation logic

## Backend Configuration

### Supported Backends

```python
# Current backends (exported and available)
backend_configs = {
    "sqlite_memory": {
        "connection_class": SQLite_IbisConnection,
        "auth_settings": SQLiteAuthSettings,
        "materialise_targets": ["pandas", "polars", "pyarrow"],
        "supports_sql": True
    },
    
    "duckdb_memory": {
        "connection_class": DuckDB_IbisConnection,  
        "auth_settings": DuckDBAuthSettings,
        "materialise_targets": ["pandas", "polars", "pyarrow"],
        "supports_sql": True
    },
    
    "pandas_df": {
        "materialise_targets": ["pandas"],
        "supports_sql": False
    },
    
    "polars_df": {
        "materialise_targets": ["polars"],
        "supports_sql": True  # Polars supports SQL
    }
}
```

### Backend Capabilities Matrix

```python
backend_capabilities = {
    "sqlite_memory": [SQL_QUERIES, COMPLEX_TYPES],
    "duckdb_memory": [SQL_QUERIES, COMPLEX_TYPES, LARGE_DATASETS, ANALYTICS],
    "pandas_df": [COMPLEX_TYPES, ANALYTICS],
    "polars_df": [SQL_QUERIES, COMPLEX_TYPES, LARGE_DATASETS, ANALYTICS]
}
```

## Data Scenarios

### Realistic Business Data

```python
# Financial transactions (100 records, multi-currency)
financial_transactions_data = {
    "transaction_id": [UUID strings],
    "amount": [Decimal values with precision],
    "currency": ["USD", "EUR", "GBP", "JPY", "CAD"],
    "transaction_date": [datetime objects with timezones],
    "transaction_type": ["DEPOSIT", "WITHDRAWAL", "TRANSFER", "PAYMENT", "REFUND"]
}

# E-commerce orders (200 records, customer relationships)  
ecommerce_orders_data = {
    "order_id": [Unique order IDs],
    "customer_id": [Customer IDs with repeats], 
    "product_id": [Product catalog references],
    "order_status": ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED"],
    "unit_price": [Realistic pricing],
    "shipping_cost": [Variable shipping]
}

# Organizational hierarchy (150 employees, manager relationships)
hierarchical_org_data = {
    "employee_id": [Employee IDs],
    "manager_id": [Manager relationships],
    "department": ["Engineering", "Sales", "Marketing"],
    "parent_department": [Hierarchical structure],
    "salary": [Realistic salary ranges]
}
```

### Edge Cases and Boundary Conditions

```python
# Numeric boundaries
numeric_boundary_data = {
    "integers": [0, 1, -1, sys.maxsize, -sys.maxsize-1],
    "floats": [0.0, float('inf'), float('-inf'), float('nan')],
    "decimals": [High-precision Decimal values]
}

# String edge cases  
string_boundary_data = {
    "empty_and_whitespace": ["", " ", "\\t", "\\n"],
    "unicode_edge_cases": ["🚀 Café naïve résumé", "Unicode normalization"],
    "length_boundaries": [Single char, very long strings]
}

# Temporal edge cases
datetime_boundary_data = {
    "historical_dates": [date(1970,1,1), date(2000,1,1)],  # Unix epoch, Y2K
    "leap_year_scenarios": [date(2000,2,29), date(1900,2,28)],
    "timezone_scenarios": [UTC, various timezones]
}
```

## Performance Testing

### Performance Matrix Configuration

```python
@pytest.mark.performance
@pytest.mark.parametrize("dataset_size,backend_type", [
    ("small", "sqlite_memory"),    # 1,000 rows × 10 cols
    ("medium", "duckdb_memory"),   # 10,000 rows × 25 cols  
    ("large", "duckdb_memory"),    # 100,000 rows × 50 cols
    ("xlarge", "polars_df")        # 1,000,000 rows × 100 cols
])
def test_performance_scaling_matrix(
    large_dataset_configs,
    dataset_size, 
    backend_type,
    performance_timer,
    memory_monitor
):
    """Test performance scaling across backends and data sizes."""
    
    config = large_dataset_configs[dataset_size]
    
    with performance_timer.time_operation(f"creation_{dataset_size}"):
        df = create_large_dataframe(config["rows"], config["cols"])
        memory_monitor.update_peak()
    
    # Assert performance thresholds
    times = performance_timer.get_times()
    memory_stats = memory_monitor.get_stats()
    
    assert times[f"creation_{dataset_size}"] < PERFORMANCE_THRESHOLD
    assert memory_stats["peak_increase_mb"] < MEMORY_THRESHOLD
```

### Performance Thresholds

```python
performance_thresholds = {
    "small": {"time": 5.0, "memory_mb": 100},
    "medium": {"time": 30.0, "memory_mb": 500}, 
    "large": {"time": 60.0, "memory_mb": 1000}
}
```

## Advanced Matrix Scenarios

### Scenario-Driven Testing

```python
matrix_scenarios = {
    "smoke_test_matrix": {
        "backends": ["sqlite_memory", "duckdb_memory", "pandas_df", "polars_df"],
        "operations": ["count", "column_names", "head"],
        "data_scenarios": ["financial_analytics"],
        "complexity": TestComplexity.SMOKE
    },
    
    "integration_test_matrix": {
        "backends": ["sqlite_memory", "duckdb_memory", "polars_df"],
        "operations": ["filter", "group_by", "join"],
        "data_scenarios": ["customer_360", "financial_analytics"],
        "complexity": TestComplexity.INTEGRATION
    }
}
```

### Cross-Backend Validation

```python
def test_cross_backend_consistency(matrix_test_validator):
    """Validate that operations produce consistent results across backends."""
    
    operation_results = {}
    
    for backend in ["pandas_df", "polars_df"]:
        df = create_dataframe_with_backend(test_data, backend)
        operation_results[backend] = {
            "row_count": len(df),
            "sum_amount": df['amount'].sum()
        }
    
    # Validate consistency
    for operation in ["row_count", "sum_amount"]:
        assert matrix_test_validator.validate_cross_backend_consistency(
            {backend: results[operation] for backend, results in operation_results.items()},
            operation
        )
```

## Running Matrix Tests

### All Matrix Tests
```bash
# Run all matrix tests
pytest tests/test_backend_datastructure_matrix.py -v

# Run with specific backend
pytest tests/test_backend_datastructure_matrix.py -k "sqlite_memory" -v

# Run performance tests only
pytest tests/test_*_matrix.py -m performance -v
```

### Parallel Execution
```bash
# Run matrix tests in parallel (recommended for performance)
pytest tests/test_*_matrix.py -n auto

# Run with coverage
pytest tests/test_*_matrix.py --cov=src/mountainash_data --cov-report=html
```

### Test Output Analysis
```bash
# Generate detailed matrix test report
pytest tests/test_*_matrix.py --tb=short -v --durations=20
```

## Benefits of Matrix Testing

### 1. **Comprehensive Coverage**
- **Single test** → **Multiple executions** across all backend combinations
- **Realistic data** ensures real-world applicability
- **Edge cases** validate robustness

### 2. **Consistency Validation**
- Same operations produce consistent results across backends
- Data transformation fidelity maintained
- Cross-platform compatibility assured

### 3. **Performance Benchmarking**
- Compare performance across backends
- Identify optimal backend for specific workloads
- Detect performance regressions

### 4. **Regression Protection**
- New backends automatically tested against full suite
- Changes to existing backends validated comprehensively
- Data structure modifications tested across ecosystem

### 5. **Scalable Test Architecture**
- Adding new backends requires minimal test code changes
- New data scenarios automatically tested across all backends
- Test maintenance overhead minimized

## Advanced Usage Examples

### Custom Matrix Configuration

```python
@pytest.fixture(params=[
    ("financial_data", "sqlite_memory", "high_precision"),
    ("time_series", "duckdb_memory", "analytics_workload"),
    ("customer_data", "polars_df", "large_dataset")
])
def custom_matrix_config(request):
    data_scenario, backend, workload_type = request.param
    return {
        "data": data_scenario,
        "backend": backend,
        "workload": workload_type
    }

def test_custom_matrix_scenario(custom_matrix_config):
    """Test with custom matrix configuration."""
    # Implementation based on configuration
    pass
```

### Conditional Backend Testing

```python
def test_backend_specific_features(backend_config):
    """Test features specific to certain backends."""
    
    if backend_config["supports_sql"]:
        # Test SQL-specific operations
        result = df.sql("SELECT COUNT(*) FROM table")
        assert result is not None
    
    if "analytics" in backend_config["capabilities"]:
        # Test analytics operations
        grouped = df.group_by('category').agg({'amount': 'sum'})
        assert grouped is not None
```

### Matrix Test Debugging

```python
def test_matrix_with_debug_info(backend_config, performance_timer):
    """Matrix test with comprehensive debugging information."""
    
    print(f"Testing backend: {backend_config['type']}")
    print(f"Capabilities: {backend_config.get('capabilities', [])}")
    
    with performance_timer.time_operation("debug_test"):
        # Test operations with timing
        result = perform_test_operations()
    
    times = performance_timer.get_times()
    print(f"Operation completed in {times['debug_test']:.3f}s")
    
    return result
```

## Best Practices

### 1. **Test Organization**
- Keep matrix tests focused on cross-backend compatibility
- Use regular tests for backend-specific features
- Group related matrix tests in classes

### 2. **Performance Considerations**
- Use appropriate data sizes for different test types
- Implement timeout limits for long-running tests
- Mark performance tests appropriately

### 3. **Error Handling**
- Use `pytest.skip()` for known backend limitations
- Provide informative skip messages
- Handle expected failures gracefully

### 4. **Maintenance**
- Update matrix configurations when adding new backends
- Keep fixture data realistic and current
- Monitor test execution times and optimize as needed

## Troubleshooting

### Common Issues

1. **Import Errors**
   ```python
   # Ensure all backend dependencies are available
   try:
       from mountainash_data import SQLite_IbisConnection
   except ImportError:
       pytest.skip("SQLite backend not available")
   ```

2. **Performance Timeout**
   ```python
   @pytest.mark.timeout(300)  # 5 minute timeout
   def test_large_dataset_matrix():
       pass
   ```

3. **Memory Issues**
   ```python
   # Monitor memory usage
   if memory_monitor.get_stats()["current_mb"] > MEMORY_LIMIT:
       pytest.skip("Test exceeds memory limit")
   ```

### Debugging Matrix Tests

```bash
# Run single backend combination
pytest tests/test_backend_datastructure_matrix.py -k "sqlite_memory and financial" -v -s

# Debug with print statements
pytest tests/test_backend_datastructure_matrix.py --capture=no

# Profile performance
pytest tests/test_backend_datastructure_matrix.py --profile
```

This matrix testing framework provides comprehensive validation of the Mountain Ash Data package across all supported backends and data structures, ensuring reliability, performance, and consistency of the unified dataframe abstraction layer.