# Enhanced Test Fixtures for mountainash-data

This directory contains comprehensive test fixtures designed to improve test coverage, realism, and edge case handling for the mountainash-data project.

## Overview

The fixture collection is organized into four main categories:

1. **Realistic Data Scenarios** (`realistic_data_fixtures.py`)
2. **Edge Cases & Boundary Conditions** (`edge_case_fixtures.py`)  
3. **Data Type Variety** (`data_type_fixtures.py`)
4. **Performance & Stress Testing** (`performance_fixtures.py`)

## Fixture Categories

### 1. Realistic Data Scenarios (`realistic_data_fixtures.py`)

Provides business-domain fixtures that simulate real-world data scenarios:

#### Business Domain Fixtures
- `financial_transactions_data()` - Multi-currency transactions with complex scenarios
- `ecommerce_orders_data()` - E-commerce orders with customer and product relationships
- `hierarchical_org_data()` - Organizational data with parent-child relationships
- `geographic_data()` - Address and location data with coordinates and hierarchies
- `user_behavior_data()` - Event tracking and analytics data
- `audit_trail_data()` - Compliance and change tracking data

#### Data Quality Testing
- `data_quality_issues_data()` - Intentional data quality problems (duplicates, inconsistencies)
- `data_generator()` - Factory fixture for generating custom datasets

#### Key Features:
- **Realistic relationships** between entities
- **Multi-currency and multi-timezone** support  
- **Hierarchical data structures** (organizations, locations)
- **Complex business rules** (e.g., financial calculations, tax scenarios)
- **Audit trails** for compliance testing

### 2. Edge Cases & Boundary Conditions (`edge_case_fixtures.py`)

Tests boundary values and edge conditions that can break systems:

#### Numeric Boundaries
- `numeric_boundary_data()` - Integer/float limits, precision issues, special values (inf, nan)

#### String Boundaries  
- `string_boundary_data()` - Empty strings, very long strings, Unicode edge cases
- `encoding_and_character_edge_cases()` - Character encoding problems, injection attempts

#### DateTime Boundaries
- `datetime_boundary_data()` - Historical dates, leap years, timezone transitions
- `concurrent_access_scenarios()` - Race conditions and version conflicts

#### Memory & Performance Boundaries
- `memory_stress_boundaries()` - Large strings, wide/tall data formats
- `schema_evolution_data()` - Schema changes and compatibility issues

#### Key Features:
- **System limits testing** (max integer, float precision)
- **Unicode and encoding** edge cases
- **Date/time boundaries** (leap years, DST transitions)
- **Injection attack patterns** for security testing
- **Concurrent access** scenarios

### 3. Data Type Variety (`data_type_fixtures.py`)

Comprehensive coverage of different data types and conversion scenarios:

#### Core Data Types
- `comprehensive_data_types()` - All major data types with realistic values
- `temporal_data_comprehensive()` - Time series with various patterns and frequencies

#### Null Handling
- `null_patterns_comprehensive()` - Various null patterns and missing data scenarios

#### Type Conversions
- `data_type_conversions()` - String-to-numeric, boolean variations, date formats
- `mixed_type_scenarios()` - Columns with inconsistent types

#### Factory Fixtures
- `data_type_factory()` - Generate data with specific type patterns
- `complex_nested_data()` - JSON, hierarchical, and nested structures

#### Key Features:
- **Complete type coverage** (numeric, string, date, boolean, UUID, JSON, etc.)
- **Null value patterns** (sparse, clustered, alternating)
- **Type coercion scenarios** for robust conversion testing
- **Nested and complex structures** (JSON, arrays, hierarchies)
- **High precision decimals** and financial calculations

### 4. Performance & Stress Testing (`performance_fixtures.py`)

Large datasets and performance monitoring for scalability testing:

#### Large Dataset Generation
- `performance_data_generator()` - Configurable large dataset generation
- `large_dataset_configs()` - Predefined size configurations (small to xlarge)
- `time_series_performance_data()` - High-frequency time series data
- `wide_table_data()` - Tables with many columns
- `high_cardinality_data()` - Datasets with many unique values
- `string_heavy_data()` - Large text fields and string processing

#### Monitoring & Benchmarking
- `memory_monitor()` - Track memory usage during tests
- `performance_timer()` - Time operation performance
- `benchmark_datasets()` - Standard benchmark configurations

#### Key Features:
- **Configurable dataset sizes** (1K to 10M+ records)
- **Memory-efficient generation** with chunking and cleanup
- **Real-time monitoring** of memory usage and performance
- **High-cardinality scenarios** for index and query testing
- **String processing stress tests** with large text fields

## Usage Examples

### Using Realistic Business Data

```python
def test_financial_transactions(financial_transactions_data):
    \"\"\"Test with realistic financial data.\"\"\"
    df = DataFrameFactory.create_from_dict(financial_transactions_data)
    
    # Test currency conversion
    usd_transactions = df.filter(df['currency'] == 'USD')
    assert len(usd_transactions) > 0
    
    # Test date range filtering
    recent = df.filter(df['transaction_date'] > datetime(2024, 6, 1))
    assert len(recent) > 0
```

### Using Edge Case Data

```python
def test_numeric_boundaries(numeric_boundary_data):
    \"\"\"Test with numeric boundary conditions.\"\"\"
    # Test integer boundaries
    for value in numeric_boundary_data['integers']:
        if value is not None:
            result = process_integer(value)
            assert result is not None

def test_string_boundaries(string_boundary_data):
    \"\"\"Test string processing with edge cases.\"\"\"
    for text in string_boundary_data['unicode_edge_cases']:
        result = process_text(text)
        assert isinstance(result, str) or result is None
```

### Using Data Type Variety

```python
def test_comprehensive_types(comprehensive_data_types):
    \"\"\"Test with all data types.\"\"\"
    df = create_dataframe(comprehensive_data_types)
    
    # Test type conversions
    assert df['integers'].dtype in [int, 'Int64']
    assert df['floats'].dtype in [float, 'Float64']
    assert df['strings'].dtype == 'object'

def test_custom_data_generation(data_type_factory):
    \"\"\"Test with dynamically generated data.\"\"\"
    # Generate 1000 integers with 10% nulls
    data = data_type_factory('integer', size=1000, null_rate=0.1)
    assert len(data) == 1000
    assert data.count(None) > 50  # Approximately 10% nulls
```

### Using Performance Fixtures

```python
def test_large_dataset_performance(performance_data_generator, memory_monitor):
    \"\"\"Test performance with large datasets.\"\"\"
    # Generate 100K records with mixed types
    dataset = performance_data_generator(
        rows=100_000, 
        cols=20, 
        cardinality_pattern="mixed"
    )
    
    df = create_dataframe(dataset)
    memory_monitor.update_peak()
    
    # Perform operations
    result = df.filter(df['int_col_000'] > 0).count()
    
    # Check memory usage
    stats = memory_monitor.get_stats()
    assert stats['peak_increase_mb'] < 500  # Memory usage limit
```

## Integration with Existing Tests

All fixtures are automatically available in tests through the updated `conftest.py`. Key fixtures are imported globally, so you can use them directly:

```python
def test_example(financial_transactions_data, memory_monitor):
    \"\"\"Example test using enhanced fixtures.\"\"\"
    # Fixtures are automatically available
    pass
```

## Best Practices

### 1. Choose Appropriate Fixtures
- Use **realistic data** for business logic testing
- Use **edge cases** for robustness testing  
- Use **performance fixtures** for scalability testing
- Use **data type variety** for conversion and compatibility testing

### 2. Memory Management
- Use `memory_monitor` fixture for tracking memory usage
- Be cautious with very large datasets in CI environments
- Consider using smaller dataset configurations for faster feedback

### 3. Performance Testing
- Use `performance_timer` to measure operation times
- Compare against baseline performance metrics
- Test with various dataset sizes to identify scalability issues

### 4. Data Quality Testing
- Use `data_quality_issues_data` to test validation and cleaning logic
- Test both detection and correction of data quality problems
- Include data profiling and validation in your test suite

## Configuration

### Environment Variables
- `MOUNTAINASH_TEST_MEMORY_LIMIT` - Maximum memory usage for performance tests (default: 1GB)
- `MOUNTAINASH_TEST_SIZE_LIMIT` - Maximum dataset size for CI (default: 100K records)
- `MOUNTAINASH_ENABLE_LARGE_TESTS` - Enable large dataset tests (default: False in CI)

### Fixture Configuration
Most fixtures accept configuration parameters for customization:

```python
# Configure dataset size
dataset = performance_data_generator(
    rows=50_000, 
    cols=15,
    null_rate=0.05,
    memory_efficient=True
)

# Configure data types
custom_data = data_type_factory(
    'string', 
    size=1000, 
    length=20, 
    chars='abcdefghijklmnopqrstuvwxyz'
)
```

## Contributing

When adding new fixtures:

1. **Follow the modular organization** - put fixtures in the appropriate category file
2. **Document thoroughly** - include docstrings explaining the fixture's purpose
3. **Consider performance** - use memory-efficient patterns for large datasets
4. **Add realistic scenarios** - base fixtures on real-world use cases
5. **Include edge cases** - test boundary conditions and error scenarios
6. **Update this README** - document new fixtures and usage patterns

## Dependencies

The enhanced fixtures require additional dependencies beyond the base test suite:

- `numpy` - For realistic data generation patterns
- `psutil` - For memory monitoring
- Standard library modules: `random`, `string`, `uuid`, `decimal`, `datetime`

All dependencies are included in the existing project requirements.