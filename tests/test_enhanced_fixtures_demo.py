"""Demo tests showcasing the enhanced fixture capabilities.

This module demonstrates how to use the enhanced fixtures for various
testing scenarios. These tests serve as both documentation and validation
of the fixture functionality.
"""

import pytest
from datetime import datetime, date
from decimal import Decimal
import polars as pl
import pandas as pd

# Import the data processing utilities we're testing
from mountainash_data.dataframes.utils.dataframe_factory import DataFrameFactory
from mountainash_data.dataframes.ibis_dataframe import IbisDataFrame


class TestRealisticDataScenarios:
    """Tests demonstrating realistic business data scenarios."""
    
    def test_financial_transaction_processing(self, financial_transactions_data):
        """Test financial transaction processing with realistic data."""
        # Test that we can process multi-currency transactions
        transactions = financial_transactions_data
        
        # Verify data quality
        assert len(transactions['transaction_id']) == 100
        assert len(set(transactions['transaction_id'])) == 100  # All unique IDs
        
        # Test currency distribution
        currencies = set(transactions['currency'])
        assert 'USD' in currencies
        assert 'EUR' in currencies
        assert len(currencies) >= 3
        
        # Test date range processing
        dates = [d for d in transactions['transaction_date'] if d is not None]
        assert len(dates) > 0
        assert all(isinstance(d, datetime) for d in dates)
        
        # Test amount processing with Decimal precision
        amounts = [a for a in transactions['amount'] if a is not None]
        assert all(isinstance(a, Decimal) for a in amounts)
        
        # Test transaction type categorization
        types = set(transactions['transaction_type'])
        expected_types = {'DEPOSIT', 'WITHDRAWAL', 'TRANSFER', 'PAYMENT', 'REFUND'}
        assert types.issubset(expected_types)
    
    def test_ecommerce_order_analysis(self, ecommerce_orders_data):
        """Test e-commerce order analysis with realistic order data."""
        orders = ecommerce_orders_data
        
        # Test order processing
        assert len(orders['order_id']) == 200
        
        # Test customer relationship patterns
        customers = set(orders['customer_id'])
        assert len(customers) > 0
        assert len(customers) < 200  # Some customers have multiple orders
        
        # Test order status distribution
        statuses = set(orders['order_status'])
        expected_statuses = {'PENDING', 'CONFIRMED', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'RETURNED'}
        assert statuses.issubset(expected_statuses)
        
        # Test financial calculations
        unit_prices = [p for p in orders['unit_price'] if p is not None]
        quantities = [q for q in orders['quantity'] if q is not None]
        assert all(p > 0 for p in unit_prices)
        assert all(q > 0 for q in quantities)
    
    def test_hierarchical_organization_data(self, hierarchical_org_data):
        """Test hierarchical organizational data processing."""
        org_data = hierarchical_org_data
        
        # Test organizational structure
        assert len(org_data['employee_id']) == 150
        
        # Test department hierarchy
        departments = set(org_data['department'])
        parent_departments = set(d for d in org_data['parent_department'] if d is not None)
        
        # Verify hierarchy relationships
        assert 'Engineering' in departments
        assert 'Frontend' in departments
        assert 'Backend' in departments
        assert 'Engineering' in parent_departments  # Engineering is a parent
        
        # Test employee relationships
        managers = set(m for m in org_data['manager_id'] if m is not None)
        employees = set(org_data['employee_id'])
        assert managers.issubset(employees)  # All managers are employees
        
        # Test salary data
        salaries = [s for s in org_data['salary'] if s is not None]
        assert all(isinstance(s, int) and s > 0 for s in salaries)


class TestEdgeCaseHandling:
    """Tests demonstrating edge case and boundary condition handling."""
    
    def test_numeric_boundary_processing(self, numeric_boundary_data):
        """Test processing of numeric boundary values."""
        # Test integer boundaries
        integers = numeric_boundary_data['integers']
        assert 0 in integers
        assert 1 in integers
        assert -1 in integers
        
        # Test that we can handle system limits
        import sys
        assert sys.maxsize in integers
        assert -sys.maxsize - 1 in integers
        
        # Test float boundaries
        floats = numeric_boundary_data['floats']
        assert 0.0 in floats
        assert -0.0 in floats
        
        # Test special float values
        import math
        special_values = [f for f in floats if not math.isnan(f) and math.isinf(f)]
        assert len(special_values) > 0  # Should have inf values
        
        # Test Decimal precision
        decimals = numeric_boundary_data['decimals']
        high_precision = [d for d in decimals if d is not None and d.as_tuple().exponent < -10]
        assert len(high_precision) > 0  # Should have high precision decimals
    
    def test_string_boundary_conditions(self, string_boundary_data):
        """Test string processing with edge cases."""
        # Test empty and whitespace handling
        empty_strings = string_boundary_data['empty_and_whitespace']
        assert "" in empty_strings
        assert " " in empty_strings
        assert "\\t" in empty_strings
        
        # Test length boundaries
        length_boundaries = string_boundary_data['length_boundaries']
        single_char = [s for s in length_boundaries if len(s) == 1]
        assert len(single_char) > 0
        
        long_strings = [s for s in length_boundaries if len(s) > 1000]
        assert len(long_strings) > 0
        
        # Test Unicode handling
        unicode_cases = string_boundary_data['unicode_edge_cases']
        unicode_strings = [s for s in unicode_cases if any(ord(c) > 127 for c in s)]
        assert len(unicode_strings) > 0
    
    def test_datetime_boundary_conditions(self, datetime_boundary_data):
        """Test datetime processing with boundary conditions."""
        # Test historical dates
        historical = datetime_boundary_data['historical_dates']
        assert date(1970, 1, 1) in historical  # Unix epoch
        assert date(2000, 1, 1) in historical  # Y2K
        
        # Test leap year handling
        leap_scenarios = datetime_boundary_data['leap_year_scenarios']
        assert date(2000, 2, 29) in leap_scenarios  # Leap day
        assert date(1900, 2, 28) in leap_scenarios  # Non-leap century
        
        # Test timezone handling
        tz_datetimes = datetime_boundary_data['datetimes_with_timezone']
        utc_times = [dt for dt in tz_datetimes if dt is not None and dt.tzinfo is not None]
        assert len(utc_times) > 0
    
    def test_null_and_missing_patterns(self, null_and_missing_data):
        """Test various null and missing data patterns."""
        # Test explicit nulls
        explicit_nulls = null_and_missing_data['explicit_nulls']
        assert all(v is None for v in explicit_nulls)
        
        # Test mixed null patterns
        mixed = null_and_missing_data['mixed_with_nulls']
        null_count = mixed.count(None)
        non_null_count = len(mixed) - null_count
        assert null_count > 0 and non_null_count > 0
        
        # Test sparse data patterns
        sparse = null_and_missing_data['sparse_data']
        assert sparse[0] == 1  # First value
        assert sparse[-1] == 10  # Last value
        assert None in sparse  # Contains nulls
        
        # Test mostly null scenario
        mostly_null = null_and_missing_data['mostly_null']
        null_ratio = mostly_null.count(None) / len(mostly_null)
        assert null_ratio > 0.8  # More than 80% nulls


class TestDataTypeVariety:
    """Tests demonstrating comprehensive data type handling."""
    
    def test_comprehensive_data_types(self, comprehensive_data_types):
        """Test processing of all major data types."""
        data_types = comprehensive_data_types
        
        # Test integer handling
        integers = [i for i in data_types['integers'] if i is not None]
        assert len(integers) > 0
        assert all(isinstance(i, int) for i in integers)
        
        # Test float handling
        floats = [f for f in data_types['floats'] if f is not None and not pd.isna(f)]
        assert len(floats) > 0
        
        # Test string handling
        strings = [s for s in data_types['strings'] if s is not None]
        assert len(strings) > 0
        assert all(isinstance(s, str) for s in strings)
        
        # Test date handling
        dates = [d for d in data_types['dates'] if d is not None]
        assert len(dates) > 0
        assert all(isinstance(d, date) for d in dates)
        
        # Test boolean handling
        booleans = [b for b in data_types['booleans'] if b is not None]
        assert len(booleans) > 0
        assert all(isinstance(b, bool) for b in booleans)
    
    def test_temporal_data_patterns(self, temporal_data_comprehensive):
        """Test various temporal data patterns."""
        temporal = temporal_data_comprehensive
        
        # Test hourly time series
        hourly = temporal['hourly_series']
        timestamps = hourly['timestamp']
        values = hourly['value']
        
        assert len(timestamps) == len(values)
        assert len(timestamps) == 168  # One week of hourly data
        
        # Test irregular series (with missing periods)
        irregular = temporal['irregular_series']
        assert len(irregular['timestamp']) < 168  # Should have some missing periods
        
        # Test high-frequency data
        high_freq = temporal['high_frequency']
        assert len(high_freq['timestamp']) == 3600  # One hour of per-second data
    
    def test_data_type_factory(self, data_type_factory):
        """Test dynamic data generation with type factory."""
        # Generate integer data
        int_data = data_type_factory('integer', size=100, null_rate=0.1)
        assert len(int_data) == 100
        
        non_null_ints = [i for i in int_data if i is not None]
        assert len(non_null_ints) > 80  # Approximately 90% non-null
        assert all(isinstance(i, int) for i in non_null_ints)
        
        # Generate string data
        string_data = data_type_factory('string', size=50, length=10)
        assert len(string_data) == 50
        
        non_null_strings = [s for s in string_data if s is not None]
        assert all(isinstance(s, str) for s in non_null_strings)
        assert all(len(s) <= 10 for s in non_null_strings)
        
        # Generate date data
        date_data = data_type_factory('date', size=30)
        assert len(date_data) == 30
        
        non_null_dates = [d for d in date_data if d is not None]
        assert all(isinstance(d, date) for d in non_null_dates)


class TestPerformanceAndScalability:
    """Tests demonstrating performance testing capabilities."""
    
    @pytest.mark.performance
    def test_memory_monitoring(self, memory_monitor):
        """Test memory monitoring during operations."""
        initial_stats = memory_monitor.get_stats()
        
        # Simulate memory-intensive operation
        large_list = list(range(100_000))
        memory_monitor.update_peak()
        
        # Verify memory tracking
        final_stats = memory_monitor.get_stats()
        assert final_stats['current_mb'] >= initial_stats['initial_mb']
        assert final_stats['peak_mb'] >= final_stats['current_mb']
    
    @pytest.mark.performance  
    def test_performance_timing(self, performance_timer):
        """Test operation timing."""
        with performance_timer.time_operation("list_creation"):
            data = list(range(10_000))
            
        with performance_timer.time_operation("list_sorting"):
            sorted_data = sorted(data, reverse=True)
        
        times = performance_timer.get_times()
        assert 'list_creation' in times
        assert 'list_sorting' in times
        assert times['list_creation'] > 0
        assert times['list_sorting'] > 0
    
    @pytest.mark.performance
    def test_large_dataset_configs(self, large_dataset_configs):
        """Test large dataset configuration handling."""
        configs = large_dataset_configs
        
        # Test configuration structure
        assert 'small' in configs
        assert 'medium' in configs
        assert 'large' in configs
        
        # Test configuration values
        small_config = configs['small']
        assert small_config['rows'] == 1_000
        assert small_config['cols'] == 10
        
        large_config = configs['large'] 
        assert large_config['rows'] == 100_000
        assert large_config['cols'] == 50


class TestDataFrameIntegration:
    """Tests demonstrating integration with DataFrames using enhanced fixtures."""
    
    def test_realistic_data_with_polars(self, financial_transactions_data):
        """Test Polars DataFrame creation with realistic financial data."""
        # Create Polars DataFrame from realistic financial data
        df = pl.DataFrame(financial_transactions_data)
        
        # Test basic operations
        assert len(df) == 100
        assert 'transaction_id' in df.columns
        assert 'amount' in df.columns
        
        # Test filtering by currency
        usd_transactions = df.filter(pl.col('currency') == 'USD')
        assert len(usd_transactions) > 0
        
        # Test aggregation by transaction type
        type_summary = df.group_by('transaction_type').agg([
            pl.len().alias('count'),
            pl.col('amount').sum().alias('total_amount')
        ])
        assert len(type_summary) > 0
    
    def test_edge_cases_with_pandas(self, string_boundary_data):
        """Test Pandas DataFrame handling of string edge cases."""
        # Create DataFrame with problematic strings
        df = pd.DataFrame({
            'id': range(len(string_boundary_data['unicode_edge_cases'])),
            'text': string_boundary_data['unicode_edge_cases']
        })
        
        # Test basic operations don't crash
        assert len(df) > 0
        
        # Test string operations handle Unicode
        non_null_text = df['text'].dropna()
        if len(non_null_text) > 0:
            lengths = non_null_text.str.len()
            assert len(lengths) > 0
    
    def test_data_quality_with_dataframe_factory(self, data_quality_issues_data):
        """Test data quality detection with DataFrame factory."""
        # Use the factory to create a DataFrame with known quality issues
        try:
            df = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(data_quality_issues_data)
            
            # Test that we can identify duplicates
            if hasattr(df, 'materialise'):
                polars_df = df.materialise('polars')
                
                # Check for duplicate customer IDs (we know there are some)
                customer_counts = polars_df.group_by('customer_id').agg(pl.len().alias('count'))
                duplicates = customer_counts.filter(pl.col('count') > 1)
                assert len(duplicates) > 0  # Should find duplicate customer IDs
                
        except Exception as e:
            # Some data quality issues might prevent DataFrame creation
            # which is also a valid test result
            assert "quality" in str(e).lower() or "validation" in str(e).lower()


# Utility functions for demonstration
def process_integer(value):
    """Example integer processing function."""
    try:
        return int(value) * 2
    except (ValueError, TypeError, OverflowError):
        return None


def process_text(text):
    """Example text processing function."""
    try:
        return str(text).strip().lower()
    except (AttributeError, UnicodeError):
        return None