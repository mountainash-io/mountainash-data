"""Performance and stress testing fixtures.

This module provides fixtures designed to test performance characteristics,
memory usage, and scalability of data processing operations.
"""

import pytest
import random
import string
# import numpy as np  # Commented out - not in base requirements
import math
from datetime import datetime, date, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Any, Generator
from uuid import uuid4
import gc
import psutil
import os


@pytest.fixture(scope="session")
def large_dataset_configs():
    """Configuration parameters for large dataset generation."""
    return {
        "small": {"rows": 1_000, "cols": 10},
        "medium": {"rows": 10_000, "cols": 25},
        "large": {"rows": 100_000, "cols": 50},
        "xlarge": {"rows": 1_000_000, "cols": 100},

        # Memory stress scenarios
        "wide": {"rows": 1_000, "cols": 1_000},      # Many columns
        "tall": {"rows": 10_000_000, "cols": 5},     # Many rows
        "huge": {"rows": 1_000_000, "cols": 200},    # Both large
    }


@pytest.fixture
def performance_data_generator():
    """Generator for creating large datasets with controlled characteristics."""

    def generate_performance_dataset(
        rows: int = 10_000,
        cols: int = 10,
        data_types: List[str] = None,
        cardinality_pattern: str = "mixed",
        null_rate: float = 0.05,
        duplicate_rate: float = 0.02,
        memory_efficient: bool = True
    ) -> Dict[str, List[Any]]:
        """
        Generate large dataset for performance testing.

        Args:
            rows: Number of rows to generate
            cols: Number of columns to generate
            data_types: List of data types to cycle through
            cardinality_pattern: "high", "low", "mixed" - controls uniqueness
            null_rate: Proportion of null values
            duplicate_rate: Proportion of duplicate rows
            memory_efficient: Use memory-efficient generation patterns
        """
        if data_types is None:
            data_types = ["int", "float", "string", "date", "bool"]

        dataset = {}

        # Memory monitoring
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        print(f"Generating {rows:,} x {cols} dataset. Initial memory: {initial_memory:.1f} MB")

        for col_idx in range(cols):
            col_type = data_types[col_idx % len(data_types)]
            col_name = f"{col_type}_col_{col_idx:03d}"

            if memory_efficient:
                # Generate data in chunks to manage memory
                chunk_size = min(10_000, rows)
                column_data = []

                for chunk_start in range(0, rows, chunk_size):
                    chunk_end = min(chunk_start + chunk_size, rows)
                    chunk_data = _generate_column_chunk(
                        col_type, chunk_end - chunk_start,
                        cardinality_pattern, null_rate, chunk_start
                    )
                    column_data.extend(chunk_data)

                    # Periodic garbage collection
                    if chunk_start % 50_000 == 0 and chunk_start > 0:
                        gc.collect()

                dataset[col_name] = column_data
            else:
                # Generate all at once (less memory efficient)
                dataset[col_name] = _generate_column_chunk(
                    col_type, rows, cardinality_pattern, null_rate, 0
                )

        # Add duplicate rows if requested
        if duplicate_rate > 0:
            num_duplicates = int(rows * duplicate_rate)
            duplicate_indices = random.sample(range(max(1, rows-1)), num_duplicates)

            for dup_idx in duplicate_indices:
                source_idx = dup_idx - 1
                for col_name in dataset:
                    dataset[col_name][dup_idx] = dataset[col_name][source_idx]

        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_used = final_memory - initial_memory
        print(f"Dataset generated. Memory used: {memory_used:.1f} MB, Final memory: {final_memory:.1f} MB")

        return dataset

    def _generate_column_chunk(
        col_type: str,
        size: int,
        cardinality_pattern: str,
        null_rate: float,
        offset: int
    ) -> List[Any]:
        """Generate a chunk of data for a specific column type."""

        data = []

        # Determine cardinality based on pattern
        if cardinality_pattern == "high":
            unique_ratio = 0.95  # 95% unique values
        elif cardinality_pattern == "low":
            unique_ratio = 0.05  # 5% unique values (lots of repetition)
        else:  # mixed
            unique_ratio = 0.6   # 60% unique values

        unique_count = max(1, int(size * unique_ratio))

        # Generate unique values for this chunk
        unique_values = []
        for i in range(unique_count):
            if col_type == "int":
                unique_values.append(random.randint(-1_000_000, 1_000_000))
            elif col_type == "float":
                unique_values.append(round(random.uniform(-1000.0, 1000.0), 4))
            elif col_type == "string":
                length = random.randint(5, 50)
                unique_values.append(''.join(random.choices(
                    string.ascii_letters + string.digits + " ", k=length
                )))
            elif col_type == "date":
                base_date = date(2020, 1, 1)
                days_offset = random.randint(0, 1825)  # 5 years
                unique_values.append(base_date + timedelta(days=days_offset))
            elif col_type == "bool":
                unique_values.append(random.choice([True, False]))
            elif col_type == "decimal":
                unique_values.append(
                    Decimal(str(round(random.uniform(-1000.0, 1000.0), 4)))
                )
            else:
                unique_values.append(f"value_{i + offset}")

        # Generate data by sampling from unique values
        for i in range(size):
            if random.random() < null_rate:
                data.append(None)
            else:
                data.append(random.choice(unique_values))

        return data

    return generate_performance_dataset


@pytest.fixture(scope="session")
def time_series_performance_data():
    """Large time series dataset for performance testing."""

    # Generate 1 year of minute-by-minute data (525,600 records)
    start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    def generate_time_series(
        duration_days: int = 365,
        frequency_minutes: int = 1,
        sensors: int = 10,
        add_noise: bool = True,
        missing_data_rate: float = 0.01
    ):
        """Generate large time series dataset."""

        total_minutes = duration_days * 24 * 60 // frequency_minutes
        timestamps = []
        sensor_data = {f"sensor_{i:02d}": [] for i in range(sensors)}

        print(f"Generating {total_minutes:,} time series records for {sensors} sensors")

        # Generate base patterns for each sensor
        sensor_patterns = {}
        for i in range(sensors):
            # Each sensor has different base patterns
            sensor_patterns[f"sensor_{i:02d}"] = {
                "base_value": random.uniform(50, 200),
                "trend": random.uniform(-0.001, 0.001),  # Daily trend
                "seasonal_amplitude": random.uniform(5, 20),  # Daily seasonality
                "noise_level": random.uniform(1, 5)
            }

        # Generate data in chunks
        chunk_size = 10_000
        for chunk_start in range(0, total_minutes, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_minutes)

            for i in range(chunk_start, chunk_end):
                timestamp = start_time + timedelta(minutes=i * frequency_minutes)
                timestamps.append(timestamp)

                # Skip some data points to simulate missing data
                if random.random() < missing_data_rate:
                    for sensor_id in sensor_data:
                        sensor_data[sensor_id].append(None)
                    continue

                for sensor_id, pattern in sensor_patterns.items():
                    # Generate realistic sensor reading
                    base = pattern["base_value"]
                    trend = pattern["trend"] * i
                    seasonal = pattern["seasonal_amplitude"] * math.sin(2 * math.pi * i / (24 * 60 / frequency_minutes))  # Daily cycle
                    noise = random.gauss(0, pattern["noise_level"]) if add_noise else 0

                    # Add occasional spikes/anomalies
                    if random.random() < 0.001:  # 0.1% chance of anomaly
                        anomaly = random.choice([-1, 1]) * pattern["base_value"] * random.uniform(0.5, 2.0)
                    else:
                        anomaly = 0

                    value = base + trend + seasonal + noise + anomaly
                    sensor_data[sensor_id].append(round(value, 3))

            # Periodic cleanup
            if chunk_start % 100_000 == 0 and chunk_start > 0:
                gc.collect()

        # Combine into final dataset
        result = {"timestamp": timestamps}
        result.update(sensor_data)

        print(f"Time series dataset generated: {len(timestamps):,} records")
        return result

    return generate_time_series


@pytest.fixture(scope="session")
def wide_table_data():
    """Dataset with many columns (wide table) for testing column operations."""

    def generate_wide_table(
        rows: int = 10_000,
        columns: int = 500,
        column_types: Dict[str, int] = None
    ):
        """Generate wide table with many columns."""

        if column_types is None:
            column_types = {
                "int": 100,
                "float": 100,
                "string": 150,
                "date": 50,
                "bool": 50,
                "decimal": 50
            }

        dataset = {}
        col_counter = 0

        print(f"Generating wide table: {rows:,} rows × {columns} columns")

        for col_type, count in column_types.items():
            for i in range(count):
                if col_counter >= columns:
                    break

                col_name = f"{col_type}_{i:04d}"

                if col_type == "int":
                    dataset[col_name] = [
                        random.randint(-1000, 1000) if random.random() > 0.05 else None
                        for _ in range(rows)
                    ]
                elif col_type == "float":
                    dataset[col_name] = [
                        round(random.uniform(-100.0, 100.0), 3) if random.random() > 0.05 else None
                        for _ in range(rows)
                    ]
                elif col_type == "string":
                    categories = [f"category_{j}" for j in range(min(20, rows // 10))]
                    dataset[col_name] = [
                        random.choice(categories) if random.random() > 0.05 else None
                        for _ in range(rows)
                    ]
                elif col_type == "date":
                    base_date = date(2020, 1, 1)
                    dataset[col_name] = [
                        base_date + timedelta(days=random.randint(0, 1825)) if random.random() > 0.05 else None
                        for _ in range(rows)
                    ]
                elif col_type == "bool":
                    dataset[col_name] = [
                        random.choice([True, False]) if random.random() > 0.05 else None
                        for _ in range(rows)
                    ]
                elif col_type == "decimal":
                    dataset[col_name] = [
                        Decimal(str(round(random.uniform(-100.0, 100.0), 4))) if random.random() > 0.05 else None
                        for _ in range(rows)
                    ]

                col_counter += 1

        print(f"Wide table generated: {len(dataset)} columns")
        return dataset

    return generate_wide_table


@pytest.fixture(scope="session")
def high_cardinality_data():
    """Dataset with high cardinality columns for testing performance with many unique values."""

    def generate_high_cardinality(rows: int = 100_000):
        """Generate dataset with high cardinality columns."""

        print(f"Generating high cardinality dataset: {rows:,} rows")

        return {
            # Very high cardinality - almost all unique
            "uuid_column": [str(uuid4()) for _ in range(rows)],
            "sequential_id": list(range(rows)),
            "unique_string": [
                f"unique_{i}_{random.randint(1000, 9999)}"
                for i in range(rows)
            ],

            # High cardinality - many unique values
            "user_id": [
                f"USER_{random.randint(1, rows // 2):08d}"
                for _ in range(rows)
            ],
            "email_domain": [
                f"user{i % (rows // 100)}@domain{random.randint(1, 1000)}.com"
                for i in range(rows)
            ],

            # Medium cardinality
            "category": [
                f"category_{random.randint(1, rows // 1000)}"
                for _ in range(rows)
            ],

            # Low cardinality
            "status": [
                random.choice(["active", "inactive", "pending", "suspended"])
                for _ in range(rows)
            ],

            # Mixed cardinality with nulls
            "optional_field": [
                f"value_{random.randint(1, rows // 10)}" if random.random() > 0.3 else None
                for _ in range(rows)
            ]
        }

    return generate_high_cardinality


@pytest.fixture(scope="session")
def string_heavy_data():
    """Dataset with large strings for testing string processing performance."""

    def generate_string_heavy(rows: int = 10_000):
        """Generate dataset with various string sizes."""

        def generate_text(min_words: int, max_words: int) -> str:
            """Generate random text with specified word count."""
            words = [
                "lorem", "ipsum", "dolor", "sit", "amet", "consectetur",
                "adipiscing", "elit", "sed", "do", "eiusmod", "tempor",
                "incididunt", "ut", "labore", "et", "dolore", "magna",
                "aliqua", "enim", "ad", "minim", "veniam", "quis"
            ]
            word_count = random.randint(min_words, max_words)
            return " ".join(random.choices(words, k=word_count))

        print(f"Generating string-heavy dataset: {rows:,} rows")

        return {
            # Short strings
            "short_text": [
                generate_text(1, 5) if random.random() > 0.05 else None
                for _ in range(rows)
            ],

            # Medium strings
            "medium_text": [
                generate_text(20, 100) if random.random() > 0.05 else None
                for _ in range(rows)
            ],

            # Long strings
            "long_text": [
                generate_text(200, 1000) if random.random() > 0.1 else None
                for _ in range(rows)
            ],

            # Very long strings (only for some records)
            "very_long_text": [
                generate_text(1000, 5000) if random.random() < 0.1 else None
                for _ in range(rows)
            ],

            # JSON strings
            "json_data": [
                f'{{"id": {i}, "data": "{generate_text(5, 20)}", "nested": {{"value": {random.randint(1, 1000)}}}}}'
                if random.random() > 0.05 else None
                for i in range(rows)
            ],

            # Repetitive strings (low cardinality)
            "repeated_text": [
                generate_text(10, 50) if i < 100 else
                random.choice([generate_text(10, 50) for _ in range(100)])
                for i in range(rows)
            ]
        }

    return generate_string_heavy


@pytest.fixture
def memory_monitor():
    """Monitor memory usage during test execution."""

    class MemoryMonitor:
        def __init__(self):
            self.process = psutil.Process(os.getpid())
            self.initial_memory = self.get_memory_mb()
            self.peak_memory = self.initial_memory

        def get_memory_mb(self) -> float:
            """Get current memory usage in MB."""
            return self.process.memory_info().rss / 1024 / 1024

        def update_peak(self):
            """Update peak memory usage."""
            current = self.get_memory_mb()
            if current > self.peak_memory:
                self.peak_memory = current

        def get_stats(self) -> Dict[str, float]:
            """Get memory statistics."""
            current = self.get_memory_mb()
            return {
                "initial_mb": self.initial_memory,
                "current_mb": current,
                "peak_mb": self.peak_memory,
                "used_mb": current - self.initial_memory,
                "peak_increase_mb": self.peak_memory - self.initial_memory
            }

        def print_stats(self):
            """Print memory statistics."""
            stats = self.get_stats()
            print(f"Memory - Initial: {stats['initial_mb']:.1f} MB, "
                  f"Current: {stats['current_mb']:.1f} MB, "
                  f"Peak: {stats['peak_mb']:.1f} MB, "
                  f"Used: {stats['used_mb']:.1f} MB")

    return MemoryMonitor()


@pytest.fixture(scope="session")
def benchmark_datasets():
    """Predefined benchmark datasets for consistent performance testing."""

    # Create standard benchmark sizes
    benchmarks = {}

    # Micro benchmark - very fast operations
    benchmarks["micro"] = {
        "rows": 100,
        "cols": 5,
        "description": "Micro benchmark for basic operations"
    }

    # Small benchmark - typical test size
    benchmarks["small"] = {
        "rows": 10_000,
        "cols": 10,
        "description": "Small benchmark for development testing"
    }

    # Medium benchmark - realistic data size
    benchmarks["medium"] = {
        "rows": 100_000,
        "cols": 25,
        "description": "Medium benchmark for realistic workloads"
    }

    # Large benchmark - stress testing
    benchmarks["large"] = {
        "rows": 1_000_000,
        "cols": 50,
        "description": "Large benchmark for performance validation"
    }

    # Wide benchmark - many columns
    benchmarks["wide"] = {
        "rows": 10_000,
        "cols": 500,
        "description": "Wide benchmark for column operations"
    }

    # Tall benchmark - many rows
    benchmarks["tall"] = {
        "rows": 10_000_000,
        "cols": 5,
        "description": "Tall benchmark for row operations"
    }

    return benchmarks


@pytest.fixture
def performance_timer():
    """Timer utility for measuring operation performance."""

    import time
    from contextlib import contextmanager

    class PerformanceTimer:
        def __init__(self):
            self.times = {}

        @contextmanager
        def time_operation(self, operation_name: str):
            """Context manager to time operations."""
            start_time = time.perf_counter()
            try:
                yield
            finally:
                end_time = time.perf_counter()
                duration = end_time - start_time
                self.times[operation_name] = duration
                print(f"{operation_name}: {duration:.4f} seconds")

        def get_times(self) -> Dict[str, float]:
            """Get all recorded times."""
            return self.times.copy()

        def print_summary(self):
            """Print summary of all timed operations."""
            if not self.times:
                print("No operations timed.")
                return

            print("\\nPerformance Summary:")
            print("-" * 40)
            total_time = 0
            for operation, duration in self.times.items():
                print(f"{operation:<30} {duration:>8.4f}s")
                total_time += duration
            print("-" * 40)
            print(f"{'Total Time':<30} {total_time:>8.4f}s")

    return PerformanceTimer()
