#!/usr/bin/env python3
"""
Cost estimation script for function kernels.

This script benchmarks function kernels and estimates their execution cost
in microseconds per million rows, which is stored in the function catalog.

Usage:
    python estimate_function_costs.py [--functions FUNC1,FUNC2,...] [--output report.json]

The script:
1. Generates synthetic test data for various types
2. Benchmarks each function kernel with different input sizes
3. Calculates cost per million rows
4. Exports results as JSON or updates the catalog directly
"""

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

import numpy as np
import pyarrow as pa
from opteryx.expression.functions.catalog import get_catalog
from opteryx.types import OrsoTypes

sys.path.insert(1, os.path.join(sys.path[0], "../../../mabel/orso"))
sys.path.insert(1, os.path.join(sys.path[0], ".."))
sys.path.insert(1, os.path.join(sys.path[0], "../../pyiceberg-firestore-gcs"))




@dataclass
class CostEstimate:
    """Result of benchmarking a function kernel."""

    function_name: str
    kernel_id: str
    engine: str
    cost_us_per_million: float
    sample_size: int
    runs: int
    min_us: float
    max_us: float
    avg_us: float
    success: bool
    error_message: Optional[str] = None


class TestDataGenerator:
    """Generate synthetic test data for benchmarking."""

    @staticmethod
    def generate_scalars(type_: OrsoTypes, size: int) -> List[Any]:
        """Generate scalar values of the given type."""
        if type_ == OrsoTypes.INTEGER:
            return list(np.random.randint(0, 1000, size))
        elif type_ == OrsoTypes.DOUBLE:
            return list(np.random.uniform(0, 1000, size))
        elif type_ == OrsoTypes.BOOLEAN:
            return [bool(x % 2) for x in range(size)]
        elif type_ == OrsoTypes.VARCHAR:
            return [f"string_{i}" for i in range(size)]
        elif type_ == OrsoTypes.DATE:
            from datetime import datetime
            from datetime import timedelta

            base = datetime(2020, 1, 1)
            return [(base + timedelta(days=i)).date() for i in range(size)]
        elif type_ == OrsoTypes.TIMESTAMP:
            from datetime import datetime
            from datetime import timedelta

            base = datetime(2020, 1, 1, 0, 0, 0)
            return [(base + timedelta(hours=i)).replace(microsecond=0) for i in range(size)]
        elif type_ == OrsoTypes.NULL:
            return [None] * size
        else:
            # Fallback for unknown types
            return [None] * size

    @staticmethod
    def create_arrow_array(type_: OrsoTypes, size: int) -> pa.Array:
        """Create a PyArrow array of the given type with synthetic data."""
        scalars = TestDataGenerator.generate_scalars(type_, size)

        # Map OrsoTypes to PyArrow types
        pa_type_map = {
            OrsoTypes.INTEGER: pa.int64(),
            OrsoTypes.DOUBLE: pa.float64(),
            OrsoTypes.BOOLEAN: pa.bool_(),
            OrsoTypes.VARCHAR: pa.string(),
            OrsoTypes.DATE: pa.date32(),
            OrsoTypes.TIMESTAMP: pa.timestamp("us"),
            OrsoTypes.NULL: pa.null(),
        }

        arrow_type = pa_type_map.get(type_, pa.null())
        try:
            return pa.array(scalars, type=arrow_type)
        except Exception:
            # If conversion fails, return null array
            return pa.array([None] * size, type=pa.null())

    @staticmethod
    def create_table(
        columns: Dict[str, OrsoTypes],
        size: int,
    ) -> pa.Table:
        """Create a PyArrow table with multiple columns."""
        arrays = {
            name: TestDataGenerator.create_arrow_array(type_, size)
            for name, type_ in columns.items()
        }
        return pa.table(arrays)


class FunctionBenchmark:
    """Benchmark function kernels and estimate costs."""

    def __init__(self, sample_sizes: Optional[List[int]] = None):
        """Initialize benchmarker.

        Args:
            sample_sizes: List of sample sizes to test. Defaults to [1000, 10000, 100000, 1000000].
        """
        self.sample_sizes = sample_sizes or [1000, 10000, 100000, 1000000]
        self.catalog = get_catalog()
        self.results: List[CostEstimate] = []

    def benchmark_kernel(
        self,
        function_name: str,
        kernel_callable: Callable,
        kernel_id: str,
        engine: str,
        test_args: Optional[List[Any]] = None,
        runs: int = 5,
    ) -> CostEstimate:
        """Benchmark a single kernel.

        Args:
            function_name: Name of the function
            kernel_callable: The kernel function to benchmark
            kernel_id: ID of the kernel
            engine: Execution engine name
            test_args: List of arguments to pass to the kernel
            runs: Number of times to run each sample size

        Returns:
            CostEstimate with benchmarking results
        """
        if test_args is None:
            test_args = []

        times_by_size = {size: [] for size in self.sample_sizes}

        try:
            for size in self.sample_sizes:
                # Generate test data
                test_data = (
                    [
                        TestDataGenerator.create_arrow_array(OrsoTypes.DOUBLE, size)
                        for _ in test_args
                    ]
                    if test_args
                    else [TestDataGenerator.create_arrow_array(OrsoTypes.DOUBLE, size)]
                )

                # Run benchmark
                for _ in range(runs):
                    start = time.perf_counter_ns()
                    try:
                        kernel_callable(*test_data)
                    except Exception:
                        # Some kernels may not work with all inputs
                        pass
                    elapsed_ns = time.perf_counter_ns() - start
                    times_by_size[size].append(elapsed_ns / 1000)  # Convert to microseconds

            # Calculate cost per million rows based on largest sample
            largest_size = self.sample_sizes[-1]
            if times_by_size[largest_size]:
                avg_us = np.mean(times_by_size[largest_size])
                min_us = np.min(times_by_size[largest_size])
                max_us = np.max(times_by_size[largest_size])

                # Extrapolate to 1 million rows
                cost_us_per_million = (avg_us / largest_size) * 1_000_000

                return CostEstimate(
                    function_name=function_name,
                    kernel_id=kernel_id,
                    engine=engine,
                    cost_us_per_million=cost_us_per_million,
                    sample_size=largest_size,
                    runs=runs,
                    min_us=min_us,
                    max_us=max_us,
                    avg_us=avg_us,
                    success=True,
                )
            else:
                return CostEstimate(
                    function_name=function_name,
                    kernel_id=kernel_id,
                    engine=engine,
                    cost_us_per_million=0.0,
                    sample_size=largest_size,
                    runs=0,
                    min_us=0.0,
                    max_us=0.0,
                    avg_us=0.0,
                    success=False,
                    error_message="No successful runs",
                )

        except Exception as e:
            return CostEstimate(
                function_name=function_name,
                kernel_id=kernel_id,
                engine=engine,
                cost_us_per_million=0.0,
                sample_size=0,
                runs=0,
                min_us=0.0,
                max_us=0.0,
                avg_us=0.0,
                success=False,
                error_message=str(e),
            )

    def benchmark_function(
        self,
        function_name: str,
        runs: int = 5,
    ) -> List[CostEstimate]:
        """Benchmark all kernels of a function.

        Args:
            function_name: Name of the function to benchmark
            runs: Number of benchmark runs per kernel

        Returns:
            List of CostEstimate objects for each kernel
        """
        func_def = self.catalog.get_definition(function_name)
        if not func_def:
            return []

        results = []
        for overload in func_def.overloads:
            kernel = overload.kernel

            # Try to benchmark the kernel
            estimate = self.benchmark_kernel(
                function_name=function_name,
                kernel_callable=kernel.callable_ref,
                kernel_id=kernel.id,
                engine=str(kernel.engine),
                runs=runs,
            )
            results.append(estimate)
            self.results.append(estimate)

        return results

    def benchmark_all(
        self,
        exclude_functions: Optional[List[str]] = None,
        runs: int = 5,
    ) -> List[CostEstimate]:
        """Benchmark all functions in the catalog.

        Args:
            exclude_functions: List of function names to skip
            runs: Number of benchmark runs per kernel

        Returns:
            List of all CostEstimate objects
        """
        exclude_functions = exclude_functions or []

        for func_def in self.catalog.list_functions():
            if func_def.name in exclude_functions:
                continue

            print(f"Benchmarking {func_def.name}...", flush=True)
            self.benchmark_function(func_def.name, runs=runs)

        return self.results

    def to_dict(self) -> Dict[str, Any]:
        """Export results as a dictionary."""
        by_function: Dict[str, List[Dict[str, Any]]] = {}

        for estimate in self.results:
            if estimate.function_name not in by_function:
                by_function[estimate.function_name] = []

            by_function[estimate.function_name].append(
                {
                    "kernel_id": estimate.kernel_id,
                    "engine": estimate.engine,
                    "cost_us_per_million": estimate.cost_us_per_million,
                    "sample_size": estimate.sample_size,
                    "runs": estimate.runs,
                    "min_us": estimate.min_us,
                    "max_us": estimate.max_us,
                    "avg_us": estimate.avg_us,
                    "success": estimate.success,
                    "error": estimate.error_message,
                }
            )

        return {
            "timestamp": time.time(),
            "total_functions": len(by_function),
            "total_kernels": len(self.results),
            "successful": sum(1 for r in self.results if r.success),
            "functions": by_function,
        }

    def export_json(self, path: Path) -> None:
        """Export results to JSON file.

        Args:
            path: Output file path
        """
        data = self.to_dict()
        path.write_text(json.dumps(data, indent=2))
        print(f"Results exported to {path}")

    def print_summary(self) -> None:
        """Print a summary of benchmark results."""
        if not self.results:
            print("No results to summarize")
            return

        successful = [r for r in self.results if r.success]
        failed = [r for r in self.results if not r.success]

        print("\n" + "=" * 80)
        print("COST ESTIMATION SUMMARY")
        print("=" * 80)
        print(f"Total kernels benchmarked: {len(self.results)}")
        print(f"Successful: {len(successful)}")
        print(f"Failed: {len(failed)}")

        if successful:
            print("\n" + "-" * 80)
            print("Estimated Costs (microseconds per million rows):")
            print("-" * 80)
            print(f"{'Function':<20} {'Kernel ID':<20} {'Engine':<10} {'Cost (μs/M)':<15}")
            print("-" * 80)

            for estimate in sorted(successful, key=lambda e: (e.function_name, e.kernel_id)):
                print(
                    f"{estimate.function_name:<20} "
                    f"{estimate.kernel_id:<20} "
                    f"{estimate.engine:<10} "
                    f"{estimate.cost_us_per_million:>14.2f}"
                )

        if failed:
            print("\n" + "-" * 80)
            print("Failed Benchmarks:")
            print("-" * 80)
            for estimate in failed:
                print(f"{estimate.function_name}: {estimate.error_message}")

        print("\n" + "=" * 80)


def main():
    parser = argparse.ArgumentParser(description="Estimate execution costs for function kernels")
    parser.add_argument(
        "--functions",
        help="Comma-separated list of function names to benchmark (default: all)",
        type=str,
    )
    parser.add_argument(
        "--output",
        help="Output JSON file path",
        type=Path,
        default=Path("function_costs.json"),
    )
    parser.add_argument(
        "--runs",
        help="Number of benchmark runs per kernel",
        type=int,
        default=5,
    )
    parser.add_argument(
        "--sample-sizes",
        help="Comma-separated sample sizes to test",
        type=str,
        default="1000,10000,100000,1000000",
    )

    args = parser.parse_args()

    # Parse sample sizes
    sample_sizes = [int(x) for x in args.sample_sizes.split(",")]

    # Create benchmarker
    benchmark = FunctionBenchmark(sample_sizes=sample_sizes)

    # Benchmark specific functions or all
    if args.functions:
        functions = [f.strip() for f in args.functions.split(",")]
        for func_name in functions:
            print(f"Benchmarking {func_name}...", flush=True)
            benchmark.benchmark_function(func_name, runs=args.runs)
    else:
        print("Benchmarking all functions...")
        benchmark.benchmark_all(runs=args.runs, exclude_functions=["EMBED"])

    # Export results
    benchmark.export_json(args.output)
    benchmark.print_summary()


if __name__ == "__main__":
    main()
