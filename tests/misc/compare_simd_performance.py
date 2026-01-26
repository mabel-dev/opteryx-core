"""
Direct performance comparison for SIMD optimizations.
Measures throughput for case-insensitive string comparison.
"""
import time
import pyarrow as pa
from opteryx.draken.vectors.string_vector import StringVector


def measure_throughput(vec, search_value, iterations=100):
    """Measure throughput in million rows per second."""
    # Warmup
    for _ in range(5):
        _ = vec.equals_ascii_case_insensitive(search_value)
    
    # Measure
    start = time.perf_counter()
    for _ in range(iterations):
        _ = vec.equals_ascii_case_insensitive(search_value)
    elapsed = time.perf_counter() - start
    
    total_rows = len(vec) * iterations
    throughput = total_rows / elapsed / 1_000_000
    return throughput


def main():
    print("=" * 80)
    print("SIMD String Comparison Performance Test")
    print("=" * 80)
    print()
    
    # Test with different string lengths to showcase SIMD benefits
    test_configs = [
        ("4-byte strings", 1_000_000, ["Test", "DATA", "item", "CODE"], "test"),
        ("16-byte strings (SIMD threshold)", 1_000_000, ["TestDataString16", "ANOTHER_STRING16"], "testdatastring16"),
        ("32-byte strings", 500_000, ["A" * 32, "B" * 32, "C" * 32], "a" * 32),
        ("64-byte strings", 250_000, ["X" * 64, "Y" * 64, "Z" * 64], "x" * 64),
        ("128-byte strings", 100_000, ["Long" * 32, "Data" * 32], "long" * 32),
    ]
    
    print(f"{'Test Case':<40} {'Rows':<12} {'Throughput (M rows/s)':<25}")
    print("-" * 80)
    
    for name, base_rows, patterns, search in test_configs:
        # Create test data by repeating patterns
        repeats = base_rows // len(patterns)
        data = patterns * repeats
        
        # Create StringVector
        arr = pa.array(data, type=pa.string())
        vec = StringVector.from_arrow(arr)
        
        # Measure
        throughput = measure_throughput(vec, search.encode('utf-8'), iterations=50)
        
        print(f"{name:<40} {len(vec):>10,}    {throughput:>8.2f}")
    
    print()
    print("=" * 80)
    print("Test complete!")
    print()
    print("Notes:")
    print("- Strings >= 16 bytes use SIMD path (NEON on ARM, AVX2 on x86)")
    print("- Optimizations applied:")
    print("  1. Single comparison range check: (c-'A')<=25 instead of dual comparison")
    print("  2. Draken iterator with nogil for better performance")


if __name__ == "__main__":
    main()
