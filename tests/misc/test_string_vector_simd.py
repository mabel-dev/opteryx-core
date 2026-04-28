"""
Performance benchmark for SIMD-accelerated string_vector operations.

Tests the equals_ascii_case_insensitive method with various string lengths
to measure the impact of SIMD optimization.
"""
import time
import pyarrow as pa
from draken.vectors.string_vector import StringVector


def benchmark_case_insensitive_equals():
    """Benchmark equals_ascii_case_insensitive with various string lengths."""
    
    # Test configurations
    test_cases = [
        ("Short (4 bytes)", ["Test", "DATA", "item", "CODE"] * 25000, "test"),
        ("Medium (16 bytes)", ["TestDataString16", "ANOTHERDATAITEM16", "lowercase_test16"] * 33333, "testdatastring16"),
        ("Long (64 bytes)", ["A" * 64, "B" * 64, "TestDataStringWithManyCharactersForBenchmarkingPurpose64!"] * 10000, "a" * 64),
        ("Very Long (256 bytes)", ["X" * 256, "Y" * 256, "TestData" * 32] * 4000, "x" * 256),
    ]
    
    results = []
    
    for name, data, search_value in test_cases:
        # Create StringVector
        arr = pa.array(data, type=pa.string())
        vec = StringVector.from_arrow(arr)
        
        # Warmup
        for _ in range(3):
            _ = vec.equals_ascii_case_insensitive(search_value.encode('utf-8'))
        
        # Benchmark
        iterations = 50
        start = time.perf_counter()
        for _ in range(iterations):
            result = vec.equals_ascii_case_insensitive(search_value.encode('utf-8'))
        elapsed = time.perf_counter() - start
        
        # Calculate metrics
        total_rows = len(data)
        avg_time_ms = (elapsed / iterations) * 1000
        throughput_mrows_sec = (total_rows * iterations / elapsed) / 1_000_000
        
        results.append({
            'test': name,
            'rows': total_rows,
            'string_len': len(search_value),
            'avg_time_ms': avg_time_ms,
            'throughput_mrows_sec': throughput_mrows_sec,
            'matches': sum(result)
        })
        
        print(f"{name:20} | {total_rows:8,} rows | {len(search_value):3} bytes | "
              f"{avg_time_ms:7.2f} ms | {throughput_mrows_sec:6.2f} M rows/sec | "
              f"{sum(result):6} matches")
    
    return results


def test_correctness():
    """Verify correctness of case-insensitive comparison."""
    data = [
        "Test", "TEST", "test", "TeSt",  # Should match "test"
        "Different", "other", "NotMatch",  # Should not match
        None,  # Null handling
    ]
    
    arr = pa.array(data, type=pa.string())
    vec = StringVector.from_arrow(arr)
    
    result = vec.equals_ascii_case_insensitive(b"test")
    
    expected = [1, 1, 1, 1, 0, 0, 0, 0]  # First 4 match, others don't
    assert list(result) == expected, f"Expected {expected}, got {list(result)}"
    print("✓ Correctness test passed")


def test_edge_cases():
    """Test edge cases for SIMD path."""
    test_cases = [
        # Test exact threshold boundary (8 bytes)
        (["12345678", "ABCDEFGH", "abcdefgh"], b"12345678", [1, 0, 0]),
        (["1234567", "ABCDEFG"], b"1234567", [1, 0]),  # Just below threshold
        
        # Empty and single character
        (["", "a", "A"], b"", [1, 0, 0]),
        (["x", "X", "y"], b"x", [1, 1, 0]),
        
        # All uppercase vs all lowercase
        (["UPPERCASE", "lowercase", "MiXeD"], b"uppercase", [1, 0, 0]),  # Only "UPPERCASE" matches
        
        # Non-ASCII should work (no modification)
        (["café", "CAFÉ"], b"caf\xc3\xa9", [1, 0]),  # UTF-8 é doesn't convert
    ]
    
    for data, search, expected in test_cases:
        arr = pa.array(data, type=pa.string())
        vec = StringVector.from_arrow(arr)
        result = list(vec.equals_ascii_case_insensitive(search))
        assert result == expected, f"Data: {data}, Search: {search}, Expected {expected}, got {result}"
    
    print("✓ Edge cases test passed")


if __name__ == "__main__":
    print("=" * 100)
    print("SIMD String Vector Performance Benchmark")
    print("=" * 100)
    
    print("\n1. Correctness Tests")
    print("-" * 100)
    test_correctness()
    test_edge_cases()
    
    print("\n2. Performance Benchmark")
    print("-" * 100)
    benchmark_case_insensitive_equals()
    
    print("\n" + "=" * 100)
    print("Benchmark complete!")
