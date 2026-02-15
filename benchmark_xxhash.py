"""
Benchmark xxhash performance with optimizations.
"""
import time

from opteryx.third_party.cyan4973.xxhash import hash_bytes


def benchmark_hash_function(test_data, iterations=1_000_000):
    """Benchmark hash function performance."""
    start = time.perf_counter()
    for _ in range(iterations):
        for data in test_data:
            hash_bytes(data)
    end = time.perf_counter()
    return end - start

# Test with various string lengths (common in analytics)
test_cases = [
    b'',
    b'a',
    b'ab',
    b'abc',
    b'abcd',
    b'hello',
    b'user_id',
    b'customer_name',
    b'product_category_name',
    b'a' * 17,
    b'a' * 32,
    b'a' * 64,
]

print("xxHash Performance Benchmark")
print("=" * 50)
print(f"Test cases: {len(test_cases)} strings (0-64 bytes)")
print(f"Iterations: 1,000,000")
print()

elapsed = benchmark_hash_function(test_cases)
total_hashes = len(test_cases) * 1_000_000
hashes_per_sec = total_hashes / elapsed

print(f"Time elapsed: {elapsed:.3f} seconds")
print(f"Hashes computed: {total_hashes:,}")
print(f"Performance: {hashes_per_sec:,.0f} hashes/second")
print(f"Average: {(elapsed/total_hashes)*1e9:.1f} nanoseconds per hash")
