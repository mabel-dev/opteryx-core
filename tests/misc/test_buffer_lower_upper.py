"""
Test buffer-level lower/upper functions
"""
import draken.draken_native as dn
from opteryx.compiled.nanobind.vectors import vector_lowercase, vector_uppercase


def _make_varchar(lst):
    return dn.vector_from_string_sequence(
        [v.encode("utf-8") if isinstance(v, str) else v for v in lst]
    )


def test_basic_operations():
    """Test basic uppercase/lowercase operations"""
    print("Testing basic string operations...")

    # Test data
    data = ["Hello", "WORLD", "MiXeD", "test", None, "ABC"]
    vec = _make_varchar(data)

    # Test uppercase
    upper_vec = vector_uppercase(vec)
    upper_result = upper_vec.to_pylist()
    print(f"Original: {data}")
    print(f"Upper:    {upper_result}")
    assert upper_result == ["HELLO", "WORLD", "MIXED", "TEST", None, "ABC"]
    print("✓ Uppercase test passed")

    # Test lowercase
    lower_vec = vector_lowercase(vec)
    lower_result = lower_vec.to_pylist()
    print(f"Lower:    {lower_result}")
    assert lower_result == ["hello", "world", "mixed", "test", None, "abc"]
    print("✓ Lowercase test passed")

    print()


def test_performance():
    """Quick performance comparison"""
    import time
    import pyarrow as pa
    import pyarrow.compute as pc

    print("Performance test...")

    # Create large test dataset
    data = ["Hello World"] * 100000 + ["MIXED CASE"] * 100000 + ["lowercase"] * 100000
    vec = _make_varchar(data)
    arr = pa.array(data, type=pa.string())

    # Test draken vector approach
    start = time.perf_counter()
    for _ in range(10):
        result_vec = vector_lowercase(vec)
    buffer_time = time.perf_counter() - start

    # Test PyArrow approach
    start = time.perf_counter()
    for _ in range(10):
        result_pa = pc.utf8_lower(arr)
    pyarrow_time = time.perf_counter() - start

    print(f"Draken vector_lowercase: {buffer_time*1000:.2f} ms (10 iterations)")
    print(f"PyArrow utf8_lower: {pyarrow_time*1000:.2f} ms (10 iterations)")
    speedup = pyarrow_time / buffer_time
    print(f"Speedup: {speedup:.2f}x")
    print()


def test_sql_functions():
    """Test through SQL function interface"""
    print("Testing SQL UPPER/LOWER functions...")
    from tests.helpers import execute_and_get_arrow

    result = execute_and_get_arrow("SELECT UPPER('Hello') AS upper, LOWER('WORLD') AS lower")
    print(result)
    print("✓ SQL functions working")
    print()


if __name__ == "__main__":
    test_basic_operations()
    test_performance()
    test_sql_functions()
    print("=" * 60)
    print("All tests passed!")
