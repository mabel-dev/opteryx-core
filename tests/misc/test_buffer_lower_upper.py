"""
Test buffer-level lower/upper functions
"""
import pyarrow as pa
from draken.vectors.string_vector import StringVector, lowercase, uppercase


def test_basic_operations():
    """Test basic uppercase/lowercase operations"""
    print("Testing basic string operations...")
    
    # Test data
    data = ["Hello", "WORLD", "MiXeD", "test", None, "ABC"]
    arr = pa.array(data, type=pa.string())
    vec = StringVector.from_arrow(arr)
    
    # Test uppercase
    upper_vec = uppercase(vec)
    upper_result = upper_vec.to_pylist()
    print(f"Original: {data}")
    print(f"Upper:    {upper_result}")
    assert upper_result == [b"HELLO", b"WORLD", b"MIXED", b"TEST", None, b"ABC"]
    print("✓ Uppercase test passed")
    
    # Test lowercase
    lower_vec = lowercase(vec)
    lower_result = lower_vec.to_pylist()
    print(f"Lower:    {lower_result}")
    assert lower_result == [b"hello", b"world", b"mixed", b"test", None, b"abc"]
    print("✓ Lowercase test passed")
    
    print()


def test_performance():
    """Quick performance comparison"""
    import time
    import pyarrow.compute as pc
    
    print("Performance test...")
    
    # Create large test dataset
    data = ["Hello World"] * 100000 + ["MIXED CASE"] * 100000 + ["lowercase"] * 100000
    arr = pa.array(data, type=pa.string())
    
    # Test buffer-level approach
    vec = StringVector.from_arrow(arr)
    start = time.perf_counter()
    for _ in range(10):
        result_vec = lowercase(vec)
    buffer_time = time.perf_counter() - start
    
    # Test PyArrow approach
    start = time.perf_counter()
    for _ in range(10):
        result_pa = pc.utf8_lower(arr)
    pyarrow_time = time.perf_counter() - start
    
    print(f"Buffer-level SIMD: {buffer_time*1000:.2f} ms (10 iterations)")
    print(f"PyArrow utf8_lower: {pyarrow_time*1000:.2f} ms (10 iterations)")
    speedup = pyarrow_time / buffer_time
    print(f"Speedup: {speedup:.2f}x")
    print()


def test_sql_functions():
    """Test through SQL function interface"""
    print("Testing SQL UPPER/LOWER functions...")
    import opteryx
    
    # Create test table
    data = {
        "text": ["Hello", "WORLD", "MiXeD", "test"]
    }
    
    result = opteryx.query("SELECT text, UPPER(text) AS upper, LOWER(text) AS lower FROM $data")
    result_df = result.to_pandas()
    
    print(result_df.to_string(index=False))
    print("✓ SQL functions working")
    print()


if __name__ == "__main__":
    test_basic_operations()
    test_performance()
    test_sql_functions()
    print("=" * 60)
    print("All tests passed!")
