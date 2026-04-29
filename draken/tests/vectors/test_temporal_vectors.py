"""Tests for temporal vector types (date32, timestamp, time) in Draken.

This module tests the temporal vector implementations including:
- Date32Vector for date values
- TimestampVector for timestamp values  
- TimeVector for time values (both time32 and time64)
- ArrayVector for list/array types

The tests validate:
- Zero-copy interoperability with Apache Arrow
- Round-trip conversion (Arrow -> Draken -> Arrow)
- Null handling for temporal types
- Basic operations and comparisons
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pytest
import pyarrow as pa
import pyarrow.compute as pc

from draken import Vector
from draken.vectors.date32_vector import Date32Vector
from draken.vectors.float64_vector import Float64Vector
from draken.vectors.int64_vector import Int64Vector
from draken.vectors.integer_vector import IntegerVector
from draken.vectors.time_vector import TimeVector
from draken.vectors.timestamp_vector import TimestampVector


def test_date32_vector():
    """Test Date32Vector with Arrow date32 arrays."""
    # Create date32 array (days since epoch)
    arrow_array = pa.array([18000, 18500, None, 19000], type=pa.date32())
    
    # Wrap in Draken vector
    vec = Vector.from_arrow(arrow_array)
    
    # Check type and length
    assert vec.length == 4
    assert vec.null_count == 1
    
    # Round trip
    roundtrip = vec.to_arrow()
    assert roundtrip.equals(arrow_array)
    
    # Check values
    pylist = vec.to_pylist()
    assert pylist == [18000, 18500, None, 19000]


def test_timestamp_vector():
    """Test TimestampVector with Arrow timestamp arrays."""
    # Create timestamp array (microseconds since epoch)
    arrow_array = pa.array([1000000, 2000000, None, 3000000], type=pa.timestamp('us'))
    
    # Wrap in Draken vector
    vec = Vector.from_arrow(arrow_array)
    
    # Check type and length
    assert vec.length == 4
    assert vec.null_count == 1
    
    # Round trip
    roundtrip = vec.to_arrow()
    assert roundtrip.equals(arrow_array)
    
    # Check values
    pylist = vec.to_pylist()
    assert pylist == [1000000, 2000000, None, 3000000]


def test_time32_vector():
    """Test TimeVector with Arrow time32 arrays."""
    # Create time32 array (seconds since midnight)
    arrow_array = pa.array([3600, 7200, None, 10800], type=pa.time32('s'))
    
    # Wrap in Draken vector
    vec = Vector.from_arrow(arrow_array)
    
    # Check type and length
    assert vec.length == 4
    assert vec.null_count == 1
    
    # Round trip
    roundtrip = vec.to_arrow()
    assert roundtrip.equals(arrow_array)
    
    # Check values
    pylist = vec.to_pylist()
    assert pylist == [3600, 7200, None, 10800]


def test_time64_vector():
    """Test TimeVector with Arrow time64 arrays."""
    # Create time64 array (microseconds since midnight)
    arrow_array = pa.array([3600000000, 7200000000, None, 10800000000], type=pa.time64('us'))
    
    # Wrap in Draken vector
    vec = Vector.from_arrow(arrow_array)
    
    # Check type and length
    assert vec.length == 4
    assert vec.null_count == 1
    
    # Round trip
    roundtrip = vec.to_arrow()
    assert roundtrip.equals(arrow_array)
    
    # Check values
    pylist = vec.to_pylist()
    assert pylist == [3600000000, 7200000000, None, 10800000000]


def test_array_vector():
    """Test ArrayVector with Arrow list arrays."""
    # Create list array
    arrow_array = pa.array([[1, 2, 3], [4, 5], None, [6, 7, 8, 9]], type=pa.list_(pa.int64()))
    
    # Wrap in Draken vector
    vec = Vector.from_arrow(arrow_array)
    
    # Check type and length
    assert vec.length == 4
    assert vec.null_count == 1
    
    # Round trip
    roundtrip = vec.to_arrow()
    assert roundtrip.equals(arrow_array)
    
    # Check values
    pylist = vec.to_pylist()
    assert pylist == [[1, 2, 3], [4, 5], None, [6, 7, 8, 9]]


def test_date32_comparisons():
    """Test comparison operations on Date32Vector."""
    arrow_array = pa.array([10, 20, 30, 40], type=pa.date32())
    vec = Vector.from_arrow(arrow_array)
    
    # Test comparison operations
    result = vec.equals(20)
    assert list(result) == [False, True, False, False]
    
    result = vec.greater_than(25)
    assert list(result) == [False, False, True, True]
    
    result = vec.less_than(25)
    assert list(result) == [True, True, False, False]


def test_timestamp_comparisons():
    """Test comparison operations on TimestampVector."""
    arrow_array = pa.array([1000, 2000, 3000, 4000], type=pa.timestamp('us'))
    vec = Vector.from_arrow(arrow_array)
    
    # Test comparison operations
    result = vec.equals(2000)
    assert list(result) == [False, True, False, False]
    
    result = vec.greater_than_or_equals(3000)
    assert list(result) == [False, False, True, True]
    
    result = vec.less_than_or_equals(3000)
    assert list(result) == [True, True, True, False]


def test_date32_min_max():
    """Test min/max operations on Date32Vector."""
    arrow_array = pa.array([50, 10, 30, 20], type=pa.date32())
    vec = Vector.from_arrow(arrow_array)
    
    assert vec.min() == 10
    assert vec.max() == 50


def test_timestamp_min_max():
    """Test min/max operations on TimestampVector."""
    arrow_array = pa.array([5000, 1000, 3000, 2000], type=pa.timestamp('us'))
    vec = Vector.from_arrow(arrow_array)
    
    assert vec.min() == 1000
    assert vec.max() == 5000


def test_temporal_null_handling():
    """Test null handling in temporal vectors."""
    # Date32 with nulls
    date_array = pa.array([10, None, 30, None], type=pa.date32())
    date_vec = Vector.from_arrow(date_array)
    
    is_null = date_vec.is_null()
    assert list(is_null) == [False, True, False, True]
    assert date_vec.null_count == 2
    
    # Timestamp with nulls
    ts_array = pa.array([None, 2000, None, 4000], type=pa.timestamp('us'))
    ts_vec = Vector.from_arrow(ts_array)
    
    is_null = ts_vec.is_null()
    assert list(is_null) == [True, False, True, False]
    assert ts_vec.null_count == 2


def test_array_nested_types():
    """Test ArrayVector with different nested types."""
    # List of strings
    string_list = pa.array([['a', 'b'], ['c'], None, ['d', 'e', 'f']], type=pa.list_(pa.string()))
    vec = Vector.from_arrow(string_list)
    assert vec.to_pylist() == [[b'a', b'b'], [b'c'], None, [b'd', b'e', b'f']]
    
    # List of floats
    float_list = pa.array([[1.1, 2.2], [3.3], None, [4.4]], type=pa.list_(pa.float64()))
    vec = Vector.from_arrow(float_list)
    assert vec.to_pylist() == [[1.1, 2.2], [3.3], None, [4.4]]


def test_typed_temporal_constructors_decode_dictionary_input():
    date_arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, None], type=pa.int8()),
        pa.array([18000, 18500], type=pa.date32()),
    )
    ts_arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, None], type=pa.int8()),
        pa.array([1000000, 2000000], type=pa.timestamp('us')),
    )
    time_arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, None], type=pa.int8()),
        pa.array([3600, 7200], type=pa.time32('s')),
    )

    date_vec = Date32Vector.from_arrow(date_arr)
    ts_vec = TimestampVector.from_arrow(ts_arr)
    time_vec = TimeVector.from_arrow(time_arr)

    assert date_vec.__class__.__name__ == "Date32Vector"
    assert ts_vec.__class__.__name__ == "TimestampVector"
    assert time_vec.__class__.__name__ == "TimeVector"
    assert date_vec.to_pylist() == [18000, 18500, 18000, None]
    assert ts_vec.to_pylist() == [1000000, 2000000, 1000000, None]
    assert time_vec.to_pylist() == [3600, 7200, 3600, None]


def test_typed_numeric_constructors_use_from_dict_for_dictionary_input():
    int64_arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, None], type=pa.int8()),
        pa.array([10, 20], type=pa.int64()),
    )
    int32_arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, None], type=pa.int8()),
        pa.array([1, 2], type=pa.int32()),
    )
    float64_arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, None], type=pa.int8()),
        pa.array([1.5, 2.5], type=pa.float64()),
    )

    with pytest.raises(TypeError, match="from_dict"):
        Int64Vector.from_arrow(int64_arr)
    with pytest.raises(TypeError, match="from_dict"):
        IntegerVector.from_arrow(int32_arr)
    with pytest.raises(TypeError, match="from_dict"):
        Float64Vector.from_arrow(float64_arr)

    int64_vec = Int64Vector.from_dict([0, 1, 0, 0], [10, 20], [True, True, True, False])
    int32_vec = IntegerVector.from_dict([0, 1, 0, 0], [1, 2], [True, True, True, False])
    float64_vec = Float64Vector.from_dict([0, 1, 0, 0], [1.5, 2.5], [True, True, True, False])

    assert int64_vec.__class__.__name__ == "Int64Vector"
    assert int32_vec.__class__.__name__ == "IntegerVector"
    assert float64_vec.__class__.__name__ == "Float64Vector"
    assert int64_vec.to_pylist() == [10, 20, 10, None]
    assert int32_vec.to_pylist() == [1, 2, 1, None]
    assert float64_vec.to_pylist() == [1.5, 2.5, 1.5, None]


def test_typed_temporal_constructors_use_from_dict_for_dictionary_input():
    date_arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, None], type=pa.int8()),
        pa.array([18000, 18500], type=pa.date32()),
    )
    timestamp_arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, None], type=pa.int8()),
        pa.array([1_000_000, 2_000_000], type=pa.timestamp("us")),
    )
    time_arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, None], type=pa.int8()),
        pa.array([3600, 7200], type=pa.time32("s")),
    )

    with pytest.raises(TypeError, match="from_dict"):
        Date32Vector.from_arrow(date_arr)
    with pytest.raises(TypeError, match="from_dict"):
        TimestampVector.from_arrow(timestamp_arr)
    with pytest.raises(TypeError, match="from_dict"):
        TimeVector.from_arrow(time_arr)

    date_vec = Date32Vector.from_dict([0, 1, 0, 0], [18000, 18500], [True, True, True, False])
    timestamp_vec = TimestampVector.from_dict(
        [0, 1, 0, 0],
        [1_000_000, 2_000_000],
        [True, True, True, False],
        "us",
    )
    time_vec = TimeVector.from_dict([0, 1, 0, 0], [3600, 7200], [True, True, True, False])

    assert date_vec.to_pylist() == [18000, 18500, 18000, None]
    assert timestamp_vec.to_pylist() == [1000000, 2000000, 1000000, None]
    assert time_vec.to_pylist() == [3600, 7200, 3600, None]


if __name__ == "__main__":
    import time
    
    start_suite = time.monotonic_ns()
    
    tests = [
        ("Date32Vector basic", test_date32_vector),
        ("TimestampVector basic", test_timestamp_vector),
        ("Time32Vector basic", test_time32_vector),
        ("Time64Vector basic", test_time64_vector),
        ("ArrayVector basic", test_array_vector),
        ("Date32 comparisons", test_date32_comparisons),
        ("Timestamp comparisons", test_timestamp_comparisons),
        ("Date32 min/max", test_date32_min_max),
        ("Timestamp min/max", test_timestamp_min_max),
        ("Temporal null handling", test_temporal_null_handling),
        ("Array nested types", test_array_nested_types),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        print(f"\033[38;2;139;233;253m==>\033[0m {test_name}...", end=" ")
        start = time.monotonic_ns()
        try:
            test_func()
            duration = (time.monotonic_ns() - start) / 1e9
            print(f"✅ \033[0;32mpassed\033[0m ({duration:.4f}s)")
            passed += 1
        except Exception as e:
            duration = (time.monotonic_ns() - start) / 1e9
            print(f"❌ \033[0;31mfailed\033[0m ({duration:.4f}s)")
            print(f"  Error: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed) if (passed + failed) > 0 else 0}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
    
    sys.exit(0 if failed == 0 else 1)
