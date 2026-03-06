import os
import sys
import pyarrow as pa

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.list_ops import list_anyop_eq
from opteryx.draken.interop.arrow import vector_from_arrow

def _test_eq_comparison(literal, test_value, expected_result, _type=pa.string()):
    array = vector_from_arrow(pa.array([[test_value]], type=pa.list_(_type)))
    result = list_anyop_eq(literal, array).to_pylist()
    assert result == [expected_result], f"Expected {literal} == {test_value} to be {expected_result} got {result[0]}"

def test_eq_strings():
    _test_eq_comparison("a", "a", 1)
    _test_eq_comparison("a", "b", 0)
    _test_eq_comparison("abc", "abc", 1)
    _test_eq_comparison("abc", "ABC", 0)
    _test_eq_comparison("", "", 1)
    _test_eq_comparison(" ", " ", 1)
    _test_eq_comparison("a\0", "a\0", 1)
    _test_eq_comparison("a\0", "a", 0)

def test_eq_bytes():
    _test_eq_comparison(b"a", b"a", 1, pa.binary())
    _test_eq_comparison(b"a", b"b", 0, pa.binary())
    _test_eq_comparison(b"abc", b"abc", 1, pa.binary())
    _test_eq_comparison(b"abc", b"ABC", 0, pa.binary())
    _test_eq_comparison(b"", b"", 1, pa.binary())
    _test_eq_comparison(b" ", b" ", 1, pa.binary())
    _test_eq_comparison(b"a\0", b"a\0", 1, pa.binary())
    _test_eq_comparison(b"a\0", b"a", 0, pa.binary())

def test_eq_integers():
    _test_eq_comparison(1, 1, 1, pa.int64())
    _test_eq_comparison(1, 0, 0, pa.int64())
    _test_eq_comparison(-1, -1, 1, pa.int64())
    _test_eq_comparison(0, 0, 1, pa.int64())
    _test_eq_comparison(123456789, 123456789, 1, pa.int64())

def test_eq_floats():
    _test_eq_comparison(1.0, 1.0, 1, pa.float64())
    _test_eq_comparison(1.0, 1.0000001, 0, pa.float64())
    _test_eq_comparison(float("inf"), float("inf"), 1, pa.float64())
    _test_eq_comparison(float("-inf"), float("-inf"), 1, pa.float64())
#    _test_eq_comparison(float("nan"), float("nan"), 0, pa.float64())  # NaN != NaN

def test_eq_booleans():
    _test_eq_comparison(True, True, 1, pa.bool_())
    _test_eq_comparison(False, False, 1, pa.bool_())
    _test_eq_comparison(True, False, 0, pa.bool_())
    _test_eq_comparison(False, True, 0, pa.bool_())

def test_eq_list_comparison():
    array = vector_from_arrow(pa.array([["a", "b", "c"], ["x", "y", "z"], []], type=pa.list_(pa.string())))
    result = list_anyop_eq("b", array).to_pylist()
    assert result == [1, 0, 0], f"Expected [1, 0, 0], got {result}"

def test_eq_nulls():
    array = vector_from_arrow(pa.array([[None, "a"], [None], []], type=pa.list_(pa.string())))
    result = list_anyop_eq("a", array).to_pylist()
    assert result == [1, 0, 0], f"Expected [1, 0, 0], got {result}"

def test_eq_float_precision():
    _test_eq_comparison(1.0000000000000001, 1.0, 1, pa.float64())  # within double precision
    _test_eq_comparison(1.0000000000001, 1.0, 0, pa.float64())     # outside of equality tolerance

def test_eq_string_unicode_and_bytes():
    _test_eq_comparison("café", "café", 1, pa.string())
    _test_eq_comparison("café", "café", 0, pa.string())  # visually same, different Unicode normalization
    _test_eq_comparison("💡", "💡", 1, pa.string())
    _test_eq_comparison("💡", "💡 ", 0, pa.string())

def test_eq_lists_with_nulls_and_values():
    array = vector_from_arrow(pa.array([[None, "a", "b"], [None], ["a"]], type=pa.list_(pa.string())))
    result = list_anyop_eq("a", array).to_pylist()
    assert result == [1, 0, 1], f"Expected [1, 0, 1], got {result}"

def test_eq_large_numbers():
    _test_eq_comparison(2**60, 2**60, 1, pa.int64())
    _test_eq_comparison(2**60, 2**60 - 1, 0, pa.int64())

def test_eq_mixed_case_strings():
    _test_eq_comparison("Test", "test", 0)
    _test_eq_comparison("TEST", "TEST", 1)
    _test_eq_comparison("Test", "Test", 1)

def test_eq_edge_strings():
    _test_eq_comparison("\0", "\0", 1)
    _test_eq_comparison("\n", "\n", 1)
    _test_eq_comparison("\t", "\t", 1)
    _test_eq_comparison("a\tb", "a\tb", 1)
    _test_eq_comparison("a\tb", "a b", 0)

def test_eq_control_characters():
    _test_eq_comparison("\x00", "\x00", 1)  # null byte
    _test_eq_comparison("\x00", "\x01", 0)  # different control chars
    _test_eq_comparison("a\x00b", "a\x00b", 1)  # null in middle
    _test_eq_comparison("a\x00b", "ab", 0)  # with vs without null
    _test_eq_comparison("\r\n", "\r\n", 1)  # CRLF
    _test_eq_comparison("\r\n", "\n", 0)  # CRLF vs LF

def test_eq_empty_and_whitespace():
    _test_eq_comparison("", "", 1)
    _test_eq_comparison(" ", "", 0)
    _test_eq_comparison("  ", " ", 0)
    _test_eq_comparison("\t", " ", 0)
    _test_eq_comparison("\n", "", 0)

def test_eq_numeric_edge_cases():
    _test_eq_comparison(0, -0, 1, pa.int64())  # zero equality
    _test_eq_comparison(2**63 - 1, 2**63 - 1, 1, pa.int64())  # max int64
    _test_eq_comparison(-(2**63), -(2**63), 1, pa.int64())  # min int64
    _test_eq_comparison(1, 2, 0, pa.int64())

def test_eq_float_edge_cases():
    _test_eq_comparison(0.0, 0.0, 1, pa.float64())
    _test_eq_comparison(-0.0, 0.0, 1, pa.float64())  # -0.0 == 0.0 per IEEE 754
    _test_eq_comparison(1e-10, 1e-10, 1, pa.float64())  # very small
    _test_eq_comparison(1e10, 1e10, 1, pa.float64())  # very large
    _test_eq_comparison(1.23456789, 1.23456789, 1, pa.float64())  # precision

def test_eq_special_unicode():
    _test_eq_comparison("👨‍👩‍👧‍👦", "👨‍👩‍👧‍👦", 1)  # family emoji
    _test_eq_comparison("🇺🇸", "🇺🇸", 1)  # flag emoji
    _test_eq_comparison("🇺🇸", "🇬🇧", 0)  # different flags
    _test_eq_comparison("Ω", "Ω", 1)  # Greek letter
    _test_eq_comparison("🔥", "💧", 0)  # different emojis

def test_eq_binary_edge_cases():
    _test_eq_comparison(b"\x00", b"\x00", 1, pa.binary())  # null byte
    _test_eq_comparison(b"\x00\x01\x02", b"\x00\x01\x02", 1, pa.binary())  # binary sequence
    _test_eq_comparison(b"\xff" * 10, b"\xff" * 10, 1, pa.binary())  # repeated bytes
    _test_eq_comparison(b"", b"", 1, pa.binary())  # empty binary
    _test_eq_comparison(b"test", b"test ", 0, pa.binary())  # different lengths

def test_eq_list_empty_and_nulls():
    # Empty list
    array = vector_from_arrow(pa.array([[]], type=pa.list_(pa.string())))
    result = list_anyop_eq("a", array).to_pylist()
    assert result == [0], f"Expected [0] for empty list, got {result}"
    
    # List with only nulls
    array = vector_from_arrow(pa.array([[None, None]], type=pa.list_(pa.string())))
    result = list_anyop_eq("a", array).to_pylist()
    assert result == [0], f"Expected [0] for null-only list, got {result}"
    
    # Multiple rows with various null patterns
    array = vector_from_arrow(pa.array([[None, "a"], ["a"], [None], []], type=pa.list_(pa.string())))
    result = list_anyop_eq("a", array).to_pylist()
    assert result == [1, 1, 0, 0], f"Expected [1, 1, 0, 0], got {result}"

def test_eq_list_duplicates():
    # List with duplicates
    array = vector_from_arrow(pa.array([["a", "a", "a"]], type=pa.list_(pa.string())))
    result = list_anyop_eq("a", array).to_pylist()
    assert result == [1], f"Expected [1], got {result}"
    
    # Mixed duplicates
    array = vector_from_arrow(pa.array([["a", "b", "a", "b"]], type=pa.list_(pa.string())))
    result = list_anyop_eq("a", array).to_pylist()
    assert result == [1], f"Expected [1], got {result}"

def test_eq_list_binary():
    # Binary list comparisons
    array = vector_from_arrow(pa.array([[b"test", b"data"]], type=pa.list_(pa.binary())))
    result = list_anyop_eq(b"test", array).to_pylist()
    assert result == [1], f"Expected [1], got {result}"
    
    array = vector_from_arrow(pa.array([[b"test", b"data"]], type=pa.list_(pa.binary())))
    result = list_anyop_eq(b"other", array).to_pylist()
    assert result == [0], f"Expected [0], got {result}"

def test_eq_list_integers():
    # Integer list comparisons
    array = vector_from_arrow(pa.array([[1, 2, 3]], type=pa.list_(pa.int64())))
    result = list_anyop_eq(2, array).to_pylist()
    assert result == [1], f"Expected [1], got {result}"
    
    array = vector_from_arrow(pa.array([[1, 2, 3]], type=pa.list_(pa.int64())))
    result = list_anyop_eq(5, array).to_pylist()
    assert result == [0], f"Expected [0], got {result}"
    
    # Negative numbers
    array = vector_from_arrow(pa.array([[-1, -2, -3]], type=pa.list_(pa.int64())))
    result = list_anyop_eq(-2, array).to_pylist()
    assert result == [1], f"Expected [1], got {result}"

def test_eq_list_floats():
    # Float list comparisons
    array = vector_from_arrow(pa.array([[1.1, 2.2, 3.3]], type=pa.list_(pa.float64())))
    result = list_anyop_eq(2.2, array).to_pylist()
    assert result == [1], f"Expected [1], got {result}"
    
    array = vector_from_arrow(pa.array([[1.1, 2.2, 3.3]], type=pa.list_(pa.float64())))
    result = list_anyop_eq(4.4, array).to_pylist()
    assert result == [0], f"Expected [0], got {result}"

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests
    run_tests()
