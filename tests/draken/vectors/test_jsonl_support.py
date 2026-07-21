"""Tests for JSONL data type support in Draken.

This module validates that Draken can handle all common data types
found in JSONL (JSON Lines) files, including:
- Integers and floats (JSON numbers, with widening int -> float)
- Booleans
- Strings
- Arrays (uniform-scalar elements, materialized as DRAKEN_ARRAY)
- Nullable columns, at both the row level and (for arrays) the element level

These tests exercise the real read path — rugo.jsonl.read_jsonl reading actual
JSONL bytes — rather than constructing Vectors/Morsels directly: Vector and Morsel
have no from_arrow/direct-construction entry point (this file previously assumed
one that never existed in this codebase), and rugo's JSONL reader is the real,
supported producer of Draken vectors from JSONL data anyway.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import json

import pytest

from draken.draken_native import DrakenType
from rugo import jsonl


def _read_column(rows, column: str):
    """Write `rows` as JSONL bytes, read them back, and return the named Vector."""
    buf = ("\n".join(json.dumps(r) for r in rows) + "\n").encode("utf-8")
    with jsonl.read_jsonl(buf) as reader:
        for morsel in reader:
            return morsel.column(column.encode("utf-8"))
    raise AssertionError("no rows survived the JSONL read")


def _read_morsel(rows):
    buf = ("\n".join(json.dumps(r) for r in rows) + "\n").encode("utf-8")
    with jsonl.read_jsonl(buf) as reader:
        for morsel in reader:
            return morsel
    raise AssertionError("no rows survived the JSONL read")


def _null_count(vec) -> int:
    return sum(1 for i in range(vec.length) if vec.is_null_at(i))


class TestJSONLBasicTypes:
    """Test support for basic JSONL data types."""

    def test_integer_support(self):
        """Test that JSON integers are read as INT64, with nulls preserved."""
        rows = [{"v": 1}, {"v": -2}, {"v": None}, {"v": 2147483647}, {"v": -9223372036854775808}]
        vec = _read_column(rows, "v")

        assert vec.type == DrakenType.INT64
        assert vec.length == len(rows)
        assert _null_count(vec) == 1
        assert vec.to_pylist() == [1, -2, None, 2147483647, -9223372036854775808]

    def test_float_support(self):
        """Test that JSON floats are read as FLOAT64, with nulls preserved."""
        rows = [{"v": 1.5}, {"v": -2.5}, {"v": None}, {"v": 0.0}, {"v": 1.5e10}]
        vec = _read_column(rows, "v")

        assert vec.type == DrakenType.FLOAT64
        assert vec.length == len(rows)
        assert _null_count(vec) == 1
        assert vec.to_pylist() == [1.5, -2.5, None, 0.0, 1.5e10]

    def test_boolean_support(self):
        """Test that JSON booleans are read as BOOL, with nulls preserved."""
        rows = [{"v": True}, {"v": False}, {"v": None}, {"v": True}, {"v": False}, {"v": None}]
        vec = _read_column(rows, "v")

        assert vec.type == DrakenType.BOOL
        assert vec.length == 6
        assert _null_count(vec) == 2
        assert vec.to_pylist() == [True, False, None, True, False, None]

    def test_string_support(self):
        """Test that JSON strings are read as VARCHAR, with nulls preserved."""
        rows = [{"v": "hello"}, {"v": "world"}, {"v": None}, {"v": ""}, {"v": "foo bar"}]
        vec = _read_column(rows, "v")

        assert vec.type == DrakenType.VARCHAR
        assert vec.length == 5
        assert _null_count(vec) == 1
        assert vec.to_pylist() == ["hello", "world", None, "", "foo bar"]

    def test_array_support(self):
        """Test that uniform-scalar JSON arrays materialize as DRAKEN_ARRAY."""
        test_cases = [
            ([{"v": [1, 2, 3]}, {"v": None}, {"v": [4]}, {"v": []}, {"v": [5, 6]}],
             [[1, 2, 3], None, [4], [], [5, 6]]),
            ([{"v": ["a", "b"]}, {"v": None}, {"v": ["c"]}],
             [["a", "b"], None, ["c"]]),
            ([{"v": [1.5, 2.5]}, {"v": None}, {"v": []}],
             [[1.5, 2.5], None, []]),
        ]

        for rows, expected in test_cases:
            vec = _read_column(rows, "v")
            assert vec.type == DrakenType.ARRAY
            assert vec.length == len(rows)
            assert _null_count(vec) > 0  # every case has one absent/null row
            assert vec.to_pylist() == expected


class TestJSONLNullableSupport:
    """Test that all types properly handle nullable columns."""

    def test_all_nulls(self):
        """Test a column whose value is JSON null on every row."""
        rows = [{"v": None}, {"v": None}, {"v": None}]
        vec = _read_column(rows, "v")
        assert _null_count(vec) == vec.length == len(rows)

    def test_no_nulls(self):
        """Test columns with no null values, across every supported type."""
        test_cases = [
            [{"v": 1}, {"v": 2}, {"v": 3}],
            [{"v": 1.5}, {"v": 2.5}],
            [{"v": True}, {"v": False}],
            [{"v": "a"}, {"v": "b"}],
            [{"v": [1, 2]}, {"v": [3]}],
        ]

        for rows in test_cases:
            vec = _read_column(rows, "v")
            assert _null_count(vec) == 0


class TestJSONLMorselIntegration:
    """Test that Morsels can handle JSONL-like data."""

    def test_create_morsel_from_jsonl_data(self):
        """Test creating a Morsel by reading JSONL rows."""
        rows = [
            {"id": 1, "name": "Alice", "score": 95.5, "active": True, "tags": ["a", "b"]},
            {"id": 2, "name": "Bob", "score": 87.2, "active": False, "tags": ["c"]},
            {"id": None, "name": None, "score": None, "active": None, "tags": None},
        ]
        morsel = _read_morsel(rows)

        assert morsel.num_rows == 3
        assert morsel.num_columns == 5
        assert morsel.column_names == [b"id", b"name", b"score", b"active", b"tags"]

    def test_morsel_roundtrip_with_nulls(self):
        """Test that a Morsel's to_arrow() round-trip preserves null counts."""
        rows = [
            {"int_col": 1, "float_col": 1.5, "bool_col": True, "str_col": "a"},
            {"int_col": None, "float_col": None, "bool_col": None, "str_col": None},
            {"int_col": 3, "float_col": 3.5, "bool_col": False, "str_col": "c"},
        ]
        morsel = _read_morsel(rows)
        roundtrip = morsel.to_arrow()

        assert roundtrip.num_rows == 3
        assert roundtrip.num_columns == 4
        for col_name in ("int_col", "float_col", "bool_col", "str_col"):
            assert roundtrip.column(col_name).null_count == 1

    def test_large_jsonl_batch(self):
        """Test handling a larger batch of JSONL-like data."""
        n = 10000
        rows = [
            {"id": i, "value": i * 1.5, "flag": i % 2 == 0, "label": f"item_{i}"}
            for i in range(n)
        ]
        morsel = _read_morsel(rows)

        assert morsel.num_rows == n
        assert morsel.num_columns == 4


class TestJSONLMixedData:
    """Test mixed data scenarios common in JSONL."""

    def test_mixed_numeric_types(self):
        """Test that int and float columns can coexist in one file."""
        rows = [
            {"int_col": 10000, "float_col": 1.5e10},
            {"int_col": 20000, "float_col": 2.5e10},
            {"int_col": 30000, "float_col": 3.5e10},
        ]
        morsel = _read_morsel(rows)

        assert morsel.num_rows == 3
        assert morsel.num_columns == 2
        assert morsel.column(b"int_col").type == DrakenType.INT64
        assert morsel.column(b"float_col").type == DrakenType.FLOAT64

    def test_empty_arrays(self):
        """Test handling of empty (but not null) arrays."""
        rows = [{"v": []}, {"v": [1]}, {"v": []}, {"v": [2, 3]}, {"v": []}]
        vec = _read_column(rows, "v")

        assert vec.type == DrakenType.ARRAY
        assert vec.length == 5
        assert _null_count(vec) == 0  # empty arrays are not null
        assert vec.to_pylist() == [[], [1], [], [2, 3], []]

    def test_nested_nulls_in_arrays(self):
        """Test that element-level nulls inside an array are preserved,
        distinct from a row whose array value is itself absent/null."""
        rows = [{"v": [1, None, 3]}, {"v": [None]}, {"v": None}]
        vec = _read_column(rows, "v")

        assert vec.type == DrakenType.ARRAY
        assert vec.length == 3
        assert _null_count(vec) == 1  # one row-level null (the whole array is absent)
        assert vec.to_pylist() == [[1, None, 3], [None], None]


if __name__ == "__main__":  # pragma: no cover
    # Running in the IDE
    import shutil
    import time

    # Get all test classes
    test_classes = [
        TestJSONLBasicTypes,
        TestJSONLNullableSupport,
        TestJSONLMorselIntegration,
        TestJSONLMixedData,
    ]

    start_suite = time.monotonic_ns()
    width = shutil.get_terminal_size((80, 20))[0] - 15
    passed = 0
    failed = 0

    print(f"RUNNING JSONL SUPPORT TESTS")
    print("=" * 70)

    for test_class in test_classes:
        print(f"\n{test_class.__name__}")
        print("-" * 70)
        test_instance = test_class()

        # Get all test methods
        test_methods = [m for m in dir(test_instance) if m.startswith("test_")]

        for test_method in test_methods:
            method = getattr(test_instance, test_method)
            print(f"  {test_method:50} ", end="", flush=True)

            try:
                start = time.monotonic_ns()
                method()
                elapsed = int((time.monotonic_ns() - start) / 1e6)
                print(f"\033[38;2;26;185;67m{elapsed:4}ms ✅\033[0m")
                passed += 1
            except Exception as err:
                elapsed = int((time.monotonic_ns() - start) / 1e6)
                print(f"\033[0;31m{elapsed:4}ms ❌\033[0m")
                print(f"    Error: {err}")
                failed += 1

    print("\n" + "=" * 70)
    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m "
        f"({(time.monotonic_ns() - start_suite) / 1e9:.2f} seconds)"
    )
    print(
        f"  \033[38;2;26;185;67m{passed} passed "
        f"({passed * 100 // (passed + failed) if (passed + failed) > 0 else 0}%)\033[0m"
    )
    print(f"  \033[38;2;255;121;198m{failed} failed\033[0m")
