# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0

"""
Comprehensive and extensive tests for group key codec edge cases and scenarios.

This test suite provides comprehensive coverage of:
- Boundary value tests for int64, date32, time32, time64, timestamp64
- String encoding edge cases (empty, long, unicode, special characters)
- Null handling in various positions and combinations
- Type combinations (pairs and triples)
- Large dataset tests (10K, 100K, 1M keys)
- String special cases (ASCII, UTF-8, emoji, RTL text, combining characters)
- Offset stability verification
- Duplicate pattern tests
- Aggregation correctness across various distributions
- Round-trip stability
- Payload integrity verification
"""

import os
import sys

import pyarrow as pa
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.aggregations.key_codec import (
    decode_multi_payload_keys,
    decode_single_payload_key,
    smoke_test_native_group_key_codec,
    smoke_test_native_single_encoded_key_codec,
    smoke_test_native_single_fixed_key_codec,
)
from opteryx.compiled.draken.morsels.morsel import Morsel

from opteryx.operators.shuffle import AggregationSpec, ShuffleGroupByOperation

# Key type constants
KEY_MULTI_FIXED_INT = 1
KEY_MULTI_FIXED_DATE32 = 2
KEY_MULTI_FIXED_TIME32 = 3
KEY_MULTI_FIXED_TIME64 = 4
KEY_MULTI_FIXED_TIMESTAMP64 = 5
KEY_MULTI_ENCODED_STRING = 6


# ============================================================================
# Helper Functions
# ============================================================================


def _normalize_value(value):
    """Normalize values for comparison."""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _rows_by_key(rows, key_columns):
    """Group rows by key columns for easier assertion."""
    if isinstance(key_columns, str):
        key_columns = [key_columns]

    out = {}
    for row in rows:
        key = tuple(_normalize_value(row[column]) for column in key_columns)
        out[key] = {k: _normalize_value(v) for k, v in row.items()}
    return out


def _finalize_rows(group_by_columns, aggregations, table):
    """Execute groupby operation and return finalized rows."""
    morsel = Morsel.from_arrow(table)
    op = ShuffleGroupByOperation(
        group_by_columns=group_by_columns,
        aggregations=aggregations,
    )
    op.ingest(morsel)
    return op.finalize().to_arrow().to_pylist()


# ============================================================================
# INT64 BOUNDARY VALUE TESTS
# ============================================================================


class TestInt64BoundaryValues:
    """Test int64 codec with boundary and edge case values."""

    def test_int64_zero_value(self):
        """Test encoding/decoding zero."""
        table = pa.table({"k": pa.array([0, 0, 1], type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(0,)]["cnt"] == 2
        assert by_key[(1,)]["cnt"] == 1

    def test_int64_max_value(self):
        """Test encoding/decoding maximum int64 value."""
        max_int64 = 9223372036854775807  # 2^63 - 1
        table = pa.table({"k": pa.array([max_int64], type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(max_int64,)]["cnt"] == 1

    def test_int64_min_value(self):
        """Test encoding/decoding minimum int64 value."""
        min_int64 = -9223372036854775808  # -2^63
        table = pa.table({"k": pa.array([min_int64], type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(min_int64,)]["cnt"] == 1

    def test_int64_positive_values(self):
        """Test various positive int64 values."""
        values = [1, 100, 1000, 1_000_000, 1_000_000_000, 1_000_000_000_000]
        table = pa.table({"k": pa.array(values, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        for val in values:
            assert by_key[(val,)]["cnt"] == 1

    def test_int64_negative_values(self):
        """Test various negative int64 values."""
        values = [-1, -100, -1000, -1_000_000, -1_000_000_000, -1_000_000_000_000]
        table = pa.table({"k": pa.array(values, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        for val in values:
            assert by_key[(val,)]["cnt"] == 1

    def test_int64_powers_of_two(self):
        """Test powers of two as boundary values."""
        values = [1 << i for i in range(0, 62)]  # 2^0 through 2^61
        table = pa.table({"k": pa.array(values, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        for val in values:
            assert by_key[(val,)]["cnt"] == 1

    def test_int64_boundary_pairs(self):
        """Test min/max boundary values together."""
        min_int64 = -9223372036854775808
        max_int64 = 9223372036854775807
        table = pa.table({"k": pa.array([min_int64, max_int64, min_int64], type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(min_int64,)]["cnt"] == 2
        assert by_key[(max_int64,)]["cnt"] == 1


# ============================================================================
# STRING ENCODING TESTS
# ============================================================================


class TestStringEncoding:
    """Test string encoding/decoding with various edge cases."""

    def test_empty_string(self):
        """Test empty string encoding (distinct from null)."""
        table = pa.table({"s": pa.array(["", "", "a"], type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert by_key[("",)]["cnt"] == 2
        assert by_key[("a",)]["cnt"] == 1

    def test_single_character_strings(self):
        """Test single character strings."""
        table = pa.table({"s": pa.array(["a", "b", "c", "a"], type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert by_key[("a",)]["cnt"] == 2
        assert by_key[("b",)]["cnt"] == 1
        assert by_key[("c",)]["cnt"] == 1

    def test_string_with_spaces(self):
        """Test strings containing spaces."""
        table = pa.table({"s": pa.array(["a b", "a  b", " ", "  ", "a b"], type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert by_key[("a b",)]["cnt"] == 2
        assert by_key[("a  b",)]["cnt"] == 1
        assert by_key[(" ",)]["cnt"] == 1
        assert by_key[("  ",)]["cnt"] == 1

    def test_string_with_newlines(self):
        """Test strings containing newline characters."""
        table = pa.table({"s": pa.array(["a\nb", "a\nb", "a\n", "\nb"], type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert by_key[("a\nb",)]["cnt"] == 2
        assert by_key[("a\n",)]["cnt"] == 1
        assert by_key[("\nb",)]["cnt"] == 1

    def test_string_with_tabs_and_special_whitespace(self):
        """Test strings with various whitespace characters."""
        table = pa.table({"s": pa.array(["a\tb", "a\tb", "a\rb", "\t", "\r"], type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert by_key[("a\tb",)]["cnt"] == 2
        assert by_key[("a\rb",)]["cnt"] == 1
        assert by_key[("\t",)]["cnt"] == 1
        assert by_key[("\r",)]["cnt"] == 1

    def test_string_unicode_ascii(self):
        """Test ASCII range Unicode characters."""
        table = pa.table({"s": pa.array(["abc", "ABC", "123", "!@#", "abc"], type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert by_key[("abc",)]["cnt"] == 2
        assert by_key[("ABC",)]["cnt"] == 1
        assert by_key[("123",)]["cnt"] == 1
        assert by_key[("!@#",)]["cnt"] == 1

    def test_string_unicode_multibyte(self):
        """Test multibyte UTF-8 characters."""
        table = pa.table({"s": pa.array(["café", "café", "日本", "中文"], type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert by_key[("café",)]["cnt"] == 2
        assert by_key[("日本",)]["cnt"] == 1
        assert by_key[("中文",)]["cnt"] == 1

    def test_string_emoji(self):
        """Test emoji characters."""
        table = pa.table({"s": pa.array(["😀", "😀", "🚀", "❤️", "😀"], type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert by_key[("😀",)]["cnt"] == 3
        assert by_key[("🚀",)]["cnt"] == 1
        assert by_key[("❤️",)]["cnt"] == 1

    def test_string_rtl_text(self):
        """Test right-to-left text."""
        table = pa.table({"s": pa.array(["שלום", "שלום", "مرحبا", "שלום"], type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert by_key[("שלום",)]["cnt"] == 3
        assert by_key[("مرحبا",)]["cnt"] == 1

    def test_string_combining_characters(self):
        """Test strings with combining diacritical marks."""
        # "e" with combining acute accent = "é" (composed differently)
        combining_e = "e\u0301"  # e + combining acute
        precomposed_e = "é"  # precomposed
        table = pa.table(
            {"s": pa.array([combining_e, combining_e, precomposed_e], type=pa.string())}
        )
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        # These should be distinct in the codec
        assert len(by_key) == 2 or by_key[(combining_e,)]["cnt"] == 2

    def test_string_zero_width_characters(self):
        """Test strings with zero-width characters."""
        zwj = "\u200d"  # Zero-width joiner
        zwnj = "\u200c"  # Zero-width non-joiner
        table = pa.table(
            {"s": pa.array(["a" + zwj + "b", "a" + zwj + "b", "a" + zwnj + "b"], type=pa.string())}
        )
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert len(by_key) == 2  # ZWJ and ZWNJ are different

    def test_string_null_bytes_in_string(self):
        """Test strings containing null bytes."""
        table = pa.table({"s": pa.array(["a\x00b", "a\x00b", "ab"], type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert len(by_key) == 2
        assert by_key[("ab",)]["cnt"] == 1

    def test_string_1kb_length(self):
        """Test 1KB string."""
        large_string = "x" * 1024
        table = pa.table({"s": pa.array([large_string, large_string, "short"], type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert by_key[(large_string,)]["cnt"] == 2
        assert by_key[("short",)]["cnt"] == 1

    def test_string_100kb_length(self):
        """Test 100KB string."""
        large_string = "y" * (100 * 1024)
        table = pa.table({"s": pa.array([large_string, "small"], type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert by_key[(large_string,)]["cnt"] == 1
        assert by_key[("small",)]["cnt"] == 1

    def test_string_varying_lengths(self):
        """Test multiple strings with varying lengths."""
        strings = ["a", "ab", "abc", "abcd", "abcde", "a", "ab"]
        table = pa.table({"s": pa.array(strings, type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert by_key[("a",)]["cnt"] == 2
        assert by_key[("ab",)]["cnt"] == 2
        assert by_key[("abc",)]["cnt"] == 1
        assert by_key[("abcd",)]["cnt"] == 1
        assert by_key[("abcde",)]["cnt"] == 1

    def test_string_all_256_byte_values(self):
        """Test strings containing all possible byte values 0-255."""
        # Create a string with all byte values
        all_bytes = bytes(range(256)).decode("latin-1")
        table = pa.table({"s": pa.array([all_bytes, all_bytes, "normal"], type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert by_key[(all_bytes,)]["cnt"] == 2
        assert by_key[("normal",)]["cnt"] == 1


# ============================================================================
# NULL HANDLING TESTS
# ============================================================================


class TestNullHandling:
    """Test null handling in various scenarios."""

    def test_null_in_single_key_all_null(self):
        """Test all null values in single key."""
        table = pa.table({"k": pa.array([None, None, None], type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(None,)]["cnt"] == 3

    def test_null_in_single_key_mixed(self):
        """Test mixed null and non-null values in single key."""
        table = pa.table({"k": pa.array([1, None, 2, None, 1], type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(1,)]["cnt"] == 2
        assert by_key[(2,)]["cnt"] == 1
        assert by_key[(None,)]["cnt"] == 2

    def test_null_in_string_key(self):
        """Test null values in string key."""
        table = pa.table({"s": pa.array(["a", None, "b", None, "a"], type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert by_key[("a",)]["cnt"] == 2
        assert by_key[("b",)]["cnt"] == 1
        assert by_key[(None,)]["cnt"] == 2

    def test_null_multi_key_all_null(self):
        """Test all columns null in multi-key."""
        table = pa.table(
            {
                "k1": pa.array([None, None], type=pa.int64()),
                "k2": pa.array([None, None], type=pa.string()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k1", "k2"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, ["k1", "k2"])
        assert by_key[(None, None)]["cnt"] == 2

    def test_null_multi_key_first_null(self):
        """Test first key null in multi-key."""
        table = pa.table(
            {
                "k1": pa.array([None, 1, None], type=pa.int64()),
                "k2": pa.array(["a", "a", "b"], type=pa.string()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k1", "k2"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, ["k1", "k2"])
        assert by_key[(None, "a")]["cnt"] == 1
        assert by_key[(1, "a")]["cnt"] == 1
        assert by_key[(None, "b")]["cnt"] == 1

    def test_null_multi_key_second_null(self):
        """Test second key null in multi-key."""
        table = pa.table(
            {
                "k1": pa.array([1, 1, 2], type=pa.int64()),
                "k2": pa.array([None, "a", None], type=pa.string()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k1", "k2"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, ["k1", "k2"])
        assert by_key[(1, None)]["cnt"] == 1
        assert by_key[(1, "a")]["cnt"] == 1
        assert by_key[(2, None)]["cnt"] == 1

    def test_null_multi_key_both_null_and_mixed(self):
        """Test both keys with nulls in mixed scenarios."""
        table = pa.table(
            {
                "k1": pa.array([1, None, None, 1, None], type=pa.int64()),
                "k2": pa.array(["a", "a", None, None, None], type=pa.string()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k1", "k2"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, ["k1", "k2"])
        assert by_key[(1, "a")]["cnt"] == 1
        assert by_key[(None, "a")]["cnt"] == 1
        assert by_key[(None, None)]["cnt"] == 2
        assert by_key[(1, None)]["cnt"] == 1

    def test_null_vs_empty_string_distinction(self):
        """Test that null and empty string are distinct."""
        table = pa.table({"s": pa.array(["", None, "", None], type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert by_key[("",)]["cnt"] == 2
        assert by_key[(None,)]["cnt"] == 2


# ============================================================================
# DATE/TIME TYPE TESTS
# ============================================================================


class TestDateTimeTypes:
    """Test date and time type boundaries."""

    def test_date32_zero_value(self):
        """Test date32 with zero (epoch)."""
        table = pa.table({"d": pa.array([0, 0, 1], type=pa.date32())})
        rows = _finalize_rows(
            group_by_columns=["d"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "d")
        assert by_key[(0,)]["cnt"] == 2

    def test_date32_positive_dates(self):
        """Test positive date32 values."""
        table = pa.table({"d": pa.array([1, 100, 1000, 1], type=pa.date32())})
        rows = _finalize_rows(
            group_by_columns=["d"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "d")
        assert by_key[(1,)]["cnt"] == 2
        assert by_key[(100,)]["cnt"] == 1

    def test_date32_negative_dates(self):
        """Test negative date32 values (before epoch)."""
        table = pa.table({"d": pa.array([-1, -100, -1], type=pa.date32())})
        rows = _finalize_rows(
            group_by_columns=["d"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "d")
        assert by_key[(-1,)]["cnt"] == 2
        assert by_key[(-100,)]["cnt"] == 1

    def test_date32_with_nulls(self):
        """Test date32 with null values."""
        table = pa.table({"d": pa.array([1, None, 2, None, 1], type=pa.date32())})
        rows = _finalize_rows(
            group_by_columns=["d"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "d")
        assert by_key[(1,)]["cnt"] == 2
        assert by_key[(2,)]["cnt"] == 1
        assert by_key[(None,)]["cnt"] == 2

    def test_time32_values(self):
        """Test time32 (seconds) values."""
        table = pa.table({"t": pa.array([0, 3600, 86400, 3600], type=pa.time32("s"))})
        rows = _finalize_rows(
            group_by_columns=["t"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "t")
        assert by_key[(0,)]["cnt"] == 1
        assert by_key[(3600,)]["cnt"] == 2

    def test_time64_values(self):
        """Test time64 (microseconds) values."""
        table = pa.table(
            {
                "t": pa.array(
                    [0, 1000000, 3600000000, 1000000],
                    type=pa.time64("us"),
                )
            }
        )
        rows = _finalize_rows(
            group_by_columns=["t"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "t")
        assert by_key[(0,)]["cnt"] == 1
        assert by_key[(1000000,)]["cnt"] == 2

    def test_timestamp_microsecond_values(self):
        """Test timestamp with microsecond precision."""
        table = pa.table(
            {
                "ts": pa.array(
                    [1000000, 2000000, 1000000, None],
                    type=pa.timestamp("us"),
                )
            }
        )
        rows = _finalize_rows(
            group_by_columns=["ts"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "ts")
        assert by_key[(1000000,)]["cnt"] == 2
        assert by_key[(2000000,)]["cnt"] == 1
        assert by_key[(None,)]["cnt"] == 1

    def test_date32_int64_combination(self):
        """Test date32 and int64 together."""
        table = pa.table(
            {
                "d": pa.array([1, 2, 1], type=pa.date32()),
                "i": pa.array([100, 100, 200], type=pa.int64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["d", "i"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, ["d", "i"])
        assert by_key[(1, 100)]["cnt"] == 1
        assert by_key[(2, 100)]["cnt"] == 1
        assert by_key[(1, 200)]["cnt"] == 1

    def test_time32_time64_combination(self):
        """Test time32 and time64 together."""
        table = pa.table(
            {
                "t32": pa.array([100, 200, 100], type=pa.time32("s")),
                "t64": pa.array([1000, 1000, 2000], type=pa.time64("us")),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["t32", "t64"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, ["t32", "t64"])
        assert by_key[(100, 1000)]["cnt"] == 1
        assert by_key[(200, 1000)]["cnt"] == 1
        assert by_key[(100, 2000)]["cnt"] == 1

    def test_timestamp_with_nulls(self):
        """Test timestamp with mixed nulls."""
        table = pa.table(
            {
                "ts": pa.array(
                    [1000, None, 2000, None, 1000],
                    type=pa.timestamp("us"),
                )
            }
        )
        rows = _finalize_rows(
            group_by_columns=["ts"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "ts")
        assert by_key[(1000,)]["cnt"] == 2
        assert by_key[(2000,)]["cnt"] == 1
        assert by_key[(None,)]["cnt"] == 2


# ============================================================================
# TYPE COMBINATION TESTS
# ============================================================================


class TestTypeCombinations:
    """Test combinations of different types."""

    def test_int64_int64_combination(self):
        """Test two int64 columns."""
        table = pa.table(
            {
                "k1": pa.array([1, 2, 1], type=pa.int64()),
                "k2": pa.array([10, 10, 20], type=pa.int64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k1", "k2"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, ["k1", "k2"])
        assert by_key[(1, 10)]["cnt"] == 1
        assert by_key[(2, 10)]["cnt"] == 1
        assert by_key[(1, 20)]["cnt"] == 1

    def test_int64_string_combination(self):
        """Test int64 and string columns."""
        table = pa.table(
            {
                "i": pa.array([1, 1, 2], type=pa.int64()),
                "s": pa.array(["a", "b", "a"], type=pa.string()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["i", "s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, ["i", "s"])
        assert by_key[(1, "a")]["cnt"] == 1
        assert by_key[(1, "b")]["cnt"] == 1
        assert by_key[(2, "a")]["cnt"] == 1

    def test_int64_date32_string_combination(self):
        """Test int64, date32, and string columns."""
        table = pa.table(
            {
                "i": pa.array([1, 1, 2], type=pa.int64()),
                "d": pa.array([100, 100, 200], type=pa.date32()),
                "s": pa.array(["a", "b", "a"], type=pa.string()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["i", "d", "s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, ["i", "d", "s"])
        assert by_key[(1, 100, "a")]["cnt"] == 1
        assert by_key[(1, 100, "b")]["cnt"] == 1
        assert by_key[(2, 200, "a")]["cnt"] == 1

    def test_date32_time32_combination(self):
        """Test date32 and time32 columns."""
        table = pa.table(
            {
                "d": pa.array([1, 1, 2], type=pa.date32()),
                "t": pa.array([100, 200, 100], type=pa.time32("s")),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["d", "t"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, ["d", "t"])
        assert by_key[(1, 100)]["cnt"] == 1
        assert by_key[(1, 200)]["cnt"] == 1
        assert by_key[(2, 100)]["cnt"] == 1

    def test_time64_timestamp_string_combination(self):
        """Test time64, timestamp, and string columns."""
        table = pa.table(
            {
                "t64": pa.array([1000, 1000, 2000], type=pa.time64("us")),
                "ts": pa.array([100, 200, 100], type=pa.timestamp("us")),
                "s": pa.array(["x", "x", "y"], type=pa.string()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["t64", "ts", "s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, ["t64", "ts", "s"])
        assert by_key[(1000, 100, "x")]["cnt"] == 1
        assert by_key[(1000, 200, "x")]["cnt"] == 1
        assert by_key[(2000, 100, "y")]["cnt"] == 1


# ============================================================================
# LARGE DATASET TESTS
# ============================================================================


class TestLargeDatasets:
    """Test codec with large datasets."""

    def test_10k_distinct_keys(self):
        """Test 10K distinct int64 keys."""
        keys = list(range(10000))
        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert len(by_key) == 10000
        for i in range(10000):
            assert by_key[(i,)]["cnt"] == 1

    def test_10k_keys_with_duplicates(self):
        """Test 10K keys with many duplicates."""
        keys = list(range(1000)) * 10  # 1000 distinct, each repeated 10 times
        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert len(by_key) == 1000
        for i in range(1000):
            assert by_key[(i,)]["cnt"] == 10

    def test_100k_keys_with_heavy_duplication(self):
        """Test 100K keys with 95% duplication."""
        # 100 distinct keys, each repeated 1000 times
        keys = list(range(100)) * 1000
        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert len(by_key) == 100
        for i in range(100):
            assert by_key[(i,)]["cnt"] == 1000

    def test_string_keys_10k_distinct(self):
        """Test 10K distinct string keys."""
        keys = [f"key_{i:05d}" for i in range(10000)]
        table = pa.table({"s": pa.array(keys, type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert len(by_key) == 10000

    def test_string_keys_100k_with_pattern(self):
        """Test 100K string keys with repeating pattern."""
        # 1000 distinct patterns, each repeated 100 times
        keys = [f"pattern_{i % 1000}" for i in range(100000)]
        table = pa.table({"s": pa.array(keys, type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert len(by_key) == 1000
        for i in range(1000):
            assert by_key[(f"pattern_{i}",)]["cnt"] == 100


# ============================================================================
# DUPLICATE PATTERN TESTS
# ============================================================================


class TestDuplicatePatterns:
    """Test various duplicate patterns."""

    def test_all_same_value(self):
        """Test all rows have same key."""
        table = pa.table({"k": pa.array([42] * 1000, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert len(by_key) == 1
        assert by_key[(42,)]["cnt"] == 1000

    def test_alternating_values(self):
        """Test alternating between two values."""
        keys = [i % 2 for i in range(1000)]
        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert len(by_key) == 2
        assert by_key[(0,)]["cnt"] == 500
        assert by_key[(1,)]["cnt"] == 500

    def test_clustered_duplicates(self):
        """Test clusters of same values."""
        # 10 clusters of 100 identical values each
        keys = []
        for cluster in range(10):
            keys.extend([cluster] * 100)
        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert len(by_key) == 10
        for i in range(10):
            assert by_key[(i,)]["cnt"] == 100

    def test_random_duplicates(self):
        """Test random distribution of duplicates."""
        # 100 distinct values, randomly distributed
        import random

        random.seed(42)
        keys = [random.randint(0, 99) for _ in range(10000)]
        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert len(by_key) == 100
        assert sum(row["cnt"] for row in by_key.values()) == 10000


# ============================================================================
# AGGREGATION CORRECTNESS TESTS
# ============================================================================


class TestAggregationCorrectness:
    """Test aggregation functions with various distributions."""

    def test_count_aggregation(self):
        """Test count aggregation."""
        table = pa.table({"k": pa.array([1, 1, 2, 2, 2, 3], type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(1,)]["cnt"] == 2
        assert by_key[(2,)]["cnt"] == 3
        assert by_key[(3,)]["cnt"] == 1

    def test_sum_aggregation(self):
        """Test sum aggregation."""
        table = pa.table(
            {
                "k": pa.array([1, 1, 2, 2], type=pa.int64()),
                "v": pa.array([10.0, 20.0, 5.0, 15.0], type=pa.float64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(1,)]["sum_v"] == 30.0
        assert by_key[(2,)]["sum_v"] == 20.0

    def test_avg_aggregation(self):
        """Test average aggregation."""
        table = pa.table(
            {
                "k": pa.array([1, 1, 2, 2, 2], type=pa.int64()),
                "v": pa.array([10.0, 20.0, 3.0, 6.0, 9.0], type=pa.float64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="avg_v", function="avg", column="v")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(1,)]["avg_v"] == 15.0
        assert by_key[(2,)]["avg_v"] == 6.0

    def test_min_aggregation(self):
        """Test min aggregation."""
        table = pa.table(
            {
                "k": pa.array([1, 1, 2, 2], type=pa.int64()),
                "v": pa.array([10.0, 20.0, 5.0, 15.0], type=pa.float64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="min_v", function="min", column="v")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(1,)]["min_v"] == 10.0
        assert by_key[(2,)]["min_v"] == 5.0

    def test_max_aggregation(self):
        """Test max aggregation."""
        table = pa.table(
            {
                "k": pa.array([1, 1, 2, 2], type=pa.int64()),
                "v": pa.array([10.0, 20.0, 5.0, 15.0], type=pa.float64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="max_v", function="max", column="v")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(1,)]["max_v"] == 20.0
        assert by_key[(2,)]["max_v"] == 15.0

    def test_multi_aggregations(self):
        """Test multiple aggregations together."""
        table = pa.table(
            {
                "k": pa.array([1, 1, 2, 2, 2], type=pa.int64()),
                "v": pa.array([10.0, 20.0, 3.0, 6.0, 9.0], type=pa.float64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt", function="count", column="*"),
                AggregationSpec(alias="sum_v", function="sum", column="v"),
                AggregationSpec(alias="avg_v", function="avg", column="v"),
                AggregationSpec(alias="min_v", function="min", column="v"),
                AggregationSpec(alias="max_v", function="max", column="v"),
            ],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(1,)]["cnt"] == 2
        assert by_key[(1,)]["sum_v"] == 30.0
        assert by_key[(1,)]["avg_v"] == 15.0
        assert by_key[(1,)]["min_v"] == 10.0
        assert by_key[(1,)]["max_v"] == 20.0


# ============================================================================
# ROUND-TRIP STABILITY TESTS
# ============================================================================


class TestRoundTripStability:
    """Test encode/decode/encode stability."""

    def test_int64_round_trip(self):
        """Test int64 round-trip encoding."""
        values = [0, 1, -1, 123456789, -123456789]
        table1 = pa.table({"k": pa.array(values, type=pa.int64())})
        rows1 = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table1,
        )

        # Re-encode with same values
        table2 = pa.table({"k": pa.array(values, type=pa.int64())})
        rows2 = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table2,
        )

        by_key1 = _rows_by_key(rows1, "k")
        by_key2 = _rows_by_key(rows2, "k")
        assert by_key1 == by_key2

    def test_string_round_trip(self):
        """Test string round-trip encoding."""
        values = ["a", "abc", "test", "", "x" * 100]
        table1 = pa.table({"s": pa.array(values, type=pa.string())})
        rows1 = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table1,
        )

        table2 = pa.table({"s": pa.array(values, type=pa.string())})
        rows2 = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table2,
        )

        by_key1 = _rows_by_key(rows1, "s")
        by_key2 = _rows_by_key(rows2, "s")
        assert by_key1 == by_key2

    def test_multi_key_round_trip(self):
        """Test multi-key round-trip encoding."""
        table1 = pa.table(
            {
                "i": pa.array([1, 2, 1], type=pa.int64()),
                "s": pa.array(["a", "b", "a"], type=pa.string()),
            }
        )
        rows1 = _finalize_rows(
            group_by_columns=["i", "s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table1,
        )

        table2 = pa.table(
            {
                "i": pa.array([1, 2, 1], type=pa.int64()),
                "s": pa.array(["a", "b", "a"], type=pa.string()),
            }
        )
        rows2 = _finalize_rows(
            group_by_columns=["i", "s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table2,
        )

        by_key1 = _rows_by_key(rows1, ["i", "s"])
        by_key2 = _rows_by_key(rows2, ["i", "s"])
        assert by_key1 == by_key2


# ============================================================================
# OFFSET STABILITY TESTS
# ============================================================================


class TestOffsetStability:
    """Test that offsets remain monotonic and non-overlapping."""

    def test_single_fixed_offset_structure(self):
        """Test offset structure for single fixed records."""
        _, _, payload_offsets, payload_bytes = smoke_test_native_single_fixed_key_codec()

        # Validate offset structure
        assert payload_offsets[0] == 0
        assert payload_offsets[-1] == len(payload_bytes)

        # Check monotonic increasing
        for i in range(1, len(payload_offsets)):
            assert payload_offsets[i] >= payload_offsets[i - 1]

    def test_single_encoded_offset_structure(self):
        """Test offset structure for single encoded records."""
        _, _, payload_offsets, payload_bytes = smoke_test_native_single_encoded_key_codec()

        # Validate offset structure
        assert payload_offsets[0] == 0
        assert payload_offsets[-1] == len(payload_bytes)

        # Check monotonic increasing
        for i in range(1, len(payload_offsets)):
            assert payload_offsets[i] >= payload_offsets[i - 1]

    def test_multi_offset_structure(self):
        """Test offset structure for multi-key records."""
        (
            _,
            _,
            _,
            _,
            payload_offsets,
            payload_bytes,
        ) = smoke_test_native_group_key_codec()

        # Validate offset structure
        assert payload_offsets[0] == 0
        assert payload_offsets[-1] == len(payload_bytes)

        # Check monotonic increasing
        for i in range(1, len(payload_offsets)):
            assert payload_offsets[i] >= payload_offsets[i - 1]


# ============================================================================
# PAYLOAD INTEGRITY TESTS
# ============================================================================


class TestPayloadIntegrity:
    """Test payload data integrity across encode/decode cycles."""

    def test_single_fixed_payload_integrity(self):
        """Test payload integrity for single fixed records."""
        decoded_value, decoded_valid, _, payload_bytes = smoke_test_native_single_fixed_key_codec()

        # Payload should contain bitmap (1 byte) + value (8 bytes)
        assert len(payload_bytes) == 9

        # First byte is bitmap
        assert payload_bytes[0] in [0x00, 0x01]

        # Valid flag matches bitmap
        if decoded_valid == 1:
            assert payload_bytes[0] == 0x01
        else:
            assert payload_bytes[0] == 0x00

    def test_single_encoded_payload_integrity(self):
        """Test payload integrity for single encoded records."""
        decoded_value, decoded_valid, _, payload_bytes = (
            smoke_test_native_single_encoded_key_codec()
        )

        # Payload should have bitmap + length + data
        assert len(payload_bytes) > 5  # At least 1 + 4 bytes

        # First byte is bitmap
        assert payload_bytes[0] in [0x00, 0x01]

    def test_large_string_payload_integrity(self):
        """Test payload integrity with large strings."""
        large_string = "x" * 10000
        table = pa.table({"s": pa.array([large_string, "short"], type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")

        # Verify data integrity by checking it comes back unchanged
        assert large_string in [k[0] for k in by_key]
        assert "short" in [k[0] for k in by_key]


# ============================================================================
# NULL PROPAGATION IN AGGREGATIONS TESTS
# ============================================================================


class TestNullPropagationInAggregations:
    """Test null handling in aggregations."""

    def test_count_with_all_nulls(self):
        """Test count with all null values."""
        table = pa.table(
            {
                "k": pa.array([1, 1, 1], type=pa.int64()),
                "v": pa.array([None, None, None], type=pa.float64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt", function="count", column="v"),
                AggregationSpec(alias="cnt_star", function="count", column="*"),
            ],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(1,)]["cnt"] == 0  # count(v) with all nulls
        assert by_key[(1,)]["cnt_star"] == 3  # count(*) counts rows

    def test_sum_with_mixed_nulls(self):
        """Test sum with mixed null values."""
        table = pa.table(
            {
                "k": pa.array([1, 1, 1], type=pa.int64()),
                "v": pa.array([10.0, None, 20.0], type=pa.float64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(1,)]["sum_v"] == 30.0  # 10 + 20, skipping null

    def test_avg_with_mixed_nulls(self):
        """Test average with mixed null values."""
        table = pa.table(
            {
                "k": pa.array([1, 1, 1], type=pa.int64()),
                "v": pa.array([10.0, None, 20.0], type=pa.float64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="avg_v", function="avg", column="v")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(1,)]["avg_v"] == 15.0  # (10 + 20) / 2

    def test_min_with_mixed_nulls(self):
        """Test min with mixed null values."""
        table = pa.table(
            {
                "k": pa.array([1, 1, 1], type=pa.int64()),
                "v": pa.array([10.0, None, 20.0], type=pa.float64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="min_v", function="min", column="v")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(1,)]["min_v"] == 10.0

    def test_max_with_mixed_nulls(self):
        """Test max with mixed null values."""
        table = pa.table(
            {
                "k": pa.array([1, 1, 1], type=pa.int64()),
                "v": pa.array([10.0, None, 20.0], type=pa.float64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="max_v", function="max", column="v")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(1,)]["max_v"] == 20.0


# ============================================================================
# STRESS COMBINATION TESTS
# ============================================================================


class TestStressCombinations:
    """Test stress combinations of nulls, large data, and edge values."""

    def test_stress_mixed_nulls_large_strings_edge_numbers(self):
        """Stress test: mixed nulls + large strings + edge int64 values."""
        min_int64 = -9223372036854775808
        max_int64 = 9223372036854775807
        large_string = "x" * 10000

        table = pa.table(
            {
                "i": pa.array([min_int64, max_int64, None, 0, min_int64, None], type=pa.int64()),
                "s": pa.array(
                    [large_string, large_string, "", None, "test", None], type=pa.string()
                ),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["i", "s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, ["i", "s"])

        assert by_key[(min_int64, large_string)]["cnt"] == 1
        assert by_key[(max_int64, large_string)]["cnt"] == 1
        assert by_key[(None, "")]["cnt"] == 1
        assert by_key[(0, None)]["cnt"] == 1
        assert by_key[(min_int64, "test")]["cnt"] == 1
        assert by_key[(None, None)]["cnt"] == 1

    def test_stress_all_types_with_nulls(self):
        """Stress test: all types with various null patterns."""
        table = pa.table(
            {
                "i": pa.array([1, None, 1, None], type=pa.int64()),
                "d": pa.array([100, 100, None, None], type=pa.date32()),
                "t": pa.array([1000, 2000, 1000, 2000], type=pa.time32("s")),
                "s": pa.array(["a", None, "a", "b"], type=pa.string()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["i", "d", "t", "s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, ["i", "d", "t", "s"])

        # Should have 4 distinct groups
        assert len(by_key) == 4
        assert sum(row["cnt"] for row in by_key.values()) == 4


# ============================================================================
# EDGE CASE INTERACTION TESTS
# ============================================================================


class TestEdgeCaseInteractions:
    """Test interactions between edge cases."""

    def test_empty_string_in_multi_key(self):
        """Test empty string in multi-key with other edge cases."""
        table = pa.table(
            {
                "i": pa.array([0, 0, 0], type=pa.int64()),
                "s": pa.array(["", "", "a"], type=pa.string()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["i", "s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, ["i", "s"])
        assert by_key[(0, "")]["cnt"] == 2
        assert by_key[(0, "a")]["cnt"] == 1

    def test_null_and_empty_string_with_numbers(self):
        """Test null vs empty string distinction with boundary numbers."""
        table = pa.table(
            {
                "i": pa.array([0, 0, 0, 0], type=pa.int64()),
                "s": pa.array(["", None, "", None], type=pa.string()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["i", "s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, ["i", "s"])
        assert by_key[(0, "")]["cnt"] == 2
        assert by_key[(0, None)]["cnt"] == 2

    def test_unicode_emoji_rtl_in_multi_key(self):
        """Test unicode, emoji, and RTL text in multi-key."""
        table = pa.table(
            {
                "emoji": pa.array(["😀", "😀", "🚀"], type=pa.string()),
                "rtl": pa.array(["שלום", "שלום", "مرحبا"], type=pa.string()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["emoji", "rtl"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, ["emoji", "rtl"])
        assert by_key[("😀", "שלום")]["cnt"] == 2
        assert by_key[("🚀", "مرحبا")]["cnt"] == 1


# ============================================================================
# STABILITY AND CONSISTENCY TESTS
# ============================================================================


class TestStabilityAndConsistency:
    """Test codec stability and consistency."""

    def test_same_execution_same_results(self):
        """Test that same groupby produces consistent results."""
        table_data = {
            "k": pa.array([1, 1, 2, 2, None], type=pa.int64()),
            "s": pa.array(["a", "b", "a", None, "c"], type=pa.string()),
        }

        rows1 = _finalize_rows(
            group_by_columns=["k", "s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=pa.table(table_data),
        )

        rows2 = _finalize_rows(
            group_by_columns=["k", "s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=pa.table(table_data),
        )

        by_key1 = _rows_by_key(rows1, ["k", "s"])
        by_key2 = _rows_by_key(rows2, ["k", "s"])
        assert by_key1 == by_key2

    def test_order_independence(self):
        """Test that row order doesn't affect groupby results."""
        values_forward = [1, 2, 3, 1, 2, 3]
        values_backward = [3, 2, 1, 3, 2, 1]

        table_forward = pa.table({"k": pa.array(values_forward, type=pa.int64())})
        table_backward = pa.table({"k": pa.array(values_backward, type=pa.int64())})

        rows_forward = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table_forward,
        )

        rows_backward = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table_backward,
        )

        by_key_forward = _rows_by_key(rows_forward, "k")
        by_key_backward = _rows_by_key(rows_backward, "k")
        assert by_key_forward == by_key_backward


# ============================================================================
# BOUNDARY AND CORNER CASE TESTS
# ============================================================================


class TestBoundaryAndCornerCases:
    """Test specific boundary and corner cases."""

    def test_single_row_single_key(self):
        """Test single row with single key."""
        table = pa.table({"k": pa.array([42], type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(42,)]["cnt"] == 1

    def test_single_row_single_string_key(self):
        """Test single row with single string key."""
        table = pa.table({"s": pa.array(["only"], type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert by_key[("only",)]["cnt"] == 1

    def test_single_row_null_key(self):
        """Test single row with null key."""
        table = pa.table({"k": pa.array([None], type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(None,)]["cnt"] == 1

    def test_single_row_null_string_key(self):
        """Test single row with null string key."""
        table = pa.table({"s": pa.array([None], type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "s")
        assert by_key[(None,)]["cnt"] == 1

    def test_two_rows_same_key(self):
        """Test two rows with same key."""
        table = pa.table({"k": pa.array([7, 7], type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(7,)]["cnt"] == 2

    def test_two_rows_different_keys(self):
        """Test two rows with different keys."""
        table = pa.table({"k": pa.array([1, 2], type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )
        by_key = _rows_by_key(rows, "k")
        assert by_key[(1,)]["cnt"] == 1
        assert by_key[(2,)]["cnt"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
