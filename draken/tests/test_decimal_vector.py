# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0

"""
Comprehensive tests for DecimalVector — the int64-backed decimal column type.

Covers:
  - Construction: from_arrow, from_constant (value, null, int, float)
  - Metadata properties: length, null_count, dtype, itemsize, ordered,
    code_width, dictionary_size, dictionary_value_type
  - Element access: __getitem__, to_pylist
  - Arrow round-trip: to_arrow
  - Row selection: take
  - Scalar comparisons: equals, not_equals, less_than, less_than_or_equals,
    greater_than, greater_than_or_equals
  - Vector-vector comparisons: equals_vector, less_than_vector, etc.
  - Set membership: in_list
  - Null predicate: is_null
  - Aggregation: sum, min, max
  - Hashing (via morsel hash) and compression (compress_into)
  - Encoding metadata stubs (no dict / no ordering)
  - Debug representation: __str__
"""

import decimal
import os
import sys
from pathlib import Path

import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from draken.vectors._decimal_vector import DecimalVector


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dense(values, precision=10, scale=2):
    """Build a DecimalVector from a Python list of Decimal/None values."""
    pa_type = pa.decimal128(precision, scale)
    arr = pa.array(
        [decimal.Decimal(str(v)) if v is not None else None for v in values],
        type=pa_type,
    )
    return DecimalVector.from_arrow(arr)


D = decimal.Decimal  # shorthand


# ===========================================================================
# Construction
# ===========================================================================


class TestFromArrow:
    def test_basic_no_nulls(self):
        v = _make_dense(["1.00", "2.00", "3.00"])
        assert len(v) == 3
        assert v.null_count == 0

    def test_with_nulls(self):
        v = _make_dense(["1.00", None, "3.00"])
        assert len(v) == 3
        assert v.null_count == 1

    def test_all_nulls(self):
        v = _make_dense([None, None])
        assert v.null_count == 2

    def test_single_element(self):
        v = _make_dense(["9.99"])
        assert len(v) == 1
        assert v[0] == D("9.99")

    def test_empty(self):
        v = _make_dense([])
        assert len(v) == 0
        assert v.null_count == 0

    def test_negative_values(self):
        v = _make_dense(["-1.50", "-99.99", "0.00"])
        assert v.to_pylist() == [D("-1.50"), D("-99.99"), D("0.00")]

    def test_precision_capped_at_18_raises(self):
        arr = pa.array([D("1.0")], type=pa.decimal128(19, 1))
        with pytest.raises(NotImplementedError):
            DecimalVector.from_arrow(arr)

    def test_wrong_arrow_type_raises(self):
        arr = pa.array([1, 2, 3], type=pa.int64())
        with pytest.raises(TypeError):
            DecimalVector.from_arrow(arr)

    def test_scale_preserved(self):
        arr = pa.array([D("3.14159")], type=pa.decimal128(10, 5))
        v = DecimalVector.from_arrow(arr)
        assert v[0] == D("3.14159")


class TestFromConstant:
    def test_decimal_value(self):
        v = DecimalVector.from_constant(D("3.14"), 5)
        assert len(v) == 5
        assert v.null_count == 0
        assert all(x == D("3.14") for x in v.to_pylist())

    def test_null_constant(self):
        v = DecimalVector.from_constant(None, 4, is_null=True)
        assert len(v) == 4
        assert v.null_count == 4
        assert v.to_pylist() == [None, None, None, None]

    def test_int_value(self):
        v = DecimalVector.from_constant(7, 3)
        assert len(v) == 3
        assert all(x == D("7") for x in v.to_pylist())

    def test_float_value(self):
        v = DecimalVector.from_constant(1.5, 2)
        assert len(v) == 2
        pyl = v.to_pylist()
        assert all(abs(float(x) - 1.5) < 1e-9 for x in pyl)

    def test_zero_length(self):
        v = DecimalVector.from_constant(D("1.0"), 0)
        assert len(v) == 0
        assert v.to_pylist() == []

    def test_negative_length_raises(self):
        with pytest.raises(ValueError):
            DecimalVector.from_constant(D("1.0"), -1)

    def test_unsupported_type_raises(self):
        with pytest.raises(TypeError):
            DecimalVector.from_constant("bad", 3)


# ===========================================================================
# Metadata
# ===========================================================================


class TestMetadata:
    def test_length_property(self):
        v = _make_dense(["1.00", "2.00"])
        assert v.length == 2

    def test_len_dunder(self):
        v = _make_dense(["1.00", "2.00", "3.00"])
        assert len(v) == 3

    def test_itemsize(self):
        v = _make_dense(["1.00"])
        assert v.itemsize == 8  # int64

    def test_dtype(self):
        v = _make_dense(["1.00"])
        # DRAKEN_INT64 constant — just verify it's an integer
        assert isinstance(v.dtype, int)

    def test_ordered_is_false(self):
        v = _make_dense(["1.00"])
        assert v.ordered is False

    def test_code_width_is_none(self):
        v = _make_dense(["1.00"])
        assert v.code_width is None

    def test_dictionary_size_is_zero(self):
        v = _make_dense(["1.00"])
        assert v.dictionary_size == 0

    def test_dictionary_value_type_is_none(self):
        v = _make_dense(["1.00"])
        assert v.dictionary_value_type is None

    def test_null_count_dense_with_nulls(self):
        v = _make_dense(["1.00", None, None])
        assert v.null_count == 2

    def test_null_count_dense_no_nulls(self):
        v = _make_dense(["1.00", "2.00"])
        assert v.null_count == 0

    def test_null_count_const(self):
        v = DecimalVector.from_constant(D("1.0"), 5)
        assert v.null_count == 0

    def test_null_count_const_null(self):
        v = DecimalVector.from_constant(None, 3, is_null=True)
        assert v.null_count == 3


# ===========================================================================
# Element access and conversion
# ===========================================================================


class TestElementAccess:
    def test_getitem_valid(self):
        v = _make_dense(["1.50", "2.50"])
        assert v[0] == D("1.50")
        assert v[1] == D("2.50")

    def test_getitem_null(self):
        v = _make_dense(["1.00", None])
        assert v[1] is None

    def test_getitem_out_of_bounds(self):
        v = _make_dense(["1.00"])
        with pytest.raises(IndexError):
            _ = v[5]

    def test_getitem_const(self):
        v = DecimalVector.from_constant(D("7.77"), 4)
        for i in range(4):
            assert v[i] == D("7.77")

    def test_getitem_const_null(self):
        v = DecimalVector.from_constant(None, 3, is_null=True)
        for i in range(3):
            assert v[i] is None

    def test_to_pylist_no_nulls(self):
        v = _make_dense(["1.00", "2.00", "3.00"])
        assert v.to_pylist() == [D("1.00"), D("2.00"), D("3.00")]

    def test_to_pylist_with_nulls(self):
        v = _make_dense(["1.00", None, "3.00"])
        assert v.to_pylist() == [D("1.00"), None, D("3.00")]

    def test_to_pylist_empty(self):
        v = _make_dense([])
        assert v.to_pylist() == []


class TestToArrow:
    def test_round_trip_no_nulls(self):
        original = [D("1.50"), D("2.50"), D("3.50")]
        v = _make_dense(["1.50", "2.50", "3.50"])
        result = v.to_arrow().to_pylist()
        assert result == original

    def test_round_trip_with_nulls(self):
        v = _make_dense(["1.00", None, "3.00"])
        result = v.to_arrow().to_pylist()
        assert result[0] == D("1.00")
        assert result[1] is None
        assert result[2] == D("3.00")

    def test_round_trip_negative(self):
        v = _make_dense(["-5.00", "0.00"])
        result = v.to_arrow().to_pylist()
        assert result == [D("-5.00"), D("0.00")]

    def test_const_to_arrow(self):
        v = DecimalVector.from_constant(D("2.50"), 3)
        result = v.to_arrow().to_pylist()
        assert all(x == D("2.50") for x in result)

    def test_const_null_to_arrow(self):
        v = DecimalVector.from_constant(None, 3, is_null=True)
        result = v.to_arrow().to_pylist()
        assert result == [None, None, None]


# ===========================================================================
# Row selection (take)
# ===========================================================================


class TestTake:
    def test_take_subset(self):
        import numpy as np

        v = _make_dense(["1.00", "2.00", "3.00", "4.00"])
        indices = np.array([0, 2], dtype=np.int32)
        t = v.take(indices)
        assert t.to_pylist() == [D("1.00"), D("3.00")]

    def test_take_with_nulls(self):
        import numpy as np

        v = _make_dense(["1.00", None, "3.00"])
        indices = np.array([1, 2], dtype=np.int32)
        t = v.take(indices)
        assert t.to_pylist() == [None, D("3.00")]

    def test_take_empty(self):
        import numpy as np

        v = _make_dense(["1.00", "2.00"])
        indices = np.array([], dtype=np.int32)
        t = v.take(indices)
        assert t.to_pylist() == []

    def test_take_const_materialises(self):
        import numpy as np

        v = DecimalVector.from_constant(D("5.55"), 5)
        indices = np.array([0, 2, 4], dtype=np.int32)
        t = v.take(indices)
        # After take, result is a dense vector
        assert t.to_pylist() == [D("5.55"), D("5.55"), D("5.55")]

    def test_take_const_null_materialises(self):
        import numpy as np

        v = DecimalVector.from_constant(None, 4, is_null=True)
        indices = np.array([1, 3], dtype=np.int32)
        t = v.take(indices)
        assert t.to_pylist() == [None, None]


# ===========================================================================
# Scalar comparisons
# ===========================================================================


class TestScalarComparisons:
    def setup_method(self):
        self.v = _make_dense(["1.00", "2.00", "3.00", None])

    def test_equals(self):
        assert self.v.equals(D("2.00")).to_pylist() == [False, True, False, None]

    def test_not_equals(self):
        assert self.v.not_equals(D("2.00")).to_pylist() == [True, False, True, None]

    def test_less_than(self):
        assert self.v.less_than(D("2.00")).to_pylist() == [True, False, False, None]

    def test_less_than_or_equals(self):
        assert self.v.less_than_or_equals(D("2.00")).to_pylist() == [True, True, False, None]

    def test_greater_than(self):
        assert self.v.greater_than(D("2.00")).to_pylist() == [False, False, True, None]

    def test_greater_than_or_equals(self):
        assert self.v.greater_than_or_equals(D("2.00")).to_pylist() == [False, True, True, None]

    def test_equals_int(self):
        v = _make_dense(["1.00", "2.00", "3.00"])
        assert v.equals(2).to_pylist() == [False, True, False]

    def test_equals_float(self):
        v = _make_dense(["1.50", "2.50"])
        assert v.equals(1.5).to_pylist() == [True, False]

    def test_no_match(self):
        assert self.v.equals(D("99.00")).to_pylist() == [False, False, False, None]

    def test_const_equals(self):
        v = DecimalVector.from_constant(D("5.00"), 4)
        assert v.equals(D("5.00")).to_pylist() == [True, True, True, True]

    def test_const_equals_no_match(self):
        v = DecimalVector.from_constant(D("5.00"), 3)
        assert v.equals(D("3.00")).to_pylist() == [False, False, False]

    def test_const_null_equals(self):
        v = DecimalVector.from_constant(None, 3, is_null=True)
        result = v.equals(D("1.00")).to_pylist()
        assert result == [None, None, None]


# ===========================================================================
# Vector-vector comparisons
# ===========================================================================


class TestVectorComparisons:
    def _pair(self, left_vals, right_vals, precision=10, scale=2):
        return _make_dense(left_vals, precision, scale), _make_dense(right_vals, precision, scale)

    def test_equals_vector(self):
        a, b = self._pair(["1.00", "2.00", "3.00"], ["1.00", "5.00", "2.00"])
        assert a.equals_vector(b).to_pylist() == [True, False, False]

    def test_not_equals_vector(self):
        a, b = self._pair(["1.00", "2.00"], ["1.00", "3.00"])
        assert a.not_equals_vector(b).to_pylist() == [False, True]

    def test_less_than_vector(self):
        a, b = self._pair(["1.00", "5.00", "3.00"], ["2.00", "3.00", "3.00"])
        assert a.less_than_vector(b).to_pylist() == [True, False, False]

    def test_less_than_or_equals_vector(self):
        a, b = self._pair(["1.00", "3.00", "5.00"], ["3.00", "3.00", "3.00"])
        assert a.less_than_or_equals_vector(b).to_pylist() == [True, True, False]

    def test_greater_than_vector(self):
        a, b = self._pair(["5.00", "2.00", "3.00"], ["3.00", "3.00", "3.00"])
        assert a.greater_than_vector(b).to_pylist() == [True, False, False]

    def test_greater_than_or_equals_vector(self):
        a, b = self._pair(["5.00", "3.00", "1.00"], ["3.00", "3.00", "3.00"])
        assert a.greater_than_or_equals_vector(b).to_pylist() == [True, True, False]

    def test_propagates_left_null(self):
        a = _make_dense(["1.00", None, "3.00"])
        b = _make_dense(["1.00", "2.00", "3.00"])
        result = a.equals_vector(b).to_pylist()
        assert result == [True, None, True]

    def test_propagates_right_null(self):
        a = _make_dense(["1.00", "2.00", "3.00"])
        b = _make_dense(["1.00", None, "3.00"])
        result = a.equals_vector(b).to_pylist()
        assert result == [True, None, True]

    def test_length_mismatch_raises(self):
        a = _make_dense(["1.00", "2.00"])
        b = _make_dense(["1.00"])
        with pytest.raises(ValueError):
            a.equals_vector(b)

    def test_scale_mismatch_raises(self):
        a = _make_dense(["1.00"], precision=10, scale=2)
        b = _make_dense(["1.000"], precision=10, scale=3)
        with pytest.raises(ValueError):
            a.equals_vector(b)


# ===========================================================================
# Set membership (in_list)
# ===========================================================================


class TestInList:
    def test_basic(self):
        v = _make_dense(["1.00", "2.00", "3.00", "4.00"])
        result = v.in_list({D("1.00"), D("3.00")})
        assert result.to_pylist() == [True, False, True, False]

    def test_propagates_null(self):
        v = _make_dense(["1.00", None, "3.00"])
        result = v.in_list({D("1.00"), D("3.00")})
        assert result.to_pylist() == [True, None, True]

    def test_no_match(self):
        v = _make_dense(["1.00", "2.00"])
        result = v.in_list({D("9.00")})
        assert result.to_pylist() == [False, False]

    def test_accepts_list(self):
        v = _make_dense(["1.00", "2.00", "3.00"])
        result = v.in_list([D("1.00"), D("3.00")])
        assert result.to_pylist() == [True, False, True]

    def test_const_match(self):
        v = DecimalVector.from_constant(D("2.00"), 4)
        result = v.in_list({D("1.00"), D("2.00")})
        assert result.to_pylist() == [True, True, True, True]

    def test_const_no_match(self):
        v = DecimalVector.from_constant(D("9.00"), 3)
        result = v.in_list({D("1.00"), D("2.00")})
        assert result.to_pylist() == [False, False, False]

    def test_const_null(self):
        v = DecimalVector.from_constant(None, 3, is_null=True)
        result = v.in_list({D("1.00")})
        assert result.to_pylist() == [None, None, None]

    def test_int_set(self):
        v = _make_dense(["1.00", "2.00", "3.00"])
        result = v.in_list({1, 3})
        assert result.to_pylist() == [True, False, True]


# ===========================================================================
# Null predicate (is_null)
# ===========================================================================


class TestIsNull:
    def test_no_nulls(self):
        v = _make_dense(["1.00", "2.00"])
        result = list(v.is_null())
        assert result == [0, 0]

    def test_with_nulls(self):
        v = _make_dense(["1.00", None, None, "4.00"])
        result = list(v.is_null())
        assert result == [0, 1, 1, 0]

    def test_all_nulls(self):
        v = _make_dense([None, None])
        result = list(v.is_null())
        assert result == [1, 1]

    def test_const_not_null(self):
        v = DecimalVector.from_constant(D("1.0"), 4)
        result = list(v.is_null())
        assert result == [0, 0, 0, 0]

    def test_const_null(self):
        v = DecimalVector.from_constant(None, 3, is_null=True)
        result = list(v.is_null())
        assert result == [1, 1, 1]

    def test_empty(self):
        # Cython does not support zero-length typed memoryviews; is_null returns
        # an empty list for zero-length vectors (consistent with Int64Vector behaviour).
        v = _make_dense([])
        result = list(v.is_null())
        assert result == []


# ===========================================================================
# Aggregation
# ===========================================================================


class TestAggregation:
    def test_sum_no_nulls(self):
        v = _make_dense(["1.00", "2.00", "3.00"])
        assert v.sum() == D("6.00")

    def test_sum_with_nulls(self):
        v = _make_dense(["1.00", None, "3.00"])
        assert v.sum() == D("4.00")

    def test_sum_all_nulls(self):
        v = _make_dense([None, None])
        assert v.sum() == D("0")

    def test_sum_const(self):
        v = DecimalVector.from_constant(D("2.50"), 4)
        assert v.sum() == D("10.0")

    def test_min_no_nulls(self):
        v = _make_dense(["3.00", "1.00", "2.00"])
        assert v.min() == D("1.00")

    def test_min_with_nulls(self):
        v = _make_dense(["3.00", None, "1.00"])
        assert v.min() == D("1.00")

    def test_min_empty_raises(self):
        v = _make_dense([])
        with pytest.raises(ValueError):
            v.min()

    def test_min_all_null_raises(self):
        v = _make_dense([None, None])
        with pytest.raises(ValueError):
            v.min()

    def test_min_const(self):
        v = DecimalVector.from_constant(D("7.77"), 3)
        assert v.min() == D("7.77")

    def test_max_no_nulls(self):
        v = _make_dense(["1.00", "5.00", "3.00"])
        assert v.max() == D("5.00")

    def test_max_with_nulls(self):
        v = _make_dense(["1.00", None, "5.00"])
        assert v.max() == D("5.00")

    def test_max_empty_raises(self):
        v = _make_dense([])
        with pytest.raises(ValueError):
            v.max()

    def test_max_all_null_raises(self):
        v = _make_dense([None, None])
        with pytest.raises(ValueError):
            v.max()

    def test_max_const(self):
        v = DecimalVector.from_constant(D("9.99"), 2)
        assert v.max() == D("9.99")

    def test_negative_min_max(self):
        v = _make_dense(["-5.00", "2.00", "-1.00"])
        assert v.min() == D("-5.00")
        assert v.max() == D("2.00")


# ===========================================================================
# Hashing and compression — tested indirectly via Morsel
#
# hash_into and compress_into are cdef methods (Cython-internal; not callable
# from Python directly).  We verify their behaviour through morsel operations
# that invoke them internally (hashing for GROUP BY, take for filtering).
# ===========================================================================


class TestHashingViaMorel:
    """Hashing is exercised via Morsel.hash() which calls hash_into internally."""

    def test_hash_no_crash(self):
        from draken.morsels.morsel import Morsel

        arr = pa.array([D("1.00"), D("2.00"), None], type=pa.decimal128(5, 2))
        tbl = pa.table({"d": arr})
        morsel = Morsel.from_arrow(tbl)
        # Morsel.hash() calls hash_into on each column — must not raise
        morsel.hash([b"d"])

    def test_hash_deterministic(self):
        from draken.morsels.morsel import Morsel

        arr = pa.array([D("1.00"), D("2.00"), D("3.00")], type=pa.decimal128(5, 2))
        tbl = pa.table({"d": arr})
        m1 = Morsel.from_arrow(tbl)
        m2 = Morsel.from_arrow(tbl)
        h1 = list(m1.hash([b"d"]))
        h2 = list(m2.hash([b"d"]))
        assert h1 == h2

    def test_null_rows_hash_consistently(self):
        from draken.morsels.morsel import Morsel

        arr = pa.array([None, None], type=pa.decimal128(5, 2))
        tbl = pa.table({"d": arr})
        m = Morsel.from_arrow(tbl)
        hashes = list(m.hash([b"d"]))
        # Both NULL rows must hash to the same sentinel value
        assert hashes[0] == hashes[1]

    def test_const_rows_hash_consistently(self):
        from draken.morsels.morsel import Morsel

        # Build a morsel whose decimal column is const-encoded by round-tripping
        # a single-value array through from_constant materialised into a morsel.
        v = DecimalVector.from_constant(D("5.00"), 3)
        arr = v.to_arrow()
        tbl = pa.table({"d": arr})
        m = Morsel.from_arrow(tbl)
        hashes = list(m.hash([b"d"]))
        # All rows have the same value — hashes must be equal
        assert hashes[0] == hashes[1] == hashes[2]


class TestCompressionViaSort:
    """compress_into is exercised indirectly via GROUP BY / sort paths that
    compress column data into sort keys.  We test correctness by verifying
    that the round-trip through to_pylist preserves values after take/filter."""

    def test_dense_round_trip_preserves_values(self):
        import numpy as np
        from draken.morsels.morsel import Morsel

        arr = pa.array([D("1.00"), D("2.00"), D("3.00")], type=pa.decimal128(5, 2))
        tbl = pa.table({"d": arr})
        morsel = Morsel.from_arrow(tbl)
        col = morsel.column(b"d")
        mask = col.greater_than(D("1.00"))
        morsel.filter_mask(mask)
        result = morsel.to_arrow()["d"].to_pylist()
        assert result == [D("2.00"), D("3.00")]

    def test_null_rows_preserved_after_take(self):
        import numpy as np
        from draken.morsels.morsel import Morsel

        arr = pa.array([D("1.00"), None, D("3.00")], type=pa.decimal128(5, 2))
        tbl = pa.table({"d": arr})
        morsel = Morsel.from_arrow(tbl)
        col = morsel.column(b"d")
        # Select only rows where value is not null (is_null returns 0 for valid)
        null_flags = list(col.is_null())
        from draken.vectors.bool_vector import BoolVector

        mask_list = [null_flags[i] == 0 for i in range(len(null_flags))]
        mask_arr = pa.array(mask_list, type=pa.bool_())
        mask_vec = BoolVector.from_arrow(mask_arr)
        morsel.filter_mask(mask_vec)
        result = morsel.to_arrow()["d"].to_pylist()
        assert result == [D("1.00"), D("3.00")]


# ===========================================================================
# __str__ representation
# ===========================================================================


class TestStr:
    def test_dense_str(self):
        v = _make_dense(["1.00", "2.00"])
        s = str(v)
        assert "DecimalVector" in s
        assert "len=2" in s

    def test_const_str(self):
        v = DecimalVector.from_constant(D("3.14"), 5)
        s = str(v)
        assert "const" in s
        assert "len=5" in s

    def test_const_null_str(self):
        v = DecimalVector.from_constant(None, 2, is_null=True)
        s = str(v)
        assert "NULL" in s


# ===========================================================================
# Integration: morsel with decimal column
# ===========================================================================


class TestMorselIntegration:
    def test_morsel_from_arrow_with_decimal(self):
        from draken.morsels.morsel import Morsel

        arr = pa.array([D("1.5"), D("2.5"), None], type=pa.decimal128(5, 1))
        tbl = pa.table({"price": arr})
        morsel = Morsel.from_arrow(tbl)
        assert morsel.num_rows == 3
        result = morsel.to_arrow()
        assert result.schema.names == ["price"]
        assert result.num_rows == 3

    def test_morsel_filter_decimal_column(self):
        from draken.morsels.morsel import Morsel

        arr = pa.array([D("1.0"), D("2.0"), D("3.0"), D("4.0")], type=pa.decimal128(5, 1))
        tbl = pa.table({"d": arr})
        morsel = Morsel.from_arrow(tbl)

        col = morsel.column(b"d")
        mask = col.greater_than(D("2.0"))
        morsel.filter_mask(mask)
        assert morsel.num_rows == 2

    def test_morsel_take_decimal_column(self):
        from draken.morsels.morsel import Morsel

        arr = pa.array([D("10.0"), D("20.0"), D("30.0")], type=pa.decimal128(5, 1))
        tbl = pa.table({"v": arr})
        morsel = Morsel.from_arrow(tbl)
        result = morsel.copy(mask=[0, 2])
        assert result.num_rows == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
