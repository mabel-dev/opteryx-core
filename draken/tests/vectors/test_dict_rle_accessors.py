"""
Tests for the encoded-form accessors on StringVector.

These accessors expose the dict / RLE representation directly so that
aggregation kernels can consume them without materializing the vector.
The tests target the Python-level wrappers (``dict_value_at``,
``dict_code_at``, ``dict_code_counts``, ``rle_value_at``,
``rle_run_length_at``, ``rle_run_count_value``) which delegate to the
underlying ``c_*`` cdef methods.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pytest

from draken.vectors.string_vector import (
    StringVector,
    _test_make_rle_string,
)


DRAKEN_ENCODING_DICTIONARY = 1
DRAKEN_ENCODING_RLE = 2


# ---------------------------------------------------------------------------
# Dictionary accessors
# ---------------------------------------------------------------------------


class TestDictAccessors:
    def test_basic_values_and_codes(self):
        v = StringVector.from_dict([0, 1, 2, 1, 0], ["alpha", "beta", "gamma"])
        assert v.encoding == DRAKEN_ENCODING_DICTIONARY
        assert len(v) == 5

        assert v.dict_value_at(0) == b"alpha"
        assert v.dict_value_at(1) == b"beta"
        assert v.dict_value_at(2) == b"gamma"

        codes = [v.dict_code_at(i) for i in range(len(v))]
        assert codes == [0, 1, 2, 1, 0]

    def test_code_counts_basic(self):
        v = StringVector.from_dict([0, 1, 2, 1, 0], ["a", "b", "c"])
        assert v.dict_code_counts() == [2, 2, 1]

    def test_code_counts_cached(self):
        v = StringVector.from_dict([0, 0, 1], ["x", "y"])
        first = v.dict_code_counts()
        second = v.dict_code_counts()
        assert first == second == [2, 1]

    def test_unreferenced_dict_codes(self):
        # codes only reference index 0; index 1 is in the dictionary but
        # never used.  Its count must be zero.
        v = StringVector.from_dict([0, 0, 0], ["used", "unused"])
        assert v.dict_code_counts() == [3, 0]

    def test_dict_with_row_nulls(self):
        v = StringVector.from_dict(
            [0, 1, 2, 1, 0],
            ["a", "b", "c"],
            row_validity=[1, 1, 1, 0, 0],
        )
        # nulls do not contribute to any code's count
        assert v.dict_code_counts() == [1, 1, 1]

    def test_zero_rows_with_dict(self):
        # Smallest constructible dict vector with no rows.
        v = StringVector.from_dict([], ["unused"])
        assert v.encoding == DRAKEN_ENCODING_DICTIONARY
        assert len(v) == 0
        assert v.dict_code_counts() == [0]

    def test_dict_value_at_out_of_range(self):
        v = StringVector.from_dict([0], ["only"])
        with pytest.raises(IndexError):
            v.dict_value_at(1)
        with pytest.raises(IndexError):
            v.dict_value_at(-1)

    def test_dict_code_at_out_of_range(self):
        v = StringVector.from_dict([0, 0], ["x"])
        with pytest.raises(IndexError):
            v.dict_code_at(2)

    def test_accessor_rejects_non_dict(self):
        dense = StringVector.from_dict([0, 0], ["x"]).materialize()
        assert dense.encoding != DRAKEN_ENCODING_DICTIONARY
        with pytest.raises(ValueError):
            dense.dict_code_counts()
        with pytest.raises(ValueError):
            dense.dict_value_at(0)


# ---------------------------------------------------------------------------
# RLE accessors
# ---------------------------------------------------------------------------


class TestRleAccessors:
    def test_basic_runs(self):
        v = _test_make_rle_string(["aa", "bb", "cc"], [3, 2, 4])
        assert v.encoding == DRAKEN_ENCODING_RLE
        assert len(v) == 9
        assert v.rle_run_count_value() == 3
        assert v.rle_value_at(0) == b"aa"
        assert v.rle_value_at(1) == b"bb"
        assert v.rle_value_at(2) == b"cc"
        assert v.rle_run_length_at(0) == 3
        assert v.rle_run_length_at(1) == 2
        assert v.rle_run_length_at(2) == 4

    def test_single_run(self):
        v = _test_make_rle_string(["solo"], [10])
        assert v.rle_run_count_value() == 1
        assert v.rle_value_at(0) == b"solo"
        assert v.rle_run_length_at(0) == 10
        assert len(v) == 10

    def test_many_short_runs(self):
        values = [f"v{i}" for i in range(50)]
        lengths = [1] * 50
        v = _test_make_rle_string(values, lengths)
        assert v.rle_run_count_value() == 50
        assert len(v) == 50
        for i in range(50):
            assert v.rle_value_at(i) == values[i].encode("utf8")
            assert v.rle_run_length_at(i) == 1

    def test_rle_value_at_out_of_range(self):
        v = _test_make_rle_string(["a"], [1])
        with pytest.raises(IndexError):
            v.rle_value_at(1)
        with pytest.raises(IndexError):
            v.rle_value_at(-1)

    def test_rle_run_length_out_of_range(self):
        v = _test_make_rle_string(["a"], [1])
        with pytest.raises(IndexError):
            v.rle_run_length_at(5)

    def test_accessor_rejects_non_rle(self):
        v = StringVector.from_dict([0], ["x"])
        with pytest.raises(ValueError):
            v.rle_run_count_value()
        with pytest.raises(ValueError):
            v.rle_value_at(0)
        with pytest.raises(ValueError):
            v.rle_run_length_at(0)


# ---------------------------------------------------------------------------
# Materialized path remains unchanged
# ---------------------------------------------------------------------------


class TestMaterializedPathPreserved:
    """Sanity check: adding the new accessors must not break to_pylist /
    materialize on dict or RLE vectors."""

    def test_dict_to_pylist(self):
        v = StringVector.from_dict([0, 1, 0], ["foo", "bar"])
        assert v.to_pylist() == [b"foo", b"bar", b"foo"]

    def test_rle_to_pylist(self):
        v = _test_make_rle_string(["x", "y"], [2, 3])
        assert v.to_pylist() == [b"x", b"x", b"y", b"y", b"y"]
