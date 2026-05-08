"""
Verify that the dict-encoded fast paths in the ungrouped aggregation kernels
produce results bit-identical to the materialized (dense) path.

Each test runs the same logical input twice — once as a dict-encoded
StringVector, once after ``.materialize()`` — and asserts equality.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pytest

from draken.morsels.morsel import Morsel
from draken.vectors.string_vector import StringVector, _test_make_rle_string
from opteryx.operators._operators import (
    AnyValueAggregate,
    CountAggregate,
    CountDistinctAggregate,
    MaxBytesAggregate,
    MinBytesAggregate,
)


def _morsel(codes, dictionary, row_validity=None):
    if row_validity is None:
        v = StringVector.from_dict(codes, dictionary)
    else:
        v = StringVector.from_dict(codes, dictionary, row_validity=row_validity)
    return Morsel.from_vectors([b"col"], [v]), v


def _dense_morsel(v):
    return Morsel.from_vectors([b"col"], [v.materialize()])


def _run(agg, *morsels):
    for m in morsels:
        agg._test_apply(m)
    return agg.get_result()


# ---------------------------------------------------------------------------
# COUNT(col)
# ---------------------------------------------------------------------------


class TestCount:
    def test_no_nulls(self):
        m, v = _morsel([0, 1, 2, 1, 0, 2, 0], ["a", "b", "c"])
        assert _run(CountAggregate(b"col", b"x"), m) == 7
        assert _run(CountAggregate(b"col", b"x"), _dense_morsel(v)) == 7

    def test_with_row_nulls(self):
        m, v = _morsel(
            [0, 1, 2, 1, 0],
            ["a", "b", "c"],
            row_validity=[1, 1, 0, 1, 0],
        )
        dict_result = _run(CountAggregate(b"col", b"x"), m)
        dense_result = _run(CountAggregate(b"col", b"x"), _dense_morsel(v))
        assert dict_result == dense_result == 3

    def test_all_nulls(self):
        m, v = _morsel(
            [0, 0], ["x"], row_validity=[0, 0],
        )
        assert _run(CountAggregate(b"col", b"x"), m) == 0
        assert _run(CountAggregate(b"col", b"x"), _dense_morsel(v)) == 0


# ---------------------------------------------------------------------------
# COUNT(DISTINCT col)
# ---------------------------------------------------------------------------


class TestCountDistinct:
    def test_basic(self):
        m, v = _morsel([0, 1, 2, 1, 0, 2, 0], ["a", "b", "c"])
        assert _run(CountDistinctAggregate(b"col", b"x"), m) == 3
        assert _run(CountDistinctAggregate(b"col", b"x"), _dense_morsel(v)) == 3

    def test_unreferenced_dict_codes_excluded(self):
        # 'b' is in the dictionary but never referenced; must not count.
        m, v = _morsel([0, 0, 0], ["a", "b"])
        dict_result = _run(CountDistinctAggregate(b"col", b"x"), m)
        dense_result = _run(CountDistinctAggregate(b"col", b"x"), _dense_morsel(v))
        assert dict_result == dense_result == 1

    def test_with_nulls(self):
        m, v = _morsel(
            [0, 1, 2, 0],
            ["x", "y", "z"],
            row_validity=[1, 1, 0, 1],
        )
        # Distinct non-null values: x, y
        dict_result = _run(CountDistinctAggregate(b"col", b"x"), m)
        dense_result = _run(CountDistinctAggregate(b"col", b"x"), _dense_morsel(v))
        assert dict_result == dense_result == 2

    def test_cross_morsel_dict_dict(self):
        # Same logical value 'shared' encoded with code 0 in both morsels but
        # in independent dictionaries — must dedupe via hash.
        v1 = StringVector.from_dict([0, 1], ["shared", "alpha"])
        v2 = StringVector.from_dict([0, 1], ["shared", "beta"])
        m1 = Morsel.from_vectors([b"col"], [v1])
        m2 = Morsel.from_vectors([b"col"], [v2])
        assert _run(CountDistinctAggregate(b"col", b"x"), m1, m2) == 3

    def test_cross_morsel_dict_dense_match(self):
        # The dict-fast-path hash must match the dense-path hash for the same
        # value, otherwise mixed-encoding morsels would over-count.
        v1 = StringVector.from_dict([0, 1], ["shared", "alpha"])
        v2 = StringVector.from_dict([0, 1], ["shared", "beta"]).materialize()
        m1 = Morsel.from_vectors([b"col"], [v1])
        m2 = Morsel.from_vectors([b"col"], [v2])
        assert _run(CountDistinctAggregate(b"col", b"x"), m1, m2) == 3

    def test_long_strings(self):
        # >32 bytes triggers the XXH3 path; ensure dict and dense agree.
        long_a = "a" * 80
        long_b = "b" * 80
        v = StringVector.from_dict([0, 1, 0, 1], [long_a, long_b])
        dict_result = _run(
            CountDistinctAggregate(b"col", b"x"),
            Morsel.from_vectors([b"col"], [v]),
        )
        dense_result = _run(
            CountDistinctAggregate(b"col", b"x"),
            _dense_morsel(v),
        )
        assert dict_result == dense_result == 2


# ---------------------------------------------------------------------------
# MIN / MAX
# ---------------------------------------------------------------------------


class TestMinMax:
    def test_min_basic(self):
        m, v = _morsel([0, 1, 2, 1], ["banana", "apple", "cherry"])
        assert _run(MinBytesAggregate(b"col", b"x"), m) == b"apple"
        assert _run(MinBytesAggregate(b"col", b"x"), _dense_morsel(v)) == b"apple"

    def test_max_basic(self):
        m, v = _morsel([0, 1, 2, 1], ["banana", "apple", "cherry"])
        assert _run(MaxBytesAggregate(b"col", b"x"), m) == b"cherry"
        assert _run(MaxBytesAggregate(b"col", b"x"), _dense_morsel(v)) == b"cherry"

    def test_min_skips_unreferenced_dict_entry(self):
        # 'aaaa' would be the lex-min if referenced — but it isn't, so
        # the result must be 'banana'.
        m, v = _morsel([1, 1, 2], ["aaaa", "banana", "cherry"])
        assert _run(MinBytesAggregate(b"col", b"x"), m) == b"banana"
        assert _run(MinBytesAggregate(b"col", b"x"), _dense_morsel(v)) == b"banana"

    def test_min_max_with_nulls(self):
        # 'apple' (code 1) is referenced only by row 1, which is null.  So
        # its valid-count is 0 and it must not be considered for min/max.
        m, v = _morsel(
            [0, 1, 2, 0],
            ["banana", "apple", "cherry"],
            row_validity=[1, 0, 1, 1],
        )
        assert _run(MinBytesAggregate(b"col", b"x"), m) == b"banana"
        assert _run(MaxBytesAggregate(b"col", b"x"), m) == b"cherry"
        assert _run(MinBytesAggregate(b"col", b"x"), _dense_morsel(v)) == b"banana"
        assert _run(MaxBytesAggregate(b"col", b"x"), _dense_morsel(v)) == b"cherry"

    def test_min_max_all_null(self):
        m, v = _morsel([0, 0], ["x"], row_validity=[0, 0])
        assert _run(MinBytesAggregate(b"col", b"x"), m) is None
        assert _run(MaxBytesAggregate(b"col", b"x"), m) is None


# ---------------------------------------------------------------------------
# ANY_VALUE
# ---------------------------------------------------------------------------


class TestAnyValue:
    def test_returns_first_valid(self):
        m, v = _morsel([0, 1, 2], ["banana", "apple", "cherry"])
        assert _run(AnyValueAggregate(b"col", b"x"), m) == b"banana"

    def test_skips_leading_nulls(self):
        m, v = _morsel(
            [0, 1, 2],
            ["banana", "apple", "cherry"],
            row_validity=[0, 1, 1],
        )
        assert _run(AnyValueAggregate(b"col", b"x"), m) == b"apple"

    def test_short_circuits_after_first(self):
        # Once seen, subsequent morsels must not change the result.
        m1, _ = _morsel([0], ["first"])
        m2, _ = _morsel([0], ["second"])
        assert _run(AnyValueAggregate(b"col", b"x"), m1, m2) == b"first"

    def test_all_null(self):
        m, _ = _morsel([0, 0], ["x"], row_validity=[0, 0])
        assert _run(AnyValueAggregate(b"col", b"x"), m) is None


# ---------------------------------------------------------------------------
# RLE-encoded fast paths
# ---------------------------------------------------------------------------


def _rle_morsel(values, run_lengths):
    v = _test_make_rle_string(values, run_lengths)
    return Morsel.from_vectors([b"col"], [v]), v


def _rle_dense_morsel(values, run_lengths):
    """Dense morsel with the same logical values for parity comparisons."""
    expanded = []
    for val, rl in zip(values, run_lengths):
        expanded.extend([val] * rl)
    if not expanded:
        # _materialize via from_dict needs a non-empty dictionary
        return None
    # Build dense via from_dict + materialize
    unique = list({v: None for v in expanded}.keys())
    code_of = {v: i for i, v in enumerate(unique)}
    codes = [code_of[v] for v in expanded]
    v = StringVector.from_dict(codes, unique).materialize()
    return Morsel.from_vectors([b"col"], [v])


class TestRleCount:
    def test_basic(self):
        m, _ = _rle_morsel(["a", "b", "c"], [3, 2, 4])
        assert _run(CountAggregate(b"col", b"x"), m) == 9

    def test_single_run(self):
        m, _ = _rle_morsel(["solo"], [10])
        assert _run(CountAggregate(b"col", b"x"), m) == 10


class TestRleCountDistinct:
    def test_basic(self):
        m, _ = _rle_morsel(["a", "b", "a", "c"], [2, 3, 1, 4])
        # 'a' appears in two non-adjacent runs but is one distinct value.
        rle_result = _run(CountDistinctAggregate(b"col", b"x"), m)
        dense_result = _run(
            CountDistinctAggregate(b"col", b"x"),
            _rle_dense_morsel(["a", "b", "a", "c"], [2, 3, 1, 4]),
        )
        assert rle_result == dense_result == 3

    def test_long_run_values(self):
        long_a = "a" * 80
        long_b = "b" * 80
        m, _ = _rle_morsel([long_a, long_b, long_a], [5, 7, 3])
        rle_result = _run(CountDistinctAggregate(b"col", b"x"), m)
        dense_result = _run(
            CountDistinctAggregate(b"col", b"x"),
            _rle_dense_morsel([long_a, long_b, long_a], [5, 7, 3]),
        )
        assert rle_result == dense_result == 2

    def test_cross_morsel_rle_dense_match(self):
        # Hash equivalence with the dense path so mixed-encoding morsels
        # don't double-count.
        m_rle, _ = _rle_morsel(["shared", "alpha"], [2, 3])
        m_dense = _rle_dense_morsel(["shared", "beta"], [2, 3])
        assert _run(CountDistinctAggregate(b"col", b"x"), m_rle, m_dense) == 3


class TestRleMinMax:
    def test_min_max(self):
        m, _ = _rle_morsel(["banana", "apple", "cherry"], [3, 2, 4])
        assert _run(MinBytesAggregate(b"col", b"x"), m) == b"apple"
        assert _run(MaxBytesAggregate(b"col", b"x"), m) == b"cherry"

    def test_min_skips_zero_length_runs(self):
        # A pathological zero-length run must not contribute.
        m, _ = _rle_morsel(["zzz", "aaa", "mmm"], [0, 5, 3])
        assert _run(MinBytesAggregate(b"col", b"x"), m) == b"aaa"


class TestRleAnyValue:
    def test_returns_first_run_value(self):
        m, _ = _rle_morsel(["first", "second"], [4, 6])
        assert _run(AnyValueAggregate(b"col", b"x"), m) == b"first"

    def test_skips_empty_run(self):
        m, _ = _rle_morsel(["never", "first"], [0, 3])
        assert _run(AnyValueAggregate(b"col", b"x"), m) == b"first"

    def test_short_circuits_after_first(self):
        m1, _ = _rle_morsel(["first"], [2])
        m2, _ = _rle_morsel(["second"], [5])
        assert _run(AnyValueAggregate(b"col", b"x"), m1, m2) == b"first"
