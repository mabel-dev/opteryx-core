"""
Native unit tests for int64 ingestion and readback in draken.draken_native.

These tests assert the CORRECT answer. They are the primary correctness signal.

Coverage matrix (per 04_testing.md §1):
  nullability:  no nulls / some nulls / all null
  size:         0 / 1 / <8 (tail) / large
  edge values:  INT64_MIN, INT64_MAX, 0, -1, negatives

Both VALUES and NULL POSITIONS are asserted.
"""

import sys

import pytest

import draken.draken_native as dn

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make(lst):
    return dn.vector_from_sequence(lst)


def pylist(lst):
    """Round-trip a Python list through vector_from_sequence and back."""
    return make(lst).to_pylist()


def null_mask(lst):
    """Return a list of booleans: True where the original list had None."""
    v = make(lst)
    pl = v.to_pylist()
    return [x is None for x in pl]


# ---------------------------------------------------------------------------
# 1.  Size × nullability
# ---------------------------------------------------------------------------


class TestSizeEmpty:
    def test_empty_no_nulls(self):
        assert pylist([]) == []

    def test_empty_len(self):
        assert len(make([])) == 0

    def test_empty_type(self):
        assert make([]).type == dn.DrakenType.INT64


class TestSizeSingle:
    def test_single_value(self):
        assert pylist([42]) == [42]

    def test_single_null(self):
        assert pylist([None]) == [None]

    def test_single_negative(self):
        assert pylist([-1]) == [-1]


class TestSizeTail:
    """<8 elements — exercises the tail of any SIMD loop."""

    def test_five_no_nulls(self):
        assert pylist([1, 2, 3, 4, 5]) == [1, 2, 3, 4, 5]

    def test_five_some_nulls(self):
        src = [1, None, 3, None, 5]
        assert pylist(src) == src

    def test_five_all_nulls(self):
        src = [None, None, None, None, None]
        assert pylist(src) == src

    def test_seven_leading_null(self):
        src = [None, 2, 3, 4, 5, 6, 7]
        assert pylist(src) == src

    def test_seven_trailing_null(self):
        src = [1, 2, 3, 4, 5, 6, None]
        assert pylist(src) == src


class TestSizeLarge:
    def test_large_no_nulls(self):
        src = list(range(10_000))
        assert pylist(src) == src

    def test_large_every_7th_null(self):
        src = [None if i % 7 == 0 else i for i in range(10_000)]
        assert pylist(src) == src

    def test_large_all_nulls(self):
        src = [None] * 10_000
        assert pylist(src) == src


# ---------------------------------------------------------------------------
# 2.  Null position correctness
# ---------------------------------------------------------------------------


class TestNullPositions:
    """Null *positions* must be preserved exactly — not just the count."""

    def test_first_null(self):
        src = [None, 1, 2, 3]
        assert null_mask(src) == [True, False, False, False]

    def test_last_null(self):
        src = [1, 2, 3, None]
        assert null_mask(src) == [False, False, False, True]

    def test_alternating(self):
        src = [None, 1, None, 3, None]
        assert null_mask(src) == [True, False, True, False, True]

    def test_none_position_value_is_None(self):
        src = [10, None, 30]
        result = pylist(src)
        assert result[1] is None

    def test_non_null_after_null_preserves_value(self):
        src = [None, 99]
        result = pylist(src)
        assert result[0] is None
        assert result[1] == 99

    def test_null_does_not_pollute_neighbours(self):
        src = [1, None, 3]
        result = pylist(src)
        assert result[0] == 1
        assert result[1] is None
        assert result[2] == 3

    def test_all_null_every_position(self):
        n = 64
        src = [None] * n
        result = pylist(src)
        for i, v in enumerate(result):
            assert v is None, f"position {i} should be None"

    def test_no_null_validity_is_absent(self):
        # All-valid vectors must not expose spurious None values.
        src = list(range(100))
        result = pylist(src)
        assert all(v is not None for v in result)


# ---------------------------------------------------------------------------
# 3.  Edge values
# ---------------------------------------------------------------------------


class TestEdgeValues:
    def test_int64_max(self):
        assert pylist([INT64_MAX]) == [INT64_MAX]

    def test_int64_min(self):
        assert pylist([INT64_MIN]) == [INT64_MIN]

    def test_zero(self):
        assert pylist([0]) == [0]

    def test_negative_one(self):
        assert pylist([-1]) == [-1]

    def test_minus_large(self):
        assert pylist([-999_999_999_999]) == [-999_999_999_999]

    def test_edge_mix(self):
        src = [INT64_MIN, -1, 0, 1, INT64_MAX]
        assert pylist(src) == src

    def test_edge_with_nulls(self):
        src = [INT64_MIN, None, INT64_MAX, None, 0]
        result = pylist(src)
        assert result == src

    def test_null_adjacent_to_zero_is_not_zero(self):
        # Regression guard: None must not alias 0 in the data buffer.
        src = [None, 0, None]
        result = pylist(src)
        assert result[0] is None, "null must be None, not 0"
        assert result[1] == 0, "zero must survive as 0"
        assert result[2] is None, "null must be None, not 0"

    def test_null_is_not_zero(self):
        # Regression guard: null must not round-trip as 0.
        result = pylist([None])
        assert result[0] is None
        assert result[0] != 0

    def test_straddling_2_to_the_53(self):
        # Correctness rule: int64 exact compare must survive int64 values > 2^53
        # (float64 cannot represent these exactly). Ingestion/readback must not
        # silently cast through float. Assert both 2^53 and 2^53+1 survive.
        v = 2**53
        assert pylist([v, v + 1]) == [v, v + 1]


# ---------------------------------------------------------------------------
# 4.  Type tag and __len__
# ---------------------------------------------------------------------------


class TestTypeAndLen:
    def test_type_tag_no_nulls(self):
        assert make([1, 2, 3]).type == dn.DrakenType.INT64

    def test_type_tag_with_nulls(self):
        assert make([1, None]).type == dn.DrakenType.INT64

    def test_type_tag_all_nulls(self):
        assert make([None, None]).type == dn.DrakenType.INT64

    def test_type_tag_value_frozen(self):
        # INT64 ABI value must be 4 (frozen per buffers.h).
        assert dn.DrakenType.INT64.value == 4

    def test_len_matches_input(self):
        for n in [0, 1, 5, 100]:
            assert len(make(list(range(n)))) == n

    def test_length_prop_matches_len(self):
        v = make([10, 20, 30])
        assert v.length == len(v)


# ---------------------------------------------------------------------------
# 5.  __getitem__
# ---------------------------------------------------------------------------


class TestGetItem:
    def test_forward_indices(self):
        v = make([10, 20, 30])
        assert v[0] == 10
        assert v[1] == 20
        assert v[2] == 30

    def test_negative_indices(self):
        v = make([10, 20, 30])
        assert v[-1] == 30
        assert v[-2] == 20
        assert v[-3] == 10

    def test_null_via_getitem(self):
        v = make([5, None, 7])
        assert v[0] == 5
        assert v[1] is None
        assert v[2] == 7

    def test_out_of_range_raises(self):
        v = make([1, 2, 3])
        with pytest.raises(IndexError):
            _ = v[3]
        with pytest.raises(IndexError):
            _ = v[-4]

    def test_edge_value_via_getitem(self):
        v = make([INT64_MIN, INT64_MAX])
        assert v[0] == INT64_MIN
        assert v[1] == INT64_MAX


# ---------------------------------------------------------------------------
# 6.  hash() — int64 single-column hash
# ---------------------------------------------------------------------------


class TestHashEmpty:
    def test_empty_returns_empty_list(self):
        assert make([]).hash() == []

    def test_empty_type_is_list(self):
        result = make([]).hash()
        assert isinstance(result, list)


class TestHashLengthAndType:
    def test_length_matches_input(self):
        for n in [1, 5, 100]:
            assert len(make(list(range(n))).hash()) == n

    def test_values_are_integers(self):
        result = make([1, 2, 3]).hash()
        for v in result:
            assert isinstance(v, int)

    def test_values_fit_uint64(self):
        result = make([1, 2, 3]).hash()
        for v in result:
            assert 0 <= v < 2**64


class TestHashDeterminism:
    def test_same_input_same_output(self):
        seq = [10, 20, 30]
        assert make(seq).hash() == make(seq).hash()

    def test_large_same_input_same_output(self):
        seq = list(range(10_000))
        assert make(seq).hash() == make(seq).hash()


class TestHashDistinctValues:
    """Different logical values must produce different hashes (collision avoidance
    for small, structurally distinct inputs)."""

    def test_consecutive_integers_are_distinct(self):
        result = make([0, 1, 2, 3, 4]).hash()
        assert len(set(result)) == 5, "expected 5 distinct hashes"

    def test_edge_values_are_distinct(self):
        result = make([INT64_MIN, -1, 0, 1, INT64_MAX]).hash()
        assert len(set(result)) == 5

    def test_hash_zero_vs_one_differ(self):
        h0 = make([0]).hash()[0]
        h1 = make([1]).hash()[0]
        assert h0 != h1


class TestHashNullHandling:
    def test_null_produces_consistent_sentinel(self):
        h1 = make([None]).hash()[0]
        h2 = make([None]).hash()[0]
        assert h1 == h2

    def test_null_hash_differs_from_zero_hash(self):
        h_null = make([None]).hash()[0]
        h_zero = make([0]).hash()[0]
        assert h_null != h_zero, "null must not hash identically to integer 0"

    def test_null_in_middle_does_not_affect_neighbours(self):
        a = make([1, None, 3]).hash()
        b = make([1, None, 3]).hash()
        assert a == b
        assert a[0] == make([1]).hash()[0]
        assert a[2] == make([3]).hash()[0]

    def test_all_nulls_same_sentinel(self):
        result = make([None, None, None, None]).hash()
        assert len(set(result)) == 1, "all nulls must hash to the same sentinel"

    def test_null_position_independence(self):
        # hash(null) must be the same regardless of where in the vector it falls
        h = make([None]).hash()[0]
        for seq in [[None, 1], [1, None], [1, None, 2], [None, None]]:
            for i, v in enumerate(seq):
                if v is None:
                    assert make(seq).hash()[i] == h


class TestHashEdgeValues:
    def test_int64_min(self):
        result = make([INT64_MIN]).hash()
        assert len(result) == 1 and isinstance(result[0], int)

    def test_int64_max(self):
        result = make([INT64_MAX]).hash()
        assert len(result) == 1 and isinstance(result[0], int)

    def test_zero(self):
        result = make([0]).hash()
        assert len(result) == 1

    def test_negative_one(self):
        result = make([-1]).hash()
        assert len(result) == 1

    def test_int64_min_max_differ(self):
        hmin = make([INT64_MIN]).hash()[0]
        hmax = make([INT64_MAX]).hash()[0]
        assert hmin != hmax


class TestHashSizetail:
    """< 8 elements — covers SIMD tail handling."""

    def test_seven_elements_no_nulls(self):
        seq = list(range(7))
        result = make(seq).hash()
        assert len(result) == 7
        assert len(set(result)) == 7

    def test_seven_elements_some_nulls(self):
        seq = [1, None, 3, None, 5, 6, 7]
        result = make(seq).hash()
        assert len(result) == 7
        null_h = make([None]).hash()[0]
        assert result[1] == null_h
        assert result[3] == null_h


class TestHashLarge:
    def test_large_no_nulls_all_distinct(self):
        n = 100_000
        result = make(list(range(n))).hash()
        assert len(result) == n
        # consecutive ints should produce no collisions at this scale
        assert len(set(result)) == n

    def test_large_with_nulls_length_correct(self):
        seq = [None if i % 7 == 0 else i for i in range(10_000)]
        result = make(seq).hash()
        assert len(result) == len(seq)

    def test_large_chunk_boundary(self):
        # 1024 is the scratch-buffer chunk size; test sizes that straddle it
        for n in [1023, 1024, 1025, 2048, 2049]:
            result = make(list(range(n))).hash()
            assert len(result) == n
