"""
Native unit tests for DATE32 ingestion, readback, and ops in draken.draken_native.

Coverage matrix:
  nullability:  no nulls / some nulls / all null
  sizes:        0 / 1 / <8 (tail) / large
  edge values:  epoch, pre-epoch, far-future
  shapes:       sequence / constant / dict
  no descriptor: DATE32 must carry no logical-type descriptor
  ops:          compare_scalar, compare_vector, hash, min, max, between, in_list,
                take, materialize, compress
  hypothesis:   round-trip identity, ordering preserved
"""

import pytest
from datetime import date

import draken.draken_native as dn

EQ, NE, GT, GE, LT, LE = 0, 1, 2, 3, 4, 5

EPOCH    = date(1970, 1, 1)
D_2024   = date(2024, 1, 1)
D_2025   = date(2025, 6, 15)
D_NEG    = date(1960, 3, 7)   # pre-epoch
D_FAR    = date(2100, 12, 31)


def seq(lst):
    return dn.vector_date32_from_sequence(lst)

def pylist(v):
    return v.to_pylist()

def cmp_s(v, scalar, op):
    return pylist(v.compare_scalar(scalar, op))


# ===========================================================================
# 1.  Type tag and no descriptor
# ===========================================================================

class TestTypeTag:
    def test_type_is_date32(self):
        assert seq([D_2024]).type == dn.DrakenType.DATE32

    def test_type_tag_with_nulls(self):
        assert seq([D_2024, None]).type == dn.DrakenType.DATE32

    def test_type_tag_empty(self):
        assert seq([]).type == dn.DrakenType.DATE32

    def test_date32_abi_value(self):
        # DRAKEN_DATE32 = 30 (frozen per buffers.h)
        assert dn.DrakenType.DATE32.value == 30

    def test_no_logical_descriptor(self):
        # DATE32 is not parameterized — logical_type_unit must be None
        assert seq([D_2024]).logical_type_unit is None

    def test_no_logical_offset(self):
        assert seq([D_2024]).logical_type_offset_minutes is None


# ===========================================================================
# 2.  Ingestion + readback round-trip
# ===========================================================================

class TestRoundTripEmpty:
    def test_empty_returns_empty(self):
        assert pylist(seq([])) == []

    def test_empty_len(self):
        assert len(seq([])) == 0


class TestRoundTripSingle:
    def test_epoch(self):
        assert pylist(seq([EPOCH])) == [EPOCH]

    def test_2024(self):
        assert pylist(seq([D_2024])) == [D_2024]

    def test_pre_epoch(self):
        assert pylist(seq([D_NEG])) == [D_NEG]

    def test_far_future(self):
        assert pylist(seq([D_FAR])) == [D_FAR]

    def test_single_null(self):
        assert pylist(seq([None])) == [None]


class TestRoundTripTail:
    def test_five_no_nulls(self):
        src = [D_2024, D_2025, EPOCH, D_NEG, D_FAR]
        assert pylist(seq(src)) == src

    def test_five_some_nulls(self):
        src = [D_2024, None, EPOCH, None, D_2025]
        assert pylist(seq(src)) == src

    def test_five_all_nulls(self):
        src = [None] * 5
        assert pylist(seq(src)) == src


class TestRoundTripLarge:
    def test_large_no_nulls(self):
        from datetime import timedelta
        base = date(2000, 1, 1)
        src = [base + timedelta(days=i) for i in range(10_000)]
        assert pylist(seq(src)) == src

    def test_large_every_7th_null(self):
        from datetime import timedelta
        base = date(2000, 1, 1)
        src = [None if i % 7 == 0 else base + timedelta(days=i) for i in range(10_000)]
        assert pylist(seq(src)) == src


# ===========================================================================
# 3.  Factory shapes: constant and dict
# ===========================================================================

class TestConstant:
    def test_value_readback(self):
        v = dn.vector_date32_from_constant(D_2024, 5)
        assert pylist(v) == [D_2024] * 5

    def test_null_all_null(self):
        v = dn.vector_date32_from_constant(None, 3)
        assert pylist(v) == [None, None, None]

    def test_empty(self):
        v = dn.vector_date32_from_constant(D_2024, 0)
        assert pylist(v) == []

    def test_type(self):
        assert dn.vector_date32_from_constant(D_2024, 3).type == dn.DrakenType.DATE32


class TestDict:
    def test_basic(self):
        v = dn.vector_date32_from_dict([D_2024, D_2025], [0, 1, 0])
        assert pylist(v) == [D_2024, D_2025, D_2024]

    def test_with_nulls(self):
        v = dn.vector_date32_from_dict([D_2024, D_2025], [0, 1, 0], [True, False, True])
        assert pylist(v) == [D_2024, None, D_2024]

    def test_type(self):
        v = dn.vector_date32_from_dict([D_2024], [0])
        assert v.type == dn.DrakenType.DATE32


# ===========================================================================
# 4.  __getitem__
# ===========================================================================

class TestGetItem:
    def test_forward_index(self):
        v = seq([D_2024, D_2025, EPOCH])
        assert v[0] == D_2024
        assert v[2] == EPOCH

    def test_negative_index(self):
        v = seq([D_2024, D_2025, EPOCH])
        assert v[-1] == EPOCH

    def test_null_via_getitem(self):
        v = seq([D_2024, None, D_2025])
        assert v[1] is None

    def test_out_of_range(self):
        v = seq([D_2024])
        with pytest.raises(IndexError):
            _ = v[1]


# ===========================================================================
# 5.  min / max
# ===========================================================================

class TestMinMax:
    def test_min(self):
        assert seq([D_2025, D_2024, EPOCH]).min() == EPOCH

    def test_max(self):
        assert seq([D_2025, D_2024, EPOCH]).max() == D_2025

    def test_min_pre_epoch(self):
        assert seq([D_NEG, EPOCH, D_2024]).min() == D_NEG

    def test_min_skips_nulls(self):
        assert seq([None, D_2025, D_2024]).min() == D_2024

    def test_max_skips_nulls(self):
        assert seq([None, D_2025, D_2024]).max() == D_2025

    def test_min_empty_raises(self):
        with pytest.raises(Exception):
            seq([]).min()

    def test_min_all_null_raises(self):
        with pytest.raises(Exception):
            seq([None, None]).min()

    def test_result_is_date(self):
        assert isinstance(seq([D_2024]).min(), date)


# ===========================================================================
# 6.  hash
# ===========================================================================

class TestHash:
    def test_length_matches(self):
        assert len(seq([D_2024, D_2025, EPOCH]).hash()) == 3

    def test_values_are_integers(self):
        for h in seq([D_2024, D_2025]).hash():
            assert isinstance(h, int)

    def test_same_input_same_hash(self):
        src = [D_2024, D_2025, EPOCH]
        assert seq(src).hash() == seq(src).hash()

    def test_distinct_dates_distinct_hashes(self):
        from datetime import timedelta
        base = date(2000, 1, 1)
        dates = [base + timedelta(days=i) for i in range(20)]
        result = seq(dates).hash()
        assert len(set(result)) == 20

    def test_null_sentinel_consistent(self):
        h1 = seq([None]).hash()[0]
        h2 = seq([None]).hash()[0]
        assert h1 == h2

    def test_null_differs_from_epoch(self):
        h_null = seq([None]).hash()[0]
        h_epoch = seq([EPOCH]).hash()[0]
        assert h_null != h_epoch


# ===========================================================================
# 7.  compare_scalar
# ===========================================================================

class TestCompareScalar:
    DATA = [D_2024, D_2025, EPOCH, D_NEG]

    def test_eq(self):
        v = seq(self.DATA)
        expected = [x == D_2024 for x in self.DATA]
        assert cmp_s(v, D_2024, EQ) == expected

    def test_lt(self):
        v = seq(self.DATA)
        expected = [x < D_2024 for x in self.DATA]
        assert cmp_s(v, D_2024, LT) == expected

    def test_null_scalar_all_null(self):
        v = seq([D_2024, D_2025])
        result = cmp_s(v, None, EQ)
        assert all(x is None for x in result)

    def test_null_row_null_output(self):
        v = seq([D_2024, None, D_2025])
        result = cmp_s(v, D_2024, EQ)
        assert result[0] is True
        assert result[1] is None
        assert result[2] is False

    def test_result_type_is_bool(self):
        assert seq([D_2024]).compare_scalar(D_2024, EQ).type == dn.DrakenType.BOOL


# ===========================================================================
# 8.  compare_vector
# ===========================================================================

class TestCompareVector:
    def test_eq_equal_vectors(self):
        src = [D_2024, D_2025, EPOCH]
        result = pylist(seq(src).compare_vector(seq(src), EQ))
        assert result == [True, True, True]

    def test_lt_ordering(self):
        a = seq([D_2024, D_2025])
        b = seq([D_2025, D_2024])
        result = pylist(a.compare_vector(b, LT))
        assert result == [True, False]

    def test_null_propagation(self):
        a = seq([D_2024, None])
        b = seq([D_2024, D_2025])
        result = pylist(a.compare_vector(b, EQ))
        assert result[0] is True
        assert result[1] is None


# ===========================================================================
# 9.  between
# ===========================================================================

class TestBetween:
    DATA = [D_NEG, EPOCH, D_2024, D_2025, D_FAR]

    def test_closed_closed(self):
        v = seq(self.DATA)
        expected = [EPOCH <= x <= D_2024 for x in self.DATA]
        assert pylist(v.between(EPOCH, D_2024, True, True)) == expected

    def test_open_open(self):
        v = seq(self.DATA)
        expected = [EPOCH < x < D_2024 for x in self.DATA]
        assert pylist(v.between(EPOCH, D_2024, False, False)) == expected

    def test_null_propagates(self):
        v = seq([D_2024, None, D_2025])
        result = pylist(v.between(EPOCH, D_2025))
        assert result[0] is True
        assert result[1] is None

    def test_result_type_is_bool(self):
        v = seq([D_2024])
        assert v.between(EPOCH, D_FAR).type == dn.DrakenType.BOOL


# ===========================================================================
# 10.  in_list
# ===========================================================================

class TestInList:
    def test_member_present(self):
        v = seq([D_2024, D_2025, EPOCH])
        result = pylist(v.in_list([D_2024, EPOCH]))
        assert result == [True, False, True]

    def test_empty_set_all_false(self):
        v = seq([D_2024, D_2025])
        assert pylist(v.in_list([])) == [False, False]

    def test_null_propagates(self):
        v = seq([D_2024, None])
        result = pylist(v.in_list([D_2024]))
        assert result[0] is True
        assert result[1] is None

    def test_result_type_is_bool(self):
        assert seq([D_2024]).in_list([D_2024]).type == dn.DrakenType.BOOL


# ===========================================================================
# 11.  take / materialize / compress
# ===========================================================================

class TestTake:
    def test_preserves_type(self):
        r = seq([D_2024, D_2025, EPOCH]).take([2, 0])
        assert r.type == dn.DrakenType.DATE32

    def test_correct_values(self):
        v = seq([D_2024, D_2025, EPOCH])
        r = v.take([2, 0, 1])
        assert pylist(r) == [EPOCH, D_2024, D_2025]

    def test_nulls(self):
        v = seq([D_2024, None, D_2025])
        r = v.take([1, 0])
        assert pylist(r)[0] is None

    def test_no_descriptor_on_take_result(self):
        r = seq([D_2024, D_2025]).take([0])
        assert r.logical_type_unit is None


class TestMaterialize:
    def test_preserves_type(self):
        r = seq([D_2024, D_2025]).materialize()
        assert r.type == dn.DrakenType.DATE32

    def test_roundtrip(self):
        src = [D_2024, None, D_2025, EPOCH]
        assert pylist(seq(src).materialize()) == src


class TestCompress:
    def test_preserves_type(self):
        r = seq([D_2024, D_2025]).compress()
        assert r.type == dn.DrakenType.DATE32

    def test_roundtrip(self):
        src = [D_2024, None, D_2025, EPOCH]
        assert pylist(seq(src).compress().materialize()) == src


# ===========================================================================
# 12.  Hypothesis property tests
# ===========================================================================

from hypothesis import given, settings
from hypothesis import strategies as st

_date_strategy = st.one_of(
    st.none(),
    st.dates(min_value=date(1, 1, 1), max_value=date(9999, 12, 31)),
)
_date_list     = st.lists(_date_strategy, min_size=0, max_size=100)
_date_nonempty = st.lists(
    st.dates(min_value=date(1900, 1, 1), max_value=date(2200, 12, 31)),
    min_size=1, max_size=100,
)


class TestHypothesis:
    @given(src=_date_list)
    @settings(max_examples=200)
    def test_roundtrip_identity(self, src):
        result = pylist(seq(src))
        assert len(result) == len(src)
        for i, (orig, got) in enumerate(zip(src, result)):
            if orig is None:
                assert got is None
            else:
                assert got == orig

    @given(src=_date_nonempty)
    @settings(max_examples=200)
    def test_ordering_preserved(self, src):
        result = pylist(seq(src))
        for a_orig, b_orig, a_got, b_got in zip(src, src[1:], result, result[1:]):
            if a_orig < b_orig:
                assert a_got < b_got
            elif a_orig > b_orig:
                assert a_got > b_got

    @given(src=_date_nonempty)
    @settings(max_examples=100)
    def test_min_max_correct(self, src):
        v = seq(src)
        assert v.min() == min(src)
        assert v.max() == max(src)
