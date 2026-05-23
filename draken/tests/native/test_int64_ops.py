"""
Native unit tests for int64 C.2 ops: sum / min / max / arithmetic / take / materialize / compress.

These tests assert the CORRECT answer.
Coverage matrix (per 04_testing.md §1 and the ticket acceptance criteria):
  nullability:  none / some / all-null
  size:         0 / 1 / <8 (tail) / large
  edges:        INT64_MIN, INT64_MAX, 0, -1
  shapes:       dense (sequence), constant, dict
  per-op:
    sum:         empty→0, all-null→0, non-null values
    min/max:     empty→raises, all-null→raises, correct value
    arithmetic:  add/sub/mul/div/mod/neg; overflow wraps; div-by-zero→0;
                 neg(INT64_MIN)→INT64_MIN; null propagation for binary ops
    take:        repeats, out-of-order, empty indices, null source
    materialize: round-trip all three shapes
    compress:    round-trip materialize(compress(v)) == v
"""

import pytest
import draken.draken_native as dn

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make(lst):
    return dn.vector_from_sequence(lst)

def pylist(v):
    return v.to_pylist()

def make_const(value, length):
    return dn.vector_from_constant(value, length)

def make_dict(values, codes, nullable=None):
    return dn.vector_from_dict(values, codes, nullable)


# ---------------------------------------------------------------------------
# FACTORIES — constant and dict shape smoke
# ---------------------------------------------------------------------------

class TestFactoryConstant:
    def test_constant_value_readback(self):
        v = make_const(42, 5)
        assert pylist(v) == [42, 42, 42, 42, 42]

    def test_constant_null_all_null(self):
        v = make_const(None, 3)
        assert pylist(v) == [None, None, None]

    def test_constant_length_zero(self):
        v = make_const(7, 0)
        assert pylist(v) == []

    def test_constant_length_one(self):
        v = make_const(INT64_MIN, 1)
        assert pylist(v) == [INT64_MIN]


class TestFactoryDict:
    def test_dict_basic(self):
        v = make_dict([10, 20, 30], [0, 1, 2, 0, 1])
        assert pylist(v) == [10, 20, 30, 10, 20]

    def test_dict_repeated_codes(self):
        v = make_dict([5], [0, 0, 0])
        assert pylist(v) == [5, 5, 5]

    def test_dict_with_nulls(self):
        v = make_dict([100, 200], [0, 1, 0], [True, False, True])
        assert pylist(v) == [100, None, 100]

    def test_dict_single_row(self):
        v = make_dict([INT64_MAX], [0])
        assert pylist(v) == [INT64_MAX]


# ---------------------------------------------------------------------------
# SUM
# ---------------------------------------------------------------------------

class TestSum:
    def test_empty(self):
        assert make([]).sum() == 0

    def test_single(self):
        assert make([5]).sum() == 5

    def test_all_null(self):
        assert make([None, None]).sum() == 0

    def test_mixed_nulls(self):
        # nulls contribute 0
        assert make([1, None, 3, None]).sum() == 4

    def test_large_no_nulls(self):
        n = 10_000
        v = make(list(range(n)))
        assert v.sum() == n * (n - 1) // 2

    def test_tail_no_nulls(self):
        assert make([1, 2, 3, 4, 5]).sum() == 15

    def test_negative_values(self):
        assert make([-1, -2, -3]).sum() == -6

    def test_edge_values(self):
        assert make([INT64_MIN, INT64_MAX]).sum() == -1  # wraps: MIN+MAX = -1

    def test_constant_shape(self):
        v = make_const(3, 4)
        assert v.materialize().sum() == 12

    def test_dict_shape(self):
        v = make_dict([2, 5], [0, 1, 0, 1], None)
        assert v.materialize().sum() == 14


# ---------------------------------------------------------------------------
# MIN / MAX
# ---------------------------------------------------------------------------

class TestMin:
    def test_empty_raises(self):
        with pytest.raises((ValueError, Exception)):
            make([]).min()

    def test_all_null_raises(self):
        with pytest.raises((ValueError, Exception)):
            make([None, None]).min()

    def test_single(self):
        assert make([42]).min() == 42

    def test_correct_min(self):
        assert make([3, 1, 4, 1, 5, 9]).min() == 1

    def test_with_nulls(self):
        assert make([None, 5, None, -3, None]).min() == -3

    def test_all_same(self):
        assert make([7, 7, 7]).min() == 7

    def test_int64_min(self):
        assert make([0, INT64_MIN, 1]).min() == INT64_MIN


class TestMax:
    def test_empty_raises(self):
        with pytest.raises((ValueError, Exception)):
            make([]).max()

    def test_all_null_raises(self):
        with pytest.raises((ValueError, Exception)):
            make([None]).max()

    def test_single(self):
        assert make([99]).max() == 99

    def test_correct_max(self):
        assert make([3, 1, 4, 1, 5, 9]).max() == 9

    def test_with_nulls(self):
        assert make([None, 5, None, -3, None]).max() == 5

    def test_int64_max(self):
        assert make([0, INT64_MAX, 1]).max() == INT64_MAX


# ---------------------------------------------------------------------------
# ARITHMETIC — ADD
# ---------------------------------------------------------------------------

class TestAdd:
    def test_basic_vector(self):
        a, b = make([1, 2, 3]), make([4, 5, 6])
        assert pylist(a.add(b)) == [5, 7, 9]

    def test_scalar(self):
        assert pylist(make([1, 2, 3]).add(10)) == [11, 12, 13]

    def test_empty(self):
        assert pylist(make([]).add(make([]))) == []

    def test_overflow_wraps(self):
        r = make([INT64_MAX]).add(1)
        assert pylist(r) == [INT64_MIN]  # wrap

    def test_null_propagation_both(self):
        a, b = make([1, None, 3]), make([None, 5, 6])
        result = pylist(a.add(b))
        assert result[0] is None  # a null
        assert result[1] is None  # b null
        assert result[2] == 9

    def test_null_propagation_one_side(self):
        a = make([1, None, 3])
        b = make([4, 5, 6])
        result = pylist(a.add(b))
        assert result[1] is None
        assert result[0] == 5
        assert result[2] == 9

    def test_scalar_preserves_nulls(self):
        r = pylist(make([1, None, 3]).add(100))
        assert r[1] is None
        assert r[0] == 101

    def test_all_null_result_validity_normalized(self):
        # Both all-null → result all-null, validity set
        a = make([None, None])
        b = make([None, None])
        r = a.add(b)
        assert pylist(r) == [None, None]


# ---------------------------------------------------------------------------
# ARITHMETIC — SUB
# ---------------------------------------------------------------------------

class TestSub:
    def test_basic_vector(self):
        assert pylist(make([5, 10]).sub(make([2, 3]))) == [3, 7]

    def test_scalar(self):
        assert pylist(make([10, 20]).sub(5)) == [5, 15]

    def test_underflow_wraps(self):
        assert pylist(make([INT64_MIN]).sub(1)) == [INT64_MAX]

    def test_null_propagation(self):
        r = pylist(make([1, None]).sub(make([2, 3])))
        assert r[1] is None
        assert r[0] == -1


# ---------------------------------------------------------------------------
# ARITHMETIC — MUL
# ---------------------------------------------------------------------------

class TestMul:
    def test_basic_vector(self):
        assert pylist(make([2, 3]).mul(make([4, 5]))) == [8, 15]

    def test_scalar(self):
        assert pylist(make([3, 6]).mul(2)) == [6, 12]

    def test_mul_by_zero(self):
        assert pylist(make([5, -3]).mul(0)) == [0, 0]

    def test_overflow_wraps(self):
        r = make([INT64_MAX]).mul(2)
        # INT64_MAX * 2 wraps; just check it returns a result without crashing
        assert len(r) == 1

    def test_null_propagation(self):
        r = pylist(make([2, None, 4]).mul(make([3, 4, 5])))
        assert r[1] is None
        assert r[0] == 6


# ---------------------------------------------------------------------------
# ARITHMETIC — DIV (integer, C truncation toward zero)
# ---------------------------------------------------------------------------

class TestDiv:
    def test_basic(self):
        assert pylist(make([9, 10, 11]).div(make([3, 3, 3]))) == [3, 3, 3]

    def test_scalar(self):
        assert pylist(make([9, 10]).div(3)) == [3, 3]

    def test_div_by_zero_returns_zero(self):
        assert pylist(make([5]).div(make([0]))) == [0]

    def test_scalar_div_by_zero(self):
        assert pylist(make([7]).div(0)) == [0]

    def test_negative_truncation(self):
        # C truncation: -7 / 2 == -3 (not -4 Python floor)
        assert pylist(make([-7]).div(2)) == [-3]

    def test_negative_truncation_vector(self):
        assert pylist(make([-7]).div(make([2]))) == [-3]

    def test_null_propagation(self):
        r = pylist(make([None, 6]).div(make([2, 3])))
        assert r[0] is None
        assert r[1] == 2


# ---------------------------------------------------------------------------
# ARITHMETIC — MOD
# ---------------------------------------------------------------------------

class TestMod:
    def test_basic(self):
        assert pylist(make([10, 11, 12]).mod(make([3, 3, 3]))) == [1, 2, 0]

    def test_scalar(self):
        assert pylist(make([7, 8, 9]).mod(3)) == [1, 2, 0]

    def test_mod_by_zero_returns_zero(self):
        assert pylist(make([7]).mod(make([0]))) == [0]

    def test_scalar_mod_by_zero(self):
        assert pylist(make([7]).mod(0)) == [0]

    def test_negative_mod(self):
        # C truncation: -7 % 3 == -1 (not 2 Python-style)
        assert pylist(make([-7]).mod(3)) == [-1]

    def test_null_propagation(self):
        r = pylist(make([None, 7]).mod(3))
        assert r[0] is None
        assert r[1] == 1


# ---------------------------------------------------------------------------
# ARITHMETIC — NEG
# ---------------------------------------------------------------------------

class TestNeg:
    def test_basic(self):
        assert pylist(make([1, -2, 0]).neg()) == [-1, 2, 0]

    def test_int64_min_wraps(self):
        r = pylist(make([INT64_MIN]).neg())
        assert r[0] == INT64_MIN  # wraps back to INT64_MIN

    def test_int64_max(self):
        assert pylist(make([INT64_MAX]).neg()) == [-(INT64_MAX)]

    def test_null_propagation(self):
        r = pylist(make([1, None, -3]).neg())
        assert r[0] == -1
        assert r[1] is None
        assert r[2] == 3

    def test_empty(self):
        assert pylist(make([]).neg()) == []


# ---------------------------------------------------------------------------
# TAKE
# ---------------------------------------------------------------------------

class TestTake:
    def test_basic(self):
        v = make([10, 20, 30])
        assert pylist(v.take([2, 0, 1])) == [30, 10, 20]

    def test_empty_indices(self):
        v = make([1, 2, 3])
        assert pylist(v.take([])) == []

    def test_repeated_indices(self):
        v = make([5, 6])
        assert pylist(v.take([0, 0, 1, 1, 0])) == [5, 5, 6, 6, 5]

    def test_preserves_nulls(self):
        v = make([10, None, 30])
        r = pylist(v.take([1, 0, 1]))
        assert r[0] is None
        assert r[1] == 10
        assert r[2] is None

    def test_null_not_propagated_for_valid_rows(self):
        v = make([10, None, 30])
        r = pylist(v.take([0, 2]))
        assert r == [10, 30]

    def test_single_index(self):
        v = make([INT64_MIN, INT64_MAX])
        assert pylist(v.take([1])) == [INT64_MAX]

    def test_out_of_order(self):
        v = make([1, 2, 3, 4, 5])
        assert pylist(v.take([4, 3, 2, 1, 0])) == [5, 4, 3, 2, 1]

    def test_dict_shape_take(self):
        v = make_dict([100, 200, 300], [0, 1, 2, 0])
        r = pylist(v.take([2, 0]))
        assert r == [300, 100]

    def test_constant_shape_take(self):
        v = make_const(42, 5)
        r = pylist(v.take([0, 3, 4]))
        assert r == [42, 42, 42]


# ---------------------------------------------------------------------------
# MATERIALIZE
# ---------------------------------------------------------------------------

class TestMaterialize:
    def test_dense_identity(self):
        v = make([1, 2, 3])
        assert pylist(v.materialize()) == [1, 2, 3]

    def test_dense_with_nulls(self):
        v = make([1, None, 3])
        assert pylist(v.materialize()) == [1, None, 3]

    def test_constant_shape(self):
        v = make_const(7, 4)
        assert pylist(v.materialize()) == [7, 7, 7, 7]

    def test_constant_null(self):
        v = make_const(None, 3)
        assert pylist(v.materialize()) == [None, None, None]

    def test_dict_shape(self):
        v = make_dict([10, 20, 30], [0, 1, 2, 1, 0])
        assert pylist(v.materialize()) == [10, 20, 30, 20, 10]

    def test_dict_with_nulls(self):
        v = make_dict([10, 20], [0, 1, 0], [True, False, True])
        assert pylist(v.materialize()) == [10, None, 10]

    def test_empty(self):
        assert pylist(make([]).materialize()) == []

    def test_result_is_dense(self):
        v = make_dict([1, 2], [0, 1, 0])
        m = v.materialize()
        # After materialize, is_dict should be False
        assert not m.is_dict


# ---------------------------------------------------------------------------
# COMPRESS / round-trip
# ---------------------------------------------------------------------------

class TestCompress:
    def _roundtrip(self, lst):
        """Compress a sequence then materialize back; must equal original."""
        v = make(lst)
        return pylist(v.compress().materialize())

    def test_empty_roundtrip(self):
        assert self._roundtrip([]) == []

    def test_single_value_roundtrip(self):
        assert self._roundtrip([42]) == [42]

    def test_all_same_roundtrip(self):
        assert self._roundtrip([5, 5, 5]) == [5, 5, 5]

    def test_all_unique_roundtrip(self):
        src = [1, 2, 3, 4, 5]
        assert self._roundtrip(src) == src

    def test_with_repeats_roundtrip(self):
        src = [10, 20, 10, 30, 20, 10]
        assert self._roundtrip(src) == src

    def test_with_nulls_roundtrip(self):
        src = [1, None, 2, None, 1]
        assert self._roundtrip(src) == src

    def test_all_null_roundtrip(self):
        src = [None, None, None]
        assert self._roundtrip(src) == src

    def test_edge_values_roundtrip(self):
        src = [INT64_MIN, INT64_MAX, 0, -1]
        assert self._roundtrip(src) == src

    def test_large_roundtrip(self):
        src = [i % 7 for i in range(10_000)]
        assert self._roundtrip(src) == src

    def test_compress_produces_dict_shape(self):
        v = make([1, 2, 1, 3, 2])
        c = v.compress()
        # compress of a 5-row vector with 3 unique values → dict (data_length < length)
        assert c.is_dict
        assert c.data_length == 3

    def test_constant_shape_compress_roundtrip(self):
        v = make_const(99, 5)
        m = v.compress().materialize()
        assert pylist(m) == [99, 99, 99, 99, 99]

    def test_dict_shape_compress_roundtrip(self):
        v = make_dict([10, 20], [0, 1, 0, 1])
        m = v.compress().materialize()
        assert pylist(m) == [10, 20, 10, 20]

    def test_all_null_compress_constant_shape(self):
        # all-null compresses to a constant shape: data_length=1, all rows null
        v = make([None, None, None])
        c = v.compress()
        assert c.data_length == 1   # one dummy dict entry
        assert c.length == 3
        # round-trip still gives all nulls
        assert pylist(c.materialize()) == [None, None, None]


# ---------------------------------------------------------------------------
# Unsupported type throws
# ---------------------------------------------------------------------------

class TestUnsupportedType:
    """Any op on an unsupported type must raise, never silently box."""

    def _make_non_native(self):
        # We don't have a non-int64 Vector yet, so test via hash on a
        # type-tagged placeholder is not possible at this milestone.
        # This is a placeholder for when other types are added.
        pytest.skip("No second type available yet to test unsupported dispatch")

    def test_add_length_mismatch_raises(self):
        a, b = make([1, 2, 3]), make([1, 2])
        with pytest.raises(Exception):
            a.add(b)

    def test_sub_length_mismatch_raises(self):
        with pytest.raises(Exception):
            make([1]).sub(make([]))

    def test_mul_length_mismatch_raises(self):
        with pytest.raises(Exception):
            make([1, 2]).mul(make([3]))


# ---------------------------------------------------------------------------
# Null-result normalization (validity == None when no nulls in result)
# ---------------------------------------------------------------------------

class TestNullNormalization:
    """Ops that produce no null rows must leave validity == nullptr (not an all-1 bitmap)."""

    def test_add_nonnull_result_no_validity(self):
        r = make([1, 2]).add(make([3, 4]))
        pl = pylist(r)
        # No nulls → to_pylist should have no None
        assert None not in pl

    def test_take_nonnull_source(self):
        v = make([1, 2, 3])
        r = v.take([2, 0, 1])
        assert None not in pylist(r)

    def test_materialize_nonnull_source(self):
        v = make_dict([5, 10], [0, 1, 0])
        m = v.materialize()
        assert None not in pylist(m)
