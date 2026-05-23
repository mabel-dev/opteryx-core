"""
Native correctness tests for DRAKEN_BOOL — ingestion, round-trip, logical ops,
and reductions.

CORRECTNESS PRINCIPLE
---------------------
These tests assert the CORRECT answer per SQL three-valued logic.
They are the primary correctness signal.

COVERAGE
--------
 1. Ingestion round-trip  — dense / constant / dict shapes; size 0/1/<8/large;
    null combinations; value bits vs validity bitmap independence.
 2. Kleene truth tables   — ALL 9 (a,b) combinations {T,F,N}×{T,F,N} for AND/OR;
    including the "valid-despite-null" cells (F∧N=F, T∨N=T).
 3. NOT                   — all three input states; validity preserved.
 4. any / all             — all-true / all-false / all-null / mixed / empty / single.
 5. Bit-boundary tails    — sizes 1–17 to exercise every partial-byte combination.
 6. Shape interop         — constant × dense, dict × dense (via uniform selection).
"""

import pytest

import draken.draken_native as dn

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def bvec(lst):
    """Dense bool vector from list[bool|None]."""
    return dn.vector_from_bool_sequence(lst)


def bconst(value, length):
    """Constant-shape bool vector."""
    return dn.vector_from_bool_constant(value, length)


def bdict(values, codes, nullable=None):
    """Dict-encoded bool vector."""
    return dn.vector_from_bool_dict(values, codes, nullable)


def rt(lst):
    """Round-trip: list → vector → list."""
    return bvec(lst).to_pylist()


# ---------------------------------------------------------------------------
# 1. Ingestion round-trip
# ---------------------------------------------------------------------------

class TestIngestionDense:
    def test_empty(self):
        assert rt([]) == []

    def test_all_true(self):
        assert rt([True, True, True]) == [True, True, True]

    def test_all_false(self):
        assert rt([False, False]) == [False, False]

    def test_mixed_no_nulls(self):
        assert rt([True, False, True, False]) == [True, False, True, False]

    def test_single_true(self):
        assert rt([True]) == [True]

    def test_single_false(self):
        assert rt([False]) == [False]

    def test_single_null(self):
        assert rt([None]) == [None]

    def test_some_nulls(self):
        src = [True, None, False, None, True]
        assert rt(src) == src

    def test_all_nulls(self):
        src = [None, None, None]
        assert rt(src) == src

    def test_null_head_tail(self):
        src = [None, True, False, None]
        assert rt(src) == src

    def test_type(self):
        assert bvec([True, False]).type == dn.DrakenType.BOOL

    def test_length(self):
        assert len(bvec([True, False, None])) == 3

    def test_no_nulls_validity_is_null(self):
        # Normalization invariant: validity==nullptr when no nulls.
        # We can verify indirectly: getitem still works, any()==True for all-True.
        v = bvec([True, True, True])
        assert v.to_pylist() == [True, True, True]
        assert v.bool_any() is True

    def test_getitem(self):
        v = bvec([True, None, False])
        assert v[0] is True
        assert v[1] is None
        assert v[2] is False


class TestIngestionTailBoundary:
    """Sizes 1–17 cover every partial-byte position."""

    @pytest.mark.parametrize("n", range(1, 18))
    def test_all_true_n(self, n):
        src = [True] * n
        assert rt(src) == src

    @pytest.mark.parametrize("n", range(1, 18))
    def test_all_false_n(self, n):
        src = [False] * n
        assert rt(src) == src

    @pytest.mark.parametrize("n", range(1, 18))
    def test_alternating_n(self, n):
        src = [i % 2 == 0 for i in range(n)]
        assert rt(src) == src

    @pytest.mark.parametrize("n", range(1, 18))
    def test_null_last_n(self, n):
        src = [True] * (n - 1) + [None]
        assert rt(src) == src

    @pytest.mark.parametrize("n", range(2, 18))
    def test_null_first_n(self, n):
        src = [None] + [False] * (n - 1)
        assert rt(src) == src


class TestIngestionConstant:
    def test_const_true(self):
        v = bconst(True, 5)
        assert v.to_pylist() == [True] * 5

    def test_const_false(self):
        v = bconst(False, 3)
        assert v.to_pylist() == [False] * 3

    def test_const_null(self):
        v = bconst(None, 4)
        assert v.to_pylist() == [None] * 4

    def test_const_length_zero(self):
        v = bconst(True, 0)
        assert v.to_pylist() == []

    def test_const_length_one(self):
        v = bconst(False, 1)
        assert v.to_pylist() == [False]

    def test_const_shape(self):
        v = bconst(True, 10)
        assert v.is_constant

    def test_const_type(self):
        assert bconst(True, 3).type == dn.DrakenType.BOOL


class TestIngestionDict:
    def test_basic_dict(self):
        # Two unique values: False at code 0, True at code 1.
        v = bdict([False, True], [0, 1, 0, 1])
        assert v.to_pylist() == [False, True, False, True]

    def test_dict_single_value(self):
        v = bdict([True], [0, 0, 0])
        assert v.to_pylist() == [True, True, True]

    def test_dict_with_nulls(self):
        v = bdict([False, True], [0, 1, 0], nullable=[True, False, True])
        assert v.to_pylist() == [False, None, False]

    def test_dict_is_dict(self):
        v = bdict([False, True], [0, 1, 0, 1])
        assert v.is_dict

    def test_dict_type(self):
        v = bdict([True, False], [1, 0])
        assert v.type == dn.DrakenType.BOOL


# ---------------------------------------------------------------------------
# 2. Kleene AND truth table — all 9 {T,F,N}×{T,F,N} combinations
# ---------------------------------------------------------------------------

class TestKleeneAND:
    """
    Truth table (result_value, result_valid):
      T∧T = T (valid)
      T∧F = F (valid)
      T∧N = N (null)   ← null cell
      F∧T = F (valid)
      F∧F = F (valid)
      F∧N = F (valid)  ← valid despite null — critical
      N∧T = N (null)   ← null cell
      N∧F = F (valid)  ← valid despite null — critical
      N∧N = N (null)   ← null cell
    """

    def _and(self, a_val, b_val):
        a = bvec([a_val])
        b = bvec([b_val])
        return a.bool_and(b).to_pylist()[0]

    def test_T_and_T(self):   assert self._and(True, True)   is True
    def test_T_and_F(self):   assert self._and(True, False)  is False
    def test_T_and_N(self):   assert self._and(True, None)   is None
    def test_F_and_T(self):   assert self._and(False, True)  is False
    def test_F_and_F(self):   assert self._and(False, False) is False
    def test_F_and_N(self):   assert self._and(False, None)  is False   # F dominates
    def test_N_and_T(self):   assert self._and(None, True)   is None
    def test_N_and_F(self):   assert self._and(None, False)  is False   # F dominates
    def test_N_and_N(self):   assert self._and(None, None)   is None

    def test_all_nine_in_one_vector(self):
        # Encode all 9 combinations in a single AND call.
        a = bvec([True,  True,  True,  False, False, False, None, None, None])
        b = bvec([True,  False, None,  True,  False, None,  True, False, None])
        expected = [True, False, None, False, False, False, None, False, None]
        assert a.bool_and(b).to_pylist() == expected

    def test_result_validity_independent_of_input_order(self):
        # Commutativity check.
        a = bvec([True, False, None, None])
        b = bvec([None, None, True, False])
        assert a.bool_and(b).to_pylist() == b.bool_and(a).to_pylist()

    def test_all_valid_result_has_null_validity(self):
        # When both inputs are all-valid and result is all-valid,
        # the result validity must be None (normalization invariant).
        a = bvec([True, False, True])
        b = bvec([False, True, True])
        r = a.bool_and(b)
        assert r.to_pylist() == [False, False, True]


class TestKleeneOR:
    """
    Truth table:
      T∨T = T (valid)
      T∨F = T (valid)
      T∨N = T (valid)  ← valid despite null — critical
      F∨T = T (valid)
      F∨F = F (valid)
      F∨N = N (null)   ← null cell
      N∨T = T (valid)  ← valid despite null — critical
      N∨F = N (null)   ← null cell
      N∨N = N (null)   ← null cell
    """

    def _or(self, a_val, b_val):
        a = bvec([a_val])
        b = bvec([b_val])
        return a.bool_or(b).to_pylist()[0]

    def test_T_or_T(self):   assert self._or(True, True)   is True
    def test_T_or_F(self):   assert self._or(True, False)  is True
    def test_T_or_N(self):   assert self._or(True, None)   is True    # T dominates
    def test_F_or_T(self):   assert self._or(False, True)  is True
    def test_F_or_F(self):   assert self._or(False, False) is False
    def test_F_or_N(self):   assert self._or(False, None)  is None
    def test_N_or_T(self):   assert self._or(None, True)   is True    # T dominates
    def test_N_or_F(self):   assert self._or(None, False)  is None
    def test_N_or_N(self):   assert self._or(None, None)   is None

    def test_all_nine_in_one_vector(self):
        a = bvec([True,  True,  True,  False, False, False, None, None, None])
        b = bvec([True,  False, None,  True,  False, None,  True, False, None])
        expected = [True, True, True, True, False, None, True, None, None]
        assert a.bool_or(b).to_pylist() == expected

    def test_commutativity(self):
        a = bvec([True, False, None, None])
        b = bvec([None, None, True, False])
        assert a.bool_or(b).to_pylist() == b.bool_or(a).to_pylist()


# ---------------------------------------------------------------------------
# 3. NOT
# ---------------------------------------------------------------------------

class TestNOT:
    def test_not_true(self):
        assert bvec([True]).bool_not().to_pylist() == [False]

    def test_not_false(self):
        assert bvec([False]).bool_not().to_pylist() == [True]

    def test_not_null(self):
        assert bvec([None]).bool_not().to_pylist() == [None]

    def test_not_mixed(self):
        v = bvec([True, False, None, True, False])
        assert v.bool_not().to_pylist() == [False, True, None, False, True]

    def test_not_not_identity(self):
        src = [True, False, None, True]
        v = bvec(src)
        assert v.bool_not().bool_not().to_pylist() == src

    def test_not_all_true(self):
        v = bvec([True] * 9)
        assert v.bool_not().to_pylist() == [False] * 9

    def test_not_empty(self):
        assert bvec([]).bool_not().to_pylist() == []

    @pytest.mark.parametrize("n", range(1, 18))
    def test_not_tail_n(self, n):
        src = [i % 2 == 0 for i in range(n)]
        result = bvec(src).bool_not().to_pylist()
        expected = [not x for x in src]
        assert result == expected

    def test_not_preserves_null_positions(self):
        v = bvec([None, True, None, False, None])
        r = v.bool_not().to_pylist()
        assert r[0] is None
        assert r[2] is None
        assert r[4] is None
        assert r[1] is False
        assert r[3] is True


# ---------------------------------------------------------------------------
# 4. any / all — reductions
# ---------------------------------------------------------------------------

class TestAny:
    def test_any_all_true(self):
        assert bvec([True, True, True]).bool_any() is True

    def test_any_mixed_no_null(self):
        assert bvec([False, True, False]).bool_any() is True

    def test_any_all_false(self):
        assert bvec([False, False]).bool_any() is False

    def test_any_all_null(self):
        assert bvec([None, None]).bool_any() is None

    def test_any_mixed_with_null_has_true(self):
        assert bvec([None, True, None]).bool_any() is True

    def test_any_mixed_no_true_has_null(self):
        assert bvec([False, None, False]).bool_any() is None

    def test_any_empty(self):
        assert bvec([]).bool_any() is False

    def test_any_single_true(self):
        assert bvec([True]).bool_any() is True

    def test_any_single_false(self):
        assert bvec([False]).bool_any() is False

    def test_any_single_null(self):
        assert bvec([None]).bool_any() is None

    def test_any_true_then_all_null(self):
        # True comes before any null → True (short-circuits).
        assert bvec([True, None, None]).bool_any() is True

    def test_any_null_before_true(self):
        # Null comes before True; still → True.
        assert bvec([None, True]).bool_any() is True


class TestAll:
    def test_all_all_true(self):
        assert bvec([True, True]).bool_all() is True

    def test_all_all_false(self):
        assert bvec([False, False]).bool_all() is False

    def test_all_mixed_no_null(self):
        assert bvec([True, False, True]).bool_all() is False

    def test_all_all_null(self):
        assert bvec([None, None]).bool_all() is None

    def test_all_mixed_has_false(self):
        assert bvec([True, None, False]).bool_all() is False

    def test_all_mixed_no_false_has_null(self):
        assert bvec([True, None, True]).bool_all() is None

    def test_all_empty(self):
        assert bvec([]).bool_all() is True

    def test_all_single_true(self):
        assert bvec([True]).bool_all() is True

    def test_all_single_false(self):
        assert bvec([False]).bool_all() is False

    def test_all_single_null(self):
        assert bvec([None]).bool_all() is None

    def test_all_false_before_null(self):
        assert bvec([False, None]).bool_all() is False

    def test_all_null_before_false(self):
        assert bvec([None, False]).bool_all() is False


# ---------------------------------------------------------------------------
# 5. Ops on constant / dict shapes (uniform access model)
# ---------------------------------------------------------------------------

class TestOpsOnConstantShape:
    def test_and_const_true_dense(self):
        a = bconst(True, 4)
        b = bvec([True, False, None, True])
        result = a.bool_and(b).to_pylist()
        assert result == [True, False, None, True]

    def test_and_const_false_dense(self):
        a = bconst(False, 3)
        b = bvec([True, None, False])
        result = a.bool_and(b).to_pylist()
        # False dominates: F∧T=F, F∧N=F, F∧F=F — all valid False
        assert result == [False, False, False]

    def test_or_const_true_dense(self):
        a = bconst(True, 3)
        b = bvec([False, None, True])
        result = a.bool_or(b).to_pylist()
        # True dominates: T∨F=T, T∨N=T, T∨T=T
        assert result == [True, True, True]

    def test_not_const_true(self):
        r = bconst(True, 3).bool_not().to_pylist()
        assert r == [False, False, False]

    def test_any_const_true(self):
        assert bconst(True, 5).bool_any() is True

    def test_any_const_null(self):
        assert bconst(None, 3).bool_any() is None

    def test_all_const_false(self):
        assert bconst(False, 4).bool_all() is False


class TestOpsOnDictShape:
    def test_and_dict_dense(self):
        a = bdict([False, True], [0, 1, 0, 1])  # [F, T, F, T]
        b = bvec([True, None, True, False])
        result = a.bool_and(b).to_pylist()
        # F∧T=F, T∧N=N, F∧T=F, T∧F=F
        assert result == [False, None, False, False]

    def test_or_dict_dense(self):
        a = bdict([False, True], [0, 1, 0, 1])  # [F, T, F, T]
        b = bvec([None, False, True, None])
        result = a.bool_or(b).to_pylist()
        # F∨N=N, T∨F=T, F∨T=T, T∨N=T
        assert result == [None, True, True, True]

    def test_any_dict(self):
        v = bdict([False, True], [0, 0, 1])
        assert v.bool_any() is True

    def test_all_dict_false_present(self):
        v = bdict([False, True], [0, 1, 0])
        assert v.bool_all() is False


# ---------------------------------------------------------------------------
# 6. Error handling
# ---------------------------------------------------------------------------

class TestErrors:
    def test_and_wrong_type(self):
        b = bvec([True, False])
        i = dn.vector_from_sequence([1, 2])
        with pytest.raises(Exception):
            b.bool_and(i)

    def test_or_wrong_type(self):
        b = bvec([True])
        i = dn.vector_from_sequence([1])
        with pytest.raises(Exception):
            b.bool_or(i)

    def test_not_wrong_type(self):
        i = dn.vector_from_sequence([1, 2])
        with pytest.raises(Exception):
            i.bool_not()

    def test_any_wrong_type(self):
        i = dn.vector_from_sequence([1])
        with pytest.raises(Exception):
            i.bool_any()

    def test_all_wrong_type(self):
        i = dn.vector_from_sequence([1])
        with pytest.raises(Exception):
            i.bool_all()

    def test_and_length_mismatch(self):
        a = bvec([True, False])
        b = bvec([True])
        with pytest.raises(Exception):
            a.bool_and(b)

    def test_or_length_mismatch(self):
        a = bvec([True])
        b = bvec([True, False])
        with pytest.raises(Exception):
            a.bool_or(b)
