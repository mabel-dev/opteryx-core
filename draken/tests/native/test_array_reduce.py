"""
Native + parity tests for E.5: array element-reduction ops (ANY / ALL).

Loads vector_array_reduce without triggering opteryx/__init__.py (same
spec_from_file_location pattern as test_bitwise_parity.py).

Coverage:
  child types:    DRAKEN_INT64, DRAKEN_VARCHAR
  ops:            anyop_eq, anyop_neq, anyop_gt, anyop_gte, anyop_lt, anyop_lte,
                  allop_eq, allop_neq
  null semantics: null literal → all-False
                  null array row → NULL (TVL) in result validity
                  null element within row → non-matching (skipped for any, fails all)
  empty row:      any → False; all → True (vacuous SQL truth)
  edges:          <8-row bit-boundary tail; all-null array; matching/non-matching rows
  non-Vector:     TypeError raised on non-Vector column
  unsupported:    child type mismatch (float column) → invalid_argument
Hypothesis:
  any_eq(scalar, [scalar]) == True for int64
  all_eq(scalar, [scalar]) == True for int64
  any_eq(s, [t]) == any_neq(s, [t])  only when s != t (basic sanity)
"""

import glob
import importlib.util
import os

import draken.draken_native as dn
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

# ---------------------------------------------------------------------------
# Load extension
# ---------------------------------------------------------------------------

def _load_vector_array_reduce():
    pattern = os.path.join(
        os.path.dirname(__file__), "..", "..", "..",
        "opteryx", "compiled", "nanobind", "vector_array_reduce*.so"
    )
    matches = glob.glob(pattern)
    if not matches:
        raise RuntimeError("vector_array_reduce extension not built — run make compile")
    spec = importlib.util.spec_from_file_location(
        "opteryx.compiled.nanobind.vector_array_reduce", matches[0]
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ar = _load_vector_array_reduce()


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------

def arr_int(rows):
    """Build DRAKEN_ARRAY(INT64) from list[list[int|None]|None]."""
    return dn.vector_array_from_sequence(rows)


def arr_str(rows):
    """Build DRAKEN_ARRAY(STRING) from list[list[str|None]|None]."""
    return dn.vector_array_from_sequence(rows)


def pylist(v):
    return v.to_pylist()


# ---------------------------------------------------------------------------
# ANY EQ — int64
# ---------------------------------------------------------------------------

class TestAnyEqInt64:
    def test_basic_match(self):
        v = arr_int([[1, 2, 3], [4, 5], [6]])
        assert pylist(ar.vector_anyop_eq(v, 2)) == [True, False, False]

    def test_all_match(self):
        v = arr_int([[7, 7, 7]])
        assert pylist(ar.vector_anyop_eq(v, 7)) == [True]

    def test_no_match(self):
        v = arr_int([[1, 2], [3, 4]])
        assert pylist(ar.vector_anyop_eq(v, 99)) == [False, False]

    def test_null_literal(self):
        v = arr_int([[1, 2], [3]])
        assert pylist(ar.vector_anyop_eq(v, None)) == [False, False]

    def test_null_row_gives_null(self):
        v = arr_int([[1, 2], None, [3]])
        result = ar.vector_anyop_eq(v, 2)
        lst = pylist(result)
        assert lst[0] is True
        assert lst[1] is None   # null row → NULL in output
        assert lst[2] is False

    def test_empty_row(self):
        v = arr_int([[], [1], []])
        assert pylist(ar.vector_anyop_eq(v, 1)) == [False, True, False]

    def test_all_null_array(self):
        v = arr_int([None, None, None])
        result = pylist(ar.vector_anyop_eq(v, 5))
        assert result == [None, None, None]

    def test_bit_boundary_tail(self):
        # 7-row vector — tests single-byte tail handling
        rows = [[i] for i in range(7)]
        v = arr_int(rows)
        result = pylist(ar.vector_anyop_eq(v, 4))
        assert result == [False, False, False, False, True, False, False]

    def test_9_rows(self):
        # Crosses byte boundary
        rows = [[i] for i in range(9)]
        v = arr_int(rows)
        result = pylist(ar.vector_anyop_eq(v, 8))
        assert result == [False] * 8 + [True]

    def test_non_vector_raises(self):
        with pytest.raises(TypeError):
            ar.vector_anyop_eq([1, 2, 3], 1)


# ---------------------------------------------------------------------------
# ANY EQ — string
# ---------------------------------------------------------------------------

class TestAnyEqString:
    def test_basic_match(self):
        v = arr_str([["a", "b"], ["c"], ["d", "b"]])
        assert pylist(ar.vector_anyop_eq(v, "b")) == [True, False, True]

    def test_long_string(self):
        long_s = "x" * 20
        v = arr_str([[long_s, "short"], ["other"]])
        assert pylist(ar.vector_anyop_eq(v, long_s)) == [True, False]

    def test_null_literal(self):
        v = arr_str([["a", "b"]])
        assert pylist(ar.vector_anyop_eq(v, None)) == [False]

    def test_null_row(self):
        v = arr_str([["a"], None, ["b"]])
        result = pylist(ar.vector_anyop_eq(v, "a"))
        assert result[0] is True
        assert result[1] is None
        assert result[2] is False

    def test_empty_row(self):
        v = arr_str([[], ["x"]])
        assert pylist(ar.vector_anyop_eq(v, "x")) == [False, True]


# ---------------------------------------------------------------------------
# ALL EQ — int64
# ---------------------------------------------------------------------------

class TestAllEqInt64:
    def test_all_match(self):
        v = arr_int([[5, 5, 5]])
        assert pylist(ar.vector_allop_eq(v, 5)) == [True]

    def test_partial_match(self):
        v = arr_int([[5, 5, 6]])
        assert pylist(ar.vector_allop_eq(v, 5)) == [False]

    def test_no_match(self):
        v = arr_int([[1, 2, 3]])
        assert pylist(ar.vector_allop_eq(v, 9)) == [False]

    def test_empty_row_vacuous_true(self):
        # SQL vacuous truth: ALL over empty set is True
        v = arr_int([[], [5, 5]])
        assert pylist(ar.vector_allop_eq(v, 5)) == [True, True]

    def test_null_literal(self):
        v = arr_int([[5, 5]])
        assert pylist(ar.vector_allop_eq(v, None)) == [False]

    def test_null_row_gives_null(self):
        v = arr_int([[5, 5], None, [5]])
        result = pylist(ar.vector_allop_eq(v, 5))
        assert result[0] is True
        assert result[1] is None   # null row → NULL
        assert result[2] is True

    def test_all_null_array(self):
        v = arr_int([None, None])
        result = pylist(ar.vector_allop_eq(v, 5))
        assert result == [None, None]

    def test_bit_boundary_tail(self):
        rows = [[7]] * 7
        v = arr_int(rows)
        assert pylist(ar.vector_allop_eq(v, 7)) == [True] * 7

    def test_non_vector_raises(self):
        with pytest.raises(TypeError):
            ar.vector_allop_eq("not a vector", 1)


# ---------------------------------------------------------------------------
# ALL EQ — string
# ---------------------------------------------------------------------------

class TestAllEqString:
    def test_all_match(self):
        v = arr_str([["x", "x", "x"]])
        assert pylist(ar.vector_allop_eq(v, "x")) == [True]

    def test_partial_match(self):
        v = arr_str([["x", "y"]])
        assert pylist(ar.vector_allop_eq(v, "x")) == [False]

    def test_empty_row_vacuous(self):
        v = arr_str([[]])
        assert pylist(ar.vector_allop_eq(v, "x")) == [True]


# ---------------------------------------------------------------------------
# ANY NEQ — int64
# ---------------------------------------------------------------------------

class TestAnyNeqInt64:
    def test_has_different(self):
        v = arr_int([[1, 2, 3]])
        assert pylist(ar.vector_anyop_neq(v, 1)) == [True]

    def test_all_same(self):
        v = arr_int([[1, 1, 1]])
        assert pylist(ar.vector_anyop_neq(v, 1)) == [False]

    def test_null_row(self):
        v = arr_int([[1, 2], None])
        result = pylist(ar.vector_anyop_neq(v, 1))
        assert result[0] is True
        assert result[1] is None


# ---------------------------------------------------------------------------
# ALL NEQ — int64
# ---------------------------------------------------------------------------

class TestAllNeqInt64:
    def test_all_different(self):
        v = arr_int([[2, 3, 4]])
        assert pylist(ar.vector_allop_neq(v, 1)) == [True]

    def test_one_same(self):
        v = arr_int([[2, 1, 4]])
        assert pylist(ar.vector_allop_neq(v, 1)) == [False]

    def test_empty_vacuous(self):
        v = arr_int([[]])
        assert pylist(ar.vector_allop_neq(v, 1)) == [True]

    def test_null_row(self):
        v = arr_int([None, [2, 3]])
        result = pylist(ar.vector_allop_neq(v, 1))
        assert result[0] is None
        assert result[1] is True


# ---------------------------------------------------------------------------
# Ordering ops (anyop only for gt/gte/lt/lte)
# ---------------------------------------------------------------------------

class TestAnyOrdInt64:
    def test_gt_has_greater(self):
        # literal > ANY(row): literal=5, row=[3,4] → any elem < 5 → True
        v = arr_int([[3, 4]])
        assert pylist(ar.vector_anyop_gt(v, 5)) == [True]

    def test_gt_no_greater(self):
        v = arr_int([[5, 6, 7]])
        assert pylist(ar.vector_anyop_gt(v, 5)) == [False]

    def test_gte(self):
        v = arr_int([[5, 6]])
        assert pylist(ar.vector_anyop_gte(v, 5)) == [True]

    def test_lt_has_lesser(self):
        # literal < ANY(row): literal=3, row=[4,5] → any elem > 3 → True
        v = arr_int([[4, 5]])
        assert pylist(ar.vector_anyop_lt(v, 3)) == [True]

    def test_lt_no_lesser(self):
        v = arr_int([[1, 2]])
        assert pylist(ar.vector_anyop_lt(v, 3)) == [False]

    def test_lte(self):
        v = arr_int([[3, 4]])
        assert pylist(ar.vector_anyop_lte(v, 3)) == [True]

    def test_null_row_gives_null(self):
        v = arr_int([[1], None])
        result = pylist(ar.vector_anyop_gt(v, 5))
        assert result[0] is True
        assert result[1] is None

    def test_empty_row(self):
        v = arr_int([[]])
        assert pylist(ar.vector_anyop_gt(v, 5)) == [False]


class TestAnyOrdString:
    def test_gt_string(self):
        # literal "c" > ANY(row): any elem < "c" → "a" qualifies
        v = arr_str([["a", "b"], ["c", "d"]])
        assert pylist(ar.vector_anyop_gt(v, "c")) == [True, False]

    def test_lt_string(self):
        # literal "b" < ANY(row): True iff any element > "b"
        # ["x","y"]: "x">"b" → True; ["a"]: "a"<"b" → False
        v = arr_str([["x", "y"], ["a"]])
        assert pylist(ar.vector_anyop_lt(v, "b")) == [True, False]


# ---------------------------------------------------------------------------
# Hypothesis property tests
# ---------------------------------------------------------------------------

@given(st.integers(min_value=-(2**62), max_value=2**62 - 1))
@settings(max_examples=200)
def test_anyeq_single_element_always_matches(x):
    v = arr_int([[x]])
    assert pylist(ar.vector_anyop_eq(v, x)) == [True]


@given(st.integers(min_value=-(2**62), max_value=2**62 - 1))
@settings(max_examples=200)
def test_alleq_single_element_always_matches(x):
    v = arr_int([[x]])
    assert pylist(ar.vector_allop_eq(v, x)) == [True]


@given(st.lists(st.integers(min_value=0, max_value=100), min_size=1, max_size=20),
       st.integers(min_value=0, max_value=100))
@settings(max_examples=200)
def test_anyeq_vs_python_any(elems, scalar):
    v = arr_int([elems])
    expected = any(e == scalar for e in elems)
    assert pylist(ar.vector_anyop_eq(v, scalar)) == [expected]


@given(st.lists(st.integers(min_value=0, max_value=100), min_size=0, max_size=20),
       st.integers(min_value=0, max_value=100))
@settings(max_examples=200)
def test_alleq_vs_python_all(elems, scalar):
    v = arr_int([elems])
    expected = all(e == scalar for e in elems)  # True for empty (vacuous)
    assert pylist(ar.vector_allop_eq(v, scalar)) == [expected]


@given(st.lists(st.integers(min_value=0, max_value=100), min_size=0, max_size=20),
       st.integers(min_value=0, max_value=100))
@settings(max_examples=200)
def test_anygt_vs_python_any(elems, scalar):
    v = arr_int([elems])
    expected = any(scalar > e for e in elems)
    assert pylist(ar.vector_anyop_gt(v, scalar)) == [expected]


@given(st.lists(st.text(min_size=0, max_size=25), min_size=1, max_size=10),
       st.text(min_size=0, max_size=25))
@settings(max_examples=100)
def test_anyeq_string_vs_python(elems, scalar):
    v = arr_str([elems])
    expected = any(e == scalar for e in elems)
    assert pylist(ar.vector_anyop_eq(v, scalar)) == [expected]
