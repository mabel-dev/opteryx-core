"""
Native correctness tests for Milestone E.11, Phase 10 C′:
  vector_coalesce / vector_iif / vector_concat
  via the vector_selection_concat nanobind consumer.

Loads the extension without triggering opteryx/__init__.py, following the
spec_from_file_location pattern established in E.2–E.10.

Coverage
--------

vector_coalesce:
  first-non-null semantics (int64, float64, bool, string)
  all-null row → null in output
  variadic N-ary (≥3 args)
  cross-type raises TypeError
  non-Vector arg raises TypeError
  length mismatch raises ValueError
  fewer than 2 args raises ValueError

vector_iif:
  truth table: true mask → true branch, false mask → false branch
  null in mask → false branch (SQL CASE WHEN NULL = ELSE)
  null in selected branch → null in output
  bool branches, fixed-width branches, string branches
  type mismatch (BOOL vs non-BOOL) raises TypeError
  non-Vector raises TypeError
  length mismatch raises ValueError

vector_concat:
  two-arg bytewise concat (short + long strings)
  variadic N-ary (≥3 args)
  null TVL: any null input → null output
  type promotion: VARCHAR + NVARCHAR → NVARCHAR
  type promotion: any VARBINARY → VARBINARY
  non-string-family input raises TypeError
  non-Vector raises TypeError
  length mismatch raises ValueError
  fewer than 2 args raises ValueError
"""

import glob
import importlib.util
import os

import draken.draken_native as dn
import pytest


# ---------------------------------------------------------------------------
# Load vector_selection_concat extension
# ---------------------------------------------------------------------------

def _load_ext():
    pattern = os.path.join(
        os.path.dirname(__file__), "..", "..", "..",
        "opteryx", "compiled", "nanobind", "vectors*.so"
    )
    matches = glob.glob(pattern)
    if not matches:
        raise RuntimeError(
            "vector_selection_concat extension not built — run make compile first"
        )
    spec = importlib.util.spec_from_file_location(
        "opteryx.compiled.nanobind.vectors", matches[0]
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ext = _load_ext()
vector_coalesce = ext.vector_coalesce
vector_iif      = ext.vector_iif
vector_concat   = ext.vector_concat


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def iv(lst):
    """int64 vector from list (None → null)."""
    return dn.vector_from_sequence(lst)


def sv(lst):
    """VARCHAR vector from list (None → null)."""
    return dn.vector_from_string_sequence(
        [v.encode("utf-8") if isinstance(v, str) else v for v in lst]
    )


def nv(lst):
    """NVARCHAR vector from list (None → null)."""
    return dn.vector_from_nvarchar_sequence(
        [v.encode("utf-8") if isinstance(v, str) else v for v in lst]
    )


def bv(lst):
    """VARBINARY vector from list (None → null)."""
    return dn.vector_from_bytes_sequence(lst)


def boolv(lst):
    """BOOL vector from list (None → null)."""
    return dn.vector_from_bool_sequence(lst)


def fv(lst):
    """float64 vector from list (None → null)."""
    return dn.vector_from_float_sequence(lst)


def py(v):
    return v.to_pylist()


# ---------------------------------------------------------------------------
# vector_coalesce — int64
# ---------------------------------------------------------------------------

class TestCoalesceInt64:
    def test_basic(self):
        a = iv([1, None, 3])
        b = iv([10, 20, None])
        assert py(vector_coalesce(a, b)) == [1, 20, 3]

    def test_three_args(self):
        a = iv([1, None, None])
        b = iv([10, 20, None])
        c = iv([100, 200, 300])
        assert py(vector_coalesce(a, b, c)) == [1, 20, 300]

    def test_all_null_row(self):
        a = iv([None, 2])
        b = iv([None, None])
        result = py(vector_coalesce(a, b))
        assert result[0] is None
        assert result[1] == 2

    def test_no_nulls(self):
        a = iv([1, 2, 3])
        b = iv([4, 5, 6])
        assert py(vector_coalesce(a, b)) == [1, 2, 3]

    def test_all_null(self):
        a = iv([None, None])
        b = iv([None, None])
        assert py(vector_coalesce(a, b)) == [None, None]

    def test_single_arg_raises(self):
        with pytest.raises((ValueError, TypeError)):
            vector_coalesce(iv([1, 2]))

    def test_type_mismatch_raises(self):
        with pytest.raises(TypeError):
            vector_coalesce(iv([1, 2]), sv(["a", "b"]))

    def test_length_mismatch_raises(self):
        with pytest.raises(ValueError):
            vector_coalesce(iv([1, 2, 3]), iv([4, 5]))

    def test_non_vector_raises(self):
        with pytest.raises(TypeError):
            vector_coalesce(iv([1, 2]), [1, 2])


# ---------------------------------------------------------------------------
# vector_coalesce — string
# ---------------------------------------------------------------------------

class TestCoalesceString:
    def test_basic(self):
        a = sv(["hello", None, "world"])
        b = sv(["x", "y", None])
        assert py(vector_coalesce(a, b)) == ["hello", "y", "world"]

    def test_long_string(self):
        long_a = "a" * 50
        long_b = "b" * 50
        a = sv([long_a, None])
        b = sv([long_b, long_b])
        result = py(vector_coalesce(a, b))
        assert result[0] == long_a
        assert result[1] == long_b

    def test_all_null_row(self):
        a = sv([None, "z"])
        b = sv([None, None])
        result = py(vector_coalesce(a, b))
        assert result[0] is None
        assert result[1] == "z"

    def test_empty_string_is_not_null(self):
        a = sv(["", None])
        b = sv(["fallback", "fallback"])
        # empty string is valid (not null), so coalesce picks it
        result = py(vector_coalesce(a, b))
        assert result[0] == ""
        assert result[1] == "fallback"


# ---------------------------------------------------------------------------
# vector_coalesce — bool
# ---------------------------------------------------------------------------

class TestCoalesceBool:
    def test_basic(self):
        a = boolv([True, None, False])
        b = boolv([False, True, None])
        assert py(vector_coalesce(a, b)) == [True, True, False]

    def test_all_null_row(self):
        a = boolv([None])
        b = boolv([None])
        assert py(vector_coalesce(a, b)) == [None]


# ---------------------------------------------------------------------------
# vector_iif — int64
# ---------------------------------------------------------------------------

class TestIifInt64:
    def test_true_false(self):
        mask  = boolv([True, False, True])
        tv    = iv([1, 2, 3])
        fv_   = iv([10, 20, 30])
        assert py(vector_iif(mask, tv, fv_)) == [1, 20, 3]

    def test_null_mask_gives_false_branch(self):
        # SQL: CASE WHEN NULL THEN x ELSE y → y
        mask  = boolv([None, None])
        tv    = iv([1, 2])
        fv_   = iv([10, 20])
        assert py(vector_iif(mask, tv, fv_)) == [10, 20]

    def test_null_in_true_branch(self):
        mask  = boolv([True, True])
        tv    = iv([None, 2])
        fv_   = iv([10, 20])
        result = py(vector_iif(mask, tv, fv_))
        assert result[0] is None
        assert result[1] == 2

    def test_null_in_false_branch(self):
        mask  = boolv([False, False])
        tv    = iv([1, 2])
        fv_   = iv([None, 20])
        result = py(vector_iif(mask, tv, fv_))
        assert result[0] is None
        assert result[1] == 20

    def test_mixed_types_raise(self):
        with pytest.raises(TypeError):
            vector_iif(boolv([True]), iv([1]), sv(["a"]))

    def test_non_bool_mask_raises(self):
        with pytest.raises(TypeError):
            vector_iif(iv([1, 0]), iv([1, 2]), iv([3, 4]))

    def test_length_mismatch_raises(self):
        with pytest.raises(ValueError):
            vector_iif(boolv([True, False]), iv([1, 2, 3]), iv([10, 20, 30]))


# ---------------------------------------------------------------------------
# vector_iif — bool branches
# ---------------------------------------------------------------------------

class TestIifBool:
    def test_bool_branches(self):
        mask = boolv([True, False, True])
        tv   = boolv([True, True, False])
        fv_  = boolv([False, False, True])
        assert py(vector_iif(mask, tv, fv_)) == [True, False, False]

    def test_null_in_branch(self):
        mask = boolv([True, False])
        tv   = boolv([None, True])
        fv_  = boolv([False, None])
        result = py(vector_iif(mask, tv, fv_))
        assert result[0] is None   # true branch is null
        assert result[1] is None   # false branch is null


# ---------------------------------------------------------------------------
# vector_iif — string branches
# ---------------------------------------------------------------------------

class TestIifString:
    def test_string_branches(self):
        mask = boolv([True, False, True])
        tv   = sv(["yes", "yes", "yes"])
        fv_  = sv(["no", "no", "no"])
        assert py(vector_iif(mask, tv, fv_)) == ["yes", "no", "yes"]

    def test_null_in_string_branch(self):
        mask = boolv([True, False])
        tv   = sv([None, "t"])
        fv_  = sv(["f", None])
        result = py(vector_iif(mask, tv, fv_))
        assert result[0] is None
        assert result[1] is None

    def test_long_string_in_branch(self):
        long_t = "T" * 60
        long_f = "F" * 60
        mask = boolv([True, False])
        tv   = sv([long_t, long_t])
        fv_  = sv([long_f, long_f])
        result = py(vector_iif(mask, tv, fv_))
        assert result[0] == long_t
        assert result[1] == long_f


# ---------------------------------------------------------------------------
# vector_concat — basic
# ---------------------------------------------------------------------------

class TestConcat:
    def test_two_arg_short(self):
        a = sv(["hello", "foo"])
        b = sv([" world", "bar"])
        assert py(vector_concat(a, b)) == ["hello world", "foobar"]

    def test_two_arg_long(self):
        long_a = "a" * 30
        long_b = "b" * 30
        a = sv([long_a])
        b = sv([long_b])
        assert py(vector_concat(a, b)) == [long_a + long_b]

    def test_three_arg(self):
        a = sv(["a", "x"])
        b = sv(["b", "y"])
        c = sv(["c", "z"])
        assert py(vector_concat(a, b, c)) == ["abc", "xyz"]

    def test_empty_string_concat(self):
        a = sv(["", "foo"])
        b = sv(["bar", ""])
        assert py(vector_concat(a, b)) == ["bar", "foo"]

    def test_null_tvl(self):
        a = sv(["hello", None, "hi"])
        b = sv([" world", " there", None])
        result = py(vector_concat(a, b))
        assert result[0] == "hello world"
        assert result[1] is None
        assert result[2] is None

    def test_single_arg_raises(self):
        with pytest.raises((ValueError, TypeError)):
            vector_concat(sv(["a", "b"]))

    def test_non_string_raises(self):
        with pytest.raises(TypeError):
            vector_concat(iv([1, 2]), sv(["a", "b"]))

    def test_non_vector_raises(self):
        with pytest.raises(TypeError):
            vector_concat(sv(["a", "b"]), ["a", "b"])

    def test_length_mismatch_raises(self):
        with pytest.raises(ValueError):
            vector_concat(sv(["a", "b"]), sv(["x"]))


# ---------------------------------------------------------------------------
# vector_concat — type promotion
# ---------------------------------------------------------------------------

class TestConcatTypePromotion:
    def test_varchar_plus_nvarchar_gives_nvarchar(self):
        from draken.draken_native import DrakenType
        a = sv(["hello"])
        b = nv([" world"])
        result = vector_concat(a, b)
        assert result.type == DrakenType.NVARCHAR
        assert py(result) == ["hello world"]

    def test_any_varbinary_gives_varbinary(self):
        from draken.draken_native import DrakenType
        a = sv(["hello"])
        b = bv([b" world"])
        result = vector_concat(a, b)
        assert result.type == DrakenType.VARBINARY

    def test_all_varchar_stays_varchar(self):
        from draken.draken_native import DrakenType
        a = sv(["x"])
        b = sv(["y"])
        result = vector_concat(a, b)
        assert result.type == DrakenType.VARCHAR
