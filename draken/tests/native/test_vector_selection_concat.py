"""
Native correctness tests for Milestone E.11, Phase 10 C′:
  vector_concat
  via the vector_selection_concat nanobind consumer.

The vector_coalesce / vector_iif tests that lived here are gone with those
bindings: COALESCE/IIF/IFNULL/IFNOTNULL are now C-ABI kernels
(draken/ops/kernels/function_null_conditional.cpp) and are covered through the
engine instead.

Coverage
--------

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


import draken.draken_native as dn
import pytest


from opteryx.compiled.nanobind import vectors as ext
vector_concat   = ext.vector_concat


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def iv(lst):
    """int64 vector from list (None → null)."""
    return dn.vector_int64_from_sequence(lst)


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
