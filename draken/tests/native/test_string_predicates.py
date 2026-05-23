"""
Native correctness tests for Milestone D.4: string in_list.

These tests assert the CORRECT answer.

Coverage matrix (per 04_testing.md §1 and D.4 acceptance criteria):

  in_list:
    set sizes:   empty / 1 / large / with duplicates
    membership:  value present / absent / all present / none present
    nullability: no-null / some-null / all-null
    sizes:       0 / 1 / 2..7 (tail-only) / 8 (byte boundary) / 9 (byte+1) / large
    string types: short (≤12 B, effectively exact) / long (>12 B, §1 fidelity)
    shapes:      dense / dict-encoded
    three-valued: null input → null output (validity=0, result bit=0)

§1 EXCEPTION (same as string eq, hash — not new):
  Long strings (>12 B) are identified by (length, prefix, hash32); the test
  scale makes collision probability negligible, so correct results are expected.
  Short strings: full content is in the hash input → exactly correct.
"""

import pytest
import draken.draken_native as dn

SHORT_ABSENT  = "hello"
SHORT_PRESENT = "world"
LONG_ABSENT   = "this is a very long string that exceeds twelve bytes - absent"
LONG_PRESENT  = "this is a very long string that exceeds twelve bytes - present"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make(lst):
    return dn.vector_from_string_sequence(lst)

def make_dict(lst):
    return dn.vector_from_string_dict_sequence(lst)

def pylist(v):
    return v.to_pylist()

def in_list(v, values):
    return pylist(v.in_list(values))

def _py_in_list(x, s):
    if x is None:
        return None
    return x in s


# ===========================================================================
# Result type and length
# ===========================================================================

class TestResultMeta:
    def test_result_type_is_bool(self):
        v = make(["a", "b", "c"])
        r = v.in_list(["a"])
        assert r.type == dn.DrakenType.BOOL

    def test_result_length_matches_input(self):
        v = make(["a", "b", "c", "d"])
        r = v.in_list(["a", "c"])
        assert len(r) == 4


# ===========================================================================
# Basic membership — short strings (≤12 B, exact)
# ===========================================================================

class TestShortStringMembership:
    def test_all_present(self):
        v = make(["a", "b", "c"])
        assert in_list(v, ["a", "b", "c"]) == [True, True, True]

    def test_none_present(self):
        v = make(["a", "b", "c"])
        assert in_list(v, ["x", "y", "z"]) == [False, False, False]

    def test_some_present(self):
        v = make(["a", "b", "c", "d", "e"])
        assert in_list(v, ["b", "d"]) == [False, True, False, True, False]

    def test_empty_set_all_false(self):
        v = make(["a", "b", "c"])
        assert in_list(v, []) == [False, False, False]

    def test_single_value_in_set(self):
        v = make(["x", "y", "z"])
        assert in_list(v, ["y"]) == [False, True, False]

    def test_duplicate_values_in_set(self):
        v = make(["a", "b", "c"])
        assert in_list(v, ["b", "b", "b"]) == [False, True, False]

    def test_empty_string_present(self):
        v = make(["", "a", "b"])
        assert in_list(v, [""]) == [True, False, False]

    def test_empty_string_absent(self):
        v = make(["a", "b"])
        assert in_list(v, ["a", "b"]) == [True, True]

    def test_exactly_twelve_bytes_present(self):
        s = "abcdefghijkl"  # exactly 12 bytes — inline boundary
        assert len(s) == 12
        v = make([s, "other"])
        assert in_list(v, [s]) == [True, False]

    def test_exactly_twelve_bytes_absent(self):
        s = "abcdefghijkl"
        v = make(["other", s])
        assert in_list(v, ["nope"]) == [False, False]


# ===========================================================================
# Long strings (>12 B, §1 fidelity — effectively exact at test scale)
# ===========================================================================

class TestLongStringMembership:
    def test_long_present(self):
        v = make([LONG_PRESENT, LONG_ABSENT])
        assert in_list(v, [LONG_PRESENT]) == [True, False]

    def test_long_absent(self):
        v = make([LONG_PRESENT, LONG_ABSENT])
        assert in_list(v, [LONG_ABSENT]) == [False, True]

    def test_long_both_in_set(self):
        v = make([LONG_PRESENT, LONG_ABSENT, "short"])
        assert in_list(v, [LONG_PRESENT, LONG_ABSENT]) == [True, True, False]

    def test_long_none_present(self):
        v = make([LONG_PRESENT, LONG_ABSENT])
        assert in_list(v, ["completely_different_long_value_x"]) == [False, False]

    def test_mixed_short_and_long(self):
        v = make(["short", LONG_PRESENT, "also_short", LONG_ABSENT])
        assert in_list(v, ["short", LONG_PRESENT]) == [True, True, False, False]

    def test_thirteen_bytes(self):
        # 13 bytes — just over the inline boundary
        s = "abcdefghijklm"
        assert len(s) == 13
        v = make([s, "other"])
        assert in_list(v, [s]) == [True, False]


# ===========================================================================
# Bit-boundary: sizes 1..9
# ===========================================================================

class TestBitBoundary:
    SHORT_VALS = ["a", "b", "c", "d", "e", "f", "g", "h", "i"]

    @pytest.mark.parametrize("n", range(1, 10))
    def test_sizes_1_to_9(self, n):
        data = self.SHORT_VALS[:n]
        v = make(data)
        s = set(data[::2])  # every other element
        result = in_list(v, list(s))
        expected = [_py_in_list(x, s) for x in data]
        assert result == expected, f"size={n}: {result} != {expected}"

    def test_exact_byte_boundary_8(self):
        data = list("abcdefgh")  # 8 single-char strings
        s = {"a", "d", "g"}
        v = make(data)
        assert in_list(v, list(s)) == [_py_in_list(x, s) for x in data]

    def test_one_past_byte_boundary_9(self):
        data = list("abcdefghi")
        s = {"b", "e", "h", "i"}
        v = make(data)
        assert in_list(v, list(s)) == [_py_in_list(x, s) for x in data]

    @pytest.mark.parametrize("n", range(1, 10))
    def test_all_absent_sizes_1_to_9(self, n):
        data = self.SHORT_VALS[:n]
        v = make(data)
        result = in_list(v, ["not_here"])
        assert result == [False] * n, f"size={n}"


# ===========================================================================
# Null semantics (TVL)
# ===========================================================================

class TestNullSemantics:
    """Null input → null output. Null does NOT match any set member."""

    def test_all_null(self):
        v = make([None, None, None])
        assert in_list(v, ["a", "b"]) == [None, None, None]

    def test_mixed_nulls(self):
        data = ["a", None, "c", None, "e"]
        v = make(data)
        result = in_list(v, ["a", "c"])
        expected = [_py_in_list(x, {"a", "c"}) for x in data]
        assert result == expected

    def test_null_not_false(self):
        v = make([None])
        result = in_list(v, ["anything"])
        assert result == [None]
        assert result[0] is None

    def test_null_with_empty_set(self):
        v = make([None, "a"])
        result = in_list(v, [])
        assert result == [None, False]

    def test_null_at_byte_boundary(self):
        data = ["a", "b", "c", "d", "e", "f", "g", None, None, "j"]
        v = make(data)
        s = {"a", "e", "j"}
        result = in_list(v, list(s))
        expected = [_py_in_list(x, s) for x in data]
        assert result == expected

    @pytest.mark.parametrize("n", range(1, 10))
    def test_all_null_sizes_1_to_9(self, n):
        v = make([None] * n)
        result = in_list(v, ["anything"])
        assert result == [None] * n, f"size={n}"

    def test_null_does_not_match(self):
        # Even if an empty string (null placeholder candidate) is in the set,
        # a null row must still be null.
        data = [None, "", "a"]
        v = make(data)
        result = in_list(v, [""])
        assert result == [None, True, False]


# ===========================================================================
# Empty vector
# ===========================================================================

class TestEmptyVector:
    def test_empty_vector_empty_set(self):
        v = make([])
        assert in_list(v, []) == []

    def test_empty_vector_nonempty_set(self):
        v = make([])
        assert in_list(v, ["a", "b"]) == []

    def test_empty_result_type(self):
        v = make([])
        r = v.in_list(["x"])
        assert r.type == dn.DrakenType.BOOL
        assert len(r) == 0


# ===========================================================================
# Dict-encoded shape
# ===========================================================================

class TestDictShape:
    def test_dict_some_present(self):
        # Repeated values force dict encoding: unique = [cat, dog, fish]
        # logical: ["cat", "dog", "fish", "cat", "dog"]
        v = make_dict(["cat", "dog", "fish", "cat", "dog"])
        assert v.is_dict
        assert in_list(v, ["cat", "fish"]) == [True, False, True, True, False]

    def test_dict_all_present(self):
        v = make_dict(["x", "y", "x", "y"])
        assert in_list(v, ["x", "y"]) == [True, True, True, True]

    def test_dict_none_present(self):
        v = make_dict(["x", "y", "x"])
        assert in_list(v, ["z"]) == [False, False, False]

    def test_dict_with_nulls(self):
        # None forces validity tracking; dict still deduplicates non-null
        v = make_dict(["a", None, "a", "b"])
        assert v.is_dict
        result = in_list(v, ["a"])
        assert result == [True, None, True, False]

    def test_dict_long_strings(self):
        v = make_dict([LONG_PRESENT, LONG_ABSENT, LONG_PRESENT, LONG_ABSENT])
        assert v.is_dict
        assert in_list(v, [LONG_PRESENT]) == [True, False, True, False]

    def test_dict_result_same_as_dense(self):
        data = ["alpha", "beta", "gamma", "alpha", "beta"]
        dense = make(data)
        dict_v = make_dict(data)
        assert dict_v.is_dict
        s = ["alpha", "gamma"]
        assert in_list(dense, s) == in_list(dict_v, s)


# ===========================================================================
# Determinism: set-build and probe hashes must match
# ===========================================================================

class TestHashPathDeterminism:
    """Confirm present values always match — catches set-build/probe divergence."""

    def test_short_present_always_matches(self):
        vals = ["abc", "xy", "hello", "hi", ""]
        v = make(vals)
        assert in_list(v, vals) == [True] * len(vals)

    def test_long_present_always_matches(self):
        vals = [LONG_PRESENT, LONG_ABSENT,
                "another long string that is over twelve chars"]
        v = make(vals)
        assert in_list(v, vals) == [True] * len(vals)

    def test_mixed_present_always_matches(self):
        vals = ["short", LONG_PRESENT, "also_short", LONG_ABSENT]
        v = make(vals)
        assert in_list(v, vals) == [True] * len(vals)


# ===========================================================================
# Large vector
# ===========================================================================

class TestLargeVector:
    N = 10_000

    def _make_data(self):
        return [f"value_{i:06d}" for i in range(self.N)]

    def test_large_nonnull(self):
        data = self._make_data()
        s = set(data[::100])
        v = make(data)
        result = in_list(v, list(s))
        expected = [x in s for x in data]
        assert result == expected

    def test_large_mixed_null(self):
        data = [f"v_{i}" if i % 7 != 0 else None for i in range(self.N)]
        s = {f"v_{i}" for i in range(0, self.N, 50) if i % 7 != 0}
        v = make(data)
        result = in_list(v, list(s))
        expected = [_py_in_list(x, s) for x in data]
        assert result == expected

    def test_large_empty_set(self):
        data = self._make_data()
        v = make(data)
        result = in_list(v, [])
        assert all(r is False for r in result)

    def test_large_full_set(self):
        data = self._make_data()
        v = make(data)
        result = in_list(v, data)
        assert all(r is True for r in result)
