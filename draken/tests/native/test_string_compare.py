"""
Native correctness tests for Milestone D.2: string hash + compare_scalar + compare_vector.

Coverage (per 04_testing.md §1):
  hash:
    equal values → equal hashes (short and long)
    null rows → NULL_HASH sentinel
    short (≤12 B) exact: distinct short strings differ in hash (beyond mixing collisions)
    long (>12 B) §1 exception: documented, same-content same-hash

  compare_scalar / compare_vector (all 6 ops: eq/ne/gt/ge/lt/le):
    short exact equality path (≤12 B)
    long hash-only equality path (>12 B) — §1 exception verified
    ordering with prefix tie (same 4-byte prefix, differ in tail)
    null operand → null output (TVL)
    empty string ""
    bit-boundary sizes (n=1..9)
    high-bit/UTF-8 bytes
    equal values across multiple rows
    compare_scalar: literal determinism (long literal eq stored twin)
"""

import pytest

import draken.draken_native as dn
from draken.draken_native import DrakenType

# Op codes (draken convention)
EQ, NE, GT, GE, LT, LE = 0, 1, 2, 3, 4, 5

# Expected hash for a null row: NULL_HASH sentinel after simd_hash_i64 mixing.
# Obtained empirically: simd_hash_i64(0x4c3f95a36ab8ecca) → 0x73d59cff8f94d86c.
# Both int64 and string nulls produce this same value (shared pipeline).
NULL_HASH_MIXED = 0x73d59cff8f94d86c


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make(lst):
    return dn.vector_from_string_sequence(lst)


def hashes(lst):
    return make(lst).hash()


def cmp_s(lst, scalar, op):
    """compare_scalar over a string vector."""
    return make(lst).compare_scalar(scalar, op).to_pylist()


def cmp_v(lst_a, lst_b, op):
    """compare_vector between two string vectors."""
    return make(lst_a).compare_vector(make(lst_b), op).to_pylist()


# ---------------------------------------------------------------------------
# 1. Hash correctness
# ---------------------------------------------------------------------------


class TestHashBasics:
    def test_empty_vector(self):
        v = make([])
        assert v.hash() == []

    def test_type_is_string(self):
        assert make(["a"]).type == DrakenType.VARCHAR


class TestHashDeterminism:
    """Equal values must produce equal hashes across calls and positions."""

    def test_short_equal_values_same_hash(self):
        s = "hello"
        h = hashes([s, s, s])
        assert h[0] == h[1] == h[2]

    def test_long_equal_values_same_hash(self):
        s = "x" * 50
        h = hashes([s, s, s])
        assert h[0] == h[1] == h[2]

    def test_cross_vector_equal_values_same_hash(self):
        s = "equal_string_value"
        h1 = hashes(["pad", s])[1]
        h2 = hashes([s, "other"])[0]
        assert h1 == h2

    def test_long_cross_vector_equal_values_same_hash(self):
        s = "x" * 100
        h1 = hashes(["first_long_arene_" + "y" * 10, s])[1]
        h2 = hashes([s])[0]
        assert h1 == h2


class TestHashNulls:
    def test_null_row_is_null_hash_mixed(self):
        h = hashes([None])
        assert h[0] == NULL_HASH_MIXED

    def test_null_hash_matches_sentinel_mixed(self):
        h = hashes(["a", None, "b"])
        assert h[1] == NULL_HASH_MIXED

    def test_null_string_hash_matches_null_int64_hash(self):
        # Both string and int64 null rows must produce the same mixed hash.
        str_null_hash = hashes([None])[0]
        int_null_hash = dn.vector_from_sequence([None]).hash()[0]
        assert str_null_hash == int_null_hash

    def test_non_null_rows_not_null_hash(self):
        h = hashes(["hello", "world"])
        assert all(v != NULL_HASH_MIXED for v in h)


class TestHashDistinction:
    """Distinct strings should (with overwhelming probability) have distinct hashes."""

    def test_short_distinct_values_distinct_hashes(self):
        strings = ["a", "b", "c", "d", "e", "f", "g", "h"]
        h = hashes(strings)
        assert len(set(h)) == len(strings), "distinct short strings should have distinct hashes"

    def test_long_distinct_values_distinct_hashes(self):
        strings = ["x" * 20 + str(i) for i in range(10)]
        h = hashes(strings)
        assert len(set(h)) == len(strings)

    def test_empty_string_has_distinct_hash_from_nonnull(self):
        h = hashes(["", "a"])
        assert h[0] != h[1]


# ---------------------------------------------------------------------------
# 2. compare_scalar: short strings (≤12 B) — exact path
# ---------------------------------------------------------------------------


class TestCmpScalarShortEq:
    """Short strings: eq/ne are exact (not hash-only)."""

    def test_exact_eq_match(self):
        assert cmp_s(["abc", "def", "abc"], "abc", EQ) == [True, False, True]

    def test_exact_ne(self):
        assert cmp_s(["abc", "def"], "abc", NE) == [False, True]

    def test_empty_string_eq(self):
        assert cmp_s(["", "a", ""], "", EQ) == [True, False, True]

    def test_case_sensitive(self):
        assert cmp_s(["Hello", "hello"], "hello", EQ) == [False, True]

    def test_boundary_12_byte_eq(self):
        s = "a" * 12
        assert cmp_s([s, "a" * 11, "a" * 13], s, EQ) == [True, False, False]


class TestCmpScalarShortOrdering:
    """Short strings: ordering via prefix then inline bytes."""

    def test_lt_simple(self):
        assert cmp_s(["apple", "banana", "cherry"], "banana", LT) == [True, False, False]

    def test_gt_simple(self):
        assert cmp_s(["apple", "banana", "cherry"], "banana", GT) == [False, False, True]

    def test_ge_includes_equal(self):
        assert cmp_s(["a", "b", "c"], "b", GE) == [False, True, True]

    def test_le_includes_equal(self):
        assert cmp_s(["a", "b", "c"], "b", LE) == [True, True, False]

    def test_prefix_tie_shorter_wins(self):
        # "abcX" > "abc" lexicographically
        assert cmp_s(["abcX", "abc", "ab"], "abc", GT) == [True, False, False]

    def test_prefix_tie_at_byte_4(self):
        # Same first 4 bytes "abcd", differ at byte 5
        a = "abcdXXX"
        b = "abcdYYY"
        assert cmp_s([a, b], "abcdYYY", LT) == [True, False]


# ---------------------------------------------------------------------------
# 3. compare_scalar: long strings (>12 B) — §1 hash-only eq path
# ---------------------------------------------------------------------------


class TestCmpScalarLongEq:
    """Long strings (>12 B): eq/ne are hash-only (§1 exception)."""

    def test_long_eq_match(self):
        s = "x" * 50
        assert cmp_s([s, "y" * 50, s], s, EQ) == [True, False, True]

    def test_long_ne(self):
        s = "hello_world_long_string_here"
        assert cmp_s([s, "other_long_string_value_"], s, NE) == [False, True]

    def test_long_exact_13_bytes(self):
        s = "c" * 13
        assert cmp_s([s, "d" * 13], s, EQ) == [True, False]

    def test_literal_determinism(self):
        # The scalar literal is built via the same ingestion path as D.1.
        # A stored long string and the same literal must match on eq.
        s = "deterministic_test_literal_" + "X" * 10
        assert cmp_s([s, s + "_extra"], s, EQ) == [True, False]

    def test_cross_vector_long_eq(self):
        # Same string at different arena offsets (offset is NOT part of eq).
        s = "prefix_" + "z" * 20
        v1 = make([s])
        v2 = make(["filler_long_string____" + "y" * 10, s])
        assert v1.compare_scalar(s, EQ).to_pylist() == [True]
        assert v2.compare_scalar(s, EQ).to_pylist() == [False, True]


class TestCmpScalarLongOrdering:
    """Long strings: ordering uses actual bytes (not hash-only)."""

    def test_long_lt(self):
        a = "apple_long_string_" + "X" * 10
        b = "banana_long_string_" + "X" * 10
        assert cmp_s([a, b], b, LT) == [True, False]

    def test_long_prefix_tie_ordering(self):
        # Same first 4 bytes "aaaa", differ at byte 13+.
        # "aaaaXXXXXXXXX" vs "aaaaYYYYYYYYY" — same prefix, differ in tail.
        a = "aaaa" + "X" * 20
        b = "aaaa" + "Y" * 20
        assert a < b
        assert cmp_s([a, b], b, LT) == [True, False]
        assert cmp_s([a, b], a, GT) == [False, True]

    def test_long_ge_includes_equal(self):
        s = "long_string_value_" + "q" * 10
        other = "long_string_value_" + "r" * 10
        assert s < other
        assert cmp_s([s, other], s, GE) == [True, True]

    def test_long_prefix_tie_exact_ordering(self):
        # Verify ordering is by actual bytes, not hash.
        a = "abcd" + "a" * 20   # same prefix "abcd", tail all 'a'
        b = "abcd" + "z" * 20   # same prefix "abcd", tail all 'z'
        assert a < b
        result = cmp_s([a, b], b, LT)
        assert result == [True, False], \
            f"prefix-tie ordering wrong: {result}"


# ---------------------------------------------------------------------------
# 4. compare_scalar: null semantics (TVL)
# ---------------------------------------------------------------------------


class TestCmpScalarNulls:
    def test_null_row_produces_null_output(self):
        result = cmp_s([None, "hello"], "hello", EQ)
        assert result[0] is None
        assert result[1] is True

    def test_all_null_all_null_output(self):
        result = cmp_s([None, None], "x", EQ)
        assert result == [None, None]

    def test_null_scalar_all_null_output(self):
        result = cmp_s(["hello", "world"], None, EQ)
        assert result == [None, None]

    def test_null_scalar_ne_all_null(self):
        result = cmp_s(["a", "b"], None, NE)
        assert result == [None, None]

    def test_mixed_null_non_null(self):
        result = cmp_s(["apple", None, "cherry"], "banana", LT)
        assert result[0] is True   # "apple" < "banana"
        assert result[1] is None   # null → null
        assert result[2] is False  # "cherry" < "banana" is False


# ---------------------------------------------------------------------------
# 5. compare_scalar: bit-boundary sizes (n = 1..9)
# ---------------------------------------------------------------------------


class TestCmpScalarBitBoundary:
    @pytest.mark.parametrize("n", range(1, 10))
    def test_eq_bit_boundary(self, n):
        data = [f"s{i}" for i in range(n)]
        target = data[n // 2]
        result = cmp_s(data, target, EQ)
        expected = [s == target for s in data]
        assert result == expected, f"n={n} eq mismatch: {result} != {expected}"

    @pytest.mark.parametrize("n", range(1, 10))
    def test_lt_bit_boundary(self, n):
        data = sorted([f"s{i:03d}" for i in range(n)])
        target = data[n // 2]
        result = cmp_s(data, target, LT)
        expected = [s < target for s in data]
        assert result == expected, f"n={n} lt mismatch: {result} != {expected}"


# ---------------------------------------------------------------------------
# 6. compare_scalar: high-bit / UTF-8 bytes
# ---------------------------------------------------------------------------


class TestCmpScalarUTF8:
    def test_high_bit_eq(self):
        s = "é_long_string_value_" + "x" * 5  # é = 0xC3 0xA9, long string
        assert cmp_s([s, "e_long_string_value_" + "x" * 5], s, EQ) == [True, False]

    def test_high_bit_ordering(self):
        # 0x7F < 0xC3 (unsigned), so "\x7f..." < "\xc0..."
        a = "\x7f" + "x" * 20
        b = "\xc0" + "x" * 20
        assert a < b
        assert cmp_s([a, b], b, LT) == [True, False]


# ---------------------------------------------------------------------------
# 7. compare_vector: basic correctness
# ---------------------------------------------------------------------------


class TestCmpVecBasic:
    def test_eq_matching_vectors(self):
        data = ["a", "b", "c"]
        assert cmp_v(data, data, EQ) == [True, True, True]

    def test_eq_mismatching_vectors(self):
        a = ["x", "y", "z"]
        b = ["x", "Y", "z"]
        assert cmp_v(a, b, EQ) == [True, False, True]

    def test_ne_vectors(self):
        a = ["x", "y"]
        b = ["x", "z"]
        assert cmp_v(a, b, NE) == [False, True]

    def test_lt_vectors(self):
        a = ["apple", "cherry"]
        b = ["banana", "banana"]
        assert cmp_v(a, b, LT) == [True, False]

    def test_gt_vectors(self):
        a = ["banana", "apple"]
        b = ["apple", "banana"]
        assert cmp_v(a, b, GT) == [True, False]

    def test_length_mismatch_raises(self):
        with pytest.raises(Exception):
            cmp_v(["a", "b"], ["a"], EQ)


class TestCmpVecLong:
    """compare_vector for long strings (>12 B)."""

    def test_long_eq(self):
        s = "x" * 50
        a = [s, "y" * 50]
        b = [s, s]
        assert cmp_v(a, b, EQ) == [True, False]

    def test_long_prefix_tie_ordering(self):
        # Same 4-byte prefix, differ in tail.
        a_str = "aaaa" + "X" * 20
        b_str = "aaaa" + "Y" * 20
        assert a_str < b_str
        assert cmp_v([a_str, b_str], [b_str, a_str], LT) == [True, False]


class TestCmpVecNulls:
    def test_null_in_a_propagates(self):
        result = cmp_v([None, "hello"], ["hello", "hello"], EQ)
        assert result[0] is None
        assert result[1] is True

    def test_null_in_b_propagates(self):
        result = cmp_v(["hello", "hello"], [None, "hello"], EQ)
        assert result[0] is None
        assert result[1] is True

    def test_null_in_both_propagates(self):
        result = cmp_v([None, "hello"], [None, "world"], EQ)
        assert result[0] is None
        assert result[1] is False

    def test_all_null(self):
        result = cmp_v([None, None], [None, None], LT)
        assert result == [None, None]


# ---------------------------------------------------------------------------
# 8. compare_vector: bit-boundary sizes (n = 1..9)
# ---------------------------------------------------------------------------


class TestCmpVecBitBoundary:
    @pytest.mark.parametrize("n", range(1, 10))
    def test_eq_bit_boundary(self, n):
        a = [f"v{i:03d}" for i in range(n)]
        b = [f"v{i:03d}" if i % 2 == 0 else f"w{i:03d}" for i in range(n)]
        result = cmp_v(a, b, EQ)
        expected = [x == y for x, y in zip(a, b)]
        assert result == expected, f"n={n}: {result} != {expected}"


# ---------------------------------------------------------------------------
# 9. §1 exception explicit documentation test
# ---------------------------------------------------------------------------


class TestSection1ExceptionDocumentation:
    """
    These tests explicitly verify the §1 exception behavior:
      - For long strings (>12 B), eq/ne are hash-only (no arena fetch).
      - For short strings (≤12 B), eq/ne are EXACT.
      - This is an architect-signed-off trade-off; documented here.
    """

    def test_short_strings_use_exact_eq(self):
        # Two short strings that differ in a trailing byte must compare as unequal.
        a = "abcdefghij"    # 10 bytes
        b = "abcdefghiX"    # 10 bytes, differ at last byte
        assert cmp_s([a, b], a, EQ) == [True, False], \
            "Short strings must use exact eq — no hash collisions tolerated"

    def test_long_strings_eq_uses_hash_only(self):
        # This test verifies the §1 path executes without errors for long strings.
        # At test scale, no actual collisions should occur.
        s = "long_string_for_section_1_test_" + "X" * 20
        result = cmp_s([s, s], s, EQ)
        assert result == [True, True], \
            "Equal long strings must return True with hash-only eq (§1)"

    def test_long_unequal_strings_ne(self):
        a = "long_string_alpha_" + "A" * 20
        b = "long_string_beta__" + "B" * 20
        result = cmp_s([a, b], a, EQ)
        assert result == [True, False], \
            "Unequal long strings must return False (no hash collision at test scale)"

    def test_short_boundary_12_exact(self):
        # At exactly 12 bytes, still inline/exact.
        a = "a" * 12
        b = "b" * 12
        assert cmp_s([a, b], a, EQ) == [True, False]

    def test_long_boundary_13_uses_hash_path(self):
        # At 13 bytes, goes to long path; hash-only for eq.
        s = "c" * 13
        result = cmp_s([s, "d" * 13], s, EQ)
        assert result == [True, False]
