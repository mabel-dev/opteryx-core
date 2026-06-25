"""
Native correctness tests for E.10: bytewise string-search consumers.

Coverage matrix:

  vector_starts_with / vector_ends_with / vector_contains (CS):
    known fixtures: prefix/suffix/substring True and False cases.
    empty needle → True for all non-null rows (SQL convention; matches old .pyx).
    null needle:
      starts_with / ends_with: treated as empty needle (always True for non-null).
      contains: all-null result (SQL: x CONTAINS NULL = NULL).
    null haystack rows → null output rows (TVL; validity bitmap preserved).
    bit-boundary sizes 1–9.
    short strings (≤12 B, inline slot) and long strings (>12 B, arena slot).
    dict-encoded shape: same result as dense for same logical values.

  vector_ci_starts_with / vector_ci_ends_with / vector_contains(ignore_case=True):
    ASCII case-fold: A–Z folded, non-ASCII bytes pass through unchanged.
    same TVL and bit-boundary guarantees as CS variants.

  Multi-type invariant (VARCHAR / NVARCHAR / VARBINARY):
    Code does not branch on string type tag for bytewise ops; the invariant is
    structural.  Tested here via dense VARCHAR + dict-encoded VARCHAR (different
    DrakenVector layouts, same bytes) giving identical results.

  vector_contains_any (array membership):
    basic True/False; empty items → all False; null rows → False; short rows.

  vector_contains_all (array membership):
    basic True/False; empty items → True for non-null rows; null rows → False.

  TypeError:
    non-Vector haystack → TypeError (raised by draken_vector_unwrap).
"""

import glob
import importlib.util
import os

import draken.draken_native as dn
import pytest

# ---------------------------------------------------------------------------
# Load extension
# ---------------------------------------------------------------------------

def _load_mod():
    pattern = os.path.join(
        os.path.dirname(__file__), "..", "..", "..",
        "opteryx", "compiled", "nanobind", "vectors*.so"
    )
    matches = glob.glob(pattern)
    if not matches:
        pytest.skip(
            "vector_string_search extension not built — run make compile",
            allow_module_level=True,
        )
    spec = importlib.util.spec_from_file_location(
        "opteryx.compiled.nanobind.vectors", matches[0]
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


vss = _load_mod()

# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------

def sv(values):
    """Dense VARCHAR Vector from list of str|None."""
    return dn.vector_from_string_sequence(
        [v.encode("utf-8") if isinstance(v, str) else v for v in values]
    )


def sv_dict(values):
    """Dict-encoded VARCHAR Vector from list of str|None."""
    return dn.vector_from_string_dict_sequence(
        [v.encode("utf-8") if isinstance(v, str) else v for v in values]
    )


def needle(s):
    """Single-value VARCHAR needle Vector."""
    return dn.vector_from_string_sequence(
        [s.encode("utf-8") if isinstance(s, str) else s]
    )


def needle_null():
    """Single-row null VARCHAR needle Vector."""
    return dn.vector_from_string_sequence([None])


def arr(rows):
    """DRAKEN_ARRAY Vector from list of list[...] | None."""
    return dn.vector_array_from_sequence(rows)


def pylist(v):
    return v.to_pylist()


# ===========================================================================
# STARTS_WITH — case-sensitive
# ===========================================================================

class TestStartsWith:
    def test_basic_true(self):
        v = sv(["hello world", "hello", "goodbye"])
        r = pylist(vss.vector_starts_with(v, needle("hello")))
        assert r == [True, True, False]

    def test_basic_false(self):
        v = sv(["abc", "def", "ghi"])
        assert pylist(vss.vector_starts_with(v, needle("xyz"))) == [False, False, False]

    def test_empty_needle_always_true(self):
        v = sv(["a", "b", ""])
        assert pylist(vss.vector_starts_with(v, needle(""))) == [True, True, True]

    def test_null_needle_treated_as_empty(self):
        # Old .pyx: null needle → needle_len=0 → empty-needle convention → True.
        v = sv(["a", "b"])
        assert pylist(vss.vector_starts_with(v, needle_null())) == [True, True]

    def test_null_haystack_propagates(self):
        v = sv(["hello", None, "world"])
        r = pylist(vss.vector_starts_with(v, needle("h")))
        assert r == [True, None, False]

    def test_all_null_haystack(self):
        v = sv([None, None, None])
        assert pylist(vss.vector_starts_with(v, needle("x"))) == [None, None, None]

    def test_exact_match_equals_starts_with(self):
        v = sv(["abc"])
        assert pylist(vss.vector_starts_with(v, needle("abc"))) == [True]

    def test_needle_longer_than_haystack(self):
        v = sv(["hi"])
        assert pylist(vss.vector_starts_with(v, needle("hello"))) == [False]

    def test_long_string_prefix(self):
        s = "this is a long string that exceeds twelve bytes"
        v = sv([s, "short"])
        pfx = s[:20]
        assert pylist(vss.vector_starts_with(v, needle(pfx))) == [True, False]

    def test_empty_haystack(self):
        v = sv([])
        assert pylist(vss.vector_starts_with(v, needle("x"))) == []

    @pytest.mark.parametrize("n", range(1, 10))
    def test_bit_boundary_sizes(self, n):
        data = [f"pre_{i}" for i in range(n)]
        v = sv(data)
        r = pylist(vss.vector_starts_with(v, needle("pre_")))
        assert r == [True] * n, f"size={n}"

    @pytest.mark.parametrize("n", range(1, 10))
    def test_bit_boundary_with_nulls(self, n):
        data = [("hi" if i % 2 == 0 else None) for i in range(n)]
        v = sv(data)
        r = pylist(vss.vector_starts_with(v, needle("h")))
        expected = [(True if x == "hi" else None) for x in data]
        assert r == expected, f"size={n}"

    def test_dict_encoded_same_as_dense(self):
        data = ["hello", "world", "help", "world", "hello"]
        assert (pylist(vss.vector_starts_with(sv(data),      needle("hel"))) ==
                pylist(vss.vector_starts_with(sv_dict(data), needle("hel"))))

    def test_result_type_is_bool(self):
        v = sv(["x"])
        assert vss.vector_starts_with(v, needle("x")).type == dn.DrakenType.BOOL

    def test_type_error_on_non_vector(self):
        with pytest.raises(TypeError):
            vss.vector_starts_with("not a vector", needle("x"))


# ===========================================================================
# CI STARTS_WITH
# ===========================================================================

class TestCiStartsWith:
    def test_upper_lower_match(self):
        v = sv(["Hello", "HELLO", "hello", "world"])
        r = pylist(vss.vector_ci_starts_with(v, needle("hell")))
        assert r == [True, True, True, False]

    def test_mixed_case_needle(self):
        v = sv(["Python", "PYTHON", "java"])
        r = pylist(vss.vector_ci_starts_with(v, needle("PY")))
        assert r == [True, True, False]

    def test_empty_needle_always_true(self):
        v = sv(["A", "B"])
        assert pylist(vss.vector_ci_starts_with(v, needle(""))) == [True, True]

    def test_null_haystack_propagates(self):
        v = sv(["Hello", None, "HELLO"])
        r = pylist(vss.vector_ci_starts_with(v, needle("hello")))
        assert r == [True, None, True]

    def test_non_ascii_bytes_unchanged(self):
        # 0xC3 0xA9 = UTF-8 'é': not affected by ASCII fold
        v = sv(["\xe9l\xe8ve"])  # "élève" stored as raw bytes
        # Needle "él" should match exactly (byte match); CI doesn't fold non-ASCII.
        r = pylist(vss.vector_ci_starts_with(v, needle("\xe9l")))
        assert r == [True]

    @pytest.mark.parametrize("n", range(1, 10))
    def test_bit_boundary(self, n):
        data = ["ABCdef"] * n
        v = sv(data)
        assert pylist(vss.vector_ci_starts_with(v, needle("abc"))) == [True] * n


# ===========================================================================
# ENDS_WITH — case-sensitive
# ===========================================================================

class TestEndsWith:
    def test_basic_true(self):
        v = sv(["hello", "world", "lo"])
        assert pylist(vss.vector_ends_with(v, needle("lo"))) == [True, False, True]

    def test_empty_needle_always_true(self):
        v = sv(["a", "b"])
        assert pylist(vss.vector_ends_with(v, needle(""))) == [True, True]

    def test_null_needle_treated_as_empty(self):
        v = sv(["a", "b"])
        assert pylist(vss.vector_ends_with(v, needle_null())) == [True, True]

    def test_null_haystack_propagates(self):
        v = sv(["end", None, "end"])
        r = pylist(vss.vector_ends_with(v, needle("end")))
        assert r == [True, None, True]

    def test_needle_longer_than_haystack(self):
        v = sv(["hi"])
        assert pylist(vss.vector_ends_with(v, needle("longer"))) == [False]

    def test_exact_match(self):
        v = sv(["abc"])
        assert pylist(vss.vector_ends_with(v, needle("abc"))) == [True]

    def test_long_string_suffix(self):
        s = "this is a long string that exceeds twelve bytes"
        v = sv([s, "short"])
        sfx = s[-15:]
        assert pylist(vss.vector_ends_with(v, needle(sfx))) == [True, False]

    @pytest.mark.parametrize("n", range(1, 10))
    def test_bit_boundary_sizes(self, n):
        data = [f"_{i}_suffix" for i in range(n)]
        v = sv(data)
        assert pylist(vss.vector_ends_with(v, needle("suffix"))) == [True] * n

    def test_dict_encoded_same_as_dense(self):
        data = ["hello", "world", "lo", "world", "hello"]
        assert (pylist(vss.vector_ends_with(sv(data),      needle("lo"))) ==
                pylist(vss.vector_ends_with(sv_dict(data), needle("lo"))))


# ===========================================================================
# CI ENDS_WITH
# ===========================================================================

class TestCiEndsWith:
    def test_upper_lower_match(self):
        v = sv(["hellO", "hELLO", "hello", "world"])
        r = pylist(vss.vector_ci_ends_with(v, needle("llo")))
        assert r == [True, True, True, False]

    def test_null_haystack_propagates(self):
        v = sv(["WORLD", None, "world"])
        r = pylist(vss.vector_ci_ends_with(v, needle("ld")))
        assert r == [True, None, True]

    @pytest.mark.parametrize("n", range(1, 10))
    def test_bit_boundary(self, n):
        data = ["endEND"] * n
        v = sv(data)
        assert pylist(vss.vector_ci_ends_with(v, needle("end"))) == [True] * n


# ===========================================================================
# CONTAINS — case-sensitive (Volnitsky)
# ===========================================================================

class TestContains:
    def test_basic_true(self):
        v = sv(["hello world", "hello", "ell"])
        assert pylist(vss.vector_contains(v, needle("ell"))) == [True, True, True]

    def test_basic_false(self):
        v = sv(["abc", "def"])
        assert pylist(vss.vector_contains(v, needle("xyz"))) == [False, False]

    def test_empty_needle_always_true(self):
        v = sv(["a", "b", ""])
        assert pylist(vss.vector_contains(v, needle(""))) == [True, True, True]

    def test_null_needle_gives_all_null(self):
        # SQL: x CONTAINS NULL = NULL for every row.
        v = sv(["a", "b", "c"])
        r = pylist(vss.vector_contains(v, needle_null()))
        assert r == [None, None, None]

    def test_null_haystack_propagates(self):
        v = sv(["hello", None, "world"])
        r = pylist(vss.vector_contains(v, needle("ell")))
        assert r == [True, None, False]

    def test_all_null_haystack(self):
        v = sv([None, None])
        r = pylist(vss.vector_contains(v, needle("x")))
        assert r == [None, None]

    def test_needle_longer_than_haystack(self):
        v = sv(["hi"])
        assert pylist(vss.vector_contains(v, needle("longer"))) == [False]

    def test_needle_equal_to_haystack(self):
        v = sv(["abc"])
        assert pylist(vss.vector_contains(v, needle("abc"))) == [True]

    def test_long_needle_in_long_haystack(self):
        body = "the quick brown fox jumps over the lazy dog"
        v = sv([body, "short"])
        assert pylist(vss.vector_contains(v, needle("brown fox"))) == [True, False]

    def test_long_needle_absent(self):
        body = "the quick brown fox jumps over the lazy dog"
        v = sv([body])
        assert pylist(vss.vector_contains(v, needle("brown cat"))) == [False]

    def test_short_string_both_sides(self):
        v = sv(["ab", "a", "b", ""])
        assert pylist(vss.vector_contains(v, needle("a"))) == [True, True, False, False]

    @pytest.mark.parametrize("n", range(1, 10))
    def test_bit_boundary_all_match(self, n):
        v = sv(["needle_here"] * n)
        assert pylist(vss.vector_contains(v, needle("needle"))) == [True] * n

    @pytest.mark.parametrize("n", range(1, 10))
    def test_bit_boundary_none_match(self, n):
        v = sv(["no match here"] * n)
        assert pylist(vss.vector_contains(v, needle("xyz"))) == [False] * n

    def test_dict_encoded_same_as_dense(self):
        data = ["hello world", "goodbye", "hello world", "world"]
        assert (pylist(vss.vector_contains(sv(data),      needle("world"))) ==
                pylist(vss.vector_contains(sv_dict(data), needle("world"))))

    def test_dict_encoded_compressed_fastpath(self):
        # Many repeats of few distinct values: input is compressed
        # (data_length < length) so the all-valid compressed fast path fires.
        # Result must match dense for the same logical values, and the result
        # itself stays compressed (reuses the input codes).
        data = (["alpha", "beta google", "gamma", "beta google"] * 64)
        dv = sv_dict(data)
        assert dv.is_compressed, "fixture must be dict-compressed to exercise the fast path"
        res = vss.vector_contains(dv, needle("google"))
        assert pylist(res) == pylist(vss.vector_contains(sv(data), needle("google")))
        assert res.is_compressed, "compressed input should yield a compressed bool result"

    def test_dict_encoded_with_nulls_matches_dense(self):
        # Nullable dict input must take the dense path (gate) and still be correct.
        data = ["hello world", None, "world", "hello world", None]
        assert (pylist(vss.vector_contains(sv(data),      needle("world"))) ==
                pylist(vss.vector_contains(sv_dict(data), needle("world"))))

    def test_result_type_is_bool(self):
        v = sv(["x"])
        assert vss.vector_contains(v, needle("x")).type == dn.DrakenType.BOOL

    def test_type_error_on_non_vector(self):
        with pytest.raises(TypeError):
            vss.vector_contains("not a vector", needle("x"))


# ===========================================================================
# CONTAINS CI
# ===========================================================================

class TestContainsCI:
    def test_upper_in_haystack(self):
        v = sv(["Hello World", "HELLO", "bye"])
        r = pylist(vss.vector_contains(v, needle("ello"), ignore_case=True))
        assert r == [True, True, False]

    def test_mixed_case_needle(self):
        v = sv(["python", "PYTHON", "java"])
        r = pylist(vss.vector_contains(v, needle("YTHO"), ignore_case=True))
        assert r == [True, True, False]

    def test_null_haystack(self):
        v = sv(["Hello", None])
        r = pylist(vss.vector_contains(v, needle("hello"), ignore_case=True))
        assert r == [True, None]

    def test_null_needle_gives_all_null(self):
        v = sv(["a", "b"])
        r = pylist(vss.vector_contains(v, needle_null(), ignore_case=True))
        assert r == [None, None]

    def test_empty_needle_always_true(self):
        v = sv(["A", "B"])
        assert pylist(vss.vector_contains(v, needle(""), ignore_case=True)) == [True, True]

    @pytest.mark.parametrize("n", range(1, 10))
    def test_bit_boundary(self, n):
        v = sv(["NEEDLE"] * n)
        assert pylist(vss.vector_contains(v, needle("needle"), ignore_case=True)) == [True] * n


# ===========================================================================
# CONTAINS_ANY — array membership
# ===========================================================================

class TestContainsAny:
    def test_basic_match(self):
        v = arr([[1, 2, 3], [4, 5], [6]])
        r = pylist(vss.vector_contains_any(v, {2}))
        assert r == [True, False, False]

    def test_multiple_items(self):
        v = arr([[1, 2, 3], [4, 5], [6]])
        r = pylist(vss.vector_contains_any(v, {2, 4}))
        assert r == [True, True, False]

    def test_no_match(self):
        v = arr([[1, 2], [3, 4]])
        assert pylist(vss.vector_contains_any(v, {99})) == [False, False]

    def test_empty_items_all_false(self):
        v = arr([[1, 2], [3]])
        assert pylist(vss.vector_contains_any(v, set())) == [False, False]

    def test_null_row_gives_false(self):
        v = arr([[1, 2], None, [3]])
        r = pylist(vss.vector_contains_any(v, {2}))
        assert r == [True, False, False]

    def test_empty_array(self):
        v = arr([])
        assert pylist(vss.vector_contains_any(v, {1})) == []

    def test_string_items(self):
        v = arr([["a", "b"], ["c"], ["a"]])
        r = pylist(vss.vector_contains_any(v, {"a"}))
        assert r == [True, False, True]


# ===========================================================================
# CONTAINS_ALL — array membership
# ===========================================================================

class TestContainsAll:
    def test_all_present(self):
        v = arr([[1, 2, 3], [1, 3], [2]])
        r = pylist(vss.vector_contains_all(v, {1, 2}))
        assert r == [True, False, False]

    def test_single_item(self):
        v = arr([[1, 2, 3], [4, 5]])
        assert pylist(vss.vector_contains_all(v, {2})) == [True, False]

    def test_empty_items_vacuously_true(self):
        # Empty needle set → True for all non-null rows (vacuous truth).
        v = arr([[1, 2], [3]])
        assert pylist(vss.vector_contains_all(v, set())) == [True, True]

    def test_null_row_gives_false(self):
        v = arr([[1, 2, 3], None, [1, 2]])
        r = pylist(vss.vector_contains_all(v, {1, 2}))
        assert r == [True, False, True]

    def test_empty_array(self):
        v = arr([])
        assert pylist(vss.vector_contains_all(v, {1})) == []

    def test_not_all_present(self):
        v = arr([[1, 2], [3, 4]])
        assert pylist(vss.vector_contains_all(v, {1, 3})) == [False, False]

    def test_string_items(self):
        v = arr([["a", "b", "c"], ["a", "b"], ["c"]])
        r = pylist(vss.vector_contains_all(v, {"a", "b"}))
        assert r == [True, True, False]
