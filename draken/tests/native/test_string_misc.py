"""
Native correctness tests for Milestone E.15: vector_levenshtein + vector_position +
vector_random_strings — bytewise string ops, pure nanobind C++.

Coverage:
  vector_levenshtein:
    classic fixtures: kitten/sitting=3, empty/"abc"=3, same/same=0
    long strings (>12 B) — extern slot path
    null TVL: any null input → null output row
    non-Vector input → TypeError
    length mismatch → invalid_argument

  vector_position (SQL POSITION, 1-based, 0=not found):
    found at start / middle / end
    not found
    empty needle → 1
    null TVL: any null input → null output row
    non-Vector input → TypeError
    length mismatch → invalid_argument

  vector_random_strings:
    output length matches requested width (short ≤12 B and long >12 B)
    all chars from alphabet a-z A-Z 0-9 _ /
    output type is VARCHAR
    row_count=0 → empty vector
    width=0 → empty strings
    negative args → ValueError
"""

import importlib.util
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
import draken.draken_native as dn

# ---------------------------------------------------------------------------
# Module loading (spec_from_file_location pattern — no opteryx import)
# ---------------------------------------------------------------------------

def _load_module(name, rel_path):
    base = os.path.join(os.path.dirname(__file__), "..", "..", "..", rel_path)
    # Find the compiled .so file.
    import glob
    candidates = glob.glob(base + "*.so") + glob.glob(base + "*.pyd")
    if not candidates:
        raise FileNotFoundError(f"Compiled module not found: {base}*.so")
    spec = importlib.util.spec_from_file_location(name, candidates[0])
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_misc = _load_module(
    "vector_string_misc",
    "opteryx/compiled/nanobind/vector_string_misc.cpython",
)
vector_levenshtein  = _misc.vector_levenshtein
vector_position     = _misc.vector_position
vector_random_strings = _misc.vector_random_strings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ALPHABET = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_/")


def make(lst):
    return dn.vector_from_string_sequence(lst)


# ---------------------------------------------------------------------------
# vector_levenshtein
# ---------------------------------------------------------------------------

class TestLevenshtein:
    def test_classic_kitten_sitting(self):
        a = make(["kitten"])
        b = make(["sitting"])
        assert vector_levenshtein(a, b).to_pylist() == [3]

    def test_empty_vs_abc(self):
        assert vector_levenshtein(make([""]), make(["abc"])).to_pylist() == [3]
        assert vector_levenshtein(make(["abc"]), make([""])).to_pylist() == [3]

    def test_same_same(self):
        assert vector_levenshtein(make(["same"]), make(["same"])).to_pylist() == [0]

    def test_batch(self):
        a = make(["kitten", "", "same", "abc"])
        b = make(["sitting", "abc", "same", ""])
        assert vector_levenshtein(a, b).to_pylist() == [3, 3, 0, 3]

    def test_long_strings_same(self):
        # >12 bytes — exercises extern slot path
        s = "abcdefghijklmnopq"
        assert vector_levenshtein(make([s]), make([s])).to_pylist() == [0]

    def test_long_strings_differ(self):
        a = make(["abcdefghijklmnopq"])
        b = make(["abcdefghijklmnopX"])  # 1 substitution
        assert vector_levenshtein(a, b).to_pylist() == [1]

    def test_null_a_is_null_output(self):
        res = vector_levenshtein(make([None, "abc"]), make(["xyz", "abc"]))
        lst = res.to_pylist()
        assert lst[0] is None
        assert lst[1] == 0

    def test_null_b_is_null_output(self):
        res = vector_levenshtein(make(["abc", "abc"]), make([None, "abc"]))
        lst = res.to_pylist()
        assert lst[0] is None
        assert lst[1] == 0

    def test_all_null(self):
        res = vector_levenshtein(make([None, None]), make([None, None]))
        assert res.to_pylist() == [None, None]

    def test_non_vector_raises(self):
        with pytest.raises(TypeError):
            vector_levenshtein("not_a_vector", make(["abc"]))

    def test_length_mismatch_raises(self):
        with pytest.raises(Exception):
            vector_levenshtein(make(["a", "b"]), make(["x"]))


# ---------------------------------------------------------------------------
# vector_position
# ---------------------------------------------------------------------------

class TestPosition:
    def test_found_at_start(self):
        assert vector_position(make(["foobar"]), make(["foo"])).to_pylist() == [1]

    def test_found_in_middle(self):
        assert vector_position(make(["hello"]), make(["ll"])).to_pylist() == [3]

    def test_found_at_end(self):
        assert vector_position(make(["hello"]), make(["lo"])).to_pylist() == [4]

    def test_not_found(self):
        assert vector_position(make(["foobar"]), make(["baz"])).to_pylist() == [0]

    def test_empty_needle_returns_1(self):
        assert vector_position(make(["foobar"]), make([""])).to_pylist() == [1]

    def test_empty_haystack_not_found(self):
        assert vector_position(make([""]), make(["x"])).to_pylist() == [0]

    def test_exact_match(self):
        assert vector_position(make(["abc"]), make(["abc"])).to_pylist() == [1]

    def test_batch(self):
        hay = make(["foobar", "foobar", "foobar", "hello"])
        ndl = make(["foo",    "baz",    "",       "ll"])
        assert vector_position(hay, ndl).to_pylist() == [1, 0, 1, 3]

    def test_long_strings(self):
        # >12-byte needle
        hay = make(["abcdefghijklmnop_suffix"])
        ndl = make(["abcdefghijklmnop"])
        assert vector_position(hay, ndl).to_pylist() == [1]
        ndl2 = make(["_suffix"])
        assert vector_position(hay, ndl2).to_pylist() == [17]

    def test_null_haystack_is_null_output(self):
        res = vector_position(make([None, "hello"]), make(["x", "ll"]))
        lst = res.to_pylist()
        assert lst[0] is None
        assert lst[1] == 3

    def test_null_needle_is_null_output(self):
        res = vector_position(make(["hello", "world"]), make([None, "or"]))
        lst = res.to_pylist()
        assert lst[0] is None
        assert lst[1] == 2

    def test_all_null(self):
        assert vector_position(make([None]), make([None])).to_pylist() == [None]

    def test_non_vector_raises(self):
        with pytest.raises(TypeError):
            vector_position("not_a_vector", make(["abc"]))

    def test_length_mismatch_raises(self):
        with pytest.raises(Exception):
            vector_position(make(["a", "b"]), make(["x"]))


# ---------------------------------------------------------------------------
# vector_random_strings
# ---------------------------------------------------------------------------

class TestRandomStrings:
    def test_correct_length_short(self):
        res = vector_random_strings(50, 8)  # ≤12 bytes — inline slot
        for s in res.to_pylist():
            assert len(s) == 8, f"expected length 8, got {len(s)}"

    def test_correct_length_long(self):
        res = vector_random_strings(50, 16)  # >12 bytes — extern slot
        for s in res.to_pylist():
            assert len(s) == 16, f"expected length 16, got {len(s)}"

    def test_charset_short(self):
        res = vector_random_strings(200, 8)
        for s in res.to_pylist():
            for c in s:
                assert c in ALPHABET, f"unexpected char {c!r}"

    def test_charset_long(self):
        res = vector_random_strings(100, 20)
        for s in res.to_pylist():
            for c in s:
                assert c in ALPHABET, f"unexpected char {c!r}"

    def test_type_is_varchar(self):
        res = vector_random_strings(10, 8)
        assert "VARCHAR" in str(res.type)

    def test_row_count_zero(self):
        res = vector_random_strings(0, 8)
        assert res.to_pylist() == []

    def test_width_zero(self):
        res = vector_random_strings(5, 0)
        assert res.to_pylist() == ["", "", "", "", ""]

    def test_negative_row_count_raises(self):
        with pytest.raises((ValueError, Exception)):
            vector_random_strings(-1, 8)

    def test_negative_width_raises(self):
        with pytest.raises((ValueError, Exception)):
            vector_random_strings(10, -1)

    def test_non_deterministic(self):
        # Two independent calls should (almost certainly) differ.
        r1 = vector_random_strings(20, 12)
        r2 = vector_random_strings(20, 12)
        assert r1.to_pylist() != r2.to_pylist(), "RNG produced identical output"
