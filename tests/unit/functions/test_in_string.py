"""
Tests for StringVector.contains() — the Draken-native InStr implementation.

Covers dense, dictionary-encoded, and constant-encoded vectors with both
case-sensitive and case-insensitive search, null handling, and unicode.
"""

import os
import sys

import pyarrow as pa
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from draken.interop.arrow import vector_from_arrow
from draken.vectors.string_vector import StringVector


def _vec(values):
    """Build a dense StringVector from a list of Python strings/Nones."""
    return vector_from_arrow(pa.array(values, type=pa.string()))


def _dict_vec(codes, dictionary):
    """Build a dictionary-encoded StringVector."""
    return StringVector.from_dict(codes, [v.encode() if isinstance(v, str) else v for v in dictionary])


def _const_vec(value, n):
    """Build a constant-encoded StringVector."""
    v = value.encode() if isinstance(value, str) else value
    return StringVector.from_constant(v, n)


def _cs(vec, needle):
    """Case-sensitive contains."""
    return vec.contains(needle.encode() if isinstance(needle, str) else needle, False).to_pylist()


def _ci(vec, needle):
    """Case-insensitive contains."""
    return vec.contains(needle.encode() if isinstance(needle, str) else needle, True).to_pylist()


# ---------------------------------------------------------------------------
# Dense vectors — case-sensitive
# ---------------------------------------------------------------------------


class TestListInString:
    def test_basic_match(self):
        vec = _vec(["hello world", "foo bar", "baz"])
        assert _cs(vec, "world") == [True, False, False]

    def test_no_match(self):
        vec = _vec(["hello", "world"])
        assert _cs(vec, "xyz") == [False, False]

    def test_all_match(self):
        vec = _vec(["abc", "xabcy", "abc_end"])
        assert _cs(vec, "abc") == [True, True, True]

    def test_needle_at_start(self):
        vec = _vec(["prefix_rest", "nothing_here"])
        assert _cs(vec, "prefix") == [True, False]

    def test_needle_at_end(self):
        vec = _vec(["rest_suffix", "nothing_here"])
        assert _cs(vec, "suffix") == [True, False]

    def test_single_char_needle(self):
        vec = _vec(["abc", "def", "ghi"])
        assert _cs(vec, "d") == [False, True, False]

    def test_single_row_match(self):
        vec = _vec(["hello"])
        assert _cs(vec, "hell") == [True]

    def test_single_row_no_match(self):
        vec = _vec(["hello"])
        assert _cs(vec, "world") == [False]

    def test_empty_vector(self):
        vec = _vec([])
        assert _cs(vec, "x") == []

    def test_empty_needle_returns_false_for_all(self):
        # Empty needle: no search performed, all False
        vec = _vec(["abc", "def"])
        assert _cs(vec, "") == [False, False]

    def test_repeated_needle(self):
        vec = _vec(["abcabcabc"])
        assert _cs(vec, "abc") == [True]

    def test_overlapping_pattern(self):
        vec = _vec(["aaaa"])
        assert _cs(vec, "aa") == [True]

    def test_case_sensitive_no_match(self):
        vec = _vec(["HELLO WORLD"])
        assert _cs(vec, "world") == [False]

    def test_special_chars(self):
        vec = _vec(["a.b", "a*b", "a+b"])
        assert _cs(vec, "a.b") == [True, False, False]
        assert _cs(vec, "a*b") == [False, True, False]

    def test_needle_longer_than_string(self):
        vec = _vec(["hi"])
        assert _cs(vec, "hello") == [False]


# ---------------------------------------------------------------------------
# Dense vectors — null handling
# ---------------------------------------------------------------------------


class TestListInStringNulls:
    def test_null_row_produces_null(self):
        # SQL null semantics: NULL contains 'x' = NULL (not False)
        vec = _vec(["hello world", None, "goodbye"])
        assert _cs(vec, "world") == [True, None, False]

    def test_all_null(self):
        vec = _vec([None, None, None])
        assert _cs(vec, "x") == [None, None, None]

    def test_null_among_matches(self):
        vec = _vec(["abc", None, "xabcx"])
        assert _cs(vec, "abc") == [True, None, True]


# ---------------------------------------------------------------------------
# Dense vectors — unicode
# ---------------------------------------------------------------------------


class TestListInStringUnicode:
    def test_unicode_needle(self):
        vec = _vec(["café au lait", "plain text", "naïve approach"])
        assert _cs(vec, "café") == [True, False, False]

    def test_multibyte_needle(self):
        vec = _vec(["日本語テスト", "hello"])
        assert _cs(vec, "テスト") == [True, False]

    def test_emoji(self):
        vec = _vec(["hello 😀 world", "no emoji here"])
        assert _cs(vec, "😀") == [True, False]


# ---------------------------------------------------------------------------
# Dense vectors — case-insensitive
# ---------------------------------------------------------------------------


class TestListInStringCaseInsensitive:
    def test_basic(self):
        vec = _vec(["Hello World", "FOO BAR", "baz"])
        assert _ci(vec, "world") == [True, False, False]

    def test_uppercase_needle(self):
        vec = _vec(["hello world", "foo"])
        assert _ci(vec, "WORLD") == [True, False]

    def test_mixed_case_needle(self):
        vec = _vec(["Hello World"])
        assert _ci(vec, "hElLo") == [True]

    def test_single_char(self):
        vec = _vec(["ABC", "def", "Ghi"])
        assert _ci(vec, "A") == [True, False, False]
        assert _ci(vec, "a") == [True, False, False]

    def test_null_produces_null(self):
        # SQL null semantics: NULL icontains 'x' = NULL
        vec = _vec(["Hello World", None, "WORLD"])
        assert _ci(vec, "world") == [True, None, True]

    def test_empty_vector(self):
        vec = _vec([])
        assert _ci(vec, "x") == []

    def test_differs_from_sensitive(self):
        vec = _vec(["HELLO", "Hello", "hello"])
        assert _cs(vec, "hello") == [False, False, True]
        assert _ci(vec, "hello") == [True, True, True]


# ---------------------------------------------------------------------------
# Dictionary-encoded vectors
# ---------------------------------------------------------------------------


class TestDictEncoded:
    def test_basic_match(self):
        # dict: ["hello world", "foo", "bar"]
        vec = _dict_vec([0, 1, 0, 2], ["hello world", "foo", "bar"])
        assert _cs(vec, "world") == [True, False, True, False]

    def test_no_match(self):
        vec = _dict_vec([0, 1, 2], ["apple", "banana", "cherry"])
        assert _cs(vec, "mango") == [False, False, False]

    def test_all_match(self):
        vec = _dict_vec([0, 1, 0], ["abcdef", "xabc"])
        assert _cs(vec, "abc") == [True, True, True]

    def test_single_entry_dict(self):
        vec = _dict_vec([0, 0, 0, 0], ["hello world"])
        assert _cs(vec, "world") == [True, True, True, True]
        assert _cs(vec, "xyz") == [False, False, False, False]

    def test_case_insensitive(self):
        vec = _dict_vec([0, 1, 0], ["Hello World", "FOO BAR"])
        assert _ci(vec, "world") == [True, False, True]
        assert _ci(vec, "foo") == [False, True, False]

    def test_large_dict(self):
        # 256 entries — code width = 1 byte (full range)
        dictionary = [f"entry{i:03d}" for i in range(256)]
        codes = [i % 256 for i in range(512)]
        vec = _dict_vec(codes, dictionary)
        # "entry001" is in the dict, "entry001" appears at code 1 (indices 1, 257)
        result = _cs(vec, "entry001")
        assert result[1] is True
        assert result[0] is False


# ---------------------------------------------------------------------------
# Constant-encoded vectors
# ---------------------------------------------------------------------------


class TestConstantEncoded:
    def test_match(self):
        vec = _const_vec("hello world", 5)
        assert _cs(vec, "world") == [True, True, True, True, True]

    def test_no_match(self):
        vec = _const_vec("hello world", 3)
        assert _cs(vec, "xyz") == [False, False, False]

    def test_case_insensitive_match(self):
        vec = _const_vec("HELLO WORLD", 4)
        assert _ci(vec, "world") == [True, True, True, True]

    def test_case_insensitive_no_match(self):
        vec = _const_vec("HELLO WORLD", 2)
        assert _cs(vec, "world") == [False, False]  # CS: no match
        assert _ci(vec, "world") == [True, True]    # CI: match

    def test_single_row(self):
        vec = _const_vec("abc", 1)
        assert _cs(vec, "abc") == [True]
        assert _cs(vec, "xyz") == [False]

    def test_empty_rows(self):
        vec = _const_vec("anything", 0)
        assert _cs(vec, "any") == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
