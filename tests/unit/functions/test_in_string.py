"""
Tests for the Draken-native vector_in_string and vector_in_string_case_insensitive.

New signature:
    vector_in_string(StringVector, str) -> BoolVector
    vector_in_string_case_insensitive(StringVector, str) -> BoolVector
"""

import os
import sys

import pyarrow as pa
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.vector_ops import vector_in_string, vector_in_string_case_insensitive
from opteryx.draken.interop.arrow import vector_from_arrow


def _vec(values):
    return vector_from_arrow(pa.array(values, type=pa.string()))


def _result(bool_vec):
    return bool_vec.to_pylist()


# ---------------------------------------------------------------------------
# vector_in_string — basic
# ---------------------------------------------------------------------------


class TestListInString:
    def test_basic_match(self):
        vec = _vec(["hello world", "foo bar", "baz"])
        assert _result(vector_in_string(vec, "world")) == [True, False, False]

    def test_no_match(self):
        vec = _vec(["hello", "world"])
        assert _result(vector_in_string(vec, "xyz")) == [False, False]

    def test_all_match(self):
        vec = _vec(["abc", "xabcy", "abc_end"])
        assert _result(vector_in_string(vec, "abc")) == [True, True, True]

    def test_needle_at_start(self):
        vec = _vec(["prefix_rest", "nothing_here"])
        assert _result(vector_in_string(vec, "prefix")) == [True, False]

    def test_needle_at_end(self):
        vec = _vec(["rest_suffix", "nothing_here"])
        assert _result(vector_in_string(vec, "suffix")) == [True, False]

    def test_single_char_needle(self):
        vec = _vec(["abc", "def", "ghi"])
        assert _result(vector_in_string(vec, "d")) == [False, True, False]

    def test_single_row_match(self):
        vec = _vec(["hello"])
        assert _result(vector_in_string(vec, "hell")) == [True]

    def test_single_row_no_match(self):
        vec = _vec(["hello"])
        assert _result(vector_in_string(vec, "world")) == [False]

    def test_empty_vector(self):
        vec = _vec([])
        assert _result(vector_in_string(vec, "x")) == []

    def test_empty_needle_returns_false_for_all(self):
        # Empty needle: no search performed, all False
        vec = _vec(["abc", "def"])
        assert _result(vector_in_string(vec, "")) == [False, False]

    def test_repeated_needle(self):
        vec = _vec(["abcabcabc"])
        assert _result(vector_in_string(vec, "abc")) == [True]

    def test_overlapping_pattern(self):
        vec = _vec(["aaaa"])
        assert _result(vector_in_string(vec, "aa")) == [True]

    def test_case_sensitive_no_match(self):
        vec = _vec(["HELLO WORLD"])
        assert _result(vector_in_string(vec, "world")) == [False]

    def test_special_chars(self):
        vec = _vec(["a.b", "a*b", "a+b"])
        assert _result(vector_in_string(vec, "a.b")) == [True, False, False]
        assert _result(vector_in_string(vec, "a*b")) == [False, True, False]

    def test_needle_longer_than_string(self):
        vec = _vec(["hi"])
        assert _result(vector_in_string(vec, "hello")) == [False]


# ---------------------------------------------------------------------------
# vector_in_string — null handling
# ---------------------------------------------------------------------------


class TestListInStringNulls:
    def test_null_row_produces_false(self):
        vec = _vec(["hello world", None, "goodbye"])
        result = _result(vector_in_string(vec, "world"))
        assert result == [True, False, False]

    def test_all_null(self):
        vec = _vec([None, None, None])
        assert _result(vector_in_string(vec, "x")) == [False, False, False]

    def test_null_among_matches(self):
        vec = _vec(["abc", None, "xabcx"])
        assert _result(vector_in_string(vec, "abc")) == [True, False, True]


# ---------------------------------------------------------------------------
# vector_in_string — unicode
# ---------------------------------------------------------------------------


class TestListInStringUnicode:
    def test_unicode_needle(self):
        vec = _vec(["café au lait", "plain text", "naïve approach"])
        assert _result(vector_in_string(vec, "café")) == [True, False, False]

    def test_multibyte_needle(self):
        vec = _vec(["日本語テスト", "hello"])
        assert _result(vector_in_string(vec, "テスト")) == [True, False]

    def test_emoji(self):
        vec = _vec(["hello 😀 world", "no emoji here"])
        assert _result(vector_in_string(vec, "😀")) == [True, False]


# ---------------------------------------------------------------------------
# vector_in_string_case_insensitive
# ---------------------------------------------------------------------------


class TestListInStringCaseInsensitive:
    def test_basic(self):
        vec = _vec(["Hello World", "FOO BAR", "baz"])
        assert _result(vector_in_string_case_insensitive(vec, "world")) == [True, False, False]

    def test_uppercase_needle(self):
        vec = _vec(["hello world", "foo"])
        assert _result(vector_in_string_case_insensitive(vec, "WORLD")) == [True, False]

    def test_mixed_case_needle(self):
        vec = _vec(["Hello World"])
        assert _result(vector_in_string_case_insensitive(vec, "hElLo")) == [True]

    def test_single_char(self):
        vec = _vec(["ABC", "def", "Ghi"])
        assert _result(vector_in_string_case_insensitive(vec, "A")) == [True, False, False]
        assert _result(vector_in_string_case_insensitive(vec, "a")) == [True, False, False]

    def test_null_produces_false(self):
        vec = _vec(["Hello World", None, "WORLD"])
        result = _result(vector_in_string_case_insensitive(vec, "world"))
        assert result == [True, False, True]

    def test_empty_vector(self):
        vec = _vec([])
        assert _result(vector_in_string_case_insensitive(vec, "x")) == []

    def test_differs_from_sensitive(self):
        vec = _vec(["HELLO", "Hello", "hello"])
        sensitive = _result(vector_in_string(vec, "hello"))
        insensitive = _result(vector_in_string_case_insensitive(vec, "hello"))
        assert sensitive == [False, False, True]
        assert insensitive == [True, True, True]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
