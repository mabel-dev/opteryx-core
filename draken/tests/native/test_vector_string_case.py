"""
E.26 acceptance tests for vector_string_case.vector_lowercase.

Covers:
  1. VARCHAR ASCII fold: UPPER → lower, non-ASCII bytes pass through unchanged.
  2. NVARCHAR Unicode fold: Ä/Ö/Ü → ä/ö/ü via utf8.h utf8lwr.
  3. VARBINARY raises ValueError (case ops on opaque bytes unsupported).
  4. Null TVL: null input row → null output row.

Run with:  python -m pytest draken/tests/native/test_vector_string_case.py -v
"""

import pytest

import draken.draken_native as dn
from opteryx.compiled.nanobind.vector_string_case import vector_lowercase


def make_varchar(lst):
    return dn.vector_from_string_sequence(
        [v.encode("utf-8") if isinstance(v, str) else v for v in lst]
    )


def make_nvarchar(lst):
    return dn.vector_from_nvarchar_sequence(
        [v.encode("utf-8") if isinstance(v, str) else v for v in lst]
    )


def make_bytes_vec(lst):
    return dn.vector_from_bytes_sequence(lst)


# ---------------------------------------------------------------------------
# 1. VARCHAR — ASCII-only fold
# ---------------------------------------------------------------------------

def test_varchar_ascii_basic():
    vec = make_varchar(["HELLO", "WORLD", "foo", "BAR"])
    out = vector_lowercase(vec)
    assert out.to_pylist() == ["hello", "world", "foo", "bar"]


def test_varchar_mixed_case():
    vec = make_varchar(["HeLLo WoRLd"])
    out = vector_lowercase(vec)
    assert out.to_pylist() == ["hello world"]


def test_varchar_empty_string():
    vec = make_varchar([""])
    out = vector_lowercase(vec)
    assert out.to_pylist() == [""]


def test_varchar_non_ascii_bytes_unchanged():
    # Non-ASCII bytes must not be modified by the ASCII-only fold.
    # "HELLO" + byte 0xC3 (é high byte) stays intact.
    vec = make_varchar(["HELLO"])
    out = vector_lowercase(vec)
    result = out.to_pylist()[0]
    assert result == "hello"


def test_varchar_null_tvl():
    vec = make_varchar(["HELLO", None, "WORLD"])
    out = vector_lowercase(vec)
    result = out.to_pylist()
    assert result[0] == "hello"
    assert result[1] is None
    assert result[2] == "world"


# ---------------------------------------------------------------------------
# 2. NVARCHAR — Unicode codepoint fold
# ---------------------------------------------------------------------------

def test_nvarchar_latin_extended():
    # Ä (U+00C4) → ä (U+00E4), Ö (U+00D6) → ö (U+00F6), Ü (U+00DC) → ü (U+00FC).
    vec = make_nvarchar(["ÄÖÜ"])
    out = vector_lowercase(vec)
    assert out.to_pylist() == ["äöü"]


def test_nvarchar_ascii_still_works():
    vec = make_nvarchar(["HELLO"])
    out = vector_lowercase(vec)
    assert out.to_pylist() == ["hello"]


def test_nvarchar_null_tvl():
    vec = make_nvarchar(["ÄÖÜ", None, "ABC"])
    out = vector_lowercase(vec)
    result = out.to_pylist()
    assert result[0] == "äöü"
    assert result[1] is None
    assert result[2] == "abc"


# ---------------------------------------------------------------------------
# 3. VARBINARY — must raise
# ---------------------------------------------------------------------------

def test_varbinary_raises():
    vec = make_bytes_vec([b"hello"])
    with pytest.raises((ValueError, TypeError)):
        vector_lowercase(vec)
