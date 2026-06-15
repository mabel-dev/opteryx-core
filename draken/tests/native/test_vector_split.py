"""
Native correctness tests for Milestone E.16b: vector_split — pure nanobind C++.

Coverage:
  Basic correctness:
    single-char delimiter, no occurrence → single segment per row
    single-char delimiter, one occurrence → two segments
    single-char delimiter, multiple occurrences → N+1 segments
    consecutive delimiters → empty-string segments between them
    leading delimiter → empty-string first segment
    trailing delimiter → empty-string last segment
    empty input string → single empty-string segment
    delimiter is ord(0) (null byte)

  Null TVL:
    null input row → null output row (zero-length child slice)
    mixed null and non-null rows

  Long strings (>12 bytes — extern slot path):
    segment > 12 bytes goes to arena

  Output type:
    result type is DRAKEN_ARRAY
    child type is DRAKEN_VARCHAR
    parent length matches input length

  Edge cases:
    empty Vector input → empty DRAKEN_ARRAY
    single-row input
    delimiter not in any row → each row is a single-element list

  Error cases:
    delimiter out of range (negative) → exception
    delimiter out of range (> 255) → exception
    non-integer delimiter → exception
    non-string Vector input → TypeError
"""

import importlib.util
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
import draken.draken_native as dn

# ---------------------------------------------------------------------------
# Module loading
# ---------------------------------------------------------------------------

def _load_module(name, rel_path):
    base = os.path.join(os.path.dirname(__file__), "..", "..", "..", rel_path)
    import glob
    candidates = glob.glob(base + "*.so") + glob.glob(base + "*.pyd")
    if not candidates:
        raise FileNotFoundError(f"Compiled module not found: {base}*.so")
    spec = importlib.util.spec_from_file_location(name, candidates[0])
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_split = _load_module(
    "vector_split_native",
    "opteryx/compiled/nanobind/vector_split_native.cpython",
)


def vector_split(rows, delimiter):
    """Helper: build a VARCHAR Vector, call vector_split, decode to list of lists."""
    vec = dn.vector_from_string_sequence(
        [v.encode("utf-8") if isinstance(v, str) else v for v in rows]
    )
    result = _split.vector_split(vec, delimiter)
    return _decode_array(result)


def _decode_array(arr_vec):
    """Decode a DRAKEN_ARRAY Vector to a Python list of lists (or None for null rows)."""
    out = []
    for i in range(len(arr_vec)):
        row = arr_vec[i]
        out.append(row)  # draken_native returns None for null rows, list for valid rows
    return out


# ---------------------------------------------------------------------------
# Basic correctness
# ---------------------------------------------------------------------------

class TestVectorSplitBasic:
    def test_no_delimiter_occurrence(self):
        result = vector_split(["hello", "world"], ord(","))
        assert result == [["hello"], ["world"]]

    def test_one_occurrence(self):
        result = vector_split(["a,b"], ord(","))
        assert result == [["a", "b"]]

    def test_multiple_occurrences(self):
        result = vector_split(["a,b,c,d"], ord(","))
        assert result == [["a", "b", "c", "d"]]

    def test_consecutive_delimiters(self):
        result = vector_split(["a,,b"], ord(","))
        assert result == [["a", "", "b"]]

    def test_leading_delimiter(self):
        result = vector_split([",ab"], ord(","))
        assert result == [["", "ab"]]

    def test_trailing_delimiter(self):
        result = vector_split(["ab,"], ord(","))
        assert result == [["ab", ""]]

    def test_empty_string(self):
        result = vector_split([""], ord(","))
        assert result == [[""]]

    def test_delimiter_null_byte(self):
        vec = dn.vector_from_string_sequence([b"a\x00b", b"c"])
        result_vec = _split.vector_split(vec, 0)
        decoded = _decode_array(result_vec)
        assert decoded == [["a", "b"], ["c"]]

    def test_multiple_rows(self):
        result = vector_split(["a,b", "x,y,z", "solo"], ord(","))
        assert result == [["a", "b"], ["x", "y", "z"], ["solo"]]

    def test_delimiter_not_present_any_row(self):
        result = vector_split(["hello", "world", "foo"], ord("|"))
        assert result == [["hello"], ["world"], ["foo"]]

    def test_single_row(self):
        result = vector_split(["one,two"], ord(","))
        assert result == [["one", "two"]]


# ---------------------------------------------------------------------------
# Null TVL
# ---------------------------------------------------------------------------

class TestVectorSplitNullTVL:
    def test_single_null_row(self):
        result = vector_split([None], ord(","))
        assert result == [None]

    def test_null_first(self):
        result = vector_split([None, "a,b"], ord(","))
        assert result[0] is None
        assert result[1] == ["a", "b"]

    def test_null_last(self):
        result = vector_split(["a,b", None], ord(","))
        assert result[0] == ["a", "b"]
        assert result[1] is None

    def test_null_middle(self):
        result = vector_split(["x", None, "y,z"], ord(","))
        assert result[0] == ["x"]
        assert result[1] is None
        assert result[2] == ["y", "z"]

    def test_all_null(self):
        result = vector_split([None, None, None], ord(","))
        assert all(r is None for r in result)

    def test_mixed_null_non_null(self):
        rows = ["a,b", None, "c", None, "d,e,f"]
        result = vector_split(rows, ord(","))
        assert result[0] == ["a", "b"]
        assert result[1] is None
        assert result[2] == ["c"]
        assert result[3] is None
        assert result[4] == ["d", "e", "f"]


# ---------------------------------------------------------------------------
# Long strings (extern slot path, >12 bytes)
# ---------------------------------------------------------------------------

class TestVectorSplitLongStrings:
    def test_long_segment_single(self):
        # Segment "hello_world!!" is 14 bytes → extern slot
        result = vector_split(["hello_world!!,x"], ord(","))
        assert result == [["hello_world!!", "x"]]

    def test_long_segment_both(self):
        # Both segments are > 12 bytes
        result = vector_split(["abcdefghijklm,nopqrstuvwxyz"], ord(","))
        assert result == [["abcdefghijklm", "nopqrstuvwxyz"]]

    def test_long_string_no_delimiter(self):
        s = "a" * 50
        result = vector_split([s], ord(","))
        assert result == [[s]]

    def test_long_string_many_delimiters(self):
        parts = ["segment_" + str(i) for i in range(5)]
        combined = ",".join(parts)
        result = vector_split([combined], ord(","))
        assert result == [parts]


# ---------------------------------------------------------------------------
# Output type assertions
# ---------------------------------------------------------------------------

class TestVectorSplitOutputType:
    def test_result_type_is_array(self):
        vec = dn.vector_from_string_sequence([b"a,b"])
        result = _split.vector_split(vec, ord(","))
        assert result.type == dn.DrakenType.ARRAY

    def test_child_type_is_varchar(self):
        vec = dn.vector_from_string_sequence([b"a,b"])
        result = _split.vector_split(vec, ord(","))
        assert result.array_child_type == dn.DrakenType.VARCHAR

    def test_result_length_matches_input(self):
        rows = ["a,b", "c,d,e", "f"]
        vec = dn.vector_from_string_sequence([r.encode("utf-8") for r in rows])
        result = _split.vector_split(vec, ord(","))
        assert len(result) == len(rows)

    def test_empty_input_returns_empty_array(self):
        vec = dn.vector_from_string_sequence([])
        result = _split.vector_split(vec, ord(","))
        assert result.type == dn.DrakenType.ARRAY
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------

class TestVectorSplitErrors:
    def test_delimiter_negative(self):
        vec = dn.vector_from_string_sequence([b"a,b"])
        with pytest.raises(Exception):
            _split.vector_split(vec, -1)

    def test_delimiter_too_large(self):
        vec = dn.vector_from_string_sequence([b"a,b"])
        with pytest.raises(Exception):
            _split.vector_split(vec, 256)

    def test_delimiter_non_integer(self):
        vec = dn.vector_from_string_sequence([b"a,b"])
        with pytest.raises(Exception):
            _split.vector_split(vec, ",")

    def test_non_string_vector(self):
        vec = dn.vector_from_sequence([1, 2, 3])
        with pytest.raises(Exception):
            _split.vector_split(vec, ord(","))
