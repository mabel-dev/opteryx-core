"""
Native + parity tests for E.6: string length/emptiness + array element count
via the vector_accessors nanobind consumer.
Updated E.7: vector_string_length now propagates nulls (null input → None output).
NVARCHAR codepoint-length and VARBINARY byte-length dispatch verified.

Loads the extension without triggering opteryx/__init__.py, following the
spec_from_file_location pattern established in E.2–E.5.

Coverage:
  vector_string_length:
    known ASCII byte counts, multibyte UTF-8 byte counts (not codepoint counts)
    empty string → 0, null row → None (SQL 3VL: LENGTH(NULL) = NULL)
    TypeError on non-string input

  vector_string_is_empty / vector_string_is_not_empty:
    empty → True / False, non-empty → False / True
    null row → null in output (validity propagated)
    empty-string row vs null row distinguishable
    TypeError on non-string input

  vector_length:
    per-row array element count for lists of varying lengths
    empty list → 0, null row → 0, no output null bitmap
    TypeError on non-ARRAY input
"""

import glob
import importlib.util
import os

import draken.draken_native as dn
import pytest


# ---------------------------------------------------------------------------
# Load vector_accessors extension
# ---------------------------------------------------------------------------

def _load_vector_accessors():
    pattern = os.path.join(
        os.path.dirname(__file__), "..", "..", "..",
        "opteryx", "compiled", "nanobind", "vector_accessors*.so"
    )
    matches = glob.glob(pattern)
    if not matches:
        raise RuntimeError(
            "vector_accessors extension not built — run make compile first"
        )
    spec = importlib.util.spec_from_file_location(
        "opteryx.compiled.nanobind.vector_accessors", matches[0]
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


va = _load_vector_accessors()


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------

def make_str(values):
    return dn.vector_from_string_sequence(
        [v.encode("utf-8") if isinstance(v, str) else v for v in values]
    )


def make_nvarchar(values):
    return dn.vector_from_nvarchar_sequence(
        [v.encode("utf-8") if isinstance(v, str) else v for v in values]
    )


def make_bytes(values):
    return dn.vector_from_bytes_sequence(values)


def make_arr(values):
    return dn.vector_array_from_sequence(values)


def make_int64(values):
    return dn.vector_from_sequence(values)


def extract_int64(vec):
    return [vec[i] for i in range(len(vec))]


def extract_bool(vec):
    return [vec[i] for i in range(len(vec))]


# ---------------------------------------------------------------------------
# vector_string_length
# ---------------------------------------------------------------------------

class TestVectorStringLength:

    def test_ascii_byte_counts(self):
        vec = make_str(["", "a", "ab", "abc", "hello"])
        out = va.vector_string_length(vec)
        assert extract_int64(out) == [0, 1, 2, 3, 5]

    def test_multibyte_utf8_byte_count_not_codepoint(self):
        # "é" = U+00E9 = 2 UTF-8 bytes
        # "中" = U+4E2D = 3 UTF-8 bytes
        # "🎉" = U+1F389 = 4 UTF-8 bytes
        vec = make_str(["é", "中", "🎉"])
        out = va.vector_string_length(vec)
        result = extract_int64(out)
        assert result[0] == 2
        assert result[1] == 3
        assert result[2] == 4

    def test_empty_string_returns_zero(self):
        vec = make_str([""])
        out = va.vector_string_length(vec)
        assert extract_int64(out) == [0]

    def test_null_row_propagates_null(self):
        vec = make_str([None, "hello", None])
        out = va.vector_string_length(vec)
        # Null rows propagate: SQL 3VL LENGTH(NULL) = NULL.
        assert out[0] is None
        assert out[1] == 5
        assert out[2] is None

    def test_all_null_vector(self):
        vec = make_str([None, None])
        out = va.vector_string_length(vec)
        assert out[0] is None
        assert out[1] is None

    def test_long_string_over_12_bytes(self):
        # Forces extern slot path (>12 bytes → arena allocation).
        s = "hello world!!"  # 13 bytes
        vec = make_str([s])
        out = va.vector_string_length(vec)
        assert extract_int64(out) == [13]

    def test_type_error_on_non_string_input(self):
        vec = make_int64([1, 2, 3])
        with pytest.raises((TypeError, RuntimeError)):
            va.vector_string_length(vec)

    def test_empty_vector(self):
        vec = make_str([])
        out = va.vector_string_length(vec)
        assert len(out) == 0

    def test_null_tvl_mixed_valid_and_null(self):
        vec = make_str(["hi", None, "café", None, ""])
        out = va.vector_string_length(vec)
        assert out[0] == 2      # "hi" = 2 bytes
        assert out[1] is None   # null → null
        assert out[2] == 5      # "café" = 5 UTF-8 bytes
        assert out[3] is None   # null → null
        assert out[4] == 0      # "" = 0 bytes (non-null)

    def test_null_tvl_all_valid_no_validity_alloc(self):
        # All-valid input must produce all-valid output (no validity bitmap wasted).
        vec = make_str(["a", "bc", "def"])
        out = va.vector_string_length(vec)
        assert out[0] == 1
        assert out[1] == 2
        assert out[2] == 3
        # No nulls in output.
        assert out[0] is not None
        assert out[1] is not None
        assert out[2] is not None


# ---------------------------------------------------------------------------
# vector_string_is_empty
# ---------------------------------------------------------------------------

class TestVectorStringIsEmpty:

    def test_empty_string_true(self):
        vec = make_str([""])
        out = va.vector_string_is_empty(vec)
        assert out[0] is True

    def test_non_empty_string_false(self):
        vec = make_str(["a", "hello", " "])
        out = va.vector_string_is_empty(vec)
        assert out[0] is False
        assert out[1] is False
        assert out[2] is False

    def test_null_row_produces_null(self):
        vec = make_str([None, "", "x", None])
        out = va.vector_string_is_empty(vec)
        assert out[0] is None   # null → null
        assert out[1] is True   # "" → True
        assert out[2] is False  # "x" → False
        assert out[3] is None   # null → null

    def test_empty_string_distinct_from_null(self):
        vec = make_str([None, ""])
        out = va.vector_string_is_empty(vec)
        assert out[0] is None   # null
        assert out[1] is True   # empty string is not null, it IS empty

    def test_all_null_vector(self):
        vec = make_str([None, None, None])
        out = va.vector_string_is_empty(vec)
        assert all(v is None for v in extract_bool(out))

    def test_mixed_lengths(self):
        vec = make_str(["", "a", "ab", ""])
        out = va.vector_string_is_empty(vec)
        assert extract_bool(out) == [True, False, False, True]

    def test_multibyte_utf8_is_not_empty(self):
        vec = make_str(["é", "中"])
        out = va.vector_string_is_empty(vec)
        assert out[0] is False
        assert out[1] is False

    def test_type_error_on_non_string_input(self):
        vec = make_int64([1, 2, 3])
        with pytest.raises((TypeError, RuntimeError)):
            va.vector_string_is_empty(vec)

    def test_empty_vector(self):
        vec = make_str([])
        out = va.vector_string_is_empty(vec)
        assert len(out) == 0

    def test_bit_boundary_crossing(self):
        # Force bit-packing boundary (9 rows crosses byte boundary at 8).
        rows = [""] * 4 + ["x"] * 4 + [""]
        vec = make_str(rows)
        out = va.vector_string_is_empty(vec)
        result = extract_bool(out)
        assert result[:4] == [True, True, True, True]
        assert result[4:8] == [False, False, False, False]
        assert result[8] is True


# ---------------------------------------------------------------------------
# vector_string_is_not_empty
# ---------------------------------------------------------------------------

class TestVectorStringIsNotEmpty:

    def test_empty_string_false(self):
        vec = make_str([""])
        out = va.vector_string_is_not_empty(vec)
        assert out[0] is False

    def test_non_empty_string_true(self):
        vec = make_str(["a", "hello", " "])
        out = va.vector_string_is_not_empty(vec)
        assert out[0] is True
        assert out[1] is True
        assert out[2] is True

    def test_null_row_produces_null(self):
        vec = make_str([None, "", "x", None])
        out = va.vector_string_is_not_empty(vec)
        assert out[0] is None
        assert out[1] is False
        assert out[2] is True
        assert out[3] is None

    def test_inverse_of_is_empty(self):
        rows = [None, "", "a", "中", None, ""]
        vec = make_str(rows)
        empty = extract_bool(va.vector_string_is_empty(vec))
        not_empty = extract_bool(va.vector_string_is_not_empty(vec))
        for e, ne in zip(empty, not_empty):
            if e is None:
                assert ne is None
            else:
                assert ne == (not e)

    def test_type_error_on_non_string_input(self):
        vec = make_int64([1, 2, 3])
        with pytest.raises((TypeError, RuntimeError)):
            va.vector_string_is_not_empty(vec)


# ---------------------------------------------------------------------------
# vector_length (array element count)
# ---------------------------------------------------------------------------

class TestVectorLength:

    def test_basic_element_counts(self):
        vec = make_arr([[1, 2, 3], [4, 5], [6]])
        out = va.vector_length(vec)
        assert extract_int64(out) == [3, 2, 1]

    def test_empty_list_returns_zero(self):
        vec = make_arr([[], [1], []])
        out = va.vector_length(vec)
        assert extract_int64(out) == [0, 1, 0]

    def test_null_row_returns_zero_no_validity(self):
        vec = make_arr([None, [1, 2], None])
        out = va.vector_length(vec)
        result = extract_int64(out)
        assert result[0] == 0
        assert result[1] == 2
        assert result[2] == 0
        # No null propagation for vector_length (matches old .pyx)
        assert out[0] == 0   # not None
        assert out[2] == 0   # not None

    def test_all_null_vector(self):
        vec = make_arr([None, None])
        out = va.vector_length(vec)
        assert extract_int64(out) == [0, 0]

    def test_varying_lengths(self):
        vec = make_arr([[]] * 3 + [[1, 2, 3, 4, 5]])
        out = va.vector_length(vec)
        result = extract_int64(out)
        assert result[:3] == [0, 0, 0]
        assert result[3] == 5

    def test_type_error_on_non_array_input(self):
        vec = make_str(["a", "b"])
        with pytest.raises((TypeError, RuntimeError)):
            va.vector_length(vec)

    def test_type_error_on_int64_input(self):
        vec = make_int64([1, 2, 3])
        with pytest.raises((TypeError, RuntimeError)):
            va.vector_length(vec)

    def test_single_row(self):
        vec = make_arr([[10, 20, 30]])
        out = va.vector_length(vec)
        assert extract_int64(out) == [3]

    def test_empty_vector(self):
        vec = make_arr([])
        out = va.vector_length(vec)
        assert len(out) == 0


# ---------------------------------------------------------------------------
# E.7 — NVARCHAR: codepoint-length dispatch
# ---------------------------------------------------------------------------

class TestVectorStringLengthNvarchar:

    def test_ascii_codepoints_equal_bytes(self):
        # ASCII: codepoint count == byte count.
        vec = make_nvarchar(["hello", "a", ""])
        out = va.vector_string_length(vec)
        assert extract_int64(out) == [5, 1, 0]

    def test_cafe_codepoints(self):
        # "café" = 4 codepoints (c, a, f, é); 5 UTF-8 bytes.
        vec = make_nvarchar(["café"])
        out = va.vector_string_length(vec)
        assert out[0] == 4

    def test_japanese_codepoints(self):
        # "日本" = 2 codepoints; 6 UTF-8 bytes.
        vec = make_nvarchar(["日本"])
        out = va.vector_string_length(vec)
        assert out[0] == 2

    def test_emoji_codepoints(self):
        # "🎉" = 1 codepoint; 4 UTF-8 bytes.
        vec = make_nvarchar(["🎉"])
        out = va.vector_string_length(vec)
        assert out[0] == 1

    def test_mixed_codepoint_counts(self):
        # "café" → 4 cp, "日本" → 2 cp, "hello" → 5 cp.
        vec = make_nvarchar(["café", "日本", "hello"])
        out = va.vector_string_length(vec)
        assert extract_int64(out) == [4, 2, 5]

    def test_null_propagates(self):
        vec = make_nvarchar([None, "café", None])
        out = va.vector_string_length(vec)
        assert out[0] is None
        assert out[1] == 4
        assert out[2] is None

    def test_type_tag_is_nvarchar(self):
        vec = make_nvarchar(["hello"])
        assert vec.type == dn.DrakenType.NVARCHAR


# ---------------------------------------------------------------------------
# E.7 — VARBINARY: byte-length dispatch, bytes objects returned
# ---------------------------------------------------------------------------

class TestVectorStringLengthVarbinary:

    def test_byte_length_basic(self):
        # b"hello" = 5 bytes, b"\x00\x01\x02" = 3 bytes.
        vec = make_bytes([b"hello", b"\x00\x01\x02", b""])
        out = va.vector_string_length(vec)
        assert extract_int64(out) == [5, 3, 0]

    def test_multibyte_utf8_is_byte_length_not_codepoints(self):
        # b"café" as raw UTF-8 = 5 bytes (not 4 codepoints).
        vec = make_bytes([b"caf\xc3\xa9"])
        out = va.vector_string_length(vec)
        assert out[0] == 5

    def test_null_propagates(self):
        vec = make_bytes([None, b"abc", None])
        out = va.vector_string_length(vec)
        assert out[0] is None
        assert out[1] == 3
        assert out[2] is None

    def test_type_tag_is_varbinary(self):
        vec = make_bytes([b"hello"])
        assert vec.type == dn.DrakenType.VARBINARY

    def test_to_pylist_returns_bytes(self):
        vals = [b"hello", b"\x00\x01", None, b""]
        vec = make_bytes(vals)
        assert vec.to_pylist() == vals
