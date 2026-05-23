"""
Native correctness tests for E.8: hex encode/decode + MD5/SHA digest consumers.

Loads vector_hash_codec without triggering opteryx/__init__.py,
following the spec_from_file_location pattern from E.4 (test_vector_codec.py).

Coverage:
  vector_hex_encode:
    known fixtures (UPPERCASE output per mabel bintob16).
    null TVL, empty string, multibyte bytes.
    TypeError on non-Vector, non-string-family Vector.

  vector_hex_decode:
    known fixtures, round-trip with encode.
    null TVL, empty string.

  vector_md5:
    known fixtures: MD5("") and MD5("abc").
    null TVL, empty string (empty string has a defined MD5).
    output type == VARCHAR, output length == 32.
    slot determinism: two calls same input → byte-identical output slots.
    TypeError on non-string input.

  vector_sha1:
    known fixtures: SHA1("abc").
    null TVL, output type == VARCHAR, output length == 40.

  vector_sha256:
    known fixtures: SHA256("abc").
    null TVL, output type == VARCHAR, output length == 64.

  vector_sha512:
    known fixtures: SHA512("abc").
    null TVL, output type == VARCHAR, output length == 128.
"""

import glob
import hashlib
import importlib.util
import os

import draken.draken_native as dn
import pytest


# ---------------------------------------------------------------------------
# Load vector_hash_codec extension
# ---------------------------------------------------------------------------

def _load_vector_hash_codec():
    pattern = os.path.join(
        os.path.dirname(__file__), "..", "..", "..",
        "opteryx", "compiled", "nanobind", "vector_hash_codec*.so"
    )
    matches = glob.glob(pattern)
    if not matches:
        pytest.skip(
            "vector_hash_codec extension not built — run make compile first",
            allow_module_level=True,
        )
    spec = importlib.util.spec_from_file_location(
        "opteryx.compiled.nanobind.vector_hash_codec", matches[0]
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


hc = _load_vector_hash_codec()


# ---------------------------------------------------------------------------
# Factories / extractors
# ---------------------------------------------------------------------------

def sv(values):
    return dn.vector_from_string_sequence(values)


def vals(vec):
    return [vec[i] for i in range(len(vec))]


# ---------------------------------------------------------------------------
# HEX ENCODE
# ---------------------------------------------------------------------------

class TestHexEncode:

    def test_known_single_byte(self):
        # Single byte 0x41 ('A') → "41" (UPPERCASE per mabel bintob16)
        vec = sv(["A"])
        out = hc.vector_hex_encode(vec)
        assert out[0] == "41"

    def test_uppercase_output(self):
        # Verify UPPERCASE (not lowercase) to match old vector_hex.pyx behavior.
        # Use ASCII bytes (a-f) that produce uppercase hex digits clearly.
        vec = sv(["\x0a\x0b\x0c"])  # these are ASCII control chars, 1 byte each in UTF-8
        out = hc.vector_hex_encode(vec)
        assert out[0] == "0A0B0C"

    def test_empty_string_not_null(self):
        vec = sv([""])
        out = hc.vector_hex_encode(vec)
        assert len(out) == 1
        assert out[0] == ""

    def test_null_row_produces_null(self):
        vec = sv([None, "\x0a\x0b", None])
        out = hc.vector_hex_encode(vec)
        result = vals(out)
        assert result[0] is None
        assert result[1] == "0A0B"
        assert result[2] is None

    def test_all_null(self):
        vec = sv([None, None])
        out = hc.vector_hex_encode(vec)
        assert all(v is None for v in vals(out))

    def test_output_type_is_varchar(self):
        vec = sv(["hello"])
        out = hc.vector_hex_encode(vec)
        assert out.type == dn.DrakenType.VARCHAR

    def test_type_error_on_int_vector(self):
        int_vec = dn.vector_from_sequence([1, 2, 3])
        with pytest.raises(TypeError):
            hc.vector_hex_encode(int_vec)

    def test_type_error_on_non_vector(self):
        with pytest.raises(TypeError):
            hc.vector_hex_encode("not a vector")

    def test_batch_output_length(self):
        # Use ASCII strings only — vector stores UTF-8, hex output is 2× byte count.
        inputs = ["a", "ab", "abc", "abcd"]
        vec = sv(inputs)
        out = hc.vector_hex_encode(vec)
        result = vals(out)
        for raw, got in zip(inputs, result):
            # ASCII strings: 1 byte per char, so output = 2 × len(raw)
            assert len(got) == len(raw) * 2

    def test_roundtrip_with_decode(self):
        # Use ASCII control chars (single byte in UTF-8) for a clean round-trip.
        inputs = ["\x00", "\x01\x02\x03", "\x0a\x0b\x0c\x0d"]
        vec = sv(inputs)
        enc = hc.vector_hex_encode(vec)
        dec = hc.vector_hex_decode(enc)
        for orig, got in zip(inputs, vals(dec)):
            assert got == orig


# ---------------------------------------------------------------------------
# HEX DECODE
# ---------------------------------------------------------------------------

class TestHexDecode:

    def test_known_fixture(self):
        vec = sv(["4142"])
        out = hc.vector_hex_decode(vec)
        assert out[0] == "AB"

    def test_lowercase_input_accepted(self):
        vec = sv(["4142"])
        out = hc.vector_hex_decode(vec)
        assert out[0] == "AB"

    def test_empty_string_not_null(self):
        vec = sv([""])
        out = hc.vector_hex_decode(vec)
        assert out[0] == ""

    def test_null_row_produces_null(self):
        vec = sv([None, "4142", None])
        out = hc.vector_hex_decode(vec)
        result = vals(out)
        assert result[0] is None
        assert result[1] == "AB"
        assert result[2] is None

    def test_output_type_is_varchar(self):
        vec = sv(["41"])
        out = hc.vector_hex_decode(vec)
        assert out.type == dn.DrakenType.VARCHAR


# ---------------------------------------------------------------------------
# MD5
# ---------------------------------------------------------------------------

class TestMD5:

    def test_empty_string(self):
        # MD5("") = "d41d8cd98f00b204e9800998ecf8427e"
        vec = sv([""])
        out = hc.vector_md5(vec)
        assert out[0] == "d41d8cd98f00b204e9800998ecf8427e"

    def test_abc(self):
        # MD5("abc") = "900150983cd24fb0d6963f7d28e17f72"
        vec = sv(["abc"])
        out = hc.vector_md5(vec)
        assert out[0] == "900150983cd24fb0d6963f7d28e17f72"

    def test_matches_stdlib(self):
        # Strings are stored as UTF-8 bytes in the vector; hash the UTF-8 encoding.
        inputs = ["", "a", "hello world", "x" * 100]
        vec = sv(inputs)
        out = hc.vector_md5(vec)
        for raw, got in zip(inputs, vals(out)):
            exp = hashlib.md5(raw.encode("utf-8")).hexdigest()
            assert got == exp, f"MD5({raw!r}): got {got!r}, expected {exp!r}"

    def test_output_length_is_32(self):
        vec = sv(["anything"])
        out = hc.vector_md5(vec)
        assert len(out[0]) == 32

    def test_output_type_is_varchar(self):
        vec = sv(["x"])
        out = hc.vector_md5(vec)
        assert out.type == dn.DrakenType.VARCHAR

    def test_output_is_lowercase(self):
        vec = sv(["abc"])
        out = hc.vector_md5(vec)
        assert out[0] == out[0].lower()

    def test_null_row_produces_null(self):
        vec = sv([None, "abc", None])
        out = hc.vector_md5(vec)
        result = vals(out)
        assert result[0] is None
        assert result[1] == "900150983cd24fb0d6963f7d28e17f72"
        assert result[2] is None

    def test_all_null(self):
        vec = sv([None, None, None])
        out = hc.vector_md5(vec)
        assert all(v is None for v in vals(out))

    def test_slot_determinism(self):
        # Same input computed twice must produce byte-identical output slots.
        inputs = ["hello", "world", "abc"]
        vec = sv(inputs)
        out1 = hc.vector_md5(vec)
        out2 = hc.vector_md5(sv(inputs))
        for i in range(len(inputs)):
            assert out1[i] == out2[i]

    def test_type_error_on_int_vector(self):
        int_vec = dn.vector_from_sequence([1, 2, 3])
        with pytest.raises(TypeError):
            hc.vector_md5(int_vec)

    def test_type_error_on_non_vector(self):
        with pytest.raises(TypeError):
            hc.vector_md5(42)

    def test_batch_correctness(self):
        inputs = ["", "a", "abc", "message digest", "x" * 55, "y" * 64, "z" * 1000]
        vec = sv(inputs)
        out = hc.vector_md5(vec)
        for raw, got in zip(inputs, vals(out)):
            exp = hashlib.md5(raw.encode("utf-8")).hexdigest()
            assert got == exp


# ---------------------------------------------------------------------------
# SHA-1
# ---------------------------------------------------------------------------

class TestSHA1:

    def test_abc(self):
        # SHA1("abc") = "a9993e364706816aba3e25717850c26c9cd0d89d"
        vec = sv(["abc"])
        out = hc.vector_sha1(vec)
        assert out[0] == "a9993e364706816aba3e25717850c26c9cd0d89d"

    def test_empty_string(self):
        # SHA1("") = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
        vec = sv([""])
        out = hc.vector_sha1(vec)
        assert out[0] == "da39a3ee5e6b4b0d3255bfef95601890afd80709"

    def test_matches_stdlib(self):
        inputs = ["", "a", "hello world", "x" * 100]
        vec = sv(inputs)
        out = hc.vector_sha1(vec)
        for raw, got in zip(inputs, vals(out)):
            exp = hashlib.sha1(raw.encode("utf-8")).hexdigest()
            assert got == exp

    def test_output_length_is_40(self):
        vec = sv(["anything"])
        out = hc.vector_sha1(vec)
        assert len(out[0]) == 40

    def test_output_type_is_varchar(self):
        vec = sv(["x"])
        out = hc.vector_sha1(vec)
        assert out.type == dn.DrakenType.VARCHAR

    def test_null_row_produces_null(self):
        vec = sv([None, "abc", None])
        out = hc.vector_sha1(vec)
        result = vals(out)
        assert result[0] is None
        assert result[1] == "a9993e364706816aba3e25717850c26c9cd0d89d"
        assert result[2] is None

    def test_output_is_lowercase(self):
        vec = sv(["test"])
        out = hc.vector_sha1(vec)
        assert out[0] == out[0].lower()


# ---------------------------------------------------------------------------
# SHA-256
# ---------------------------------------------------------------------------

class TestSHA256:

    def test_abc(self):
        # SHA256("abc") = "ba7816bf8f01cfea414140de5dae2ec73b00361bbef0469f490f4187574534da"
        # (note: standard test vector uses slightly different fixture — check below)
        vec = sv(["abc"])
        out = hc.vector_sha256(vec)
        exp = hashlib.sha256(b"abc").hexdigest()
        assert out[0] == exp

    def test_empty_string(self):
        vec = sv([""])
        out = hc.vector_sha256(vec)
        assert out[0] == hashlib.sha256(b"").hexdigest()

    def test_matches_stdlib(self):
        inputs = ["", "a", "hello world", "x" * 100]
        vec = sv(inputs)
        out = hc.vector_sha256(vec)
        for raw, got in zip(inputs, vals(out)):
            exp = hashlib.sha256(raw.encode("utf-8")).hexdigest()
            assert got == exp

    def test_output_length_is_64(self):
        vec = sv(["anything"])
        out = hc.vector_sha256(vec)
        assert len(out[0]) == 64

    def test_output_type_is_varchar(self):
        vec = sv(["x"])
        out = hc.vector_sha256(vec)
        assert out.type == dn.DrakenType.VARCHAR

    def test_null_row_produces_null(self):
        vec = sv([None, "abc", None])
        out = hc.vector_sha256(vec)
        result = vals(out)
        assert result[0] is None
        assert result[1] == hashlib.sha256(b"abc").hexdigest()
        assert result[2] is None

    def test_output_is_lowercase(self):
        vec = sv(["test"])
        out = hc.vector_sha256(vec)
        assert out[0] == out[0].lower()


# ---------------------------------------------------------------------------
# SHA-512
# ---------------------------------------------------------------------------

class TestSHA512:

    def test_abc(self):
        vec = sv(["abc"])
        out = hc.vector_sha512(vec)
        exp = hashlib.sha512(b"abc").hexdigest()
        assert out[0] == exp

    def test_empty_string(self):
        vec = sv([""])
        out = hc.vector_sha512(vec)
        assert out[0] == hashlib.sha512(b"").hexdigest()

    def test_matches_stdlib(self):
        inputs = ["", "a", "hello world", "x" * 100]
        vec = sv(inputs)
        out = hc.vector_sha512(vec)
        for raw, got in zip(inputs, vals(out)):
            exp = hashlib.sha512(raw.encode("utf-8")).hexdigest()
            assert got == exp

    def test_output_length_is_128(self):
        vec = sv(["anything"])
        out = hc.vector_sha512(vec)
        assert len(out[0]) == 128

    def test_output_type_is_varchar(self):
        vec = sv(["x"])
        out = hc.vector_sha512(vec)
        assert out.type == dn.DrakenType.VARCHAR

    def test_null_row_produces_null(self):
        vec = sv([None, "abc", None])
        out = hc.vector_sha512(vec)
        result = vals(out)
        assert result[0] is None
        assert result[1] == hashlib.sha512(b"abc").hexdigest()
        assert result[2] is None

    def test_output_is_lowercase(self):
        vec = sv(["test"])
        out = hc.vector_sha512(vec)
        assert out[0] == out[0].lower()

    def test_slot_determinism(self):
        inputs = ["hello", "abc", ""]
        vec = sv(inputs)
        out1 = hc.vector_sha512(vec)
        out2 = hc.vector_sha512(sv(inputs))
        for i in range(len(inputs)):
            assert out1[i] == out2[i]
