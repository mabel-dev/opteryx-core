"""
Native + parity tests for E.4: base64/85 codec via vector_codec consumer.

Loads the nanobind extension without triggering opteryx/__init__.py,
following the spec_from_file_location pattern established in E.2/E.3.

Coverage:
  base64 encode / decode:
    round-trip (decode(encode(x)) == x)
    known fixtures matching stdlib base64.b64encode / b64decode
    null TVL: null input row → null output row
    empty string → empty string (not null)
    multibyte UTF-8 round-trip
    all-null vector, mixed-null vector
    TypeError on non-DRAKEN_VARCHAR input

  base85 encode / decode:
    round-trip
    known fixtures matching stdlib base64.b85encode / b85decode
    null TVL, empty string, multibyte UTF-8
    TypeError on non-DRAKEN_VARCHAR input
"""

import base64 as stdlib_base64
import glob
import importlib.util
import os

import draken.draken_native as dn
import pytest


# ---------------------------------------------------------------------------
# Load vector_codec extension
# ---------------------------------------------------------------------------

def _load_vector_codec():
    pattern = os.path.join(
        os.path.dirname(__file__), "..", "..", "..",
        "opteryx", "compiled", "nanobind", "vector_codec*.so"
    )
    matches = glob.glob(pattern)
    if not matches:
        pytest.skip("vector_codec extension not built — run make compile first", allow_module_level=True)
    spec = importlib.util.spec_from_file_location(
        "opteryx.compiled.nanobind.vector_codec", matches[0]
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


vc = _load_vector_codec()


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------

def make_string_vec(values):
    """Build a DRAKEN_VARCHAR Vector from a list[str | None]."""
    return dn.vector_from_string_sequence(values)


def extract_string_vec(vec):
    """Extract list[str | None] from a DRAKEN_VARCHAR Vector."""
    return [vec[i] for i in range(len(vec))]


# ---------------------------------------------------------------------------
# BASE64 ENCODE
# ---------------------------------------------------------------------------

class TestBase64Encode:

    def test_known_fixtures(self):
        # RFC 4648 test vectors.
        inputs  = ["", "f", "fo", "foo", "foob", "fooba", "foobar"]
        expects = ["", "Zg==", "Zm8=", "Zm9v", "Zm9vYg==", "Zm9vYmE=", "Zm9vYmFy"]
        vec = make_string_vec(inputs)
        out = vc.vector_base64_encode(vec)
        result = extract_string_vec(out)
        for got, exp in zip(result, expects):
            assert got == exp, f"{got!r} != {exp!r}"

    def test_matches_stdlib(self):
        data = ["hello", "world", "abc", "x" * 13, "\xff\x00\xab"]
        vec = make_string_vec(data)
        out = vc.vector_base64_encode(vec)
        result = extract_string_vec(out)
        for raw, got in zip(data, result):
            exp = stdlib_base64.b64encode(raw.encode()).decode()
            assert got == exp

    def test_empty_string_is_not_null(self):
        vec = make_string_vec([""])
        out = vc.vector_base64_encode(vec)
        assert len(out) == 1
        val = out[0]
        assert val == ""

    def test_null_row_produces_null(self):
        vec = make_string_vec([None, "hello", None])
        out = vc.vector_base64_encode(vec)
        result = extract_string_vec(out)
        assert result[0] is None
        assert result[1] == stdlib_base64.b64encode(b"hello").decode()
        assert result[2] is None

    def test_all_null_vector(self):
        vec = make_string_vec([None, None, None])
        out = vc.vector_base64_encode(vec)
        assert all(v is None for v in extract_string_vec(out))

    def test_multibyte_utf8_roundtrip(self):
        # Japanese, Arabic, emoji — opaque bytes round-trip through encode+decode.
        texts = ["こんにちは", "مرحبا", "🎉🚀", "日本語テスト"]
        vec = make_string_vec(texts)
        encoded = vc.vector_base64_encode(vec)
        decoded = vc.vector_base64_decode(encoded)
        result = extract_string_vec(decoded)
        for orig, got in zip(texts, result):
            assert got == orig

    def test_type_error_on_non_string_vector(self):
        int_vec = dn.vector_from_sequence([1, 2, 3])
        with pytest.raises(TypeError):
            vc.vector_base64_encode(int_vec)

    def test_type_error_on_non_vector(self):
        with pytest.raises(TypeError):
            vc.vector_base64_encode("not a vector")


# ---------------------------------------------------------------------------
# BASE64 DECODE
# ---------------------------------------------------------------------------

class TestBase64Decode:

    def test_known_fixtures(self):
        encoded = ["", "Zg==", "Zm8=", "Zm9v", "Zm9vYg==", "Zm9vYmE=", "Zm9vYmFy"]
        expects = ["", "f", "fo", "foo", "foob", "fooba", "foobar"]
        vec = make_string_vec(encoded)
        out = vc.vector_base64_decode(vec)
        result = extract_string_vec(out)
        for got, exp in zip(result, expects):
            assert got == exp, f"{got!r} != {exp!r}"

    def test_matches_stdlib(self):
        raws = ["hello", "world", "abc", "x" * 13]
        encoded = [stdlib_base64.b64encode(r.encode()).decode() for r in raws]
        vec = make_string_vec(encoded)
        out = vc.vector_base64_decode(vec)
        result = extract_string_vec(out)
        for raw, got in zip(raws, result):
            assert got == raw

    def test_null_row_produces_null(self):
        vec = make_string_vec([None, "aGVsbG8=", None])
        out = vc.vector_base64_decode(vec)
        result = extract_string_vec(out)
        assert result[0] is None
        assert result[1] == "hello"
        assert result[2] is None

    def test_empty_string_is_not_null(self):
        vec = make_string_vec([""])
        out = vc.vector_base64_decode(vec)
        assert out[0] == ""


# ---------------------------------------------------------------------------
# BASE64 ROUND-TRIP
# ---------------------------------------------------------------------------

class TestBase64RoundTrip:

    def test_single_byte_lengths(self):
        for n in range(0, 50):
            raw = "a" * n
            vec = make_string_vec([raw])
            enc = vc.vector_base64_encode(vec)
            dec = vc.vector_base64_decode(enc)
            assert dec[0] == raw, f"round-trip failed at length {n}"

    def test_high_byte_data(self):
        # Bytes 0x00–0xFF as a raw sequence (encoded as Latin-1 string).
        raw = "".join(chr(i) for i in range(256))
        vec = make_string_vec([raw])
        enc = vc.vector_base64_encode(vec)
        dec = vc.vector_base64_decode(enc)
        assert dec[0] == raw

    def test_batch_roundtrip(self):
        inputs = ["", "a", "ab", "abc", "hello world", "x" * 100, "y" * 1000]
        vec = make_string_vec(inputs)
        enc = vc.vector_base64_encode(vec)
        dec = vc.vector_base64_decode(enc)
        result = extract_string_vec(dec)
        for orig, got in zip(inputs, result):
            assert got == orig


# ---------------------------------------------------------------------------
# BASE85 ENCODE
# ---------------------------------------------------------------------------

class TestBase85Encode:

    def test_known_fixture_hello_world(self):
        raw = "hello world"
        vec = make_string_vec([raw])
        out = vc.vector_base85_encode(vec)
        exp = stdlib_base64.b85encode(raw.encode()).decode()
        assert out[0] == exp

    def test_matches_stdlib(self):
        data = ["hello", "world", "abc", "x" * 12, "\xff\x00\xab\xcd"]
        vec = make_string_vec(data)
        out = vc.vector_base85_encode(vec)
        result = extract_string_vec(out)
        for raw, got in zip(data, result):
            exp = stdlib_base64.b85encode(raw.encode()).decode()
            assert got == exp

    def test_empty_string_is_not_null(self):
        vec = make_string_vec([""])
        out = vc.vector_base85_encode(vec)
        assert out[0] == ""

    def test_null_row_produces_null(self):
        vec = make_string_vec([None, "hello", None])
        out = vc.vector_base85_encode(vec)
        result = extract_string_vec(out)
        assert result[0] is None
        assert result[1] == stdlib_base64.b85encode(b"hello").decode()
        assert result[2] is None

    def test_multibyte_utf8_roundtrip(self):
        texts = ["こんにちは", "مرحبا", "🎉", "日本語テスト"]
        vec = make_string_vec(texts)
        encoded = vc.vector_base85_encode(vec)
        decoded = vc.vector_base85_decode(encoded)
        for orig, got in zip(texts, extract_string_vec(decoded)):
            assert got == orig

    def test_type_error_on_non_string_vector(self):
        int_vec = dn.vector_from_sequence([1, 2, 3])
        with pytest.raises(TypeError):
            vc.vector_base85_encode(int_vec)

    def test_type_error_on_non_vector(self):
        with pytest.raises(TypeError):
            vc.vector_base85_encode(42)


# ---------------------------------------------------------------------------
# BASE85 DECODE
# ---------------------------------------------------------------------------

class TestBase85Decode:

    def test_known_fixture_hello_world(self):
        encoded = stdlib_base64.b85encode(b"hello world").decode()
        vec = make_string_vec([encoded])
        out = vc.vector_base85_decode(vec)
        assert out[0] == "hello world"

    def test_matches_stdlib(self):
        raws = ["hello", "world", "abc", "x" * 12]
        encoded = [stdlib_base64.b85encode(r.encode()).decode() for r in raws]
        vec = make_string_vec(encoded)
        out = vc.vector_base85_decode(vec)
        result = extract_string_vec(out)
        for raw, got in zip(raws, result):
            assert got == raw

    def test_null_row_produces_null(self):
        enc_hello = stdlib_base64.b85encode(b"hello").decode()
        vec = make_string_vec([None, enc_hello, None])
        out = vc.vector_base85_decode(vec)
        result = extract_string_vec(out)
        assert result[0] is None
        assert result[1] == "hello"
        assert result[2] is None

    def test_empty_string_is_not_null(self):
        vec = make_string_vec([""])
        out = vc.vector_base85_decode(vec)
        assert out[0] == ""


# ---------------------------------------------------------------------------
# BASE85 ROUND-TRIP
# ---------------------------------------------------------------------------

class TestBase85RoundTrip:

    def test_lengths_0_to_49(self):
        for n in range(0, 50):
            raw = "a" * n
            vec = make_string_vec([raw])
            enc = vc.vector_base85_encode(vec)
            dec = vc.vector_base85_decode(enc)
            assert dec[0] == raw, f"round-trip failed at length {n}"

    def test_batch_roundtrip(self):
        inputs = ["", "a", "ab", "abc", "abcd", "hello world", "x" * 100]
        vec = make_string_vec(inputs)
        enc = vc.vector_base85_encode(vec)
        dec = vc.vector_base85_decode(enc)
        result = extract_string_vec(dec)
        for orig, got in zip(inputs, result):
            assert got == orig
