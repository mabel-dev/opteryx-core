import base64 as stdlib_base64
import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.third_party.mabel.base16 import decode, encode


def test_base16_encode_simple_string():
    data = b"hello world"
    assert encode(data) == stdlib_base64.b16encode(data)


def test_base16_decode_simple_string():
    encoded = stdlib_base64.b16encode(b"hello world")
    assert decode(encoded) == b"hello world"


def test_base16_encode_decode_roundtrip():
    data = b"x" * 13333336
    assert decode(encode(data)) == data


def test_base16_encode_empty():
    assert encode(b"") == b""


def test_base16_decode_empty():
    assert decode(b"") == b""


def test_base16_known_values():
    pairs = [
        (b"", b""),
        (b"f", b"66"),
        (b"fo", b"666F"),
        (b"foo", b"666F6F"),
        (b"foob", b"666F6F62"),
        (b"fooba", b"666F6F6261"),
        (b"foobar", b"666F6F626172"),
    ]
    for raw, expected in pairs:
        assert encode(raw) == expected
        assert decode(expected) == raw


def test_base16_decode_invalid_input_returns_empty():
    assert decode(b"ZZ") == b""
    assert decode(b"GG") == b""


def test_base16_decode_odd_length_returns_empty():
    assert decode(b"A") == b""
    assert decode(b"ABC") == b""


def test_base16_decode_accepts_lowercase():
    assert decode(b"666f6f") == b"foo"


def test_base16_decode_accepts_mixed_case():
    assert decode(b"666F6f") == b"foo"


def test_base16_length_alignments():
    for i in range(0, 100):
        data = b"x" * i
        assert decode(encode(data)) == data, f"failed at length {i}"


def test_base16_high_bytes():
    data = bytes(range(256))
    assert decode(encode(data)) == data


def test_base16_encode_accepts_only_bytes():
    with pytest.raises(TypeError):
        encode("not bytes")


def test_base16_decode_accepts_only_bytes():
    with pytest.raises(TypeError):
        decode("not bytes")


def test_base16_large_binary():
    data = os.urandom(10_000_001)
    assert decode(encode(data)) == data


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
