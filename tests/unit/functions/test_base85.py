import base64 as stdlib_base64
import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.third_party.mabel.base85 import decode, encode


def test_base85_encode_simple_string():
    data = b"hello world"
    assert encode(data) == stdlib_base64.b85encode(data)


def test_base85_decode_simple_string():
    encoded = stdlib_base64.b85encode(b"hello world")
    assert decode(encoded) == b"hello world"


def test_base85_encode_decode_roundtrip():
    data = b"x" * 1_000_001
    assert decode(encode(data)) == data


def test_base85_encode_empty():
    assert encode(b"") == b""


def test_base85_decode_empty():
    assert decode(b"") == b""


def test_base85_length_alignments():
    for i in range(0, 100):
        data = b"x" * i
        assert decode(encode(data)) == data, f"failed at length {i}"


def test_base85_high_bytes():
    data = bytes(range(256))
    assert decode(encode(data)) == data


def test_base85_matches_stdlib_for_random():
    for _ in range(20):
        data = os.urandom(127)
        assert encode(data) == stdlib_base64.b85encode(data)
        assert decode(stdlib_base64.b85encode(data)) == data


def test_base85_encode_accepts_only_bytes():
    with pytest.raises(TypeError):
        encode("not bytes")


def test_base85_decode_accepts_only_bytes():
    with pytest.raises(TypeError):
        decode("not bytes")


def test_base85_large_binary():
    data = os.urandom(2_000_001)
    assert decode(encode(data)) == data


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
