"""
Offline tests for the native GCS gRPC reader (src/cpp/gcs_grpc): CRC32C and
the streaming ReadObject response parser. No network.
"""

import os
import random
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.compiled.gcs_grpc import crc32c_of, parse_read_object_body_for_test


def _crc32c_ref(data: bytes) -> int:
    c = 0xFFFFFFFF
    for b in data:
        c ^= b
        for _ in range(8):
            c = (c >> 1) ^ (0x82F63B78 if c & 1 else 0)
    return c ^ 0xFFFFFFFF


def _varint(v: int) -> bytes:
    out = bytearray()
    while v >= 0x80:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v)
    return bytes(out)


def _len_field(number: int, payload: bytes) -> bytes:
    return _varint((number << 3) | 2) + _varint(len(payload)) + payload


def _fixed32_field(number: int, value: int) -> bytes:
    return _varint((number << 3) | 5) + value.to_bytes(4, "little")


def _varint_field(number: int, value: int) -> bytes:
    return _varint(number << 3) + _varint(value)


def _frame(message: bytes, compressed: int = 0) -> bytes:
    return bytes([compressed]) + len(message).to_bytes(4, "big") + message


def _response(content: bytes, *, crc=None, generation=None, extra=b"") -> bytes:
    """A ReadObjectResponse: checksummed_data(1){content(1), crc32c(2)}, metadata(4){generation(3)}."""
    data = _len_field(1, content) + _fixed32_field(2, _crc32c_ref(content) if crc is None else crc)
    msg = _len_field(1, data) + extra
    if generation is not None:
        msg += _len_field(4, _len_field(1, b"name") + _varint_field(3, generation))
    return _frame(msg)


def test_crc32c_check_value():
    assert crc32c_of(b"123456789") == 0xE3069283
    assert crc32c_of(b"") == 0


def test_crc32c_matches_reference_every_tail_length():
    rng = random.Random(7)
    for n in range(0, 70):
        data = bytes(rng.getrandbits(8) for _ in range(n))
        assert crc32c_of(data) == _crc32c_ref(data), n


@pytest.mark.parametrize("chunk", [1, 2, 3, 5, 7, 64, 4096, 1 << 20])
def test_parser_reassembles_at_every_split(chunk):
    rng = random.Random(chunk)
    parts = [bytes(rng.getrandbits(8) for _ in range(n)) for n in (1000, 1, 3000, 17)]
    # Unknown fields (varint 3, fixed64 5, len 9) must be skipped wherever they sit.
    unknown = _varint_field(30, 12345) + _varint((5 << 3) | 1) + b"\x00" * 8 + _len_field(9, b"skip me")
    body = _response(parts[0], generation=42, extra=unknown)
    body += b"".join(_response(p) for p in parts[1:])
    err, data, generation = parse_read_object_body_for_test(body, sum(map(len, parts)), chunk)
    assert err == ""
    assert data == b"".join(parts)
    assert generation == 42


def test_parser_rejects_crc_mismatch():
    body = _response(b"abcdef", crc=_crc32c_ref(b"abcdeg"))
    err, _, _ = parse_read_object_body_for_test(body, 6, 3)
    assert "CRC32C mismatch" in err


def test_parser_rejects_more_data_than_requested():
    err, _, _ = parse_read_object_body_for_test(_response(b"x" * 10), 9, 4)
    assert "more data than the range requested" in err


def test_parser_reports_short_stream():
    err, _, _ = parse_read_object_body_for_test(_response(b"x" * 10), 20, 4)
    assert err.startswith("short")


def test_parser_reports_truncated_message():
    body = _response(b"x" * 10)
    err, _, _ = parse_read_object_body_for_test(body[:-3], 10, 4)
    assert err == "stream ended inside a message"


def test_parser_rejects_compressed_message():
    body = _frame(_len_field(1, _len_field(1, b"abc")), compressed=1)
    err, _, _ = parse_read_object_body_for_test(body, 3, 4)
    assert "compressed" in err


def test_parser_rejects_field_overrunning_message():
    # checksummed_data claims 50 bytes inside a 10-byte message.
    msg = _varint((1 << 3) | 2) + _varint(50) + b"\x00" * 8
    err, _, _ = parse_read_object_body_for_test(_frame(msg), 5, 4)
    assert "overruns" in err


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
