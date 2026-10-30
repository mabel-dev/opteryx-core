import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import datetime
from opteryx.compiled.functions.timestamp import parse_iso

_EPOCH = datetime.datetime(1970, 1, 1)


def _micros(bts):
    """Microseconds since the epoch, via parse_iso (which applies any UTC offset)."""
    dt = parse_iso(bts)
    assert dt is not None, bts
    return (dt - _EPOCH) // datetime.timedelta(microseconds=1)


def test_parse_timestamp():    # Test valid timestamp

    assert _micros(b"2023-10-01 12:00:00") == 1696161600000000, _micros(b"2023-10-01 12:00:00")
    assert _micros(b"2023-10-01T12:00:00Z") == 1696161600000000

    # Test invalid timestamp
    assert parse_iso(b"invalid-timestamp") is None
 
    # Test edge cases
    assert _micros(b"1970-01-01 00:00:00") == 0, _micros(b"1970-01-01 00:00:00")
    assert _micros(b"1970-01-01 00:00:00.000001") == 1, _micros(b"1970-01-01 00:00:00.000001")
    assert _micros(b"1969-12-31 23:59:59") == -1000000, _micros(b"1969-12-31 23:59:59")
    assert _micros(b"1969-12-31 23:59:59.999999") == -1, _micros(b"1969-12-31 23:59:59.999999")
    assert _micros(b"1969-12-31 23:59:59.999998") == -2, _micros(b"1969-12-31 23:59:59.999998")
    assert _micros(b"1969-07-20 08:17:40") == -14226140000000, _micros(b"1969-07-20 08:17:40")
    assert _micros(b"9999-12-31 23:59:59") == 253402300799000000, _micros(b"9999-12-31 23:59:59")

    assert _micros(b"2023-10-01") == 1696118400000000, _micros(b"2023-10-01")
    assert parse_iso(b"2023-10-01") == datetime.datetime(2023,10,1,0,0,0), f"{parse_iso(b'2023-10-01')} != {datetime.datetime(2023,10,1,0,0,0)}"

    # Test invalid formats
    assert parse_iso(b"2023-13-01 12:00:00") is None   # month out of range
    assert parse_iso(b"2023-02-30 12:00:00") is None   # day out of range for the month


def test_parse_timestamp_fractional_and_offset_matrix():
    # Regression for the length-guard bug: `parse_iso` (and the since-removed `parse_iso_timestamp`)
    # rejected any timestamp longer than 26 chars, which excludes 6-digit
    # microseconds combined with a 'Z' or '+HH:MM'/'-HH:MM' offset - exactly
    # what datetime.isoformat() emits. Cross 0-6 fractional digits with each
    # suffix form and check the compiled entry point.
    base = "2025-06-15T00:00:00"
    suffixes = {
        "": datetime.timedelta(0),
        "Z": datetime.timedelta(0),
        "+02:00": datetime.timedelta(hours=2),
        "-05:30": datetime.timedelta(hours=-5, minutes=-30),
    }

    for digits in range(0, 7):  # 0..6 fractional digits
        frac = "".join(str((digits + i) % 10) for i in range(digits))  # deterministic, non-trivial
        micros = int(frac.ljust(6, "0")) if digits else 0

        for suffix, offset in suffixes.items():
            text = base + (f".{frac}" if digits else "") + suffix
            bts = text.encode("utf-8")

            local_dt = datetime.datetime(2025, 6, 15, 0, 0, 0, micros)
            expected_dt = local_dt - offset

            got_dt = parse_iso(bts)
            assert got_dt == expected_dt, f"{text!r}: parse_iso -> {got_dt}, expected {expected_dt}"



def test_parse_timestamp_rejects_overlong_input():
    # The longest valid ISO form is YYYY-MM-DDTHH:MM:SS.ffffff+HH:MM (32 chars).
    # Anything past that - however it got long - must still be rejected, not
    # silently truncated or accepted.
    assert parse_iso(b"2025-06-15T00:00:00.123456+00:00extra") is None
    assert parse_iso(b"2025-06-15T00:00:00.1234567Z") is None  # 7 fractional digits, too long

    # And short/malformed inputs must not read past the end of the buffer.
    for bad in (b"", b"2", b"2025", b"2025-0", b"2025-06-1"):
        assert parse_iso(bad) is None


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()