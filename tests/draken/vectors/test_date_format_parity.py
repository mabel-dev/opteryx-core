"""
Parity test for vector_date_format.

The compiled formatter (opteryx/compiled/nanobind/vector_temporal_arith.cpp)
replaces the old per-row strftime(gmtime_r) path.  This test asserts the new
output is byte-identical to strftime+gmtime over a broad range of timestamps
and every supported specifier.

Deliberate pins that are NOT compared against strftime (Opteryx choices /
platform-dependent under strftime):
  %q  → quarter (1-4); not a real strftime specifier.
  %z/%Z → "+0000"/"GMT" (UTC); compared, since gmtime is UTC.
"""

import datetime
import os
import sys
import time

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from opteryx.compiled.nanobind.vectors import vector_date_format


# Specifiers we emit ourselves with a stable C-locale/UTC strftime definition,
# compared byte-for-byte against the platform strftime.
#
# Excluded from the strftime comparison (asserted explicitly below instead):
#   %q  — Opteryx extension, not a real strftime specifier.
#   %Z  — platform-divergent: macOS strftime → "UTC", glibc/prod → "GMT".
#         We canonicalise to "GMT" (the prod value), removing the old split.
#   %P  — macOS strftime is broken (emits "P"); glibc/prod → lowercase "am"/"pm".
SPECIFIERS = list("YymdHIMSpjAaBbzwuUWVGgeklcxXFTRr")

# A spread of epoch-seconds: epoch, leap days, century boundaries, year-ends,
# far future, plus a deterministic scatter hitting every weekday / month /
# second-of-day.  Kept >= 0 because gmtime on some libc rejects negatives, and
# vector_from_sequence takes datetime objects (>= year 1).
SECONDS = [
    0,              # 1970-01-01 (Thursday)
    68169600,       # 1972-02-29 (leap day)
    951782400,      # 2000-02-29 (leap day, /400)
    1582934400,     # 2020-02-29
    1593561600,     # 2020-07-01
    1609459199,     # 2020-12-31 23:59:59
    1750000862,     # 2025-06-15 12:01:02
    4102444800,     # 2100-01-01 (non-leap century)
    13569465600,    # 2400-01-01
]
SECONDS += [i * 91234567 + (i * 37 % 86400) for i in range(150)]
SECONDS = sorted(set(SECONDS))


def _dt(epoch_s: int) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(epoch_s, datetime.timezone.utc).replace(
        tzinfo=None
    )


def _pylist(vec):
    return [b.decode() if isinstance(b, (bytes, bytearray)) else b for b in vec.to_pylist()]


def _expected(epoch_s: int, fmt: str) -> str:
    return time.strftime(fmt, time.gmtime(epoch_s))


@pytest.mark.parametrize("spec", SPECIFIERS)
def test_timestamp_parity(spec):
    vec = vector_from_sequence([_dt(s) for s in SECONDS], dtype=DrakenType.TIMESTAMP64)
    got = _pylist(vector_date_format(vec, f"%{spec}"))
    for s, g in zip(SECONDS, got):
        assert g == _expected(s, f"%{spec}"), (
            f"%{spec} @ {_dt(s)}: got {g!r} want {_expected(s, f'%{spec}')!r}"
        )


@pytest.mark.parametrize("spec", "YymdjAaBbeUWVGgwu")
def test_date32_parity(spec):
    """DATE32 input: time-of-day fields are zero; date fields must match."""
    days = sorted({s // 86400 for s in SECONDS})
    vec = vector_from_sequence(
        [datetime.date(1970, 1, 1) + datetime.timedelta(days=d) for d in days],
        dtype=DrakenType.DATE32,
    )
    got = _pylist(vector_date_format(vec, f"%{spec}"))
    for d, g in zip(days, got):
        assert g == _expected(d * 86400, f"%{spec}"), (
            f"%{spec} @ day {d}: got {g!r} want {_expected(d * 86400, f'%{spec}')!r}"
        )


def test_composite_and_literals():
    vec = vector_from_sequence([_dt(s) for s in SECONDS], dtype=DrakenType.TIMESTAMP64)
    for fmt in [
        "%Y-%m-%d %H:%M:%S",
        "%A, %d %B %Y",
        "[%Y]/%j (%a)",
        "100%% done at %H:%M",
        "%F %T",
        "ISO %G-W%V-%u",
        "%c",
        "%x %X",
        "%I:%M %p",
        "%Y-%m-%dT%H:%M:%S%z",
    ]:
        got = _pylist(vector_date_format(vec, fmt))
        for s, g in zip(SECONDS, got):
            assert g == _expected(s, fmt), (
                f"{fmt!r} @ {_dt(s)}: got {g!r} want {_expected(s, fmt)!r}"
            )


def test_pinned_specifiers():
    """%Z and %P are canonicalised to the glibc/prod values, not macOS strftime."""
    vec = vector_from_sequence(
        [_dt(0), _dt(1750000862)], dtype=DrakenType.TIMESTAMP64  # 00:.. AM, 12:.. PM
    )
    assert _pylist(vector_date_format(vec, "%Z")) == ["GMT", "GMT"]
    assert _pylist(vector_date_format(vec, "%P")) == ["am", "pm"]


def test_nulls_propagate():
    vec = vector_from_sequence(
        [_dt(0), None, _dt(1750000862)], dtype=DrakenType.TIMESTAMP64
    )
    got = _pylist(vector_date_format(vec, "%Y-%m-%d"))
    assert got[0] == "1970-01-01"
    assert got[1] is None
    assert got[2] == "2025-06-15"


def test_quarter_extension():
    """%q is an Opteryx extension → SQL quarter (1-4)."""
    months = [datetime.datetime(2025, m, 15) for m in (1, 4, 7, 10)]
    vec = vector_from_sequence(months, dtype=DrakenType.TIMESTAMP64)
    assert _pylist(vector_date_format(vec, "%q")) == ["1", "2", "3", "4"]


if __name__ == "__main__":
    import subprocess

    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", "-q", __file__]))
