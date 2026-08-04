"""Unsigned integers as a cast SOURCE: → unsigned (any width), and → FLOAT64.

Unsigned columns used to be nearly one-way. From an unsigned source only INT64,
DECIMAL and text were reachable, which meant:

  1. **No route to floating point at all.** The only way out was INT64, and that
     RAISES above 2^63-1 — so the top half of the UINT64 range could not enter
     float arithmetic by any path.
  2. **No width changes.** `UINT32 → UINT64` is a widening that cannot fail, and
     it was refused, because the draken_cast_integer_to_uint* family takes SIGNED
     sources only and rejects an unsigned one outright.

Both are now kernels. What must stay true: narrowings are RANGE-CHECKED (loud,
never wrapped), and the float conversion is NOT range-checked because it does not
need to be — every uint64 is representable as a double.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx

_SESSION = opteryx.session()

UINT64_MAX = 18446744073709551615


def _col(sql, colname="x"):
    out = []
    for morsel in _SESSION.execute_to_morsels(sql):
        out.extend(morsel.column(colname).to_pylist())
    return out


def _lit(value, source):
    """A one-row unsigned column of `source` width — not a folded literal."""
    return f"(SELECT CAST(a AS {source}) AS u FROM (SELECT {value} AS a) AS i) AS t"


def test_unsigned_widens_to_every_wider_width():
    """Cannot fail, and was refused anyway."""
    for source, target in (("UINT8", "UINT16"), ("UINT8", "UINT64"),
                           ("UINT16", "UINT32"), ("UINT32", "UINT64")):
        sql = f"SELECT CAST(u AS {target}) AS x FROM {_lit(200, source)}"
        assert _col(sql) == [200], (source, target)


def test_unsigned_narrows_when_the_value_fits():
    assert _col(f"SELECT CAST(u AS UINT8) AS x FROM {_lit(200, 'UINT32')}") == [200]
    assert _col(f"SELECT CAST(u AS UINT16) AS x FROM {_lit(60000, 'UINT64')}") == [60000]


def test_unsigned_narrowing_is_range_checked_not_wrapped():
    """300 must NOT become 44. Loud is the only acceptable outcome."""
    with pytest.raises(Exception):
        _col(f"SELECT CAST(u AS UINT8) AS x FROM {_lit(300, 'UINT64')}")
    with pytest.raises(Exception):
        _col(f"SELECT CAST(u AS UINT32) AS x FROM {_lit(4294967296, 'UINT64')}")


def test_unsigned_same_width_is_a_copy():
    for width in ("UINT8", "UINT16", "UINT32", "UINT64"):
        assert _col(f"SELECT CAST(u AS {width}) AS x FROM {_lit(7, width)}") == [7], width


def test_unsigned_to_double():
    assert _col(f"SELECT CAST(u AS DOUBLE) AS x FROM {_lit(5, 'UINT64')}") == [5.0]
    assert _col("SELECT CAST(CAST(id AS UINT32) AS DOUBLE) AS x FROM $planets")[:3] == [
        1.0,
        2.0,
        3.0,
    ]


def test_unsigned_to_double_covers_the_range_int64_cannot():
    """The point of the kernel: UINT64_MAX has no INT64 route (that raises), so
    before this it could not reach float at all. Above 2^53 a double loses low
    bits — that is floating point, not an error."""
    got = _col(f"SELECT CAST(u AS DOUBLE) AS x FROM {_lit(UINT64_MAX, 'UINT64')}")
    assert got == [float(UINT64_MAX)], got
    with pytest.raises(Exception):
        _col(f"SELECT CAST(u AS INTEGER) AS x FROM {_lit(UINT64_MAX, 'UINT64')}")


def test_unsigned_casts_preserve_nulls():
    sql = (
        "SELECT CAST(u AS {t}) AS x FROM "
        "(SELECT CASE WHEN id > 4 THEN CAST(id AS UINT32) ELSE NULL END AS u "
        "FROM $planets) AS s"
    )
    for target in ("UINT64", "UINT8", "DOUBLE"):
        got = _col(sql.format(t=target))
        assert got[:4] == [None, None, None, None], (target, got[:4])
        assert got[4] is not None, (target, got[4])


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"✅ {name}")
    print("All unsigned numeric cast tests passed.")
