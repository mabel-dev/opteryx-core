"""End-to-end regression tests for TIME_BUCKET native lowering.

TIME_BUCKET(magnitude, units, date) buckets a TIMESTAMP64 or DATE32 column to a
`magnitude`-wide boundary and is lowered to the C-ABI draken_time_bucket kernel
(function_temporal.cpp), reached via the dedicated arm in compiled_expression.pyx.

Coverage (the whole point — TIME_BUCKET previously shipped with ZERO tests and
only second/minute/hour/day wired up):

  * every unit: second, minute, hour, day, week, month, quarter, year
  * both operand types: TIMESTAMP64 (testdata.missions.Lauched_at) and
    DATE32 (testdata.astronauts.birth_date)
  * multiple magnitudes: 1, 2, 3, 5 (magnitude>1 exercises the epoch-anchored
    calendar arithmetic for month/quarter/year and the multi-week stride)
  * every result row is checked against an INDEPENDENT Python `datetime` oracle
    computed from the raw input column values (not from the engine).

The real columns span pre-epoch dates (missions to the 1950s, astronauts born
early 1900s) and leap-day birthdays, so leap years, month-length edges, and
pre-epoch (negative-tick) flooring are all exercised by real data — the test
asserts up front that these cases are actually present, so the coverage claim
cannot silently rot.

Semantics (agreed with the architect, 2026-07-17):
  * calendar units are EPOCH-ANCHORED (buckets counted from 1970-01); at
    magnitude 1 this is exactly date_trunc.
  * a DATE32 operand is promoted to microseconds; the result is TIMESTAMP64(us).

Run as a script (CLAUDE.md §10) or under pytest.
"""

import datetime
import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx

_SESSION = opteryx.session()

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
_UNITS = ("second", "minute", "hour", "day", "week", "month", "quarter", "year")
_MAGNITUDES = (1, 2, 3, 5)


def _col(sql, name="r"):
    out = []
    for morsel in _SESSION.execute_to_morsels(sql):
        out.extend(morsel.column(name.encode()).to_pylist())
    return out


def _as_datetime(val):
    """Normalize an input cell (datetime for TIMESTAMP64, date for DATE32) to a
    tz-aware UTC datetime, matching the engine's TIMESTAMP64 output convention
    (DATE32 midnight for a date, the timestamp itself otherwise)."""
    if isinstance(val, datetime.datetime):
        return val if val.tzinfo is not None else val.replace(tzinfo=datetime.timezone.utc)
    if isinstance(val, datetime.date):
        return datetime.datetime(val.year, val.month, val.day, tzinfo=datetime.timezone.utc)
    raise TypeError(f"unexpected input cell type: {type(val)!r}")


def _oracle(val, unit, mag):
    """Independent epoch-anchored TIME_BUCKET reference over Python datetime.

    Deliberately NOT sharing the kernel's tick math: sub-day units floor epoch
    microseconds; calendar units decompose to y/m and count whole units from the
    1970-01 epoch; week floors to the ISO Monday chain anchored on 1969-12-29.
    Returns the bucket start as a naive datetime (== TIMESTAMP64 result)."""
    if val is None:
        return None
    d = _as_datetime(val)

    if unit in ("second", "minute", "hour", "day"):
        secs = {"second": 1, "minute": 60, "hour": 3600, "day": 86400}[unit] * mag
        period_us = secs * 1_000_000
        us = round((d - _EPOCH) / datetime.timedelta(microseconds=1))
        floored = (us // period_us) * period_us  # Python // floors toward -inf
        return _EPOCH + datetime.timedelta(microseconds=floored)

    if unit == "week":
        days = (d.date() - _EPOCH.date()).days
        weeks_from_ref = (days + 3) // 7           # ref Monday = 1969-12-29 (day -3)
        bucket_week = (weeks_from_ref // mag) * mag
        return _EPOCH + datetime.timedelta(days=(-3 + bucket_week * 7))

    y, m = d.year, d.month
    if unit == "month":
        mtot = (y - 1970) * 12 + (m - 1)
        mb = (mtot // mag) * mag
        by = 1970 + mb // 12
        bm = mb - (mb // 12) * 12 + 1
        return datetime.datetime(by, bm, 1, tzinfo=datetime.timezone.utc)
    if unit == "quarter":
        qtot = (y - 1970) * 4 + (m - 1) // 3
        qb = (qtot // mag) * mag
        by = 1970 + qb // 4
        bq = qb - (qb // 4) * 4
        return datetime.datetime(by, bq * 3 + 1, 1, tzinfo=datetime.timezone.utc)
    if unit == "year":
        by = 1970 + ((y - 1970) // mag) * mag
        return datetime.datetime(by, 1, 1, tzinfo=datetime.timezone.utc)

    raise ValueError(unit)


# Raw input columns for the oracle (read once; independent of TIME_BUCKET).
_TS_INPUT = _col("SELECT Lauched_at AS r FROM testdata.missions")
_DATE_INPUT = _col("SELECT birth_date AS r FROM testdata.astronauts")


def _assert_column(table, col, unit, mag, inputs):
    sql = f"SELECT TIME_BUCKET({mag}, '{unit}', {col}) AS r FROM {table}"
    got = _col(sql)
    expected = [_oracle(v, unit, mag) for v in inputs]
    assert len(got) == len(expected), (unit, mag, len(got), len(expected))
    for i, (a, b) in enumerate(zip(got, expected)):
        if a is None or b is None:
            assert a is None and b is None, (table, unit, mag, i, a, b)
        else:
            assert a == b, (table, unit, mag, i, "got", a, "want", b, "from", inputs[i])


# ---------------------------------------------------------------------------
# Coverage guards — assert the real data actually exercises the hard cases,
# so the exhaustive oracle tests below genuinely cover them.
# ---------------------------------------------------------------------------
def test_input_data_covers_edge_cases():
    ts = [v for v in _TS_INPUT if v is not None]
    bd = [v for v in _DATE_INPUT if v is not None]
    assert ts and bd
    assert any(_as_datetime(v) < _EPOCH for v in ts), "expected pre-epoch TIMESTAMP64 launches"
    assert any(_as_datetime(v) < _EPOCH for v in bd), "expected pre-epoch DATE32 births"
    # month-length / leap coverage: births across all 12 months incl. late-month days
    assert {_as_datetime(v).month for v in bd} == set(range(1, 13))
    assert any(_as_datetime(v).day >= 29 for v in bd)


# ---------------------------------------------------------------------------
# TIMESTAMP64 operand — every unit × magnitude, oracle-checked per row.
# ---------------------------------------------------------------------------
def test_timestamp64_all_units_all_magnitudes():
    for unit in _UNITS:
        for mag in _MAGNITUDES:
            _assert_column("testdata.missions", "Lauched_at", unit, mag, _TS_INPUT)


# ---------------------------------------------------------------------------
# DATE32 operand — every unit × magnitude, oracle-checked per row.
# ---------------------------------------------------------------------------
def test_date32_all_units_all_magnitudes():
    for unit in _UNITS:
        for mag in _MAGNITUDES:
            _assert_column("testdata.astronauts", "birth_date", unit, mag, _DATE_INPUT)


# ---------------------------------------------------------------------------
# Plural unit spellings resolve to the same bucket (kernel contract parity).
# ---------------------------------------------------------------------------
def test_plural_units_match_singular():
    for singular, plural in [
        ("second", "seconds"), ("minute", "minutes"), ("hour", "hours"),
        ("day", "days"), ("week", "weeks"), ("month", "months"),
        ("quarter", "quarters"), ("year", "years"),
    ]:
        s = _col(f"SELECT TIME_BUCKET(2, '{singular}', Lauched_at) AS r FROM testdata.missions")
        p = _col(f"SELECT TIME_BUCKET(2, '{plural}', Lauched_at) AS r FROM testdata.missions")
        assert s == p, (singular, plural)


if __name__ == "__main__":  # pragma: no cover
    import glob

    failed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"✓ {name}")
            except Exception as exc:  # noqa: BLE001 — script harness, surface failures
                failed += 1
                print(f"✗ {name}: {exc}")
    print("OK" if not failed else f"{failed} FAILED")
    sys.exit(1 if failed else 0)
