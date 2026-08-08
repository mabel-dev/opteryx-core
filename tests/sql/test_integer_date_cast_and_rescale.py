"""CAST(<integer> AS DATE) and the CAST-predicate rescale that must agree with it.

Two things are pinned here, and they are two halves of one contract.

1. `CAST(<integer> AS DATE)` reads the integer as DAYS-SINCE-EPOCH — which is
   exactly what a DATE32 stores, so the kernel
   (`draken_cast_integer_to_date32` / `draken_cast_uint_to_date32`) is the int32
   narrowing with the temporal tag. Before it existed a UINT16 date column
   (ClickBench `EventDate`) could not be cast at all: "No native CAST UINT16 →
   DATE" killed the query on any connector that did not absorb the predicate.

2. `_try_normalize_cast_predicate` rewrites `col::T op <literal>` into a
   cast-free comparison on the raw column. It ASSERTS the same days-since-epoch
   reading for an integer source, so if the kernel meant anything else, a
   predicate the optimizer rewrote and one it left alone would select different
   rows. The parity tests below are what stop those two drifting apart.

The rescale reasons in canonical microseconds and handles both directions — a
truncating cast (`ts::DATE`, target coarser than the column) and an exact one
(`int_seconds::TIMESTAMP[s]`, target finer). Assuming the literal was expressed
in the CAST TARGET's units — rather than in its own type's units — silently
multiplied a µs literal by 10^6 for a `::TIMESTAMP[s]` target and returned zero
rows. `test_timestamp_unit_rescale_matches_raw_comparison` is that regression.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import datetime
import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx
from opteryx.exceptions import SqlError

EPOCH = datetime.date(1970, 1, 1)


def _rows(sql):
    session = opteryx.session()
    return [
        morsel[i] for morsel in session.execute_to_morsels(sql) for i in range(morsel.num_rows)
    ]


def _scalar(sql):
    rows = _rows(sql)
    assert len(rows) == 1, f"expected one row from {sql!r}, got {len(rows)}"
    return rows[0][0]


# ---------------------------------------------------------------------------
# CAST(<integer> AS DATE) — days since epoch
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("days", [0, 1, -1, 15887, 15901, 730, -719162])
def test_integer_literal_to_date_is_days_since_epoch(days):
    assert _scalar(f"SELECT CAST({days} AS DATE)") == EPOCH + datetime.timedelta(days=days)


def test_integer_column_to_date_is_days_since_epoch():
    # A COLUMN, not a literal: this exercises the kernel, not the plan-time fold.
    # A registered stub would pass a literal-only test.
    rows = _rows("SELECT id, CAST(id AS DATE) FROM $planets")
    assert rows, "no rows"
    for raw, as_date in rows:
        assert as_date == EPOCH + datetime.timedelta(days=raw)


def test_unsigned_column_to_date_is_days_since_epoch():
    rows = _rows(
        "SELECT x, CAST(x AS DATE) FROM (SELECT CAST(id * 1000 AS UINT16) AS x FROM $planets) AS t"
    )
    assert rows, "no rows"
    for raw, as_date in rows:
        assert as_date == EPOCH + datetime.timedelta(days=raw)


def test_literal_and_column_int_to_date_agree():
    """The plan-time fold and the runtime kernel must produce the same date."""
    folded = _scalar("SELECT CAST(15901 AS DATE)")
    from_column = _rows(
        "SELECT CAST(x AS DATE) FROM (SELECT CAST(15901 AS INT64) AS x FROM $planets LIMIT 1) AS t"
    )[0][0]
    assert folded == from_column == datetime.date(2013, 7, 15)


def test_out_of_range_integer_to_date_fails_loud():
    # The kernel range-checks and raises; it never wraps to an arbitrary date.
    with pytest.raises(Exception):
        _rows(
            "SELECT CAST(x AS DATE) FROM "
            "(SELECT CAST(9999999999 AS UINT64) AS x FROM $planets LIMIT 1) AS t"
        )


def test_out_of_range_try_cast_to_date_is_null():
    rows = _rows(
        "SELECT TRY_CAST(x AS DATE) FROM "
        "(SELECT CAST(9999999999 AS UINT64) AS x FROM $planets LIMIT 1) AS t"
    )
    assert [r[0] for r in rows] == [None]


def test_out_of_range_date_literal_fails_loud_and_try_cast_is_null():
    # The literal fold returns before the function-level cast-disposition handler,
    # so it applies both dispositions itself — a bare OverflowError here (and a
    # TRY_CAST that raised instead of yielding NULL) was the symptom of it not.
    with pytest.raises(SqlError):
        _rows("SELECT CAST(3000000 AS DATE)")
    assert _scalar("SELECT TRY_CAST(3000000 AS DATE)") is None


# ---------------------------------------------------------------------------
# The predicate rescale must select the same rows as the un-rewritten form
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("op", ["=", "<>", "<", "<=", ">", ">="])
def test_date_predicate_rescale_matches_raw_comparison(op):
    """`col::DATE op <date>` must select exactly `col op <days>`.

    $planets.id is a small integer, so day 3 sits inside the data and every
    operator has a non-trivial answer.
    """
    day = 3
    as_date = (EPOCH + datetime.timedelta(days=day)).isoformat()
    via_cast = _scalar(f"SELECT COUNT(*) FROM $planets WHERE id::DATE {op} '{as_date}'::DATE")
    via_raw = _scalar(f"SELECT COUNT(*) FROM $planets WHERE id {op} {day}")
    assert via_cast == via_raw, f"{op}: cast form {via_cast} != raw form {via_raw}"


@pytest.mark.parametrize("op", ["=", "<>", "<", "<=", ">", ">="])
def test_timestamp_unit_rescale_matches_raw_comparison(op):
    """`int_seconds::TIMESTAMP[s] op <timestamp>` must select `int op <seconds>`.

    The literal is stored in MICROSECONDS while the column holds SECONDS. Reading
    the literal as if it were in the cast target's units left the µs value
    unscaled, so `>=` matched nothing at all.
    """
    seconds = 5
    stamp = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=seconds)
    literal = stamp.isoformat(sep=" ")
    via_cast = _scalar(
        f"SELECT COUNT(*) FROM $planets WHERE id::TIMESTAMP[s] {op} '{literal}'::TIMESTAMP"
    )
    via_raw = _scalar(f"SELECT COUNT(*) FROM $planets WHERE id {op} {seconds}")
    assert via_cast == via_raw, f"{op}: cast form {via_cast} != raw form {via_raw}"


@pytest.mark.parametrize("op", ["<", "<=", ">", ">="])
def test_sub_unit_timestamp_literal_rounds_to_the_same_rows(op):
    """A literal landing BETWEEN two column units must still select exactly.

    id::TIMESTAMP[s] against 5.5s: the rewrite has to round the boundary the
    right way per operator (ceil for >/>=, floor for </<=), not truncate blindly.
    """
    literal = "1970-01-01 00:00:05.500000"
    via_cast = _scalar(
        f"SELECT COUNT(*) FROM $planets WHERE id::TIMESTAMP[s] {op} '{literal}'::TIMESTAMP"
    )
    # 5.5 seconds sits strictly between id=5 and id=6.
    boundary = {"<": "id <= 5", "<=": "id <= 5", ">": "id >= 6", ">=": "id >= 6"}[op]
    via_raw = _scalar(f"SELECT COUNT(*) FROM $planets WHERE {boundary}")
    assert via_cast == via_raw, f"{op}: cast form {via_cast} != raw form {via_raw}"


@pytest.mark.parametrize("op", ["<", "<=", ">", ">="])
def test_timestamp_truncating_cast_rescale_matches_raw_comparison(op):
    """`ts::DATE op <date>` — the TRUNCATING direction, where the cast floors.

    `Lauched_at` is a TIMESTAMP64 (µs) and the target is DATE (days), so the cast
    discards the time-of-day. The rewrite has to reproduce that floor exactly:
    `ts::DATE <= D` is `ts < midnight(D+1)`, NOT `ts <= midnight(D)` — the whole
    point of the +1-and-flip adjustment.
    """
    boundary = "1960-06-15"
    via_cast = _scalar(
        f"SELECT COUNT(*) FROM testdata.missions WHERE Lauched_at::DATE {op} '{boundary}'::DATE"
    )
    # The same row set expressed without a cast: a day D is the µs half-open
    # interval [midnight(D), midnight(D+1)).
    same_day_start = f"'{boundary} 00:00:00'::TIMESTAMP"
    next_day_start = "'1960-06-16 00:00:00'::TIMESTAMP"
    raw = {
        "<": f"Lauched_at < {same_day_start}",
        "<=": f"Lauched_at < {next_day_start}",
        ">": f"Lauched_at >= {next_day_start}",
        ">=": f"Lauched_at >= {same_day_start}",
    }[op]
    via_raw = _scalar(f"SELECT COUNT(*) FROM testdata.missions WHERE {raw}")
    assert via_cast == via_raw, f"{op}: cast form {via_cast} != raw form {via_raw}"


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
