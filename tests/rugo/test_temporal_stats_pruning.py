"""TIMESTAMP / DATE predicates prune row groups on footer min/max.

`decode_value` decodes TIMESTAMP and DATE statistics to the bare physical int
(epoch units / epoch days), so a `datetime` / `date` literal compared against
them raised TypeError, which stage 1 reads as "don't prune": every row group was
decoded and only stage 2 filtered. The answer stayed right, so nothing reported
it — only the decode work grew. A pruned row group yields no morsel (see the
read_parquet docstring), so these tests count MORSELS to prove stage 1 ran.

The literal is converted with draken's own scalar conversions — the ones the
row-level compare applies — so both stages honour the column's unit and treat
an aware datetime as its UTC instant and a naive one as UTC.
"""

import datetime
import io

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from rugo import parquet

ROWS = 10_000
ROW_GROUP = 1_000           # 10 row groups
START = datetime.datetime(2024, 1, 1)
STEP = datetime.timedelta(minutes=50)   # 10k rows ≈ 347 days, sorted
UNIT_SCALE = {"ms": 1_000, "us": 1, "ns": 1}


def _orders(unit, tz):
    """Sorted `ordered_at` timestamp[unit, tz] plus its DATE, as parquet bytes."""
    stamps = [START + STEP * i for i in range(ROWS)]
    if tz is not None:
        stamps = [s.replace(tzinfo=datetime.timezone.utc) for s in stamps]
    ts = pa.array(stamps, pa.timestamp("us", tz=tz)).cast(pa.timestamp(unit, tz=tz))
    table = pa.table({
        "order_id": pa.array(range(ROWS), pa.int64()),
        "ordered_at": ts,
        "ordered_on": pa.array([s.date() for s in stamps], pa.date32()),
    })
    sink = io.BytesIO()
    pq.write_table(table, sink, row_group_size=ROW_GROUP)
    return sink.getvalue()


def _write(tmp_path, data):
    path = tmp_path / "orders.parquet"
    path.write_bytes(data)
    return str(path)


def _morsel_rows(source, predicate):
    with parquet.read_parquet(source, columns=["order_id"], predicates=[predicate]) as r:
        return [m.num_rows for m in r]


def _epoch(dt, unit):
    """`dt` (naive = UTC) as an integer instant in `unit`."""
    aware = dt if dt.tzinfo else dt.replace(tzinfo=datetime.timezone.utc)
    delta = aware - datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    us = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    return {"ms": us // 1_000, "us": us, "ns": us * 1_000}[unit]


BOUND = datetime.datetime(2024, 12, 1)
EXPECTED = sum(1 for i in range(ROWS) if START + STEP * i >= BOUND)


@pytest.mark.parametrize("unit", ["ms", "us", "ns"])
@pytest.mark.parametrize("tz", [None, "UTC"])
@pytest.mark.parametrize("as_path", [True, False])
def test_datetime_bound_prunes_like_the_raw_int_bound(tmp_path, unit, tz, as_path):
    data = _orders(unit, tz)
    source = _write(tmp_path, data) if as_path else data

    by_datetime = _morsel_rows(source, ("ordered_at", ">=", BOUND))
    by_int = _morsel_rows(source, ("ordered_at", ">=", _epoch(BOUND, unit)))

    assert sum(by_datetime) == EXPECTED
    # Only the last row group can hold rows >= the bound: the other nine are
    # pruned and yield nothing, exactly as the raw-int bound always did.
    assert len(by_datetime) == 1
    assert by_datetime == by_int


@pytest.mark.parametrize("unit", ["ms", "us", "ns"])
def test_upper_bound_and_equality_prune(tmp_path, unit):
    path = _write(tmp_path, _orders(unit, None))
    first = START + STEP * 10

    assert _morsel_rows(path, ("ordered_at", "<", first)) == [10]
    assert _morsel_rows(path, ("ordered_at", "=", first)) == [1]
    # START is row 0 (row group 0), BOUND is row 9648 (row group 9): the eight
    # row groups between hold neither and are pruned.
    assert _morsel_rows(path, ("ordered_at", "in", [START, BOUND])) == [1, 1]


def test_aware_datetime_prunes_at_its_utc_instant(tmp_path):
    path = _write(tmp_path, _orders("us", "UTC"))
    plus5 = datetime.timezone(datetime.timedelta(hours=5))
    aware = datetime.datetime(2024, 12, 1, 5, 0, tzinfo=plus5)   # == BOUND in UTC

    assert _morsel_rows(path, ("ordered_at", ">=", aware)) == \
        _morsel_rows(path, ("ordered_at", ">=", BOUND))
    assert len(_morsel_rows(path, ("ordered_at", ">=", aware))) == 1


@pytest.mark.parametrize("as_path", [True, False])
def test_date_bound_prunes(tmp_path, as_path):
    data = _orders("us", None)
    source = _write(tmp_path, data) if as_path else data
    day = BOUND.date()

    rows = _morsel_rows(source, ("ordered_on", ">=", day))
    assert len(rows) == 1
    assert sum(rows) == sum(1 for i in range(ROWS) if (START + STEP * i).date() >= day)
    assert rows == _morsel_rows(source, ("ordered_on", ">=", (day - datetime.date(1970, 1, 1)).days))


def test_non_temporal_literal_on_timestamp_column_raises(tmp_path):
    """A str is no timestamp: refused before pruning rather than read as
    "type mismatch — don't prune" and decoded to the same failure later."""
    path = _write(tmp_path, _orders("us", None))
    with pytest.raises(TypeError):
        _morsel_rows(path, ("ordered_at", ">=", "2024-12-01"))
