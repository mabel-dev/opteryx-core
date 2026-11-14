"""INTERVAL interpretation, checked against an oracle that shares no code with the engine.

Every expectation here comes from this file: unit lengths, the field ladder, and the
calendar arithmetic are written out below from first principles (Python's `calendar`
and `datetime`), never imported from `opteryx.expression.intervals`. A test that asked
the engine's own ladder what `MONTH TO DAY` means is how `'1 3' MONTH TO DAY` shipped as
1 month 3 WEEKS — engine and test agreed, and both were wrong.

What is pinned:

  * the literal itself — `SELECT INTERVAL ...` yields `(months, microseconds)`;
  * every unit, every spelling (singular, plural, any case), every `X TO Y` range;
  * a range's values fill from the LEADING unit, and WEEK is a field only when named
    as an end of the range (`MONTH TO DAY` is months then days);
  * calendar arithmetic: months first (clamped to the target month's last day), then
    the exact part — the Postgres rule — for `+` and `-`, scalar and per-row;
  * clean SqlErrors (never a raw ValueError/OverflowError) for every malformed literal.
"""

import calendar
import datetime
import itertools
import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import SqlError

# ---------------------------------------------------------------------------------------
# The oracle
# ---------------------------------------------------------------------------------------

US = 1
MS = 1_000 * US
SEC = 1_000 * MS
MIN = 60 * SEC
HOUR = 60 * MIN
DAY = 24 * HOUR
WEEK = 7 * DAY

# unit -> (months, microseconds) for one of it
UNIT = {
    "YEAR": (12, 0),
    "MONTH": (1, 0),
    "WEEK": (0, WEEK),
    "DAY": (0, DAY),
    "HOUR": (0, HOUR),
    "MINUTE": (0, MIN),
    "SECOND": (0, SEC),
    "MILLISECOND": (0, MS),
    "MICROSECOND": (0, US),
}
LADDER = list(UNIT)  # dicts keep insertion order: largest first

INT64_MAX = 2**63 - 1


def fields_of(leading, last=None):
    """The fields a literal's values fill, in order. WEEK is not a field of a SQL range
    — `MONTH TO DAY` is months then days — unless the range names it as an end."""
    lo = LADDER.index(leading)
    hi = LADDER.index(last) if last else len(LADDER) - 1
    return [u for u in LADDER[lo : hi + 1] if u != "WEEK" or u in (leading, last)]


def oracle(values, leading, last=None):
    fields = fields_of(leading, last)
    assert len(values) <= len(fields)
    months = micros = 0
    for value, unit in zip(values, fields):
        m, u = UNIT[unit]
        months += value * m
        micros += value * u
    return (months, micros)


def add_months(ts, n):
    """Calendar month shift, clamped to the target month's last day (Jan 31 + 1 → Feb 29)."""
    total = ts.year * 12 + (ts.month - 1) + n
    year, month = divmod(total, 12)
    month += 1
    day = min(ts.day, calendar.monthrange(year, month)[1])
    return ts.replace(year=year, month=month, day=day)


def apply(ts, interval, sign=1):
    """Months first, then the exact part — for subtraction too (Postgres semantics)."""
    months, micros = interval
    return add_months(ts, sign * months) + datetime.timedelta(microseconds=sign * micros)


# ---------------------------------------------------------------------------------------
# Engine helpers
# ---------------------------------------------------------------------------------------

UTC = datetime.timezone.utc


def column(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None or morsel.num_rows == 0:
            continue
        out.extend(morsel.column(morsel.column_names[0]))
    return out


def scalar(sql):
    values = column(sql)
    assert len(values) == 1, f"{sql} returned {len(values)} rows"
    return values[0]


def interval_of(literal):
    return tuple(scalar(f"SELECT {literal} AS x"))


def literal(values, leading, last=None):
    text = " ".join(str(v) for v in values)
    return f"INTERVAL '{text}' {leading}" + (f" TO {last}" if last else "")


def ts_sql(ts):
    return f"CAST('{ts.strftime('%Y-%m-%d %H:%M:%S.%f')}' AS TIMESTAMP)"


def naive(value):
    if isinstance(value, datetime.datetime) and value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    return value


# ---------------------------------------------------------------------------------------
# 1. Hand-written expectations — no oracle, just the answer
# ---------------------------------------------------------------------------------------

HAND_WRITTEN = [
    ("INTERVAL '1' YEAR", (12, 0)),
    ("INTERVAL '1' MONTH", (1, 0)),
    ("INTERVAL '1' WEEK", (0, 604_800_000_000)),
    ("INTERVAL '1' DAY", (0, 86_400_000_000)),
    ("INTERVAL '1' HOUR", (0, 3_600_000_000)),
    ("INTERVAL '1' MINUTE", (0, 60_000_000)),
    ("INTERVAL '1' SECOND", (0, 1_000_000)),
    ("INTERVAL '1' MILLISECOND", (0, 1_000)),
    ("INTERVAL '1' MICROSECOND", (0, 1)),
    ("INTERVAL '0' DAY", (0, 0)),
    ("INTERVAL '-0' DAY", (0, 0)),
    ("INTERVAL '+5' MINUTE", (0, 300_000_000)),
    ("INTERVAL '-2' HOUR", (0, -7_200_000_000)),
    ("INTERVAL '007' DAY", (0, 604_800_000_000)),
    ("INTERVAL '13' MONTH", (13, 0)),
    ("INTERVAL '25' HOUR", (0, 90_000_000_000)),
    ("INTERVAL '90' MINUTE", (0, 5_400_000_000)),
    ("INTERVAL '1500' MILLISECOND", (0, 1_500_000)),
    ("INTERVAL '1 3' YEAR TO MONTH", (15, 0)),
    ("INTERVAL '-1 -3' YEAR TO MONTH", (-15, 0)),
    ("INTERVAL '1 -3' YEAR TO MONTH", (9, 0)),
    ("INTERVAL '1' YEAR TO MONTH", (12, 0)),
    ("INTERVAL '1 3' MONTH TO DAY", (1, 259_200_000_000)),
    ("INTERVAL '2 3' WEEK TO DAY", (0, 1_468_800_000_000)),
    ("INTERVAL '1 2' YEAR TO DAY", (14, 0)),
    ("INTERVAL '1 2 3' YEAR TO DAY", (14, 259_200_000_000)),
    ("INTERVAL '1 2' DAY TO HOUR", (0, 93_600_000_000)),
    ("INTERVAL '1 2 3' DAY TO MINUTE", (0, 93_780_000_000)),
    ("INTERVAL '1 2 3 4' DAY TO SECOND", (0, 93_784_000_000)),
    ("INTERVAL '1 2 3 4 5 6' DAY TO MICROSECOND", (0, 93_784_005_006)),
    ("INTERVAL '1 2' HOUR TO MINUTE", (0, 3_720_000_000)),
    ("INTERVAL '1 2 3' HOUR TO SECOND", (0, 3_723_000_000)),
    ("INTERVAL '1 2' MINUTE TO SECOND", (0, 62_000_000)),
    ("INTERVAL '1 2 3 4 5 6 7 8' YEAR TO MICROSECOND", (14, 273_906_007_008)),
    ("INTERVAL '1 2 3 4 5 6 7' WEEK TO MICROSECOND", (0, 788_645_006_007)),
    ("INTERVAL '1 2' DAY", (0, 93_600_000_000)),  # no TO: walks on from DAY
    ("INTERVAL '9223372036854775807' MICROSECOND", (0, INT64_MAX)),
]


@pytest.mark.parametrize("sql, expected", HAND_WRITTEN, ids=[c[0] for c in HAND_WRITTEN])
def test_hand_written_literal(sql, expected):
    assert interval_of(sql) == expected


def test_hand_written_rows_agree_with_the_oracle():
    """Guards the oracle itself: it must reproduce every hand-computed answer."""
    import re

    for sql, expected in HAND_WRITTEN:
        m = re.fullmatch(r"INTERVAL '([^']*)' (\w+)(?: TO (\w+))?", sql)
        values = [int(v) for v in m.group(1).split()]
        assert oracle(values, m.group(2), m.group(3)) == expected, sql


# ---------------------------------------------------------------------------------------
# 2. Every unit × every spelling × a spread of magnitudes and signs
# ---------------------------------------------------------------------------------------

MAGNITUDES = [0, 1, 2, 7, 12, 30, 31, 59, 60, 61, 100, 365, 1000, 99999, -1, -12, -61]


def spellings(unit):
    yield unit
    yield unit + "S"
    yield unit.lower()
    yield unit.lower() + "s"
    yield unit.capitalize()
    yield unit.capitalize() + "s"


@pytest.mark.parametrize(
    "unit, spelled, n",
    [(u, s, n) for u in LADDER for s in spellings(u) for n in MAGNITUDES],
)
def test_single_unit(unit, spelled, n):
    assert interval_of(f"INTERVAL '{n}' {spelled}") == oracle([n], unit)


# ---------------------------------------------------------------------------------------
# 3. Every `X TO Y` range, filled to every length
# ---------------------------------------------------------------------------------------

# The parser rejects the singular `SECOND TO MILLISECOND` / `SECOND TO MICROSECOND`
# (though it takes the plurals). That is grammar, not interpretation — not pinned here.
UNPARSEABLE = {("SECOND", "MILLISECOND"), ("SECOND", "MICROSECOND")}

RANGES = [
    (a, b)
    for a, b in itertools.combinations(LADDER, 2)
    if (a, b) not in UNPARSEABLE
]

VALUE_SETS = [
    [1, 2, 3, 4, 5, 6, 7, 8, 9],
    [11, 23, 59, 59, 999, 999, 1, 1, 1],
    [-1, -2, -3, -4, -5, -6, -7, -8, -9],
    [0, 0, 0, 0, 0, 0, 0, 0, 1],
    [3, -1, 4, -1, 5, -9, 2, -6, 5],
]


@pytest.mark.parametrize(
    "leading, last, count, values",
    [
        (a, b, k, vs[:k])
        for a, b in RANGES
        for k in range(1, len(fields_of(a, b)) + 1)
        for vs in VALUE_SETS
    ],
)
def test_range_fills_from_the_leading_field(leading, last, count, values):
    assert interval_of(literal(values, leading, last)) == oracle(values, leading, last)


@pytest.mark.parametrize("leading, last", RANGES)
def test_range_spellings_agree(leading, last):
    fields = fields_of(leading, last)
    values = list(range(1, len(fields) + 1))
    text = " ".join(map(str, values))
    expected = oracle(values, leading, last)
    for a, b in [
        (leading + "S", last + "S"),
        (leading.lower(), last.lower()),
        (leading.capitalize(), last.lower() + "s"),
    ]:
        assert interval_of(f"INTERVAL '{text}' {a} TO {b}") == expected, (a, b)


@pytest.mark.parametrize("leading, last", [r for r in RANGES if "WEEK" not in r])
def test_week_is_never_an_inner_field(leading, last):
    """No range that does not name WEEK may ever produce a week-sized step: give every
    field the value 1 and the microsecond part must contain no 7-day term."""
    fields = fields_of(leading, last)
    assert "WEEK" not in fields
    values = [1] * len(fields)
    months, micros = interval_of(literal(values, leading, last))
    assert micros == sum(UNIT[f][1] for f in fields)
    assert months == sum(UNIT[f][0] for f in fields)


@pytest.mark.parametrize("last", LADDER[LADDER.index("WEEK") + 1 :])
def test_week_leading_range_uses_weeks(last):
    if ("WEEK", last) in UNPARSEABLE:
        pytest.skip("unparseable")
    assert interval_of(f"INTERVAL '2' WEEK TO {last}") == (0, 2 * WEEK)


@pytest.mark.parametrize("leading", LADDER[: LADDER.index("WEEK")])
def test_week_trailing_range_uses_weeks(leading):
    fields = fields_of(leading, "WEEK")
    assert fields[-1] == "WEEK"
    values = [1] * len(fields)
    assert interval_of(literal(values, leading, "WEEK")) == oracle(values, leading, "WEEK")


# ---------------------------------------------------------------------------------------
# 4. Equivalent spellings of one duration compare equal
# ---------------------------------------------------------------------------------------

EQUIVALENT = [
    ["INTERVAL '1' DAY", "INTERVAL '24' HOUR", "INTERVAL '1440' MINUTE", "INTERVAL '86400' SECOND",
     "INTERVAL '86400000' MILLISECOND", "INTERVAL '86400000000' MICROSECOND", "INTERVAL '0 24' DAY TO HOUR",
     "INTERVAL '1 0 0 0' DAY TO SECOND", "INTERVAL '23 60' HOUR TO MINUTE"],
    ["INTERVAL '1' WEEK", "INTERVAL '7' DAY", "INTERVAL '168' HOUR", "INTERVAL '1 0' WEEK TO DAY",
     "INTERVAL '0 7' WEEK TO DAY", "INTERVAL '6 24' DAY TO HOUR"],
    ["INTERVAL '1' YEAR", "INTERVAL '12' MONTH", "INTERVAL '1 0' YEAR TO MONTH", "INTERVAL '0 12' YEAR TO MONTH",
     "INTERVAL '2 -12' YEAR TO MONTH"],
    ["INTERVAL '90' MINUTE", "INTERVAL '1 30' HOUR TO MINUTE", "INTERVAL '5400' SECOND", "INTERVAL '2 -30' HOUR TO MINUTE"],
    ["INTERVAL '1 3' MONTH TO DAY", "INTERVAL '0 1 3' YEAR TO DAY"],
]


@pytest.mark.parametrize(
    "a, b",
    [pair for group in EQUIVALENT for pair in itertools.combinations(group, 2)],
)
def test_equivalent_literals(a, b):
    assert interval_of(a) == interval_of(b)
    assert scalar(f"SELECT {a} = {b} AS x") is True


# ---------------------------------------------------------------------------------------
# 5. Calendar arithmetic on literals
# ---------------------------------------------------------------------------------------

BASES = [
    datetime.datetime(2020, 1, 1),
    datetime.datetime(2020, 1, 15, 12, 30, 45),
    datetime.datetime(2020, 1, 29),
    datetime.datetime(2020, 1, 30),
    datetime.datetime(2020, 1, 31),
    datetime.datetime(2020, 2, 28),
    datetime.datetime(2020, 2, 29),  # leap day
    datetime.datetime(2021, 2, 28),
    datetime.datetime(2020, 3, 31, 23, 59, 59),
    datetime.datetime(2019, 12, 31, 23, 59, 59, 999999),
    datetime.datetime(2000, 2, 29),  # leap century
    datetime.datetime(1900, 2, 28),  # non-leap century
    datetime.datetime(1999, 12, 31),
    datetime.datetime(2038, 1, 19, 3, 14, 7),
]

INTERVALS = [
    ("1", "YEAR", None),
    ("4", "YEARS", None),
    ("100", "YEAR", None),
    ("1", "MONTH", None),
    ("11", "MONTHS", None),
    ("13", "MONTH", None),
    ("-1", "MONTH", None),
    ("1", "WEEK", None),
    ("1", "DAY", None),
    ("29", "DAYS", None),
    ("366", "DAY", None),
    ("1", "HOUR", None),
    ("25", "HOURS", None),
    ("1", "MINUTE", None),
    ("1", "SECOND", None),
    ("1", "MILLISECOND", None),
    ("1", "MICROSECOND", None),
    ("1 1", "YEAR", "MONTH"),
    ("1 3", "MONTH", "DAY"),
    ("1 1", "MONTH", "DAY"),
    ("0 30", "MONTH", "DAY"),
    ("2 3", "WEEK", "DAY"),
    ("1 1 1", "YEAR", "DAY"),
    ("1 2 3 4", "DAY", "SECOND"),
    ("1 -1", "MONTH", "DAY"),
    ("-1 1", "MONTH", "DAY"),
]


def _parsed(text, leading, last):
    unit = leading if leading in UNIT else leading[:-1]  # plural → singular
    return oracle([int(v) for v in text.split()], unit, last)


@pytest.mark.parametrize("sign, op", [(1, "+"), (-1, "-")])
@pytest.mark.parametrize("text, leading, last", INTERVALS)
@pytest.mark.parametrize("base", BASES, ids=lambda b: b.isoformat())
def test_timestamp_arithmetic(base, text, leading, last, sign, op):
    sql_interval = f"INTERVAL '{text}' {leading}" + (f" TO {last}" if last else "")
    expected = apply(base, _parsed(text, leading, last), sign)
    got = naive(scalar(f"SELECT {ts_sql(base)} {op} {sql_interval} AS x"))
    assert got == expected


@pytest.mark.parametrize("text, leading, last", INTERVALS)
@pytest.mark.parametrize("base", BASES[:6], ids=lambda b: b.isoformat())
def test_interval_plus_timestamp_commutes(base, text, leading, last):
    sql_interval = f"INTERVAL '{text}' {leading}" + (f" TO {last}" if last else "")
    a = scalar(f"SELECT {ts_sql(base)} + {sql_interval} AS x")
    b = scalar(f"SELECT {sql_interval} + {ts_sql(base)} AS x")
    assert a == b


@pytest.mark.parametrize("base", BASES, ids=lambda b: b.isoformat())
def test_months_apply_before_days(base):
    """`(1 month, 3 days)` is not `3 days, then 1 month`: Jan 29 + it is Mar 3, not Mar 1."""
    got = naive(scalar(f"SELECT {ts_sql(base)} + INTERVAL '1 3' MONTH TO DAY AS x"))
    assert got == add_months(base, 1) + datetime.timedelta(days=3)


def test_month_end_clamping_examples():
    cases = [
        ("2020-01-31", "1", "2020-02-29"),
        ("2021-01-31", "1", "2021-02-28"),
        ("2020-03-31", "-1", "2020-02-29"),
        ("2020-05-31", "1", "2020-06-30"),
        ("2020-02-29", "12", "2021-02-28"),
        ("2020-08-31", "-6", "2020-02-29"),
    ]
    for start, months, end in cases:
        got = naive(scalar(f"SELECT CAST('{start}' AS TIMESTAMP) + INTERVAL '{months}' MONTH AS x"))
        assert got == datetime.datetime.fromisoformat(end), (start, months)


def test_chained_month_additions_clamp_each_step():
    """Each `+` is its own calendar step: Jan 31 +1m → Feb 29, +1m → Mar 29. Adding the
    intervals first gives 2 months in one step: Mar 31. Both are correct; they differ."""
    stepwise = naive(scalar(
        "SELECT CAST('2020-01-31' AS TIMESTAMP) + INTERVAL '1' MONTH + INTERVAL '1' MONTH AS x"
    ))
    grouped = naive(scalar(
        "SELECT CAST('2020-01-31' AS TIMESTAMP) + (INTERVAL '1' MONTH + INTERVAL '1' MONTH) AS x"
    ))
    assert stepwise == datetime.datetime(2020, 3, 29)
    assert grouped == datetime.datetime(2020, 3, 31)


def epoch_us(ts):
    return (ts - datetime.datetime(1970, 1, 1)) // datetime.timedelta(microseconds=1)


def test_add_then_subtract_round_trips_exact_units():
    """Compared as INT64 epochs, not rendered datetimes — see the xfail below for why."""
    for unit in ["WEEK", "DAY", "HOUR", "MINUTE", "SECOND", "MILLISECOND", "MICROSECOND"]:
        for base in BASES:
            base_sql = f"CAST('{base.strftime('%Y-%m-%d %H:%M:%S')}' AS TIMESTAMP)"
            whole = base.replace(microsecond=0)
            once = scalar(f"SELECT CAST({base_sql} + INTERVAL '37' {unit} AS INT64) AS x")
            back = scalar(
                f"SELECT CAST({base_sql} + INTERVAL '37' {unit} - INTERVAL '37' {unit} AS INT64) AS x"
            )
            assert once == epoch_us(whole) + oracle([37], unit)[1], (unit, base)
            assert back == epoch_us(whole), (unit, base)


@pytest.mark.xfail(
    strict=True,
    reason="TIMESTAMP precision, not INTERVAL: around 1900 a stored "
    "-2203977599963000 renders as .037001, and CAST of '...00.037000' stores ...962999",
)
def test_pre_epoch_fractional_timestamp_is_exact():
    rendered = naive(scalar(
        "SELECT CAST('1900-02-28 00:00:00' AS TIMESTAMP) + INTERVAL '37' MILLISECOND AS x"
    ))
    parsed = scalar("SELECT CAST(CAST('1900-02-28 00:00:00.037000' AS TIMESTAMP) AS INT64) AS x")
    assert rendered == datetime.datetime(1900, 2, 28, 0, 0, 0, 37000)
    assert parsed == epoch_us(datetime.datetime(1900, 2, 28, 0, 0, 0, 37000))


# ---------------------------------------------------------------------------------------
# 6. Interval-with-interval arithmetic and ordering
# ---------------------------------------------------------------------------------------

EXACT = [(u, n) for u in LADDER[2:] for n in (1, 3, 59, 1000)]


@pytest.mark.parametrize("a, b", list(itertools.combinations(EXACT, 2))[::3])
def test_interval_addition_and_subtraction(a, b):
    (ua, na), (ub, nb) = a, b
    ia, ib = oracle([na], ua), oracle([nb], ub)
    add = interval_of(f"INTERVAL '{na}' {ua} + INTERVAL '{nb}' {ub}")
    sub = interval_of(f"INTERVAL '{na}' {ua} - INTERVAL '{nb}' {ub}")
    assert add == (ia[0] + ib[0], ia[1] + ib[1])
    assert sub == (ia[0] - ib[0], ia[1] - ib[1])


@pytest.mark.parametrize("a, b", list(itertools.combinations(EXACT, 2))[1::3])
def test_exact_interval_ordering(a, b):
    (ua, na), (ub, nb) = a, b
    ma, mb = oracle([na], ua)[1], oracle([nb], ub)[1]
    sa, sb = f"INTERVAL '{na}' {ua}", f"INTERVAL '{nb}' {ub}"
    assert scalar(f"SELECT {sa} < {sb} AS x") is (ma < mb)
    assert scalar(f"SELECT {sa} > {sb} AS x") is (ma > mb)
    assert scalar(f"SELECT {sa} = {sb} AS x") is (ma == mb)


def test_timestamp_difference_is_an_exact_interval():
    for a, b in itertools.combinations(BASES, 2):
        got = tuple(scalar(f"SELECT {ts_sql(b)} - {ts_sql(a)} AS x"))
        delta = b - a
        assert got == (0, (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds), (a, b)


# ---------------------------------------------------------------------------------------
# 7. Per-row (non-constant) arithmetic — a column, not a folded literal
# ---------------------------------------------------------------------------------------

# $planets ids 1..9 become the month of a per-row timestamp, so the interval is applied
# to a real column rather than constant-folded at plan time.
ROW_DAYS = ["01", "15", "28"]
ROW_YEARS = [2019, 2020]


def row_timestamps(year, day):
    return [datetime.datetime(year, month, int(day)) for month in range(1, 10)]


def row_ts_sql(year, day):
    return f"CAST(CONCAT('{year}-', LPAD(CAST(id AS VARCHAR), 2, '0'), '-{day}') AS TIMESTAMP)"


@pytest.mark.parametrize("sign, op", [(1, "+"), (-1, "-")])
@pytest.mark.parametrize("text, leading, last", INTERVALS)
@pytest.mark.parametrize("year, day", list(itertools.product(ROW_YEARS, ROW_DAYS)))
def test_per_row_arithmetic(year, day, text, leading, last, sign, op):
    sql_interval = f"INTERVAL '{text}' {leading}" + (f" TO {last}" if last else "")
    got = [
        naive(v)
        for v in column(f"SELECT {row_ts_sql(year, day)} {op} {sql_interval} AS x FROM $planets ORDER BY id")
    ]
    interval = _parsed(text, leading, last)
    assert got == [apply(ts, interval, sign) for ts in row_timestamps(year, day)]


@pytest.mark.parametrize("text, leading, last", INTERVALS)
@pytest.mark.parametrize("threshold", ["2020-03-01", "2020-06-15", "2021-01-01"])
def test_interval_in_a_predicate(text, leading, last, threshold):
    sql_interval = f"INTERVAL '{text}' {leading}" + (f" TO {last}" if last else "")
    cutoff = datetime.datetime.fromisoformat(threshold)
    interval = _parsed(text, leading, last)
    expected = sorted(
        month for month, ts in enumerate(row_timestamps(2020, "15"), start=1) if apply(ts, interval) > cutoff
    )
    got = column(
        f"SELECT id FROM $planets WHERE {row_ts_sql(2020, '15')} + {sql_interval} "
        f"> CAST('{threshold}' AS TIMESTAMP) ORDER BY id"
    )
    assert got == expected


# ---------------------------------------------------------------------------------------
# 8. Whitespace inside the quotes is padding
# ---------------------------------------------------------------------------------------

@pytest.mark.parametrize(
    "text, leading, last, expected",
    [
        (" 1", "DAY", None, (0, DAY)),
        ("1 ", "DAY", None, (0, DAY)),
        ("  1  ", "DAY", None, (0, DAY)),
        ("1  3", "YEAR", "MONTH", (15, 0)),
        (" 1 3 ", "YEAR", "MONTH", (15, 0)),
        ("1\t3", "YEAR", "MONTH", (15, 0)),
        ("1   2   3   4", "DAY", "SECOND", (0, DAY + 2 * HOUR + 3 * MIN + 4 * SEC)),
    ],
)
def test_whitespace_is_padding(text, leading, last, expected):
    sql = f"INTERVAL '{text}' {leading}" + (f" TO {last}" if last else "")
    assert interval_of(sql) == expected


# ---------------------------------------------------------------------------------------
# 9. Malformed literals: always a SqlError, never a raw Python exception
# ---------------------------------------------------------------------------------------

@pytest.mark.parametrize(
    "sql, match",
    [
        ("INTERVAL '' DAY", "no value"),
        ("INTERVAL '   ' DAY", "no value"),
        ("INTERVAL '1.5' DAY", "not a whole number"),
        ("INTERVAL '1,5' DAY", "not a whole number"),
        ("INTERVAL 'one' DAY", "not a whole number"),
        ("INTERVAL '1e3' SECOND", "not a whole number"),
        ("INTERVAL '--1' DAY", "not a whole number"),
        ("INTERVAL '1-3' YEAR TO MONTH", "not a whole number"),
        ("INTERVAL '1:30' HOUR TO MINUTE", "not a whole number"),
        ("INTERVAL '+-1' DAY", "not a whole number"),
        ("INTERVAL '\u00b2' DAY", "not a whole number"),  # superscript two: isdigit() but not int()
        ("INTERVAL '\u0661' DAY", "not a whole number"),  # Arabic-Indic one
        ("INTERVAL '1 2 3' YEAR TO MONTH", "covers only 2 unit"),
        ("INTERVAL '1 2 3' MONTH TO DAY", "covers only 2 unit"),
        ("INTERVAL '1 2 3 4' WEEK TO HOUR", "covers only 3 unit"),
        ("INTERVAL '1 2' MICROSECOND", "covers only 1 unit"),
        ("INTERVAL '1 2' DAY TO MONTH", "larger unit to a smaller one"),
        ("INTERVAL '1 2' HOUR TO DAY", "larger unit to a smaller one"),
        ("INTERVAL '2' DAY TO DAY", "larger unit to a smaller one"),
        ("INTERVAL '2' MONTHS TO MONTH", "larger unit to a smaller one"),
        ("INTERVAL '999999999' DAY", "out of range"),
        ("INTERVAL '106751992' DAY", "out of range"),
        ("INTERVAL '9223372036854775808' MICROSECOND", "out of range"),
        ("INTERVAL '-9223372036854775809' MICROSECOND", "out of range"),
        ("INTERVAL '15250285' WEEK", "out of range"),  # 15,250,284 weeks is the last that fits
        ("INTERVAL '768614336404564651' YEAR", "out of range"),
        ("INTERVAL '3558400' MONTH", "out of range"),
        ("INTERVAL '296534' YEAR", "out of range"),
        ("INTERVAL '-3558400' MONTH", "out of range"),
        ("INTERVAL '3558399 30' MONTH TO DAY", "out of range"),
        ("INTERVAL 1 DAY", "VARCHAR"),
    ],
)
def test_malformed_literal(sql, match):
    with pytest.raises(SqlError, match=match):
        scalar(f"SELECT {sql} AS x")


# ---------------------------------------------------------------------------------------
# 10. Negation: `-INTERVAL`, `-(expr)`, and `0 - interval`
# ---------------------------------------------------------------------------------------

@pytest.mark.parametrize(
    "sql, expected",
    [
        ("-INTERVAL '1' DAY", (0, -DAY)),
        ("- INTERVAL '1' DAY", (0, -DAY)),
        ("-INTERVAL '-1' DAY", (0, DAY)),
        ("-INTERVAL '+5' MINUTE", (0, -5 * MIN)),
        ("-INTERVAL '3' days", (0, -3 * DAY)),
        ("-INTERVAL '2' WEEK", (0, -2 * WEEK)),
        ("-INTERVAL '1 3' YEAR TO MONTH", (-15, 0)),
        ("-INTERVAL '1 -3' MONTH TO DAY", (-1, 3 * DAY)),
        ("-(INTERVAL '1' HOUR)", (0, -HOUR)),
        ("-((INTERVAL '1' HOUR))", (0, -HOUR)),
        ("-(-INTERVAL '1' HOUR)", (0, HOUR)),
        ("-(INTERVAL '1' MONTH + INTERVAL '1' DAY)", (-1, -DAY)),
        ("0 - INTERVAL '1' DAY", (0, -DAY)),
        ("0 - INTERVAL '1 3' MONTH TO DAY", (-1, -3 * DAY)),
        ("INTERVAL '2' DAY + -INTERVAL '1' DAY", (0, DAY)),
        ("-INTERVAL '0' DAY", (0, 0)),
    ],
)
def test_negation(sql, expected):
    assert interval_of(sql) == expected


@pytest.mark.parametrize("text, leading, last", INTERVALS)
@pytest.mark.parametrize("base", BASES[:8], ids=lambda b: b.isoformat())
def test_adding_a_negation_is_subtracting(base, text, leading, last):
    sql_interval = f"INTERVAL '{text}' {leading}" + (f" TO {last}" if last else "")
    plus_neg = scalar(f"SELECT {ts_sql(base)} + -{sql_interval} AS x")
    plus_neg_paren = scalar(f"SELECT {ts_sql(base)} + -({sql_interval}) AS x")
    minus = scalar(f"SELECT {ts_sql(base)} - {sql_interval} AS x")
    assert plus_neg == minus == plus_neg_paren


def test_negating_a_computed_interval_per_row():
    """Not a literal: `-(ts - anchor)` must negate the per-row difference."""
    got = column(
        f"SELECT -({row_ts_sql(2020, '15')} - CAST('2020-01-01' AS TIMESTAMP)) AS x FROM $planets ORDER BY id"
    )
    expected = [
        (0, -epoch_us(ts) + epoch_us(datetime.datetime(2020, 1, 1))) for ts in row_timestamps(2020, "15")
    ]
    assert [tuple(v) for v in got] == expected


def test_minus_on_numbers_is_untouched():
    assert column("SELECT -id AS x FROM $planets ORDER BY id") == [-i for i in range(1, 10)]
    assert column("SELECT 0 - id AS x FROM $planets ORDER BY id") == [-i for i in range(1, 10)]
    assert scalar("SELECT -1.5 AS x") == -1.5


def test_binary_minus_is_untouched():
    assert naive(scalar("SELECT CAST('2020-01-02' AS TIMESTAMP) - INTERVAL '1' DAY AS x")) == datetime.datetime(
        2020, 1, 1
    )
    assert interval_of("INTERVAL '2' DAY - INTERVAL '1' DAY") == (0, DAY)


# ---------------------------------------------------------------------------------------
# 11. INTERVAL × number, number × INTERVAL, INTERVAL ÷ number (Postgres semantics)
# ---------------------------------------------------------------------------------------

from fractions import Fraction

MONTH_US = 30 * DAY  # a fractional month spills into the sub-month part at 30 days


def scale_exact(interval, k, divide=False):
    """Integer factor/divisor: exact. Months scale (toward zero on division), the month
    remainder spills into µs, µs rounds half-to-even (Python's round on a Fraction)."""
    months, micros = interval
    if not divide:
        return (months * k, micros * k)
    whole = int(Fraction(months, k))  # toward zero
    carried = micros + (months - whole * k) * MONTH_US
    return (whole, round(Fraction(carried, k)))


def scale_float(interval, f, divide=False):
    """Float factor: IEEE double, as Postgres — the factors below are exact binary
    fractions, so this is exact too."""
    months, micros = interval
    mf = months / f if divide else months * f
    whole = int(mf)  # trunc toward zero
    usf = (micros / f if divide else micros * f) + (mf - whole) * MONTH_US
    return (whole, round(usf))


HAND_SCALED = [
    ("INTERVAL '1' MONTH * 1.5", (1, 15 * DAY)),
    ("INTERVAL '1' MONTH / 2", (0, 15 * DAY)),
    ("INTERVAL '3' MONTH / 2", (1, 15 * DAY)),
    ("INTERVAL '-3' MONTH / 2", (-1, -15 * DAY)),
    ("INTERVAL '1' MONTH / -2", (0, -15 * DAY)),
    ("INTERVAL '2' YEAR / 3", (8, 0)),
    ("INTERVAL '1' YEAR * 0.5", (6, 0)),
    ("INTERVAL '1 1' MONTH TO DAY * 3", (3, 3 * DAY)),
    ("3 * INTERVAL '1' DAY", (0, 3 * DAY)),
    ("1.5 * INTERVAL '1' DAY", (0, 36 * HOUR)),
    ("INTERVAL '1' DAY / 3", (0, 8 * HOUR)),
    ("INTERVAL '1' DAY / 0.5", (0, 2 * DAY)),
    ("INTERVAL '1' MONTH * 0", (0, 0)),
    ("INTERVAL '1' MONTH * -2", (-2, 0)),
    ("INTERVAL '1' MONTH / 7", (0, 370_285_714_286)),
    ("INTERVAL '1' MICROSECOND / 2", (0, 0)),  # 0.5 → 0 (half to even)
    ("INTERVAL '3' MICROSECOND / 2", (0, 2)),  # 1.5 → 2
    ("INTERVAL '5' MICROSECOND / 2", (0, 2)),  # 2.5 → 2
    ("INTERVAL '-3' MICROSECOND / 2", (0, -2)),  # -1.5 → -2
    ("INTERVAL '1' WEEK * 2", (0, 2 * WEEK)),
    ("(INTERVAL '1' DAY + INTERVAL '1' MONTH) * 2", (2, 2 * DAY)),
    ("INTERVAL '1' DAY * 2 * 3", (0, 6 * DAY)),
    ("INTERVAL '1' MONTH * 2 / 4", (0, 15 * DAY)),
]


@pytest.mark.parametrize("sql, expected", HAND_SCALED, ids=[c[0] for c in HAND_SCALED])
def test_hand_written_scaling(sql, expected):
    assert interval_of(sql) == expected


SCALE_INTERVALS = [
    "INTERVAL '1' YEAR", "INTERVAL '1' MONTH", "INTERVAL '7' MONTH", "INTERVAL '-5' MONTH",
    "INTERVAL '1' WEEK", "INTERVAL '1' DAY", "INTERVAL '1' HOUR", "INTERVAL '1' SECOND",
    "INTERVAL '1' MICROSECOND", "INTERVAL '999' MICROSECOND", "INTERVAL '1 3' MONTH TO DAY",
    "INTERVAL '1 2 3 4' DAY TO SECOND", "INTERVAL '2 -10' YEAR TO MONTH", "INTERVAL '-1 15' MONTH TO DAY",
]
INT_FACTORS = [-12, -3, -1, 0, 1, 2, 3, 7, 12, 1000]
INT_DIVISORS = [-7, -2, -1, 1, 2, 3, 4, 7, 12, 30, 31, 1000]
FLOAT_FACTORS = [0.5, 1.5, 2.5, 0.25, -0.5, -1.0, 3.0, 0.125, 0.0]
FLOAT_DIVISORS = [0.5, 2.0, 4.0, -0.25, 8.0, 1.0]


@pytest.mark.parametrize("k", INT_FACTORS)
@pytest.mark.parametrize("iv", SCALE_INTERVALS)
def test_multiply_by_integer(iv, k):
    expected = scale_exact(interval_of(iv), k)
    assert interval_of(f"{iv} * {k}") == expected
    assert interval_of(f"{k} * {iv}") == expected


@pytest.mark.parametrize("k", INT_DIVISORS)
@pytest.mark.parametrize("iv", SCALE_INTERVALS)
def test_divide_by_integer(iv, k):
    assert interval_of(f"{iv} / {k}") == scale_exact(interval_of(iv), k, divide=True)


@pytest.mark.parametrize("f", FLOAT_FACTORS)
@pytest.mark.parametrize("iv", SCALE_INTERVALS)
def test_multiply_by_float(iv, f):
    expected = scale_float(interval_of(iv), f)
    assert interval_of(f"{iv} * {f!r}") == expected
    assert interval_of(f"{f!r} * {iv}") == expected


@pytest.mark.parametrize("f", FLOAT_DIVISORS)
@pytest.mark.parametrize("iv", SCALE_INTERVALS)
def test_divide_by_float(iv, f):
    assert interval_of(f"{iv} / {f!r}") == scale_float(interval_of(iv), f, divide=True)


@pytest.mark.parametrize("iv", SCALE_INTERVALS)
def test_integer_and_float_factors_agree_when_exact(iv):
    for k in (2, 3, -4):
        assert interval_of(f"{iv} * {k}") == interval_of(f"{iv} * {float(k)!r}")


@pytest.mark.parametrize("iv", SCALE_INTERVALS)
def test_multiply_then_divide_round_trips(iv):
    for k in (2, 3, 7, 12):
        assert interval_of(f"{iv} * {k} / {k}") == interval_of(iv)


def test_awkward_float_factor_is_within_a_microsecond_of_exact():
    """A factor with no exact binary form goes through double, as in Postgres — so it
    may miss the exact answer, but never by more than a microsecond."""
    for iv in SCALE_INTERVALS:
        for f in (0.1, 1 / 3, 2 / 3, 0.7):
            months, micros = interval_of(f"{iv} * {f!r}")
            exact = Fraction(interval_of(iv)[0] * MONTH_US + interval_of(iv)[1]) * Fraction(f)
            assert abs(months * MONTH_US + micros - exact) <= 1, (iv, f)


@pytest.mark.parametrize("year, day", [(2020, "15"), (2019, "28")])
def test_stepping_by_n_months_per_row(year, day):
    """The motivating case: `anchor + INTERVAL '1' MONTH * id` steps by id months."""
    anchor = datetime.datetime(2020, 1, 31)
    got = [
        naive(v)
        for v in column(
            f"SELECT {ts_sql(anchor)} + INTERVAL '1' MONTH * id AS x FROM $planets ORDER BY id"
        )
    ]
    assert got == [add_months(anchor, i) for i in range(1, 10)]


def test_interval_times_a_column_per_row():
    assert [tuple(v) for v in column("SELECT INTERVAL '1 1' MONTH TO DAY * id AS x FROM $planets ORDER BY id")] == [
        (i, i * DAY) for i in range(1, 10)
    ]
    assert [tuple(v) for v in column("SELECT id * INTERVAL '1' HOUR AS x FROM $planets ORDER BY id")] == [
        (0, i * HOUR) for i in range(1, 10)
    ]


def test_interval_divided_by_a_column_per_row():
    got = [tuple(v) for v in column("SELECT INTERVAL '1' MONTH / id AS x FROM $planets ORDER BY id")]
    assert got == [scale_exact((1, 0), i, divide=True) for i in range(1, 10)]


def test_computed_interval_times_a_float_column():
    got = column(
        f"SELECT ({row_ts_sql(2020, '15')} - CAST('2020-01-01' AS TIMESTAMP)) * (id / 2.0) AS x "
        f"FROM $planets ORDER BY id"
    )
    expected = [
        scale_float((0, epoch_us(ts) - epoch_us(datetime.datetime(2020, 1, 1))), i / 2.0)
        for i, ts in enumerate(row_timestamps(2020, "15"), start=1)
    ]
    assert [tuple(v) for v in got] == expected


def test_scaled_interval_in_a_predicate():
    got = column(
        f"SELECT id FROM $planets WHERE {row_ts_sql(2020, '15')} + INTERVAL '1' WEEK * id "
        f"> CAST('2020-06-01' AS TIMESTAMP) ORDER BY id"
    )
    expected = [
        i
        for i, ts in enumerate(row_timestamps(2020, "15"), start=1)
        if ts + datetime.timedelta(weeks=i) > datetime.datetime(2020, 6, 1)
    ]
    assert got == expected


@pytest.mark.parametrize(
    "sql, match",
    [
        ("INTERVAL '1' DAY / 0", "division by zero"),
        ("INTERVAL '1' DAY / 0.0", "division by zero"),
        ("INTERVAL '1' MONTH / (id - id)", "division by zero"),
        ("INTERVAL '106751991' DAY * 2", "overflow"),
        ("INTERVAL '3558399' MONTH * 2", "overflow"),
        ("INTERVAL '1' MONTH * 9223372036854775807", "overflow"),
        ("INTERVAL '1' DAY * 1e300", "overflow"),
    ],
)
def test_scaling_fails_loud(sql, match):
    source = " FROM $planets" if "id" in sql else ""
    with pytest.raises(Exception, match=match):
        column(f"SELECT {sql} AS x{source}")


@pytest.mark.parametrize(
    "sql",
    [
        "2 / INTERVAL '1' DAY",
        "INTERVAL '1' DAY * INTERVAL '1' DAY",
        "INTERVAL '1' DAY / INTERVAL '1' DAY",
        "INTERVAL '1' DAY % 2",
        "INTERVAL '1' DAY * 'x'",
        "1 - INTERVAL '1' DAY",
    ],
)
def test_meaningless_interval_arithmetic_is_refused(sql):
    from opteryx.exceptions import IncorrectTypeError

    with pytest.raises(IncorrectTypeError):
        scalar(f"SELECT {sql} AS x")


def test_largest_and_smallest_representable():
    assert interval_of("INTERVAL '9223372036854775807' MICROSECOND") == (0, INT64_MAX)
    assert interval_of("INTERVAL '-9223372036854775808' MICROSECOND") == (0, -(2**63))
    assert interval_of("INTERVAL '106751991' DAY") == (0, 106751991 * DAY)
    assert interval_of("INTERVAL '15250284' WEEK") == (0, 15250284 * WEEK)
    # Storage packs months × 30 days + µs into one int64, so months run out far sooner.
    assert interval_of("INTERVAL '3558399' MONTH") == (3558399, 0)
    assert interval_of("INTERVAL '296533' YEAR") == (296533 * 12, 0)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
