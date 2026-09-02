"""INTERVAL unit spellings, and the units the engine actually implements.

The parser accepts far more unit spellings than the planner ever handled. It hands
`leading_field` back as WRITTEN — singular (`"Day"`), plural (`"Days"`), and for a
couple of units as a single-key mapping (`WEEK` arrives as `{"Week": None}`). The
builder resolved that against a six-entry tuple of singulars with `parts.index()`,
so every other spelling escaped as a bare

    ValueError: tuple.index(x): x not in tuple

with no query text, no position, and nothing the user could act on. `INTERVAL '7' days`
— which Postgres and DuckDB both accept — was enough to trigger it.

Two obligations are pinned here:

  1. Every spelling the parser accepts either evaluates, or raises SqlError naming the
     offending unit. No raw ValueError, ever.
  2. A plural is a pure ALIAS of its singular. `INTERVAL '7' days` and `INTERVAL '7' day`
     must produce the identical timestamp — a normalisation that quietly resolved to a
     neighbouring rung of the ladder would be a silent wrong answer, not a syntax error.

WEEK, MILLISECOND and MICROSECOND were added at the same time; they fold into the
existing (months, microseconds) representation, so WEEK is asserted as exactly 7 DAY
rather than against a hand-written literal.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import QueryParseError
from opteryx.exceptions import SqlError
from opteryx.expression.intervals import INTERVAL_UNITS

TS = "CAST('2020-01-15 00:00:00' AS TIMESTAMP)"


def scalar(sql):
    session = opteryx.session()
    for morsel in session.execute_to_morsels(sql):
        if morsel is None or morsel.num_rows == 0:
            continue
        return morsel.column(morsel.column_names[0])[0]
    return None


@pytest.mark.parametrize(
    "singular, plural",
    [
        ("year", "years"),
        ("month", "months"),
        ("week", "weeks"),
        ("day", "days"),
        ("hour", "hours"),
        ("minute", "minutes"),
        ("second", "seconds"),
        ("millisecond", "milliseconds"),
        ("microsecond", "microseconds"),
    ],
)
def test_plural_unit_is_an_alias_of_the_singular(singular, plural):
    """The plural must land on the SAME rung, not merely parse."""
    assert scalar(f"SELECT {TS} + INTERVAL '3' {plural} AS x") == scalar(
        f"SELECT {TS} + INTERVAL '3' {singular} AS x"
    )


def test_week_is_seven_days():
    assert scalar(f"SELECT {TS} - INTERVAL '1' week AS x") == scalar(
        f"SELECT {TS} - INTERVAL '7' day AS x"
    )


def test_sub_second_units():
    assert scalar(f"SELECT {TS} + INTERVAL '1500' millisecond AS x") == scalar(
        f"SELECT {TS} + INTERVAL '1500000' microsecond AS x"
    )


def test_multi_value_interval_walks_the_ladder():
    """`YEAR TO MONTH` still consumes consecutive rungs after WEEK was inserted."""
    assert scalar(f"SELECT {TS} + INTERVAL '1 3' YEAR TO MONTH AS x") == scalar(
        f"SELECT {TS} + INTERVAL '15' MONTH AS x"
    )


@pytest.mark.parametrize("unit", ["doy", "dow", "century", "decade", "epoch"])
def test_unsupported_unit_raises_sql_error(unit):
    """Parseable but unimplemented — an actionable SqlError, never a ValueError.

    Units the PARSER itself rejects (`quarters`, `isoweek`) are excluded: they never
    reach the builder, so they prove nothing about this guard.
    """
    with pytest.raises(SqlError, match="not a supported unit"):
        scalar(f"SELECT INTERVAL '7' {unit} AS x")


@pytest.mark.parametrize("unit", ["moments", "fortnight", "quarters", "dais"])
def test_unparseable_unit_names_the_unit(unit):
    """The OTHER half of the same mistake.

    A unit sqlparser does not recognise never reaches the builder — the grammar rejects
    it with "INTERVAL requires a unit after the literal value" and a bare column number,
    quoting nothing the reader wrote. The reader cannot tell this apart from the
    unimplemented-unit case above, so it must not read differently: name the unit, and
    quote the SAME valid list. Both lists come from `INTERVAL_UNITS`, which is what stops
    them drifting.
    """
    with pytest.raises(QueryParseError) as raised:
        scalar(f"SELECT INTERVAL '7' {unit} AS x")
    message = str(raised.value)
    assert unit.upper() in message, message
    assert "not a supported INTERVAL unit" in message, message
    for named in ("YEAR", "WEEK", "MICROSECOND"):
        assert named in message, message


def test_unparseable_unit_underlines_the_unit():
    """The caret has to land on the word the message is about, not on the position
    sqlparser happened to give up at with a zero-width mark."""
    sql = "SELECT INTERVAL '7' moments AS x"
    with pytest.raises(QueryParseError) as raised:
        scalar(sql)
    position = raised.value.position
    assert sql[position.start_offset : position.end_offset] == "moments"


def test_missing_unit_still_explains_itself():
    """No unit at all: there is no word to name, so say what is missing instead of
    naming an empty one."""
    with pytest.raises(QueryParseError) as raised:
        scalar("SELECT INTERVAL '7' AS x")
    message = str(raised.value)
    assert "needs a unit" in message, message
    assert "MICROSECOND" in message, message


def test_both_failure_paths_quote_one_list():
    """`INTERVAL_UNITS` is the single source; neither path may hand-roll its own."""
    units = [unit.upper() for unit in INTERVAL_UNITS]

    with pytest.raises(SqlError) as unimplemented:
        scalar("SELECT INTERVAL '7' century AS x")
    with pytest.raises(QueryParseError) as unparseable:
        scalar("SELECT INTERVAL '7' moments AS x")

    for message in (str(unimplemented.value), str(unparseable.value)):
        for unit in units:
            assert unit in message, message


def test_non_numeric_value_raises_sql_error():
    with pytest.raises(SqlError, match="not a whole number"):
        scalar("SELECT INTERVAL 'x' day AS x")


def test_plural_unit_in_a_predicate():
    """The reported shape: a plural unit inside a WHERE clause."""
    assert (
        scalar(
            f"SELECT COUNT(*) AS n FROM $planets WHERE {TS} > current_timestamp - INTERVAL '7' days"
        )
        is not None
    )


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
