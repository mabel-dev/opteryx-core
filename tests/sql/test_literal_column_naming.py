"""How an unaliased literal names its output column.

An expression with no alias is named after its own rendering, and that rendering
is produced by `_format_literal` in opteryx/expression/formatter.pyx. A literal's
`.type` is a `ColumnType`, so the PHYSICAL tag plus its descriptor is what
decides the spelling. That branch used to compare `.type` against
`LogicalCategory` members — comparisons that are permanently False since the type
unification — so every literal fell through to `str(value)` and was named with
the raw PHYSICAL value it is stored as:

    SELECT INTERVAL '1' DAY                  ->  (0, 86400000000)
    SELECT NULL                              ->  None
    SELECT CAST('2020-01-01' AS TIMESTAMP)   ->  1577836800000000
    SELECT CAST('2020-01-01' AS DATE)        ->  18262
    SELECT 'abc'                             ->  abc          (unquoted)

The second thing being defended here is INJECTIVITY. This rendering doubles as an
expression's identity — the binder resolves a literal against an existing column
by it, and the planner dedups projections by it — so two distinct literals that
render alike become one column carrying one value. Every "distinct" test below is
that property, not cosmetics.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx


def column_names(sql):
    session = opteryx.session()
    for morsel in session.execute_to_morsels(sql):
        return [
            n.decode("utf-8") if isinstance(n, bytes) else n for n in morsel.column_names
        ]
    return []


def one_name(sql):
    names = column_names(sql)
    assert len(names) == 1, names
    return names[0]


# ---------------------------------------------------------------------------
# The five that were named with their physical value
# ---------------------------------------------------------------------------


def test_interval_literal_is_named_as_an_interval():
    """Stored as a `(months, microseconds)` pair; named as the interval it is."""
    assert one_name("SELECT INTERVAL '1' DAY") == "1 DAY"
    assert one_name("SELECT INTERVAL '3' MONTH") == "3 MONTH"


def test_null_literal_is_named_null():
    """SQL's `null`, not Python's `None`."""
    assert one_name("SELECT NULL") == "null"


def test_typed_null_literal_is_also_named_null():
    """A typed NULL is physically VARCHAR but holds no value — the physical tag
    must not decide the rendering when there is nothing to render."""
    assert one_name("SELECT CAST(NULL AS VARCHAR)") == "null"
    assert one_name("SELECT CAST(NULL AS INTEGER)") == "null"


def test_timestamp_literal_is_named_as_a_timestamp():
    """Stored as int64 microseconds since the epoch."""
    assert one_name("SELECT CAST('2020-01-01' AS TIMESTAMP)") == (
        "TIMESTAMP '2020-01-01T00:00:00.000000'"
    )
    assert one_name("SELECT CAST('1969-07-20 20:17:40' AS TIMESTAMP)") == (
        "TIMESTAMP '1969-07-20T20:17:40.000000'"
    )


def test_date_literal_is_named_as_a_date():
    """Stored as days since the epoch. Pre-epoch dates are negative, so the
    calendar conversion has to floor rather than truncate."""
    assert one_name("SELECT CAST('2020-01-01' AS DATE)") == "DATE '2020-01-01'"
    assert one_name("SELECT CAST('1969-07-20' AS DATE)") == "DATE '1969-07-20'"


def test_string_literal_is_named_quoted():
    """Unquoted, a string literal is indistinguishable from anything else that
    renders the same text — an identifier, or an address."""
    assert one_name("SELECT 'abc'") == "'abc'"
    assert one_name("SELECT 'it''s'") == "'it''s'"


def test_time_literal_is_named_as_a_time():
    assert one_name("SELECT CAST('12:34:56' AS TIME)") == "TIME '12:34:56'"


# ---------------------------------------------------------------------------
# Injectivity — the rendering is also the expression's identity
# ---------------------------------------------------------------------------


def test_a_string_literal_is_distinct_from_an_address():
    """Both render the text `192.168.1.1`; quoting the string is what keeps them
    two columns. Unquoted they collide and the query dies as ambiguous."""
    assert column_names("SELECT '192.168.1.1', CAST('192.168.1.1' AS IPV4)") == [
        "'192.168.1.1'",
        "192.168.1.1",
    ]


def test_a_string_literal_is_distinct_from_the_number_it_spells():
    assert column_names("SELECT '42', 42") == ["'42'", "42"]


def test_a_string_literal_is_distinct_from_the_date_it_spells():
    """Quoting alone is not enough here — both would be `'2020-01-01'`. The type
    word is what separates them, so a temporal literal carries one."""
    assert column_names("SELECT '2020-01-01', CAST('2020-01-01' AS DATE)") == [
        "'2020-01-01'",
        "DATE '2020-01-01'",
    ]


def test_a_string_literal_is_distinct_from_the_time_it_spells():
    assert column_names("SELECT '12:34:56', CAST('12:34:56' AS TIME)") == [
        "'12:34:56'",
        "TIME '12:34:56'",
    ]


def test_two_distinct_dates_are_two_distinct_columns():
    assert column_names(
        "SELECT CAST('2020-01-01' AS DATE), CAST('2021-01-01' AS DATE)"
    ) == ["DATE '2020-01-01'", "DATE '2021-01-01'"]


def test_two_distinct_intervals_are_two_distinct_columns():
    assert column_names("SELECT INTERVAL '1' DAY, INTERVAL '2' DAY") == [
        "1 DAY",
        "2 DAY",
    ]


def test_two_distinct_strings_are_two_distinct_columns():
    assert column_names("SELECT 'a', 'b'") == ["'a'", "'b'"]


def test_quote_escaping_keeps_distinct_strings_distinct():
    """`a'b` and `a''b` must not both render `'a''b'` — doubling is applied to the
    VALUE's quotes, so the rendering stays injective."""
    assert column_names("SELECT 'a''b', 'a''''b'") == ["'a''b'", "'a''''b'"]


# ---------------------------------------------------------------------------
# Unchanged — these already named themselves sensibly
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT 1", "1"),
        ("SELECT 1.5", "1.5"),
        ("SELECT 3232235777", "3232235777"),
        ("SELECT CAST(3232235777 AS UINT32)", "3232235777"),
    ],
)
def test_numeric_literals_are_named_with_their_number(sql, expected):
    assert one_name(sql) == expected


def test_an_alias_outranks_every_rendering():
    for sql in (
        "SELECT INTERVAL '1' DAY AS x",
        "SELECT NULL AS x",
        "SELECT 'abc' AS x",
        "SELECT CAST('2020-01-01' AS TIMESTAMP) AS x",
        "SELECT CAST('2020-01-01' AS DATE) AS x",
    ):
        assert one_name(sql) == "x", sql


if __name__ == "__main__":  # pragma: no cover
    import pytest as _pytest

    raise SystemExit(_pytest.main([__file__, "-q"]))
