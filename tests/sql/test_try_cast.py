"""TRY_CAST — a row that cannot be converted becomes NULL instead of failing the query.

TRY_CAST was refused on a COLUMN for every target, not just some. The admission
table returned "no kernel" whenever `safe=True`, on the theory that TRY_CAST
would fall back to a Python closure — but this engine has no closure fallback, so
the plan compiler refused the query outright. The one exception was
CAST(json AS ARRAY<T>), whose kernel already took the disposition in its ctx.

That exception is now the rule: the disposition rides in the kernel's context
(`binary_op_ctx.safe`, or `format_ctx.safe` for the two pattern-parsing kernels),
so ONE kernel serves both dispositions. The alternative — a parallel
draken_cast_try_* family — would be two implementations of one conversion, free
to disagree about what "converts" means.

Two properties are defended throughout:

  1. TRY_CAST NULLs the failing rows and KEEPS the rest. Row-level, not
     query-level: a bad row must not poison a good one.
  2. Plain CAST still raises. If TRY_CAST and CAST behaved the same, the dialect
     would have no way to say "I want this to fail".

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx

_SESSION = opteryx.session()


def _col(sql, colname="x"):
    out = []
    for morsel in _SESSION.execute_to_morsels(sql):
        out.extend(morsel.column(colname).to_pylist())
    return out


# Targets whose conversion can fail from a string source — the dominant TRY_CAST
# use: "parse this text, null what does not parse".
UNPARSEABLE_TARGETS = [
    "INTEGER",
    "FLOAT64",
    "BOOLEAN",
    "DATE",
    "TIMESTAMP",
    "IPV4",
    "UINT8",
    "UINT64",
    "INT8",
    "INT16",
    "INT32",
    "FLOAT32",
]


@pytest.mark.parametrize("target", UNPARSEABLE_TARGETS)
def test_try_cast_nulls_an_unparseable_string_for_every_target(target):
    """$planets.name is never a number, a date or an address."""
    got = _col(f"SELECT TRY_CAST(name AS {target}) AS x FROM $planets")
    assert got == [None] * 9, (target, got)


@pytest.mark.parametrize("target", UNPARSEABLE_TARGETS)
def test_plain_cast_still_raises_for_every_target(target):
    """The counterpart property. A CAST that quietly nulled would leave the
    dialect no way to demand failure."""
    with pytest.raises(Exception):
        _col(f"SELECT CAST(name AS {target}) AS x FROM $planets")


def test_try_cast_is_row_level_not_query_level():
    """The whole point: bad rows null out, good rows survive, in place."""
    sql = (
        "SELECT TRY_CAST(v AS INTEGER) AS x FROM "
        "(SELECT CASE WHEN id > 6 THEN 'zz' WHEN id > 3 THEN '7' ELSE NULL END AS v "
        "FROM $planets) AS s"
    )
    assert _col(sql) == [None, None, None, 7, 7, 7, None, None, None]


def test_try_cast_nulls_out_of_range_values_per_row():
    """Not just parse failures — a value that does not FIT the target width is
    equally 'cannot convert'."""
    assert _col("SELECT TRY_CAST(id * 100 AS UINT8) AS x FROM $planets")[:4] == [
        100,
        200,
        None,
        None,
    ]
    assert _col("SELECT TRY_CAST(0 - id AS UINT32) AS x FROM $planets") == [None] * 9
    assert _col("SELECT TRY_CAST(id * 100 AS INT8) AS x FROM $planets")[:3] == [100, None, None]


def test_try_cast_survives_every_encoding_shape():
    """`bad` is indexed by PHYSICAL value, so a constant- or dict-shaped input
    has to map back through the selection to null the right LOGICAL rows. Getting
    that wrong nulls the wrong rows, which no error would ever reveal."""
    # constant shape: one physical value, nine logical rows
    assert _col("SELECT TRY_CAST(a AS INTEGER) AS x FROM (SELECT 'x' AS a FROM $planets) AS s") == [
        None
    ] * 9
    # two repeated physical values, alternating — only one of them is bad
    assert _col(
        "SELECT TRY_CAST(v AS UINT8) AS x FROM "
        "(SELECT CASE WHEN id % 2 = 0 THEN '300' ELSE '7' END AS v FROM $planets) AS s"
    ) == [7, None, 7, None, 7, None, 7, None, 7]


def test_try_cast_keeps_pre_existing_nulls_null():
    """A row that was already NULL stays NULL — it is not a conversion failure,
    and must not be reported as one either."""
    sql = (
        "SELECT TRY_CAST(v AS INTEGER) AS x FROM "
        "(SELECT CASE WHEN id > 4 THEN '5' ELSE NULL END AS v FROM $planets) AS s"
    )
    assert _col(sql) == [None, None, None, None, 5, 5, 5, 5, 5]


def test_try_cast_folds_an_out_of_range_literal_to_null():
    """The literal path has to reach the same verdict as the kernel. Parsing
    `300` SUCCEEDS, so the range check has to happen at bind time too — without
    it the value sailed through typed INT8 and died in the vector constructor,
    where TRY_CAST had no way to intervene."""
    assert _col("SELECT TRY_CAST(300 AS INT8) AS x") == [None]
    assert _col("SELECT TRY_CAST('300' AS UINT8) AS x") == [None]
    assert _col("SELECT TRY_CAST(-1 AS UINT8) AS x") == [None]
    assert _col("SELECT TRY_CAST('x' AS INTEGER) AS x") == [None]
    # …and the plain cast reports it readably, rather than as a bare OverflowError
    with pytest.raises(Exception) as err:
        _col("SELECT CAST(300 AS INT8) AS x")
    assert "out of range" in str(err.value), str(err.value)
    # the in-range literal is untouched
    assert _col("SELECT CAST(127 AS INT8) AS x") == [127]


def test_try_cast_over_a_conversion_that_cannot_fail_is_just_a_cast():
    """A widening has no failure mode, so the disposition is irrelevant to it —
    TRY_CAST must not make it null anything."""
    assert _col("SELECT TRY_CAST(id AS INTEGER) AS x FROM $planets")[:3] == [1, 2, 3]
    assert _col("SELECT TRY_CAST(id AS FLOAT64) AS x FROM $planets")[:3] == [1.0, 2.0, 3.0]
    assert _col("SELECT TRY_CAST(CAST(id AS UINT8) AS UINT64) AS x FROM $planets")[:3] == [1, 2, 3]


def test_try_cast_parses_the_rows_that_are_valid():
    """Guard against the lazy implementation that nulls everything."""
    assert _col("SELECT TRY_CAST(v AS IPV4) AS x FROM (SELECT '192.168.1.1' AS v) AS s") == [
        "192.168.1.1"
    ]
    assert _col("SELECT TRY_CAST(v AS DATE) AS x FROM (SELECT '2020-01-01' AS v) AS s")[
        0
    ].isoformat() == "2020-01-01"
    assert _col("SELECT TRY_CAST(v AS BOOLEAN) AS x FROM (SELECT 'yes' AS v) AS s") == [True]


def test_try_cast_to_decimal_nulls_what_will_not_fit():
    """The DECIMAL target's failures are a declared PRECISION/SCALE it cannot
    honour, not a parse — the same disposition has to reach that kernel too."""
    assert _col("SELECT TRY_CAST(gravity AS DECIMAL(4,0)) AS x FROM $planets")[:3] == [
        None,
        None,
        None,
    ]
    assert _col("SELECT TRY_CAST(id * 100000 AS DECIMAL(3,1)) AS x FROM $planets")[:3] == [
        None,
        None,
        None,
    ]
    with pytest.raises(Exception):
        _col("SELECT CAST(gravity AS DECIMAL(4,0)) AS x FROM $planets")
    # a rescale that DOES fit is untouched
    assert str(_col("SELECT CAST(gravity AS DECIMAL(12,3)) AS x FROM $planets")[0]) == "3.700"


def test_cast_null_to_a_numeric_type_carries_that_type():
    """A folded `CAST(NULL AS <numeric>)` is a TYPED null, not an untyped one.

    Untyped, it reached arithmetic as DRAKEN_NULL and there was no path: `10 /
    CAST(NULL AS FLOAT)` died with "cross-type vector arithmetic not supported",
    and `id + CAST(NULL AS INTEGER)` was refused at the compiler gate. The
    justification for leaving it untyped — that numeric kernels short-circuit on
    the DRAKEN_NULL tag — did not hold for the ARITHMETIC kernels.
    """
    from draken.draken_native import DrakenType

    def _type(sql):
        for morsel in _SESSION.execute_to_morsels(sql):
            return morsel.column("x").type
        return None

    assert _type("SELECT CAST(NULL AS FLOAT) AS x") == DrakenType.FLOAT64
    assert _type("SELECT CAST(NULL AS INT32) AS x") == DrakenType.INT32
    assert _type("SELECT CAST(NULL AS UINT8) AS x") == DrakenType.UINT8
    # the arithmetic those types exist to make possible
    assert _col("SELECT 10 / CAST(NULL AS FLOAT) AS x") == [None]
    assert _col("SELECT id + CAST(NULL AS INTEGER) AS x FROM $planets")[:3] == [None, None, None]


def test_null_stamping_leaves_the_descriptor_and_non_numeric_targets_alone():
    """IPV4's category is INTEGER but it carries a DESCRIPTOR, and whether a
    folded null should carry that is a separate question this change does not
    re-answer. VARBINARY and temporal targets are untouched for the reasons
    already recorded at the fold."""
    from draken.draken_native import DrakenType

    def _type(sql):
        for morsel in _SESSION.execute_to_morsels(sql):
            return morsel.column("x").type
        return None

    for sql in (
        "SELECT CAST(NULL AS IPV4) AS x",
        "SELECT CAST(NULL AS DATE) AS x",
        "SELECT CAST(NULL AS VARBINARY) AS x",
    ):
        assert _type(sql) == DrakenType.NULL, sql
    # VARCHAR was already stamped, and stays stamped
    assert _type("SELECT CAST(NULL AS VARCHAR) AS x") == DrakenType.VARCHAR


def test_binary_operators_are_unaffected_by_the_shared_context():
    """The disposition lives on binary_op_ctx, which every arithmetic and
    comparison op also uses. It must read as 0 there: an arithmetic overflow is
    not a value anyone asked to null out."""
    assert _col("SELECT id * 3 + 1 AS x FROM $planets")[:4] == [4, 7, 10, 13]
    assert _col("SELECT COUNT(*) AS x FROM $planets WHERE id * 2 > 10") == [4]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
