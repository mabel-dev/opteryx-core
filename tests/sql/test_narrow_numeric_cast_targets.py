"""CAST to INT8 / INT16 / INT32 / FLOAT32.

These four widths could be READ (a Parquet file or a catalog schema hands the
engine an INT8 column) but never ASKED FOR: `CAST(x AS INT32)` was rejected by
the planner. The reason it stayed rejected for so long is worth stating, because
it is the thing these tests defend:

    The cast target arm mapped INT8/INT16/INT32 onto INT64-PRODUCING kernels and
    FLOAT32 onto a FLOAT64-producing one. Simply accepting the SQL name would
    have DECLARED INT32 and PRODUCED INT64 — a declared-vs-actual divergence,
    which is worse than the refusal, because nothing downstream would notice.

So every test here checks the ACTUAL vector type alongside the value.

⚠ INT8 MEANS EIGHT BITS. Postgres spells BIGINT as `int8` (eight BYTES); this
engine's vocabulary is INT8/INT16/INT32/INT64 by bit width throughout (draken's
DrakenType, `str(ColumnType)`, the catalog's stored names), and INT8 meaning
eight bytes next to INT64 meaning eight bytes would be indefensible. TINYINT and
REAL remain rejected — as aliases they now suggest the exact width.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx
from draken.draken_native import DrakenType
from opteryx.exceptions import SqlError

_SESSION = opteryx.session()


def _typed(sql, colname="x"):
    """(physical type, values) — the type is half the assertion."""
    values = []
    physical = None
    for morsel in _SESSION.execute_to_morsels(sql):
        column = morsel.column(colname)
        physical = column.type
        values.extend(column.to_pylist())
    return physical, values


NARROW = [
    ("INT8", DrakenType.INT8),
    ("INT16", DrakenType.INT16),
    ("INT32", DrakenType.INT32),
]


@pytest.mark.parametrize("target, physical", NARROW)
def test_narrow_int_target_produces_that_exact_width(target, physical):
    got_type, values = _typed(f"SELECT CAST(id AS {target}) AS x FROM $planets")
    assert got_type == physical, (target, got_type)
    assert values[:4] == [1, 2, 3, 4], values[:4]


def test_float32_target_produces_float32():
    got_type, values = _typed("SELECT CAST(id AS FLOAT32) AS x FROM $planets")
    assert got_type == DrakenType.FLOAT32, got_type
    assert values[:3] == [1.0, 2.0, 3.0], values[:3]


@pytest.mark.parametrize("target, physical", NARROW)
def test_every_source_family_reaches_the_narrow_int_targets(target, physical):
    """Signed, unsigned, float, bool, string and decimal — the same six families
    the unsigned targets take."""
    sources = {
        "signed": "SELECT CAST(id AS {t}) AS x FROM $planets",
        "unsigned": "SELECT CAST(CAST(id AS UINT32) AS {t}) AS x FROM $planets",
        "float": "SELECT CAST(CAST(id AS FLOAT64) AS {t}) AS x FROM $planets",
        "bool": "SELECT CAST(a AS {t}) AS x FROM (SELECT true AS a) AS s",
        "string": "SELECT CAST(a AS {t}) AS x FROM (SELECT '7' AS a) AS s",
        "decimal": "SELECT CAST(gravity AS {t}) AS x FROM $planets",
    }
    for family, sql in sources.items():
        got_type, values = _typed(sql.format(t=target))
        assert got_type == physical, (target, family, got_type)
        assert values and values[0] is not None, (target, family)


def test_every_source_family_reaches_float32():
    sources = (
        "SELECT CAST(id AS FLOAT32) AS x FROM $planets",
        "SELECT CAST(CAST(id AS UINT32) AS FLOAT32) AS x FROM $planets",
        "SELECT CAST(CAST(id AS FLOAT64) AS FLOAT32) AS x FROM $planets",
        "SELECT CAST(a AS FLOAT32) AS x FROM (SELECT true AS a) AS s",
        "SELECT CAST(a AS FLOAT32) AS x FROM (SELECT '2.5' AS a) AS s",
        "SELECT CAST(gravity AS FLOAT32) AS x FROM $planets",
    )
    for sql in sources:
        got_type, values = _typed(sql)
        assert got_type == DrakenType.FLOAT32, (sql, got_type)
        assert values and values[0] is not None, sql


def test_narrowing_is_range_checked_never_wrapped():
    """id*100 exceeds INT8 for most planets. 900 must not become -116."""
    with pytest.raises(Exception):
        _typed("SELECT CAST(id * 100 AS INT8) AS x FROM $planets")
    with pytest.raises(Exception):
        _typed("SELECT CAST(a AS INT16) AS x FROM (SELECT 40000 AS a) AS s")
    with pytest.raises(Exception):
        _typed("SELECT CAST(a AS INT8) AS x FROM (SELECT '300' AS a) AS s")


def test_narrow_int_boundaries_are_inclusive():
    """The edge values themselves must pass — an off-by-one in the range check
    would only ever show up here."""
    for target, low, high in (
        ("INT8", -128, 127),
        ("INT16", -32768, 32767),
        ("INT32", -2147483648, 2147483647),
    ):
        for value in (low, high):
            _, values = _typed(f"SELECT CAST(a AS {target}) AS x FROM (SELECT {value} AS a) AS s")
            assert values == [value], (target, value, values)


def test_float32_loses_precision_but_does_not_lose_the_number():
    """Precision loss IS the type's contract; magnitude loss is not."""
    _, values = _typed("SELECT CAST(a AS FLOAT32) AS x FROM (SELECT 3.14159265358979 AS a) AS s")
    assert abs(values[0] - 3.14159265358979) < 1e-6
    assert values[0] != 3.14159265358979  # it really did narrow
    with pytest.raises(Exception):
        _typed("SELECT CAST(a AS FLOAT32) AS x FROM (SELECT 1.0e300 AS a) AS s")


def test_float32_widens_back_to_double_through_a_kernel():
    """FLOAT32 → FLOAT64 is a WIDENING, not a retag: 4-byte and 8-byte payloads.
    It was previously listed as an identity passthrough, which the compiler gate
    refused — the refusal was the only thing between that entry and a 4-byte
    buffer being read at an 8-byte stride."""
    got_type, values = _typed(
        "SELECT CAST(CAST(id AS FLOAT32) AS FLOAT64) AS x FROM $planets"
    )
    assert got_type == DrakenType.FLOAT64, got_type
    assert values[:3] == [1.0, 2.0, 3.0], values[:3]


def test_narrow_results_survive_the_rest_of_the_engine():
    """A narrow column is only useful if the operators downstream accept it."""
    assert _typed("SELECT COUNT(*) AS x FROM $planets WHERE CAST(id AS INT32) > 4")[1] == [5]
    assert _typed("SELECT SUM(CAST(id AS INT32)) AS x FROM $planets")[1] == [45]
    assert _typed("SELECT CAST(id AS INT8) AS x FROM $planets ORDER BY x DESC")[1][0] == 9
    assert len(_typed("SELECT DISTINCT CAST(id AS FLOAT32) AS x FROM $planets")[1]) == 9
    # …and back out again, through the widths that already worked.
    assert _typed("SELECT CAST(CAST(id AS INT32) AS VARCHAR) AS x FROM $planets")[1][:2] == [
        "1",
        "2",
    ]


def test_narrow_casts_preserve_nulls():
    sql = (
        "SELECT CAST(d AS {t}) AS x FROM "
        "(SELECT CASE WHEN id > 4 THEN id ELSE NULL END AS d FROM $planets) AS s"
    )
    for target in ("INT8", "INT16", "INT32", "FLOAT32"):
        _, values = _typed(sql.format(t=target))
        assert values[:4] == [None, None, None, None], (target, values[:4])
        assert values[4] is not None, (target, values[4])


def test_float_is_double_and_float_precision_picks_the_width():
    """FLOAT means DOUBLE PRECISION here — the same meaning the schema reader
    gives it, and re-pointing it at FLOAT32 would narrow every stored FLOAT
    column. FLOAT(p) follows the standard's binary precision instead of dropping
    the argument: p<=24 is single, above it double."""
    assert _typed("SELECT CAST(id AS FLOAT) AS x FROM $planets")[0] == DrakenType.FLOAT64
    assert _typed("SELECT CAST(id AS FLOAT(53)) AS x FROM $planets")[0] == DrakenType.FLOAT64
    assert _typed("SELECT CAST(id AS FLOAT(24)) AS x FROM $planets")[0] == DrakenType.FLOAT32


@pytest.mark.parametrize(
    "alias, suggestion",
    [("TINYINT", "INT8"), ("SMALLINT", "INT16"), ("REAL", "FLOAT32"), ("NUMERIC", "DECIMAL")],
)
def test_aliases_are_still_rejected_but_now_name_the_exact_width(alias, suggestion):
    """TINYINT is not a name in this dialect — but the error should send the
    reader to INT8, not to INTEGER, which is a different (wider) type."""
    with pytest.raises(SqlError) as err:
        _typed(f"SELECT CAST(1 AS {alias}) AS x")
    assert suggestion in str(err.value), str(err.value)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
