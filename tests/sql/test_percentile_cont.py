"""PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x [DESC]) — the exact percentile.

One native aggregate with MEDIAN (AggFn::Quantile in native_group_sinks.hpp;
MEDIAN is p = 0.5): every non-NULL value is buffered as a double and the answer
is selected with std::nth_element at finalize, interpolating linearly at rank
p * (n - 1) as fma(lo, 1 - d, hi * d), DuckDB's arithmetic
(QuantileState::finalize_quantile). FLOAT64, numeric operands only. Quantiles
over the same column in one aggregate share ONE buffer
(AggSpec2::quantile_owner).

DuckDB is the oracle where it is right. One place it is not: at exactly p = 0
with DESC, DuckDB 1.5.4 returns the MINIMUM; the standard (and PostgreSQL) put
rank 0 of a descending order at the maximum, and that is pinned here instead.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import math
import os
import random
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import duckdb
import pytest

import opteryx
from opteryx.exceptions import NotSupportedError, UnsupportedSyntaxError

_SESSION = opteryx.session()
_TINY_GLOB = os.path.join(os.path.dirname(__file__), "..", "..", "testdata", "clickbench_tiny", "*.parquet")


def _rows(sql):
    out = []
    for morsel in _SESSION.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            out.append(tuple(morsel[i]))
    return out


def _names(sql):
    names = None
    for morsel in _SESSION.execute_to_morsels(sql):
        names = [n.decode() if type(n) is bytes else n for n in morsel.column_names]
    return names


def _same(a, b):
    if a is None or b is None:
        return a is None and b is None
    if type(a) is float and type(b) is float and math.isnan(a) and math.isnan(b):
        return True
    return a == b


def _assert_rows_equal(got, expected):
    assert len(got) == len(expected), (got, expected)
    for g, e in zip(got, expected):
        assert len(g) == len(e) and all(_same(x, y) for x, y in zip(g, e)), (got, expected)


def _pc(p, column="x", direction=""):
    return f"PERCENTILE_CONT({p}) WITHIN GROUP (ORDER BY {column}{direction})"


# ---- interpolation --------------------------------------------------------------


def test_interpolation_ascending():
    sql = (
        f"SELECT {_pc(0)}, {_pc(0.25)}, {_pc(0.5)}, {_pc(0.75)}, {_pc(1)}, {_pc(0.1)} "
        "FROM (VALUES (1), (2), (3), (4)) AS t(x)"
    )
    # rank p * (n - 1) over 1, 2, 3, 4: 0, 0.75, 1.5, 2.25, 3, 0.3
    assert _rows(sql) == [(1.0, 1.75, 2.5, 3.25, 4.0, 1.3)]


def test_interpolation_descending_ranks_from_the_top():
    sql = (
        f"SELECT {_pc(0, direction=' DESC')}, {_pc(0.25, direction=' DESC')}, "
        f"{_pc(1, direction=' DESC')}, {_pc(0.1, direction=' DESC')} "
        "FROM (VALUES (1), (2), (3), (4)) AS t(x)"
    )
    # p = 0 DESC is the MAXIMUM (rank 0 of the descending order) — DuckDB says 1.
    assert _rows(sql) == [(4.0, 3.25, 1.0, 3.7)]


def test_explicit_asc_is_the_default_direction():
    # Same name either way, so one per query (a result cannot hold a column twice).
    values = "FROM (VALUES (5), (1), (9), (2)) AS t(x)"
    assert _rows(f"SELECT {_pc(0.3)} {values}") == _rows(f"SELECT {_pc(0.3, direction=' ASC')} {values}")


def test_single_value_is_every_percentile():
    sql = f"SELECT {_pc(0)}, {_pc(0.37)}, {_pc(1, direction=' DESC')} FROM (VALUES (7)) AS t(x)"
    assert _rows(sql) == [(7.0, 7.0, 7.0)]


def test_median_is_percentile_cont_half():
    sql = (
        f"SELECT MEDIAN(x), {_pc(0.5)} "
        "FROM (VALUES (1.5), (8.25), (3.0), (100.0), (-2.0), (4.5)) AS t(x)"
    )
    ((median, p50),) = _rows(sql)
    assert median == p50 == 3.75


def test_nan_ranks_above_every_value():
    sql = (
        f"SELECT {_pc(0.5)}, {_pc(1)}, {_pc(0, direction=' DESC')}, {_pc(0)} "
        "FROM (VALUES (1.0), (2.0), (CAST('nan' AS FLOAT64))) AS t(x)"
    )
    ((p50, p100, top, bottom),) = _rows(sql)
    assert p50 == 2.0 and bottom == 1.0
    assert math.isnan(p100) and math.isnan(top)


# ---- NULLs and empty input --------------------------------------------------------


def test_nulls_are_ignored():
    # A non-NULL first row: VALUES cannot type a column whose first value is NULL
    # (a separate, pre-existing limitation — COUNT fails on it the same way).
    sql = f"SELECT {_pc(0.5)}, COUNT(x) FROM (VALUES (10), (NULL), (20), (NULL), (30)) AS t(x)"
    assert _rows(sql) == [(20.0, 3)]


def test_all_null_input_is_null():
    sql = f"SELECT {_pc(0.5, 'id')} FROM (SELECT IIF(id > 0, NULL, id) AS id FROM $planets) AS t"
    assert _rows(sql) == [(None,)]


def test_empty_input_ungrouped_is_one_null_row():
    sql = f"SELECT {_pc(0.5, 'id')}, {_pc(0.9, 'id')} FROM $planets WHERE id > 100"
    assert _rows(sql) == [(None, None)]


def test_empty_input_grouped_is_no_rows():
    sql = f"SELECT name, {_pc(0.5, 'id')} FROM $planets WHERE id > 100 GROUP BY name"
    assert _rows(sql) == []


def test_grouped_all_null_group_is_null():
    sql = (
        f"SELECT g, {_pc(0.5)} FROM (VALUES (1, 10), (1, 20), (2, NULL), (2, NULL), (3, 5)) "
        "AS t(g, x) GROUP BY g ORDER BY g"
    )
    assert _rows(sql) == [(1, 15.0), (2, None), (3, 5.0)]


# ---- several percentiles, shared buffers --------------------------------------------


def test_several_percentiles_of_one_column_share_a_buffer():
    # MEDIAN and four PERCENTILE_CONTs (one DESC): one buffer, five answers.
    sql = (
        f"SELECT MEDIAN(id), {_pc(0.1, 'id')}, {_pc(0.9, 'id')}, {_pc(0.9, 'id', ' DESC')}, "
        f"{_pc(0.33, 'id')} FROM $planets"
    )
    values = sorted(r[0] for r in _rows("SELECT id FROM $planets"))
    got = _rows(sql)[0]

    def oracle(p, desc=False):
        ordered = list(reversed(values)) if desc else values
        rn = p * (len(ordered) - 1)
        lo, hi = ordered[math.floor(rn)], ordered[math.ceil(rn)]
        d = rn - math.floor(rn)
        return float(lo) if d == 0 else math.fma(lo, 1 - d, hi * d)

    assert got == (oracle(0.5), oracle(0.1), oracle(0.9), oracle(0.9, True), oracle(0.33))


def test_percentiles_of_different_columns_and_distinct_median():
    # MEDIAN(DISTINCT x) buffers the DEDUPED values, so it must not share x's buffer.
    sql = (
        f"SELECT {_pc(0.5)}, MEDIAN(DISTINCT x), {_pc(0.5, 'y')}, {_pc(0.25)} "
        "FROM (VALUES (1, 100), (1, 200), (1, 300), (4, 400)) AS t(x, y)"
    )
    assert _rows(sql) == [(1.0, 2.5, 250.0, 1.0)]


def test_grouped_several_percentiles():
    # $planets' measures are DECIMAL, which the exact quantile refuses — read as FLOAT64.
    source = (
        "(SELECT id % 3 AS k, CAST(gravity AS FLOAT64) AS gravity, "
        "CAST(density AS FLOAT64) AS density FROM $planets) AS t"
    )
    sql = (
        f"SELECT k, {_pc(0.5, 'gravity')}, {_pc(0.9, 'gravity')}, MEDIAN(gravity), "
        f"{_pc(0.9, 'density', ' DESC')} FROM {source} "
        "GROUP BY k ORDER BY k"
    )
    per_key = {}
    for key, gravity, density in _rows(f"SELECT k, gravity, density FROM {source}"):
        per_key.setdefault(key, ([], []))
        per_key[key][0].append(gravity)
        per_key[key][1].append(density)
    duck = duckdb.connect()
    expected = []
    for key in sorted(per_key):
        gravities, densities = per_key[key]
        g = ",".join(f"('{v!r}'::DOUBLE)" for v in gravities)
        d = ",".join(f"('{v!r}'::DOUBLE)" for v in densities)
        (p50, p90, med) = duck.sql(
            f"SELECT {_pc(0.5)}, {_pc(0.9)}, MEDIAN(x) FROM (VALUES {g}) t(x)"
        ).fetchone()
        (d90,) = duck.sql(f"SELECT {_pc(0.9, direction=' DESC')} FROM (VALUES {d}) t(x)").fetchone()
        expected.append((key, p50, p90, med, d90))
    _assert_rows_equal(_rows(sql), expected)


# ---- against DuckDB -----------------------------------------------------------------


def test_clickbench_extended_q54_q55_match_duckdb():
    q54 = (
        "SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY ResponseEndTiming), "
        "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ResponseEndTiming), "
        "PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY ResponseEndTiming) "
        "FROM {T} WHERE ResponseEndTiming > 0"
    )
    q55 = (
        "SELECT PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ResponseEndTiming) "
        "FROM {T} WHERE ResponseEndTiming > 0"
    )
    for sql in (q54, q55):
        got = _rows(sql.format(T="testdata.clickbench_tiny"))
        expected = duckdb.sql(sql.format(T=f"'{_TINY_GLOB}'")).fetchall()
        _assert_rows_equal(got, expected)
    # Pinned too, so the oracle cannot drift with it.
    assert _rows(q55.format(T="testdata.clickbench_tiny")) == [(2187.7600000000093,)]


def test_clickbench_grouped_matches_duckdb():
    sql = (
        "SELECT RegionID, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ResponseEndTiming), "
        "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ResolutionWidth DESC), "
        "MEDIAN(ResponseEndTiming) FROM {T} WHERE ResponseEndTiming > 0 "
        "GROUP BY RegionID ORDER BY RegionID"
    )
    got = _rows(sql.format(T="testdata.clickbench_tiny"))
    expected = [tuple(r) for r in duckdb.sql(sql.format(T=f"'{_TINY_GLOB}'")).fetchall()]
    assert len(got) > 10
    _assert_rows_equal(got, expected)


def test_random_floats_match_duckdb_bit_for_bit():
    rng = random.Random(20261023)
    duck = duckdb.connect()
    for _ in range(25):
        n = rng.randint(1, 80)
        # The first value is never NULL: VALUES cannot type a column from a NULL.
        values = [
            None if i and rng.random() < 0.1 else round(rng.uniform(-1e4, 1e4), rng.randint(0, 7))
            for i in range(n)
        ]
        groups = [rng.randint(0, 3) for _ in range(n)]
        direction = rng.choice(("", " DESC"))
        # p = 0 is left out under DESC: DuckDB answers it wrongly (see the module
        # docstring); test_interpolation_descending_ranks_from_the_top pins it.
        # Distinct fractions: a result cannot hold the same column twice.
        fractions = {1, 0.5} | {round(rng.random(), rng.randint(1, 4)) or 0.5 for _ in range(4)}
        if not direction:
            fractions.add(0)
        fractions = sorted(fractions)
        select = ", ".join(_pc(p, direction=direction) for p in fractions)
        ours = ", ".join(f"({g}, {'NULL' if v is None else repr(v)})" for g, v in zip(groups, values))
        # DuckDB reads `1.5::DOUBLE` as a DECIMAL first; from text it parses the
        # double directly, which is what our float literal does.
        theirs = ", ".join(
            f"({g}, {'NULL' if v is None else repr(str(v))}::DOUBLE)" for g, v in zip(groups, values)
        )
        for grouped in (False, True):
            head, tail = ("SELECT g, ", " GROUP BY g ORDER BY g") if grouped else ("SELECT ", "")
            got = _rows(f"{head}{select} FROM (VALUES {ours}) AS t(g, x){tail}")
            expected = [
                tuple(r)
                for r in duck.sql(f"{head}{select} FROM (VALUES {theirs}) AS t(g, x){tail}").fetchall()
            ]
            _assert_rows_equal(got, expected)


# ---- naming -------------------------------------------------------------------------


def test_column_is_named_as_written():
    names = _names(f"SELECT {_pc(0.25, 'id')}, {_pc(0.9, 'id', ' DESC')} FROM $planets")
    assert names == [
        "PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY id)",
        "PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY id DESC)",
    ]


# ---- refusals -----------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        # WITHIN GROUP on anything else used to be dropped silently.
        "SELECT SUM(id) WITHIN GROUP (ORDER BY name) FROM $planets",
        "SELECT MEDIAN(id) WITHIN GROUP (ORDER BY name DESC) FROM $planets",
        "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY id) FROM $planets",
        "SELECT PERCENTILE_CONT(0.5) FROM $planets",
        "SELECT PERCENTILE_CONT(id, 0.5) FROM $planets",
        "SELECT PERCENTILE_CONT(1.5) WITHIN GROUP (ORDER BY id) FROM $planets",
        "SELECT PERCENTILE_CONT(-0.5) WITHIN GROUP (ORDER BY id) FROM $planets",
        "SELECT PERCENTILE_CONT(id) WITHIN GROUP (ORDER BY id) FROM $planets",
        "SELECT PERCENTILE_CONT(NULL) WITHIN GROUP (ORDER BY id) FROM $planets",
        "SELECT PERCENTILE_CONT('0.5') WITHIN GROUP (ORDER BY id) FROM $planets",
        "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY id, name) FROM $planets",
        "SELECT PERCENTILE_CONT(DISTINCT 0.5) WITHIN GROUP (ORDER BY id) FROM $planets",
        "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY id NULLS LAST) FROM $planets",
        "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY id) FILTER (WHERE id > 2) FROM $planets",
        "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY id) OVER () FROM $planets",
    ],
)
def test_refused_shapes(sql):
    with pytest.raises(UnsupportedSyntaxError):
        _rows(sql)


@pytest.mark.parametrize(
    "operand",
    ["name", "CAST(id AS DECIMAL(10, 2))", "CAST('2020-01-01' AS DATE)"],
)
def test_non_numeric_operands_are_refused(operand):
    with pytest.raises(NotSupportedError):
        _rows(f"SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {operand}) FROM $planets")


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
