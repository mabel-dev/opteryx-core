# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Statistics coverage (docs/MANIFEST_SUM_STATISTIC_DESIGN.md §7, P3): the row groups
of an ungrouped aggregate's scan - parquet or skene - whose footer statistics prove
EVERY row satisfies the predicate are answered from those statistics at plan time:
left out of the scan, their partials seeded into the aggregate.

The oracle is the switch: every query runs with `disable_statistics_coverage` off
AND on, and the answers must be identical - value AND type (rows compared by repr).
Alongside it, whether coverage actually fired is asserted from the scan's reading,
so a right answer produced by reading everything cannot pass for the feature.

Refusals are part of the contract: a predicate or aggregate the statistics cannot
answer EXACTLY must cover nothing (floats, strings, UINT64 bounds, DISTINCT,
expressions), and a foreign writer's bounds are never trusted.

GROUP BY (P4): a covered row group in which every key column holds ONE value
(or only NULLs) is folded into that group's seed and merged by the GROUP BY sink
with the groups it sinks from the rest of the scan.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import shutil
import sys
from pathlib import Path

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pyarrow
import pyarrow.parquet as pq
import pytest

import opteryx
from rugo.parquet import write_parquet
from skene import SkeneWriter

_DATASET = "stats_coverage"
_DIR = Path("testdata") / _DATASET
_SKENE_DATASET = "stats_coverage_skene"
_SKENE_DIR = Path("testdata") / _SKENE_DATASET
_ROWS_PER_GROUP = 250

# 6000 rows over 3 files, 256-row row groups, sorted by k. n is NULL on
# [2000, 2300) - one boundary-straddling stretch of nulls.
_SOURCE = (
    "SELECT CAST(g AS INT32) AS k, "
    "CAST(g AS DATE) AS d, "
    "CAST(g % 1000 AS INT16) - 500 AS s, "
    "CAST(g AS UINT32) AS u, "
    "CAST(g AS UINT64) + CAST(18446744073709000000 AS UINT64) AS big, "
    "CASE WHEN g >= 2000 AND g < 2300 THEN NULL ELSE CAST(g AS INT64) * 3 END AS n, "
    "CAST(g AS FLOAT64) / 2 AS f, "
    "CAST(g % 7 AS VARCHAR) AS v, "
    # GROUP BY keys (P4): each constant over two whole row groups; bn is NULL
    # over the first two.
    "CAST(FLOOR((g - 1) / 500) AS INT32) AS b, "
    "CASE WHEN g <= 500 THEN NULL ELSE CAST(FLOOR((g - 1) / 500) AS UINT8) END AS bn, "
    "CAST(CAST(FLOOR((g - 1) / 500) AS INT32) AS DATE) AS bd, "
    # constant per file, so in every row group of either format
    "CAST(FLOOR((g - 1) / 2000) AS INT16) AS part "
    "FROM generate_series({a}, {b}) AS g"
)
_RANGES = [(1, 2000), (2001, 4000), (4001, 6000)]


@pytest.fixture(scope="module", autouse=True)
def _dataset():
    for directory in (_DIR, _SKENE_DIR):
        if directory.exists():
            shutil.rmtree(directory)
        directory.mkdir(parents=True)
    session = opteryx.session()
    for i, (a, b) in enumerate(_RANGES):
        morsel = list(session.execute_to_morsels(_SOURCE.format(a=a, b=b)))[0]
        with open(_DIR / f"part-{i}.parquet", "wb") as handle:
            handle.write(write_parquet(morsel, max_rows_per_row_group=_ROWS_PER_GROUP))
        # skene: one row group per _ROWS_PER_GROUP rows, statistics on
        writer = SkeneWriter(read_acceleration=True)
        for start in range(a, b + 1, _ROWS_PER_GROUP):
            end = min(start + _ROWS_PER_GROUP - 1, b)
            group = list(session.execute_to_morsels(_SOURCE.format(a=start, b=end)))[0]
            writer.add_row_group(group)
        writer.write_to(str(_SKENE_DIR / f"part-{i}.skene"))
    yield
    shutil.rmtree(_DIR)
    shutil.rmtree(_SKENE_DIR)


def _run(sql, enabled):
    """(rows as reprs, the scan's reading or None)."""
    session = opteryx.session()
    setting = f"SET disable_statistics_coverage = {'false' if enabled else 'true'}"
    for _ in session.execute_to_morsels(setting):
        pass
    rows = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            rows.append(tuple(repr(value) for value in morsel[i]))
    readings = [r for r in session.telemetry.get("operations", {}).values() if "row_groups_read" in r]
    return rows, (readings[0] if len(readings) == 1 else None)


def _oracle(sql):
    """Coverage on vs off: identical answers. Returns the ON arm's scan reading."""
    on_rows, on_reading = _run(sql, True)
    off_rows, off_reading = _run(sql, False)
    assert on_rows == off_rows, f"coverage changed the answer\n{sql}\non:  {on_rows}\noff: {off_rows}"
    assert off_reading is None or "row_groups_answered_from_statistics" not in off_reading
    return on_reading


def _covered(reading):
    return 0 if reading is None else reading.get("row_groups_answered_from_statistics", 0)


# Every oracle runs against both formats: the parquet adapter and the skene one
# share the classifier and the seed, and must agree with a full read alike.
_TABLES = [f"testdata.{_DATASET}", f"testdata.{_SKENE_DATASET}"]


@pytest.mark.parametrize("_T", _TABLES)
@pytest.mark.parametrize(
    "where",
    [
        "k >= 1000",
        "k > 999 AND k <= 5000",
        "k BETWEEN 1000 AND 4000",
        "k < 3500",
        "k <> 3000",
        "n IS NOT NULL",
        "k >= 1000 AND n IS NOT NULL",
        "d >= CAST('1970-02-01' AS DATE)",
    ],
)
def test_covered_row_groups_answer_exactly(_T, where):
    reading = _oracle(
        f"SELECT COUNT(*), COUNT(n), SUM(s), AVG(u), SUM(n), MIN(d), MAX(k), MIN(s), MAX(u) "
        f"FROM {_T} WHERE {where}"
    )
    assert _covered(reading) > 0, f"nothing was answered from statistics for WHERE {where}"


@pytest.mark.parametrize("_T", _TABLES)
def test_every_row_group_covered_still_types_the_answer(_T):
    # All row groups covered: the scan reads nothing, so no morsel ever reaches the
    # aggregate - its output types come from the seed alone and must match.
    reading = _oracle(f"SELECT MIN(d), MAX(s), SUM(u), AVG(k), COUNT(*) FROM {_T} WHERE k >= 1")
    assert reading is not None and reading["row_groups_read"] == 0
    assert _covered(reading) > 0


@pytest.mark.parametrize("_T", _TABLES)
def test_a_predicate_matching_nothing_answers_empty_aggregates(_T):
    _oracle(f"SELECT COUNT(*), SUM(s), MIN(d) FROM {_T} WHERE k > 100000")


@pytest.mark.parametrize("_T", _TABLES)
def test_a_sum_past_int64_raises_the_engines_overflow_either_way(_T):
    for enabled in (True, False):
        with pytest.raises(Exception, match="SUM overflow"):
            _run(f"SELECT SUM(big) FROM {_T} WHERE k >= 1", enabled)


@pytest.mark.parametrize("_T", _TABLES)
@pytest.mark.parametrize(
    "sql",
    [
        # a conjunct with no exact term: floats, strings, expressions
        "SELECT COUNT(*), MAX(k) FROM {T} WHERE f > 100.0",
        "SELECT COUNT(*), MAX(k) FROM {T} WHERE v <> '3'",
        "SELECT COUNT(*), MAX(k) FROM {T} WHERE k >= 1000 AND v = '2'",
        "SELECT COUNT(*), MAX(k) FROM {T} WHERE k + 1 > 1000",
        # an aggregate the statistics cannot answer
        "SELECT SUM(f) FROM {T} WHERE k >= 1000",
        "SELECT MIN(big) FROM {T} WHERE k >= 1000",
        "SELECT MIN(v) FROM {T} WHERE k >= 1000",
        "SELECT COUNT(DISTINCT s) FROM {T} WHERE k >= 1000",
        "SELECT SUM(k + 1), COUNT(*) FROM {T} WHERE k >= 1000 AND k * 2 > 0",
    ],
)
def test_what_the_statistics_cannot_answer_is_read(_T, sql):
    reading = _oracle(sql.format(T=_T))
    assert _covered(reading) == 0


@pytest.mark.parametrize("_T", _TABLES)
def test_in_list_covers_only_single_valued_row_groups_and_answers_right(_T):
    # Every row group holds many k values, so an IN list covers none - but its
    # exact term still proves the non-matching row groups empty.
    reading = _oracle(f"SELECT COUNT(*), SUM(s) FROM {_T} WHERE k IN (10, 3000, 5999)")
    assert _covered(reading) == 0


def test_a_foreign_writers_bounds_are_not_trusted():
    ds = Path("testdata") / "stats_coverage_foreign"
    if ds.exists():
        shutil.rmtree(ds)
    ds.mkdir(parents=True)
    try:
        pq.write_table(
            pyarrow.table({"k": pyarrow.array(range(1, 3001), pyarrow.int32())}),
            ds / "arrow.parquet",
            row_group_size=500,
        )
        reading = _oracle("SELECT COUNT(*), MAX(k), MIN(k) FROM testdata.stats_coverage_foreign WHERE k >= 1000")
        assert _covered(reading) == 0
    finally:
        shutil.rmtree(ds)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))


# ---- GROUP BY (P4) ----------------------------------------------------------

_GROUPED_AGGS = "COUNT(*), COUNT(n), SUM(s), AVG(u), SUM(n), MIN(d), MAX(k), MIN(s), MAX(u)"


@pytest.mark.parametrize("_T", _TABLES)
@pytest.mark.parametrize(
    "where",
    [
        "k > 0",
        # a boundary row group (1001-1250) shares bucket 2 with a covered one:
        # the seeded group and the sunk group must merge into one
        "k >= 1100",
        "k BETWEEN 1100 AND 4900",
        "n IS NOT NULL",
        "d >= CAST('1970-02-01' AS DATE)",
    ],
)
@pytest.mark.parametrize("keys", ["b", "bn", "bd", "b, bn", "bn, bd, b"])
def test_single_valued_row_groups_seed_their_groups(_T, where, keys):
    reading = _oracle(
        f"SELECT {keys}, {_GROUPED_AGGS} FROM {_T} WHERE {where} GROUP BY {keys} ORDER BY {keys}"
    )
    assert _covered(reading) > 0, f"nothing was answered from statistics for GROUP BY {keys}"


@pytest.mark.parametrize("_T", _TABLES)
def test_every_row_group_seeded_still_types_the_groups(_T):
    # Every row group single-valued in `part`: the scan reads nothing, so no
    # morsel types the sink - the seed must.
    reading = _oracle(f"SELECT part, COUNT(*), SUM(s), MIN(d), MAX(u) FROM {_T} GROUP BY part ORDER BY part")
    assert _covered(reading) == 24
    assert reading.get("records_out", 0) == 0


@pytest.mark.parametrize("_T", _TABLES)
@pytest.mark.parametrize(
    "sql",
    [
        # HAVING filters seeded and sunk groups alike
        "SELECT b, SUM(n) FROM {T} WHERE k >= 1100 GROUP BY b HAVING SUM(n) > 2000000 ORDER BY b",
        # GROUP BY -> ORDER BY aggregate LIMIT: the sink's top-k cut ranks seeded groups
        "SELECT b, COUNT(n) AS c FROM {T} WHERE k >= 1100 GROUP BY b ORDER BY c DESC, b LIMIT 3",
        "SELECT bn, MAX(k) AS m FROM {T} GROUP BY bn ORDER BY m LIMIT 2",
        # a key that is only hashed, never emitted
        "SELECT COUNT(*), SUM(s) FROM {T} GROUP BY b ORDER BY 2",
    ],
)
def test_seeded_groups_flow_through_having_topk_and_hash_only_keys(_T, sql):
    reading = _oracle(sql.format(T=_T))
    assert _covered(reading) > 0


@pytest.mark.parametrize("_T", _TABLES)
@pytest.mark.parametrize(
    "sql",
    [
        # the key varies inside every row group: read, never guessed
        "SELECT k, COUNT(*) FROM {T} WHERE k < 300 GROUP BY k ORDER BY k",
        # keys whose bounds are not their values
        "SELECT f, COUNT(*) FROM {T} WHERE k < 300 GROUP BY f ORDER BY f",
        "SELECT v, COUNT(*) FROM {T} GROUP BY v ORDER BY v",
        "SELECT big, COUNT(*) FROM {T} WHERE k < 300 GROUP BY big ORDER BY big",
        # a computed key
        "SELECT b + 1, COUNT(*) FROM {T} GROUP BY b + 1 ORDER BY 1",
        # an aggregate the statistics cannot answer
        "SELECT b, COUNT(DISTINCT s) FROM {T} GROUP BY b ORDER BY b",
        # grouping sets add a key with no statistics
        "SELECT b, COUNT(*) FROM {T} GROUP BY ROLLUP(b) ORDER BY b",
    ],
)
def test_what_grouped_statistics_cannot_answer_is_read(_T, sql):
    reading = _oracle(sql.format(T=_T))
    assert _covered(reading) == 0
