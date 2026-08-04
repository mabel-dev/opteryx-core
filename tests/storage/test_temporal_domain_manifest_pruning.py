# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
End-to-end: manifest file pruning must not compare temporal values across raw
integer domains.

File bounds hold a temporal column's RAW physical integer - DATE32 stores days
since the epoch, TIMESTAMP64 stores unit-scaled ticks - and a predicate literal
arrives as its own raw integer. Compare those directly and ~20_000 days sits
beside ~1.7e15 microseconds; every file fails the bound test and the query
returns zero rows with no error at all.

Measured on this dataset before the guard existed, with a real ANALYZE manifest:

    date_added   >= CAST('2025-01-01' AS DATE)             -> 6 rows   correct
    date_added   >= CAST('2025-01-01T00:00:00' AS TIMESTAMP) -> 0 rows, 3 files pruned
    published_at <  CAST('2025-01-01T00:00:00' AS TIMESTAMP) -> 3 rows   correct
    published_at <  CAST('2025-01-01' AS DATE)             -> 0 rows, 3 files pruned

Both directions fail, with opposite operators: a microsecond literal against day
bounds kills `>`/`>=`/`=`, and a day literal against microsecond bounds kills
`<`/`<=`. Checking one direction with one operator - which is how this survived
- looks like a clean pass.

These tests write real Parquet files under a throwaway `testdata/` subdirectory
and run `ANALYZE TABLE` (needed for real lower_bounds/upper_bounds via this
connector - see filesystem_connector.py), so the bounds under test are the ones
a catalog actually produces, ordinal-encoded, not hand-built ints.

Every assertion is against a ground truth computed in Python from the source
rows, NOT merely against the other literal form. Two forms agreeing on zero rows
would otherwise be a passing test for a totally broken engine.

The telemetry assertions are the other half of the contract. Declining the
pushdown is the fix, so `files_pruned == 0` on a cross-domain predicate is
correct - but the same-domain cases MUST still prune, or "no wrong answers" was
bought by turning the optimisation off everywhere.
"""

import datetime
import os
import shutil
import sys
from pathlib import Path

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from rugo.parquet import write_parquet

DATASET_DIR = "_tmp_temporal_domain"

# Three files with disjoint, ordered date ranges - the shape that makes file
# pruning both possible and observable. `n` identifies the row so assertions can
# compare row identity, not just a count that could match by coincidence.
ROWS = [
    # (n, date)
    (1, datetime.date(2023, 1, 1)),
    (2, datetime.date(2023, 6, 15)),
    (3, datetime.date(2023, 12, 31)),
    (4, datetime.date(2025, 1, 1)),
    (5, datetime.date(2025, 6, 15)),
    (6, datetime.date(2025, 12, 31)),
    (7, datetime.date(2026, 4, 1)),
    (8, datetime.date(2026, 6, 15)),
    (9, datetime.date(2026, 8, 3)),
]
FILE_SIZE = 3
TIME_OF_DAY = datetime.time(12, 30, 15)

# Boundaries chosen to hit every interesting position: before all data, exactly
# on a file's first row, mid-file, exactly on the last row, and after all data.
BOUNDARIES = ("2022-01-01", "2025-01-01", "2025-06-15", "2026-08-03", "2027-01-01")

# (SQL operator, python operator name) - every comparison the pruner handles.
OPERATORS = (">=", ">", "<", "<=", "=", "!=")


def _values_sql(batch):
    tuples = ", ".join(
        f"('{d.isoformat()}', '{d.isoformat()}T{TIME_OF_DAY.isoformat()}', {n})" for n, d in batch
    )
    return (
        "SELECT CAST(d AS DATE) AS date_added, "
        "CAST(t AS TIMESTAMP) AS published_at, n "
        f"FROM (VALUES {tuples}) AS v(d, t, n)"
    )


@pytest.fixture(scope="module")
def dataset():
    """A three-file, ANALYZE'd dataset. Built once - ANALYZE is the expensive
    part and nothing in these tests mutates the data."""
    directory = Path("testdata") / DATASET_DIR
    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True)
    session = opteryx.session()
    for index in range(0, len(ROWS), FILE_SIZE):
        batch = ROWS[index : index + FILE_SIZE]
        morsel = list(session.execute_to_morsels(_values_sql(batch)))[0]
        morsel.materialize()
        (directory / f"part-{index // FILE_SIZE}.parquet").write_bytes(write_parquet(morsel))
    name = f"testdata.{DATASET_DIR}"
    list(session.execute_to_morsels(f"ANALYZE TABLE {name}"))
    try:
        yield name
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def _query(sql):
    """Returns (sorted `n` values, files_pruned)."""
    session = opteryx.session()
    values = []
    for morsel in session.execute_to_morsels(sql):
        morsel.materialize()
        values.extend(morsel.column(b"n").to_pylist())
    return sorted(values), dict(session.telemetry).get("files_pruned", 0)


def _compare(left, operator, right):
    if operator == ">=":
        return left >= right
    if operator == ">":
        return left > right
    if operator == "<":
        return left < right
    if operator == "<=":
        return left <= right
    if operator == "=":
        return left == right
    if operator == "!=":
        return left != right
    raise AssertionError(f"unhandled operator {operator}")


def _expected(column, operator, boundary, time_of_day=None):
    """Ground truth, computed from ROWS rather than from the engine.

    A DATE column compared against a datetime is promoted to midnight on that
    date, which is what makes a midnight literal equivalent to the date-only
    form and a non-midnight literal deliberately NOT equivalent.
    """
    limit_date = datetime.date.fromisoformat(boundary)
    limit = datetime.datetime.combine(limit_date, time_of_day or datetime.time(0, 0, 0))
    matched = []
    for n, date in ROWS:
        if column == "date_added":
            value = datetime.datetime.combine(date, datetime.time(0, 0, 0))
        else:
            value = datetime.datetime.combine(date, TIME_OF_DAY)
        if _compare(value, operator, limit):
            matched.append(n)
    return sorted(matched)


# ---------------------------------------------------------------------------
# DATE column vs a full datetime literal - the reported failure.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("boundary", BOUNDARIES)
@pytest.mark.parametrize("operator", OPERATORS)
def test_date_column_datetime_literal_matches_date_literal(dataset, operator, boundary):
    date_form, _ = _query(
        f"SELECT * FROM {dataset} WHERE date_added {operator} CAST('{boundary}' AS DATE)"
    )
    datetime_form, _ = _query(
        f"SELECT * FROM {dataset} "
        f"WHERE date_added {operator} CAST('{boundary}T00:00:00' AS TIMESTAMP)"
    )
    expected = _expected("date_added", operator, boundary)

    assert date_form == expected, f"date-only literal is wrong for {operator} {boundary}"
    assert datetime_form == expected, f"datetime literal is wrong for {operator} {boundary}"


# ---------------------------------------------------------------------------
# TIMESTAMP column vs a date-only literal - same defect, opposite operators.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("boundary", BOUNDARIES)
@pytest.mark.parametrize("operator", OPERATORS)
def test_timestamp_column_date_literal_matches_timestamp_literal(dataset, operator, boundary):
    timestamp_form, _ = _query(
        f"SELECT * FROM {dataset} "
        f"WHERE published_at {operator} CAST('{boundary}T00:00:00' AS TIMESTAMP)"
    )
    date_form, _ = _query(
        f"SELECT * FROM {dataset} WHERE published_at {operator} CAST('{boundary}' AS DATE)"
    )
    expected = _expected("published_at", operator, boundary)

    assert timestamp_form == expected, f"timestamp literal is wrong for {operator} {boundary}"
    assert date_form == expected, f"date-only literal is wrong for {operator} {boundary}"


# ---------------------------------------------------------------------------
# A non-midnight literal is NOT equivalent to the date-only form, and must not
# be "fixed" into equivalence. DATE promotes to midnight, so 2025-06-15 sits
# strictly before 2025-06-15T12:00:00.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("operator", OPERATORS)
def test_non_midnight_literal_against_date_column_keeps_midnight_semantics(dataset, operator):
    rows, _ = _query(
        f"SELECT * FROM {dataset} "
        f"WHERE date_added {operator} CAST('2025-06-15T12:00:00' AS TIMESTAMP)"
    )

    assert rows == _expected("date_added", operator, "2025-06-15", datetime.time(12, 0, 0))


# ---------------------------------------------------------------------------
# Pruning still has to work. "No wrong answers" is trivially satisfiable by
# never pruning anything, which would be a silent performance regression across
# every temporal query in the product.
# ---------------------------------------------------------------------------


def test_same_domain_predicate_still_prunes_files(dataset):
    rows, pruned = _query(f"SELECT * FROM {dataset} WHERE date_added >= CAST('2026-01-01' AS DATE)")

    assert rows == [7, 8, 9]
    assert pruned == 2, f"expected the two older files to be pruned, got {pruned}"


def test_same_domain_timestamp_predicate_still_prunes_files(dataset):
    rows, pruned = _query(
        f"SELECT * FROM {dataset} WHERE published_at < CAST('2024-01-01T00:00:00' AS TIMESTAMP)"
    )

    assert rows == [1, 2, 3]
    assert pruned == 2


def test_cross_domain_predicate_declines_pruning_but_answers_correctly(dataset):
    # The trade the fix makes, stated explicitly: one avoidable scan instead of
    # a wrong answer. If this ever starts pruning again, the bounds comparison
    # has become type-aware enough to be trusted - and this assertion, not a
    # silent 0-row response, is what should fail first.
    rows, pruned = _query(
        f"SELECT * FROM {dataset} WHERE date_added >= CAST('2026-01-01T00:00:00' AS TIMESTAMP)"
    )

    assert rows == [7, 8, 9]
    assert pruned == 0


# ---------------------------------------------------------------------------
# Compound predicates. AND-conjuncts are pushed down individually, so a mixed
# pair must prune on the safe half and still read everything the unsafe half
# needs; BETWEEN is the other shape prune_files handles.
# ---------------------------------------------------------------------------


def test_mixed_safe_and_unsafe_conjuncts(dataset):
    rows, _ = _query(
        f"SELECT * FROM {dataset} "
        f"WHERE date_added >= CAST('2025-01-01' AS DATE) "
        f"AND date_added < CAST('2026-06-15T00:00:00' AS TIMESTAMP)"
    )

    assert rows == [4, 5, 6, 7]


def test_between_with_datetime_literals(dataset):
    rows, _ = _query(
        f"SELECT * FROM {dataset} WHERE date_added "
        f"BETWEEN CAST('2025-01-01T00:00:00' AS TIMESTAMP) "
        f"AND CAST('2026-06-15T00:00:00' AS TIMESTAMP)"
    )

    assert rows == [4, 5, 6, 7, 8]


def test_between_with_date_literals_still_matches(dataset):
    rows, _ = _query(
        f"SELECT * FROM {dataset} WHERE date_added "
        f"BETWEEN CAST('2025-01-01' AS DATE) AND CAST('2026-06-15' AS DATE)"
    )

    assert rows == [4, 5, 6, 7, 8]


def test_disjunction_of_cross_domain_predicates(dataset):
    # An OR is not pushed down at all, so it was already correct pre-fix - it is
    # how the bug was localised to pruning rather than to evaluation. Keep it as
    # the control: evaluation and pruning must now agree.
    rows, _ = _query(
        f"SELECT * FROM {dataset} "
        f"WHERE date_added >= CAST('2026-01-01T00:00:00' AS TIMESTAMP) "
        f"OR date_added < CAST('2023-06-15T00:00:00' AS TIMESTAMP)"
    )

    assert rows == [1, 7, 8, 9]


def test_negated_cross_domain_predicate(dataset):
    rows, _ = _query(
        f"SELECT * FROM {dataset} "
        f"WHERE NOT (date_added >= CAST('2026-01-01T00:00:00' AS TIMESTAMP))"
    )

    assert rows == [1, 2, 3, 4, 5, 6]


# ---------------------------------------------------------------------------
# Aggregates and ORDER BY/LIMIT read through different operators but the same
# pruned file list, so a wrong prune shows up as a wrong scalar rather than as
# missing rows.
# ---------------------------------------------------------------------------


def test_count_over_cross_domain_predicate(dataset):
    session = opteryx.session()
    counts = []
    for morsel in session.execute_to_morsels(
        f"SELECT COUNT(*) AS c FROM {dataset} "
        f"WHERE date_added >= CAST('2025-01-01T00:00:00' AS TIMESTAMP)"
    ):
        morsel.materialize()
        counts.extend(morsel.column(b"c").to_pylist())

    assert counts == [6]


def test_min_max_over_cross_domain_predicate(dataset):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(
        f"SELECT MIN(n) AS lo, MAX(n) AS hi FROM {dataset} "
        f"WHERE published_at <= CAST('2025-12-31' AS DATE)"
    ):
        morsel.materialize()
        rows.append((morsel.column(b"lo").to_pylist(), morsel.column(b"hi").to_pylist()))

    # published_at is 12:30:15, so 2025-12-31T00:00:00 excludes row 6.
    assert rows == [([1], [5])]


def test_order_by_limit_over_cross_domain_predicate(dataset):
    rows, _ = _query(
        f"SELECT * FROM {dataset} "
        f"WHERE date_added >= CAST('2025-01-01T00:00:00' AS TIMESTAMP) "
        f"ORDER BY date_added ASC LIMIT 3"
    )

    assert rows == [4, 5, 6]


# ---------------------------------------------------------------------------
# A second silent-wrong-answer defect, found while building the tests above:
# `TopNManifestPruningStrategy` ranked files by the sort column's bounds and
# accumulated `record_count` until it reached the LIMIT, then dropped every
# file below that threshold. `record_count` is the file's TOTAL row count, so
# the moment the Scan also carried a residual WHERE filter, the accumulation
# counted rows that would not survive - and files holding the only rows that
# WOULD survive got dropped. The answer was short, or empty, with no error.
#
# The strategy fired whenever the HeapSort read directly from the Scan, which
# includes the case where the predicate was pushed INTO the Scan - so "there is
# a WHERE clause" did not prevent it.
#
# Fixed by gating the strategy on `node.predicates` being empty (same gate,
# same reason, as LimitFilesPruningStrategy) - see the module docstring on
# TopNManifestPruningStrategy. Measured on an unmodified build before the fix,
# no temporal types involved, and the second case is about as ordinary a
# predicate as exists:
#
#     WHERE n IN (4,5,6) ORDER BY date_added ASC LIMIT 3  -> <empty>  (want 4, 5, 6)
#     WHERE n <> 1       ORDER BY date_added ASC LIMIT 3  -> 2, 3     (want 2, 3, 4)
# ---------------------------------------------------------------------------


def test_topn_manifest_pruning_is_unsound_under_a_residual_filter(dataset):
    unlimited, _ = _query(f"SELECT * FROM {dataset} WHERE n IN (4, 5, 6) ORDER BY date_added ASC")
    limited, _ = _query(
        f"SELECT * FROM {dataset} WHERE n IN (4, 5, 6) ORDER BY date_added ASC LIMIT 3"
    )

    assert unlimited == [4, 5, 6]
    assert limited == [4, 5, 6]


def test_topn_manifest_pruning_returns_a_short_page_for_an_ordinary_filter(dataset):
    # `n <> 1` excludes exactly one row out of nine, so LIMIT 3 has eight rows to
    # choose from and must return three. It returns two.
    limited, _ = _query(f"SELECT * FROM {dataset} WHERE n <> 1 ORDER BY date_added ASC LIMIT 3")

    assert limited == [2, 3, 4]
