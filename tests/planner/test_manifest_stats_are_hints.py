"""`Manifest.stats_are_authoritative`: the line between a statistic that may
decide an ANSWER and one that may only shape a PLAN.

A manifest written by the commit that produced the data cannot disagree with
it, so its bounds may prune a file away and its counts may answer COUNT(*)
without reading anything. A manifest assembled by a refresh over an external
source (a PostgreSQL server's `pg_stats`, an Iceberg snapshot summary) describes
that source as it was at the last refresh. Using those as law returns a WRONG
ANSWER - fewer rows than exist, a stale COUNT, a MIN that is not the minimum -
and nothing downstream can detect it.

The flag defaults to FALSE (hints) deliberately: a producer that forgets to
speak up loses an optimisation, where the opposite default would lose rows.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.models.file_entry import FileEntry
from opteryx.models.manifest import Manifest
from opteryx.types import logical_type as _lt
from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity


def _schema():
    return RelationSchema(
        name="t",
        columns=[
            SchemaColumn(
                name="id",
                column_type=_lt.INT64,
                identity=mint_column_identity("t", "id"),
            )
        ],
    )


def _entry(record_count=100):
    return FileEntry(
        file_path="s3://bucket/one.parquet",
        file_format="PARQUET",
        record_count=record_count,
        file_size_in_bytes=1024,
    )


def _manifest(authoritative, record_count=100):
    return Manifest(
        [_entry(record_count)], _schema(), stats_are_authoritative=authoritative
    )


# ---- the flag itself ---------------------------------------------------------


def test_the_default_is_hints():
    """Forgetting to speak up must cost an optimisation, never a row."""
    assert Manifest([_entry()], _schema()).stats_are_authoritative is False


def test_the_flag_survives_a_prune():
    """Pruning is copy-on-write; a clone that forgot the flag would be treated
    as authoritative-by-omission on the next strategy to look at it."""
    for authoritative in (True, False):
        manifest = _manifest(authoritative)
        assert manifest.subset([0]).stats_are_authoritative is authoritative


# ---- LAW: answering the query from the statistics ----------------------------


def test_count_is_not_answered_from_hint_statistics():
    """`get_count_from_manifest` returning a number REMOVES the scan, so
    the number is what the user is told. None makes the caller abandon the
    rewrite and read the data instead."""
    from opteryx.planner.optimizer.strategies.statistics_only_response import (
        get_count_from_manifest,
    )

    assert get_count_from_manifest(_manifest(True)) == 100
    assert get_count_from_manifest(_manifest(False)) is None


def test_min_max_is_not_answered_from_hint_bounds():
    from opteryx.planner.optimizer.strategies.statistics_only_response import (
        get_min_max_from_manifest,
    )

    assert get_min_max_from_manifest(_manifest(False), "id", "MIN") is None
    assert get_min_max_from_manifest(_manifest(False), "id", "MAX") is None


# ---- HINT: the estimator still gets the number -------------------------------


def test_a_hint_count_is_an_estimate_not_a_metric():
    """The number is still used - filter selectivity, join ordering and
    distinct-value estimates all read it. What changes is the PROVENANCE: a
    metric claims to be known, and `result_size_guard` reads that claim."""
    from opteryx.planner.optimizer.statistics_refresh import _scan_base_stats

    class _Node:
        def __init__(self, manifest):
            self.manifest = manifest
            self.schema = _schema()
            self.uuid = "n1"
            self.columns = []

    measured = _scan_base_stats(_Node(_manifest(True)))
    assert measured.row_count_metric == 100
    assert measured.row_count_estimate is None

    hinted = _scan_base_stats(_Node(_manifest(False)))
    assert hinted.row_count_estimate == 100
    assert hinted.row_count_metric is None
    # Still a real number, so join-cardinality estimation does not decline it -
    # `_subtree_sources_are_backed` accepts `row_count_metric or row_count_estimate`
    # and refuses only the fabricated placeholder.
    assert hinted.row_count == 100


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
