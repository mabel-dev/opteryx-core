# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Regression: the catalog manifest reader once silently dropped the catalog's
per-file null counts.

The opteryx_catalog package's ParquetManifestEntry.to_dict() carries a
"null_counts" key: a positional list parallel to "field_ids", exactly like
"min_lengths"/"max_lengths". The reader (then FileEntry.from_datafile, now
opteryx_connector._catalog_manifest) handled that positional-list-to-column
mapping correctly for min/max values and lengths, but hardcoded the null
counts to "unknown" regardless of what the entry actually carried.

Consequence: Manifest.get_total_null_count() made every catalog-backed file
look like it had "unknown nullability" no matter how many real nulls the
column had. Anything gated on that - e.g. TopNManifestPruningStrategy's
NULL-safety check - silently never fired for catalog-backed tables.
"""

from __future__ import annotations

from opteryx.compiled.structures.plan_steps import ExitStep
from opteryx.compiled.structures.plan_steps import ScanStep
from opteryx.connectors.opteryx_connector import _catalog_manifest
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.optimizer.statistics_refresh import refresh_statistics
from opteryx.planner.plan_context import PlanContext
from opteryx.types.logical_type import INT64
from opteryx.types.schema import RelationSchema

# Bound columns are minted by a query's ColumnTable; these tests share one.
_PLAN_CONTEXT = PlanContext()


def _schema(*columns):
    """`columns` are (name, field_id) pairs, in schema order."""
    return RelationSchema(
        name="t",
        columns=[
            _PLAN_CONTEXT.columns.relation_column("t", name, column_type=INT64, field_id=field_id)
            for name, field_id in columns
        ],
    )


def _entry(**overrides):
    entry = {
        "file_path": "f1.parquet",
        "record_count": 100,
        "file_size_in_bytes": 1000,
        "field_ids": [1, 5],
        "min_values": [10, 1],
        "max_values": [99, 42],
    }
    entry.update(overrides)
    return entry


def _manifest(schema, entry):
    return _catalog_manifest(schema, True, [entry], {}, None)


def _scan_statistics(manifest):
    """(store, scan nid): the statistics refresh of a Scan over `manifest`
    (all of its schema's columns) in _PLAN_CONTEXT - the query whose
    ColumnTable minted the schema's columns. The scan's base statistics are
    what the planner reads of the manifest, keyed by column identity."""
    plan = LogicalPlan(_PLAN_CONTEXT)
    scan = plan.add_node(ScanStep(relation="t", schema=manifest.schema, manifest=manifest))
    plan.add_edge(scan, plan.add_node(ExitStep()))
    refresh_statistics(plan, _PLAN_CONTEXT)
    return _PLAN_CONTEXT.statistics, scan


def test_null_counts_keyed_by_real_field_id():
    # field_ids order is [tweet_id=1, followers=5]; null_counts must land on
    # the SAME field_id, not the position in some other column ordering.
    manifest = _manifest(_schema(("tweet_id", 1), ("followers", 5)), _entry(null_counts=[0, 7]))

    assert manifest.get_total_null_count("tweet_id") == 0
    assert manifest.get_total_null_count("followers") == 7


def test_no_null_counts_key_stays_unknown():
    # Older manifest rows / entries without the key at all - must stay
    # "unknown", never guessed as zero.
    manifest = _manifest(_schema(("tweet_id", 1), ("followers", 5)), _entry())

    # No file counts nulls: the planner records no null fraction for any
    # column (the refresh records one for every column whenever any file of a
    # manifest with a known row count counts nulls - it would be 0.0 here, a
    # guess of "no nulls").
    store, scan = _scan_statistics(manifest)
    for column in manifest.schema.columns:
        assert store.null_fraction(scan, column.identity) is None
    assert manifest.get_total_null_count("tweet_id") is None
    assert manifest.get_total_null_count("followers") is None


def test_falls_back_to_position_when_no_field_ids():
    entry = {
        "file_path": "f1.parquet",
        "record_count": 100,
        "file_size_in_bytes": 1000,
        "null_counts": [3, 4],
    }
    manifest = _manifest(_schema(("a", 1), ("b", 2)), entry)

    assert manifest.get_total_null_count("a") == 3
    assert manifest.get_total_null_count("b") == 4


def test_get_total_null_count_resolves_for_catalog_backed_files():
    # End-to-end through the field-id -> load-time-position mapping and
    # get_total_null_count, with a schema whose column order deliberately does
    # NOT match field_id order (the exact shape that exposed the original
    # MIN/MAX field-id bug).
    schema = _schema(("followers", 5), ("tweet_id", 1))
    manifest = _manifest(schema, _entry(null_counts=[0, 7]))

    assert manifest.get_total_null_count("followers") == 7
    assert manifest.get_total_null_count("tweet_id") == 0
