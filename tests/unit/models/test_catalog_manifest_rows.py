# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The Manifest a catalog snapshot's manifest rows decode to (written by the
catalog's encoder, decoded natively - tests/manifests.py `catalog_manifest`).

Covers the min_lengths/max_lengths extraction (read back through a Scan's
base statistics, as the planner reads them) backing the length-aware
selectivity guard (STARTS_WITH/INSTR/ENDS_WITH): the catalog's own manifest
entry dict carries them (opteryx_catalog's ParquetManifestEntry.to_dict()
includes "min_lengths"/"max_lengths" as positional lists parallel to
field_ids, same shape as min_values/max_values), and a reader that keyed the
bounds but dropped the lengths left the guard blind.

Every per-column list in a row is keyed by the row's own `field_ids`, each
mapped to its load-time position through the schema columns' `field_id`; a row
with no `field_ids` is positional (schema order).
"""

from __future__ import annotations

from opteryx.compiled.structures.plan_steps import ExitStep
from opteryx.compiled.structures.plan_steps import ScanStep
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.optimizer.statistics_refresh import refresh_statistics
from opteryx.planner.plan_context import PlanContext
from opteryx.types.logical_type import INT64
from opteryx.types.logical_type import VARCHAR
from opteryx.types.schema import RelationSchema
from tests.manifests import catalog_manifest

# Bound columns are minted by a query's ColumnTable; these tests share one.
_PLAN_CONTEXT = PlanContext()


def _schema(*columns):
    """`columns` are (name, column_type, field_id) triples, in schema order."""
    return RelationSchema(
        name="t",
        columns=[
            _PLAN_CONTEXT.columns.relation_column("t", name, column_type=column_type, field_id=field_id)
            for name, column_type, field_id in columns
        ],
    )


def _manifest(schema, entry, bounds_are_ordinal=True):
    return catalog_manifest(schema, [entry], bounds_are_ordinal=bounds_are_ordinal)


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


def _length_bounds(manifest, name):
    """Column `name`'s length bounds as the planner reads them: the base
    statistics of a Scan over `manifest`."""
    store, scan = _scan_statistics(manifest)
    return store.length_bounds(scan, manifest.schema.find_column(name).identity)


def test_length_bounds_keyed_by_real_field_id():
    # field_ids deliberately non-sequential/offset from position, mirroring
    # the live catalog schema that exposed the ordinal_bounds field_id bug.
    schema = _schema(("a", VARCHAR, 1), ("b", VARCHAR, 3), ("c", VARCHAR, 4))
    entry = {
        "file_path": "f1",
        "record_count": 10,
        "file_size_in_bytes": 100,
        "field_ids": [1, 3, 4],
        "min_values": [1, 2, 3],
        "max_values": [9, 8, 7],
        "min_lengths": [2, 40, 7],
        "max_lengths": [5, 60, 9],
    }
    manifest = _manifest(schema, entry)

    assert _length_bounds(manifest, "a") == (2, 5)
    assert _length_bounds(manifest, "b") == (40, 60)
    assert _length_bounds(manifest, "c") == (7, 9)


def test_length_bounds_positional_without_field_ids():
    # Older manifest rows with no field_ids at all -- positional (schema
    # order), same convention the bounds use.
    schema = _schema(("a", VARCHAR, 1), ("b", VARCHAR, 2))
    entry = {
        "file_path": "f1",
        "record_count": 10,
        "file_size_in_bytes": 100,
        "min_values": [1, 2],
        "max_values": [9, 8],
        "min_lengths": [3, 11],
        "max_lengths": [6, 20],
    }
    manifest = _manifest(schema, entry)

    assert _length_bounds(manifest, "a") == (3, 6)
    assert _length_bounds(manifest, "b") == (11, 20)


def test_length_bounds_none_when_absent():
    schema = _schema(("a", VARCHAR, 1))
    entry = {
        "file_path": "f1",
        "record_count": 10,
        "file_size_in_bytes": 100,
        "min_values": [1],
        "max_values": [9],
    }
    manifest = _manifest(schema, entry)

    assert _length_bounds(manifest, "a") is None


def test_keys_by_the_rows_own_field_ids_in_file_order():
    # A row's `field_ids` are in the FILE's column order. Keying by the
    # schema's field ids (schema order) instead made each column read another
    # column's stats - public.github.events had created_at bounded by a URL
    # string.
    schema = _schema(("id", INT64, 1), ("count", INT64, 2), ("url", VARCHAR, 3))
    entry = {
        "file_path": "f1",
        "record_count": 10,
        "file_size_in_bytes": 100,
        "field_ids": (1, 3, 2),
        "min_values": (10, VARCHAR.ordinalize("https://a"), 100),
        "max_values": (19, VARCHAR.ordinalize("https://z"), 199),
        "null_counts": (0, 4, 0),
    }
    manifest = _manifest(schema, entry)

    assert manifest.min_max("id") == (10, 19)
    assert manifest.min_max("count") == (100, 199)
    assert manifest.get_total_null_count("id") == 0
    assert manifest.get_total_null_count("count") == 0
    assert manifest.get_total_null_count("url") == 4


def test_tuple_field_ids_shorter_than_schema_keeps_its_stats():
    # A file written without some schema columns (github.events rows with no
    # org_* columns): its own ids line up with its own stats, so the stats are
    # kept. Keyed against the full schema instead, the length mismatch
    # dropped every bound and null count on the file.
    schema = _schema(("id", INT64, 1), ("count", INT64, 2), ("url", VARCHAR, 3))
    entry = {
        "file_path": "f1",
        "record_count": 10,
        "file_size_in_bytes": 100,
        "field_ids": (2, 1),
        "min_values": (100, 10),
        "max_values": (199, 19),
        "null_counts": (0, 0),
    }
    manifest = _manifest(schema, entry, bounds_are_ordinal=False)

    assert manifest.min_max("id") == (10, 19)
    assert manifest.min_max("count") == (100, 199)
    assert manifest.min_max("url") == (None, None)
    assert manifest.get_total_null_count("id") == 0
    assert manifest.get_total_null_count("count") == 0
    # the file never recorded `url`: unknown, never guessed as zero
    assert manifest.get_total_null_count("url") is None


if __name__ == "__main__":  # pragma: no cover
    import pytest

    pytest.main([__file__, "-v"])
