# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Regression: the scan statistics cache must not serve pre-pruning statistics
after a pruning strategy shrinks the scan's manifest.

_scan_stats memoises each scan's base statistics keyed by
(node.uuid, id(node.schema), id(node.manifest), wanted) on the invariant that
a Manifest attached to a plan node is immutable. The pruning operations
(prune_files / prune_files_for_topn / subset) are therefore copy-on-write:
they return a NEW Manifest which the strategy assigns to node.manifest, so
the id()-keyed cache misses and recomputes over the pruned file set. Before
that contract, ManifestPruning/TopNManifestPruning/LimitFilesPruning mutated
the manifest in place (same id) and every later refresh with an unchanged
`wanted` set re-served PRE-pruning record counts and bounds — feeding
PredicateOrderingStrategy (which runs after the LIMIT pruners) stale costs.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.models.file_entry import FileEntry
from opteryx.models.manifest import Manifest
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.optimizer.statistics_refresh import _scan_stats
from opteryx.types.logical_type import INT64
from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity


def _schema():
    return RelationSchema(
        name="t",
        columns=[
            SchemaColumn(
                name="value",
                column_type=INT64,
                identity=mint_column_identity("t", "value"),
            ),
        ],
    )


def _file(path, lo, hi, record_count):
    return FileEntry(
        file_path=path,
        file_format="PARQUET",
        record_count=record_count,
        file_size_in_bytes=0,
        lower_bounds={0: lo},
        upper_bounds={0: hi},
    )


def _comparison(op, value):
    identifier = Node(NodeType.IDENTIFIER, source_column="value")
    literal = Node(NodeType.LITERAL, value=value)
    return Node(NodeType.COMPARISON_OPERATOR, value=op, left=identifier, right=literal)


def _scan_node(manifest, schema):
    node = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    node.schema = schema
    node.manifest = manifest
    return node


def test_refresh_after_prune_reflects_pruned_file_set():
    schema = _schema()
    manifest = Manifest(
        files=[_file("low", 0, 100, 10), _file("high", 1000, 2000, 20)],
        schema=schema,
    )
    node = _scan_node(manifest, schema)
    cache: dict = {}

    before = _scan_stats(node, base_stats_cache=cache)
    assert before.row_count == 30

    # What ManifestPruningStrategy does: copy-on-write prune, re-assign.
    node.manifest = node.manifest.prune_files([_comparison("Gt", 500)])
    assert node.manifest.get_file_count() == 1

    after = _scan_stats(node, base_stats_cache=cache)
    assert after.row_count == 20, (
        f"cache served pre-pruning statistics: got {after.row_count}, want 20"
    )


def test_prune_files_is_copy_on_write():
    schema = _schema()
    manifest = Manifest(
        files=[_file("low", 0, 100, 10), _file("high", 1000, 2000, 20)],
        schema=schema,
    )

    pruned = manifest.prune_files([_comparison("Gt", 500)])

    # A real prune hands back a NEW object and leaves the original untouched.
    assert pruned is not manifest
    assert manifest.get_file_count() == 2
    assert manifest.get_record_count() == 30
    assert pruned.get_file_count() == 1
    assert pruned.get_record_count() == 20

    # A prune that removes nothing hands the SAME object back — no epoch
    # churn, no cache invalidation, nothing changed.
    unpruned = manifest.prune_files([_comparison("Gt", -1)])
    assert unpruned is manifest


def test_prune_files_for_topn_is_copy_on_write():
    schema = _schema()
    manifest = Manifest(
        files=[_file("low", 0, 100, 10), _file("high", 1000, 2000, 20)],
        schema=schema,
    )

    pruned = manifest.prune_files_for_topn("value", descending=True, limit=5)

    assert pruned is not manifest
    assert manifest.get_file_count() == 2
    assert pruned.get_file_count() == 1
    assert pruned.files[0].file_path == "high"


def test_subset_is_copy_on_write_and_tracks_live_rows():
    schema = _schema()
    manifest = Manifest(
        files=[_file("a", 0, 10, 5), _file("b", 20, 30, 5), _file("c", 40, 50, 5)],
        schema=schema,
    )

    picked = manifest.subset([2, 0])

    assert picked is not manifest
    assert manifest.get_file_count() == 3
    assert [f.file_path for f in picked.files] == ["c", "a"]
    # The sketch-vector row mapping follows the reorder/truncation.
    assert picked._live_rows == [2, 0]

    # Subset of a subset composes through to ORIGINAL vector rows.
    again = picked.subset([1])
    assert [f.file_path for f in again.files] == ["a"]
    assert again._live_rows == [0]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
