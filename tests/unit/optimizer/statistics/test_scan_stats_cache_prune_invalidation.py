# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Regression: the scan statistics cache must not serve pre-pruning statistics
after a pruning strategy shrinks the scan's manifest.

The statistics refresh memoises each scan's base statistics in the query's
StatisticsStore, keyed by (the manifest's identity, the schemas, the consulted
columns) - `scan_base_statistics` in src/cpp/planner/statistics_refresh.hpp -
on the invariant that a Manifest attached to a plan node is immutable. The pruning operations
(prune_files / prune_files_for_topn / subset) are therefore copy-on-write:
they return a NEW Manifest which the strategy assigns to node.manifest, so
the identity-keyed cache misses and recomputes over the pruned file set. Before
that contract, ManifestPruning/TopNManifestPruning/LimitFilesPruning mutated
the manifest in place (same id) and every later refresh with an unchanged
`wanted` set re-served PRE-pruning record counts and bounds — feeding
PredicateOrderingStrategy (which runs after the LIMIT pruners) stale costs.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

# Importing opteryx.planner.optimizer (the package) first resolves the
# pre-existing import cycle a compiled planner module hits when imported first.
import opteryx.planner.optimizer  # noqa: F401
from opteryx.compiled.structures.expressions import Comparison
from opteryx.compiled.structures.expressions import Literal
from opteryx.compiled.structures.plan_steps import ScanStep
from opteryx.expression import NodeType
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.optimizer.statistics_refresh import refresh_statistics
from opteryx.types.logical_type import INT64
from opteryx.types.schema import RelationSchema
from opteryx.compiled.structures.expressions import LogicalColumn
from opteryx.planner.plan_context import PlanContext
from tests.manifests import FileSpec
from tests.manifests import build_manifest


def _schema(plan_context):
    return RelationSchema(
        name="t",
        columns=[
            plan_context.columns.relation_column(
                "t",
                "value",
                column_type=INT64,
            ),
        ],
    )


def _file(path, lo, hi, record_count):
    return FileSpec(
        file_path=path,
        file_format="PARQUET",
        record_count=record_count,
        file_size_in_bytes=0,
        lower_bounds={0: lo},
        upper_bounds={0: hi},
    )


def _comparison(plan_context, op, value):
    """`value <op> <value>` in the query's own expression arena - the arena
    native pruning reads the predicate from."""
    arena = plan_context.expressions
    identifier = LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="value", arena=arena)
    literal = Literal(value=value, type=INT64, arena=arena)
    return Comparison(value=op, left=identifier, right=literal, arena=arena)


def _scan_node(manifest, schema):
    node = ScanStep()
    node.schema = schema
    node.manifest = manifest
    return node


def test_refresh_after_prune_reflects_pruned_file_set():
    plan_context = PlanContext()
    schema = _schema(plan_context)
    manifest = build_manifest(schema, [_file("low", 0, 100, 10), _file("high", 1000, 2000, 20)])
    node = _scan_node(manifest, schema)
    plan = LogicalPlan(plan_context)
    nid = plan.add_node(node)
    store = plan_context.statistics  # holds the scan base memo across refreshes

    refresh_statistics(plan, plan_context)
    assert store.row_count(nid) == 30

    # What ManifestPruningStrategy does: copy-on-write prune, re-assign.
    node.manifest = node.manifest.prune_files(
        [_comparison(plan_context, "Gt", 500)], plan_context=plan_context
    )
    assert node.manifest.get_file_count() == 1

    refresh_statistics(plan, plan_context)
    after = store.row_count(nid)
    assert after == 20, (
        f"cache served pre-pruning statistics: got {after}, want 20"
    )


def test_prune_files_is_copy_on_write():
    plan_context = PlanContext()
    schema = _schema(plan_context)
    manifest = build_manifest(schema, [_file("low", 0, 100, 10), _file("high", 1000, 2000, 20)])

    pruned = manifest.prune_files([_comparison(plan_context, "Gt", 500)], plan_context=plan_context)

    # A real prune hands back a NEW object and leaves the original untouched.
    assert pruned is not manifest
    assert manifest.get_file_count() == 2
    assert manifest.get_record_count() == 30
    assert pruned.get_file_count() == 1
    assert pruned.get_record_count() == 20

    # A prune that removes nothing hands the SAME object back — no epoch
    # churn, no cache invalidation, nothing changed.
    unpruned = manifest.prune_files([_comparison(plan_context, "Gt", -1)], plan_context=plan_context)
    assert unpruned is manifest


def test_prune_files_for_topn_is_copy_on_write():
    schema = _schema(PlanContext())
    manifest = build_manifest(schema, [_file("low", 0, 100, 10), _file("high", 1000, 2000, 20)])

    pruned = manifest.prune_files_for_topn("value", descending=True, limit=5)

    assert pruned is not manifest
    assert manifest.get_file_count() == 2
    assert pruned.get_file_count() == 1
    assert pruned.get_file_paths() == ["high"]


def _vector_rows(manifest):
    """Each file's row in the manifest's sketch vectors."""
    return [manifest.native.file_row(row)["vector_row"] for row in range(manifest.get_file_count())]


def test_subset_is_copy_on_write_and_tracks_live_rows():
    schema = _schema(PlanContext())
    manifest = build_manifest(
        schema, [_file("a", 0, 10, 5), _file("b", 20, 30, 5), _file("c", 40, 50, 5)]
    )

    picked = manifest.subset([2, 0])

    assert picked is not manifest
    assert manifest.get_file_count() == 3
    assert picked.get_file_paths() == ["c", "a"]
    # The sketch-vector row mapping follows the reorder/truncation.
    assert _vector_rows(picked) == [2, 0]

    # Subset of a subset composes through to ORIGINAL vector rows.
    again = picked.subset([1])
    assert again.get_file_paths() == ["a"]
    assert _vector_rows(again) == [0]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
