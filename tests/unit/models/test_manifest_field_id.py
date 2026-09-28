"""
Regression tests for the field-id manifest statistics fix.

Bug: MIN/MAX over a column read the wrong file bound whenever a column's
position in `self.schema.columns` (used as a fallback "field_id") didn't match
the position a file's own writer used for its min/max lists.

The manifest now has ONE key space - every per-column statistic is keyed by the
column's LOAD-TIME position - and a catalog row's field-id-keyed lists are
mapped onto those positions once, where the rows are read
(`opteryx_connector._catalog_manifest`), through the schema's real,
catalog-assigned `field_id`s. A row with no `field_ids` of its own was written
in schema order, so its lists are positional. The consumers
(`get_min_max_from_manifest`, `Manifest.prune_files`) find a column by NAME, so
a live schema pruned by projection pushdown cannot redirect them to another
column's bounds.
"""

from __future__ import annotations
from opteryx.models.manifest import Manifest

from opteryx.expression import NodeType
from opteryx.connectors.opteryx_connector import _catalog_manifest
from opteryx.planner.optimizer.strategies.statistics_only_response import (
    get_min_max_from_manifest,
)
from opteryx.types.logical_type import INT64
from opteryx.types.schema import RelationSchema
from opteryx.compiled.structures.expressions import Comparison
from opteryx.compiled.structures.expressions import Literal
from opteryx.compiled.structures.expressions import LogicalColumn
from opteryx.planner.plan_context import PlanContext


def _schema_with_field_ids(plan_context, names_and_ids):
    """Build a RelationSchema whose column order deliberately does NOT match
    the catalog field-ids assigned to those columns — this is exactly the
    "schema evolution reordered things" shape that exposed the bug."""
    return RelationSchema(
        name="t",
        columns=[
            plan_context.columns.relation_column(
                "t",
                n,
                column_type=INT64,
                field_id=fid,
            )
            for n, fid in names_and_ids
        ],
    )


def _row(field_ids, min_values, max_values):
    """A catalog manifest row: per-column lists in the row's own column order,
    keyed by its `field_ids` (None = written in schema order)."""
    row = {
        "file_path": "f1",
        "record_count": 10,
        "file_size_in_bytes": 0,
        "min_values": min_values,
        "max_values": max_values,
    }
    if field_ids is not None:
        row["field_ids"] = field_ids
    return row


def _manifest(schema, rows):
    return _catalog_manifest(schema, False, rows, {}, None)


def test_catalog_rows_are_keyed_by_real_field_id_over_position():
    plan_context = PlanContext()
    # "followers" sits at schema position 0, but its real catalog field-id is 5;
    # the row lists its columns in its own order (tweet_id, followers).
    schema = _schema_with_field_ids(plan_context, [("followers", 5), ("tweet_id", 1)])
    manifest = _manifest(schema, [_row([1, 5], [100, 7], [999, 42])])

    assert manifest.min_max("followers") == (7, 42)
    assert manifest.min_max("tweet_id") == (100, 999)


def test_catalog_rows_without_field_ids_are_positional():
    plan_context = PlanContext()
    schema = RelationSchema(
        name="t",
        columns=[
            plan_context.columns.relation_column("t", "a", column_type=INT64),
            plan_context.columns.relation_column("t", "b", column_type=INT64),
        ],
    )
    manifest = _manifest(schema, [_row(None, [1, 10], [2, 20])])

    assert manifest.min_max("a") == (1, 2)
    assert manifest.min_max("b") == (10, 20)


def test_get_min_max_from_manifest_reads_correct_column_via_field_id():
    plan_context = PlanContext()
    # Two columns; the file's own min/max lists are in "tweet_id, followers"
    # order (positions 0/1) but the *schema's* field-ids for them are 1 and 5
    # respectively (mirrors the reported gdelt_events-style mismatch).
    schema = _schema_with_field_ids(plan_context, [("followers", 5), ("tweet_id", 1)])

    # tweet_id min=100 max=999, followers min=7 max=42
    manifest = _manifest(schema, [_row([1, 5], [100, 7], [999, 42])])

    assert get_min_max_from_manifest(manifest, "followers", "MIN") == 7
    assert get_min_max_from_manifest(manifest, "followers", "MAX") == 42
    assert get_min_max_from_manifest(manifest, "tweet_id", "MIN") == 100
    assert get_min_max_from_manifest(manifest, "tweet_id", "MAX") == 999


def test_prune_files_resolves_field_id_after_projection_pushdown():
    plan_context = PlanContext()
    # Reproduce the documented "MAX(followers) answered with MAX(tweet_id)"
    # shape: the manifest is built over the load-time schema (tweet_id at
    # position 0, followers at position 1, field ids 1 and 5), then projection
    # pushdown prunes the live schema down to just `followers` - now at live
    # position 0, which holds tweet_id's bounds.
    schema = _schema_with_field_ids(plan_context, [("tweet_id", 1), ("followers", 5)])
    manifest = _manifest(schema, [_row([1, 5], [100, 7], [999, 42])])
    manifest = Manifest(manifest.native, manifest.schema.with_columns([manifest.schema.columns[1]]))

    # `followers > 100` should prune the file (max is 42), not read tweet_id's
    # bounds (max 999) and keep it.
    identifier = LogicalColumn(
        node_type=NodeType.IDENTIFIER, source_column="followers", arena=plan_context.expressions
    )
    literal = Literal(type=INT64, value=100, arena=plan_context.expressions)
    predicate = Comparison(value="Gt", left=identifier, right=literal, arena=plan_context.expressions)

    manifest = manifest.prune_files([predicate], plan_context=plan_context)

    assert manifest.get_file_count() == 0
