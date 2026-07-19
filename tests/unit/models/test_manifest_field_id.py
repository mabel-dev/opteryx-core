"""
Regression tests for the field-id manifest statistics fix.

Bug: MIN/MAX over a column read the wrong file-entry bound whenever a
column's position in `self.schema.columns` (used as a fallback "field_id")
didn't match the position a file's own writer used for its min/max lists.
`Manifest._resolve_field_id` now prefers a real, catalog-assigned
`SchemaColumn.field_id` over any positional guess, and consumers
(`get_min_max_from_manifest`, `Manifest.prune_files`) read `FileEntry`'s
field-id-keyed `lower_bounds`/`upper_bounds` dict instead of indexing the
positional `min_values`/`max_values` lists by that id.
"""

from __future__ import annotations

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.models.file_entry import FileEntry
from opteryx.models.manifest import Manifest
from opteryx.planner.optimizer.strategies.statistics_only_response import (
    get_min_max_from_manifest,
)
from opteryx.types.logical_type import INT64
from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity


def _schema_with_field_ids(names_and_ids):
    """Build a RelationSchema whose column order deliberately does NOT match
    the catalog field-ids assigned to those columns — this is exactly the
    "schema evolution reordered things" shape that exposed the bug."""
    return RelationSchema(
        name="t",
        columns=[
            SchemaColumn(
                name=n,
                column_type=INT64,
                identity=mint_column_identity("t", n),
                field_id=fid,
            )
            for n, fid in names_and_ids
        ],
    )


def test_resolve_field_id_prefers_real_field_id_over_position():
    # "followers" sits at schema position 0, but its real catalog field-id is 5.
    schema = _schema_with_field_ids([("followers", 5), ("tweet_id", 1)])
    manifest = Manifest(files=[], schema=schema)

    assert manifest._resolve_field_id("followers") == 5
    assert manifest._resolve_field_id("tweet_id") == 1


def test_resolve_field_id_falls_back_to_load_time_position_when_no_field_id():
    schema = RelationSchema(
        name="t",
        columns=[
            SchemaColumn(name="a", column_type=INT64, identity=mint_column_identity("t", "a")),
            SchemaColumn(name="b", column_type=INT64, identity=mint_column_identity("t", "b")),
        ],
    )
    manifest = Manifest(files=[], schema=schema)

    assert manifest._resolve_field_id("a") == 0
    assert manifest._resolve_field_id("b") == 1


def test_get_min_max_from_manifest_reads_correct_column_via_field_id():
    # Two columns; the file's own min/max lists are in "tweet_id, followers"
    # order (positions 0/1) but the *schema's* field-ids for them are 1 and 5
    # respectively (mirrors the reported gdelt_events-style mismatch).
    schema = _schema_with_field_ids([("followers", 5), ("tweet_id", 1)])

    file_entry = FileEntry(
        file_path="f1",
        file_format="PARQUET",
        record_count=10,
        file_size_in_bytes=0,
        lower_bounds={1: 100, 5: 7},  # tweet_id min=100, followers min=7
        upper_bounds={1: 999, 5: 42},  # tweet_id max=999, followers max=42
    )

    manifest = Manifest(files=[file_entry], schema=schema)

    assert get_min_max_from_manifest(manifest, "followers", "MIN") == 7
    assert get_min_max_from_manifest(manifest, "followers", "MAX") == 42
    assert get_min_max_from_manifest(manifest, "tweet_id", "MIN") == 100
    assert get_min_max_from_manifest(manifest, "tweet_id", "MAX") == 999


def test_get_min_max_from_manifest_does_not_use_positional_min_values_when_field_id_keyed_bounds_exist():
    # Regression guard: even if a legacy positional min_values/max_values list
    # is present (backward-compat leftover), the field-id-keyed lower_bounds/
    # upper_bounds dict must win — indexing the positional list by a real
    # field-id (5) would go out of range / read the wrong slot.
    schema = _schema_with_field_ids([("followers", 5)])

    file_entry = FileEntry(
        file_path="f1",
        file_format="PARQUET",
        record_count=10,
        file_size_in_bytes=0,
        lower_bounds={5: 7},
        upper_bounds={5: 42},
        min_values=[999],  # positional list — index 5 would be out of range
        max_values=[999],
    )

    manifest = Manifest(files=[file_entry], schema=schema)

    assert get_min_max_from_manifest(manifest, "followers", "MIN") == 7
    assert get_min_max_from_manifest(manifest, "followers", "MAX") == 42


def test_prune_files_resolves_field_id_after_projection_pushdown():
    # Reproduce the documented "MAX(followers) answered with MAX(tweet_id)"
    # shape: after projection pushdown, self.schema is pruned down to just
    # `followers` at schema position 0 — but the file's real bounds are keyed
    # by followers' true field-id (5), not position 0.
    pruned_schema = _schema_with_field_ids([("followers", 5)])

    file_entry = FileEntry(
        file_path="f1",
        file_format="PARQUET",
        record_count=10,
        file_size_in_bytes=0,
        lower_bounds={5: 7},
        upper_bounds={5: 42},
    )
    manifest = Manifest(files=[file_entry], schema=pruned_schema)

    # `followers > 100` should prune the file (max is 42), not silently read
    # field_id=0 (which doesn't exist in lower_bounds/upper_bounds) and skip
    # pruning.
    identifier = Node(NodeType.IDENTIFIER, source_column="followers")
    literal = Node(NodeType.LITERAL, type=INT64, value=100)
    predicate = Node(NodeType.COMPARISON_OPERATOR, value="Gt", left=identifier, right=literal)

    manifest.prune_files([predicate])

    assert manifest.files == []
