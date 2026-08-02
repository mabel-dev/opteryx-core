# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Regression: FileEntry.from_datafile() was silently dropping the catalog's
per-file null counts.

The opteryx_catalog package's ParquetManifestEntry.to_dict() carries a
"null_counts" key: a positional list parallel to "field_ids", exactly like
"min_lengths"/"max_lengths" (verified against the installed opteryx_catalog
package - see file_entry.py's from_datafile). from_datafile already handled
that positional-list-vs-field-id-dict conversion correctly for min/max
values and lengths, but hardcoded `null_value_counts=None` regardless of
what the entry actually carried.

Consequence: Manifest.get_total_null_count() (opteryx/models/manifest.py)
reads FileEntry.null_value_counts, so every catalog-backed FileEntry looked
like it had "unknown nullability" no matter how many real nulls the column
had. Anything gated on that - e.g. TopNManifestPruningStrategy's NULL-safety
check - silently never fired for catalog-backed tables.
"""

from __future__ import annotations

from types import SimpleNamespace

from opteryx.models.file_entry import FileEntry
from opteryx.models.manifest import Manifest
from opteryx.types.logical_type import INT64
from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity


def _datafile_entry(**overrides):
    entry = {
        "file_path": "f1.parquet",
        "record_count": 100,
        "file_size_in_bytes": 1000,
        "field_ids": [1, 5],
        "min_values": [10, 1],
        "max_values": [99, 42],
    }
    entry.update(overrides)
    return SimpleNamespace(entry=entry)


def test_from_datafile_extracts_null_counts_keyed_by_real_field_id():
    # field_ids order is [tweet_id=1, followers=5]; null_counts must land on
    # the SAME field_id, not the position in some other column ordering.
    fe = FileEntry.from_datafile(_datafile_entry(null_counts=[0, 7]))

    assert fe.null_value_counts == {1: 0, 5: 7}


def test_from_datafile_with_no_null_counts_key_stays_none():
    # Older manifest rows / entries without the key at all - must stay
    # "unknown", never guessed as zero.
    fe = FileEntry.from_datafile(_datafile_entry())

    assert fe.null_value_counts is None


def test_from_datafile_falls_back_to_position_when_no_field_ids():
    entry = {
        "file_path": "f1.parquet",
        "record_count": 100,
        "file_size_in_bytes": 1000,
        "null_counts": [3, 4],
    }
    fe = FileEntry.from_datafile(SimpleNamespace(entry=entry))

    assert fe.null_value_counts == {0: 3, 1: 4}


def test_get_total_null_count_now_resolves_for_catalog_backed_files():
    # End-to-end through Manifest._resolve_field_id + get_total_null_count,
    # with a schema whose column order deliberately does NOT match field_id
    # order (the exact shape that exposed the original MIN/MAX field-id bug).
    schema = RelationSchema(
        name="t",
        columns=[
            SchemaColumn(
                name="followers",
                column_type=INT64,
                identity=mint_column_identity("t", "followers"),
                field_id=5,
            ),
            SchemaColumn(
                name="tweet_id",
                column_type=INT64,
                identity=mint_column_identity("t", "tweet_id"),
                field_id=1,
            ),
        ],
    )
    fe = FileEntry.from_datafile(_datafile_entry(null_counts=[0, 7]))
    manifest = Manifest(files=[fe], schema=schema)

    assert manifest.get_total_null_count("followers") == 7
    assert manifest.get_total_null_count("tweet_id") == 0


def test_non_dict_entry_datafile_shape_leaves_null_value_counts_none():
    # The generic getattr()-based fallback path (no known producer carries
    # null counts in this shape) - must not guess.
    datafile = SimpleNamespace(file_path="f1.parquet", record_count=10, file_size_in_bytes=100)
    fe = FileEntry.from_datafile(datafile)

    assert fe.null_value_counts is None
