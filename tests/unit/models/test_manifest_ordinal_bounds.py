# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Regression tests for Manifest's `bounds_are_ordinal` flag and its use in
`prune_files`.

ANALYZE's native per-file statistics pass writes min_values/max_values into
the dataset manifest as `Vector.ordinalize()` ordinal int64 keys, not real
decoded values (see manifest_io.write_manifest_parquet's docstring). A
predicate literal must be run through the SAME `ColumnType.ordinalize`
transform before it is comparable to those bounds. These tests exercise
`Manifest.prune_files` directly (no filesystem/ANALYZE I/O) so the pruning
arithmetic itself is pinned down:

- INT columns: ordinalize is an identity widen, so ordinal-encoded pruning
  must behave exactly like real-value pruning.
- FLOAT/VARCHAR columns: the ordinal key is NOT the real value (a monotonic
  but lossy bit-transform) — pruning must still be correct because both the
  bound and the literal go through the same transform.
- bounds_are_ordinal=False (the default — catalog DataFile bounds,
  LocalStoreConnector's parquet-footer bounds) must keep comparing real
  values directly, completely unaffected by the ordinalize path.
- A physical type with no scalar ordinalize kernel (TIMESTAMP/TIME/
  DECIMAL128) must not crash pruning — the predicate is conservatively
  skipped (file kept), not pruned on a comparison that can't be made safely.
"""

from __future__ import annotations

import datetime

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.models.file_entry import FileEntry
from opteryx.models.manifest import Manifest
from opteryx.types.logical_type import FLOAT64, INT64, TIMESTAMP, VARCHAR
from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity


def _schema(column_type, name="value"):
    return RelationSchema(
        name="t",
        columns=[
            SchemaColumn(
                name=name, column_type=column_type, identity=mint_column_identity("t", name)
            )
        ],
    )


def _comparison(column_name, op, value):
    identifier = Node(NodeType.IDENTIFIER, source_column=column_name)
    literal = Node(NodeType.LITERAL, value=value)
    return Node(NodeType.COMPARISON_OPERATOR, value=op, left=identifier, right=literal)


def _between(column_name, lower, upper):
    identifier = Node(NodeType.IDENTIFIER, source_column=column_name)
    return Node(
        NodeType.BETWEEN,
        left=identifier,
        right=Node(NodeType.LITERAL, value=lower),
        centre=Node(NodeType.LITERAL, value=upper),
    )


def _file_entry(lower, upper):
    return FileEntry(
        file_path="f1",
        file_format="PARQUET",
        record_count=10,
        file_size_in_bytes=0,
        lower_bounds={0: lower},
        upper_bounds={0: upper},
    )


# ---------------------------------------------------------------------------
# INT columns: ordinalize() is an identity widen, so ordinal-encoded pruning
# must match real-value pruning exactly.
# ---------------------------------------------------------------------------


def test_int_ordinal_bounds_prune_out_of_range_value():
    schema = _schema(INT64)
    entry = _file_entry(INT64.ordinalize(10), INT64.ordinalize(20))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest.prune_files([_comparison("value", "Gt", 100)])

    assert manifest.files == []


def test_int_ordinal_bounds_keep_in_range_value():
    schema = _schema(INT64)
    entry = _file_entry(INT64.ordinalize(10), INT64.ordinalize(20))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest.prune_files([_comparison("value", "Gt", 5)])

    assert len(manifest.files) == 1


def test_int_ordinal_bounds_match_real_value_bounds_behaviour():
    """Identity ordinalize -> pruning decisions must be identical to a
    hypothetical real-value comparison over the same numbers."""
    schema = _schema(INT64)

    for op, literal in (("Gt", 25), ("Lt", 5), ("Eq", 15), ("Eq", 999), ("GtEq", 20)):
        ordinal_manifest = Manifest(
            files=[_file_entry(INT64.ordinalize(10), INT64.ordinalize(20))],
            schema=schema,
            bounds_are_ordinal=True,
        )
        real_manifest = Manifest(
            files=[_file_entry(10, 20)],
            schema=schema,
            bounds_are_ordinal=False,
        )

        ordinal_manifest.prune_files([_comparison("value", op, literal)])
        real_manifest.prune_files([_comparison("value", op, literal)])

        assert (ordinal_manifest.files == []) == (real_manifest.files == []), (op, literal)


def test_int_ordinal_bounds_between_prunes_out_of_range():
    schema = _schema(INT64)
    entry = _file_entry(INT64.ordinalize(10), INT64.ordinalize(20))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest.prune_files([_between("value", 100, 200)])

    assert manifest.files == []


# ---------------------------------------------------------------------------
# FLOAT columns: ordinal key is NOT the real value.
# ---------------------------------------------------------------------------


def test_float_ordinal_bounds_are_not_real_values():
    # Sanity-check the premise: the stored ordinal key for a float bound is a
    # different number to the float itself.
    assert FLOAT64.ordinalize(10.5) != 10.5
    assert FLOAT64.ordinalize(10.5) != int(10.5)


def test_float_ordinal_bounds_prune_out_of_range_value():
    schema = _schema(FLOAT64)
    entry = _file_entry(FLOAT64.ordinalize(10.0), FLOAT64.ordinalize(20.0))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    # 100.0 is well outside [10.0, 20.0] — must prune despite the bounds being
    # stored as unrelated-looking ordinal integers.
    manifest.prune_files([_comparison("value", "Gt", 100.0)])

    assert manifest.files == []


def test_float_ordinal_bounds_keep_in_range_value():
    schema = _schema(FLOAT64)
    entry = _file_entry(FLOAT64.ordinalize(10.0), FLOAT64.ordinalize(20.0))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest.prune_files([_comparison("value", "Eq", 15.5)])

    assert len(manifest.files) == 1


def test_float_ordinal_bounds_prune_negative_values_correctly():
    # Negative floats ordinalize to a different sign/magnitude relationship
    # than the raw IEEE bits (see ordinalize_scalar_f64) — exercise a range
    # that straddles zero and a literal clearly outside it.
    schema = _schema(FLOAT64)
    entry = _file_entry(FLOAT64.ordinalize(-5.0), FLOAT64.ordinalize(5.0))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest.prune_files([_comparison("value", "Lt", -100.0)])

    assert manifest.files == []


def test_float_ordinal_bounds_between_keeps_overlapping_range():
    schema = _schema(FLOAT64)
    entry = _file_entry(FLOAT64.ordinalize(10.0), FLOAT64.ordinalize(20.0))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest.prune_files([_between("value", 15.0, 16.0)])

    assert len(manifest.files) == 1


def test_float_ordinal_bounds_between_prunes_disjoint_range():
    schema = _schema(FLOAT64)
    entry = _file_entry(FLOAT64.ordinalize(10.0), FLOAT64.ordinalize(20.0))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest.prune_files([_between("value", 1000.0, 2000.0)])

    assert manifest.files == []


# ---------------------------------------------------------------------------
# VARCHAR columns: ordinal key is a lossy 8-byte-prefix bit-transform.
# ---------------------------------------------------------------------------


def test_varchar_ordinal_bounds_are_not_real_values():
    assert VARCHAR.ordinalize("apple") != "apple"
    assert isinstance(VARCHAR.ordinalize("apple"), int)


def test_varchar_ordinal_bounds_prune_out_of_range_value():
    schema = _schema(VARCHAR)
    entry = _file_entry(VARCHAR.ordinalize("banana"), VARCHAR.ordinalize("cherry"))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    # "apple" sorts before "banana" — out of [banana, cherry] range.
    manifest.prune_files([_comparison("value", "Eq", "apple")])

    assert manifest.files == []


def test_varchar_ordinal_bounds_keep_in_range_value():
    schema = _schema(VARCHAR)
    entry = _file_entry(VARCHAR.ordinalize("banana"), VARCHAR.ordinalize("cherry"))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest.prune_files([_comparison("value", "Eq", "banana")])

    assert len(manifest.files) == 1


def test_varchar_ordinal_bounds_between_prunes_disjoint_range():
    schema = _schema(VARCHAR)
    entry = _file_entry(VARCHAR.ordinalize("banana"), VARCHAR.ordinalize("cherry"))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest.prune_files([_between("value", "xylophone", "zebra")])

    assert manifest.files == []


def test_varchar_ordinal_bounds_gt_prunes_correctly():
    schema = _schema(VARCHAR)
    entry = _file_entry(VARCHAR.ordinalize("banana"), VARCHAR.ordinalize("cherry"))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest.prune_files([_comparison("value", "Gt", "zebra")])

    assert manifest.files == []


# ---------------------------------------------------------------------------
# bounds_are_ordinal=False (default): real-value comparison path must be
# completely unaffected — this is LocalStoreConnector's / catalog DataFile's
# path (parquet-footer / catalog bounds), never ordinal-encoded.
# ---------------------------------------------------------------------------


def test_default_bounds_are_not_ordinal():
    schema = _schema(INT64)
    manifest = Manifest(files=[], schema=schema)
    assert manifest.bounds_are_ordinal is False


def test_real_value_bounds_still_compare_literal_directly_for_float():
    """With bounds_are_ordinal left False, a FLOAT literal must be compared
    AS-IS against the stored bound (no ordinalize) — exactly the pre-existing
    LocalStoreConnector/catalog behaviour. Store the bound as an ordinal key
    but leave bounds_are_ordinal False: since the literal is never converted,
    it must be compared against the (numerically unrelated) ordinal integer
    and therefore NOT prune a value that would be in-range under real
    comparison — proving the literal was never routed through ordinalize."""
    schema = _schema(FLOAT64)
    ordinal_min = FLOAT64.ordinalize(10.0)
    ordinal_max = FLOAT64.ordinalize(20.0)
    entry = _file_entry(ordinal_min, ordinal_max)
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=False)

    # 15.0 is well within the REAL range [10.0, 20.0], but the stored bounds
    # are huge ordinal integers — a direct (non-ordinalized) comparison finds
    # 15.0 far below both bounds and prunes the file.
    manifest.prune_files([_comparison("value", "Lt", 15.0)])

    assert manifest.files == [], "literal must not have been ordinalized"


def test_real_value_bounds_pruning_matches_pre_existing_behaviour():
    """LocalStoreConnector / catalog-origin FileEntry bounds are real decoded
    values; pruning over them must behave exactly as before this change."""
    schema = _schema(INT64)
    entry = _file_entry(10, 20)
    manifest = Manifest(files=[entry], schema=schema)  # bounds_are_ordinal defaults False

    manifest.prune_files([_comparison("value", "Gt", 25)])
    assert manifest.files == []

    manifest = Manifest(files=[_file_entry(10, 20)], schema=schema)
    manifest.prune_files([_comparison("value", "Gt", 5)])
    assert len(manifest.files) == 1


def test_real_value_varchar_bounds_unaffected():
    schema = _schema(VARCHAR)
    entry = FileEntry(
        file_path="f1",
        file_format="PARQUET",
        record_count=10,
        file_size_in_bytes=0,
        lower_bounds={0: b"banana"},
        upper_bounds={0: b"cherry"},
    )
    manifest = Manifest(files=[entry], schema=schema)

    manifest.prune_files([_comparison("value", "Eq", "apple")])

    assert manifest.files == []


# ---------------------------------------------------------------------------
# Physical types with no scalar ordinalize kernel: conservative skip, no crash.
# ---------------------------------------------------------------------------


def test_unsupported_ordinalize_type_skips_pruning_without_crashing():
    schema = _schema(TIMESTAMP())
    entry = _file_entry(0, 100)
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    predicate = _comparison("value", "Gt", datetime.datetime(2099, 1, 1))
    manifest.prune_files([predicate])

    # Can't safely ordinalize a TIMESTAMP literal at this entry point — the
    # predicate is skipped (file kept), not used to wrongly prune or crash.
    assert len(manifest.files) == 1
