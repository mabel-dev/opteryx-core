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

    manifest = manifest.prune_files([_comparison("value", "Gt", 100)])

    assert manifest.files == []


def test_int_ordinal_bounds_keep_in_range_value():
    schema = _schema(INT64)
    entry = _file_entry(INT64.ordinalize(10), INT64.ordinalize(20))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest = manifest.prune_files([_comparison("value", "Gt", 5)])

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

        ordinal_manifest = ordinal_manifest.prune_files([_comparison("value", op, literal)])
        real_manifest = real_manifest.prune_files([_comparison("value", op, literal)])

        assert (ordinal_manifest.files == []) == (real_manifest.files == []), (op, literal)


def test_int_ordinal_bounds_between_prunes_out_of_range():
    schema = _schema(INT64)
    entry = _file_entry(INT64.ordinalize(10), INT64.ordinalize(20))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest = manifest.prune_files([_between("value", 100, 200)])

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
    manifest = manifest.prune_files([_comparison("value", "Gt", 100.0)])

    assert manifest.files == []


def test_float_ordinal_bounds_keep_in_range_value():
    schema = _schema(FLOAT64)
    entry = _file_entry(FLOAT64.ordinalize(10.0), FLOAT64.ordinalize(20.0))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest = manifest.prune_files([_comparison("value", "Eq", 15.5)])

    assert len(manifest.files) == 1


def test_float_ordinal_bounds_prune_negative_values_correctly():
    # Negative floats ordinalize to a different sign/magnitude relationship
    # than the raw IEEE bits (see ordinalize_scalar_f64) — exercise a range
    # that straddles zero and a literal clearly outside it.
    schema = _schema(FLOAT64)
    entry = _file_entry(FLOAT64.ordinalize(-5.0), FLOAT64.ordinalize(5.0))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest = manifest.prune_files([_comparison("value", "Lt", -100.0)])

    assert manifest.files == []


def test_float_ordinal_bounds_between_keeps_overlapping_range():
    schema = _schema(FLOAT64)
    entry = _file_entry(FLOAT64.ordinalize(10.0), FLOAT64.ordinalize(20.0))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest = manifest.prune_files([_between("value", 15.0, 16.0)])

    assert len(manifest.files) == 1


def test_float_ordinal_bounds_between_prunes_disjoint_range():
    schema = _schema(FLOAT64)
    entry = _file_entry(FLOAT64.ordinalize(10.0), FLOAT64.ordinalize(20.0))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest = manifest.prune_files([_between("value", 1000.0, 2000.0)])

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
    manifest = manifest.prune_files([_comparison("value", "Eq", "apple")])

    assert manifest.files == []


def test_varchar_ordinal_bounds_keep_in_range_value():
    schema = _schema(VARCHAR)
    entry = _file_entry(VARCHAR.ordinalize("banana"), VARCHAR.ordinalize("cherry"))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest = manifest.prune_files([_comparison("value", "Eq", "banana")])

    assert len(manifest.files) == 1


def test_varchar_ordinal_bounds_between_prunes_disjoint_range():
    schema = _schema(VARCHAR)
    entry = _file_entry(VARCHAR.ordinalize("banana"), VARCHAR.ordinalize("cherry"))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest = manifest.prune_files([_between("value", "xylophone", "zebra")])

    assert manifest.files == []


def test_varchar_ordinal_bounds_gt_prunes_correctly():
    schema = _schema(VARCHAR)
    entry = _file_entry(VARCHAR.ordinalize("banana"), VARCHAR.ordinalize("cherry"))
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    manifest = manifest.prune_files([_comparison("value", "Gt", "zebra")])

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
    manifest = manifest.prune_files([_comparison("value", "Lt", 15.0)])

    assert manifest.files == [], "literal must not have been ordinalized"


def test_real_value_bounds_pruning_matches_pre_existing_behaviour():
    """LocalStoreConnector / catalog-origin FileEntry bounds are real decoded
    values; pruning over them must behave exactly as before this change."""
    schema = _schema(INT64)
    entry = _file_entry(10, 20)
    manifest = Manifest(files=[entry], schema=schema)  # bounds_are_ordinal defaults False

    manifest = manifest.prune_files([_comparison("value", "Gt", 25)])
    assert manifest.files == []

    manifest = Manifest(files=[_file_entry(10, 20)], schema=schema)
    manifest = manifest.prune_files([_comparison("value", "Gt", 5)])
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

    manifest = manifest.prune_files([_comparison("value", "Eq", "apple")])

    assert manifest.files == []


# ---------------------------------------------------------------------------
# Physical types with no scalar ordinalize kernel: conservative skip, no crash.
# ---------------------------------------------------------------------------


def test_unsupported_ordinalize_type_skips_pruning_without_crashing():
    schema = _schema(TIMESTAMP())
    entry = _file_entry(0, 100)
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    predicate = _comparison("value", "Gt", datetime.datetime(2099, 1, 1))
    manifest = manifest.prune_files([predicate])

    # Can't safely ordinalize a TIMESTAMP literal at this entry point — the
    # predicate is skipped (file kept), not used to wrongly prune or crash.
    assert len(manifest.files) == 1


# ---------------------------------------------------------------------------
# Manifest.get_ordinal_bounds — backs the STARTS_WITH ordinal-bounds
# selectivity estimator tier. field_id is the trap: a catalog-backed dataset
# assigns real, non-positional field_ids (observed live: insert_id=1,
# labels=2, log_name=3, receive_timestamp=4, ...) and FileEntry.min_values/
# max_values are a PLAIN POSITIONAL list (0-indexed by load-time schema
# order), never indexable by field_id directly — only lower_bounds/
# upper_bounds (the dict form) are correctly re-keyed by real field_id. Using
# `min_values[field_id]` instead of `lower_bounds.get(field_id)` silently
# reads a DIFFERENT column's bound whenever field_id != position — this is
# exactly the bug this section pins down.
# ---------------------------------------------------------------------------


def _multi_col_schema(*, names_and_field_ids):
    return RelationSchema(
        name="t",
        columns=[
            SchemaColumn(
                name=name,
                column_type=VARCHAR,
                identity=mint_column_identity("t", name),
                field_id=field_id,
            )
            for name, field_id in names_and_field_ids
        ],
    )


def _bounded_file_entry(bounds: dict, record_count: int = 10):
    """A FileEntry whose lower_bounds/upper_bounds dict is keyed by the field_ids
    given in `bounds` (``{field_id: (lower, upper)}``) — the dict form catalog
    FileEntry.from_datafile produces via ``zip(field_ids, min_values)``, i.e.
    what Manifest.get_ordinal_bounds must read, not the positional list."""
    return FileEntry(
        file_path="f1",
        file_format="PARQUET",
        record_count=record_count,
        file_size_in_bytes=0,
        lower_bounds={fid: lo for fid, (lo, _hi) in bounds.items()},
        upper_bounds={fid: hi for fid, (_lo, hi) in bounds.items()},
    )


def test_get_ordinal_bounds_uses_real_field_id_not_position():
    # Three columns; field_ids deliberately offset/non-sequential from
    # position, mirroring the live catalog schema that exposed this bug
    # (log_name at POSITION 1 but real field_id 3).
    schema = _multi_col_schema(
        names_and_field_ids=[("a", 1), ("log_name", 3), ("c", 4)]
    )
    entry = _bounded_file_entry(
        {
            1: (VARCHAR.ordinalize("aaa"), VARCHAR.ordinalize("azz")),
            3: (VARCHAR.ordinalize("log-alpha"), VARCHAR.ordinalize("log-omega")),
            4: (VARCHAR.ordinalize("ccc"), VARCHAR.ordinalize("czz")),
        }
    )
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    bounds = manifest.get_ordinal_bounds("log_name")

    assert bounds == (VARCHAR.ordinalize("log-alpha"), VARCHAR.ordinalize("log-omega"))
    # Not column "a"'s or "c"'s bounds — the exact failure mode of indexing
    # min_values[field_id] against a 0-indexed positional list.
    assert bounds != (VARCHAR.ordinalize("aaa"), VARCHAR.ordinalize("azz"))
    assert bounds != (VARCHAR.ordinalize("ccc"), VARCHAR.ordinalize("czz"))


def test_get_ordinal_bounds_aggregates_across_files():
    schema = _multi_col_schema(names_and_field_ids=[("value", 0)])
    lo1, hi1 = VARCHAR.ordinalize("mango"), VARCHAR.ordinalize("peach")
    lo2, hi2 = VARCHAR.ordinalize("apple"), VARCHAR.ordinalize("kiwi")
    entries = [
        _bounded_file_entry({0: (lo1, hi1)}),
        _bounded_file_entry({0: (lo2, hi2)}),
    ]
    manifest = Manifest(files=entries, schema=schema, bounds_are_ordinal=True)

    assert manifest.get_ordinal_bounds("value") == (min(lo1, lo2), max(hi1, hi2))


def test_get_ordinal_bounds_excludes_negative_sentinel():
    # A negative bound can only be a producer's own "no real bound" sentinel
    # (e.g. the catalog manifest builder's NULL_FLAG = -(1<<63) for a column
    # outside its compressible-categories set) — never a genuine
    # string-family ordinal key (draken/ops/ordinalize.h's byte-prefix
    # transform is always non-negative). A file carrying only the sentinel
    # must not corrupt the aggregate with it.
    schema = _multi_col_schema(names_and_field_ids=[("value", 0)])
    real_lo, real_hi = VARCHAR.ordinalize("mango"), VARCHAR.ordinalize("peach")
    entries = [
        _bounded_file_entry({0: (-(1 << 63), -(1 << 63))}),  # sentinel-only file
        _bounded_file_entry({0: (real_lo, real_hi)}),
    ]
    manifest = Manifest(files=entries, schema=schema, bounds_are_ordinal=True)

    assert manifest.get_ordinal_bounds("value") == (real_lo, real_hi)


def test_get_ordinal_bounds_all_sentinel_returns_none():
    schema = _multi_col_schema(names_and_field_ids=[("value", 0)])
    entry = _bounded_file_entry({0: (-(1 << 63), -(1 << 63))})
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    assert manifest.get_ordinal_bounds("value") is None


def test_get_ordinal_bounds_none_when_bounds_not_ordinal():
    schema = _multi_col_schema(names_and_field_ids=[("value", 0)])
    entry = _bounded_file_entry({0: (10, 20)})
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=False)

    assert manifest.get_ordinal_bounds("value") is None


def test_get_ordinal_bounds_none_for_unknown_column():
    schema = _multi_col_schema(names_and_field_ids=[("value", 0)])
    entry = _bounded_file_entry({0: (10, 20)})
    manifest = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)

    assert manifest.get_ordinal_bounds("missing") is None


# ---------------------------------------------------------------------------
# Manifest.get_length_bounds — backs the length-aware hard-impossibility
# guard shared by STARTS_WITH/INSTR/ENDS_WITH selectivity estimation. Same
# field_id-vs-position trap as get_ordinal_bounds: FileEntry.min_lengths/
# max_lengths (the plain list) is positional by write order, never indexable
# by real field_id — only min_length_bounds/max_length_bounds (the dict form
# FileEntry.from_datafile now builds via zip(field_ids, min_lengths)) is
# field_id-correct. No bounds_are_ordinal gate (lengths are plain integers
# regardless); non-positive bounds are excluded instead (0 is ambiguous
# between "no data" and "genuinely empty string" — see get_length_bounds'
# own docstring).
# ---------------------------------------------------------------------------


def _length_bounded_file_entry(bounds: dict, record_count: int = 10):
    """A FileEntry whose min_length_bounds/max_length_bounds dict is keyed by
    the field_ids given in `bounds` (``{field_id: (min_len, max_len)}``)."""
    return FileEntry(
        file_path="f1",
        file_format="PARQUET",
        record_count=record_count,
        file_size_in_bytes=0,
        min_length_bounds={fid: lo for fid, (lo, _hi) in bounds.items()},
        max_length_bounds={fid: hi for fid, (_lo, hi) in bounds.items()},
    )


def test_get_length_bounds_uses_real_field_id_not_position():
    schema = _multi_col_schema(names_and_field_ids=[("a", 1), ("log_name", 3), ("c", 4)])
    entry = _length_bounded_file_entry({1: (2, 5), 3: (40, 60), 4: (7, 9)})
    manifest = Manifest(files=[entry], schema=schema)

    bounds = manifest.get_length_bounds("log_name")

    assert bounds == (40, 60)
    assert bounds != (2, 5)
    assert bounds != (7, 9)


def test_get_length_bounds_aggregates_across_files():
    schema = _multi_col_schema(names_and_field_ids=[("value", 0)])
    entries = [
        _length_bounded_file_entry({0: (10, 25)}),
        _length_bounded_file_entry({0: (5, 30)}),
    ]
    manifest = Manifest(files=entries, schema=schema)

    assert manifest.get_length_bounds("value") == (5, 30)


def test_get_length_bounds_excludes_non_positive_values():
    # 0 is the catalog's "no data computed for this file" default (min_len =
    # max_len = 0, only overwritten when the file has a non-null value) --
    # ambiguous with a genuinely empty string, so treated as no signal, not
    # a real bound of 0.
    schema = _multi_col_schema(names_and_field_ids=[("value", 0)])
    entries = [
        _length_bounded_file_entry({0: (0, 0)}),  # no-data-computed file
        _length_bounded_file_entry({0: (8, 12)}),
    ]
    manifest = Manifest(files=entries, schema=schema)

    assert manifest.get_length_bounds("value") == (8, 12)


def test_get_length_bounds_all_non_positive_returns_none():
    schema = _multi_col_schema(names_and_field_ids=[("value", 0)])
    entry = _length_bounded_file_entry({0: (0, 0)})
    manifest = Manifest(files=[entry], schema=schema)

    assert manifest.get_length_bounds("value") is None


def test_get_length_bounds_does_not_require_bounds_are_ordinal():
    # Unlike get_ordinal_bounds, lengths are never ordinal-encoded -- must
    # work identically regardless of bounds_are_ordinal.
    schema = _multi_col_schema(names_and_field_ids=[("value", 0)])
    entry = _length_bounded_file_entry({0: (8, 12)})
    manifest_ordinal = Manifest(files=[entry], schema=schema, bounds_are_ordinal=True)
    manifest_real = Manifest(files=[entry], schema=schema, bounds_are_ordinal=False)

    assert manifest_ordinal.get_length_bounds("value") == (8, 12)
    assert manifest_real.get_length_bounds("value") == (8, 12)


def test_get_length_bounds_none_for_unknown_column():
    schema = _multi_col_schema(names_and_field_ids=[("value", 0)])
    entry = _length_bounded_file_entry({0: (8, 12)})
    manifest = Manifest(files=[entry], schema=schema)

    assert manifest.get_length_bounds("missing") is None
