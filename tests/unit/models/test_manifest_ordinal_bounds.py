# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Regression tests for Manifest's `bounds_are_ordinal` flag and its use in
`prune_files`.

ANALYZE's native per-file statistics pass writes min_values/max_values into
the dataset manifest as `Vector.ordinalize()` ordinal int64 keys, not real
decoded values (draken/ops/ordinalize.h states what they are). A
predicate literal must be run through the SAME ordinalize transform before it
is comparable to those bounds. These tests exercise `Manifest.prune_files`
directly (no filesystem/ANALYZE I/O) so the pruning arithmetic itself is
pinned down:

- INT columns: ordinalize is an identity widen, so ordinal-encoded pruning
  must behave exactly like real-value pruning.
- FLOAT/VARCHAR columns: the ordinal key is NOT the real value (a monotonic
  but lossy bit-transform) — pruning must still be correct because both the
  bound and the literal go through the same transform.
- bounds_are_ordinal=False (LocalStoreConnector's parquet-footer bounds) must
  keep comparing real values directly, completely unaffected by the ordinalize
  path.
- A physical type with no scalar ordinalize kernel (DECIMAL128) must not crash
  pruning — the predicate is conservatively skipped (file kept), not pruned on
  a comparison that can't be made safely.
"""

from __future__ import annotations

import decimal

from opteryx.connectors.opteryx_connector import _catalog_manifest
from opteryx.expression import NodeType
from opteryx.types.logical_type import DECIMAL, FLOAT64, INT64, VARCHAR
from opteryx.types.schema import RelationSchema
from opteryx.compiled.structures.expressions import Between
from opteryx.compiled.structures.expressions import Comparison
from opteryx.compiled.structures.expressions import Literal
from opteryx.compiled.structures.expressions import LogicalColumn
from opteryx.planner.plan_context import PlanContext
from tests.manifests import NULL_FLAG
from tests.manifests import FileSpec
from tests.manifests import build_manifest


def _schema(plan_context, column_type, name="value"):
    return RelationSchema(
        name="t",
        columns=[
            plan_context.columns.relation_column(
                "t", name, column_type=column_type)
        ],
    )


def _literal(plan_context, value, column_type):
    return Literal(value=value, type=column_type, arena=plan_context.expressions)


def _column(plan_context, column_name):
    return LogicalColumn(
        node_type=NodeType.IDENTIFIER, source_column=column_name, arena=plan_context.expressions
    )


def _comparison(plan_context, column_name, op, value, column_type):
    return Comparison(
        value=op,
        left=_column(plan_context, column_name),
        right=_literal(plan_context, value, column_type),
        arena=plan_context.expressions,
    )


def _between(plan_context, column_name, lower, upper, column_type):
    return Between(
        left=_column(plan_context, column_name),
        right=_literal(plan_context, lower, column_type),
        centre=_literal(plan_context, upper, column_type),
        arena=plan_context.expressions,
    )


def _file_entry(lower, upper):
    return FileSpec(
        file_path="f1",
        record_count=10,
        lower_bounds={0: lower},
        upper_bounds={0: upper},
    )


def _manifest(plan_context, column_type, lower, upper, *, bounds_are_ordinal):
    return build_manifest(
        _schema(plan_context, column_type),
        [_file_entry(lower, upper)],
        bounds_are_ordinal=bounds_are_ordinal,
    )


# ---------------------------------------------------------------------------
# INT columns: ordinalize() is an identity widen, so ordinal-encoded pruning
# must match real-value pruning exactly.
# ---------------------------------------------------------------------------


def test_int_ordinal_bounds_prune_out_of_range_value():
    plan_context = PlanContext()
    manifest = _manifest(
        plan_context, INT64, INT64.ordinalize(10), INT64.ordinalize(20), bounds_are_ordinal=True
    )

    manifest = manifest.prune_files(
        [_comparison(plan_context, "value", "Gt", 100, INT64)], plan_context=plan_context
    )

    assert manifest.get_file_count() == 0


def test_int_ordinal_bounds_keep_in_range_value():
    plan_context = PlanContext()
    manifest = _manifest(
        plan_context, INT64, INT64.ordinalize(10), INT64.ordinalize(20), bounds_are_ordinal=True
    )

    manifest = manifest.prune_files(
        [_comparison(plan_context, "value", "Gt", 5, INT64)], plan_context=plan_context
    )

    assert manifest.get_file_count() == 1


def test_int_ordinal_bounds_match_real_value_bounds_behaviour():
    """Identity ordinalize -> pruning decisions must be identical to a
    hypothetical real-value comparison over the same numbers."""
    plan_context = PlanContext()
    schema = _schema(plan_context, INT64)

    for op, literal in (("Gt", 25), ("Lt", 5), ("Eq", 15), ("Eq", 999), ("GtEq", 20)):
        ordinal_manifest = build_manifest(
            schema,
            [_file_entry(INT64.ordinalize(10), INT64.ordinalize(20))],
            bounds_are_ordinal=True,
        )
        real_manifest = build_manifest(
            schema,
            [_file_entry(10, 20)],
            bounds_are_ordinal=False,
        )

        predicate = [_comparison(plan_context, "value", op, literal, INT64)]
        ordinal_manifest = ordinal_manifest.prune_files(predicate, plan_context=plan_context)
        real_manifest = real_manifest.prune_files(predicate, plan_context=plan_context)

        assert ordinal_manifest.get_file_count() == real_manifest.get_file_count(), (op, literal)


def test_int_ordinal_bounds_between_prunes_out_of_range():
    plan_context = PlanContext()
    manifest = _manifest(
        plan_context, INT64, INT64.ordinalize(10), INT64.ordinalize(20), bounds_are_ordinal=True
    )

    manifest = manifest.prune_files(
        [_between(plan_context, "value", 100, 200, INT64)], plan_context=plan_context
    )

    assert manifest.get_file_count() == 0


# ---------------------------------------------------------------------------
# FLOAT columns: ordinal key is NOT the real value.
# ---------------------------------------------------------------------------


def test_float_ordinal_bounds_are_not_real_values():
    # Sanity-check the premise: the stored ordinal key for a float bound is a
    # different number to the float itself.
    assert FLOAT64.ordinalize(10.5) != 10.5
    assert FLOAT64.ordinalize(10.5) != int(10.5)


def test_float_ordinal_bounds_prune_out_of_range_value():
    plan_context = PlanContext()
    manifest = _manifest(
        plan_context, FLOAT64, FLOAT64.ordinalize(10.0), FLOAT64.ordinalize(20.0), bounds_are_ordinal=True
    )

    # 100.0 is well outside [10.0, 20.0] — must prune despite the bounds being
    # stored as unrelated-looking ordinal integers.
    manifest = manifest.prune_files(
        [_comparison(plan_context, "value", "Gt", 100.0, FLOAT64)], plan_context=plan_context
    )

    assert manifest.get_file_count() == 0


def test_float_ordinal_bounds_keep_in_range_value():
    plan_context = PlanContext()
    manifest = _manifest(
        plan_context, FLOAT64, FLOAT64.ordinalize(10.0), FLOAT64.ordinalize(20.0), bounds_are_ordinal=True
    )

    manifest = manifest.prune_files(
        [_comparison(plan_context, "value", "Eq", 15.5, FLOAT64)], plan_context=plan_context
    )

    assert manifest.get_file_count() == 1


def test_float_ordinal_bounds_prune_negative_values_correctly():
    plan_context = PlanContext()
    # Negative floats ordinalize to a different sign/magnitude relationship
    # than the raw IEEE bits (see ordinalize_scalar_f64) — exercise a range
    # that straddles zero and a literal clearly outside it.
    manifest = _manifest(
        plan_context, FLOAT64, FLOAT64.ordinalize(-5.0), FLOAT64.ordinalize(5.0), bounds_are_ordinal=True
    )

    manifest = manifest.prune_files(
        [_comparison(plan_context, "value", "Lt", -100.0, FLOAT64)], plan_context=plan_context
    )

    assert manifest.get_file_count() == 0


def test_float_ordinal_bounds_between_keeps_overlapping_range():
    plan_context = PlanContext()
    manifest = _manifest(
        plan_context, FLOAT64, FLOAT64.ordinalize(10.0), FLOAT64.ordinalize(20.0), bounds_are_ordinal=True
    )

    manifest = manifest.prune_files(
        [_between(plan_context, "value", 15.0, 16.0, FLOAT64)], plan_context=plan_context
    )

    assert manifest.get_file_count() == 1


def test_float_ordinal_bounds_between_prunes_disjoint_range():
    plan_context = PlanContext()
    manifest = _manifest(
        plan_context, FLOAT64, FLOAT64.ordinalize(10.0), FLOAT64.ordinalize(20.0), bounds_are_ordinal=True
    )

    manifest = manifest.prune_files(
        [_between(plan_context, "value", 1000.0, 2000.0, FLOAT64)], plan_context=plan_context
    )

    assert manifest.get_file_count() == 0


# ---------------------------------------------------------------------------
# VARCHAR columns: ordinal key is a lossy 8-byte-prefix bit-transform.
# ---------------------------------------------------------------------------


def test_varchar_ordinal_bounds_are_not_real_values():
    assert VARCHAR.ordinalize("apple") != "apple"
    assert type(VARCHAR.ordinalize("apple")) is int


def _varchar_manifest(plan_context):
    return _manifest(
        plan_context,
        VARCHAR,
        VARCHAR.ordinalize("banana"),
        VARCHAR.ordinalize("cherry"),
        bounds_are_ordinal=True,
    )


def test_varchar_ordinal_bounds_prune_out_of_range_value():
    plan_context = PlanContext()
    manifest = _varchar_manifest(plan_context)

    # "apple" sorts before "banana" — out of [banana, cherry] range.
    manifest = manifest.prune_files(
        [_comparison(plan_context, "value", "Eq", b"apple", VARCHAR)], plan_context=plan_context
    )

    assert manifest.get_file_count() == 0


def test_varchar_ordinal_bounds_keep_in_range_value():
    plan_context = PlanContext()
    manifest = _varchar_manifest(plan_context)

    manifest = manifest.prune_files(
        [_comparison(plan_context, "value", "Eq", b"banana", VARCHAR)], plan_context=plan_context
    )

    assert manifest.get_file_count() == 1


def test_varchar_ordinal_bounds_between_prunes_disjoint_range():
    plan_context = PlanContext()
    manifest = _varchar_manifest(plan_context)

    manifest = manifest.prune_files(
        [_between(plan_context, "value", b"xylophone", b"zebra", VARCHAR)], plan_context=plan_context
    )

    assert manifest.get_file_count() == 0


def test_varchar_ordinal_bounds_gt_prunes_correctly():
    plan_context = PlanContext()
    manifest = _varchar_manifest(plan_context)

    manifest = manifest.prune_files(
        [_comparison(plan_context, "value", "Gt", b"zebra", VARCHAR)], plan_context=plan_context
    )

    assert manifest.get_file_count() == 0


# ---------------------------------------------------------------------------
# bounds_are_ordinal=False: real-value comparison path must be completely
# unaffected — this is LocalStoreConnector's path (parquet-footer bounds),
# never ordinal-encoded.
# ---------------------------------------------------------------------------


def test_real_value_bounds_still_compare_literal_directly_for_float():
    """With bounds_are_ordinal False, a FLOAT literal must be compared AS-IS
    against the stored bound (no ordinalize). Store the bound as an ordinal key
    but declare the bounds real: since the literal is never converted, it must
    be compared against the (numerically unrelated) ordinal integer and
    therefore prune a value that would be in-range under real comparison —
    proving the literal was never routed through ordinalize."""
    plan_context = PlanContext()
    ordinal_min = FLOAT64.ordinalize(10.0)
    ordinal_max = FLOAT64.ordinalize(20.0)
    manifest = _manifest(plan_context, FLOAT64, ordinal_min, ordinal_max, bounds_are_ordinal=False)

    # 15.0 is well within the REAL range [10.0, 20.0], but the stored bounds
    # are huge ordinal integers — a direct (non-ordinalized) comparison finds
    # 15.0 far below both bounds and prunes the file.
    manifest = manifest.prune_files(
        [_comparison(plan_context, "value", "Lt", 15.0, FLOAT64)], plan_context=plan_context
    )

    assert manifest.get_file_count() == 0, "literal must not have been ordinalized"


def test_real_value_bounds_pruning_matches_pre_existing_behaviour():
    """Parquet-footer bounds are real decoded values; pruning over them must
    behave exactly as before the ordinal dialect existed."""
    plan_context = PlanContext()
    manifest = _manifest(plan_context, INT64, 10, 20, bounds_are_ordinal=False)

    manifest = manifest.prune_files(
        [_comparison(plan_context, "value", "Gt", 25, INT64)], plan_context=plan_context
    )
    assert manifest.get_file_count() == 0

    manifest = _manifest(plan_context, INT64, 10, 20, bounds_are_ordinal=False)
    manifest = manifest.prune_files(
        [_comparison(plan_context, "value", "Gt", 5, INT64)], plan_context=plan_context
    )
    assert manifest.get_file_count() == 1


def test_real_value_varchar_bounds_unaffected():
    plan_context = PlanContext()
    manifest = _manifest(plan_context, VARCHAR, b"banana", b"cherry", bounds_are_ordinal=False)

    manifest = manifest.prune_files(
        [_comparison(plan_context, "value", "Eq", b"apple", VARCHAR)], plan_context=plan_context
    )

    assert manifest.get_file_count() == 0


# ---------------------------------------------------------------------------
# Physical types with no scalar ordinalize kernel: conservative skip, no crash.
# ---------------------------------------------------------------------------


def test_unsupported_ordinalize_type_skips_pruning_without_crashing():
    plan_context = PlanContext()
    # precision > 18 selects the int128-backed DECIMAL128 tier, which draken
    # gives no ordinal key.
    d128 = DECIMAL(38, 4)
    manifest = _manifest(plan_context, d128, 0, 100, bounds_are_ordinal=True)

    predicate = _comparison(plan_context, "value", "Gt", decimal.Decimal("999999"), d128)
    manifest = manifest.prune_files([predicate], plan_context=plan_context)

    # Can't ordinalize a DECIMAL128 literal — the predicate is skipped (file
    # kept), not used to wrongly prune or crash.
    assert manifest.get_file_count() == 1


# ---------------------------------------------------------------------------
# Manifest.get_ordinal_bounds — backs the STARTS_WITH ordinal-bounds
# selectivity estimator tier. field_id is the trap: a catalog-backed dataset
# assigns real, non-positional field_ids (observed live: insert_id=1,
# labels=2, log_name=3, receive_timestamp=4, ...) and a catalog row's
# min_values/max_values lists are keyed by the row's own `field_ids`, never
# indexable by field_id directly. `_catalog_manifest` maps each field id to the
# column's load-time position - the manifest's one key space - and reading a
# list by field_id instead silently reads a DIFFERENT column's bound whenever
# field_id != position — this is exactly the bug this section pins down.
# ---------------------------------------------------------------------------


def _multi_col_schema(plan_context, *, names_and_field_ids):
    return RelationSchema(
        name="t",
        columns=[
            plan_context.columns.relation_column(
                "t",
                name,
                column_type=VARCHAR,
                field_id=field_id,
            )
            for name, field_id in names_and_field_ids
        ],
    )


def _catalog_row(field_ids, **lists):
    """A catalog manifest row: per-column lists in the row's own column order,
    keyed by `field_ids`."""
    return {
        "file_path": "f1",
        "record_count": 10,
        "file_size_in_bytes": 0,
        "field_ids": field_ids,
        **lists,
    }


def _ordinal_bounded_file(lower, upper):
    """A file whose one column (position 0) is bounded by ordinal keys."""
    return FileSpec(
        file_path="f1",
        record_count=10,
        lower_bounds={0: lower},
        upper_bounds={0: upper},
    )


def test_get_ordinal_bounds_uses_real_field_id_not_position():
    plan_context = PlanContext()
    # Three columns; field_ids deliberately offset/non-sequential from
    # position, mirroring the live catalog schema that exposed this bug
    # (log_name at POSITION 1 but real field_id 3).
    schema = _multi_col_schema(
        plan_context, names_and_field_ids=[("a", 1), ("log_name", 3), ("c", 4)]
    )
    row = _catalog_row(
        [1, 3, 4],
        min_values=[
            VARCHAR.ordinalize("aaa"),
            VARCHAR.ordinalize("log-alpha"),
            VARCHAR.ordinalize("ccc"),
        ],
        max_values=[
            VARCHAR.ordinalize("azz"),
            VARCHAR.ordinalize("log-omega"),
            VARCHAR.ordinalize("czz"),
        ],
    )
    manifest = _catalog_manifest(schema, True, [row], {}, None)

    bounds = manifest.get_ordinal_bounds("log_name")

    assert bounds == (VARCHAR.ordinalize("log-alpha"), VARCHAR.ordinalize("log-omega"))
    # Not column "a"'s or "c"'s bounds — the exact failure mode of indexing
    # a positional list by field_id.
    assert bounds != (VARCHAR.ordinalize("aaa"), VARCHAR.ordinalize("azz"))
    assert bounds != (VARCHAR.ordinalize("ccc"), VARCHAR.ordinalize("czz"))


def test_get_ordinal_bounds_aggregates_across_files():
    plan_context = PlanContext()
    schema = _multi_col_schema(plan_context, names_and_field_ids=[("value", 0)])
    lo1, hi1 = VARCHAR.ordinalize("mango"), VARCHAR.ordinalize("peach")
    lo2, hi2 = VARCHAR.ordinalize("apple"), VARCHAR.ordinalize("kiwi")
    files = [_ordinal_bounded_file(lo1, hi1), _ordinal_bounded_file(lo2, hi2)]
    manifest = build_manifest(schema, files, bounds_are_ordinal=True)

    assert manifest.get_ordinal_bounds("value") == (min(lo1, lo2), max(hi1, hi2))


def test_get_ordinal_bounds_excludes_negative_sentinel():
    # A negative bound can only be a producer's own "no real bound" sentinel
    # (e.g. the catalog manifest builder's NULL_FLAG = -(1<<63) for a column
    # outside its compressible-categories set) — never a genuine
    # string-family ordinal key (draken/ops/ordinalize.h's byte-prefix
    # transform is always non-negative). A file carrying only the sentinel
    # must not corrupt the aggregate with it.
    plan_context = PlanContext()
    schema = _multi_col_schema(plan_context, names_and_field_ids=[("value", 0)])
    real_lo, real_hi = VARCHAR.ordinalize("mango"), VARCHAR.ordinalize("peach")
    files = [
        _ordinal_bounded_file(NULL_FLAG, NULL_FLAG),  # sentinel-only file
        _ordinal_bounded_file(real_lo, real_hi),
    ]
    manifest = build_manifest(schema, files, bounds_are_ordinal=True)

    assert manifest.get_ordinal_bounds("value") == (real_lo, real_hi)


def test_get_ordinal_bounds_all_sentinel_returns_none():
    plan_context = PlanContext()
    schema = _multi_col_schema(plan_context, names_and_field_ids=[("value", 0)])
    files = [_ordinal_bounded_file(NULL_FLAG, NULL_FLAG)]
    manifest = build_manifest(schema, files, bounds_are_ordinal=True)

    assert manifest.get_ordinal_bounds("value") is None


def test_get_ordinal_bounds_none_when_bounds_not_ordinal():
    plan_context = PlanContext()
    schema = _multi_col_schema(plan_context, names_and_field_ids=[("value", 0)])
    manifest = build_manifest(schema, [_ordinal_bounded_file(10, 20)], bounds_are_ordinal=False)

    assert manifest.get_ordinal_bounds("value") is None


def test_get_ordinal_bounds_none_for_unknown_column():
    plan_context = PlanContext()
    schema = _multi_col_schema(plan_context, names_and_field_ids=[("value", 0)])
    manifest = build_manifest(schema, [_ordinal_bounded_file(10, 20)], bounds_are_ordinal=True)

    assert manifest.get_ordinal_bounds("missing") is None


# ---------------------------------------------------------------------------
# Manifest.get_length_bounds — backs the length-aware hard-impossibility
# guard shared by STARTS_WITH/INSTR/ENDS_WITH selectivity estimation. Same
# field_id-vs-position trap as get_ordinal_bounds: a catalog row's
# min_lengths/max_lengths lists are keyed by the row's own `field_ids`, and
# `_catalog_manifest` maps them to load-time positions. No bounds_are_ordinal
# gate (lengths are plain integers regardless); non-positive bounds are
# excluded instead (0 is ambiguous between "no data" and "genuinely empty
# string" — see get_length_bounds' own docstring).
# ---------------------------------------------------------------------------


def _length_bounded_file(min_length, max_length):
    """A file whose one column (position 0) carries (min_len, max_len)."""
    return FileSpec(
        file_path="f1",
        record_count=10,
        min_length_bounds={0: min_length},
        max_length_bounds={0: max_length},
    )


def test_get_length_bounds_uses_real_field_id_not_position():
    plan_context = PlanContext()
    schema = _multi_col_schema(
        plan_context, names_and_field_ids=[("a", 1), ("log_name", 3), ("c", 4)]
    )
    row = _catalog_row([1, 3, 4], min_lengths=[2, 40, 7], max_lengths=[5, 60, 9])
    manifest = _catalog_manifest(schema, False, [row], {}, None)

    bounds = manifest.get_length_bounds("log_name")

    assert bounds == (40, 60)
    assert bounds != (2, 5)
    assert bounds != (7, 9)


def test_get_length_bounds_aggregates_across_files():
    plan_context = PlanContext()
    schema = _multi_col_schema(plan_context, names_and_field_ids=[("value", 0)])
    files = [_length_bounded_file(10, 25), _length_bounded_file(5, 30)]
    manifest = build_manifest(schema, files)

    assert manifest.get_length_bounds("value") == (5, 30)


def test_get_length_bounds_excludes_non_positive_values():
    # 0 is the catalog's "no data computed for this file" default (min_len =
    # max_len = 0, only overwritten when the file has a non-null value) --
    # ambiguous with a genuinely empty string, so treated as no signal, not
    # a real bound of 0.
    plan_context = PlanContext()
    schema = _multi_col_schema(plan_context, names_and_field_ids=[("value", 0)])
    files = [
        _length_bounded_file(0, 0),  # no-data-computed file
        _length_bounded_file(8, 12),
    ]
    manifest = build_manifest(schema, files)

    assert manifest.get_length_bounds("value") == (8, 12)


def test_get_length_bounds_all_non_positive_returns_none():
    plan_context = PlanContext()
    schema = _multi_col_schema(plan_context, names_and_field_ids=[("value", 0)])
    manifest = build_manifest(schema, [_length_bounded_file(0, 0)])

    assert manifest.get_length_bounds("value") is None


def test_get_length_bounds_does_not_require_bounds_are_ordinal():
    # Unlike get_ordinal_bounds, lengths are never ordinal-encoded -- must
    # work identically regardless of bounds_are_ordinal.
    plan_context = PlanContext()
    schema = _multi_col_schema(plan_context, names_and_field_ids=[("value", 0)])
    manifest_ordinal = build_manifest(schema, [_length_bounded_file(8, 12)], bounds_are_ordinal=True)
    manifest_real = build_manifest(schema, [_length_bounded_file(8, 12)], bounds_are_ordinal=False)

    assert manifest_ordinal.get_length_bounds("value") == (8, 12)
    assert manifest_real.get_length_bounds("value") == (8, 12)


def test_get_length_bounds_none_for_unknown_column():
    plan_context = PlanContext()
    schema = _multi_col_schema(plan_context, names_and_field_ids=[("value", 0)])
    manifest = build_manifest(schema, [_length_bounded_file(8, 12)])

    assert manifest.get_length_bounds("missing") is None
