# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Manifest pruning must not treat a float column's bounds as covering a NaN.

Float semantics are architect-locked (draken/ops/float_ops.h, 2026-05-22): NaN
is a VALUE with its validity bit set, and it ranks ABOVE every finite and ±inf.
So `NaN > 1000.0` is TRUE and a file holding one must be read for that predicate.

Whether the file's recorded bounds can see that NaN depends on WHERE THE BOUNDS
CAME FROM, and the two provenances genuinely differ:

* `bounds_are_ordinal=True` (ANALYZE / skene) — bounds are `Vector.ordinalize()`
  int64 keys, and ordinalize maps canonical quiet NaN to 9221120237041090560,
  strictly above +inf's 9218868437227405312. The upper bound DOES cover a NaN
  row, so pruning stays fully enabled. That half is asserted here too: the fix
  must not become a blanket "floats are unprunable", which would quietly cost
  the catalog its range pruning on every float column it has.
* `bounds_are_ordinal=False` (CTAS via `write_parquet_with_bounds`) — bounds are
  rugo's parquet min/max, and rugo skips NaN to spec
  (rugo/src/parquet/_parquet_writer.hpp). The NaN is invisible, so the upper
  bound is not an upper bound and the ops resting on it must stand down.

The direction matters: only the tests that reason about values ABOVE col_max are
unsound. `Lt`/`LtEq`/`Eq` keep pruning on floats in both provenances, because a
NaN never satisfies them for a non-NaN literal. Those are pinned below so the
stand-down can never be widened into one that disables float pruning wholesale.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.models.file_entry import FileEntry
from opteryx.models.manifest import Manifest
from opteryx.types.logical_type import FLOAT64, INT64
from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity

# Bounds over a file whose real values are 0.0 .. 10.0 PLUS one NaN. The NaN is
# absent from both bounds — that absence is the whole subject.
LOW = 0.0
HIGH = 10.0

# A literal above every recorded value. Every op below is evaluated against it,
# so "prunes" and "does not prune" are both meaningful answers.
ABOVE = 1000.0

# Ops whose prune test reasons about values above col_max (or about the group
# being a single value), and so cannot survive an invisible NaN.
UNSOUND = ("Gt", "GtEq", "NotEq")
# Ops a NaN can never satisfy for a non-NaN literal — pruning stays correct.
SOUND = ("Lt", "LtEq", "Eq")


def _schema(column_type, name="value"):
    return RelationSchema(
        name="t",
        columns=[
            SchemaColumn(
                name=name, column_type=column_type, identity=mint_column_identity("t", name)
            )
        ],
    )


def _file(lower, upper, path="f1", record_count=10):
    return FileEntry(
        file_path=path,
        file_format="PARQUET",
        record_count=record_count,
        file_size_in_bytes=0,
        lower_bounds={0: lower},
        upper_bounds={0: upper},
    )


def _comparison(op, value, column_name="value"):
    return Node(
        NodeType.COMPARISON_OPERATOR,
        value=op,
        left=Node(NodeType.IDENTIFIER, source_column=column_name),
        right=Node(NodeType.LITERAL, value=value),
    )


def _between(lower, upper, column_name="value"):
    return Node(
        NodeType.BETWEEN,
        left=Node(NodeType.IDENTIFIER, source_column=column_name),
        right=Node(NodeType.LITERAL, value=lower),
        centre=Node(NodeType.LITERAL, value=upper),
    )


def _manifest(column_type, *, ordinal, lower=LOW, upper=HIGH):
    return Manifest(
        files=[_file(lower, upper)], schema=_schema(column_type), bounds_are_ordinal=ordinal
    )


# `NotEq` prunes only on `min == max == value`, so it needs a single-valued file
# to be asked a real question at all — against [0.0, 10.0] it declines to prune
# for reasons that have nothing to do with NaN, and a test built that way passes
# whatever the guard does. Each op is therefore paired with bounds and a literal
# that make pruning the answer when NaN is not in the picture.
SINGLE = 5.0


def _case(op, ordinalize=None):
    """(lower, upper, literal) that the bounds DISPROVE for `op`, so a correct
    pruner drops the file whenever an invisible NaN is not a factor."""
    lower, upper, literal = {
        "Gt": (LOW, HIGH, ABOVE),
        "GtEq": (LOW, HIGH, ABOVE),
        "NotEq": (SINGLE, SINGLE, SINGLE),
        "Lt": (LOW, HIGH, LOW - 1.0),
        "LtEq": (LOW, HIGH, LOW - 1.0),
        "Eq": (LOW, HIGH, ABOVE),
    }[op]
    if ordinalize is not None:
        lower, upper = ordinalize(lower), ordinalize(upper)
    return lower, upper, literal


# ---------------------------------------------------------------------------
# real-value bounds (CTAS) — the upper bound cannot see a NaN
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("op", UNSOUND)
def test_float_file_is_kept_for_ops_a_nan_would_satisfy(op):
    lower, upper, literal = _case(op)
    manifest = _manifest(FLOAT64, ordinal=False, lower=lower, upper=upper)

    manifest.prune_files([_comparison(op, literal)])

    assert len(manifest.files) == 1, (
        f"{op} pruned a float file on bounds that cannot see a NaN — a NaN row "
        f"satisfies `{op} {literal}` and would be silently dropped"
    )


@pytest.mark.parametrize("op", SOUND)
def test_float_file_still_prunes_for_ops_a_nan_cannot_satisfy(op):
    # `< -1.0` / `<= -1.0` / `= 1000.0` are all disproved by [0.0, 10.0], and a
    # NaN satisfies none of them, so the prune is correct and must still happen.
    lower, upper, literal = _case(op)
    manifest = _manifest(FLOAT64, ordinal=False, lower=lower, upper=upper)

    manifest.prune_files([_comparison(op, literal)])

    assert len(manifest.files) == 0, f"{op} stopped pruning floats — the fix is too wide"


@pytest.mark.parametrize("op", UNSOUND + SOUND)
def test_non_float_columns_are_untouched(op):
    # An INT64 column cannot hold a NaN, so every op must still prune. Pinned
    # because a guard written against the wrong thing (all numerics, say) would
    # cost every integer range predicate its pruning and never fail a NaN test.
    lower, upper, literal = _case(op)
    manifest = _manifest(INT64, ordinal=False, lower=int(lower), upper=int(upper))

    manifest.prune_files([_comparison(op, int(literal))])

    assert len(manifest.files) == 0, f"{op} stopped pruning an INT64 column"


def test_between_keeps_the_arm_a_nan_cannot_satisfy():
    # BETWEEN is two conjuncts. `value BETWEEN 1000.0 AND 2000.0` is disproved
    # ONLY by the `max < lower` half — the unsound one — so the file is kept.
    manifest = _manifest(FLOAT64, ordinal=False)
    manifest.prune_files([_between(ABOVE, ABOVE * 2)])
    assert len(manifest.files) == 1, "BETWEEN pruned a float file on the NaN-blind arm"

    # `value BETWEEN -20.0 AND -10.0` is disproved by the `min > upper` half,
    # which a NaN cannot affect — that arm must still prune.
    manifest = _manifest(FLOAT64, ordinal=False)
    manifest.prune_files([_between(-20.0, -10.0)])
    assert len(manifest.files) == 0, "BETWEEN lost the sound half of its float pruning"


def test_topn_pruning_stands_down_for_float_columns():
    # DESC top-n: the NaN rows ARE the top-n but sit outside every `hi`, so a
    # file holding them ranks last and is dropped. The zero-NULL precondition
    # this method documents does not cover a NaN — a NaN is not a null.
    manifest = Manifest(
        files=[_file(LOW, HIGH, path="f1"), _file(100.0, 200.0, path="f2")],
        schema=_schema(FLOAT64),
        bounds_are_ordinal=False,
    )

    manifest.prune_files_for_topn("value", descending=True, limit=1)

    assert len(manifest.files) == 2, "top-n pruning dropped a float file that may hold a NaN"


# ---------------------------------------------------------------------------
# ordinal bounds (ANALYZE) — ordinalize ranks NaN highest, so bounds are real
# ---------------------------------------------------------------------------


def test_ordinalize_puts_nan_above_every_float():
    # The premise the ordinal half of the fix rests on, asserted rather than
    # assumed: if this ever stops holding, the catalog's float pruning becomes
    # unsound and these tests must go red rather than the engine going quiet.
    nan_ordinal = FLOAT64.ordinalize(float("nan"))
    assert nan_ordinal > FLOAT64.ordinalize(float("inf"))
    assert nan_ordinal > FLOAT64.ordinalize(1.7976931348623157e308)


@pytest.mark.parametrize("op", UNSOUND)
def test_ordinal_float_bounds_still_prune(op):
    # Ordinal bounds DO cover a NaN, so there is nothing to stand down from.
    lower, upper, literal = _case(op, ordinalize=FLOAT64.ordinalize)
    manifest = _manifest(FLOAT64, ordinal=True, lower=lower, upper=upper)

    manifest.prune_files([_comparison(op, literal)])

    assert len(manifest.files) == 0, (
        f"{op} stopped pruning ordinal float bounds — those bounds rank NaN "
        f"highest and are a real bound"
    )
