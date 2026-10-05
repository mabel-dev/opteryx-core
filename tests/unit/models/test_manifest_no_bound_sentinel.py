# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Manifest pruning must treat INT64_MIN bounds as "no bound", not as a real one.

INT64_MIN is the codebase-wide "this producer computed no bound for this
column" sentinel smuggled through as a plain int rather than None
(`RelationStatistics.update_lower`/`update_upper` already reject exactly this
value). The catalog's manifest builder emits it for every column whose
category falls outside its compressible-categories set - which today includes
EVERY unsigned width, because its logical-type table maps no "uintN" name.

Read as a real bound, `col = <anything>` evaluates the Eq prune handler as
`v < -2**63 or v > -2**63` -> True, so EVERY file is dropped and the query
returns zero rows. That is a silent wrong answer, not a missed optimisation,
and it fires for a plain UINT32 column just as it does for IPV4 (whose
physical type IS uint32).

The guard is value-exact - NOT "any negative". A signed column's genuine
ordinal key is routinely negative and pruning on those is correct; the
negative-bound tests below pin that down so the guard can never be widened
into one that silently disables pruning for ordinary signed data.

In the ordinal dialect a producer records the sentinel as NO bound at all (the
native builder is never handed it - see tests/manifests.py and the native
manifest decoder); in the real-value dialect it arrives as a plain INT64_MIN
value and the native pruner's own guard must disqualify it.
"""

from __future__ import annotations

import os
import sys
from opteryx.compiled.structures.expressions import Between
from opteryx.compiled.structures.expressions import Comparison
from opteryx.compiled.structures.expressions import Literal

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.expression import NodeType
from opteryx.types.logical_type import INT64, IPV4, UINT32
from opteryx.types.schema import RelationSchema
from opteryx.compiled.structures.expressions import LogicalColumn
from opteryx.planner.plan_context import PlanContext
from tests.manifests import FileSpec
from tests.manifests import build_manifest

# The sentinel itself. Spelled out rather than imported so a change to the
# constant (tests/manifests.py's NULL_FLAG, native_manifest.hpp's kNoBound) has
# to be a deliberate, visible decision here too.
NO_BOUND = -(1 << 63)

# 10.0.0.1 and 203.0.113.42 as uint32 - the CTAS repro's real values. The top
# of the range exceeds INT32_MAX, which is exactly where an unsigned column's
# statistics historically went wrong.
IP_LOW = 167772161
IP_HIGH = 3405774848


def _schema(plan_context, column_type, name="value"):
    return RelationSchema(
        name="t",
        columns=[
            plan_context.columns.relation_column(
                "t", name, column_type=column_type)
        ],
    )


def _file(lower, upper, path="f1", record_count=10):
    return FileSpec(
        file_path=path,
        record_count=record_count,
        lower_bounds={0: lower},
        upper_bounds={0: upper},
    )


def _manifest(plan_context, column_type, files, *, ordinal):
    return build_manifest(_schema(plan_context, column_type), files, bounds_are_ordinal=ordinal)


def _literal(plan_context, value, column_type):
    return Literal(value=value, type=column_type, arena=plan_context.expressions)


def _column(plan_context, column_name):
    return LogicalColumn(
        node_type=NodeType.IDENTIFIER, source_column=column_name, arena=plan_context.expressions
    )


def _comparison(plan_context, op, value, column_type, column_name="value"):
    return Comparison(
        value=op,
        left=_column(plan_context, column_name),
        right=_literal(plan_context, value, column_type),
        arena=plan_context.expressions,
    )


def _between(plan_context, lower, upper, column_type, column_name="value"):
    return Between(
        left=_column(plan_context, column_name),
        right=_literal(plan_context, lower, column_type),
        centre=_literal(plan_context, upper, column_type),
        arena=plan_context.expressions,
    )


# ---------------------------------------------------------------------------
# prune_files: a sentinel bound is no evidence, so the file must be kept.
# ---------------------------------------------------------------------------


def test_sentinel_bounds_keep_file_for_every_comparison_operator():
    plan_context = PlanContext()
    # Eq is the one that returned zero rows in production, but every handler
    # dereferences the same bounds - none of them may act on the sentinel.
    for op, literal in (
        ("Eq", IP_LOW),
        ("NotEq", IP_LOW),
        ("Gt", IP_LOW),
        ("GtEq", IP_LOW),
        ("Lt", IP_LOW),
        ("LtEq", IP_LOW),
    ):
        manifest = _manifest(plan_context, UINT32, [_file(NO_BOUND, NO_BOUND)], ordinal=True)
        manifest = manifest.prune_files(
            [_comparison(plan_context, op, literal, UINT32)], plan_context=plan_context
        )
        assert manifest.get_file_count() == 1, f"{op} pruned a file on a no-bound sentinel"


def test_sentinel_bounds_keep_file_for_ipv4_column():
    plan_context = PlanContext()
    # IPV4 is physically uint32, so it lands in the identical catalog gap.
    manifest = _manifest(plan_context, IPV4, [_file(NO_BOUND, NO_BOUND)], ordinal=True)

    manifest = manifest.prune_files(
        [_comparison(plan_context, "Eq", IP_LOW, IPV4)], plan_context=plan_context
    )

    assert manifest.get_file_count() == 1


def test_sentinel_bounds_keep_file_for_between():
    plan_context = PlanContext()
    manifest = _manifest(plan_context, UINT32, [_file(NO_BOUND, NO_BOUND)], ordinal=True)

    manifest = manifest.prune_files(
        [_between(plan_context, 1, 10, UINT32)], plan_context=plan_context
    )

    assert manifest.get_file_count() == 1


def test_one_sentinel_bound_is_enough_to_disqualify_the_pair():
    plan_context = PlanContext()
    # A producer that computed one end but not the other still has no usable
    # range - half a bound must not be pruned on.
    for lower, upper in ((NO_BOUND, IP_HIGH), (IP_LOW, NO_BOUND)):
        manifest = _manifest(plan_context, UINT32, [_file(lower, upper)], ordinal=True)
        manifest = manifest.prune_files(
            [_comparison(plan_context, "Eq", 999999, UINT32)], plan_context=plan_context
        )
        assert manifest.get_file_count() == 1


def test_sentinel_file_kept_while_real_bounded_file_still_prunes():
    plan_context = PlanContext()
    # The guard must not disarm pruning for files that DO carry statistics.
    manifest = _manifest(
        plan_context,
        UINT32,
        [
            _file(NO_BOUND, NO_BOUND, path="no_stats"),
            _file(IP_LOW, IP_LOW + 5, path="has_stats"),
        ],
        ordinal=True,
    )

    manifest = manifest.prune_files(
        [_comparison(plan_context, "Eq", IP_HIGH, UINT32)], plan_context=plan_context
    )

    assert manifest.get_file_paths() == ["no_stats"]


# ---------------------------------------------------------------------------
# The guard is the exact value, not "negative". Ordinary signed data whose
# bounds are genuinely negative must still prune.
# ---------------------------------------------------------------------------


def test_negative_but_real_bounds_still_prune():
    plan_context = PlanContext()
    manifest = _manifest(
        plan_context,
        INT64,
        [_file(INT64.ordinalize(-100), INT64.ordinalize(-50))],
        ordinal=True,
    )

    manifest = manifest.prune_files(
        [_comparison(plan_context, "Gt", 0, INT64)], plan_context=plan_context
    )

    assert manifest.get_file_count() == 0


def test_int64_min_plus_one_is_a_real_bound_and_still_prunes():
    plan_context = PlanContext()
    # The nearest value to the sentinel that is NOT the sentinel - pins the
    # boundary so the guard can't drift into a range check.
    manifest = _manifest(
        plan_context, INT64, [_file(NO_BOUND + 1, NO_BOUND + 10)], ordinal=True
    )

    manifest = manifest.prune_files(
        [_comparison(plan_context, "Gt", 0, INT64)], plan_context=plan_context
    )

    assert manifest.get_file_count() == 0


# ---------------------------------------------------------------------------
# prune_files_for_topn: its docstring already promises that files with no
# bound are kept AND excluded from the ranking. The sentinel is that case.
# These manifests carry REAL-value bounds, so the sentinel reaches the native
# side as a plain INT64_MIN value and the native guard itself is exercised.
# ---------------------------------------------------------------------------


def test_topn_keeps_sentinel_file_and_still_prunes_the_others():
    plan_context = PlanContext()
    # keep: 10 rows at 900..1000 satisfies LIMIT 5 on its own, so `low` is
    # provably outside the top-5 and must go. `no_stats` carries no evidence
    # either way and must survive.
    manifest = _manifest(
        plan_context,
        INT64,
        [
            _file(900, 1000, path="high", record_count=10),
            _file(0, 100, path="low", record_count=10),
            _file(NO_BOUND, NO_BOUND, path="no_stats", record_count=10),
        ],
        ordinal=False,
    )

    manifest = manifest.prune_files_for_topn("value", descending=True, limit=5)

    assert sorted(manifest.get_file_paths()) == ["high", "no_stats"]


def test_topn_ascending_sentinel_does_not_delete_every_real_file():
    plan_context = PlanContext()
    # The worst case, and the reason this guard belongs in topn too: ascending,
    # a sentinel file sorts FIRST (lo == INT64_MIN), so it is the first file
    # accumulated and its own INT64_MIN `hi` becomes the threshold. Every real
    # file then has lo > threshold and ALL of them are dropped - measured
    # pre-fix, the 3-file manifest below came back holding only `no_stats`.
    manifest = _manifest(
        plan_context,
        INT64,
        [
            _file(0, 100, path="low", record_count=10),
            _file(900, 1000, path="high", record_count=10),
            _file(NO_BOUND, NO_BOUND, path="no_stats", record_count=10),
        ],
        ordinal=False,
    )

    manifest = manifest.prune_files_for_topn("value", descending=False, limit=5)

    assert "low" in manifest.get_file_paths()


def test_topn_ascending_keeps_sentinel_file():
    plan_context = PlanContext()
    manifest = _manifest(
        plan_context,
        INT64,
        [
            _file(0, 100, path="low", record_count=10),
            _file(900, 1000, path="high", record_count=10),
            _file(NO_BOUND, NO_BOUND, path="no_stats", record_count=10),
        ],
        ordinal=False,
    )

    manifest = manifest.prune_files_for_topn("value", descending=False, limit=5)

    assert sorted(manifest.get_file_paths()) == ["low", "no_stats"]


def test_topn_vector_rows_stay_aligned_when_a_sentinel_file_survives():
    plan_context = PlanContext()
    # Each file indexes the native sketch vectors by its ORIGINAL file position
    # (its vector row); a kept sentinel file must not shift that mapping.
    manifest = _manifest(
        plan_context,
        INT64,
        [
            _file(0, 100, path="low", record_count=10),
            _file(NO_BOUND, NO_BOUND, path="no_stats", record_count=10),
            _file(900, 1000, path="high", record_count=10),
        ],
        ordinal=False,
    )

    manifest = manifest.prune_files_for_topn("value", descending=True, limit=5)

    assert manifest.get_file_paths() == ["no_stats", "high"]
    assert [manifest.native.file_row(row)["vector_row"] for row in range(2)] == [1, 2]
