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
"""

from __future__ import annotations

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.models.file_entry import FileEntry
from opteryx.models.manifest import Manifest
from opteryx.types.logical_type import INT64, IPV4, UINT32
from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity

# The sentinel itself. Spelled out rather than imported so a change to the
# constant in manifest.py has to be a deliberate, visible decision here too.
NO_BOUND = -(1 << 63)

# 10.0.0.1 and 203.0.113.42 as uint32 - the CTAS repro's real values. The top
# of the range exceeds INT32_MAX, which is exactly where an unsigned column's
# statistics historically went wrong.
IP_LOW = 167772161
IP_HIGH = 3405774848


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


# ---------------------------------------------------------------------------
# prune_files: a sentinel bound is no evidence, so the file must be kept.
# ---------------------------------------------------------------------------


def test_sentinel_bounds_keep_file_for_every_comparison_operator():
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
        manifest = Manifest(
            files=[_file(NO_BOUND, NO_BOUND)],
            schema=_schema(UINT32),
            bounds_are_ordinal=True,
        )
        manifest.prune_files([_comparison(op, literal)])
        assert len(manifest.files) == 1, f"{op} pruned a file on a no-bound sentinel"


def test_sentinel_bounds_keep_file_for_ipv4_column():
    # IPV4 is physically uint32, so it lands in the identical catalog gap.
    manifest = Manifest(
        files=[_file(NO_BOUND, NO_BOUND)], schema=_schema(IPV4), bounds_are_ordinal=True
    )

    manifest.prune_files([_comparison("Eq", IP_LOW)])

    assert len(manifest.files) == 1


def test_sentinel_bounds_keep_file_for_between():
    manifest = Manifest(
        files=[_file(NO_BOUND, NO_BOUND)], schema=_schema(UINT32), bounds_are_ordinal=True
    )

    manifest.prune_files([_between(1, 10)])

    assert len(manifest.files) == 1


def test_one_sentinel_bound_is_enough_to_disqualify_the_pair():
    # A producer that computed one end but not the other still has no usable
    # range - half a bound must not be pruned on.
    for lower, upper in ((NO_BOUND, IP_HIGH), (IP_LOW, NO_BOUND)):
        manifest = Manifest(
            files=[_file(lower, upper)], schema=_schema(UINT32), bounds_are_ordinal=True
        )
        manifest.prune_files([_comparison("Eq", 999999)])
        assert len(manifest.files) == 1


def test_sentinel_file_kept_while_real_bounded_file_still_prunes():
    # The guard must not disarm pruning for files that DO carry statistics.
    manifest = Manifest(
        files=[
            _file(NO_BOUND, NO_BOUND, path="no_stats"),
            _file(IP_LOW, IP_LOW + 5, path="has_stats"),
        ],
        schema=_schema(UINT32),
        bounds_are_ordinal=True,
    )

    manifest.prune_files([_comparison("Eq", IP_HIGH)])

    assert [f.file_path for f in manifest.files] == ["no_stats"]


# ---------------------------------------------------------------------------
# The guard is the exact value, not "negative". Ordinary signed data whose
# bounds are genuinely negative must still prune.
# ---------------------------------------------------------------------------


def test_negative_but_real_bounds_still_prune():
    manifest = Manifest(
        files=[_file(INT64.ordinalize(-100), INT64.ordinalize(-50))],
        schema=_schema(INT64),
        bounds_are_ordinal=True,
    )

    manifest.prune_files([_comparison("Gt", 0)])

    assert manifest.files == []


def test_int64_min_plus_one_is_a_real_bound_and_still_prunes():
    # The nearest value to the sentinel that is NOT the sentinel - pins the
    # boundary so the guard can't drift into a range check.
    manifest = Manifest(
        files=[_file(NO_BOUND + 1, NO_BOUND + 10)],
        schema=_schema(INT64),
        bounds_are_ordinal=True,
    )

    manifest.prune_files([_comparison("Gt", 0)])

    assert manifest.files == []


# ---------------------------------------------------------------------------
# prune_files_for_topn: its docstring already promises that files with no
# bound are kept AND excluded from the ranking. The sentinel is that case.
# ---------------------------------------------------------------------------


def test_topn_keeps_sentinel_file_and_still_prunes_the_others():
    # keep: 10 rows at 900..1000 satisfies LIMIT 5 on its own, so `low` is
    # provably outside the top-5 and must go. `no_stats` carries no evidence
    # either way and must survive.
    manifest = Manifest(
        files=[
            _file(900, 1000, path="high", record_count=10),
            _file(0, 100, path="low", record_count=10),
            _file(NO_BOUND, NO_BOUND, path="no_stats", record_count=10),
        ],
        schema=_schema(INT64),
    )

    manifest.prune_files_for_topn("value", descending=True, limit=5)

    assert sorted(f.file_path for f in manifest.files) == ["high", "no_stats"]


def test_topn_ascending_sentinel_does_not_delete_every_real_file():
    # The worst case, and the reason this guard belongs in topn too: ascending,
    # a sentinel file sorts FIRST (lo == INT64_MIN), so it is the first file
    # accumulated and its own INT64_MIN `hi` becomes the threshold. Every real
    # file then has lo > threshold and ALL of them are dropped - measured
    # pre-fix, the 3-file manifest below came back holding only `no_stats`.
    manifest = Manifest(
        files=[
            _file(0, 100, path="low", record_count=10),
            _file(900, 1000, path="high", record_count=10),
            _file(NO_BOUND, NO_BOUND, path="no_stats", record_count=10),
        ],
        schema=_schema(INT64),
    )

    manifest.prune_files_for_topn("value", descending=False, limit=5)

    assert "low" in [f.file_path for f in manifest.files]


def test_topn_ascending_keeps_sentinel_file():
    manifest = Manifest(
        files=[
            _file(0, 100, path="low", record_count=10),
            _file(900, 1000, path="high", record_count=10),
            _file(NO_BOUND, NO_BOUND, path="no_stats", record_count=10),
        ],
        schema=_schema(INT64),
    )

    manifest.prune_files_for_topn("value", descending=False, limit=5)

    assert sorted(f.file_path for f in manifest.files) == ["low", "no_stats"]


def test_topn_live_rows_stay_aligned_when_a_sentinel_file_survives():
    # _live_rows indexes the native sketch vectors by ORIGINAL file position;
    # a kept sentinel file must not shift that mapping.
    manifest = Manifest(
        files=[
            _file(0, 100, path="low", record_count=10),
            _file(NO_BOUND, NO_BOUND, path="no_stats", record_count=10),
            _file(900, 1000, path="high", record_count=10),
        ],
        schema=_schema(INT64),
    )

    manifest.prune_files_for_topn("value", descending=True, limit=5)

    assert [f.file_path for f in manifest.files] == ["no_stats", "high"]
    assert manifest._live_rows == [1, 2]
