# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Manifest pruning must not compare temporal values across raw integer domains.

File bounds hold a temporal column's RAW physical integer - DATE32 stores days
since the epoch, TIMESTAMP64 stores unit-scaled ticks - and a predicate literal
arrives as its own raw integer. Compare the two directly and ~20_000 days sits
beside ~1.7e15 microseconds. `_comparable_literal` cannot see it: both sides are
plain ints, so it waves them through.

The consequence is not a missed optimisation, it is zero rows with HTTP 200 and
no error. Measured against a 1657-row DATE column with data through 2026-08-03:

    date_added ge 2025-01-01                -> 418   (correct)
    date_added ge 2025-01-01T00:00:00Z      -> 0     (every file pruned)

The asymmetry is the fingerprint. `max_ < v` holds for every file when the
literal is microseconds, so Gt/GtEq/Eq drop the whole table while Lt/LtEq prune
nothing and answer correctly. Reverse the pairing - a TIMESTAMP column against a
date-only literal - and the failure reverses with it: `min_ >= v` holds
everywhere, so Lt/LtEq return nothing instead. Both directions are covered here.

The guard is types-only, so it is settled once per predicate. It must stay
narrow: same-domain temporal pruning, and every non-temporal pairing, has to
keep pruning exactly as before, which the negative cases at the bottom pin down.
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
from draken.draken_native import TimestampUnit
from opteryx.types.logical_type import DATE, INT64, TIME, TIMESTAMP, VARCHAR
from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity

US_PER_DAY = 86_400_000_000

# 2025-01-01 and 2026-08-03 in each domain. The bounds below deliberately
# straddle the literal so a correct comparison keeps the file and an
# incorrect one drops it - a file that would be kept either way proves nothing.
DAY_2025_01_01 = 20089
DAY_2026_08_03 = 20668
US_2025_01_01 = DAY_2025_01_01 * US_PER_DAY
US_2026_08_03 = DAY_2026_08_03 * US_PER_DAY

ALL_OPS = ("Eq", "NotEq", "Gt", "GtEq", "Lt", "LtEq")


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


def _comparison(op, value, literal_type=None, column_name="value"):
    return Node(
        NodeType.COMPARISON_OPERATOR,
        value=op,
        left=Node(NodeType.IDENTIFIER, source_column=column_name),
        right=Node(NodeType.LITERAL, type=literal_type, value=value),
    )


def _between(lower, upper, literal_type=None, column_name="value"):
    return Node(
        NodeType.BETWEEN,
        left=Node(NodeType.IDENTIFIER, source_column=column_name),
        right=Node(NodeType.LITERAL, type=literal_type, value=lower),
        centre=Node(NodeType.LITERAL, type=literal_type, value=upper),
    )


def _prune(column_type, bounds, predicate, bounds_are_ordinal=False):
    manifest = Manifest(
        files=[_file(*bounds)],
        schema=_schema(column_type),
        bounds_are_ordinal=bounds_are_ordinal,
    )
    manifest.prune_files([predicate])
    return manifest.files


# The catalog path (`OpteryxConnector`, which is what the hosted service runs)
# builds its Manifest with `bounds_are_ordinal=True`, and `ColumnType.ordinalize`
# is the identity for DATE32/TIMESTAMP64 - the ordinal key IS the raw tick. So
# the domain mismatch survives ordinalisation unchanged, and both bound
# encodings have to be covered or the tests miss the shape production actually
# runs. `BOUND_ENCODINGS` parametrises the cases where that distinction bites.
BOUND_ENCODINGS = (False, True)


# ---------------------------------------------------------------------------
# DATE column (days) vs TIMESTAMP literal (microseconds) - the reported bug.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bounds_are_ordinal", BOUND_ENCODINGS)
@pytest.mark.parametrize("op", ALL_OPS)
def test_date_column_with_timestamp_literal_keeps_the_file_for_every_operator(
    op, bounds_are_ordinal
):
    # The file spans 2025-01-01..2026-08-03, so it holds rows on both sides of
    # the literal and NO operator may drop it.
    files = _prune(
        DATE,
        (DAY_2025_01_01, DAY_2026_08_03),
        _comparison(op, US_2025_01_01, literal_type=TIMESTAMP()),
        bounds_are_ordinal=bounds_are_ordinal,
    )

    assert len(files) == 1, f"{op} pruned a file comparing DATE days against TIMESTAMP us"


def test_date_column_with_timestamp_literal_is_the_ge_zero_rows_repro():
    # The exact production shape: `date_added ge 2025-01-01T00:00:00Z`. Pre-fix
    # the GtEq handler evaluated `max_ < v` as `20668 < 1735689600000000` -> True
    # for every file in the table, and the response was `"value": []`.
    files = _prune(
        DATE,
        (DAY_2025_01_01, DAY_2026_08_03),
        _comparison("GtEq", US_2025_01_01, literal_type=TIMESTAMP()),
    )

    assert len(files) == 1


def test_date_column_with_timestamp_literal_between_keeps_the_file():
    files = _prune(
        DATE,
        (DAY_2025_01_01, DAY_2026_08_03),
        _between(US_2025_01_01, US_2026_08_03, literal_type=TIMESTAMP()),
    )

    assert len(files) == 1


# ---------------------------------------------------------------------------
# The reverse pairing: TIMESTAMP column (microseconds) vs DATE literal (days).
# Gt/GtEq happen to be harmless here, which is why testing one direction with
# one operator - as the original bug report did - reads as "control case, fine".
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bounds_are_ordinal", BOUND_ENCODINGS)
@pytest.mark.parametrize("op", ALL_OPS)
def test_timestamp_column_with_date_literal_keeps_the_file_for_every_operator(
    op, bounds_are_ordinal
):
    files = _prune(
        TIMESTAMP(),
        (US_2025_01_01, US_2026_08_03),
        _comparison(op, DAY_2025_01_01, literal_type=DATE),
        bounds_are_ordinal=bounds_are_ordinal,
    )

    assert len(files) == 1, f"{op} pruned a file comparing TIMESTAMP us against DATE days"


def test_timestamp_column_with_date_literal_is_the_lt_zero_rows_repro():
    # `published_at lt 2026-07-01` returned 0 in production while
    # `published_at lt 2026-07-01T00:00:00Z` returned 32841: the Lt handler
    # evaluated `min_ >= v` as `1735689600000000 >= 20089` -> True everywhere.
    files = _prune(
        TIMESTAMP(),
        (US_2025_01_01, US_2026_08_03),
        _comparison("Lt", DAY_2026_08_03, literal_type=DATE),
    )

    assert len(files) == 1


# ---------------------------------------------------------------------------
# Same physical type, different unit - TIMESTAMP64[s] bounds against a
# TIMESTAMP64[us] literal is the same 1e6 scale error with no type change to
# make it visible.
# ---------------------------------------------------------------------------


def test_timestamp_columns_with_mismatched_units_keep_the_file():
    seconds = TIMESTAMP(unit=TimestampUnit.SECONDS)
    files = _prune(
        seconds,
        (US_2025_01_01 // 1_000_000, US_2026_08_03 // 1_000_000),
        _comparison("GtEq", US_2025_01_01, literal_type=TIMESTAMP()),
    )

    assert len(files) == 1


def test_time_column_against_timestamp_literal_keeps_the_file():
    # TIME ticks are time-of-day, so they are never comparable with an
    # epoch-relative TIMESTAMP tick no matter how the units line up.
    files = _prune(
        TIME(),
        (0, 23 * 3600 * 1_000_000),
        _comparison("GtEq", US_2025_01_01, literal_type=TIMESTAMP()),
    )

    assert len(files) == 1


# ---------------------------------------------------------------------------
# Negative cases. The guard is scoped to cross-domain temporal pairs; widening
# it would silently disable pruning for everything else.
# ---------------------------------------------------------------------------


def test_date_column_with_date_literal_still_prunes():
    # Same domain, so the comparison is real and the file provably holds nothing
    # at or after 2026-08-04.
    files = _prune(
        DATE,
        (DAY_2025_01_01, DAY_2026_08_03),
        _comparison("Gt", DAY_2026_08_03, literal_type=DATE),
    )

    assert files == []


def test_timestamp_column_with_timestamp_literal_still_prunes():
    files = _prune(
        TIMESTAMP(),
        (US_2025_01_01, US_2026_08_03),
        _comparison("Gt", US_2026_08_03, literal_type=TIMESTAMP()),
    )

    assert files == []


def test_date_column_with_date_literal_between_still_prunes():
    files = _prune(
        DATE,
        (DAY_2025_01_01, DAY_2026_08_03),
        _between(DAY_2026_08_03 + 1, DAY_2026_08_03 + 10, literal_type=DATE),
    )

    assert files == []


def test_non_temporal_columns_still_prune():
    files = _prune(INT64, (0, 100), _comparison("Gt", 500, literal_type=INT64))

    assert files == []


def test_untyped_literal_keeps_pruning():
    # Producers that don't stamp a type on the literal node predate this guard;
    # they must keep the pruning they have rather than silently lose it.
    files = _prune(INT64, (0, 100), _comparison("Gt", 500))

    assert files == []


def test_temporal_column_against_non_temporal_literal_is_not_this_guards_business():
    # `date_col >= 100` is a type error the binder rejects before pruning runs.
    # Answering "mismatch" here would be harmless but wrong-headed; the point is
    # that the guard only fires when BOTH sides are temporal.
    from opteryx.models.manifest import _temporal_domain_mismatch

    assert _temporal_domain_mismatch(DATE, INT64) is False
    assert _temporal_domain_mismatch(DATE, VARCHAR) is False
    assert _temporal_domain_mismatch(INT64, TIMESTAMP()) is False


def test_mixed_predicates_drop_only_the_unsafe_one():
    # A query carrying both a safe and an unsafe predicate must keep pruning on
    # the safe one - the guard drops predicates, not pruning.
    manifest = Manifest(
        files=[
            _file(DAY_2025_01_01, DAY_2026_08_03, path="in_range"),
            _file(DAY_2025_01_01 - 100, DAY_2025_01_01 - 50, path="out_of_range"),
        ],
        schema=_schema(DATE),
    )

    manifest.prune_files(
        [
            _comparison("GtEq", US_2025_01_01, literal_type=TIMESTAMP()),  # unsafe, ignored
            _comparison("GtEq", DAY_2025_01_01, literal_type=DATE),  # safe, prunes
        ]
    )

    assert [f.file_path for f in manifest.files] == ["in_range"]
