# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Skene IO — schema glue between the filesystem connector and libskene.

Unlike parquet (_rugo_schema.py's lossy string-typed mapping) and JSONL
(sampled inference), a skene footer carries the exact DrakenType and
LogicalType descriptor each column was written with — the conversion here is
an identity reconstruction, not a translation. IPv4 stays IPV4, DECIMAL keeps
its precision/scale, TIMESTAMP keeps its unit.
"""

from typing import Any, Dict

from draken.draken_native import DrakenType
from draken.draken_native import LogicalKind
from draken.draken_native import LogicalType
from draken.draken_native import TimestampUnit

from opteryx.types.logical_type import ColumnType
from opteryx.types.schema import RelationSchema
from opteryx.types.schema import SchemaColumn
from opteryx.types.schema import mint_column_identity

__all__ = [
    "skene_aggregate_row_group_statistics",
    "skene_column_type",
    "skene_metadata_to_schema",
    "skene_statistics_positions",
]

# skene format.h StatFlag bits.
KSTAT_MIN_MAX = 0x3  # kStatMin | kStatMax
KSTAT_NULL_COUNT = 0x4  # kStatNullCount


def skene_statistics_positions(columns, position_by_name: Dict[str, int]) -> list:
    """Map each per-row-group statistics slot to a schema position, or None.

    A row group's `column_statistics` list is DEPTH FIRST over the schema and
    includes ARRAY children, so slot i stops being column i the moment any
    column has one. Resolving by position rather than by this walk would land a
    child's bounds on whichever top-level column happened to follow it.

    Children map to None: manifest bounds are keyed by top-level schema position,
    and an element's min/max is not the array's.
    """
    positions: list = []

    def walk(column: Dict[str, Any], top_level: bool) -> None:
        positions.append(position_by_name.get(column["name"]) if top_level else None)
        for child in column.get("children") or ():
            walk(child, False)

    for column in columns:
        walk(column, True)
    return positions


def skene_aggregate_row_group_statistics(row_groups, positions) -> tuple:
    """Aggregate a skene file's PER-ROW-GROUP statistics blobs to FILE level.

    Statistics in a .skene footer describe a row group, not the file, and a file
    holds up to 16 of them. Returns
    ``(lower_bounds, upper_bounds, null_counts, distinct_counts)``, each keyed by
    the schema position `positions[slot]` names (slots mapping to None — ARRAY
    children — are skipped). A column absent from a dict is NOT TRACKED for this
    file; absence is never zero.

    Three INDEPENDENT aggregations over one walk, each with its own "unknown"
    state: a row group that bounds nothing still carries a usable null count, so
    one missing statistic must not discard the other two.

    **bounds** — UNION. A file-level bound is necessarily wider than any one row
    group's; that coarsening is expected. A column is bounded only when EVERY row
    group bounds it, since a union over a subset would exclude rows the file
    actually holds.

    **null_count** — SUM. A row belongs to exactly one row group, so the file
    total is the sum, and EVERY row group must carry it: a partial sum understates
    nulls, and `Manifest.get_total_null_count`'s caller (TopN manifest pruning)
    reads a total of 0 as "provably no nulls".

    **NDV** — from the SKETCH when every row group carries one, and only from
    the scalars when they do not.

    The sketch path is exact arithmetic: the union of KMV sketches is the K
    smallest of their combined hashes, so overlap between row groups is measured
    rather than guessed, and a column with fewer than K distinct values comes
    back EXACT. This is the whole reason skene stores the hashes.

    The scalar path is the fallback for files written before sketches existed,
    and it is a guess: distinct counts do NOT sum (two row groups can hold the
    same value), so it mirrors the parquet rule
    (``rugo/src/parquet/metadata.cpp``, AggregateColumnStats) — SUM when this row
    group's range is provably DISJOINT from the range already merged, MAX
    otherwise (the safe floor), UNKNOWN if any row group lacks the statistic.
    ⚠️ Measured on TPC-H `l_comment`, that rule lands 17.6x under the truth: all
    23 row groups share an identical min ordinal, so the disjointness test never
    fires, while their value sets are 91% disjoint. Treat it as a compatibility
    shim, not a design.

    Disjointness is judged on ORDINALS, sound in the one direction that matters:
    ordinalize is monotonic, so ``ord(a) < ord(b)`` implies ``a < b``. It is not
    injective (string ordinals collide on a shared 8-byte prefix), which can only
    make a genuinely disjoint pair look overlapping — costing a MAX where a SUM
    was available. That understates NDV; it never overstates it.

    Each NDV travels as ``(count, is_exact)`` because the two halves are one
    value: exact means skene's value ordering deduplicated the column and the
    count is a BOUND; not-exact means a KMV sketch estimated it. A MAX over
    overlapping ranges is a floor rather than a count, so it yields an ESTIMATE
    however exact each contributor was.
    """
    from opteryx.utils.kmv import merge_min_k

    lower: Dict[int, int] = {}
    upper: Dict[int, int] = {}
    nulls: Dict[int, int] = {}
    distincts: Dict[int, tuple] = {}
    sketches: Dict[int, list] = {}
    floors: Dict[int, int] = {}

    for slot, position in enumerate(positions):
        if position is None:
            continue
        low = None
        high = None
        bounded = True
        null_total = 0
        nulls_known = True
        ndv_total = None
        ndv_is_exact = True
        ndv_known = True
        ndv_lo = None
        ndv_hi = None
        # Sketch union. One row group without a sketch voids it for the file —
        # a union missing a row group's hashes silently undercounts, and there is
        # no way to tell that apart from a genuinely smaller column.
        rg_sketches = []
        sketch_known = True
        # The largest EXACT per-row-group count. A row group is a subset of the
        # file and the file of the relation, so this is a hard LOWER BOUND on
        # both — and unlike the merged count it survives a MAX step, which is
        # exactly when the K=32 estimator most needs flooring.
        ndv_floor = 0

        for row_group in row_groups:
            statistics = row_group["column_statistics"][slot]
            if statistics is None:
                # Nothing tracked for this column in this row group: every
                # aggregation is unknown for the file.
                bounded = False
                nulls_known = False
                ndv_known = False
                sketch_known = False
                break
            flags = statistics["flags"]
            has_bounds = (flags & KSTAT_MIN_MAX) == KSTAT_MIN_MAX
            rg_low = statistics["min_ordinal"] if has_bounds else None
            rg_high = statistics["max_ordinal"] if has_bounds else None

            if not has_bounds:
                bounded = False
            elif bounded:
                if low is None:
                    low, high = rg_low, rg_high
                else:
                    low = min(low, rg_low)
                    high = max(high, rg_high)

            if nulls_known:
                if flags & KSTAT_NULL_COUNT:
                    null_total += statistics["null_count"]
                else:
                    nulls_known = False

            # None is skene's spelling of NOT TRACKED — the native emitter gates
            # on kStatNdv, so a v1 blob (whose `ndv` bytes were never written)
            # reads as None, never as 0.
            rg_sketch = statistics["sketch"]
            if rg_sketch is None:
                sketch_known = False
            elif sketch_known:
                rg_sketches.append(rg_sketch)

            rg_ndv = statistics["ndv"]
            if rg_ndv is not None and statistics["ndv_exact"]:
                ndv_floor = max(ndv_floor, rg_ndv)
            if rg_ndv is None:
                ndv_known = False
            elif ndv_known:
                if ndv_total is None:
                    ndv_total = rg_ndv
                    ndv_is_exact = statistics["ndv_exact"]
                    ndv_lo, ndv_hi = rg_low, rg_high
                else:
                    disjoint = (
                        rg_low is not None
                        and ndv_lo is not None
                        and (rg_low > ndv_hi or rg_high < ndv_lo)
                    )
                    if disjoint:
                        ndv_total += rg_ndv
                        # A sum of exact counts over disjoint ranges is still
                        # exact; one sketched contributor taints it.
                        ndv_is_exact = ndv_is_exact and statistics["ndv_exact"]
                    else:
                        ndv_total = max(ndv_total, rg_ndv)
                        ndv_is_exact = False
                    if rg_low is None or ndv_lo is None:
                        ndv_lo = ndv_hi = None
                    else:
                        ndv_lo = min(ndv_lo, rg_low)
                        ndv_hi = max(ndv_hi, rg_high)

        if bounded and low is not None:
            lower[position] = low
            upper[position] = high
        if nulls_known:
            nulls[position] = null_total

        if sketch_known and rg_sketches:
            # The sketch is kept as well as the count it implies: a caller
            # merging FILES needs the hashes, and a total cannot be un-merged.
            sketches[position] = merge_min_k(rg_sketches)
        # The scalar merge is kept even when a sketch exists, and NOT because it
        # is a better estimate — it is not. An EXACT count is a hard LOWER BOUND
        # on the relation (a subset cannot hold more distinct values than the
        # whole), and the K=32 estimator's ~18% error can land under it: measured
        # on `l_shipdate`, 2002 estimated against 2526 exact in a single row
        # group. A bound the data proves outranks an estimate that contradicts it.
        if ndv_known and ndv_total is not None:
            distincts[position] = (ndv_total, ndv_is_exact)
        if ndv_floor:
            floors[position] = ndv_floor

    return lower, upper, nulls, distincts, sketches, floors


def skene_column_type(column: Dict[str, Any]) -> ColumnType:
    """Reconstruct a ColumnType from one skene footer column entry
    (skene.read_metadata()'s per-column dict — raw draken enum ints)."""
    physical = DrakenType(column["type"])
    logical_entry = column.get("logical")
    logical = None
    if logical_entry is not None:
        logical = LogicalType(
            kind=LogicalKind(logical_entry["kind"]),
            unit=TimestampUnit(logical_entry["unit"]),
            offset_minutes=logical_entry["offset_minutes"],
            precision=logical_entry["precision"],
            scale=logical_entry["scale"],
            dimension=logical_entry["dimension"],
        )
    element = None
    if physical == DrakenType.ARRAY:
        children = column.get("children") or []
        # A well-formed skene ARRAY column carries exactly one child (the
        # element); a childless one is malformed and read_morsel would reject
        # it, so failing here is early, not different.
        element = skene_column_type(children[0])
    return ColumnType(physical, logical, element)


def skene_metadata_to_schema(metadata: Dict[str, Any], schema_name: str) -> RelationSchema:
    """RelationSchema from skene.read_metadata() output. Exact, not inferred."""
    columns = [
        SchemaColumn(
            name=column["name"],
            column_type=skene_column_type(column),
            identity=mint_column_identity(schema_name, column["name"]),
        )
        for column in metadata["columns"]
    ]
    return RelationSchema(name=schema_name, columns=columns)
