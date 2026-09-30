# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Manifest - a relation's file set and its statistics, for the planner.

The Manifest is attached to each READ/Scan node during binding: the optimizer
prunes files and costs plans with it, and execution follows those decisions
deterministically.

It is a FAÇADE (native plan graph Q8, architect rulings 2026-09-27): the files
and every statistic about them are native rows - a NativeManifest
(src/cpp/planner/native_manifest.hpp), keyed by each column's LOAD-TIME
position - and every estimate, every prune and every answer read off the
statistics runs natively over them (manifest_estimates.hpp,
predicate_bounds.hpp, manifest_prune.hpp). What lives here is the relation's
LIVE schema (projection pushdown prunes it; statistics stay keyed by load-time
position, which is why a column is always found by NAME) and the translation
from a column name to that position.

Copy-on-write: a Manifest attached to a plan node is immutable for the life of
the plan - the optimizer's scan-statistics cache keys its memoised base
statistics by the manifest's identity, so an in-place change would re-serve
pre-pruning numbers. Every narrowing (`prune_files`, `prune_files_for_topn`,
`subset`) returns a NEW Manifest; `self` is never modified.
"""

from typing import Any, Dict, List, Optional, Tuple

from opteryx.types.schema import RelationSchema

class Manifest:
    """A relation's files and statistics: the native rows and the live schema.

    `native` is the NativeManifest a producer built (footers, a decoded
    manifest parquet, a catalog's rows); `schema` the relation's schema as
    bound. `stats_are_authoritative` and `bounds_are_ordinal` travel with the
    rows (see NativeManifestBuilder):

      * stats_are_authoritative - written by the commit that produced the data,
        so they cannot disagree with it. Only then may a consumer treat them as
        LAW - prune a file away, answer COUNT/MIN/MAX without reading, or
        eliminate a LIMIT - because each of those returns a WRONG ANSWER on a
        stale number rather than a slow one. Hint statistics remain fully usable
        for ESTIMATION.
      * bounds_are_ordinal - the bounds are `Vector.ordinalize()` keys
        (draken/ops/ordinalize.h), not decoded values; predicate literals are
        ordinalized before they are compared. One dialect per manifest.
    """

    __slots__ = ("native", "schema")

    def __init__(self, native, schema: RelationSchema):
        self.native = native
        self.schema = schema

    # ================================================================
    # The rows
    # ================================================================

    @property
    def stats_are_authoritative(self) -> bool:
        return self.native.stats_are_authoritative

    @property
    def bounds_are_ordinal(self) -> bool:
        return self.native.bounds_are_ordinal

    def get_record_count(self) -> Optional[int]:
        """LIVE rows across every file (physical minus merge-on-read deletes),
        or None when any file's count is UNKNOWN - a partial sum reported as a
        total is a wrong answer (COUNT(*) is answered from it, LIMIT nodes are
        deleted against it). Test `is not None`: an empty relation is 0."""
        return self.native.record_count()

    def has_deletes(self) -> bool:
        """True when any file carries merge-on-read delete debt: bounds and null
        counts then describe a SUPERSET of the live rows - valid for pruning,
        wrong as query answers."""
        return self.native.has_deletes()

    def get_file_count(self) -> int:
        return len(self.native)

    def get_row_group_count(self) -> Optional[int]:
        """Row groups across every file, or None when any file's is unknown."""
        return self.native.row_group_count()

    def get_total_size(self) -> int:
        return self.native.total_size()

    def get_file_paths(self) -> List[str]:
        return self.native.file_paths()

    def file_formats(self) -> List[str]:
        return self.native.file_formats()

    def file_sizes(self) -> List[int]:
        """Each file's size in bytes, in file order; 0 where the producer had none."""
        return self.native.file_sizes()

    def record_counts(self) -> List[Optional[int]]:
        """Each file's PHYSICAL row count, in file order; None where unknown."""
        return self.native.record_counts()

    def uncompressed_sizes(self) -> List[Optional[int]]:
        return self.native.uncompressed_sizes()

    def deleted_record_counts(self) -> List[int]:
        return self.native.deleted_record_counts()

    def delete_positions(self) -> Dict[str, tuple]:
        """{path: deleted row ordinals} for every file with merge-on-read deletes;
        raises for a file whose deletes were never resolved."""
        return self.native.delete_positions()

    def subset(self, positions: List[int]) -> "Manifest":
        """A new Manifest over the files at `positions` (indexes into this one,
        in the order given). See the module docstring on copy-on-write."""
        return Manifest(self.native.subset(list(positions)), self.schema)

    # ================================================================
    # Columns
    # ================================================================

    def position_of(self, column) -> Optional[int]:
        """The column's LOAD-TIME position - the key its statistics are stored
        under - or None when the manifest has no such column. By NAME: the live
        schema may have been projected down since the manifest was built."""
        name = column.decode("utf-8") if type(column) is bytes else column
        return self.native.position_of(name)

    def _live_types(self) -> Dict[str, int]:
        return {
            col.name: col.column_type.type_id
            for col in self.schema.columns
            if col.column_type is not None
        }

    # ================================================================
    # File pruning (the optimizer's)
    # ================================================================

    def _predicate_ids(self, predicates: List, plan_context) -> List[int]:
        """The predicates' expression ids - which must be rows of THIS query's
        arena: the native derivation reads them there."""
        ids = []
        for predicate in predicates:
            if predicate is None:
                continue
            if predicate.arena is not plan_context.expressions:
                raise ValueError(
                    "a predicate from another query's expression arena reached file pruning"
                )
            ids.append(predicate.expr_id)
        return ids

    def prune_files(self, predicates: List, *, plan_context) -> "Manifest":
        """The files `predicates` (a conjunction) cannot rule out: bounds,
        membership sketches, null counts and case-fold identity, each sound in
        one direction only - a term that cannot be decided keeps the file.
        Returns `self` when nothing was pruned."""
        rows = self.native.prune_files(
            plan_context.expressions,
            plan_context.columns,
            self._predicate_ids(predicates, plan_context),
            self._live_types(),
        )
        if len(rows) == len(self.native):
            return self
        return self.subset(rows)

    def prune_files_for_topn(self, column_name: str, descending: bool, limit: int) -> "Manifest":
        """The files that can hold a top-`limit` row of `column_name` for
        ``ORDER BY column_name [ASC|DESC] LIMIT limit``. SAFE ONLY when the
        column has no NULLs across the manifest - the caller checks
        `get_total_null_count(column_name) == 0` first. Returns `self` when
        nothing was pruned."""
        if limit is None:
            return self
        rows = self.native.prune_files_for_topn(column_name, descending, limit, self._live_types())
        if len(rows) == len(self.native):
            return self
        return self.subset(rows)

    # Op codes for `ordinal_zone_map_terms`, mirrored in
    # src/cpp/engine/native_skene_scan_source.hpp's SkeneZoneTerm.
    ZONE_OP_EQ = 0
    ZONE_OP_GT = 1
    ZONE_OP_GTEQ = 2
    ZONE_OP_LT = 3
    ZONE_OP_LTEQ = 4

    def ordinal_zone_map_terms(self, predicates: List, *, plan_context) -> List[tuple]:
        """`(column_name, op_code, ordinal)` terms a ROW-GROUP zone map can be
        tested against - a conjunction; [] when the bounds are not ordinal."""
        return self.native.zone_map_terms(
            plan_context.expressions,
            plan_context.columns,
            self._predicate_ids(predicates, plan_context),
            self._live_types(),
        )

    # ================================================================
    # Answers read straight off the statistics
    # ================================================================

    def min_max(self, column: str) -> Tuple[Any, Any]:
        """(MIN, MAX) of the column across every file, as values - the
        statistics-only MIN/MAX answer, whose type gate admits only columns
        whose bound IS the value. (None, None) when nothing bounds it."""
        position = self.position_of(column)
        if position is None:
            return None, None
        return self.native.extremes(position)

    def file_key_ranges(self, column: str) -> List[Tuple[int, Any, Any]]:
        """Per-file ``(position, min, max)`` on one column, for the files whose
        manifest bounds it - compaction planning's overlap reasoning. Files with
        no usable bound are omitted rather than given a fabricated range."""
        position = self.position_of(column)
        if position is None:
            return []
        return self.native.key_ranges(position)

    def file_value_bounds(self, column: str) -> Optional[List[Tuple[Any, Any]]]:
        """Per-file ``(min, max)`` of one column as tagged VALUES (``("int", v)`` /
        ``("bytes", b)``), in file order, from the file's own bounds else its footer's.

        ``None`` - not a partial list - when the column is unknown or ANY file lacks a
        bound that is a value (ordinal-only bounds are an encoding, not a value): a caller
        reasoning about the whole file set must not reason about a subset of it."""
        position = self.position_of(column)
        if position is None:
            return None
        bounds = []
        for row in range(self.get_file_count()):
            cell = self.native.cell(row, position)
            source = cell["bounds"]
            if source["min"] is None or source["max"] is None:
                footer = cell["footer"]
                if footer is None:
                    return None
                source = footer["bounds"]
            if source["min"] is None or source["max"] is None:
                return None
            bounds.append((source["min"], source["max"]))
        return bounds

    def show_morsel(self):
        """SHOW MANIFEST's rows (bounds rendered as text)."""
        return self.native.show_morsel()

    # ================================================================
    # Estimates (for cost-based optimization)
    # ================================================================

    def estimate_cardinality(self, column) -> Optional[int]:
        """Distinct values: a provably EXACT footer count, else the KMV union of
        the sketches (exact below K). The execution-variant strategies
        (distinct_pushdown, hash_map_variant) rely on its near-exact semantics."""
        position = self.position_of(column)
        return None if position is None else self.native.estimate_cardinality(position)

    def get_total_null_count(self, column) -> Optional[int]:
        """Total nulls, or None when any file's count is unknown (a partial
        total would overcount non-null values)."""
        position = self.position_of(column)
        return None if position is None else self.native.total_null_count(position)

    def get_total_sum(self, column) -> Optional[int]:
        """The column's EXACT sum of non-null values over every file (a Python
        int), or None when any file's sum is unknown, any file has deletes, or
        the total overflows int128 - never a partial sum."""
        position = self.position_of(column)
        return None if position is None else self.native.total_sum(position)

    def estimate_selectivity(self, predicate) -> float:
        """Estimated fraction of rows matching `predicate`, from this manifest's
        own statistics (the native estimator, src/cpp/planner/selectivity.hpp)."""
        from opteryx.compiled.planner.statistics import manifest_selectivity

        return manifest_selectivity(self, predicate)
