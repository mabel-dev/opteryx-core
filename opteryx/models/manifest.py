# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Manifest - encapsulates table manifest data and statistics operations.

The Manifest is attached to each READ/Scan node during binding and provides:
- File pruning (called by optimizer)
- Cardinality/selectivity estimation (for cost-based optimization)
- Column statistics (for schema annotation)

One Manifest per READ node. Optimizer uses it to make decisions,
execution follows those decisions deterministically.
"""

from typing import Any, Dict, List, Optional, Tuple

from opteryx.models.file_entry import FileEntry
from opteryx.third_party.maki_nage.distogram import Distogram, merge
from opteryx.types.schema import RelationSchema


def _comparable_literal(literal_value: Any, bound_sample: Any) -> Optional[Any]:
    """Coerce a predicate literal into the representation a stored bound uses.

    File bounds are native decoded values (int/float/datetime/Decimal) or, for
    strings, raw utf-8 bytes (see ``parquet_writer._serialize_bound`` /
    ``manifest_io._decode_bound``) - never a separately-encoded integer. A
    literal must be compared in that SAME representation; returns None when
    the literal can't be safely coerced, so the caller skips pruning instead
    of comparing incompatible types (which is either a wrong prune or a
    TypeError, depending on what the two sides happen to be).
    """
    if isinstance(bound_sample, (bytes, bytearray)):
        if isinstance(literal_value, str):
            return literal_value.encode("utf-8")
        if isinstance(literal_value, (bytes, bytearray)):
            return bytes(literal_value)
        return None

    if isinstance(bound_sample, bool):
        return literal_value if isinstance(literal_value, bool) else None

    if isinstance(bound_sample, (int, float)):
        if isinstance(literal_value, bool):
            return None
        if isinstance(literal_value, (int, float)):
            return literal_value
        return None

    if isinstance(literal_value, type(bound_sample)) or isinstance(bound_sample, type(literal_value)):
        return literal_value

    return None


class Manifest:
    """
    Encapsulates manifest data and statistics operations for a table.

    Created during binding phase from catalog's table.scan() results.
    Provides methods for optimizer to make pruning and costing decisions.
    """

    def __init__(
        self,
        files: List[FileEntry],
        schema: RelationSchema,
        min_k_vector=None,
        histogram_vector=None,
        bounds_are_ordinal: bool = False,
        char_class_vector=None,
    ):
        """
        Initialize Manifest with file entries and schema.

        Args:
            files: List of FileEntry objects from catalog scan
            schema: Table schema (RelationSchema)
            min_k_vector: optional native draken ``array<array<uint64>>`` Vector
                holding every file's min-k sketch (one outer row per file, one
                middle row per column). This is the ONLY representation of the
                sketches — reductions run as native kernels over it. None when the
                relation has no sketches, in which case sketch-derived statistics
                (NDV, membership pruning) are simply unavailable.
            histogram_vector: optional native ``array<array<int64>>`` Vector of
                per-file, per-column histogram bins, used the same way.
            char_class_vector: optional native ``array<array<uint64>>`` Vector of
                per-file, per-column 8-class byte counts (VARCHAR/NVARCHAR/
                VARBINARY columns only), used the same way as min_k_vector/
                histogram_vector — backs the LIKE '%needle%' selectivity
                char-class estimator. None when the relation has no char-class
                stats (nothing ANALYZE'd, or a catalog predating this accessor).
            bounds_are_ordinal: True when every FileEntry in `files` carries
                lower_bounds/upper_bounds encoded as ``Vector.ordinalize()``
                ordinal int64 keys (ANALYZE's native per-file statistics pass —
                see opteryx.models.manifest_io.write_manifest_parquet's
                docstring). `prune_files` ordinalizes predicate literals through
                `ColumnType.ordinalize` before comparing against bounds when
                this is set. False (default) means bounds are real decoded
                values (catalog DataFile bounds, LocalStoreConnector's
                parquet-footer bounds) — the two representations are never
                mixed within one Manifest instance, so this is a single flag
                for the whole object, set explicitly by the producer
                (filesystem_connector.py) rather than inferred.
        """
        self.files = files
        self.schema = schema
        self._min_k_vector = min_k_vector
        self._histogram_vector = histogram_vector
        self._char_class_vector = char_class_vector
        self.bounds_are_ordinal = bounds_are_ordinal
        # The sketch vectors are built once over the FULL file set and are indexed
        # by original file position. prune_files shrinks self.files, so we track the
        # surviving original row indices to keep native reductions aligned with the
        # (pruned) file list — matching the Python paths that iterate self.files.
        # None means "no pruning yet": row i of the vectors == self.files[i].
        self._live_rows: Optional[List[int]] = None
        # Everything positional about a column — sketch slots AND per-file stats
        # field_ids — is positional against the schema as it was at LOAD time, but
        # projection pushdown later prunes `self.schema` down to the referenced
        # columns. Resolving a column's position via the (possibly projected) schema
        # therefore silently reads a DIFFERENT column's data. Snapshot the load-time
        # order once and resolve through it. See _sketch_index and _resolve_field_id.
        self._load_time_columns: Dict[str, int] = {
            col.name: idx for idx, col in enumerate(schema.columns)
        }

        # Lazy-computed mappings
        self._field_id_to_name: Optional[Dict[int, str]] = None
        self._name_to_field_id: Optional[Dict[str, int]] = None
        self._column_bounds_cache: Dict[str, Tuple[Any, Any]] = {}
        self._distogram_cache: Dict[str, Distogram] = {}

    def _sketch_index(self, column_name: str) -> Optional[int]:
        """Positional index of `column_name` in the sketch vectors' column axis.

        Resolved against the load-time schema snapshot, never the live (projected)
        schema — the sketch vectors' middle dimension is fixed at load-time width.
        """
        return self._load_time_columns.get(column_name)

    # ================================================================
    # Basic Aggregates
    # ================================================================

    def get_record_count(self) -> int:
        """Total record count across all files."""
        return sum(f.record_count for f in self.files)

    def get_file_count(self) -> int:
        """Number of files in manifest."""
        return len(self.files)

    def get_total_size(self) -> int:
        """Total size in bytes across all files."""
        return sum(f.file_size_in_bytes for f in self.files)

    # ================================================================
    # File Pruning (called by optimizer)
    # ================================================================

    def _column_type(self, column_name: str):
        """ColumnType for `column_name`, resolved by NAME against the current
        schema — never by position (projection pushdown may prune `self.schema`
        to a subset of columns, same trap `_resolve_field_id` documents). Used
        only to ordinalize predicate literals when `bounds_are_ordinal`; returns
        None when the column isn't in scope (predicate skipped, not guessed).
        """
        for col in self.schema.columns:
            if col.name == column_name:
                return col.column_type
        return None

    def _ordinalize_literal(self, column_name: str, literal_value: Any) -> Optional[Any]:
        """Convert `literal_value` into the ordinal int64 key space used by
        this Manifest's (ordinal-encoded) bounds for `column_name`. Returns
        None when the column's type can't be resolved, or when the physical
        type has no scalar ordinalize kernel (TIMESTAMP64/TIME32/TIME64/
        DECIMAL128 — see ColumnType.ordinalize) — callers treat None as "can't
        safely compare", the same conservative skip `_comparable_literal`
        already uses elsewhere in this function, not a silent wrong answer.
        """
        column_type = self._column_type(column_name)
        if column_type is None:
            return None
        try:
            return column_type.ordinalize(literal_value)
        except ValueError:
            return None

    def prune_files(self, predicates: List) -> None:
        """
        Filter files based on predicates using min/max bounds.

        Called by optimizer to determine which files to read.
        Returns list of files that might contain matching rows.

        This is NOT called at execution time - the optimizer makes
        this decision and execution just follows it.

        Args:
            predicates: List of predicate Node objects to evaluate

        Returns:
            Filtered list of FileEntry objects
        """
        from opteryx.expression import NodeType

        # Define handlers for each comparison operator
        # Returns True if file can be pruned (skipped)
        handlers = {
            "Eq": lambda v, min_, max_: v < min_ or v > max_,
            "NotEq": lambda v, min_, max_: min_ == max_ == v,
            "Gt": lambda v, min_, max_: max_ <= v,
            "GtEq": lambda v, min_, max_: max_ < v,
            "Lt": lambda v, min_, max_: min_ >= v,
            "LtEq": lambda v, min_, max_: min_ > v,
        }

        if not predicates:
            # No predicates = no pruning
            return self.files

        kept_files = []
        kept_rows: List[int] = []

        # Native exact-set file elimination for `col = <int>`: keep masks indexed by
        # original vector row (1=keep, 0=provably absent). Empty unless a native
        # sketch vector and an eligible integer-equality predicate are both present.
        membership_masks = self._membership_keep_masks(predicates)

        for position, file_entry in enumerate(self.files):
            # Original vector-row index for this file (identity until first prune).
            original_row = position if self._live_rows is None else self._live_rows[position]
            skip_file = False

            # Native sketch elimination (conservative: only drops files whose
            # unsaturated sketch proves the value absent).
            for mask in membership_masks:
                if mask[original_row] == 0:
                    skip_file = True
                    break
            if skip_file:
                continue

            # Check each predicate
            for predicate in predicates:
                # Handle simple comparisons: column op literal
                if (
                    predicate.node_type == NodeType.COMPARISON_OPERATOR
                    and predicate.value in handlers
                    and predicate.left.node_type == NodeType.IDENTIFIER
                    and predicate.right.node_type == NodeType.LITERAL
                ):
                    # Get column name and literal value
                    column_name = predicate.left.source_column
                    literal_value = predicate.right.value

                    # Normalize literal value
                    if getattr(literal_value, "item", None) is not None:
                        literal_value = literal_value.item()

                    # Resolve via the shared field-id resolution path (prefers a
                    # real catalog field_id; falls back to load-time schema
                    # position) — never index against `self.schema` directly
                    # here, since projection pushdown may have pruned it to a
                    # subset of columns by the time this runs, which would
                    # silently resolve a different column's bounds.
                    field_id = self._resolve_field_id(column_name)

                    if field_id is None:
                        continue

                    # Get bounds for this field
                    if not file_entry.lower_bounds or not file_entry.upper_bounds:
                        continue

                    min_value = file_entry.lower_bounds.get(field_id)
                    max_value = file_entry.upper_bounds.get(field_id)

                    if min_value is not None and max_value is not None:
                        compare_value = literal_value
                        if self.bounds_are_ordinal:
                            # Bounds are Vector.ordinalize() ordinal keys, not real
                            # values (see __init__'s bounds_are_ordinal docstring) -
                            # the literal must go through the SAME transform before
                            # it is comparable to min_value/max_value.
                            compare_value = self._ordinalize_literal(column_name, literal_value)
                            if compare_value is None:
                                continue
                        comparable_value = _comparable_literal(compare_value, min_value)
                        if comparable_value is None:
                            # Bound is stored in a representation this literal can't
                            # be safely compared against (e.g. a string literal
                            # against non-string bounds) - skip pruning rather than
                            # risk comparing incompatible types.
                            continue
                        # Check if file can be pruned
                        prune_func = handlers.get(predicate.value)
                        if prune_func and prune_func(comparable_value, min_value, max_value):
                            skip_file = True
                            break

                elif (
                    predicate.node_type == NodeType.BETWEEN
                    and predicate.left.node_type == NodeType.IDENTIFIER
                    and predicate.right.node_type == NodeType.LITERAL
                    and predicate.centre.node_type == NodeType.LITERAL
                ):
                    column_name = predicate.left.source_column
                    lower = predicate.right.value
                    upper = predicate.centre.value
                    if getattr(lower, "item", None) is not None:
                        lower = lower.item()
                    if getattr(upper, "item", None) is not None:
                        upper = upper.item()

                    # See the comparison-operator branch above: resolve via the
                    # shared field-id path, not a direct lookup against the
                    # (possibly projection-pruned) live schema.
                    field_id = self._resolve_field_id(column_name)

                    if field_id is None:
                        continue

                    if not file_entry.lower_bounds or not file_entry.upper_bounds:
                        continue

                    min_value = file_entry.lower_bounds.get(field_id)
                    max_value = file_entry.upper_bounds.get(field_id)

                    if min_value is not None and max_value is not None:
                        compare_lower, compare_upper = lower, upper
                        if self.bounds_are_ordinal:
                            compare_lower = self._ordinalize_literal(column_name, lower)
                            compare_upper = self._ordinalize_literal(column_name, upper)
                            if compare_lower is None or compare_upper is None:
                                continue
                        comparable_lower = _comparable_literal(compare_lower, min_value)
                        comparable_upper = _comparable_literal(compare_upper, min_value)
                        if comparable_lower is None or comparable_upper is None:
                            continue
                        if max_value < comparable_lower or min_value > comparable_upper:
                            skip_file = True
                            break

            if not skip_file:
                kept_files.append(file_entry)
                kept_rows.append(original_row)

        self.files = kept_files
        self._live_rows = kept_rows

    # ================================================================
    # File Accessors
    # ================================================================

    def get_file_paths(self) -> List[str]:
        """Get file paths from the manifest."""
        return [file.file_path for file in self.files]

    # ================================================================
    # Estimation Methods (for cost-based optimization)
    # ================================================================

    def estimate_selectivity(self, predicate) -> float:
        """Estimate fraction of rows matching predicate.

        Delegates to cost_estimation.selectivity.estimate_selectivity, which
        operates over a RelationStatistics view of this manifest.
        """
        from opteryx.planner.cost_estimation.selectivity import estimate_selectivity
        return estimate_selectivity(predicate, self._as_relation_statistics())

    def _as_relation_statistics(self):
        """Build a fresh RelationStatistics snapshot of this manifest.

        No caching: selectivity walks rebuild this on each call. Cheap because
        the underlying manifest accessors (estimate_cardinality, get_distogram,
        estimate_null_fraction) already memoise as needed.
        """
        from opteryx.planner.optimizer.statistics import (
            ColumnRange,
            ColumnStatistics,
            RelationStatistics,
        )
        total_rows = self.get_record_count()
        has_null_counts = any(
            (f.column_stats is not None and f.column_stats.has_any_null_counts())
            or bool(f.null_value_counts)
            for f in self.files
        )
        # Keyed by column identity to match RelationStatistics' contract — the
        # predicate walker in cost_estimation.selectivity looks columns up by
        # the bound identifier's identity, never by name.
        columns: dict = {}
        for col in self.schema.columns:
            col_name = getattr(col, "name", None)
            identity = getattr(col, "identity", None)
            if not col_name or not isinstance(identity, bytes):
                continue
            null_fraction = None
            if has_null_counts:
                null_fraction = self.estimate_null_fraction(col_name)
            char_class_stats = self.get_char_class_stats(col_name)
            columns[identity] = ColumnStatistics(
                column_name=col_name,
                data_type=str(getattr(col, "type", "")),
                distinct_count=self.estimate_cardinality(col_name),
                value_range=ColumnRange(),
                histogram=self.get_distogram(col_name),
                null_fraction=null_fraction,
                class_proportions=char_class_stats[0] if char_class_stats else None,
                avg_length=char_class_stats[1] if char_class_stats else None,
                ordinal_bounds=self.get_ordinal_bounds(col_name),
                length_bounds=self.get_length_bounds(col_name),
            )
        return RelationStatistics(row_count=total_rows, columns=columns)

    # ================================================================
    # Char-class stats (LIKE '%needle%' selectivity)
    # ================================================================

    def get_char_class_stats(self, column: str) -> Optional[Tuple[dict, float]]:
        """(class_proportions, avg_length) for `column`, or None.

        Backs the LIKE '%needle%' char-class selectivity estimator
        (opteryx.planner.cost_estimation.selectivity). None when the relation
        has no char-class stats for this column (nothing ANALYZE'd, a
        non-string column, or a catalog predating this accessor — the same
        "no signal, fall through to the flat constant" shape estimate_cardinality/
        get_distogram already use for their own missing-vector case).

        class_proportions: {class_name: fraction of this column's bytes in
        that class}, the 8 classes opteryx.planner.cost_estimation.selectivity
        uses. avg_length: mean string length in bytes, DERIVED here (not
        stored) as char_total_bytes / max(1, true_non_null_count) — the
        total-bytes figure is exactly sum(class totals) (every byte belongs
        to one class), and true_non_null_count comes from summing this
        column's real null_counts against record_count across the manifest's
        live files (post-prune) — both boxed on FileEntry, not the native
        vector, so a plain Python sum over files (not rows) is fine here.
        """
        if self._char_class_vector is None:
            return None

        field_id = self._sketch_index(column)
        if field_id is None:
            return None

        from opteryx.compiled.nanobind.vectors import char_class_field_totals
        from opteryx.planner.cost_estimation.selectivity import _CHAR_CLASSES

        handle = self._native_handle(self._char_class_vector)
        totals = char_class_field_totals(handle, field_id, self._live_rows)
        if totals is None:
            return None

        total_bytes = sum(totals)
        if total_bytes <= 0:
            return None

        class_proportions = {
            name: totals[i] / total_bytes for i, name in enumerate(_CHAR_CLASSES)
        }

        non_null_rows = 0
        for file_entry in self.files:
            null_counts = file_entry.null_counts
            null_count = (
                null_counts[field_id]
                if null_counts and field_id < len(null_counts) and null_counts[field_id] is not None
                else 0
            )
            non_null_rows += max(0, (file_entry.record_count or 0) - null_count)

        avg_length = total_bytes / max(1, non_null_rows)
        return class_proportions, avg_length

    def get_ordinal_bounds(self, column: str) -> Optional[Tuple[int, int]]:
        """Relation-wide (lo, hi) ordinal-key bounds for `column`, or None.

        Backs the STARTS_WITH ordinal-bounds selectivity estimator tier
        (opteryx.planner.cost_estimation.selectivity) — a coarser, cheaper
        fallback than a full histogram: just the overall span, aggregated
        from each live file's min_values/max_values, no bin-level detail.
        None when `bounds_are_ordinal` is False (the bounds are real decoded
        values, not ordinal keys — see __init__'s docstring), the column
        isn't found, or no file carries a usable bound for it.

        A "usable" bound excludes both `None` (the local ANALYZE path's "not
        computed" marker — see _analyze_one_file's `min_max = None`) AND any
        negative value: a genuine ColumnType.ordinalize() key for a
        string-family column is always >= 0 (draken/ops/ordinalize.h treats
        the byte prefix as unsigned before the sign-fitting right-shift), so
        a negative entry can only be a producer's own "no real bound"
        sentinel (e.g. the catalog manifest builder's NULL_FLAG = -(1<<63)
        for a column outside its compressible-categories set) smuggled
        through as a plain int rather than None — never real string data.

        Reads FileEntry.lower_bounds/upper_bounds (the dict form, keyed by
        the SAME field_id `_resolve_field_id` returns), never min_values/
        max_values (the list form) directly — that list is positional in
        whatever order its producer emitted it, NOT indexable by field_id:
        a catalog-backed FileEntry's field_ids can start above 0 or have
        gaps (FileEntry.from_datafile re-keys lower_bounds/upper_bounds via
        `zip(field_ids, min_values)` precisely because of this), so
        `min_values[field_id]` silently reads a DIFFERENT column's bound
        when field_id != position. prune_files has the same requirement and
        already uses the dict form for exactly this reason.
        """
        if not self.bounds_are_ordinal:
            return None
        field_id = self._resolve_field_id(column)
        if field_id is None:
            return None

        lo: Optional[int] = None
        hi: Optional[int] = None
        for file_entry in self.files:
            lower_bounds = file_entry.lower_bounds
            upper_bounds = file_entry.upper_bounds
            if not lower_bounds or not upper_bounds:
                continue
            v_min = lower_bounds.get(field_id)
            v_max = upper_bounds.get(field_id)
            if v_min is None or v_max is None or v_min < 0 or v_max < 0:
                continue
            lo = v_min if lo is None else min(lo, v_min)
            hi = v_max if hi is None else max(hi, v_max)

        if lo is None or hi is None:
            return None
        return lo, hi

    def get_length_bounds(self, column: str) -> Optional[Tuple[int, int]]:
        """Relation-wide (min_length, max_length) in bytes for `column`, or None.

        Backs the length-aware hard-impossibility guard shared by the
        containment-style selectivity estimators (STARTS_WITH, INSTR,
        ENDS_WITH — opteryx.planner.cost_estimation.selectivity): a needle
        longer than the column's observed maximum length can never match, a
        cheap, certain check with no probabilistic reasoning needed. No
        `bounds_are_ordinal` gate here (unlike get_ordinal_bounds) — a
        string's byte length is never ordinal-encoded, it's always a plain
        integer regardless of how the value bounds are stored.

        Excludes non-positive bounds (`<= 0`), not just `None`: the catalog's
        stats builder initializes min_len/max_len to 0 and only overwrites
        them if the file has a non-null value for the column, so `0` is
        ambiguous between "no data computed for this file" and "a genuinely
        empty string" — treating it as a real bound risks a false-positive
        "impossible" verdict for a column that simply has no stats yet.

        Reads FileEntry.min_length_bounds/max_length_bounds (the field_id-
        correct dict form), never the positional min_lengths/max_lengths
        list — same field_id-vs-position requirement get_ordinal_bounds has,
        see that method's docstring for the full reasoning.
        """
        field_id = self._resolve_field_id(column)
        if field_id is None:
            return None

        lo: Optional[int] = None
        hi: Optional[int] = None
        for file_entry in self.files:
            min_length_bounds = file_entry.min_length_bounds
            max_length_bounds = file_entry.max_length_bounds
            if not min_length_bounds or not max_length_bounds:
                continue
            v_min = min_length_bounds.get(field_id)
            v_max = max_length_bounds.get(field_id)
            if v_min is None or v_max is None or v_min <= 0 or v_max <= 0:
                continue
            lo = v_min if lo is None else min(lo, v_min)
            hi = v_max if hi is None else max(hi, v_max)

        if lo is None or hi is None:
            return None
        return lo, hi

    # ================================================================
    # Histograms / Distograms
    # ================================================================

    def get_distogram(self, column: str) -> Optional[Distogram]:
        """Build or retrieve a combined distogram for the column from per-file histograms."""

        if column in self._distogram_cache:
            return self._distogram_cache[column]

        field_id = self._resolve_field_id(column)
        if field_id is None:
            return None

        # Histograms live only as the whole-column native vector; gather this
        # column's per-file bin slices from it (no boxing) and fold them with the
        # native load_counts_i64 + merge. No vector => no histogram for this
        # relation (nothing has ANALYZE'd/produced one).
        if self._histogram_vector is None:
            return None

        sketch_id = self._sketch_index(column)
        if sketch_id is None:
            return None

        combined = self._native_distogram(sketch_id)
        if combined is not None:
            self._distogram_cache[column] = combined
        return combined

    def _membership_keep_masks(self, predicates: List) -> List:
        """Native exact-set keep masks for eligible integer-equality predicates.

        For each ``col = <int literal>`` on an integer column, hashes the literal
        with the column's exact draken physical type — provenance-matched to how
        the writer built the sketch, so the probe hash is guaranteed to equal the
        value's hash in the sketch — and asks ``sketch_keep_mask`` which files
        provably lack the value. Returns one bytes mask per eligible predicate
        (indexed by ORIGINAL vector row; 1=keep, 0=eliminate).

        Restricted to integer columns/literals: that is the case where the Python
        literal's representation is unambiguously the column's, so the hash match
        is certain. Other types are left to bounds-only pruning — a mismatched
        probe hash would wrongly eliminate a matching file, so they are excluded
        rather than risk a wrong answer.
        """
        if self._min_k_vector is None:
            return []

        from draken.draken_native import DrakenType
        from draken.interop.vector_sequence import vector_from_sequence
        from opteryx.compiled.nanobind.vectors import sketch_keep_mask
        from opteryx.expression import NodeType

        integer_physical = {
            DrakenType.INT8,
            DrakenType.INT16,
            DrakenType.INT32,
            DrakenType.INT64,
        }
        handle = self._native_handle(self._min_k_vector)
        masks: List = []

        for predicate in predicates:
            if not (
                predicate.node_type == NodeType.COMPARISON_OPERATOR
                and predicate.value == "Eq"
                and predicate.left.node_type == NodeType.IDENTIFIER
                and predicate.right.node_type == NodeType.LITERAL
            ):
                continue

            column_name = predicate.left.source_column
            # Sketch index, not the live schema position: the vector's column axis
            # is fixed at load-time width and projection may have pruned the schema.
            field_id = self._sketch_index(column_name)
            physical = None
            for col in self.schema.columns:
                if col.name == column_name:
                    physical = col.column_type.physical if col.column_type is not None else None
                    break
            if field_id is None or physical not in integer_physical:
                continue

            value = predicate.right.value
            if getattr(value, "item", None) is not None:  # numpy scalar -> python int
                value = value.item()
            if not isinstance(value, int) or isinstance(value, bool):
                continue

            probe = vector_from_sequence([value], dtype=physical).hash()
            masks.append(sketch_keep_mask(handle, field_id, [int(probe[0])]))

        return masks

    @staticmethod
    def _native_handle(vector):
        """Underlying draken_native.Vector handle for a sketch vector.

        A sketch vector legitimately arrives as either of two types: a draken
        ``Vector`` wrapper (what ``Morsel.column()`` yields — the manifest read
        path) or an already-native ``draken_native.Vector`` (what the array
        constructors yield). Dispatch is on the concrete type, not a probe for
        an attribute, so anything else fails loud rather than degrading silently.
        """
        import draken.draken_native as _dn

        if isinstance(vector, _dn.Vector):
            return vector
        return vector._nb

    def _native_distogram(self, field_id: int) -> Optional[Distogram]:
        """Fold the column's per-file histograms into one Distogram natively.

        `field_id` is a SKETCH index (see _sketch_index) — the histogram vector's
        column axis and FileEntry.min_values/max_values are both positional against
        the load-time schema, so the same index addresses all three. Gathers the
        bin-count slices from the whole-column histogram vector (no boxing) and
        folds them with load_counts_i64 + merge. Produces the same Distogram as the
        Python fold.
        """
        from opteryx.compiled.nanobind.vectors import histogram_field_slices
        from opteryx.third_party.maki_nage.distogram import load_counts_i64

        handle = self._native_handle(self._histogram_vector)
        counts_b, offsets_b = histogram_field_slices(handle, field_id)
        counts = memoryview(counts_b).cast("q")      # int64 bin counts, files concatenated
        offsets = memoryview(offsets_b).cast("i")    # int32[n_files + 1]

        combined: Optional[Distogram] = None
        for position, file_entry in enumerate(self.files):
            # counts are indexed by ORIGINAL vector row; min/max by the surviving
            # FileEntry. _live_rows maps the pruned position back to the vector row.
            original_row = position if self._live_rows is None else self._live_rows[position]
            start = offsets[original_row]
            end = offsets[original_row + 1]
            if end <= start:
                continue
            # Prefer the positional min_values/max_values list (catalog/
            # LocalStoreConnector FileEntry — unchanged behavior). Fall back to
            # lower_bounds/upper_bounds (dict keyed by the SAME positional
            # index for ANALYZE-manifest-sourced bounds — see
            # filesystem_connector.py's _read_dataset_manifest) for a FileEntry
            # that only carries the dict form, e.g. a filesystem-connector
            # dataset built from ANALYZE's manifest, which never sets the list
            # form at all (mirrors manifest_io._file_entry_bounds_as_values'
            # own "prefer list, else decode dict" precedence).
            if (
                file_entry.min_values is not None
                and file_entry.max_values is not None
                and field_id < len(file_entry.min_values)
                and field_id < len(file_entry.max_values)
            ):
                col_min = file_entry.min_values[field_id]
                col_max = file_entry.max_values[field_id]
            elif file_entry.lower_bounds is not None and file_entry.upper_bounds is not None:
                col_min = file_entry.lower_bounds.get(field_id)
                col_max = file_entry.upper_bounds.get(field_id)
            else:
                continue
            if col_min is None or col_max is None:
                continue
            dgram = load_counts_i64(counts[start:end], float(col_min), float(col_max))
            combined = dgram if combined is None else merge(combined, dgram)
        return combined

    def estimate_cardinality(self, column) -> Optional[int]:
        """
        Estimate distinct values in column using K-Minimum Values (KMV).

        Merges each file's min-k sketch for the column and applies the KMV
        estimator (exact count when the merged sketch is under K). Reduced
        natively over the whole-column sketch vector — no boxing. Returns None
        when the relation carries no sketches (nothing produced them).
        """
        # identity may be bytes; resolve to str for field mapping
        col_name = column.decode("utf-8") if isinstance(column, bytes) else column
        if self._min_k_vector is None:
            return None

        from opteryx.compiled.nanobind.vectors import kmv_ndv

        sketch_id = self._sketch_index(col_name)
        if sketch_id is None:
            return None
        # _live_rows (None until first prune) keeps the merge over the surviving
        # files only, matching the file set the rest of the plan will read.
        return kmv_ndv(self._native_handle(self._min_k_vector), sketch_id, self._live_rows)

    def get_total_null_count(self, column) -> Optional[int]:
        """Total nulls for a column across all files.

        Returns None if any file is missing null counts for this column —
        a partial answer would silently overcount non-null values, so callers
        that need an exact answer (e.g. statistics-only COUNT(col)) must treat
        None as "unknown" and fall back to reading data.
        """
        col_name = column.decode("utf-8") if isinstance(column, bytes) else column
        field_id = self._resolve_field_id(col_name)
        if field_id is None:
            return None

        total = 0
        for file in self.files:
            if file.column_stats is not None:
                nc = file.column_stats.get_null_count(field_id)
                if nc is None:
                    return None
                total += nc
            elif file.null_value_counts:
                if field_id not in file.null_value_counts:
                    return None
                total += file.null_value_counts[field_id]
            else:
                return None
        return total

    def estimate_null_fraction(self, column) -> Optional[float]:
        """Estimate fraction of nulls in column using catalog null counts if present."""
        col_name = column.decode("utf-8") if isinstance(column, bytes) else column
        field_id = self._resolve_field_id(col_name)
        if field_id is None:
            return None

        total_rows = self.get_record_count()
        if total_rows == 0:
            return None

        null_count = 0
        for file in self.files:
            if file.column_stats is not None:
                nc = file.column_stats.get_null_count(field_id)
                if nc is not None:
                    null_count += nc
            elif file.null_value_counts and field_id in file.null_value_counts:
                null_count += file.null_value_counts[field_id]

        return null_count / total_rows if total_rows > 0 else 0.0

    # ================================================================
    # Column Statistics
    # ================================================================

    def get_column_bounds(self, column: str) -> Optional[Tuple[bytes, bytes]]:
        """
        Get aggregated min/max bounds for column across all files.

        Returns raw serialized bytes as stored in manifest.

        Args:
            column: Column name

        Returns:
            Tuple of (min_bytes, max_bytes), or None if not available
        """
        if column in self._column_bounds_cache:
            return self._column_bounds_cache[column]

        field_id = self._get_field_id(column)
        if field_id is None:
            return None

        min_val = None
        max_val = None

        for file in self.files:
            if file.lower_bounds and field_id in file.lower_bounds:
                file_min = file.lower_bounds[field_id]
                if min_val is None or file_min < min_val:
                    min_val = file_min

            if file.upper_bounds and field_id in file.upper_bounds:
                file_max = file.upper_bounds[field_id]
                if max_val is None or file_max > max_val:
                    max_val = file_max

        result = (min_val, max_val) if min_val is not None and max_val is not None else None
        self._column_bounds_cache[column] = result
        return result

    def get_column_stats(self, column: str) -> Dict[str, Any]:
        """
        Get comprehensive statistics for a column.

        Args:
            column: Column name

        Returns:
            Dictionary with available statistics:
                - bounds: (min, max) tuple
                - null_fraction: fraction of nulls
                - estimated_cardinality: distinct count estimate
        """
        return {
            "bounds": self.get_column_bounds(column),
            "null_fraction": self.estimate_null_fraction(column),
            "estimated_cardinality": self.estimate_cardinality(column),
        }

    # ================================================================
    # Schema Mapping Helpers
    # ================================================================

    def _build_field_mappings(self):
        """Build field_id <-> column_name mappings from schema."""
        if self._field_id_to_name is not None:
            return

        self._field_id_to_name = {}
        self._name_to_field_id = {}

        # Build mapping from schema
        # Note: This assumes schema has field_id information
        # May need adjustment based on actual schema structure
        for column in self.schema.columns:
            if getattr(column, "field_id", None) is not None:
                field_id = column.field_id
                self._field_id_to_name[field_id] = column.name
                self._name_to_field_id[column.name] = field_id

    def _resolve_field_id(self, column_name: str) -> Optional[int]:
        """Resolve column to field_id; fall back to schema index when field_id is absent."""
        self._build_field_mappings()

        # Prefer explicit field_id mapping when present
        if self._name_to_field_id and column_name in self._name_to_field_id:
            return self._name_to_field_id[column_name]

        # Fallback: position in the LOAD-TIME schema, which is the order the
        # per-file stats are keyed by. NEVER the live `self.schema` — projection
        # pushdown prunes it to the referenced columns, so a lookup there returns a
        # position in the pruned list and silently reads another column's stats
        # (MAX(followers) answering with MAX(tweet_id) when followers pruned to
        # index 0). Same trap _sketch_index documents.
        return self._load_time_columns.get(column_name)

    def _get_field_id(self, column_name: str) -> Optional[int]:
        """Get field_id for column name."""
        self._build_field_mappings()
        return self._name_to_field_id.get(column_name)

    def _get_column_name(self, field_id: int) -> Optional[str]:
        """Get column name for field_id."""
        self._build_field_mappings()
        return self._field_id_to_name.get(field_id)

    # ================================================================
    # Debug/Inspection
    # ================================================================

    def summary(self) -> Dict[str, Any]:
        """Get summary information for debugging."""
        return {
            "file_count": self.get_file_count(),
            "record_count": self.get_record_count(),
            "total_size_bytes": self.get_total_size(),
            "avg_file_size": (
                self.get_total_size() / self.get_file_count() if self.get_file_count() > 0 else 0
            ),
            "files_with_bounds": sum(1 for f in self.files if f.lower_bounds or f.upper_bounds),
            # Sketches are whole-relation native vectors, not per-file lists.
            "has_k_hashes": self._min_k_vector is not None,
            "has_histograms": self._histogram_vector is not None,
            "has_char_class_stats": self._char_class_vector is not None,
        }

