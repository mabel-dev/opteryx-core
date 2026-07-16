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

from opteryx.compiled.structures.relation_statistics import to_int
from opteryx.models.file_entry import FileEntry
from opteryx.third_party.maki_nage.distogram import Distogram, load, merge
from opteryx.types.schema import RelationSchema


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
    ):
        """
        Initialize Manifest with file entries and schema.

        Args:
            files: List of FileEntry objects from catalog scan
            schema: Table schema (RelationSchema)
            min_k_vector: optional native draken ``array<array<uint64>>`` Vector
                holding every file's min-k sketch (one outer row per file, one
                middle row per column). When present, sketch reductions run as
                native kernels over this vector instead of the per-file Python
                ``FileEntry.min_k_hashes`` lists. None on paths that don't supply
                it (e.g. LocalStore), which fall back to the Python merge.
            histogram_vector: optional native ``array<array<int64>>`` Vector of
                per-file, per-column histogram bins, used the same way.
        """
        self.files = files
        self.schema = schema
        self._min_k_vector = min_k_vector
        self._histogram_vector = histogram_vector
        # The sketch vectors are built once over the FULL file set and are indexed
        # by original file position. prune_files shrinks self.files, so we track the
        # surviving original row indices to keep native reductions aligned with the
        # (pruned) file list — matching the Python paths that iterate self.files.
        # None means "no pruning yet": row i of the vectors == self.files[i].
        self._live_rows: Optional[List[int]] = None

        # Lazy-computed mappings
        self._field_id_to_name: Optional[Dict[int, str]] = None
        self._name_to_field_id: Optional[Dict[str, int]] = None
        self._column_bounds_cache: Dict[str, Tuple[Any, Any]] = {}
        self._distogram_cache: Dict[str, Distogram] = {}

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

                    p = literal_value
                    literal_value = to_int(literal_value)

                    # Get field_id for this column from schema
                    # Bounds are indexed by field_id (int)
                    field_id = None
                    for i, col in enumerate(self.schema.columns):
                        if col.name == column_name:
                            field_id = i
                            break

                    # For now, skip this file if we can't map column to bounds
                    # In a full implementation, we'd need proper field_id mapping
                    # from the schema
                    if field_id is None:
                        continue

                    # Get bounds for this field
                    if not file_entry.lower_bounds or not file_entry.upper_bounds:
                        continue

                    min_value = file_entry.lower_bounds.get(field_id)
                    max_value = file_entry.upper_bounds.get(field_id)

                    if min_value is not None and max_value is not None:
                        # Check if file can be pruned
                        prune_func = handlers.get(predicate.value)
                        if prune_func and prune_func(literal_value, min_value, max_value):
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

                    field_id = None
                    for i, col in enumerate(self.schema.columns):
                        if col.name == column_name:
                            field_id = i
                            break

                    if field_id is None:
                        continue

                    if not file_entry.lower_bounds or not file_entry.upper_bounds:
                        continue

                    min_value = file_entry.lower_bounds.get(field_id)
                    max_value = file_entry.upper_bounds.get(field_id)

                    if min_value is not None and max_value is not None:
                        if max_value < lower or min_value > upper:
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
        columns: dict = {}
        for col in self.schema.columns:
            col_name = getattr(col, "name", None)
            if not col_name:
                continue
            null_fraction = None
            if has_null_counts:
                null_fraction = self.estimate_null_fraction(col_name)
            columns[col_name] = ColumnStatistics(
                column_name=col_name,
                data_type=str(getattr(col, "type", "")),
                distinct_count=self.estimate_cardinality(col_name),
                value_range=ColumnRange(),
                histogram=self.get_distogram(col_name),
                null_fraction=null_fraction,
            )
        return RelationStatistics(row_count=total_rows, columns=columns)

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

        # Native path: gather the column's per-file histogram slices from the whole
        # column vector (no boxing) and fold them with the native load_counts_i64 +
        # merge. Same bin math as the Python fold below (load_counts_i64 reproduces
        # the (bin_idx + 0.5) center and min==max single-bin case exactly).
        if self._histogram_vector is not None:
            combined = self._native_distogram(field_id)
            if combined is not None:
                self._distogram_cache[column] = combined
            return combined

        combined: Optional[Distogram] = None

        for file_entry in self.files:
            # Ensure histograms and min/max are present and aligned
            if not file_entry.histogram_counts:
                continue
            if file_entry.min_values is None or file_entry.max_values is None:
                continue
            if field_id >= len(file_entry.histogram_counts):
                continue
            if field_id >= len(file_entry.min_values) or field_id >= len(file_entry.max_values):
                continue

            col_hist = file_entry.histogram_counts[field_id]
            col_min = file_entry.min_values[field_id]
            col_max = file_entry.max_values[field_id]

            if col_hist is None or col_min is None or col_max is None:
                continue

            if getattr(col_hist, "__iter__", None) is None:
                continue
            counts = list(col_hist)

            if not counts:
                continue

            col_min_f = float(col_min)
            col_max_f = float(col_max)

            bins: List[Tuple[float, int]] = []
            if col_min_f == col_max_f:
                bins = [(col_min_f, sum(counts))]
            else:
                num_bins = len(counts)
                span = col_max_f - col_min_f
                for bin_idx, count in enumerate(counts):
                    if count == 0:
                        continue
                    center = col_min_f + (bin_idx + 0.5) * span / num_bins
                    bins.append((center, int(count)))

            if not bins:
                continue

            dgram = load(bins, col_min_f, col_max_f)
            combined = dgram if combined is None else merge(combined, dgram)

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
            field_id = None
            physical = None
            for idx, col in enumerate(self.schema.columns):
                if col.name == column_name:
                    field_id = idx
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

        Sketch vectors arriving from the catalog are draken ``Vector`` wrappers;
        the native kernels take the wrapped ``_nb`` handle. Accessed directly (not
        probed) so a broken contract fails loud rather than silently degrading.
        """
        return vector._nb

    def _native_distogram(self, field_id: int) -> Optional[Distogram]:
        """Fold the column's per-file histograms into one Distogram natively.

        Gathers the field_id bin-count slices from the whole-column histogram
        vector (no boxing) and folds them with load_counts_i64 + ``+``. Per-file
        min/max come from the (cheap, flat) FileEntry lists, exactly as the Python
        path indexes them. Produces the same Distogram as the Python fold.
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
            if file_entry.min_values is None or file_entry.max_values is None:
                continue
            if field_id >= len(file_entry.min_values) or field_id >= len(file_entry.max_values):
                continue
            col_min = file_entry.min_values[field_id]
            col_max = file_entry.max_values[field_id]
            if col_min is None or col_max is None:
                continue
            dgram = load_counts_i64(counts[start:end], float(col_min), float(col_max))
            combined = dgram if combined is None else merge(combined, dgram)
        return combined

    def estimate_cardinality(self, column) -> Optional[int]:
        """
        Estimate distinct values in column using K-Minimum Values (KMV).

        Uses min-k hashes from file entries if available, otherwise returns None.
        Merges min-k hashes across all files and applies KMV estimator formula.
        """
        K = 32
        HASH_RANGE = 2**64

        # identity may be bytes; resolve to str for field mapping
        col_name = column.decode("utf-8") if isinstance(column, bytes) else column
        field_id = self._resolve_field_id(col_name)
        if field_id is None:
            return None

        # Native path: KMV union over the whole-column sketch vector, no boxing.
        # Same field_id semantics as the Python merge below (positional index into
        # each file's per-column sketch), so this is behaviour-preserving.
        if self._min_k_vector is not None:
            from opteryx.compiled.nanobind.vectors import kmv_ndv

            # _live_rows (None until first prune) keeps the merge over the same
            # surviving files the Python fallback below would iterate.
            return kmv_ndv(self._native_handle(self._min_k_vector), field_id, self._live_rows)

        # Fallback (no native vector, e.g. LocalStore): per-file Python merge.
        # Merge min-k hashes from all files
        min_k_hashes = []

        for file_entry in self.files:
            if file_entry.min_k_hashes and field_id < len(file_entry.min_k_hashes):
                file_hashes = file_entry.min_k_hashes[field_id]
                if file_hashes:
                    # Merge keeping k smallest distinct hashes
                    min_k_hashes = sorted(set(min_k_hashes + file_hashes))[:K]

        if not min_k_hashes:
            return None

        # Apply KMV estimator formula
        if len(min_k_hashes) < K:
            # Exact count when we have fewer than K distinct values
            return len(min_k_hashes)

        # Estimate: (k-1) * hash_range / kth_smallest_hash
        return int((K - 1) * HASH_RANGE / min_k_hashes[K - 1])

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

        # Fallback: positional index in schema
        for idx, col in enumerate(self.schema.columns):
            if col.name == column_name:
                return idx

        return None

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
            "files_with_k_hashes": sum(1 for f in self.files if f.min_k_hashes),
            "files_with_histograms": sum(1 for f in self.files if f.histogram_counts),
        }

