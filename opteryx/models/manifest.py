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

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from orso.schema import RelationSchema

from opteryx.compiled.structures.relation_statistics import to_int
from opteryx.models.file_entry import FileEntry
from opteryx.third_party.maki_nage.distogram import Distogram
from opteryx.third_party.maki_nage.distogram import load


class Manifest:
    """
    Encapsulates manifest data and statistics operations for a table.

    Created during binding phase from catalog's table.scan() results.
    Provides methods for optimizer to make pruning and costing decisions.
    """

    def __init__(self, files: List[FileEntry], schema: RelationSchema):
        """
        Initialize Manifest with file entries and schema.

        Args:
            files: List of FileEntry objects from catalog scan
            schema: Table schema (RelationSchema)
        """
        self.files = files
        self.schema = schema

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
        from opteryx.managers.expression import NodeType

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

        for file_entry in self.files:
            skip_file = False

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
                    if hasattr(literal_value, "item"):
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

            if not skip_file:
                kept_files.append(file_entry)

        self.files = kept_files

    # ================================================================
    # File Accessors
    # ================================================================

    def get_file_paths(self) -> List[str]:
        """Get file paths from the manifest."""
        return [file.file_path for file in self.files]

    # ================================================================
    # Estimation Methods (for cost-based optimization)
    # ================================================================

    def estimate_selectivity(self, _predicate) -> float:
        """
        Estimate fraction of rows matching predicate.

        Uses histograms if available, otherwise falls back to bounds
        or simple heuristics.

        Args:
            predicate: Predicate expression to evaluate

        Returns:
            Estimated selectivity (0.0 to 1.0)

        Note:
            Placeholder - will be implemented with actual estimation logic.
        """
        return 0.5

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

            if not hasattr(col_hist, "__iter__"):
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
            combined = dgram if combined is None else combined + dgram

        if combined is not None:
            self._distogram_cache[column] = combined

        return combined

    def estimate_cardinality(self, column: str) -> Optional[int]:
        """
        Estimate distinct values in column using K-Minimum Values (KMV).

        Uses min-k hashes from file entries if available, otherwise returns None.
        Merges min-k hashes across all files and applies KMV estimator formula.
        """
        K = 32
        HASH_RANGE = 2**64

        field_id = self._resolve_field_id(column)
        if field_id is None:
            return None

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

    def estimate_null_fraction(self, column: str) -> Optional[float]:
        """Estimate fraction of nulls in column using catalog null counts if present."""
        field_id = self._resolve_field_id(column)
        if field_id is None:
            return None

        total_rows = self.get_record_count()
        if total_rows == 0:
            return None

        null_count = 0
        for file in self.files:
            if file.null_value_counts and field_id in file.null_value_counts:
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
            if hasattr(column, "field_id"):
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
            "avg_file_size": self.get_total_size() / self.get_file_count()
            if self.get_file_count() > 0
            else 0,
            "files_with_bounds": sum(1 for f in self.files if f.lower_bounds or f.upper_bounds),
            "files_with_k_hashes": sum(1 for f in self.files if f.min_k_hashes),
            "files_with_histograms": sum(1 for f in self.files if f.histogram_counts),
        }
