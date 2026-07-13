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
from opteryx.third_party.maki_nage.distogram import Distogram, load
from opteryx.types.schema import RelationSchema

# Hash a NULL row carries in a draken ``Vector.hash()`` sketch. It is a single
# constant across every column type (all null rows hash through the same
# NULL_HASH sentinel), so it can be stripped from a KMV set to recover the
# distinct NON-null values — which is what SQL COUNT(DISTINCT) counts. Sketches
# produced by the older null-skipping populator simply never contain it, so the
# discard is a harmless no-op there.
_NULL_ROW_HASH: Optional[int] = None


def _null_row_hash() -> int:
    global _NULL_ROW_HASH
    if _NULL_ROW_HASH is None:
        import draken.draken_native as _dn

        _NULL_ROW_HASH = _dn.vector_null_from_length(1).hash()[0]
    return _NULL_ROW_HASH


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

        # Per-file sketch buffers (min_k_hashes, histograms, min/max lists) are
        # POSITIONAL, aligned to the schema column order AT CONSTRUCTION time.
        # The scan's `schema` is later projected down to just the referenced
        # columns, so resolving a sketch index against `self.schema` at use time
        # would desync (a projected 1-column schema maps every name to 0). Snapshot
        # the load-time column order here and use it for all positional sketch
        # lookups (KMV cardinality, MinHash pruning); it stays valid regardless of
        # later projection.
        self._sketch_col_index: Dict[str, int] = {
            col.name: i for i, col in enumerate(schema.columns) if getattr(col, "name", None)
        }

        # Lazy-computed mappings
        self._field_id_to_name: Optional[Dict[int, str]] = None
        self._name_to_field_id: Optional[Dict[str, int]] = None
        self._column_bounds_cache: Dict[str, Tuple[Any, Any]] = {}
        self._distogram_cache: Dict[str, Distogram] = {}

    def _sketch_index(self, column) -> Optional[int]:
        """Positional index of a column into the per-file sketch buffers, using
        the load-time column order (see ``__init__``). Robust to later projection
        of ``self.schema``."""
        col_name = column.decode("utf-8") if isinstance(column, bytes) else column
        return self._sketch_col_index.get(col_name)

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

            # MinHash/KMV elimination — exact-set membership for equality-family
            # predicates. Only fires when a file carries an EXACT sketch for the
            # column (a sketch below K holds every distinct value hash); does
            # nothing otherwise, so files without stats are never wrongly pruned.
            if not skip_file:
                for predicate in predicates:
                    if self._minhash_prune_file(file_entry, predicate):
                        skip_file = True
                        break

            if not skip_file:
                kept_files.append(file_entry)

        self.files = kept_files

    # ================================================================
    # MinHash / KMV exact-set optimizations
    # ================================================================

    # A sketch strictly below K holds every distinct value hash in the file — the
    # column is fully represented and membership tests are exact. At K the sketch
    # is a saturated approximation and must not be used to eliminate anything.
    _KMV_K = 32

    @staticmethod
    def _normalize_literal(value):
        """Unwrap a numpy/native scalar to a plain Python value."""
        item = getattr(value, "item", None)
        if item is not None:
            return value.item()
        return value

    def _column_physical_int(self, column_name: str) -> Optional[int]:
        """Integer DrakenType tag for a column's physical type, or None."""
        for col in self.schema.columns:
            if col.name == column_name:
                col_type = getattr(col, "column_type", None)
                physical = getattr(col_type, "physical", None) if col_type is not None else None
                if physical is None:
                    return None
                return int(physical.value)
        return None

    def _minhash_prune_file(self, file_entry, predicate) -> bool:
        """Return True if ``file_entry`` cannot satisfy ``predicate``, proven via
        the file's exact KMV sketch.

        Handles the equality family only:
          - ``col = v``      prune when v's hash is absent from the exact sketch.
          - ``col IN (…)``   prune when no listed value's hash is in the sketch.
          - ``col != v``     prune when the file's only distinct value is v.
          - ``col NOT IN(…)``prune when every distinct value is in the excluded set.

        Conservative throughout: any uncertainty (no sketch, saturated sketch,
        un-hashable literal, non-identifier operand) returns False (keep file).
        NULLs are safe — a file with nulls carries the null sentinel in its
        sketch, which blocks the ``!=`` / ``NOT IN`` "all rows excluded" cases
        (correctly, since a null row is neither included nor excluded) without
        ever affecting the ``=`` / ``IN`` membership tests.
        """
        from opteryx.expression import NodeType
        from opteryx.compiled.expression.compiled_expression import hash_literal_kmv

        if predicate.node_type != NodeType.COMPARISON_OPERATOR:
            return False
        op = predicate.value
        if op not in ("Eq", "NotEq", "InList", "NotInList"):
            return False
        if (
            predicate.left.node_type != NodeType.IDENTIFIER
            or predicate.right.node_type != NodeType.LITERAL
        ):
            return False

        column_name = predicate.left.source_column
        field_id = self._sketch_index(column_name)
        if field_id is None:
            return False

        if not file_entry.min_k_hashes or field_id >= len(file_entry.min_k_hashes):
            return False
        sketch = file_entry.min_k_hashes[field_id]
        if sketch is None or len(sketch) >= self._KMV_K:
            # Absent or saturated — not an exact representation.
            return False
        sketch_set = set(sketch)

        physical = self._column_physical_int(column_name)
        if physical is None:
            return False

        if op in ("Eq", "NotEq"):
            values = [self._normalize_literal(predicate.right.value)]
        else:
            raw = predicate.right.value
            if not isinstance(raw, (list, tuple, set, frozenset)):
                return False
            values = [self._normalize_literal(v) for v in raw]
            if not values:
                return False

        hashes = []
        for v in values:
            if v is None:
                # A NULL in an equality-family list can't be reasoned about via
                # the value sketch — bail rather than risk an unsound prune.
                return False
            h = hash_literal_kmv(v, physical)
            if h is None:
                return False  # unsupported/non-representable literal — keep file
            hashes.append(h)
        hash_set = set(hashes)

        if op == "Eq":
            return hashes[0] not in sketch_set
        if op == "InList":
            return sketch_set.isdisjoint(hash_set)
        if op == "NotEq":
            return sketch_set == {hashes[0]}
        # NotInList
        return sketch_set.issubset(hash_set)

    def exact_distinct_count(self, column, *, exclude_nulls: bool = True) -> Optional[int]:
        """Exact number of distinct values for a column, or None if it can't be
        proven exact.

        Exact only when EVERY surviving file carries a KMV sketch for the column
        and the merged distinct-hash set stays below K — a merged set below K is
        complete (no file saturated, nothing truncated), so its size is the true
        distinct count. At K or above the sketch is an approximation and this
        returns None.

        ``exclude_nulls`` (default True) strips the null-row sentinel, giving SQL
        COUNT(DISTINCT) semantics (nulls excluded) with no reliance on separately
        tracked null counts. Pass ``exclude_nulls=False`` for ``SELECT DISTINCT``
        row-count semantics, where a NULL is one distinct row: the returned count
        then equals exactly the number of rows ``SELECT DISTINCT col`` emits.
        """
        field_id = self._sketch_index(column)
        if field_id is None:
            return None
        if not self.files:
            return 0

        merged: set = set()
        for file_entry in self.files:
            if not file_entry.min_k_hashes or field_id >= len(file_entry.min_k_hashes):
                return None
            file_hashes = file_entry.min_k_hashes[field_id]
            if file_hashes is None:
                return None
            merged.update(file_hashes)
            if len(merged) >= self._KMV_K:
                # Merged set saturated — completeness can no longer be guaranteed.
                return None
        if exclude_nulls:
            merged.discard(_null_row_hash())
        return len(merged)

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
            combined = dgram if combined is None else combined + dgram

        if combined is not None:
            self._distogram_cache[column] = combined

        return combined

    def estimate_cardinality(self, column) -> Optional[int]:
        """
        Estimate distinct values in column using K-Minimum Values (KMV).

        Uses min-k hashes from file entries if available, otherwise returns None.
        Merges min-k hashes across all files and applies KMV estimator formula.
        """
        K = 32
        HASH_RANGE = 2**64

        # Sketch buffers are positional against the load-time schema.
        field_id = self._sketch_index(column)
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

