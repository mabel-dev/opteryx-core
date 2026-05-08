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

                elif (
                    predicate.node_type == NodeType.BETWEEN
                    and predicate.left.node_type == NodeType.IDENTIFIER
                    and predicate.right.node_type == NodeType.LITERAL
                    and predicate.centre.node_type == NodeType.LITERAL
                ):
                    column_name = predicate.left.source_column
                    lower = predicate.right.value
                    upper = predicate.centre.value
                    if hasattr(lower, "item"):
                        lower = lower.item()
                    if hasattr(upper, "item"):
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

    def estimate_selectivity(self, predicate) -> float:
        """
        Estimate fraction of rows matching predicate.

        Uses histograms if available, otherwise falls back to NDV/null counts
        or textbook constants. Never raises on missing stats; degrades to the
        next tier and finally to a constant.

        Args:
            predicate: Predicate expression Node.

        Returns:
            Estimated selectivity in [0.0, 1.0].
        """
        return _clamp01(self._selectivity(predicate))

    def _selectivity(self, node) -> float:
        from opteryx.expression import NodeType

        if node is None:
            return 1.0

        node_type = getattr(node, "node_type", None)

        if node_type == NodeType.AND:
            return self._selectivity(node.left) * self._selectivity(node.right)

        if node_type == NodeType.OR:
            s1 = self._selectivity(node.left)
            s2 = self._selectivity(node.right)
            return 1.0 - (1.0 - s1) * (1.0 - s2)

        if node_type == NodeType.NOT:
            return 1.0 - self._selectivity(node.centre)

        if node_type == NodeType.UNARY_OPERATOR:
            op = node.value
            col_name = _identifier_name(node.centre)
            if col_name is None:
                return 1.0
            if op == "IsNull":
                return self._selectivity_is_null(col_name)
            if op == "IsNotNull":
                return 1.0 - self._selectivity_is_null(col_name)
            return 1.0

        if node_type == NodeType.BETWEEN:
            return self._selectivity_between(node)

        if node_type == NodeType.COMPARISON_OPERATOR:
            return self._selectivity_comparison(node)

        return 1.0

    def _selectivity_comparison(self, node) -> float:
        from opteryx.expression import NodeType

        op = node.value
        left, right = node.left, node.right

        # Normalise to (identifier, literal) and possibly invert op for swapped operands.
        col_name = _identifier_name(left)
        literal_node = right
        if col_name is None:
            col_name = _identifier_name(right)
            literal_node = left
            op = _SWAPPED_OP.get(op, op)
        if col_name is None:
            return 1.0

        if literal_node is None or literal_node.node_type != NodeType.LITERAL:
            return 1.0

        literal_value = _literal_scalar(literal_node)

        if op == "Eq":
            return self._selectivity_eq(col_name, literal_value)
        if op == "NotEq":
            return 1.0 - self._selectivity_eq(col_name, literal_value)
        if op in ("Lt", "LtEq", "Gt", "GtEq"):
            return self._selectivity_range(col_name, op, literal_value)
        if op == "InList":
            return self._selectivity_in(col_name, literal_value)
        if op == "NotInList":
            return 1.0 - self._selectivity_in(col_name, literal_value)
        if op in ("Like", "ILike", "RLike"):
            return _selectivity_like(literal_value)
        if op in ("NotLike", "NotILike", "NotRLike"):
            return 1.0 - _selectivity_like(literal_value)
        return 1.0

    # ---- predicate-kind helpers ----

    def _selectivity_eq(self, col_name: str, literal_value) -> float:
        # Histogram-based: bin density at literal.
        dgram = self.get_distogram(col_name)
        lit_f = _to_float(literal_value)
        if dgram is not None and lit_f is not None:
            total = float(dgram.count())
            if total > 0:
                bins_len = dgram.bin_count
                if bins_len > 0:
                    span = dgram.max - dgram.min
                    if span > 0 and bins_len > 1:
                        bin_width = span / bins_len
                        below = _count_up_to(dgram, lit_f - bin_width / 2.0)
                        above = _count_up_to(dgram, lit_f + bin_width / 2.0)
                        density = (above - below) / total
                        # Tighten the equality estimate using NDV when known.
                        ndv = self.estimate_cardinality(col_name)
                        if ndv and ndv > 0:
                            density = min(density, max(1.0 / ndv, density / max(ndv, 1)))
                        return _clamp01(density)
                    # Single-bin or zero-width histogram: literal lands inside iff it equals min.
                    if span == 0:
                        return 1.0 if lit_f == dgram.min else 0.0

        ndv = self.estimate_cardinality(col_name)
        if ndv and ndv > 0:
            return 1.0 / ndv

        return 0.1

    def _selectivity_range(self, col_name: str, op: str, literal_value) -> float:
        dgram = self.get_distogram(col_name)
        lit_f = _to_float(literal_value)
        if dgram is not None and lit_f is not None:
            total = float(dgram.count())
            if total > 0:
                below = _count_up_to(dgram, lit_f)
                fraction_below = below / total
                if op in ("Lt", "LtEq"):
                    return _clamp01(fraction_below)
                # Gt / GtEq
                return _clamp01(1.0 - fraction_below)

        return 0.25

    def _selectivity_in(self, col_name: str, literal_value) -> float:
        if not isinstance(literal_value, (list, tuple, set, frozenset)):
            return 0.1
        values = list(literal_value)
        n = len(values)
        if n == 0:
            return 0.0

        dgram = self.get_distogram(col_name)
        if dgram is not None:
            total = float(dgram.count())
            if total > 0 and dgram.bin_count > 0:
                span = dgram.max - dgram.min
                if span > 0:
                    bin_width = span / dgram.bin_count
                    accumulated = 0.0
                    coerced_any = False
                    for v in values:
                        f = _to_float(v)
                        if f is None:
                            continue
                        coerced_any = True
                        below = _count_up_to(dgram, f - bin_width / 2.0)
                        above = _count_up_to(dgram, f + bin_width / 2.0)
                        accumulated += (above - below) / total
                    if coerced_any:
                        return _clamp01(accumulated)

        ndv = self.estimate_cardinality(col_name)
        if ndv and ndv > 0:
            return min(1.0, n / ndv)

        return min(1.0, n * 0.1)

    def _selectivity_between(self, node) -> float:
        from opteryx.expression import NodeType

        col_name = _identifier_name(node.left)
        if col_name is None:
            return 1.0

        # Mirror prune_files: right is one bound, centre is the other. The
        # ordering is sometimes (lower=right, upper=centre); be tolerant and
        # sort the two bounds rather than assume a fixed pairing.
        right = node.right
        centre = node.centre
        if right is None or centre is None:
            return 1.0
        if right.node_type != NodeType.LITERAL or centre.node_type != NodeType.LITERAL:
            return 1.0

        a = _to_float(_literal_scalar(right))
        b = _to_float(_literal_scalar(centre))

        dgram = self.get_distogram(col_name)
        if dgram is not None and a is not None and b is not None:
            total = float(dgram.count())
            if total > 0:
                lo, hi = (a, b) if a <= b else (b, a)
                fraction = (_count_up_to(dgram, hi) - _count_up_to(dgram, lo)) / total
                return _clamp01(fraction)

        return 0.25

    def _selectivity_is_null(self, col_name: str) -> float:
        # estimate_null_fraction reports 0.0 even when no file carries null
        # counts at all; treat "no file knows" as missing stats and fall back.
        if not any(f.null_value_counts for f in self.files):
            return 0.05
        nf = self.estimate_null_fraction(col_name)
        if nf is None:
            return 0.05
        return _clamp01(nf)

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
            if not file.null_value_counts or field_id not in file.null_value_counts:
                return None
            total += file.null_value_counts[field_id]
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
            "avg_file_size": (
                self.get_total_size() / self.get_file_count() if self.get_file_count() > 0 else 0
            ),
            "files_with_bounds": sum(1 for f in self.files if f.lower_bounds or f.upper_bounds),
            "files_with_k_hashes": sum(1 for f in self.files if f.min_k_hashes),
            "files_with_histograms": sum(1 for f in self.files if f.histogram_counts),
        }


# ================================================================
# Selectivity helpers (module-level; pure)
# ================================================================


_SWAPPED_OP = {
    "Lt": "Gt",
    "LtEq": "GtEq",
    "Gt": "Lt",
    "GtEq": "LtEq",
    "Eq": "Eq",
    "NotEq": "NotEq",
}


def _clamp01(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return float(value)


def _identifier_name(node) -> Optional[str]:
    from opteryx.expression import NodeType

    if node is None or getattr(node, "node_type", None) != NodeType.IDENTIFIER:
        return None
    name = getattr(node, "source_column", None)
    if name is None:
        name = getattr(node, "value", None)
    if isinstance(name, bytes):
        try:
            name = name.decode("utf-8")
        except UnicodeDecodeError:
            return None
    return name if isinstance(name, str) else None


def _literal_scalar(node):
    value = getattr(node, "value", None)
    if hasattr(value, "item") and not isinstance(value, (list, tuple, set, frozenset)):
        try:
            return value.item()
        except (ValueError, TypeError):
            return value
    return value


def _to_float(value) -> Optional[float]:
    """Coerce a literal to float for histogram-domain comparisons.

    Returns None if the value is not numerically meaningful — caller should
    fall back to the next stats tier rather than guess.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _count_up_to(dgram, value: float) -> float:
    from opteryx.third_party.maki_nage.distogram import count_up_to

    return count_up_to(dgram, value)


def _selectivity_like(literal_value) -> float:
    """LIKE selectivity heuristic: prefix patterns are tighter than substring."""
    if isinstance(literal_value, bytes):
        try:
            literal_value = literal_value.decode("utf-8")
        except UnicodeDecodeError:
            return 0.1
    if not isinstance(literal_value, str):
        return 0.1
    if (
        literal_value.endswith("%")
        and "%" not in literal_value[:-1]
        and "_" not in literal_value
    ):
        return 0.25
    return 0.1
