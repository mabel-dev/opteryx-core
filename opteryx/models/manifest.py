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

from opteryx.exceptions import md_code
from opteryx.models.file_entry import FileEntry
from opteryx.third_party.maki_nage.distogram import Distogram, merge
from opteryx.types.logical_type import LogicalCategory
from opteryx.types.schema import RelationSchema

# INT64_MIN, the codebase-wide "this producer computed no real bound for this
# column" sentinel, smuggled through as a plain int rather than None.
# RelationStatistics.update_lower/update_upper
# (compiled/structures/relation_statistics.pyx) already reject exactly this
# value - value-exact, NOT "any negative", because a signed column's genuine
# ordinal key is routinely negative and pruning on those is correct.
#
# It reaches a FileEntry from the catalog's manifest builder, which leaves
# col_min/col_max at NULL_FLAG = -(1<<63) for any column whose category is
# outside its compressible-categories set - which today includes EVERY
# unsigned width (UINT8/16/32/64), since its logical-type table maps no
# "uintN" name. Treated as a real bound, `col = <anything>` prunes on
# `v < -2**63 or v > -2**63` -> True and EVERY file is dropped: zero rows
# returned for any equality/range predicate over an unsigned column.
#
# Skipping is safe in one direction only, which is the direction that matters:
# it can only ever KEEP a file that might have been pruned, never drop one that
# should be read. An INT64 column genuinely holding INT64_MIN as its minimum
# therefore loses pruning on that file - correct answer, one avoidable read.
_NO_BOUND_SENTINEL = -(1 << 63)


def _is_real_bound(min_value: Any, max_value: Any) -> bool:
    """True when both bounds are usable for pruning.

    None is the other "not computed" marker (ANALYZE's `min_max = None` path,
    see _analyze_one_file); `_NO_BOUND_SENTINEL` is the same statement made by
    a producer that had no None to hand. Both mean the same thing here.
    """
    if min_value is None or max_value is None:
        return False
    return min_value != _NO_BOUND_SENTINEL and max_value != _NO_BOUND_SENTINEL


# Physical types whose stored representation is a bare temporal integer whose
# meaning depends on a domain: DATE32 stores days, TIMESTAMP64 stores unit-scaled
# ticks (s/ms/us/ns), TIME32/TIME64 store time-of-day ticks. File bounds hold the
# column's raw integer, so two temporal operands are only order-comparable when
# they share both the physical type and (where applicable) the unit.
# Lazily populated from DrakenType on first use - importing it at module scope
# would pull the type system in ahead of everything that imports Manifest.
_TEMPORAL_PHYSICALS = None


def _temporal_domain_mismatch(column_type: Any, literal_type: Any) -> bool:
    """True when a column and a predicate literal are both temporal but occupy
    different raw integer domains - DATE32 (days) vs TIMESTAMP64 (microseconds),
    or two TIMESTAMP64s at different units.

    `_comparable_literal` cannot catch this: both sides arrive as plain ints, so
    it waves them through. The result is not a near miss but a total one - a DATE
    column against a `2025-01-01T00:00:00Z` literal compares ~20_000 days against
    ~1.7e15 microseconds, `max_ < v` holds for EVERY file, every file is pruned,
    and the query returns zero rows with no error. The asymmetry is the tell: the
    lower-bound operators (Lt/LtEq) prune nothing and answer correctly, while
    Gt/GtEq/Eq drop the whole table. Reverse the pairing (TIMESTAMP column, DATE
    literal) and so does the failure - Lt/LtEq return nothing instead.

    Declining the pushdown costs one avoidable scan; the row-level filter
    promotes both sides to a common unit and produces the right answer. This is
    the same guard, for the same reason, that
    `connectors.parquet_io.predicates._temporal_domain_mismatch` applies one
    layer down at row-group granularity - the two layers prune independently, so
    a fix in one does nothing for the other.
    """
    global _TEMPORAL_PHYSICALS
    if _TEMPORAL_PHYSICALS is None:
        from opteryx.types.logical_type import DrakenType

        _TEMPORAL_PHYSICALS = frozenset(
            {
                DrakenType.DATE32,
                DrakenType.TIMESTAMP64,
                DrakenType.TIME32,
                DrakenType.TIME64,
            }
        )

    if column_type is None or literal_type is None:
        return False

    column_physical = getattr(column_type, "physical", None)
    literal_physical = getattr(literal_type, "physical", None)
    if column_physical not in _TEMPORAL_PHYSICALS or literal_physical not in _TEMPORAL_PHYSICALS:
        # Not both temporal, so not a temporal-domain question. A temporal column
        # against a non-temporal literal (`date_col >= 100`) is a type error the
        # binder has already rejected before pruning ever runs.
        return False
    if column_physical != literal_physical:
        return True  # e.g. DATE32 (days) vs TIMESTAMP64 (ticks)

    # Same physical type: only the unit can still differ (TIMESTAMP64/TIME*).
    column_logical = getattr(column_type, "logical", None)
    literal_logical = getattr(literal_type, "logical", None)
    column_unit = column_logical.unit if column_logical is not None else None
    literal_unit = literal_logical.unit if literal_logical is not None else None
    return column_unit != literal_unit


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
        # by original file position. Pruning (copy-on-write — prune_files/
        # prune_files_for_topn/subset return a NEW Manifest, see subset's
        # docstring) shrinks the file list, so the clone tracks the surviving
        # original row indices to keep native reductions aligned with the
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

    def get_record_count(self) -> Optional[int]:
        """Total record count across all files, or None when it is UNKNOWN.

        Unknown is not zero. A single file whose `record_count is None` makes the
        WHOLE total unknown: consumers answer COUNT(*) from this number and delete
        LIMIT nodes against it, so reporting a partial sum as a total is a wrong
        answer rather than an approximation. Callers must test `is not None`, not
        truthiness - a real, empty relation legitimately totals 0.
        """
        total = 0
        for f in self.files:
            if f.record_count is None:
                return None
            total += f.record_count
        return total

    def get_file_count(self) -> int:
        """Number of files in manifest."""
        return len(self.files)

    def get_row_group_count(self) -> Optional[int]:
        """Total row groups across all files, or None when it is UNKNOWN.

        Same doctrine as get_record_count: one file that does not know makes the
        whole total unknown, because a partial sum reported as a total is a wrong
        number rather than an approximate one. Only producers that read file
        footers populate it.
        """
        total = 0
        for f in self.files:
            if f.row_group_count is None:
                return None
            total += f.row_group_count
        return total

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
        except (TypeError, ValueError):
            # TypeError is the same statement made by the native kernel rather
            # than by ColumnType: an INT64 column's ordinalize rejects a float
            # literal with "'float' object cannot be interpreted as an integer".
            # Pruning is an optimisation, so "can't ordinalize this" must cost a
            # scan, never raise out of the optimizer and fail the whole query.
            return None

    def _predicate_domain_mismatch(self, predicate) -> bool:
        """True when `predicate` compares a column against a literal that is not
        order-comparable with the column's raw stored bounds, so pruning on it
        would be a wrong answer rather than a wrong guess.

        Covers the same two predicate shapes `prune_files` knows how to prune -
        `column <op> literal` and `column BETWEEN literal AND literal` - and
        answers False for anything else, since a shape that is never pruned needs
        no guard. See `_temporal_domain_mismatch` for what makes the two sides
        incomparable and why silence here is a zero-row result, not a slow one.

        The column type is resolved by NAME against the live schema and falls
        back to the predicate node's own bound `schema_column`, which survives
        projection pushdown having pruned the column out of `self.schema`.
        """
        from opteryx.expression import NodeType

        if predicate.node_type == NodeType.COMPARISON_OPERATOR:
            if (
                predicate.left.node_type != NodeType.IDENTIFIER
                or predicate.right.node_type != NodeType.LITERAL
            ):
                return False
            literal_types = (getattr(predicate.right, "type", None),)
        elif predicate.node_type == NodeType.BETWEEN:
            if (
                predicate.left.node_type != NodeType.IDENTIFIER
                or predicate.right.node_type != NodeType.LITERAL
                or predicate.centre.node_type != NodeType.LITERAL
            ):
                return False
            literal_types = (
                getattr(predicate.right, "type", None),
                getattr(predicate.centre, "type", None),
            )
        else:
            return False

        column_type = self._column_type(predicate.left.source_column)
        if column_type is None:
            schema_column = getattr(predicate.left, "schema_column", None)
            column_type = getattr(schema_column, "column_type", None)

        return any(
            _temporal_domain_mismatch(column_type, literal_type) for literal_type in literal_types
        )

    def _bounds_may_omit_nan(self, column_name: str) -> bool:
        """True when a NaN row of *column_name* could sit OUTSIDE this manifest's
        recorded bounds — a property of the column and of where the bounds came
        from, independent of any predicate.

        The two provenances differ — measured, not assumed:

        * `bounds_are_ordinal` (ANALYZE / skene): bounds are
          `Vector.ordinalize()` int64 keys, and ordinalize maps a canonical quiet
          NaN to 9221120237041090560 — strictly above +inf's 9218868437227405312
          (draken/ops/ordinalize.h says so, and it checks out at the boundary).
          The max ordinal therefore DOES cover a NaN row, the bounds are a real
          bound, and every op stays prunable.
        * otherwise (CTAS via `write_parquet_with_bounds`): bounds are rugo's
          parquet min/max, which skips NaN to spec
          (rugo/src/parquet/_parquet_writer.hpp). Same hole as the row-group
          footer path — see `connectors/parquet_io/predicates.py`, which states
          the op list `_nan_invisible_to_bounds` shares.

        A column whose type cannot be resolved is treated as possibly-float.
        """
        if self.bounds_are_ordinal:
            return False
        column_type = self._column_type(column_name)
        return column_type is None or column_type.category == LogicalCategory.FLOAT

    def _nan_invisible_to_bounds(self, column_name: str, op: str) -> bool:
        """True when *op* on *column_name* cannot be decided from this manifest's
        bounds, because a NaN outside them would satisfy it.

        The op list is imported rather than restated: which prune tests rest on
        the upper bound is the same question for a file bound as for a row-group
        bound, and two copies would be two things to keep in step. Lazy, to keep
        this model layer free of a module-level connector import.
        """
        from opteryx.connectors.parquet_io.predicates import _NAN_UNSOUND_OPS

        return op in _NAN_UNSOUND_OPS and self._bounds_may_omit_nan(column_name)

    def _with_files(self, kept_files: List[FileEntry], kept_rows: List[int]) -> "Manifest":
        """New Manifest over `kept_files`, sharing everything file-set-independent
        with this one by reference (schema, sketch vectors, load-time column
        snapshot, field-id mappings) — the copy-on-write half of the pruning
        contract, see `subset`. `kept_rows` is the surviving ORIGINAL vector-row
        index per kept file (the `_live_rows` invariant). The per-column caches
        start empty: they memoise answers derived from the file set, which is
        exactly what changed.
        """
        clone = Manifest.__new__(Manifest)
        clone.files = kept_files
        clone.schema = self.schema
        clone._min_k_vector = self._min_k_vector
        clone._histogram_vector = self._histogram_vector
        clone._char_class_vector = self._char_class_vector
        clone.bounds_are_ordinal = self.bounds_are_ordinal
        clone._live_rows = kept_rows
        clone._load_time_columns = self._load_time_columns
        clone._field_id_to_name = self._field_id_to_name
        clone._name_to_field_id = self._name_to_field_id
        clone._column_bounds_cache = {}
        clone._distogram_cache = {}
        return clone

    def subset(self, positions: List[int]) -> "Manifest":
        """New Manifest keeping only the files at `positions` (indexes into the
        CURRENT `self.files`, in the order given). `self` is left untouched.

        Pruning is copy-on-write by contract: a Manifest attached to a plan
        node is immutable for the life of the plan, because the optimizer's
        scan-statistics cache (statistics_refresh._scan_stats) keys its
        memoised base statistics by `id(node.manifest)`. An in-place prune
        keeps the id and re-serves PRE-pruning row counts and bounds to every
        later refresh; assigning the returned object to `node.manifest` makes
        the cache miss and recompute honestly.
        """
        kept_files = [self.files[position] for position in positions]
        kept_rows = [
            position if self._live_rows is None else self._live_rows[position]
            for position in positions
        ]
        return self._with_files(kept_files, kept_rows)

    # Op codes for `ordinal_zone_map_terms`, mirrored in
    # src/cpp/engine/native_skene_scan_source.hpp's SkeneZoneTerm. Small ints
    # rather than strings because the consumer is a C++ claim builder that must
    # not carry a string table, let alone this module's op vocabulary.
    ZONE_OP_EQ = 0
    ZONE_OP_GT = 1
    ZONE_OP_GTEQ = 2
    ZONE_OP_LT = 3
    ZONE_OP_LTEQ = 4

    def ordinal_zone_map_terms(self, predicates: List) -> List[tuple]:
        """`(column_name, op_code, ordinal)` terms a ROW-GROUP zone map can be
        tested against, for the conjuncts of `predicates` that are safely prunable.

        This is `prune_files`' reasoning, factored so a reader that prunes at a
        FINER grain than the file can reuse it instead of restating it. Everything
        that makes a bounds comparison sound or unsound — the ordinal dialect
        (`_ordinalize_literal`), the temporal-domain mismatch guard
        (`_predicate_domain_mismatch`), the NaN-invisibility rule
        (`_nan_invisible_to_bounds`) — is decided HERE, in the one place that knows
        the column's type. What crosses to the consumer is three numbers and a
        name, and the consumer does arithmetic. A second site deciding any of the
        above would be a second dialect, which is the exact failure
        `bounds_are_ordinal` exists to prevent (see [[ordinalize-vs-to-int]] in the
        __init__ docstring: two "value -> int64" functions exist and agree only for
        plain int64).

        The terms are a CONJUNCTION: every one of them must be satisfiable for a
        row group to be worth reading, so a consumer may skip on any single term
        proving emptiness. Conjuncts this cannot express are simply absent — a
        missing term costs a read, never an answer.

        Deliberately excluded:
          * `NotEq`. It prunes only on `min == max == v`, which reads ordinal
            equality as value uniformity. String ordinals pack the first 8 content
            bytes and are monotonic but NOT injective, so that inference is false
            for exactly the type where a row group is most likely to be uniform.
            `prune_files` handles it with a type test; here the payoff is too small
            to carry the rule into another consumer.
          * Anything that is not `column <op> literal` or
            `column BETWEEN literal AND literal` — the same two shapes
            `prune_files` knows, since this is the same reasoning.

        Returns [] when the bounds are not ordinal: the caller's statistics are,
        so a non-ordinal literal is not comparable with them at all.
        """
        from opteryx.expression import NodeType
        from opteryx.planner.optimizer.strategies.split_conjunctive_predicates import (
            _inner_split,
        )

        if not self.bounds_are_ordinal:
            return []

        comparisons = {
            "Eq": self.ZONE_OP_EQ,
            "Gt": self.ZONE_OP_GT,
            "GtEq": self.ZONE_OP_GTEQ,
            "Lt": self.ZONE_OP_LT,
            "LtEq": self.ZONE_OP_LTEQ,
        }
        terms: List[tuple] = []

        def _literal(node):
            value = node.value
            # numpy/pyarrow scalars reach predicates as 0-d wrappers; ordinalize
            # wants the python value, same unwrap prune_files does.
            return value.item() if getattr(value, "item", None) is not None else value

        def _emit(column_name, op_code, raw_value):
            ordinal = self._ordinalize_literal(column_name, raw_value)
            # None is "this type has no scalar ordinalize kernel", not zero.
            if ordinal is None or not isinstance(ordinal, int):
                return
            terms.append((column_name, op_code, ordinal))

        # `predicates` is a list of separately-pushed conjuncts, but any one of
        # them can itself be an AND tree or a DNF node (this engine's n-ary AND)
        # after PredicateOrderingStrategy. `_inner_split` is the ONE splitter that
        # knows both shapes — never write a second one.
        conjuncts = []
        for predicate in predicates or []:
            if predicate is not None:
                conjuncts.extend(_inner_split(predicate))

        for conjunct in conjuncts:
            if self._predicate_domain_mismatch(conjunct):
                continue
            if (
                conjunct.node_type == NodeType.COMPARISON_OPERATOR
                and conjunct.value in comparisons
                and conjunct.left.node_type == NodeType.IDENTIFIER
                and conjunct.right.node_type == NodeType.LITERAL
            ):
                column_name = conjunct.left.source_column
                if self._nan_invisible_to_bounds(column_name, conjunct.value):
                    continue
                _emit(column_name, comparisons[conjunct.value], _literal(conjunct.right))
            elif (
                conjunct.node_type == NodeType.BETWEEN
                and conjunct.left.node_type == NodeType.IDENTIFIER
                and conjunct.right.node_type == NodeType.LITERAL
                and conjunct.centre.node_type == NodeType.LITERAL
            ):
                # BETWEEN is two conjuncts and they fail differently under NaN —
                # the lower half is the GtEq test, the upper half the LtEq test —
                # so each half asks the NaN question under its OWN op, exactly as
                # prune_files does rather than gating both on one answer.
                column_name = conjunct.left.source_column
                if not self._nan_invisible_to_bounds(column_name, "GtEq"):
                    _emit(column_name, self.ZONE_OP_GTEQ, _literal(conjunct.right))
                if not self._nan_invisible_to_bounds(column_name, "LtEq"):
                    _emit(column_name, self.ZONE_OP_LTEQ, _literal(conjunct.centre))

        return terms

    def prune_files(self, predicates: List) -> "Manifest":
        """
        Filter files based on predicates using min/max bounds.

        Called by optimizer to determine which files to read.

        This is NOT called at execution time - the optimizer makes
        this decision and execution just follows it.

        Copy-on-write: `self` is never modified. Returns `self` when no file
        was pruned, otherwise a new Manifest over the surviving files (see
        `subset` for why the caller must re-assign `node.manifest`).

        Args:
            predicates: List of predicate Node objects to evaluate

        Returns:
            The Manifest describing the surviving file set.
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

        # Whether a literal is order-comparable with a column's raw stored bounds
        # depends only on the two TYPES, so it is settled once per predicate here
        # rather than re-derived for every file below. A predicate that fails the
        # check is dropped entirely: it must not reach the bounds comparison, and
        # it must not reach `_membership_keep_masks` either, which eliminates
        # files on raw integer equality and would drop them on the same
        # cross-domain compare.
        predicates = [p for p in (predicates or []) if not self._predicate_domain_mismatch(p)]

        if not predicates:
            # No predicates (or none left that can be safely compared) = no pruning
            return self

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

                    # A NaN this file's bounds cannot see would satisfy the
                    # predicate, so the bounds cannot disprove it.
                    if self._nan_invisible_to_bounds(column_name, predicate.value):
                        continue

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

                    if _is_real_bound(min_value, max_value):
                        compare_value = literal_value
                        if self.bounds_are_ordinal and predicate.value == "NotEq":
                            # String ordinals pack the first 8 content bytes and
                            # are MONOTONIC BUT NOT INJECTIVE (skene format.h,
                            # same dialect as ANALYZE bounds): two different
                            # values can share one ordinal. The NotEq handler
                            # prunes on `min == max == v`, which reads ordinal
                            # equality as value uniformity — false for strings
                            # ('abcdefgh1' vs 'abcdefgh2' collide), so a file
                            # holding the OTHER value would be wrongly dropped.
                            # Range handlers stay safe (monotonicity is enough);
                            # only NotEq needs injectivity, so only NotEq skips.
                            column_type = self._column_type(column_name)
                            if column_type is None or column_type.category in (
                                LogicalCategory.VARCHAR,
                                LogicalCategory.VARBINARY,
                            ):
                                continue
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

                    if _is_real_bound(min_value, max_value):
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
                        # BETWEEN is two conjuncts and they fail differently. The
                        # `max_value < lower` half is the GtEq test — a NaN is
                        # >= every bound, so it goes when the bounds can't see a
                        # NaN. The `min_value > upper` half is the LtEq test and
                        # stays sound, because a NaN is never <= a non-NaN bound.
                        if not self._bounds_may_omit_nan(column_name):
                            if max_value < comparable_lower:
                                skip_file = True
                                break
                        if min_value > comparable_upper:
                            skip_file = True
                            break

            if not skip_file:
                kept_files.append(file_entry)
                kept_rows.append(original_row)

        if len(kept_files) == len(self.files):
            return self
        return self._with_files(kept_files, kept_rows)

    def prune_files_for_topn(self, column_name: str, descending: bool, limit: int) -> "Manifest":
        """Drop files that provably cannot hold any of the top-`limit` rows of
        `column_name` for a single-column ``ORDER BY column_name [ASC|DESC]
        LIMIT limit`` query.

        SAFE ONLY when `column_name` has zero NULLs across the whole manifest.
        The caller (TopNManifestPruningStrategy) MUST have already verified
        `get_total_null_count(column_name) == 0` before calling this - this
        method does not re-check, so a caller mistake here is a silent wrong
        answer, not a defensive no-op. With that precondition, record_count IS
        the non-null row count for every file, so no separate null-aware
        accounting is needed.

        Copy-on-write, same contract as `prune_files`: `self` is never
        modified; returns `self` when nothing is pruned, otherwise a new
        Manifest over the surviving files (see `subset`).

        Algorithm: rank the files that carry a (lower_bound, upper_bound) for
        this column by the value nearest the query's "best" end - max
        descending for DESC, min ascending for ASC - and accumulate
        record_count across that ranking until it reaches `limit`. The
        threshold is the worst-case (min for DESC, max for ASC) bound seen
        across every file folded into that accumulation so far: because we
        only know each file's range, not its distribution, the `limit`
        guaranteed rows could - in the worst case - all sit down at the
        lowest bound among the files needed to reach `limit`, not just the
        bound of the file that happened to cross the threshold last. Any file
        (including ones excluded from the ranking below) whose own bound
        cannot reach that threshold is provably outside the top-`limit` and
        is dropped.

        Files with no bound for this column are always kept, and are excluded
        from the ranking/accumulation - no stats means no evidence to prune
        on, and such a file must not be allowed to tighten the threshold
        applied to files that DO have stats.
        """
        field_id = self._resolve_field_id(column_name)
        if field_id is None or limit is None or limit <= 0:
            return self

        # NaN breaks this in BOTH directions when the bounds cannot see it (the
        # non-ordinal, rugo-min/max provenance — see `_nan_invisible_to_bounds`),
        # and the zero-NULL precondition above does NOT cover it: a NaN is a
        # value with its validity bit set, so it is counted in `record_count` and
        # contributes no null.
        #   DESC — NaN ranks ABOVE every value, so the NaN rows ARE the top-n,
        #          but they are missing from every file's `hi`. A file holding
        #          them ranks low and is dropped as provably-outside.
        #   ASC  — NaN rows can never be in the top-n, yet `record_count` counts
        #          them toward `limit`, so accumulation reaches the limit on rows
        #          that do not qualify and sets a threshold that is too tight.
        # Neither is recoverable from bounds that omit the value, so the whole
        # prune stands down for such a column.
        if self._bounds_may_omit_nan(column_name):
            return self

        # position in self.files -> (lower_bound, upper_bound)
        bounds_by_position: Dict[int, Tuple[Any, Any]] = {}
        for position, file_entry in enumerate(self.files):
            if not file_entry.lower_bounds or not file_entry.upper_bounds:
                continue
            lo = file_entry.lower_bounds.get(field_id)
            hi = file_entry.upper_bounds.get(field_id)
            if not _is_real_bound(lo, hi):
                # `_NO_BOUND_SENTINEL` is this docstring's "no bound" case
                # stated as an int instead of None, and admitting it breaks
                # BOTH directions - measured, not assumed:
                #   ASC  - it sorts FIRST (lo == INT64_MIN), so it is the first
                #          file accumulated and its own INT64_MIN `hi` becomes
                #          the threshold. Every real file then has lo > that
                #          and EVERY ONE is dropped.
                #   DESC - it sorts last, so it never reaches accumulation, but
                #          its INT64_MIN `hi` is below any real threshold and
                #          the file itself is dropped - rows that could be in
                #          the top-n, discarded on no evidence.
                continue
            bounds_by_position[position] = (lo, hi)

        if not bounds_by_position:
            # No file carries stats for this column - nothing safe to prune.
            return self

        ranked = sorted(
            bounds_by_position.items(),
            key=lambda item: item[1][1] if descending else item[1][0],
            reverse=descending,
        )

        accumulated = 0
        threshold = None
        for position, (lo, hi) in ranked:
            file_rows = self.files[position].record_count
            # An unknown row count contributes NO rows toward `limit` - it is not
            # "zero, therefore empty", it is no evidence. The file still tightens
            # the threshold below, which can only widen what is kept (min for
            # DESC, max for ASC), never prune on rows we cannot count.
            accumulated += file_rows if file_rows is not None else 0
            candidate = lo if descending else hi
            if threshold is None:
                threshold = candidate
            elif descending:
                threshold = min(threshold, candidate)
            else:
                threshold = max(threshold, candidate)
            if accumulated >= limit:
                break

        kept_files = []
        kept_rows = []
        for position, file_entry in enumerate(self.files):
            original_row = position if self._live_rows is None else self._live_rows[position]
            bounds = bounds_by_position.get(position)
            if bounds is not None:
                lo, hi = bounds
                if descending:
                    if hi < threshold:
                        continue  # provably below the guaranteed top-`limit` floor
                elif lo > threshold:
                    continue  # provably above the guaranteed top-`limit` ceiling
            kept_files.append(file_entry)
            kept_rows.append(original_row)

        if len(kept_files) == len(self.files):
            return self
        return self._with_files(kept_files, kept_rows)

    # ================================================================
    # File Accessors
    # ================================================================

    def get_file_paths(self) -> List[str]:
        """Get file paths from the manifest."""
        return [file.file_path for file in self.files]

    def sketch_vectors_by_file(
        self,
    ) -> Tuple[Optional[Dict[str, List]], Optional[Dict[str, List]], Optional[Dict[str, List]]]:
        """Box the native min_k/histogram/char_class vectors into per-file lists.

        Returns (sketches, histograms, char_classes), each either None (this
        Manifest holds no such vector) or a ``{file_path: positional-by-field-id
        list}`` dict — the shape manifest_io.file_entries_to_manifest_morsel's
        sketches/histograms/char_classes parameters expect. For SHOW MANIFEST
        only: an admin/diagnostic path where boxing every file is fine, unlike
        the planner's hot-path kernels which reduce the native vectors directly
        (see __init__'s min_k_vector/histogram_vector/char_class_vector docs).
        Honors _live_rows so this stays correct if ever called after
        prune_files has shrunk self.files.
        """

        def _box(vector) -> Optional[Dict[str, List]]:
            if vector is None:
                return None
            rows = vector.to_pylist()
            if self._live_rows is not None:
                return {
                    fe.file_path: rows[self._live_rows[i]] for i, fe in enumerate(self.files)
                }
            return {fe.file_path: rows[i] for i, fe in enumerate(self.files)}

        return (
            _box(self._min_k_vector),
            _box(self._histogram_vector),
            _box(self._char_class_vector),
        )

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
        row_count_is_metric = total_rows is not None
        if total_rows is None:
            # RelationStatistics.row_count is a real int and selectivity does
            # arithmetic on it, so an unknown count needs a stand-in. Use the same
            # no-signal constant statistics_refresh substitutes, so every estimate
            # in the planner sits on one scale rather than two.
            from opteryx.planner.optimizer.statistics_refresh import _UNKNOWN_ROW_COUNT

            total_rows = _UNKNOWN_ROW_COUNT
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
            distinct_count = self.estimate_cardinality(col_name)
            if distinct_count is None:
                # Range-derived fallback for un-ANALYZE'd relations — same
                # costing-only substitution statistics_refresh makes.
                distinct_count = self.estimate_range_cardinality(col_name)
            columns[identity] = ColumnStatistics(
                column_name=col_name,
                data_type=str(getattr(col, "type", "")),
                distinct_count=distinct_count,
                value_range=ColumnRange(),
                histogram=self.get_distogram(col_name),
                null_fraction=null_fraction,
                class_proportions=char_class_stats[0] if char_class_stats else None,
                avg_length=char_class_stats[1] if char_class_stats else None,
                ordinal_bounds=self.get_ordinal_bounds(col_name),
                length_bounds=self.get_length_bounds(col_name),
            )
        if row_count_is_metric:
            return RelationStatistics(columns=columns, row_count_metric=total_rows)
        return RelationStatistics(columns=columns, row_count_estimate=total_rows)

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
            # The bins the producer says it wrote must be the bins that are
            # here. load_counts_i64 spaces the bin centres across
            # (col_min, col_max) by the slice length, so reading N counts as if
            # they were M puts every boundary in the wrong place — a wrong
            # selectivity, not a missing one. Fail loud rather than coerce.
            stored_bins = file_entry.histogram_bins
            if stored_bins is not None and stored_bins != (end - start):
                raise ValueError(
                    f"Histogram bin count mismatch for {md_code(file_entry.file_path)}: the "
                    f"manifest records {stored_bins} bins but the histogram holds "
                    f"{end - start}."
                )
            dgram = load_counts_i64(counts[start:end], float(col_min), float(col_max))
            combined = dgram if combined is None else merge(combined, dgram)
        return combined

    def _exact_cardinality_from_footers(self, col_name: str) -> Optional[int]:
        """Relation-level EXACT distinct count from per-file footer NDV, or None.

        skene's `kStatNdvExact` means value ordering deduplicated the column, so
        the stored count IS the file's distinct non-null count — a BOUND, not an
        estimate. That per-FILE bound only survives to the RELATION when the
        files' value sets cannot overlap, so this returns a number in exactly two
        provable cases and None otherwise:

          * one file — its count is trivially the relation's;
          * several files whose ordinal ranges are pairwise STRICTLY disjoint,
            in which case the counts add with no double counting.

        Disjointness on ordinals is sound in the direction used here: ordinalize
        is monotonic, so `ord(a) < ord(b)` implies `a < b`. It is not injective
        (string ordinals collide on a shared 8-byte prefix), so a touching pair
        (`hi == lo`) may or may not share a value and is NOT provable — hence the
        strict `<`. Non-injectivity can only cost us a provable case, never
        manufacture one.

        ⚠️ Counts distinct NON-NULL values, matching skene's definition. The KMV
        path this backs up is built the same way, and both consumers
        (distinct_pushdown, hash_map_variant) use the number as a perf signal, so
        a NULL is not worth a +1 fudge in either direction.
        """
        field_id = self._resolve_field_id(col_name)
        if field_id is None or not self.files:
            return None

        total = 0
        intervals = []
        for file in self.files:
            counted = (file.distinct_value_counts or {}).get(field_id)
            # None is not tracked; not-exact is a sketch, which cannot be summed
            # into a bound however many files agree.
            if counted is None or not counted[1]:
                return None
            total += counted[0]
            intervals.append(
                ((file.lower_bounds or {}).get(field_id), (file.upper_bounds or {}).get(field_id))
            )

        if len(self.files) == 1:
            return total
        # Comparing bounds across files requires them to speak ONE dialect;
        # ordinal-vs-decoded would be a meaningless inequality.
        if not self.bounds_are_ordinal:
            return None
        if any(low is None or high is None for low, high in intervals):
            return None
        intervals.sort()
        for lower, upper in zip(intervals, intervals[1:]):
            if not lower[1] < upper[0]:
                return None
        return total

    def _cardinality_from_sketches(self, col_name: str) -> Optional[int]:
        """Relation NDV by unioning the live files' skene KMV sketches, or None.

        This is the answer a stored sketch exists to give. A per-file distinct
        COUNT cannot be merged — two files reporting 250,000 each may hold
        250,000 between them or 500,000, and min/max cannot tell those apart
        (measured on TPC-H `l_comment`: every row group shares an identical min
        ordinal while the value sets are 91% disjoint, so a range-based rule
        lands 17.6x low). The K smallest hashes CAN be merged, exactly, so the
        overlap is measured rather than guessed.

        EXACT below K, which is the regime most columns live in: a union holding
        fewer than K hashes holds every distinct value the relation has.

        Requires EVERY live file to carry a sketch — a union missing one file's
        hashes undercounts, and nothing distinguishes that from a genuinely
        smaller column.
        """
        field_id = self._resolve_field_id(col_name)
        if field_id is None or not self.files:
            return None

        sketches = []
        for file in self.files:
            sketch = (file.distinct_sketches or {}).get(field_id)
            if sketch is None:
                return None
            sketches.append(sketch)

        from opteryx.utils.kmv import estimate_from_min_k, merge_min_k

        count, exact = estimate_from_min_k(merge_min_k(sketches))

        if not exact:
            # Floor the estimate with what the footers PROVE. A file whose own
            # distinct count is exact holds that many distinct values, and a
            # subset cannot hold more than the whole — so the largest exact
            # per-file count is a hard lower bound on the relation, and the K=32
            # estimator's ~18% error can land below it (measured on l_shipdate:
            # 2002 estimated against 2526 proven in one file).
            #
            # MAX, never SUM: two files' exact counts may describe the same
            # values, which is the entire reason the sketch exists.
            floor = 0
            for file in self.files:
                proven = (file.distinct_floors or {}).get(field_id)
                if proven:
                    floor = max(floor, proven)
                counted = (file.distinct_value_counts or {}).get(field_id)
                if counted is not None and counted[1]:
                    floor = max(floor, counted[0])
            count = max(count, floor)

        # A distinct count cannot exceed the rows it was counted over. The
        # estimator can overshoot above K; below K it is exact and this is a
        # no-op.
        total_rows = self.get_record_count()
        if total_rows is not None and total_rows > 0:
            count = min(count, int(total_rows))
        return max(1, count) if count else count

    def estimate_cardinality(self, column) -> Optional[int]:
        """
        Estimate distinct values in column using K-Minimum Values (KMV).

        Merges each file's min-k sketch for the column and applies the KMV
        estimator (exact count when the merged sketch is under K). Reduced
        natively over the whole-column sketch vector — no boxing. Returns None
        when the relation carries no sketches (nothing produced them).

        A provably EXACT footer count outranks the sketch: skene's value ordering
        deduplicates a column outright, and where that count survives to the
        relation (see `_exact_cardinality_from_footers`) it is a bound, which is
        strictly stronger than KMV's near-exact estimate. This is the only source
        here that is not a sketch, so it is consulted FIRST — and it also gives
        this method an answer for un-ANALYZE'd skene relations, which previously
        got None and dropped `distinct_pushdown` and `hash_map_variant` on the
        floor.
        """
        # identity may be bytes; resolve to str for field mapping
        col_name = column.decode("utf-8") if isinstance(column, bytes) else column

        exact = self._exact_cardinality_from_footers(col_name)
        if exact is not None:
            return exact

        if self._min_k_vector is None:
            # No ANALYZE sketches. skene files carry their own, which merge the
            # same way and are exact below K — see _cardinality_from_sketches.
            return self._cardinality_from_sketches(col_name)

        from opteryx.compiled.nanobind.vectors import kmv_ndv

        sketch_id = self._sketch_index(col_name)
        if sketch_id is None:
            return None
        # _live_rows (None until first prune) keeps the merge over the surviving
        # files only, matching the file set the rest of the plan will read.
        return kmv_ndv(self._native_handle(self._min_k_vector), sketch_id, self._live_rows)

    def estimate_range_cardinality(self, column) -> Optional[int]:
        """NDV estimate from per-file footer statistics — no data read.

        The dataless fallback for relations nobody has ANALYZE'd (no KMV
        sketches — the norm for plain parquet directories). COSTING ONLY:
        deliberately a separate method from ``estimate_cardinality``, whose
        near-exact KMV semantics are load-bearing for the execution-variant
        strategies (distinct_pushdown, hash_map_variant) — those must never
        act on a number derived like this.

        Per-file NDV, in priority order:

          1. The footer's own ``Statistics.distinct_count`` — a REAL
             hash-derived count rugo's writer emits for bloom-eligible columns
             (any type, strings included), pre-merged across row groups by
             AggregateColumnStats.
          2. An integer column's bounds span (``max - min + 1``, capped at the
             file's rows).
          3. Any other numeric column: ``rows // 2``.

        A file resolving none of these makes the WHOLE column unknown (None):
        unknown stays unknown — a fabricated per-file stand-in is exactly the
        ``input_rows // 2`` class of lie this method exists to replace, and it
        measurably backfires (an enum-like VARCHAR estimated at half the
        relation drove TPC-DS Q85's equality selectivity to ~0 and a 660x
        slower plan). Strings therefore get an estimate ONLY from a real
        footer count; bloom-occupancy estimation is the agreed follow-up for
        foreign-written files.

        Files merge sequentially by value-range overlap so two files covering
        the same values count them once: numeric ranges accrue the fraction of
        a file's estimate proportional to the part of its range OUTSIDE the
        running range; non-numeric (byte-comparable) ranges sum when disjoint
        and take the max when overlapping (the safe floor); files without
        comparable bounds take the max. Result is capped at the relation's row
        count. None when the total row count is unknown or zero.
        """
        col_name = column.decode("utf-8") if isinstance(column, bytes) else column
        field_id = self._resolve_field_id(col_name)
        if field_id is None:
            return None

        total_rows = self.get_record_count()
        if total_rows is None or total_rows <= 0:
            return None

        # Ordinalized bounds (ANALYZE manifests, skene footers) are decoded
        # values only for identity-mapped categories — same gate as
        # get_value_range. A VARCHAR's prefix-packed ordinal span says nothing
        # about its distinct-value count, and must not drive the span rule OR
        # the overlap merge.
        bounds_usable = True
        if self.bounds_are_ordinal:
            column_type = self._column_type(col_name)
            bounds_usable = column_type is not None and column_type.category in (
                LogicalCategory.INTEGER,
                LogicalCategory.DATE,
            )

        # Gather (rows, ndv, lo, hi) per file; any unresolvable file makes the
        # column unknown — a partial merge would count that file's values
        # either zero times or twice.
        per_file: list = []
        for file in self.files:
            rows = file.record_count
            if rows is None or rows <= 0:
                return None
            footer_ndv = None
            if file.column_stats is not None:
                footer_ndv = file.column_stats.get_distinct_count(field_id)
            elif file.distinct_value_counts is not None:
                # skene footers (filesystem_connector's SKENE branch) carry NDV
                # as (count, is_exact) rather than inside a FileColumnStats.
                # Same slot in the priority order as the parquet footer count
                # above, and used the same way: this method is COSTING ONLY, so
                # even an exact per-file count is consumed as an estimate here —
                # the merge below is a bound, not a count, the moment two files
                # can share a value. An exact NDV that a consumer may treat as a
                # BOUND has no route through this method by design.
                counted = file.distinct_value_counts.get(field_id)
                if counted is not None:
                    footer_ndv = counted[0]
            file_min = file_max = None
            if bounds_usable:
                if file.column_stats is not None:
                    file_min = file.column_stats.get_min(field_id)
                    file_max = file.column_stats.get_max(field_id)
                elif file.lower_bounds is not None or file.upper_bounds is not None:
                    file_min = (file.lower_bounds or {}).get(field_id)
                    file_max = (file.upper_bounds or {}).get(field_id)
            numeric = (
                type(file_min) in (int, float)
                and type(file_max) in (int, float)
                and file_max >= file_min
            )
            if footer_ndv is not None:
                ndv = int(min(rows, footer_ndv))
            elif numeric and type(file_min) is int and type(file_max) is int:
                ndv = min(rows, file_max - file_min + 1)
            elif numeric:
                ndv = max(1, rows // 2)
            else:
                return None
            per_file.append((int(rows), ndv, file_min, file_max, numeric))

        if not per_file:
            return None

        running_ndv = 0.0
        running_lo = running_hi = None
        running_numeric = False
        first = True
        for rows, ndv, lo, hi, numeric in per_file:
            file_ndv = float(ndv)
            if first:
                first = False
                running_ndv = file_ndv
                running_lo, running_hi = (lo, hi) if lo is not None else (None, None)
                running_numeric = numeric
                continue
            if numeric and running_numeric and running_lo is not None:
                # Numeric overlap: accrue the fraction of this file's range
                # outside the running range.
                width = float(hi) - float(lo)
                if width <= 0.0:
                    fraction = 0.0 if running_lo <= lo <= running_hi else 1.0
                else:
                    outside = max(0.0, float(running_lo) - float(lo)) + max(
                        0.0, float(hi) - float(running_hi)
                    )
                    fraction = min(1.0, outside / width)
                running_ndv += fraction * file_ndv
                running_lo = min(running_lo, lo)
                running_hi = max(running_hi, hi)
            elif (
                lo is not None
                and running_lo is not None
                and type(lo) is type(running_lo)
                and type(hi) is type(running_hi)
            ):
                # Comparable non-numeric bounds (byte strings): disjoint ranges
                # hold disjoint values (sum); overlapping ranges take the max —
                # exact for the common shape where every file spans the same
                # enum domain.
                if hi < running_lo or lo > running_hi:
                    running_ndv += file_ndv
                else:
                    running_ndv = max(running_ndv, file_ndv)
                running_lo = min(running_lo, lo)
                running_hi = max(running_hi, hi)
            else:
                # No overlap information: max is the safe floor.
                running_ndv = max(running_ndv, file_ndv)

        return max(1, min(int(running_ndv), int(total_rows)))

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

    def get_total_uncompressed_size(self, column) -> Optional[int]:
        """Total uncompressed byte size for a column across all files, or None.

        Returns None if any file is missing size data for this column -- a
        partial sum would silently understate the true size, same
        conservative contract as get_total_null_count.

        Two sources, indexed differently on purpose:
          * file.column_stats (FileColumnStats, local/filesystem_connector
            path) is keyed by real field_id, same as get_min/get_max/
            get_null_count -- resolved via _resolve_field_id.
          * file.column_uncompressed_sizes_in_bytes (catalog path) is a
            plain list "aligned with schema field order" (FileEntry's own
            docstring) that from_datafile passes straight through with NO
            field_id remapping, unlike lower_bounds/upper_bounds/
            min_length_bounds -- so it must be indexed by LOAD-TIME POSITION
            (_load_time_columns), never by the catalog field_id, which can
            differ from position after schema evolution.
        """
        col_name = column.decode("utf-8") if isinstance(column, bytes) else column
        field_id = self._resolve_field_id(col_name)
        position = self._load_time_columns.get(col_name)
        if field_id is None and position is None:
            return None

        total = 0
        for file in self.files:
            if file.column_stats is not None:
                if field_id is None:
                    return None
                size = file.column_stats.get_uncompressed_size(field_id)
                if size is None:
                    return None
                total += size
            elif file.column_uncompressed_sizes_in_bytes:
                if position is None or position >= len(file.column_uncompressed_sizes_in_bytes):
                    return None
                size = file.column_uncompressed_sizes_in_bytes[position]
                if size is None:
                    return None
                total += size
            else:
                return None
        return total

    # Categories whose dense-equivalent bytes come from ANALYZE's char_total_bytes
    # rather than from a fixed width. Mirrors _analyze._STRING_CATEGORIES, which is
    # the set char_class_stats() is actually run over -- a category listed here but
    # not there reads back None for every file, never a wrong number.
    _DENSE_CHAR_CATEGORIES = frozenset(
        {LogicalCategory.VARCHAR, LogicalCategory.NVARCHAR, LogicalCategory.VARBINARY}
    )

    def get_total_dense_size(self, column) -> Optional[int]:
        """Total DENSE-EQUIVALENT byte size for a column across all files, or None.

        This is the COMMERCIAL number: what the column's values would occupy if
        stored densely, one value per row, with no dictionary or run-length
        encoding. It is deliberately NOT get_total_uncompressed_size, which
        reports the parquet footer's ``total_uncompressed_size`` -- the
        decompressed size of the ENCODED pages (dictionary page + index values).
        The two differ by more than an order of magnitude in both directions: a
        4-distinct-value VARCHAR measured 12.8x LARGER dense than its footer
        size, while a fully-distinct VARCHAR measured 0.76x -- SMALLER, because
        the footer figure carries offset framing this one excludes. Neither
        number is a safe substitute for the other, and neither bounds the other.

        Derived, never stored, on purpose: everything it needs is already on the
        manifest, and a stored copy would be a fourth per-column size list free
        to drift from the three that produced it. The consequence is that the
        fixed-width arm works on ANY manifest -- no ANALYZE required, since
        record_count alone answers it -- and only the string arm needs a stats
        pass to have run.

        Per column, in one of three arms:

          * FIXED WIDTH (integers, floats, DATE32/TIME/TIMESTAMP64, DECIMAL,
            DECIMAL128, INTERVAL) -- ``record_count * fixed_itemsize()``, the
            single canonical native width table (draken_type_fixed_itemsize in
            core/buffers.h). NULL rows ARE charged their slot: a dense vector
            allocates the slot whether or not it holds a value, which is what
            "stored densely" means.

          * STRING FAMILY (VARCHAR/NVARCHAR/VARBINARY) -- ANALYZE's
            ``char_total_bytes``, the native char_class_stats() byte total.
            VALUES ONLY: no per-row offset, no validity bitmap. A NULL string
            contributes nothing.

          * EVERYTHING ELSE -- None. ARRAY, VARIANT, VECTOR_FP16 and NULL have
            no dense measure recorded anywhere yet. BOOL is here too and is the
            surprising member: its fixed_itemsize() is 0 because draken stores
            booleans bit-packed, and fabricating a width outside the canonical
            table to cover it is exactly the drift that table exists to prevent.
            A native pass for these is the follow-on work; until then a consumer
            MUST read None as UNKNOWN, never as zero bytes -- billing a column
            nothing because nobody measured it is a silent revenue error.

        None if ANY live file is missing the input for its arm, matching
        get_total_uncompressed_size's and get_total_null_count's no-partial-sums
        contract: a partial sum understates the true size and is
        indistinguishable from a genuinely small relation.

        Indexing follows get_total_uncompressed_size's own split, for the same
        reason. The TYPE is resolved by NAME against the live schema (via
        _column_type), which returns None -- unknown, not guessed -- for a
        column projection pushdown has pruned out of scope. ``char_total_bytes``
        is a plain positional list that both producers (the local ANALYZE path's
        _field_ids and the catalog path's raw pass-through) key by LOAD-TIME
        POSITION with no field_id remapping, so it is indexed through
        _sketch_index and NEVER through _resolve_field_id, whose catalog
        field_id can differ from position after schema evolution.
        """
        col_name = column.decode("utf-8") if isinstance(column, bytes) else column

        column_type = self._column_type(col_name)
        if column_type is None:
            return None
        physical = column_type.physical
        if physical is None:
            return None

        fixed_width = physical.fixed_itemsize()
        if fixed_width:
            total = 0
            for file in self.files:
                # Unknown is not zero -- same rule get_record_count applies to
                # the relation total, applied here per file.
                if file.record_count is None:
                    return None
                total += file.record_count * fixed_width
            return total

        if column_type.category not in self._DENSE_CHAR_CATEGORIES:
            return None

        position = self._sketch_index(col_name)
        if position is None:
            return None

        total = 0
        for file in self.files:
            char_total_bytes = file.char_total_bytes
            if not char_total_bytes or position >= len(char_total_bytes):
                return None
            size = char_total_bytes[position]
            if size is None:
                return None
            total += size
        return total

    def estimate_null_fraction(self, column) -> Optional[float]:
        """Estimate fraction of nulls in column using catalog null counts if present."""
        col_name = column.decode("utf-8") if isinstance(column, bytes) else column
        field_id = self._resolve_field_id(col_name)
        if field_id is None:
            return None

        total_rows = self.get_record_count()
        # None (unknown) as well as 0 - there is no fraction to report without a
        # denominator, and dividing by an unknown one would be a fabricated ratio.
        if not total_rows:
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

    def get_value_range(self, column: str) -> Optional[Tuple[Any, Any]]:
        """Aggregated DECODED (min, max) for `column` across all live files.

        Distinct from ``get_column_bounds``, which reads only the
        ``lower_bounds``/``upper_bounds`` dict form and returns the raw
        serialized bytes. That dict form is populated on the catalog path but
        is ``None`` for a parquet-footer manifest built by the filesystem
        connector -- which is exactly where TPC-H/JOB relations come from, so
        every blob-backed relation reported "no bounds" despite the footer
        carrying real per-row-group min/max.

        Reads the two-tier source the rest of the codebase already treats as
        canonical (see statistics_only_response._manifest_bound): typed
        ``FileEntry.column_stats`` first -- the lazy Cython view over the
        parquet footer -- then the field-id-keyed bounds dicts. Never indexes
        the positional ``min_values``/``max_values`` lists by field_id; that
        list is ordered by write position and reads a DIFFERENT column
        whenever field_id != position.

        Returns None when no file carries a usable bound for the column.
        """
        field_id = self._resolve_field_id(column)
        if field_id is None:
            return None

        if self.bounds_are_ordinal:
            # The bounds dicts hold ordinalize() ORDINALS, not decoded values
            # (ANALYZE manifests, skene footers). Callers of this method want
            # decoded values — handing an ordinal to the selectivity estimator
            # compares it against real predicate literals in the wrong space
            # (a VARCHAR's prefix-packed int against a bytes literal). Serve
            # them only for categories whose ordinal space IS the value space
            # (identity mapping); everything else gets "no estimate", which
            # costs estimation quality, never correctness.
            column_type = self._column_type(column)
            if column_type is None or column_type.category not in (
                LogicalCategory.INTEGER,
                LogicalCategory.DATE,
            ):
                return None

        min_val = None
        max_val = None
        for file in self.files:
            if file.column_stats is not None:
                file_min = file.column_stats.get_min(field_id)
                file_max = file.column_stats.get_max(field_id)
            elif file.lower_bounds is not None or file.upper_bounds is not None:
                file_min = (file.lower_bounds or {}).get(field_id)
                file_max = (file.upper_bounds or {}).get(field_id)
            else:
                continue
            if file_min is not None and (min_val is None or file_min < min_val):
                min_val = file_min
            if file_max is not None and (max_val is None or file_max > max_val):
                max_val = file_max

        if min_val is None or max_val is None:
            return None
        return (min_val, max_val)

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

