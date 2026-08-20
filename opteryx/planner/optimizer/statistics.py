"""
Statistics estimation primitives for cardinality predictions.

Supports:
- Column and relation statistics with histogram backing
- Cardinality estimation for GROUP BY and joins
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import Optional
from typing import Union

_KEEP = object()
"""Sentinel for ColumnStatistics.but(): "keep this field's current value"."""

if TYPE_CHECKING:
    from opteryx.third_party.maki_nage import Distogram


@dataclass(frozen=True)
class ColumnRange:
    """Represents the range of values in a column."""

    lower_bound: Optional[Union[int, float, str]] = None
    upper_bound: Optional[Union[int, float, str]] = None

    def intersect(self, other: "ColumnRange") -> "ColumnRange":
        """Compute intersection of two ranges."""
        if self.lower_bound is None and other.lower_bound is None:
            new_lower = None
        elif self.lower_bound is None:
            new_lower = other.lower_bound
        elif other.lower_bound is None:
            new_lower = self.lower_bound
        else:
            new_lower = max(self.lower_bound, other.lower_bound)

        if self.upper_bound is None and other.upper_bound is None:
            new_upper = None
        elif self.upper_bound is None:
            new_upper = other.upper_bound
        elif other.upper_bound is None:
            new_upper = self.upper_bound
        else:
            new_upper = min(self.upper_bound, other.upper_bound)

        return ColumnRange(new_lower, new_upper)

    def width(self) -> Optional[float]:
        """Estimate the span of the range (for numeric types)."""
        if self.lower_bound is None or self.upper_bound is None:
            return None
        try:
            return float(self.upper_bound) - float(self.lower_bound)
        except (TypeError, ValueError):
            return None


@dataclass
class ColumnStatistics:
    """Statistics for a single column.

    ``column_name`` is diagnostic only — it is NOT the key. Column names are
    not unique across a plan (``it1.info``, ``mi.info`` and ``mi_idx.info`` are
    three different columns all named ``info``), so ``RelationStatistics``
    keys on the column *identity*. See that class for the contract.
    """

    column_name: str
    data_type: str

    # Cardinality: number of distinct values
    distinct_count: Optional[int] = None

    # Range of values
    value_range: ColumnRange = ColumnRange()

    # Distribution information (histogram, sketch, etc.)
    # Can be a Distogram from maki_nage library for histogram-backed estimation
    histogram: Optional[object] = None

    null_fraction: Optional[float] = None

    # LIKE '%needle%' char-class selectivity estimator inputs (VARCHAR/
    # NVARCHAR/VARBINARY columns only; None elsewhere or when no ANALYZE has
    # produced char-class stats). class_proportions: {class_name: proportion
    # of this column's bytes in that class}, the 8 classes from
    # opteryx.planner.cost_estimation.selectivity's char-class estimator.
    # avg_length: mean string length in bytes, derived (not stored) from
    # char_total_bytes / true_non_null_count at read time — see
    # Manifest.get_char_class_stats.
    class_proportions: Optional[dict] = None
    avg_length: Optional[float] = None

    # STARTS_WITH (prefix LIKE) ordinal-bounds selectivity estimator input
    # (VARCHAR/NVARCHAR/VARBINARY columns only; None elsewhere, or when the
    # manifest's bounds aren't ordinalized, or when no file carries a real
    # bound for this column). (lo, hi): the relation-wide ordinal-key range —
    # min/max of ColumnType.ordinalize() applied to this column's real
    # values across every live file — aggregated from Manifest per-file
    # min_values/max_values. This is a WEAKER, cheaper signal than `histogram`
    # (no bin-level detail, just the overall span) and is populated
    # independently of it: a relation can have ordinal_bounds without ever
    # having been ANALYZE'd for a histogram (per-file min/max exist from
    # ordinary writes; the richer histogram/char-class stats need an explicit
    # ANALYZE pass). Deliberately NOT the same field as `value_range`
    # (ColumnRange): that field is untyped/unused today and, if wired up
    # later, is expected to carry REAL decoded values for numeric columns —
    # mixing ordinal keys into it would silently corrupt any future numeric
    # consumer that doesn't know to check `bounds_are_ordinal` first.
    ordinal_bounds: Optional[tuple] = None

    # Length-aware hard-impossibility guard input, shared by the
    # containment-style selectivity estimators (STARTS_WITH, INSTR,
    # ENDS_WITH — opteryx.planner.cost_estimation.selectivity). (min_length,
    # max_length): relation-wide observed string byte-length range, from
    # Manifest.get_length_bounds. Distinct from `avg_length` above:
    # avg_length is a soft, probabilistic signal (a needle close to the
    # average is less LIKELY to match); length_bounds is a hard, certain one
    # (a needle longer than the observed maximum CANNOT match — no
    # probability involved). Populated independently of histogram/
    # class_proportions, same as ordinal_bounds.
    #
    # NOTE (NVARCHAR caveat): min/max length as computed by the external
    # catalog stats builder is CHARACTER length (Python len() on a decoded
    # str), not BYTE length, while every selectivity estimator here compares
    # against a needle's BYTE length (predicate literals are bytes by the
    # time they reach selectivity.py) and the local ANALYZE path's native
    # char_class_stats() kernel is byte-based. UTF-8 byte length >= char
    # length always, so a catalog-sourced max_length used as a byte ceiling
    # can UNDER-state the true limit for non-ASCII content — risking a false
    # "impossible" verdict. Consumers MUST skip the hard guard for NVARCHAR
    # columns (where non-ASCII content concentrates in practice) and only
    # trust this field for VARCHAR/VARBINARY, where it's safe either way.
    length_bounds: Optional[tuple] = None

    # Estimated total on-disk uncompressed size (bytes) of this column's
    # values AT THIS PLAN NODE — a relation total, not a per-row average,
    # mirroring row_count: it is rescaled at every operator that changes
    # row_count (Filter selectivity, Limit, Join cardinality, Aggregate/
    # Distinct output rows, Union) the same way row_count itself is, via
    # StatisticsRefreshVisitor. Populated at Scan from (in priority order)
    # Manifest.get_total_uncompressed_size (real per-file measured bytes),
    # avg_length * row_count (string columns with ANALYZE'd char-class
    # stats), or row_count * DrakenType.fixed_itemsize() (fixed-width
    # columns, via the single canonical native width table — see
    # draken_type_fixed_itemsize in core/buffers.h). None when none of
    # those signals are available (e.g. a variable-width column with no
    # ANALYZE pass and no manifest-level size) — never fabricated.
    total_bytes: Optional[int] = None

    def but(
        self,
        *,
        value_range=_KEEP,
        histogram=_KEEP,
        distinct_count=_KEEP,
        total_bytes=_KEEP,
    ) -> "ColumnStatistics":
        """Copy with the given fields changed — the statistics propagators'
        replacement for ``dataclasses.replace``, which re-derives the field
        list on every call and was the single hottest function in planning.
        Only the four fields the propagators actually rewrite are exposed;
        add a parameter here rather than reintroducing ``replace``.
        """
        return ColumnStatistics(
            column_name=self.column_name,
            data_type=self.data_type,
            distinct_count=self.distinct_count if distinct_count is _KEEP else distinct_count,
            value_range=self.value_range if value_range is _KEEP else value_range,
            histogram=self.histogram if histogram is _KEEP else histogram,
            null_fraction=self.null_fraction,
            class_proportions=self.class_proportions,
            avg_length=self.avg_length,
            ordinal_bounds=self.ordinal_bounds,
            length_bounds=self.length_bounds,
            total_bytes=self.total_bytes if total_bytes is _KEEP else total_bytes,
        )


@dataclass
class RelationStatistics:
    """Statistics for an entire relation/intermediate result.

    ``columns`` is keyed by ``SchemaColumn.identity`` (opaque ``bytes``), NOT by
    column name. This mirrors the invariant ``SchemaColumn.__post_init__``
    already enforces: a name is not an identity, and keying on it "collapses
    distinct columns that share a name — every self-join, and any join of
    tables with a common column name — into one".

    Keying on name silently merged unrelated columns here: a join of two
    relations that both have an ``id`` dropped one side's stats outright, and
    range constraints gathered from ``it1.info``, ``mi.info`` and
    ``mi_idx.info`` were intersected as though they described one column.
    """

    row_count: int
    columns: dict[bytes, ColumnStatistics]

    # Pre-filter row count of the largest base relation underneath this node --
    # a *domain* size, not a cardinality. Join-key NDV is frequently absent
    # (Parquet rarely carries distinct-count statistics), and the tdom fallback
    # that stands in for it must divide by the size of the key's domain, not by
    # the post-filter row count: |A| x |B| / min(|A|, |B|) is identically
    # max(|A|, |B|), so a filtered dimension table would predict zero reduction
    # no matter how selective its filter. None means "same as row_count" so
    # every existing construction site keeps its previous meaning.
    base_row_count: Optional[int] = None

    @property
    def domain_row_count(self) -> int:
        """Base (pre-filter) row count, falling back to the live row count."""
        return self.row_count if self.base_row_count is None else self.base_row_count

    def copy(self) -> "RelationStatistics":
        """Create a shallow copy with new column dict."""
        return RelationStatistics(
            row_count=self.row_count,
            columns={k: v for k, v in self.columns.items()},
            base_row_count=self.base_row_count,
        )

    def get_column(self, identity: bytes) -> Optional[ColumnStatistics]:
        """Retrieve statistics for a column by its identity."""
        return self.columns.get(identity)

    def with_row_count(self, new_count: int) -> "RelationStatistics":
        """Return a copy with updated row count."""
        return RelationStatistics(
            row_count=new_count, columns=self.columns, base_row_count=self.base_row_count
        )

    def update_column_range(self, identity: bytes, new_range: ColumnRange) -> "RelationStatistics":
        """Return a copy with an updated column range."""
        new_stats = self.copy()
        col_stats = new_stats.columns.get(identity)
        if col_stats:
            new_stats.columns[identity] = col_stats.but(value_range=new_range)
        return new_stats




