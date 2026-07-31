"""
Statistics estimation primitives for cardinality predictions.

Supports:
- Column and relation statistics with histogram backing
- Cardinality estimation for GROUP BY and joins
"""

from dataclasses import dataclass
from dataclasses import replace
from typing import TYPE_CHECKING
from typing import Optional
from typing import Union

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

    def copy(self) -> "RelationStatistics":
        """Create a shallow copy with new column dict."""
        return RelationStatistics(
            row_count=self.row_count, columns={k: v for k, v in self.columns.items()}
        )

    def get_column(self, identity: bytes) -> Optional[ColumnStatistics]:
        """Retrieve statistics for a column by its identity."""
        return self.columns.get(identity)

    def with_row_count(self, new_count: int) -> "RelationStatistics":
        """Return a copy with updated row count."""
        return replace(self, row_count=new_count)

    def update_column_range(self, identity: bytes, new_range: ColumnRange) -> "RelationStatistics":
        """Return a copy with an updated column range."""
        new_stats = self.copy()
        col_stats = new_stats.columns.get(identity)
        if col_stats:
            new_col = replace(col_stats, value_range=new_range)
            new_stats.columns[identity] = new_col
        return new_stats




