"""Join output cardinality estimation.

Pure arithmetic over pre-resolved key statistics. The caller is responsible
for walking manifests to populate `KeyStats`; this module does not touch
plans or manifests directly.

Formula (single equi-key inner join):
    |A ⋈_k B| ≈ |A| × |B| / max(NDV(A.k), NDV(B.k))

For multi-key equi-joins the per-key selectivities are multiplied
(independence assumption — wrong under correlated keys, but matches what
other engines do without column-group statistics).

Nulls do not match in equi-joins, so each side's effective row count is
reduced by its null-fraction before the formula is applied.
"""

from dataclasses import dataclass
from enum import Enum
from math import isqrt
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple

from opteryx.planner.cost_estimation.fallback_selectivity import EQ_UNKNOWN_NDV_FALLBACK

_VALID_JOIN_TYPES = frozenset(
    {"inner", "left outer", "right outer", "full outer", "cross", "semi", "anti"}
)


class NdvProvenance(Enum):
    """Where a ``KeyStats.ndv`` came from.

    Mirrors the metric/estimate split ``RelationStatistics`` already enforces
    on row counts (see ``optimizer/statistics.py``): a number with no
    provenance, used by something that ACTS on it, is the dishonesty that
    split exists to stop. Same disease here — an absent join-key NDV is stood
    in for by the key's DOMAIN size (``plan_adapter._key_stats_with_tdom``,
    ``statistics_refresh._equi_key_classes``), and once that stand-in is
    written into ``ndv`` nothing downstream can tell it from a distinct count
    read off a manifest.

    MEASURED is a distinct count somebody counted. DOMAIN_STANDIN is an UPPER
    BOUND on the NDV -- a pre-filter relation size, or a value-range span --
    substituted because no distinct count existed. UNKNOWN is no NDV at all,
    and pairs with ``ndv is None``.

    Consumers that only need a divisor read ``ndv`` (``_key_selectivity``
    deliberately uses the stand-in: dividing by a post-filter row count would
    make a filtered dimension table predict zero reduction). Consumers that
    ACT on the number being a real distinct count -- ``apply_occupancy_bound``
    -- must check the provenance.
    """

    MEASURED = "measured"
    DOMAIN_STANDIN = "domain_standin"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class KeyStats:
    ndv: Optional[int]
    null_fraction: Optional[float]
    ndv_provenance: NdvProvenance = NdvProvenance.UNKNOWN

    def __post_init__(self):
        # No default provenance for a present NDV: a construction site that
        # does not say where its number came from must fail here, not be
        # silently read as a distinct count somewhere downstream.
        if (self.ndv is None) != (self.ndv_provenance is NdvProvenance.UNKNOWN):
            raise ValueError(
                "ndv and ndv_provenance must agree: ndv=None iff provenance is UNKNOWN "
                f"(got ndv={self.ndv!r}, ndv_provenance={self.ndv_provenance!r})"
            )

    @property
    def ndv_is_measured(self) -> bool:
        """True only for a counted distinct value, never for a stand-in."""
        return self.ndv_provenance is NdvProvenance.MEASURED


def _key_selectivity(left: KeyStats, right: KeyStats) -> float:
    if left.ndv is None or right.ndv is None:
        return EQ_UNKNOWN_NDV_FALLBACK
    denom = max(left.ndv, right.ndv)
    if denom <= 0:
        return EQ_UNKNOWN_NDV_FALLBACK
    return 1.0 / denom


def composite_key_ndv(ndvs: Iterable[Optional[int]]) -> Optional[int]:
    """Compose one side's per-column key NDVs into a single key NDV.

    For a multi-column key the composite NDV is at least ``max`` of the
    per-column NDVs and at most their product; ``max`` is the standard
    conservative lower bound (architect ruling 2026-08-21). This is the ONE
    composition both the cardinality estimator (statistics_refresh) and the
    build-side chooser (join_ordering) use -- they previously disagreed
    (max vs min) and read different NDVs for the same join.

    Returns None when no NDV is known for any column.
    """
    known = [n for n in ndvs if n is not None]
    return max(known) if known else None


def apply_occupancy_bound(
    equi_keys: List[Tuple[KeyStats, KeyStats]],
    left_domain_rows: int,
    right_domain_rows: int,
) -> List[Tuple[KeyStats, KeyStats]]:
    """Bound a COMPOSITE key's domain by the rows available to hold it.

    ``_inner_estimate`` multiplies one selectivity per key class under an
    independence assumption, so N classes divide by the PRODUCT of their
    domains. For a genuinely composite key that product counts *possible* key
    tuples, and it can exceed the number that could physically exist. TPC-H
    Q09's ``partsupp ⋈ lineitem`` keys on ``(ps_partkey, ps_suppkey)`` — two
    distinct classes, 200,000 x 100,000 = 2e10 possible tuples against
    8,000,000 rows to hold them — and estimated 23,994 rows against a true
    59,986,052. Being 2,500x under made the cheapest-looking first join the
    single most expensive one available, and DPccp built the whole tree off
    it; the same shape put a 6-million-row input on the BUILD side of three
    consecutive joins in the refresh path.

    A relation cannot contain more distinct key tuples than it has rows, so
    the composite domain is capped at the smaller side's PRE-filter row count
    (this is the row-group occupancy bound evaluated at relation granularity;
    the per-row-group form ``sum(min(rows_rg, cells_rg))`` collapses to
    ``|R|`` for every relation measured in TPC-H and JOB). The per-class
    factor is ``max(left.ndv, right.ndv)`` — exactly the divisor
    ``_key_selectivity`` applies — so the bound caps the product actually
    being charged (architect ruling 2026-08-21; the refresh copy previously
    multiplied only the left NDV).

    Collapsing to a single pair is exactly equivalent when the product is
    already under the bound (one divisor of P == N divisors multiplying to
    P), and null fractions keep their worst-case-per-side composition because
    ``_effective_rows`` takes the max across the key list either way.

    Bails out unchanged when any class has an unknown NDV: ``_key_selectivity``
    falls back to a flat constant for those, so there is no product to bound
    and inventing one would silently overwrite that fallback.

    WIDENS the bound, rather than applying it at full strength, when no class
    in the key knows a MEASURED NDV. An absent join-key NDV is stood in for by
    the key's domain size upstream (``plan_adapter._key_stats_with_tdom``,
    ``_equi_key_classes``) -- correct for ``_key_selectivity``, which needs a
    domain in its divisor, but it also made the ``ndv is None`` guard above
    unreachable from the plan-adapter path, so this bound silently ran on a
    product of upper bounds as though every factor had been counted.
    ``KeyStats.ndv_provenance`` is what tells the two apart; see the widening
    at the code for why the answer is to loosen the bound and not to drop it.

    Callers must note the PRE-bound class count in telemetry: after a collapse
    the returned list no longer reveals that the join had a composite key.

    This is the ONE occupancy bound — statistics_refresh and dpccp both call
    it; they previously carried diverging copies that each claimed to mirror
    the other.
    """
    if len(equi_keys) < 2:
        return equi_keys

    composite = 1
    any_measured = False
    for left_stat, right_stat in equi_keys:
        if left_stat.ndv is None or right_stat.ndv is None:
            return equi_keys
        if left_stat.ndv_is_measured or right_stat.ndv_is_measured:
            any_measured = True
        # The per-pair divisor _key_selectivity actually applies.
        composite *= max(left_stat.ndv, right_stat.ndv)

    bound = max(1, min(left_domain_rows, right_domain_rows))
    if not any_measured:
        # Every factor in `composite` is an upper bound standing in for an
        # absent distinct count, so the product is an upper bound too -- but
        # the occupancy argument is a fact about the RELATION, not about where
        # the NDVs came from, so it does not stop applying. What changes is how
        # much of the gap between the product and the bound is signal: with no
        # measured NDV anywhere in the key, none of it is. Split the difference
        # in log space -- the bound still binds, at the geometric mean of the
        # two numbers rather than at the bound itself.
        #
        # Dropping the bound outright here (the shape the `ndv is None` guard
        # above has) was measured and is worse: TPC-H Q20's hash join estimates
        # 894 against an actual 5,843 with this widening, and 1 without it --
        # the unbounded product of two stand-in domains collapses the join to
        # nothing, which is the failure the bound exists to stop. Full strength
        # instead of widened puts it at 800,000.
        #
        # Q20 is the ONLY query in TPC-H, TPC-DS and ClickBench whose estimates
        # this branch moves at all (measured 2026-08-21, every other query
        # byte-identical under all three treatments). Do not expect broad
        # movement from touching it, and do not read a suite-wide change after
        # editing here as having come from here.
        bound = max(bound, isqrt(composite * bound))
    if composite <= bound:
        return equi_keys

    left_null = [k[0].null_fraction for k in equi_keys if k[0].null_fraction is not None]
    right_null = [k[1].null_fraction for k in equi_keys if k[1].null_fraction is not None]
    # The collapsed NDV is a row count, not a distinct count -- say so, or the
    # next consumer of this list inherits exactly the conflation fixed here.
    return [(
        KeyStats(
            ndv=bound,
            null_fraction=max(left_null) if left_null else None,
            ndv_provenance=NdvProvenance.DOMAIN_STANDIN,
        ),
        KeyStats(
            ndv=bound,
            null_fraction=max(right_null) if right_null else None,
            ndv_provenance=NdvProvenance.DOMAIN_STANDIN,
        ),
    )]


def _null_fraction(stat: KeyStats) -> float:
    if stat.null_fraction is None:
        return 0.0
    return stat.null_fraction


def _effective_rows(rows: int, keys: List[KeyStats]) -> float:
    """Reduce row count by the worst-case (max) null fraction across keys.

    Mirrors the per-side null-fraction composition in
    JoinOrderingStrategy._key_null_fraction.
    """
    if not keys:
        return float(rows)
    worst_null = max(_null_fraction(k) for k in keys)
    if worst_null <= 0.0:
        return float(rows)
    return float(rows) * (1.0 - worst_null)


def _inner_estimate(
    left_rows: int,
    right_rows: int,
    equi_keys: List[Tuple[KeyStats, KeyStats]],
    extra_predicates_selectivity: float,
) -> float:
    if not equi_keys:
        # No equi predicates → degenerates to a cross product modulated by
        # any non-equi conjuncts.
        return float(left_rows) * float(right_rows) * extra_predicates_selectivity

    left_stats = [pair[0] for pair in equi_keys]
    right_stats = [pair[1] for pair in equi_keys]

    eff_left = _effective_rows(left_rows, left_stats)
    eff_right = _effective_rows(right_rows, right_stats)

    selectivity = 1.0
    for left_stat, right_stat in equi_keys:
        selectivity *= _key_selectivity(left_stat, right_stat)

    return eff_left * eff_right * selectivity * extra_predicates_selectivity


def estimate_join_cardinality(
    left_rows: int,
    right_rows: int,
    join_type: str,
    equi_keys: List[Tuple[KeyStats, KeyStats]],
    extra_predicates_selectivity: float = 1.0,
) -> int:
    """Estimate the row count of a join result.

    See module docstring for formulae. Returns a non-negative int, floored
    at 1 so a zero estimate cannot propagate as a multiplicative zero
    through downstream cost arithmetic.
    """
    if left_rows < 0 or right_rows < 0:
        raise ValueError(
            f"row counts must be non-negative (got left={left_rows}, right={right_rows})"
        )
    if join_type not in _VALID_JOIN_TYPES:
        raise ValueError(f"unknown join_type: {join_type!r}")
    if extra_predicates_selectivity < 0.0:
        raise ValueError(
            f"extra_predicates_selectivity must be non-negative (got {extra_predicates_selectivity})"
        )

    if join_type == "cross":
        result = float(left_rows) * float(right_rows) * extra_predicates_selectivity
        return max(1, int(result))

    inner = _inner_estimate(left_rows, right_rows, equi_keys, extra_predicates_selectivity)

    if join_type == "inner":
        result = inner
    elif join_type == "left outer":
        result = max(inner, float(left_rows))
    elif join_type == "right outer":
        result = max(inner, float(right_rows))
    elif join_type == "full outer":
        # inner matches plus the null-extended rows from each side. A side's
        # matched rows are bounded by both its own row count and the inner
        # estimate (a row can match many partners, so matched <= inner), so
        # unmatched ≈ max(0, rows - inner). This keeps the result >= inner —
        # the old max(l + r - inner, l, r) form collapsed below the inner
        # estimate whenever inner > l + r (many-to-many keys).
        left_unmatched = max(0.0, float(left_rows) - inner)
        right_unmatched = max(0.0, float(right_rows) - inner)
        result = inner + left_unmatched + right_unmatched
    elif join_type == "semi":
        result = min(float(left_rows), inner)
    elif join_type == "anti":
        result = max(0.0, float(left_rows) - inner)
    else:  # pragma: no cover — guarded by _VALID_JOIN_TYPES check above
        raise ValueError(f"unhandled join_type: {join_type!r}")

    return max(1, int(result))


def estimate_after_filter(input_rows: int, selectivity: float) -> int:
    """Row count after applying a filter with given selectivity.

    Floored at 1 so a zero estimate cannot propagate as a multiplicative
    zero through downstream cost arithmetic.
    """
    if input_rows < 0:
        raise ValueError(f"input_rows must be non-negative (got {input_rows})")
    if selectivity < 0.0:
        raise ValueError(f"selectivity must be non-negative (got {selectivity})")
    return max(1, int(input_rows * selectivity))


def estimate_group_by_cardinality(
    input_rows: int,
    group_key_ndvs: List[Optional[int]],
) -> int:
    """Cardinality after GROUP BY.

    Output is min(input rows, product of group-key NDVs) — the product under
    an independence assumption, capped because a grouped aggregate can never
    emit more rows than it consumes.

    A key with an UNKNOWN NDV (a None/non-positive entry) makes the whole
    estimate the input row count: the cap is the only sound bound we have.
    The previous behaviour fabricated ``input_rows // 2`` per unknown key —
    a number grounded in nothing, which saturated the cap the moment any key
    lacked statistics and told the guard a group-by reduces nothing. Row-level
    NDV availability is the caller's problem (see
    ``Manifest.estimate_range_cardinality`` for the dataless fallback); this
    function does not invent statistics it wasn't given.

    Args:
        input_rows: rows entering the group-by.
        group_key_ndvs: distinct-value count per group key. Use None for
            keys whose NDV is unknown.

    Returns:
        Estimated output row count, >= 1.
    """
    if input_rows <= 0:
        return 1
    if not group_key_ndvs:
        return 1
    cardinality = 1
    for ndv in group_key_ndvs:
        if ndv is None or ndv <= 0:
            return max(1, input_rows)
        cardinality *= ndv
    return max(1, min(cardinality, input_rows))
