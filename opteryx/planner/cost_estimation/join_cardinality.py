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
from typing import List
from typing import Optional
from typing import Tuple

# Fallback selectivity for an equality predicate when NDV is unknown for
# either side of the key. Mirrors the equality fallback used by the
# per-predicate selectivity estimator.
_EQUALITY_FALLBACK_SELECTIVITY = 0.1

_VALID_JOIN_TYPES = frozenset(
    {"inner", "left outer", "right outer", "full outer", "cross", "semi", "anti"}
)


@dataclass(frozen=True)
class KeyStats:
    ndv: Optional[int]
    null_fraction: Optional[float]


def _key_selectivity(left: KeyStats, right: KeyStats) -> float:
    if left.ndv is None or right.ndv is None:
        return _EQUALITY_FALLBACK_SELECTIVITY
    denom = max(left.ndv, right.ndv)
    if denom <= 0:
        return _EQUALITY_FALLBACK_SELECTIVITY
    return 1.0 / denom


def _null_fraction(stat: KeyStats) -> float:
    if stat.null_fraction is None:
        return 0.0
    return stat.null_fraction


def _effective_rows(rows: int, keys: List[KeyStats]) -> float:
    """Reduce row count by the worst-case (max) null fraction across keys.

    Mirrors the per-relation null-fraction composition in
    join_ordering.get_column_null_fractions.
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
        result = max(
            float(left_rows) + float(right_rows) - inner,
            float(left_rows),
            float(right_rows),
        )
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

    Output is the product of group-key NDVs (independence assumption),
    capped at the input row count. Unknown NDVs (None entries) contribute
    a fallback of input_rows / 2 each — same heuristic as the previous
    implementation.

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
        if ndv is not None and ndv > 0:
            cardinality *= ndv
        else:
            cardinality *= max(1, input_rows // 2)
    return max(1, min(cardinality, input_rows))
