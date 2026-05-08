"""Predicate ordering cost arithmetic.

Pure, side-effect-free ordering for conjunctive (AND-chain) predicates given
pre-resolved per-predicate selectivity and cost. The caller is responsible
for resolving stats from manifests / catalogs; this module does not touch
plans, manifests, or expression nodes.

Formula:
    For independent predicates applied in order π,
        total_cost(π) = Σ_i  cost(π_i) × Π_{j<i}  sel(π_j)

For ``len(predicates) <= brute_force_threshold`` we enumerate permutations
exhaustively. Above the threshold we use the rank-ordering theorem
(Hellerstein & Stonebraker, 1993): sorting by

    rank(p) = (sel(p) - 1) / cost(p)

ascending minimises ``total_cost`` under the independence assumption. This
is provably optimal for independent conjuncts and is what the brute-force
search converges to — just in O(n log n) instead of O(n!).

Independence between predicates is assumed (matches join cardinality
estimation; correlation needs column-group statistics we don't have).
"""

from dataclasses import dataclass
from itertools import permutations
from typing import Hashable
from typing import List
from typing import Tuple
from typing import TypeVar

PredicateId = TypeVar("PredicateId", bound=Hashable)


@dataclass(frozen=True)
class PredicateStats:
    """Pre-resolved statistics for a single predicate.

    selectivity: fraction of rows passing, in [0.0, 1.0].
    cost:        relative per-row evaluation cost; must be > 0.
    """

    selectivity: float
    cost: float


def _validate(predicates: List[Tuple[PredicateId, PredicateStats]]) -> None:
    for _, stats in predicates:
        if not (0.0 <= stats.selectivity <= 1.0):
            raise ValueError(
                f"selectivity must be in [0.0, 1.0] (got {stats.selectivity})"
            )
        if stats.cost <= 0.0:
            raise ValueError(f"cost must be > 0 (got {stats.cost})")


def _brute_force(stats: List[PredicateStats]) -> Tuple[int, ...]:
    n = len(stats)
    best_order = tuple(range(n))
    best_cost = float("inf")

    for arrangement in permutations(range(n)):
        cumulative = 1.0
        cost = 0.0
        for idx in arrangement:
            cost += stats[idx].cost * cumulative
            cumulative *= stats[idx].selectivity
        # Strict < preserves the first-seen permutation on ties; permutations()
        # yields the identity (0, 1, ..., n-1) first, so the input order wins
        # all ties.
        if cost < best_cost:
            best_cost = cost
            best_order = arrangement

    return best_order


def _rank_order(stats: List[PredicateStats]) -> List[int]:
    # rank(p) = (sel - 1) / cost. Ascending rank = more-selective-per-unit-work
    # first. Python's sort is stable, so equal ranks preserve input order.
    return sorted(range(len(stats)), key=lambda i: (stats[i].selectivity - 1.0) / stats[i].cost)


def order_predicates(
    predicates: List[Tuple[PredicateId, PredicateStats]],
    *,
    brute_force_threshold: int = 6,
) -> List[PredicateId]:
    """Return predicate IDs in optimal application order.

    See module docstring for the cost model and assumptions.

    Raises ValueError on out-of-range selectivity, non-positive cost, or
    ``brute_force_threshold < 1``.
    """
    if brute_force_threshold < 1:
        raise ValueError(
            f"brute_force_threshold must be >= 1 (got {brute_force_threshold})"
        )

    if not predicates:
        return []

    _validate(predicates)

    if len(predicates) == 1:
        return [predicates[0][0]]

    stats = [s for _, s in predicates]

    if len(predicates) <= brute_force_threshold:
        order = _brute_force(stats)
    else:
        order = _rank_order(stats)

    return [predicates[i][0] for i in order]
