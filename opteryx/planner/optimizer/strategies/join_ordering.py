# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Join Ordering

Type: Cost-Based / Correctness
Goal: Faster Joins

Build a left-deep join tree, where the left relation of any pair is the smaller relation.

We also decide whether a join needs the nested-loop strategy: a non-equi conjunct
(pure theta, or mixed equi+theta) has no hash key to build from, so nested loop is
the only correct execution — never a cost-based choice. A PURE equi join always
uses the hash-join mode; there is no longer a nested-loop-vs-hash-join cost
trade-off to make for equi joins in the native engine (see below).

Join Ordering Rules (from COST-BASED-OPTIMIZER.md):
1. If one table is more than 3x the bytes of the other, larger table goes right (memory pressure heuristic)
2. If cardinalities are within 1%, larger table goes right
3. Otherwise, use cardinality estimation of join column(s) to decide left/right tables
   -- but only where it does not contradict the row counts: a cardinality
   preference may break a row-count near-tie, never overturn it (see _decide_swap_reasoned)
4. If table sizes and cardinalities are the same (e.g. self join), don't change order

Historical note: this strategy used to ALSO route a pure equi join to
"nested loop" when the smaller side was tiny and the larger side was in a
calibrated size window (see scratch/.archive/_sweep_join_crossover.py). That
sweep measured the OLD Cython nested_loop_join.pyx (a genuine O(n*m) loop)
against hashed_inner_join.pyx (a genuine hash join) — two different
algorithms with a real crossover. In the native engine, "inner" and
"nested_loop" compile to the IDENTICAL native join2 build/probe mechanism
(opteryx/managers/execution/compiler.py's `_compile_join`, both mode 0); the
only difference is that "nested_loop" pays for an extra, wholly redundant
residual re-check of the equi condition it already keyed on. There is no
longer a scenario where choosing nested_loop over hash for a pure equi join
helps — confirmed empirically (forced hash vs forced nested_loop across the
old calibrated window showed no measurable difference) — so the heuristic
was removed rather than re-tuned.
"""

from opteryx.expression import NodeType, binary_operands
from opteryx.planner.binder.join_helpers import band_operand_leg
from opteryx.planner.cost_estimation import composite_key_ndv
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import flip_join_leg_labels
from .optimization_strategy import get_nodes_of_type_from_logical_plan

_NON_EQUI_COMPARATORS = ("NotEq", "Lt", "Gt", "LtEq", "GtEq")


def _col_value(col):
    """Return the underlying column identifier regardless of object shape."""
    return getattr(col, "value", col)


def _contains_non_equi_comparator(condition) -> bool:
    """True if `condition` (a join ON expression) contains a non-equi comparison
    anywhere in its AND-tree — either the whole predicate is a pure theta
    comparison (`a > b`), or it's compound with at least one non-equi conjunct
    (`a = b AND c != d`). Both shapes are inexpressible as a hash-join key: a
    hash join has nothing to build a key from for the non-equi part, so nested
    loop (keyed build when an equi conjunct exists, cartesian otherwise, always
    with the residual filter) is the only correct strategy — not a cost-based
    choice, so this must never be gated by row-count."""
    if getattr(condition, "node_type", None) == NodeType.AND:
        left, right = binary_operands(condition)
        return _contains_non_equi_comparator(left) or _contains_non_equi_comparator(right)
    return _col_value(condition) in _NON_EQUI_COMPARATORS


# ---- BAND JOIN recognition ---------------------------------------------------------
#
# A BAND join is an equi-join whose ON clause also bounds ONE column of one leg
# BOTH ABOVE AND BELOW by values from the other leg:
#
#   ON f.client = l.client
#      AND l.event_time <= f.flow_start
#      AND l.event_time  > f.flow_start - INTERVAL '20' SECOND
#
# Executed as a hash join the equality alone pairs every flow with every one of its
# client's lookups and the band discards >99% of them one node up. Executed as a
# band join the build side is kept sorted by the banded column within each equi
# group, and a probe row emits the contiguous slice between two bisects — the
# discarded pairs are never formed. See docs/BAND_JOIN_PROPOSAL.md.
#
# ⛔ BOTH bounds are required, and that is a restriction, not an oversight. A
# one-sided bound selects a PREFIX of the sorted run, unbounded in size, which is
# usually a worse plan than the hash join it would replace.
#
# ⛔ The two band conjuncts are CONSUMED by the range, not also applied as a
# residual. That is only sound because recognition admits NOTHING ELSE in the ON
# clause: equi conjuncts (which stay join keys) plus exactly the two band conjuncts.
# Anything else declines the whole shape, so "consume" and "is exactly equivalent"
# are one decision rather than two.

_BAND_COMPARATORS = ("Lt", "LtEq", "Gt", "GtEq")

# Which end of the BANDED column a comparator establishes, and whether that end is
# closed — read once for the banded operand on the left of the comparator, and
# mirrored when it is on the right. Getting this table backwards shifts the answer
# by exactly the rows sitting ON a boundary, which no interior-range test notices.
_BAND_BOUND_ON_LEFT = {
    "Lt": ("upper", False),
    "LtEq": ("upper", True),
    "Gt": ("lower", False),
    "GtEq": ("lower", True),
}
_BAND_BOUND_ON_RIGHT = {
    "Lt": ("lower", False),
    "LtEq": ("lower", True),
    "Gt": ("upper", False),
    "GtEq": ("upper", True),
}


def _and_conjuncts(condition):
    """Flatten an ON tree's AND spine. An OR is one leaf and carries no band."""
    if condition is None:
        return []
    if getattr(condition, "node_type", None) == NodeType.AND:
        left, right = binary_operands(condition)
        return _and_conjuncts(left) + _and_conjuncts(right)
    return [condition]


def _band_conjunct(conjunct, left_relation_names, right_relation_names):
    """Read one comparison as (banded_identifier, banded_leg, bound_expr, end, closed).

    The banded operand must be a BARE column — it is the thing the build side gets
    sorted by, so it has to be a column that exists, not a value computed per pair.
    The bound operand must read from the OTHER leg only; a bound reading the banded
    leg is a single-relation filter and a bound reading both is a theta this cannot
    bisect.
    """
    if conjunct.node_type != NodeType.COMPARISON_OPERATOR:
        return None
    if conjunct.value not in _BAND_COMPARATORS:
        return None
    if conjunct.left is None or conjunct.right is None:
        return None

    for banded, bound, table in (
        (conjunct.left, conjunct.right, _BAND_BOUND_ON_LEFT),
        (conjunct.right, conjunct.left, _BAND_BOUND_ON_RIGHT),
    ):
        if banded.node_type != NodeType.IDENTIFIER:
            continue
        if banded.schema_column is None:
            continue
        banded_leg = band_operand_leg(banded, left_relation_names, right_relation_names)
        bound_leg = band_operand_leg(bound, left_relation_names, right_relation_names)
        if banded_leg is None or bound_leg is None:
            continue
        if banded_leg == bound_leg:
            continue
        end, closed = table[conjunct.value]
        return banded, banded_leg, bound, end, closed
    return None


def _recognize_band(node):
    """The band descriptor for this join's ON clause, or None.

    Returns (banded_identifier, banded_leg, lower_expr, lower_closed, upper_expr,
    upper_closed). Called only for INNER joins whose ON already carries a non-equi
    conjunct.
    """
    left_names = list(node.left_relation_names or ())
    right_names = list(node.right_relation_names or ())
    if not left_names or not right_names:
        return None

    bounds = {}
    banded = None
    equi_seen = False
    for conjunct in _and_conjuncts(node.on):
        if (
            conjunct.node_type == NodeType.COMPARISON_OPERATOR
            and conjunct.value == "Eq"
        ):
            equi_seen = True
            continue
        read = _band_conjunct(conjunct, left_names, right_names)
        if read is None:
            return None   # something in the ON this shape does not model
        identifier, leg, bound, end, closed = read
        if banded is None:
            banded = (identifier, leg)
        elif identifier.schema_column.identity != banded[0].schema_column.identity:
            return None   # two different columns banded — not one sorted run
        if end in bounds:
            return None   # two lower or two upper bounds do not close a band
        bounds[end] = (bound, closed)

    # An UNKEYED band would build one sorted run over the whole relation. That may
    # well be a good plan, but it is a different one -- it is the shape the CROSS
    # JOIN spelling lands on -- and costing it is not this change.
    if not equi_seen or banded is None:
        return None
    if set(bounds) != {"lower", "upper"}:
        return None

    lower_expr, lower_closed = bounds["lower"]
    upper_expr, upper_closed = bounds["upper"]

    # ⛔ ALL THREE must carry the SAME type. The band is answered by BISECTING the
    # build side's sort order with the probe's bound values, so the two sides have to
    # agree on what order the values are in. Two types that merely compare correctly
    # through the expression engine can normalise differently under the sort key —
    # this is the class of bug ASOF hit on a VARCHAR key (see AsofKeyKind) — and here
    # it would not raise, it would return a bisect over garbage.
    #
    # ASOF answers the same problem by materialising a coercion CAST. That is
    # available here too, and deliberately not taken yet: a coercion changes which
    # values are equal, and the four inclusivity edges have to be re-argued against
    # the coerced type before it can be trusted. Declining leaves today's plan.
    band_type = banded[0].schema_column.column_type
    for bound_expr in (lower_expr, upper_expr):
        bound_column = bound_expr.schema_column
        if bound_column is None or bound_column.column_type != band_type:
            return None

    return banded[0], banded[1], lower_expr, lower_closed, upper_expr, upper_closed


def _join_key_identity(col):
    """Identity of a join-key column, matching how RelationStatistics.columns
    is keyed (see statistics_refresh._column_identity).

    Join keys arrive as raw identity ``bytes``; nodes carry theirs on
    ``.schema_column``. Returns None when none can be resolved (NDV/null then
    go unknown). Never falls back to the column *name* — names are not unique
    across a plan, so a name lookup can silently return another relation's
    statistics."""
    if isinstance(col, bytes):
        return col
    schema_column = getattr(col, "schema_column", None)
    if schema_column is not None:
        identity = getattr(schema_column, "identity", None)
        if isinstance(identity, bytes):
            return identity
    identity = getattr(col, "identity", None)
    return identity if isinstance(identity, bytes) else None


# Which rule in _decide_swap_reasoned produced the answer. Reported verbatim in the
# OPTIMIZATIONS block, because "swapped" and "kept" are the same word for five
# different pieces of reasoning and only the rule says which numbers mattered.
_RULE_MEMORY_PRESSURE = "rule 1, memory pressure"
_RULE_CARDINALITY_TIE = "rule 2, near-equal cardinality"
_RULE_CARDINALITY = "rule 3, cardinality"
_RULE_CARDINALITY_BLOCKED = "rule 3 overruled by row counts"
_RULE_ROWS_ONLY = "no cardinality data, row counts only"


def _decide_swap_reasoned(
    left_rows, right_rows, left_ndv, right_ndv, left_null, right_null
):
    """Decide whether to swap the join's sides so the smaller/cheaper relation
    ends up on the left (build) side. Returns ``(swap, rule)`` — the rule being
    which of the rules below actually produced the answer.

    Pure function of per-side row counts, join-key NDVs and join-key null
    fractions. Row counts are *post-filter* ``statistics.row_count`` when
    available (so a heavily-filtered large table is correctly seen as small),
    falling back to the binder's pre-filter row estimate otherwise. The 3x and
    1% thresholds are unchanged from the previous size-only implementation, and
    with unknown NDV/null this reduces to "smaller side on the left" exactly as
    before.

    Rule 3's cardinality preference is subordinate to the row counts: it may pick
    a side when the rows are close, but it may never move the larger side onto the
    build leg. Rule 1's own direction (the >3x memory-pressure swap) is decided
    before any NDV is read and is unaffected.
    """
    # Rule 1: memory pressure — one side dominates the other in rows.
    if left_rows > 3 * right_rows:
        return True, _RULE_MEMORY_PRESSURE
    if right_rows > 3 * left_rows:
        return False, _RULE_MEMORY_PRESSURE

    # Effective rows discount join keys that are partly NULL (worst-case side).
    left_eff = left_rows * (1.0 - left_null) if left_null else left_rows
    right_eff = right_rows * (1.0 - right_null) if right_null else right_rows

    # Rules 2 & 3: cardinality-aware when both join-key NDVs are known.
    if left_ndv is not None and right_ndv is not None:
        denom = max(left_ndv, right_ndv)
        card_diff_pct = (abs(left_ndv - right_ndv) / denom * 100.0) if denom else 0.0
        if card_diff_pct <= 1.0:
            # Rule 2: near-equal cardinality — smaller effective rows on the left.
            return left_eff > right_eff, _RULE_CARDINALITY_TIE
        # Rule 3: prefer smaller cardinality left; tie-break on effective rows.
        swap = left_ndv > right_ndv or (left_ndv == right_ndv and left_eff > right_eff)
        # ...but never on NDV alone against the row counts. An NDV is an ESTIMATE
        # and the range-derived fallback (Manifest.estimate_range_cardinality, used
        # whenever nothing has been ANALYZE'd) is routinely wrong by multiples and
        # in either direction; the row count is the thing the build side actually
        # has to materialise. TPC-H Q18 at SF100 is the case that named this rule:
        # left 286M rows / ndv 53.3M against right 600M rows / ndv 37.3M — both
        # sides key on orderkey, whose true NDV is 150M — so Rule 1 abstained at
        # 2.1x and Rule 3 moved the 600M-row lineitem scan onto the build leg
        # (3.3s -> 13.6s; Q10 likewise 1.5s -> 2.5s). A cardinality preference may
        # break a row-count near-tie, never overturn it.
        if swap and right_eff > left_eff:
            return False, _RULE_CARDINALITY_BLOCKED
        # NOTE the guard is deliberately one-directional. Its mirror -- Rule 3
        # DECLINING a swap and so leaving the larger side on the build leg -- is a
        # real hole (reachable: left 600M/ndv 37M vs right 286M/ndv 53M) but
        # closing it was measured NET NEGATIVE at SF100: Q10 1.7s -> 3.1s and
        # Q09 4.2s -> 5.3s against Q07's gain. Surfaced, not fixed.
        return swap, _RULE_CARDINALITY

    # Fallback: no cardinality data — smaller effective rows on the left.
    return right_eff < left_eff, _RULE_ROWS_ONLY


# How much larger the build leg must be estimated to be before a SEMI/ANTI join is
# worth exchanging. Not taste: the estimates this reads are known to run LOW, and
# were measured low on the very query the rule targets — TPC-H Q21 at SF100 estimates
# its semi-join's left leg at 1,600,101 rows against an actual 7,313,671 (4.6x), and
# `l_receiptdate > l_commitdate` at 200,012,634 against 379,356,474 (1.9x). A margin
# of 10 keeps the decision correct through an error of that size instead of assuming
# the numbers are right. Q21's real ratio is 82:1, so it clears this comfortably —
# the swap fires where it is robust, not wherever it would help by a nose.
_SWAP_BUILD_RATIO = 10.0

# Operators that consume their whole input before emitting. If one of these sits
# between the join and any LIMIT, nothing downstream could have short-circuited the
# probe, so the exchange costs no streaming that was ever going to happen.
_BLOCKING_ABOVE = (
    LogicalPlanStepType.AggregateAndGroup,
    LogicalPlanStepType.Aggregate,
    LogicalPlanStepType.Order,
    LogicalPlanStepType.HeapSort,
    LogicalPlanStepType.Distinct,
    LogicalPlanStepType.Window,
    LogicalPlanStepType.FramedWindow,
)


def _limit_can_short_circuit(plan, join_nid) -> bool:
    """Could a LIMIT above this join have stopped the probe early?

    Walks from the join toward the exit. A blocking operator found first means the
    answer is no — the rows were all going to be read regardless. A Limit found first
    means yes, and the exchange would take a query that could stop after ten rows and
    make it read the whole streamed relation.

    Unknown shapes answer YES (do not swap). A wrong "no" here turns a fast query
    slow, which is precisely the regression the ratio gate is being careful about.
    """
    seen = set()
    frontier = [join_nid]
    while frontier:
        nid = frontier.pop()
        for target, _s, _r in plan.outgoing_edges(nid):
            if target in seen:
                continue
            seen.add(target)
            node_type = plan[target].node_type
            if node_type in _BLOCKING_ABOVE:
                continue        # this branch is safe; do not walk past it
            if node_type == LogicalPlanStepType.Limit:
                return True
            frontier.append(target)
    return False


def _ratio_text(larger, smaller) -> str:
    """A side ratio, at a precision that stays readable across the range it spans.
    The semi/anti gate compares against a margin of 10 and the real ratios run from
    0.004 to several thousand, where a single format string renders one end as
    "0.00x" and the other as "6e+03x" — both unreadable at exactly the moment the
    number is the whole argument."""
    ratio = larger / smaller
    if ratio >= 100:
        return f"{ratio:,.0f}"
    if ratio >= 1:
        return f"{ratio:.2f}"
    return f"{ratio:.3g}"


# "this rule did not consult that number", distinct from a consulted-but-unknown None.
_NOT_CONSULTED = object()


def _side_facts(rows, ndv=_NOT_CONSULTED, null_fraction=None) -> str:
    """One side's decision inputs, as text.

    A statistic the rule CONSULTED and did not get is spelled "unknown" rather than
    omitted or defaulted — "ndv unknown" and "ndv 1" send the reader to different
    places, and the point of recording a decision is that a wrong one is traceable
    to the number that caused it. A statistic the rule never looks at (the semi/anti
    exchange reads rows only) is left out entirely, so the record never implies a
    missing statistic mattered when it did not.
    """
    parts = [f"{rows:,} rows" if rows is not None else "rows unknown"]
    if ndv is not _NOT_CONSULTED:
        parts.append(f"ndv {ndv:,}" if ndv is not None else "ndv unknown")
    if null_fraction:
        parts.append(f"key null {null_fraction:.3f}")
    return ", ".join(parts)


class JoinOrderingStrategy(OptimizationStrategy):
    optimization_technique = "cost"
    requires = ("joins-planned",)

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if node.node_type == LogicalPlanStepType.Join and node.type == "cross join":
            # 1438
            pass

        # SEMI/ANTI: the build side is pinned to the RIGHT leg by compiler.py, which
        # also pins WHICH LEG IS MATERIALISED — and those are separate questions. When
        # the materialised leg is the far larger one, the join can be exchanged: build
        # the left leg with match tracking, stream the right one past it, then emit the
        # marked (SEMI) or unmarked (ANTI) build rows. Same rows, materialisation on the
        # other side. The exchange is BLOCKING, which is why the LIMIT check gates it.
        #
        # Only plain semi/anti. "left anti null-aware" (NOT IN) and the not-distinct
        # set-operation joins decide their answer from a property of the build side, so
        # exchanging the legs would change which relation that property is read from.
        if node.node_type == LogicalPlanStepType.Join and node.type in (
            "left semi",
            "left anti",
        ):
            left_stats, right_stats = self._side_statistics(
                context.pre_optimized_tree, context.node_id
            )
            left_rows = self._side_rows(left_stats, node.left_size)
            right_rows = self._side_rows(right_stats, node.right_size)
            # Absent statistics are fail-safe: keep today's shape rather than exchange
            # a join on a fabricated number.
            #
            # The three ways this declines are recorded apart, because they point at
            # different work: no statistics is an ESTIMATOR gap, a ratio under
            # _SWAP_BUILD_RATIO is the rule behaving as designed, and a LIMIT that
            # could short-circuit is a correctness gate that would refuse the exchange
            # however large the ratio got.
            sides = f"left {_side_facts(left_rows)}, right {_side_facts(right_rows)}"
            if not left_rows or not right_rows:
                self.record_decision(
                    f"{node.type} join exchange",
                    f"declined, no row statistics: {sides}",
                )
            elif right_rows < left_rows * _SWAP_BUILD_RATIO:
                self.record_decision(
                    f"{node.type} join exchange",
                    f"declined, ratio {_ratio_text(right_rows, left_rows)}x below the"
                    f" {_SWAP_BUILD_RATIO:g}x margin: {sides}",
                )
            elif _limit_can_short_circuit(context.pre_optimized_tree, context.node_id):
                self.record_decision(
                    f"{node.type} join exchange",
                    f"declined, a LIMIT above the join could stop the probe early"
                    f" (ratio {_ratio_text(right_rows, left_rows)}x): {sides}",
                )
            else:
                node.swap_build_side = True
                self.telemetry.optimization_semi_anti_build_side_swapped = (
                    getattr(
                        self.telemetry, "optimization_semi_anti_build_side_swapped", 0
                    )
                    + 1
                )
                self.record_decision(
                    f"{node.type} join exchange",
                    f"exchanged, ratio {_ratio_text(right_rows, left_rows)}x clears the"
                    f" {_SWAP_BUILD_RATIO:g}x margin: {sides}",
                )
                context.optimized_plan[context.node_id] = node

        if node.node_type == LogicalPlanStepType.Join and node.type == "inner":
            # Only reorder joins whose legs carry reader UUIDs. Joins without
            # them (window-function partitions, set-op / IN-subquery rewrites)
            # have a synthetic relation ($win-*, derived) on one side whose
            # statistics are meaningless for build-side selection anyway.
            # The non-equi / nested-loop classification below still runs for
            # these joins (it's a correctness concern), only the swap is gated.
            can_reorder = bool(node.left_readers) and bool(node.right_readers)

            should_swap = False
            if not can_reorder:
                # Not "nothing happened": the swap was skipped, and the reason is a
                # property of the PLAN (a synthetic leg), not of any statistic. A
                # reader chasing a bad build side needs to know the rules never ran.
                self.record_decision(
                    "inner join build side",
                    "kept, leg has no reader (synthetic relation): "
                    f"left {_side_facts(node.left_size)},"
                    f" right {_side_facts(node.right_size)}",
                )
            else:
                # Apply join ordering rules from COST-BASED-OPTIMIZER.md, fed from the
                # refreshed per-node statistics (post-filter row counts and join-key
                # NDV/null fractions) rather than the binder's pre-filter size estimate.
                left_stats, right_stats = self._side_statistics(
                    context.pre_optimized_tree, context.node_id
                )
                left_rows = self._side_rows(left_stats, node.left_size)
                right_rows = self._side_rows(right_stats, node.right_size)
                left_ndv = self._key_ndv(left_stats, node.left_columns)
                right_ndv = self._key_ndv(right_stats, node.right_columns)
                left_null = self._key_null_fraction(left_stats, node.left_columns)
                right_null = self._key_null_fraction(right_stats, node.right_columns)

                should_swap, rule = _decide_swap_reasoned(
                    left_rows, right_rows, left_ndv, right_ndv, left_null, right_null
                )
                # BOTH outcomes are recorded, with the same numbers. A build side left
                # where it was is a decision, not an absence of one — Q18 at SF100 was
                # 13.6s because rule 3 moved the 600M-row side onto the build leg, and
                # a report that only spoke when it swapped would have been silent about
                # every join it got right AND about the row-count guard that now stops
                # it. The rule names which numbers actually mattered.
                self.record_decision(
                    "inner join build side",
                    f"{'swapped' if should_swap else 'kept'} ({rule}): "
                    f"left {_side_facts(left_rows, left_ndv, left_null)},"
                    f" right {_side_facts(right_rows, right_ndv, right_null)}",
                )

            # Perform the swap if needed
            if should_swap:
                # fmt:off
                node.left_size, node.right_size = node.right_size, node.left_size
                node.left_columns, node.right_columns = node.right_columns, node.left_columns
                node.left_column, node.right_column = node.right_column, node.left_column
                node.left_readers, node.right_readers = node.right_readers, node.left_readers
                node.left_relation_names, node.right_relation_names = node.right_relation_names, node.left_relation_names
                # fmt:on
                flip_join_leg_labels(context.optimized_plan, context.node_id)
                self.telemetry.optimization_inner_join_smallest_table_left += 1
                context.optimized_plan[context.node_id] = node

            # A non-equi conjunct anywhere (pure theta, or mixed equi+theta) can't be
            # expressed as a hash-join key — nested loop is the only correct execution
            # strategy, unconditionally, regardless of row count. A PURE equi join
            # never takes this branch and always stays "inner" (hash join) — see the
            # module docstring for why there is no longer a nested-loop-vs-hash-join
            # cost trade-off to make for equi joins in the native engine.
            if _contains_non_equi_comparator(node.on):
                # ONE decision point. A band and a theta-nested-loop are two answers
                # to the same question -- "what execution strategy does a non-equi ON
                # clause need?" -- so they are an if/elif here rather than two
                # strategies that have to be kept from both claiming the shape.
                node.type = self._band_or_nested_loop(node)
                context.optimized_plan[context.node_id] = node

        return context

    def _band_or_nested_loop(self, node) -> str:
        """"band" when the ON clause is an equi-join plus a closed band on a BUILD-side
        column, otherwise "nested loop" — today's answer, unchanged.

        Unconditional for the shape, by ruling: the band form removes row emissions
        rather than making them cheaper, and its worst case (a band selecting the
        whole equi group) costs one sort per group against a hash-then-filter worst
        case that is unbounded. The join cardinality estimate this would otherwise be
        costed against is still ~180x low on frequency skew, i.e. wrong in exactly the
        direction that would decline the band. See docs/BAND_JOIN_PROPOSAL.md §Cost.
        """
        band = _recognize_band(node)
        if band is None:
            return "nested loop"
        identifier, banded_leg, lower_expr, lower_closed, upper_expr, upper_closed = band

        # `_compile_join` builds from the LEFT leg for INNER (mode 0) and probes the
        # right. The banded column is the one the build side gets SORTED by, so it has
        # to be the left leg here.
        #
        # The build side itself is NOT re-chosen for the band: the cardinality/NDV
        # rule and its row-count guard above have already run and stand. A band whose
        # column landed on the probe leg is invertible when its bounds are
        # `column ± literal` (`l.t <= f.t AND l.t > f.t - 20s` IS
        # `f.t >= l.t AND f.t < l.t + 20s`), which would let the band apply in either
        # orientation. That inversion is NOT implemented yet — it needs the shifted
        # bound expressions synthesised — so this declines and the join runs exactly
        # as it does today.
        if banded_leg != "left":
            self.telemetry.optimization_band_join_declined_probe_side += 1
            self.record_decision(
                "band join",
                "declined: the banded column is on the probe leg and bound inversion"
                " is not implemented",
            )
            return "nested loop"

        node.band_column = identifier.schema_column.identity
        # Carried for EXPLAIN only. `band_column` is an IDENTITY, which is what the
        # compiler resolves against the build layout and what must never be replaced
        # by a name (names are not unique across a plan); the name rides alongside so
        # the plan line reads as the user's column rather than as a hash.
        node.band_column_name = identifier.schema_column.name
        node.band_lower = lower_expr
        node.band_lower_closed = lower_closed
        node.band_upper = upper_expr
        node.band_upper_closed = upper_closed
        self.telemetry.optimization_band_join += 1
        self.record_decision(
            "band join",
            f"applied: {identifier.schema_column.name} bounded"
            f" {'[' if lower_closed else '('}lower,"
            f" upper{']' if upper_closed else ')'} within the equi key",
        )
        return "band"

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan

    def should_i_run(self, plan):
        # only run if there are LIMIT clauses in the plan
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Join,))
        return len(candidates) > 0

    @staticmethod
    def _side_statistics(plan, join_nid):
        """Return (left_stats, right_stats) RelationStatistics for the join's two
        inputs, identified by the 'left'/'right' edge labels. Either may be None
        when statistics are absent or a side is unlabelled.

        Cross-join→inner-converted joins carry unlabelled ingoing edges
        (label ``None``); without a fallback every such join would read the
        binder's pre-filter size estimate instead of the refreshed post-filter
        statistics. When labels are missing we fall back to ingoing-edge
        insertion order (left, then right) — mirroring
        ``statistics_refresh._split_join_children``.
        """
        left = right = None
        ordered = []
        for child_nid, _, label in plan.ingoing_edges(join_nid):
            stats = getattr(plan[child_nid], "statistics", None)
            ordered.append(stats)
            if label == "left":
                left = stats
            elif label == "right":
                right = stats
        if left is None and ordered:
            left = ordered[0]
        if right is None and len(ordered) > 1:
            right = ordered[1]
        return left, right

    @staticmethod
    def _side_rows(stats, fallback):
        """Post-filter row count for a side, falling back to the binder estimate."""
        if stats is not None and getattr(stats, "row_count", None) is not None:
            return stats.row_count
        return fallback

    @staticmethod
    def _key_ndv(stats, key_columns):
        """Composite join-key NDV for a side, or None when unavailable.

        Composition across the key columns is ``composite_key_ndv`` (max of
        the known per-column NDVs) -- the same helper the cardinality
        estimator uses, so the build-side chooser and the estimator read the
        SAME NDV for the same join. This used to take ``min``, which
        understates a composite key's domain.
        """
        if stats is None:
            return None
        ndvs = []
        for col in key_columns or []:
            identity = _join_key_identity(col)
            col_stats = stats.get_column(identity) if identity is not None else None
            if col_stats is not None and col_stats.distinct_count is not None:
                ndvs.append(col_stats.distinct_count)
        return composite_key_ndv(ndvs)

    @staticmethod
    def _key_null_fraction(stats, key_columns):
        """Worst-case (highest) join-key null fraction for a side, or None."""
        if stats is None:
            return None
        fractions = []
        for col in key_columns or []:
            identity = _join_key_identity(col)
            col_stats = stats.get_column(identity) if identity is not None else None
            if col_stats is not None and col_stats.null_fraction is not None:
                fractions.append(col_stats.null_fraction)
        return max(fractions) if fractions else None
