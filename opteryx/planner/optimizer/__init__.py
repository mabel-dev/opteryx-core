# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This module implements a cost-based query optimizer using the Visitor pattern. Unlike the binder,
which processes the logical plan from the scanners up to the projection, this optimizer starts at
the projection and traverses down towards the scanners. This top-down approach is effective for
the primary activities involved in optimization, such as splitting nodes, performing individual
node rewrites, and pushing down predicates and projections.

The optimizer applies a series of strategies, each encapsulating a specific optimization rule.
These strategies are applied sequentially, allowing for incremental improvements to the logical
plan.

Key Concepts:
- Visitor Pattern: Used to traverse and modify the logical plan.
- Strategies: Encapsulate individual optimization rules, applied either per-node or per-plan.
- Context: Maintains the state during optimization, including the pre-optimized and optimized plans.

The `CostBasedOptimizerVisitor` class orchestrates the optimization process by applying each
strategy in sequence. The `do_optimizer` function serves as the entry point for optimizing a
logical plan.

Example Usage:
    optimized_plan = do_optimizer(logical_plan)

This module aims to enhance query performance through systematic and incremental optimization steps.
"""

from opteryx import config
from opteryx.config import DISABLE_OPTIMIZER
from opteryx.config import VALIDATE_OPTIMIZER_PLANS
from opteryx.exceptions import InvalidInternalStateError
from opteryx.models import QueryTelemetry
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.optimizer.plan_validator import validate_plan
from opteryx.planner.optimizer.strategies import (
    BooleanSimplificationStrategy,
    CastSimplificationStrategy,
    ConstantFoldingStrategy,
    CorrelatedFiltersStrategy,
    CrossJoinChainReorderStrategy,
    DecorrelateSubqueryStrategy,
    CrossJoinFilterPushdownStrategy,
    DisjunctionSimplificationStrategy,
    DisjunctiveDomainPushdownStrategy,
    DistinctPushdownStrategy,
    FilterImpliedGroupKeyReductionStrategy,
    FunctionRewriteStrategy,
    GroupKeyReductionStrategy,
    HashMapVariantStrategy,
    JoinEliminationStrategy,
    JoinOrderingStrategy,
    JoinPlanningStrategy,
    JoinRewriteStrategy,
    LengthOnlyColumnStrategy,
    LimitEliminationStrategy,
    LimitFilesPruningStrategy,
    LimitPushdownStrategy,
    ManifestPruningStrategy,
    OperatorFusionStrategy,
    PredicateCompactionStrategy,
    PredicateOrderingStrategy,
    PredicatePushdownStrategy,
    PredicateRewriteStrategy,
    ProjectFusionStrategy,
    ProjectionPushdownStrategy,
    RedundantCastEliminationStrategy,
    RedundantOperationsStrategy,
    SplitConjunctivePredicatesStrategy,
    StatisticsOnlyResponseStrategy,
    TimestampCastSinkStrategy,
    TopNManifestPruningStrategy,
    TopNScanPushdownStrategy,
    WindowTopKFusionStrategy,
)

from .statistics_refresh import refresh_statistics
from .strategies.optimization_strategy import OptimizerContext

__all__ = ["do_optimizer"]


# Strategy class name -> opteryx.config.Features flag name. One entry per strategy
# in OptimizerVisitor.strategies, checked centrally in optimize() so every strategy
# gets an A/B kill-switch without each one hand-rolling its own flag check. A few
# strategies (PredicateOrdering/PredicatePushdown/ManifestPruning) ALSO check their
# own flag inline in should_i_run from before this table existed — listing them here
# too is harmless (same flag, checked twice) and keeps this the complete registry.
_STRATEGY_DISABLE_FLAGS = {
    "BooleanSimplificationStrategy": "disable_boolean_simplification",
    "CastSimplificationStrategy": "disable_cast_simplification",
    "ConstantFoldingStrategy": "disable_constant_folding",
    "CorrelatedFiltersStrategy": "disable_correlated_filters",
    "DecorrelateSubqueryStrategy": "disable_decorrelate_subquery",
    "CrossJoinFilterPushdownStrategy": "disable_cross_join_filter_pushdown",
    "DisjunctionSimplificationStrategy": "disable_disjunction_simplification",
    "DisjunctiveDomainPushdownStrategy": "disable_disjunctive_domain_pushdown",
    "DistinctPushdownStrategy": "disable_distinct_pushdown",
    "FilterImpliedGroupKeyReductionStrategy": "disable_filter_implied_group_key_reduction",
    "FunctionRewriteStrategy": "disable_function_rewrite",
    "GroupKeyReductionStrategy": "disable_group_key_reduction",
    "HashMapVariantStrategy": "disable_hash_map_variant",
    "JoinEliminationStrategy": "disable_join_elimination",
    "JoinOrderingStrategy": "disable_join_ordering",
    "JoinPlanningStrategy": "disable_join_planning",
    "JoinRewriteStrategy": "disable_join_rewrite",
    "LengthOnlyColumnStrategy": "disable_length_only_column",
    "LimitEliminationStrategy": "disable_limit_elimination",
    "LimitFilesPruningStrategy": "disable_limit_files_pruning",
    "LimitPushdownStrategy": "disable_limit_pushdown",
    "ManifestPruningStrategy": "disable_manifest_pruning",
    "OperatorFusionStrategy": "disable_operator_fusion",
    "PredicateCompactionStrategy": "disable_predicate_compaction",
    "PredicateOrderingStrategy": "disable_predicate_ordering",
    "PredicatePushdownStrategy": "disable_predicate_pushdown",
    "PredicateRewriteStrategy": "disable_predicate_rewrite",
    "ProjectFusionStrategy": "disable_project_fusion",
    "ProjectionPushdownStrategy": "disable_projection_pushdown",
    "RedundantCastEliminationStrategy": "disable_redundant_cast_elimination",
    "RedundantOperationsStrategy": "disable_redundant_operations",
    "SplitConjunctivePredicatesStrategy": "disable_split_conjunctive_predicates",
    "StatisticsOnlyResponseStrategy": "disable_statistics_only_response",
    "TimestampCastSinkStrategy": "disable_timestamp_cast_sink",
    "TopNManifestPruningStrategy": "disable_topn_manifest_pruning",
    "TopNScanPushdownStrategy": "disable_topn_scan_pushdown",
    "WindowTopKFusionStrategy": "disable_window_topk_fusion",
}


def _validate_strategy_order(strategies) -> None:
    """Assert the declared ordering contract between strategies (WP-2).

    Each strategy may declare ``requires`` capability tokens that must be
    ``provides``-ed by a strategy ordered *earlier* in the pipeline (see the
    declarations on the strategy classes). This walks the list once and fails
    loudly if a requirement is unmet, so a careless reorder of
    ``OptimizerVisitor.strategies`` — or a typo in a token — is caught at
    construction time rather than surfacing as a wrong plan much later.

    Purely structural: it inspects the list order, never the plan, and runs
    once per OptimizerVisitor construction.
    """
    all_provided: set = set()
    for strategy in strategies:
        all_provided.update(strategy.provides)

    provided_so_far: set = set()
    for strategy in strategies:
        name = type(strategy).__name__
        for token in strategy.requires:
            if token not in all_provided:
                raise InvalidInternalStateError(
                    f"Optimizer strategy {name} requires capability '{token}', but no "
                    "strategy in the pipeline provides it (typo in a requires/provides "
                    "token, or a missing strategy)."
                )
            if token not in provided_so_far:
                providers = ", ".join(
                    type(s).__name__ for s in strategies if token in s.provides
                )
                raise InvalidInternalStateError(
                    f"Optimizer strategy {name} requires capability '{token}' (provided "
                    f"by {providers}) to run before it, but {name} is ordered earlier. "
                    "Fix the order of OptimizerVisitor.strategies."
                )
        provided_so_far.update(strategy.provides)


class OptimizerVisitor:
    def __init__(self, telemetry: QueryTelemetry):
        """
        Initialize the OptimizerVisitor with a list of optimization strategies.
        Each strategy encapsulates a specific optimization rule.
        """
        self.telemetry = telemetry
        self.strategies = [
            # Removes scalar subqueries by turning them into joins. Must run
            # before any strategy that reasons about joins or pushes predicates,
            # since it introduces a join and moves a predicate across it.
            DecorrelateSubqueryStrategy(telemetry),
            ConstantFoldingStrategy(telemetry),
            # Drops no-op LIMITs before StatisticsOnlyResponseStrategy runs, so
            # e.g. `SELECT COUNT(*) FROM t LIMIT n` is answered from the
            # manifest in this same pass instead of falling through to a scan.
            LimitEliminationStrategy(telemetry),
            StatisticsOnlyResponseStrategy(telemetry),
            BooleanSimplificationStrategy(telemetry),
            RedundantCastEliminationStrategy(telemetry),  # CAST(x AS T) where x is T -> x
            CastSimplificationStrategy(telemetry),  # DISABLED: Causes plan corruption
            DisjunctionSimplificationStrategy(telemetry),
            # Derives implied-but-weaker per-column domain predicates (IN-list or
            # range) from an OR-of-AND filter that DisjunctionSimplification couldn't
            # factor (its branches share no identical predicate — e.g. TPC-H Q7's
            # bilateral trade filter). ANDs them onto the untouched OR so the split
            # below turns them into their own pushable Filter steps.
            DisjunctiveDomainPushdownStrategy(telemetry),
            SplitConjunctivePredicatesStrategy(telemetry),
            PredicateRewriteStrategy(telemetry),
            FunctionRewriteStrategy(telemetry),
            GroupKeyReductionStrategy(telemetry),
            PredicateCompactionStrategy(telemetry),
            JoinPlanningStrategy(telemetry),  # Cost-based DPccp; no-op when flag off
            PredicatePushdownStrategy(telemetry),
            CrossJoinFilterPushdownStrategy(
                telemetry
            ),  # Convert CROSS JOIN + filters to INNER JOIN
            # Runs after pushdown so join-key ranges (from scan predicates) are
            # propagated; pushes the realized range onto the opposite scan.
            CorrelatedFiltersStrategy(telemetry),
            ManifestPruningStrategy(telemetry),  # Apply after predicate pushdown
            FilterImpliedGroupKeyReductionStrategy(telemetry),
            ProjectionPushdownStrategy(telemetry),
            # Sink pure-retag INT64::TIMESTAMP[unit] casts into the scan output
            # type (reader retags; cast resolves to identity). After projection
            # pushdown so scan.columns/scan.predicates are settled.
            TimestampCastSinkStrategy(telemetry),
            JoinEliminationStrategy(telemetry),
            JoinRewriteStrategy(telemetry),
            JoinOrderingStrategy(telemetry),
            DistinctPushdownStrategy(telemetry),
            OperatorFusionStrategy(telemetry),
            TopNScanPushdownStrategy(telemetry),  # WP-2: top-N spec onto scan feeding HeapSort
            TopNManifestPruningStrategy(telemetry),  # prune files using topn spec + manifest min/max
            LimitPushdownStrategy(telemetry),
            LimitFilesPruningStrategy(telemetry),  # Prune files for LIMIT queries (after pushdown)
            #            EmptyTableStrategy(telemetry),
            PredicateOrderingStrategy(telemetry),
            # Strips no-op Subquery boundary nodes (among other redundant nodes) —
            # a `FROM (SELECT ...)` leaves a Subquery node between the two Project
            # nodes it wraps, so Project<->Project fusion must run AFTER this, once
            # the pair is directly adjacent, not before.
            RedundantOperationsStrategy(telemetry),
            # Fuses adjacent Project->Project pairs into one physical pass. After
            # ProjectionPushdownStrategy so it sees already-pruned column lists,
            # and after RedundantOperationsStrategy so Subquery boundary nodes
            # between two Projects are already gone.
            ProjectFusionStrategy(telemetry),
            # After RedundantOperationsStrategy/ProjectFusionStrategy so the chain
            # between a ranking Window and its `WHERE rank <= K` filter is already
            # collapsed (no Subquery boundary, adjacent Projects fused) — fewer hops
            # for the search to walk through.
            WindowTopKFusionStrategy(telemetry),
            ConstantFoldingStrategy(telemetry),
            # Runs last: all other strategies have had their say.
            # Uses FileEntry.stats_by_name for range detection — projection-stable.
            HashMapVariantStrategy(telemetry),
            # Runs dead last: it enumerates every reference to a column, so
            # every strategy that can add, remove or rewrite one must already
            # have run. Annotates scans only — it rewrites nothing.
            LengthOnlyColumnStrategy(telemetry),
        ]
        _validate_strategy_order(self.strategies)

    def traverse(self, plan: LogicalPlan, strategy) -> LogicalPlan:
        """
        Traverse the logical plan tree and apply the given optimization strategy.

        Parameters:
            plan (LogicalPlan): The logical plan to optimize.
            strategy: The optimization strategy to apply.

        Returns:
            LogicalPlan: The optimized logical plan.
        """
        exit_points = plan.get_exit_points()
        if not exit_points:
            # Empty plan, return as-is
            return plan

        root_nid = exit_points.pop()
        context = OptimizerContext(plan)

        def _inner(nid, parent_nid, context):
            node = context.pre_optimized_tree[nid]
            context.node_id = nid
            context.parent_nid = parent_nid
            context = strategy.visit(node, context)

            for child, _, _ in plan.ingoing_edges(nid):
                _inner(child, nid, context)

        _inner(root_nid, None, context)
        # some strategies operate on the entire plan at once, or need to be told
        # there's no more nodes, we handle both with the .complete
        optimized_plan = strategy.complete(context.optimized_plan, context)
        return optimized_plan

    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        """
        Optimize the logical plan by applying all registered strategies in sequence.

        Parameters:
            plan (LogicalPlan): The logical plan to optimize.

        Returns:
            LogicalPlan: The fully optimized logical plan.
        """
        current_plan = plan
        # Plans enter the optimizer with no propagated statistics, so treat them
        # as stale until refresh_statistics has populated per-node estimates.
        current_plan.statistics_are_stale = True
        for strategy in self.strategies:
            flag_name = _STRATEGY_DISABLE_FLAGS.get(type(strategy).__name__)
            if flag_name is not None and getattr(config.features, flag_name):
                continue
            if strategy.should_i_run(current_plan):
                if (
                    strategy.optimization_technique == "cost"
                    and getattr(current_plan, "statistics_are_stale", True)
                ):
                    current_plan = refresh_statistics(current_plan, telemetry=self.telemetry)
                before = (len(current_plan), len(current_plan.edges()))
                current_plan = self.traverse(current_plan, strategy)
                self.telemetry.add_plan_rewrite(
                    "optimizer",
                    strategy.__class__.__name__,
                    before,
                    (len(current_plan), len(current_plan.edges())),
                )
                if VALIDATE_OPTIMIZER_PLANS:
                    # Debug guardrail (WP-3): localise plan corruption to the
                    # strategy that produced it. Off by default; zero cost then.
                    validate_plan(current_plan, where=strategy.__class__.__name__)
                if strategy.optimization_technique != "cost":
                    # Heuristic strategies that ran may have rewritten the plan;
                    # invalidate stats so the next cost-based strategy refreshes.
                    current_plan.statistics_are_stale = True
                ## DEBUG: print(f"AFTER {strategy.__class__.__name__}")
                ## DEBUG: print(current_plan.draw())
        # DEBUG: print("AFTER OPTIMIZATION")
        # DEBUG: print(current_plan.draw())
        return current_plan


def do_optimizer(plan: LogicalPlan, telemetry: QueryTelemetry) -> LogicalPlan:
    """
    Perform optimization on the given logical plan.

    Parameters:
        plan (LogicalPlan): The logical plan to optimize.
        telemetry (QueryTelemetry)

    Returns:
        LogicalPlan: The optimized logical plan.
    """
    if DISABLE_OPTIMIZER:  # pragma: no cover
        message = "[OPTERYX] The optimizer has been disabled, 'DISABLE_OPTIMIZER' variable is TRUE."
        print(message)
        telemetry.add_message(message)
        return plan
    optimizer = OptimizerVisitor(telemetry)
    return optimizer.optimize(plan)
