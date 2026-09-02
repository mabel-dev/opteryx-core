# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Join Condition Hoist

Type: Heuristic / Correctness
Goal: Every ON-clause conjunct is either a join condition the engine evaluates,
      a WHERE predicate, or a loud refusal — never silently dropped.

A join's ON clause is an AND of conjuncts, and each conjunct is one of:

  * an EQUALITY between the two legs        — the join key (stays in ON)
  * a two-sided NON-EQUALITY comparison      — a theta residual the INNER join
                                              evaluates per pair (stays in ON;
                                              the compiler refuses it on every
                                              other join type)
  * a two-sided condition that is NOT a comparison — an OR, a NOT, a CNF, a
    boolean function. The INNER join's residual channel reads comparison
    leaves only, so this used to reach execution and be DROPPED:
    `ON p.id = s.id AND (p.id = 3 OR s.id = 4)` answered the unfiltered join.
  * a SINGLE-LEG condition                   — a filter on that leg, not a
                                              join condition at all

For an INNER join, `ON k AND f` is exactly `ON k WHERE f`, so every conjunct
that is not a key or a theta comparison is moved into ONE Filter node directly
above the join. From there it is an ordinary WHERE predicate: every expression
rewrite in this optimizer (they visit Filter nodes only — boolean and disjunction
simplification, domain derivation, the IN-list / LIKE / TRUNC rewrites,
compaction) sees it, the split hands each conjunct its own node, pushdown places
a single-leg predicate on its leg, and a two-sided one stays above the join
where it is evaluated over the joined rows. PredicatePushdown does carve
single-leg ON conjuncts out itself, but it runs AFTER all of those rewrites, so
an ON-clause `(a.x = 1 OR a.x = 2)` reached the leg as a bare OR while the
identical WHERE spelling became `a.x IN (1, 2)`.

For an OUTER (and ANTI) join the equivalence does not hold: a conjunct on the
BUILD side is a match-candidate pre-filter that PredicatePushdown already
applies before the join (correctly), but a conjunct on the PRESERVED side is
neither a pre-filter (it would drop preserved rows that should surface with
NULLs) nor a WHERE (same), and the engine's outer join has no residual channel
to evaluate it per pair. Those used to be applied as a WHERE — `p LEFT JOIN s ON
p.id = s.id AND p.id > 3` returned 6 rows instead of 9 — and a two-sided
non-comparison crashed a later strategy before the compiler could refuse it.
Both are refused HERE, by name, with the rewrite that expresses what the user
most likely meant. A LEFT SEMI join is the one non-inner type where a
preserved-side conjunct IS a WHERE (a semi join emits exactly the left rows that
match), so it is left for pushdown as before.

Runs first (after decorrelation, which mints joins of its own). Only the join
types named in `_PRESERVED_LEGS` / "inner" / "left semi" are touched; ASOF,
UNNEST and CROSS joins have no ON in this sense and are left alone.
"""

from opteryx.exceptions import UnsupportedSyntaxError, compose, md_code, md_syntax
from opteryx.expression import NodeType, format_expression, get_all_nodes_of_type
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from opteryx.utils import random_string

from .disjunction_simplification import _build_and, _split_and
from .optimization_strategy import (
    OptimizationStrategy,
    OptimizerContext,
    filter_referenced_columns,
    get_nodes_of_type_from_logical_plan,
)

# Which leg(s) an outer/anti join PRESERVES — rows from that leg surface even with
# no match, so a filter on it cannot be applied before OR after the join.
_PRESERVED_LEGS = {
    "left outer": ("left",),
    "right outer": ("right",),
    "full outer": ("left", "right"),
    "left anti": ("left",),
}
_HANDLED_TYPES = frozenset(_PRESERVED_LEGS) | {"inner", "left semi"}


def _identifier_sources(expression) -> set:
    return {
        identifier.source
        for identifier in get_all_nodes_of_type(expression, (NodeType.IDENTIFIER,))
        if identifier.source is not None
    }


def _unwrap(node):
    while node is not None and node.node_type == NodeType.NESTED:
        node = node.centre
    return node


def _is_comparison(conjunct) -> bool:
    return _unwrap(conjunct).node_type == NodeType.COMPARISON_OPERATOR


def _is_equality(conjunct) -> bool:
    unwrapped = _unwrap(conjunct)
    return unwrapped.node_type == NodeType.COMPARISON_OPERATOR and unwrapped.value == "Eq"


def _refuse_preserved_side(conjunct, join_type: str):
    raise UnsupportedSyntaxError(
        compose(
            f"the {md_syntax('ON')} condition {md_code(format_expression(conjunct))} filters only "
            f"the preserved side of a {md_syntax(join_type)} join, which is not supported",
            f"Move it to the {md_syntax('WHERE')} clause if rows without a match should be "
            "dropped, or if they should be kept and the column is the join key, write the "
            "condition against the other relation's key column instead",
        )
    )


def _refuse_two_sided(conjunct, join_type: str):
    raise UnsupportedSyntaxError(
        compose(
            f"the {md_syntax('ON')} condition {md_code(format_expression(conjunct))} is not an "
            f"equality between the two relations, on a {md_syntax(join_type)} join",
            f"Only an {md_syntax('INNER JOIN')} supports a non-equality join condition",
        )
    )


class JoinConditionHoistStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if (
            node.node_type != LogicalPlanStepType.Join
            or node.type not in _HANDLED_TYPES
            or node.on is None
        ):
            return context

        left_names = set(node.left_relation_names or ())
        right_names = set(node.right_relation_names or ())
        if not left_names or not right_names:
            return context
        join_names = left_names | right_names
        preserved = _PRESERVED_LEGS.get(node.type, ())

        hoisted = []
        kept = []
        for conjunct in _split_and(node.on):
            sources = _identifier_sources(conjunct)
            touches_left = bool(sources & left_names)
            touches_right = bool(sources & right_names)
            single_leg = bool(sources) and (touches_left ^ touches_right) and sources <= join_names
            two_sided = touches_left and touches_right and sources <= join_names

            if node.type == "inner":
                # key and theta comparisons stay; everything else is a WHERE
                if two_sided and _is_comparison(conjunct):
                    kept.append(conjunct)
                elif single_leg or two_sided:
                    hoisted.append(conjunct)
                else:
                    kept.append(conjunct)
                continue

            # outer / semi / anti
            if two_sided:
                if not _is_equality(conjunct):
                    _refuse_two_sided(conjunct, node.type)
                kept.append(conjunct)
            elif single_leg:
                leg = "left" if touches_left else "right"
                if leg in preserved:
                    _refuse_preserved_side(conjunct, node.type)
                # build-side (or semi-join left-side) filter: pushdown applies it
                # before the join — the correct place — leave it in the ON clause
                kept.append(conjunct)
            else:
                kept.append(conjunct)

        if not hoisted:
            return context
        if not kept:
            # Every conjunct was a filter: there is no join key. The same refusal
            # PredicatePushdown gives a keyless INNER join, raised before any
            # strategy can see an ON-less inner join.
            raise UnsupportedSyntaxError(
                f"{md_syntax('INNER JOIN')} has no valid conditions, did you mean "
                f"{md_syntax('CROSS JOIN')}?"
            )

        node.on = _build_and(kept)
        node.columns = get_all_nodes_of_type(node.on, (NodeType.IDENTIFIER,))
        context.optimized_plan[context.node_id] = node

        condition = _build_and(hoisted)
        filter_node = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
        filter_node.condition = condition
        filter_node.columns = filter_referenced_columns(condition)
        relations = set()
        for column in filter_node.columns:
            if column.source is not None:
                relations.add(column.source)
            if column.schema_column is not None:
                relations.update(column.schema_column.origin or [])
        filter_node.relations = relations
        filter_node.all_relations = set(node.all_relations or ())

        # Inserted in complete(): splicing a node onto this join's outgoing edge
        # while the traversal is still walking down from it is the mid-traversal
        # mutation predicate_pushdown documents as unsafe.
        context.bag.setdefault("join_condition_hoist", []).append((context.node_id, filter_node))
        self.telemetry.optimization_join_condition_hoist += len(hoisted)
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        for join_nid, filter_node in context.bag.get("join_condition_hoist", []):
            plan.insert_node_after(random_string(), filter_node, join_nid)
        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return any(
            node.type in _HANDLED_TYPES and node.on is not None
            for _, node in get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Join,))
        )
