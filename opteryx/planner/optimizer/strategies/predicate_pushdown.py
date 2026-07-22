# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Predicate Pushdown

Type: Heuristic
Goal: Filter rows as early as possible

One main heuristic strategy is it eliminate rows to be processed as early
as possible, to do that we try to push filter conditions to as close to the
read step as much as possible, including pushing to the system actually
performing the read.

This eliminates rows to be processed as early as possible to reduce the
number of steps and processes each row goes through.

We also push filters into JOIN conditions, the more restrictive and fewer
the number of rows returned from a JOIN the better, so rather than filter
after a join, we add conditions to the JOIN.
"""

from opteryx.connectors.capabilities import PredicatePushable
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType, format_expression, get_all_nodes_of_type
from opteryx.expression.formatter import ExpressionColumn
from opteryx.models import Node
from opteryx.planner.binder.common import extract_join_fields
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from opteryx.types.logical_type import LogicalCategory, ColumnType, BOOLEAN as _CT_BOOLEAN
from opteryx.types.logical_type import LogicalCategory as LC
from opteryx.utils import random_string

from .optimization_strategy import OptimizationStrategy, OptimizerContext
from .predicate_rewriter import rewrite_date_trunc_to_range

# Comparison ops that rewrite_date_trunc_to_range (predicate_rewriter.py) knows how
# to turn into a raw-column range/literal — the set of ops the TRUNC-alias inline
# below is allowed to attempt.
_TRUNC_REWRITE_OPS: frozenset = frozenset({"Eq", "NotEq", "Lt", "LtEq", "Gt", "GtEq"})


def _emitted_identities(plan, nid, memo):
    """Column identities emitted by the subtree rooted at `nid`.

    A predicate may only be placed (as a Filter) above a node that emits every
    column it references — otherwise it would reference a column not present in
    that node's output. Scans/FunctionDatasets originate their schema columns;
    Project emits exactly its projected (+ order-by) columns; Aggregate emits its
    aggregates + group keys; everything else passes its children's columns through.
    Memoised per call; safe because emitted sets are determined by these
    column-defining nodes, which Filter removal/insertion doesn't change.
    """
    cached = memo.get(nid)
    if cached is not None:
        return cached
    memo[nid] = frozenset()  # cycle guard
    node = plan[nid]
    nt = node.node_type
    if nt in (LogicalPlanStepType.Scan, LogicalPlanStepType.FunctionDataset):
        sch = node.schema
        ids = frozenset(c.identity for c in sch.columns) if sch is not None else frozenset()
    elif nt == LogicalPlanStepType.Project:
        cols = list(node.columns or []) + list(getattr(node, "order_by_columns", None) or [])
        ids = frozenset(c.schema_column.identity for c in cols if c.schema_column is not None)
    elif nt in (LogicalPlanStepType.Aggregate, LogicalPlanStepType.AggregateAndGroup):
        cols = list(node.aggregates or []) + list(node.groups or [])
        ids = frozenset(c.schema_column.identity for c in cols if c.schema_column is not None)
    else:
        acc = set()
        for child, _, _ in plan.ingoing_edges(nid):
            acc |= _emitted_identities(plan, child, memo)
        ids = frozenset(acc)
    memo[nid] = ids
    return ids


def _predicate_column_ids(predicate):
    """The set of column identities a predicate references."""
    cond = predicate.condition if getattr(predicate, "condition", None) is not None else predicate
    out = set()
    for ident in get_all_nodes_of_type(cond, (NodeType.IDENTIFIER,)):
        sc = getattr(ident, "schema_column", None)
        if sc is not None and sc.identity is not None:
            out.add(sc.identity)
    return out


def _deep_pushdown_target(plan, start_nid, predicate_ids, group_key_identity, emit_memo):
    """Deepest node at/below `start_nid` whose OWN emitted identities cover
    `predicate_ids`, reachable by descending single-child hops that a filter
    may legally cross. Returns the target nid, or None if nothing reachable
    emits the predicate's columns.

    A filter placed directly above the returned node is valid (that node's
    output stream carries every column the filter references) and is reached
    by crossing only nodes it is sound to push a filter past:

      - Project nodes — row-count-preserving, so filtering above vs. below is
        equivalent.
      - the single Aggregate/AggregateAndGroup whose group keys include
        `group_key_identity`. Crossing an aggregate is sound ONLY because the
        caller guarantees the predicate is group-invariant for it: a
        unit-aligned range on the raw column that this group key is TRUNC()'d
        from is constant within every group (all rows of a group share one
        truncated bucket, and the range is exactly that bucket's boundaries),
        so filtering it pre-aggregation removes exactly the groups it would
        remove post-aggregation. A non-monotonic or non-unit-aligned predicate
        would NOT be group-invariant, which is why `group_key_identity` gates
        this to the one aggregate whose key generated the rewrite.

    The walk STOPS (never guesses) at any branching node (Join/Union — more
    than one child) and any other single-child node type (Limit/Distinct/
    Unnest/...), because pushing a filter past those can change the result.
    """
    current = start_nid
    best = None
    while current is not None:
        node = plan[current]
        if predicate_ids <= _emitted_identities(plan, current, emit_memo):
            best = current
        children = list(plan.ingoing_edges(current))
        if len(children) != 1:
            break
        nt = node.node_type
        if nt == LogicalPlanStepType.Project:
            pass  # row-count-preserving: always crossable by a filter
        elif nt in (LogicalPlanStepType.Aggregate, LogicalPlanStepType.AggregateAndGroup):
            group_ids = {
                g.schema_column.identity
                for g in (node.groups or [])
                if g.schema_column is not None
            }
            if group_key_identity is None or group_key_identity not in group_ids:
                break
        else:
            break
        current = children[0][0]
    return best


def _add_condition(existing_condition, new_condition):
    if not existing_condition:
        return new_condition
    _and = Node(node_type=NodeType.AND)
    _and.left = new_condition
    _and.right = existing_condition
    return _and


def _unwrap_nested(expression):
    """Peel NESTED wrappers off an alias-expression template.

    ProjectFusionStrategy inlines a bare-passthrough alias (e.g. `billing_date`
    re-exporting a lower Project's `TRUNC(x, 'day') AS billing_date`) by wrapping
    the inlined expression in a NESTED node so the original schema_column/alias/
    query_column survive on the wrapper (see project_fusion.py's
    `_substitute_column`). The TRUNC-alias detection below matches on
    `node_type == FUNCTION and value == "TRUNC"`, which a NESTED wrapper fails
    even though its `.centre` is exactly that FUNCTION node — unwrap first so
    fusion doesn't silently starve this rewrite."""
    while isinstance(expression, Node) and expression.node_type == NodeType.NESTED:
        expression = expression.centre
    return expression


# Microseconds per unit for each CAST target type name (from cast_node.value).
# _TIMESTAMP_NS is sub-µs (fractional scale) so is intentionally excluded.
_LITERAL_SCALE_US: dict = {
    "DATE":            86_400_000_000,
    "_TIMESTAMP_DAYS": 86_400_000_000,
    "_TIMESTAMP_S":    1_000_000,
    "_TIMESTAMP_MS":   1_000,
    "_TIMESTAMP_US":   1,
}

# Microseconds per column-unit for LogicalCategory that can appear as a CAST target.
_COL_SCALE_US: dict = {
    LogicalCategory.DATE:      86_400_000_000,  # stores int32 days
    LogicalCategory.TIMESTAMP: 1,               # stores int64 µs
}

# When l_scale > c_scale (the CAST truncates via floor division), LtEq and Gt
# need the literal bumped by one column-unit and the operator flipped so that
# the pushed predicate has the same row set as the original CAST predicate.
_FLOOR_CAST_OP_ADJUST: dict = {
    "LtEq": "Lt",
    "Gt":   "GtEq",
}

_INVERT_COMPARISON_OP: dict = {
    "Gt": "Lt", "GtEq": "LtEq", "Lt": "Gt", "LtEq": "GtEq",
    "Eq": "Eq", "NotEq": "NotEq",
}

_SIMPLE_COMPARISON_OPS: frozenset = frozenset({"Eq", "Lt", "LtEq", "Gt", "GtEq", "NotEq"})


def _get_equi_join_pairs(on_node):
    """Extract (left_col, right_col) IDENTIFIER pairs from an equi-join ON condition."""
    if on_node is None:
        return []
    if on_node.node_type == NodeType.AND:
        return _get_equi_join_pairs(on_node.left) + _get_equi_join_pairs(on_node.right)
    if (
        on_node.node_type == NodeType.COMPARISON_OPERATOR
        and on_node.value == "Eq"
        and getattr(on_node, "left", None) is not None
        and getattr(on_node, "right", None) is not None
        and on_node.left.node_type == NodeType.IDENTIFIER
        and on_node.right.node_type == NodeType.IDENTIFIER
    ):
        return [(on_node.left, on_node.right)]
    return []


def _normalize_col_op_lit(condition):
    """Return (ident, op, literal) for a simple col OP literal predicate, col on left.

    Returns (None, None, None) if the condition is not a plain col-vs-scalar comparison.
    """
    if condition.node_type != NodeType.COMPARISON_OPERATOR:
        return None, None, None
    if condition.value not in _SIMPLE_COMPARISON_OPS:
        return None, None, None
    left = getattr(condition, "left", None)
    right = getattr(condition, "right", None)
    if left is None or right is None:
        return None, None, None
    if get_all_nodes_of_type(condition, (NodeType.FUNCTION, NodeType.CAST, NodeType.AGGREGATOR)):
        return None, None, None
    if left.node_type == NodeType.IDENTIFIER and right.node_type == NodeType.LITERAL:
        return left, condition.value, right
    if right.node_type == NodeType.IDENTIFIER and left.node_type == NodeType.LITERAL:
        return right, _INVERT_COMPARISON_OP[condition.value], left
    return None, None, None


def _make_implied_filter(op, target_col, lit_node):
    """Build a Filter LogicalPlanNode applying op between target_col and lit_node."""
    new_cond = Node(NodeType.COMPARISON_OPERATOR, value=op, left=target_col, right=lit_node)
    return LogicalPlanNode(
        node_type=LogicalPlanStepType.Filter,
        condition=new_cond,
        columns=[target_col],
        relations={target_col.source},
        all_relations={target_col.source},
        nid=random_string(),
    )



def _try_normalize_cast_predicate(condition: Node):
    """Strip CAST from CAST(IDENTIFIER) op LITERAL predicates and rescale the literal.

    Converts ``CAST(col, T2) op literal(T2)`` into ``col op rescaled_literal``
    where the literal value is converted from T2 units to col's native units using
    the ratio ``l_scale / c_scale`` (both expressed in microseconds).

    Returns a new condition Node, or None if the predicate cannot be normalised.
    """
    if condition.node_type != NodeType.COMPARISON_OPERATOR:
        return None

    op = condition.value
    left, right = condition.left, condition.right

    if left.node_type == NodeType.CAST and right.node_type == NodeType.LITERAL:
        cast_node, literal_node = left, right
    elif right.node_type == NodeType.CAST and left.node_type == NodeType.LITERAL:
        cast_node, literal_node = right, left
        op = _INVERT_COMPARISON_OP.get(op, op)
    else:
        return None

    identifier = getattr(cast_node, "left", None)
    if identifier is None or identifier.node_type != NodeType.IDENTIFIER:
        return None

    col_sc = getattr(identifier, "schema_column", None)
    cast_sc = getattr(cast_node, "schema_column", None)
    if col_sc is None or cast_sc is None:
        return None

    c_scale = _COL_SCALE_US.get(col_sc.category)
    l_scale = _LITERAL_SCALE_US.get(cast_node.value)
    if l_scale is None:
        return None
    # An INTEGER column being cast to a temporal type is a type assertion — the integer
    # stores values in the cast target's units already (e.g. EventDate::DATE stores days).
    if c_scale is None and col_sc.category == LogicalCategory.INTEGER:
        c_scale = l_scale
    if c_scale is None or l_scale < c_scale:
        return None  # unknown column type or literal is finer-grained than column

    literal_value = literal_node.value
    if not isinstance(literal_value, int):
        return None

    # Eq/NotEq across different scales can't be expressed as a single comparison.
    if l_scale != c_scale and op in ("Eq", "NotEq"):
        return None

    # LtEq / Gt with floor-truncating casts need +1 on the literal and a flipped op.
    adjusted_op = _FLOOR_CAST_OP_ADJUST.get(op, op) if l_scale != c_scale else op
    literal_adjust = 1 if op in _FLOOR_CAST_OP_ADJUST and l_scale != c_scale else 0

    rescaled = (literal_value + literal_adjust) * l_scale // c_scale

    new_literal = Node(node_type=NodeType.LITERAL)
    new_literal.value = rescaled
    new_literal.schema_column = col_sc

    new_condition = Node(node_type=NodeType.COMPARISON_OPERATOR)
    new_condition.value = adjusted_op
    new_condition.left = identifier
    new_condition.right = new_literal
    new_condition.schema_column = condition.schema_column
    return new_condition


class PredicatePushdownStrategy(OptimizationStrategy):
    provides = ("predicates-pushed",)

    def should_i_run(self, plan):
        from opteryx import config

        return not config.features.disable_predicate_pushdown

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type in (
            LogicalPlanStepType.Scan,
            LogicalPlanStepType.FunctionDataset,
        ):
            # Handle predicates specific to node types
            context = self._handle_predicates(node, context)
            context.optimized_plan.add_node(context.node_id, LogicalPlanNode(**node.properties))
            if context.last_nid:
                context.optimized_plan.add_edge(context.node_id, context.last_nid)

        elif node.node_type in (LogicalPlanStepType.Limit, LogicalPlanStepType.Union):
            # don't push filters past limits

            for predicate in context.collected_predicates:
                self.telemetry.optimization_predicate_pushdown += 1
                context.optimized_plan.insert_node_after(
                    random_string(), predicate, context.node_id
                )
            context.collected_predicates = []

        elif node.node_type == LogicalPlanStepType.Filter:
            self._inline_project_alias_predicates(node, context)
            # collect predicates we can probably push
            # A predicate is pushable if:
            # - It's a HAVING predicate (has aggregators), OR
            # - It references at least one relation, has no aggregations, and is a simple comparison
            has_agg = get_all_nodes_of_type(node.condition, (NodeType.AGGREGATOR,))
            identifiers = get_all_nodes_of_type(node.condition, (NodeType.IDENTIFIER,))

            # Allow pushdown if:
            # 1. Has aggregators (HAVING clause) - will be pushed into aggregate
            # 2. OR: references relations AND no aggregators AND simple comparison (regular predicate)
            is_simple_comparison = (
                node.condition.node_type
                in (
                    NodeType.COMPARISON_OPERATOR,
                    NodeType.BETWEEN,
                    NodeType.UNARY_OPERATOR,  # IsNull, IsNotNull, IsEmpty, IsNotEmpty
                )
                and len(identifiers) >= 1  # At least one column reference
            ) or (
                node.condition.node_type == NodeType.FUNCTION
                and len(identifiers) >= 1
                and getattr(getattr(node.condition, "schema_column", None), "category", None)
                == LogicalCategory.BOOLEAN
            )

            is_having_predicate = bool(has_agg)
            # A TRUNC-alias rewrite (_inline_project_alias_predicates) may have
            # produced an AND/OR/BETWEEN condition that isn't a "simple
            # comparison", but stamped an explicit, validated deep_restore_target
            # on the node. Such a predicate MUST be collected so complete() can
            # place it at that target -- leaving it in place (the `else` branch)
            # would strand it above the alias's Project, where the raw column it
            # now references isn't in the stream (a physical-plan compile crash).
            has_deep_target = node.deep_restore_target is not None
            is_regular_pushable = (
                len(node.relations) > 0
                and not has_agg
                and (is_simple_comparison or has_deep_target)
            )

            if is_having_predicate or is_regular_pushable:
                # record where the node was, so we can put it back
                node.nid = context.node_id
                node.plan_path = context.optimized_plan.trace_to_root(context.node_id)

                context.collected_predicates.append(node)
                context.optimized_plan.remove_node(context.node_id, heal=True)
            else:
                context.optimized_plan[context.node_id] = node

        elif node.node_type == LogicalPlanStepType.Project:
            # A predicate collected above this Project is expressed in terms of its
            # OUTPUT schema. Passthrough/renamed columns preserve their source
            # SchemaColumn's identity end-to-end (binder.locate_identifier reuses
            # the same object rather than minting a new one), so a predicate
            # referencing only such columns can safely keep flowing down through
            # collected_predicates unchanged -- it will still correctly match
            # further down (by identity) at a Scan, Join, or Aggregate boundary.
            #
            # A predicate that also needs an identity only this Project's OWN
            # output defines (a computed expression -- FUNCTION/CAST/etc mint a
            # fresh identity, see SchemaColumn.__post_init__) cannot be relocated
            # below this Project. It is deliberately left untouched rather than
            # eagerly materialised here with insert_node_after: doing so mutates
            # this node's CURRENT outgoing edge mid-traversal, which is unsafe
            # when that edge is shared/aliased with a not-yet-visited ancestor
            # (e.g. an outer GROUP BY ALL reusing the SELECT list's expression
            # node -- see the copy-memo comment in
            # opteryx/compiled/structures/node.pyx) -- confirmed by a real
            # crash repro (a TRUNC() re-application above an aggregate lost its
            # function_ref) that reproduced ONLY with the eager insert_node_after,
            # not with the passive fallback below. complete() already restores
            # an unplaced predicate to its recorded original position via
            # plan_path, safely, at the end of the whole traversal instead.
            #
            # Expression substitution (rewriting a predicate on a computed alias
            # in terms of the underlying expression -- e.g. exploiting TRUNC's
            # monotonicity) is explicitly OUT OF SCOPE here; see
            # _inline_project_alias_predicates for the one existing special case
            # (boolean alias compared to a literal) and the TRUNC-alias case.
            _emit_memo = {}
            child_ids = frozenset()
            for child, _, _ in context.optimized_plan.ingoing_edges(context.node_id):
                child_ids |= _emitted_identities(context.optimized_plan, child, _emit_memo)

            passthrough_predicates = []
            remaining_predicates = []
            for predicate in context.collected_predicates:
                predicate_ids = _predicate_column_ids(predicate)
                if predicate_ids and predicate_ids <= child_ids:
                    passthrough_predicates.append(predicate)
                else:
                    remaining_predicates.append(predicate)

            context.collected_predicates = passthrough_predicates + remaining_predicates

        elif node.node_type == LogicalPlanStepType.Unnest:
            # if we're a CROSS JOIN UNNEST, we can push some filters into the UNNEST
            remaining_predicates = []
            for predicate in context.collected_predicates:
                # NOT conditions don't have a left/right so need special handling
                if predicate.condition.centre is not None:
                    remaining_predicates.append(predicate)
                    continue
                known_columns = set(col.schema_column.identity for col in predicate.columns)
                query_columns = {
                    predicate.condition.left.schema_column.identity,
                    predicate.condition.right.schema_column.identity,
                }

                # If the predicate only references columns from the relation feeding the UNNEST,
                # move the filter before the UNNEST so we reduce the number of rows expanded.
                if (
                    predicate.relations
                    and node.unnest_column is not None
                    and predicate.relations.issubset({node.unnest_column.source})
                    and node.unnest_target.schema_column.identity not in known_columns
                ):
                    self.telemetry.optimization_predicate_pushdown += 1
                    context.optimized_plan.insert_node_before(
                        predicate.nid, predicate, context.node_id
                    )
                    continue

                # NOTE: folding an Eq/InList filter on the unnested column INTO the
                # UNNEST (`node.filters`) is intentionally disabled. The native
                # UnnestOperator (src/cpp/engine/native_unnest.hpp) does plain
                # expansion only and does not apply a folded value filter. Falling
                # through to the branch below places the predicate as a standalone
                # FilterNode AFTER the unnest, which compiles to the proven native
                # ExprFilter (correct; it filters the expanded stream rather than
                # during expansion — a perf, not correctness, difference). Re-enable
                # the fold once the native operator applies node.filters.
                # A predicate that references the UNNEST TARGET must be placed directly
                # ABOVE the unnest: the target column does not exist below it, so
                # letting it stay in `remaining_predicates` pushes it past the unnest
                # (and any join) down onto the scan, where resolving the target's
                # identity dies with a KeyError. `unnest_target` is a LogicalColumn and
                # has NO `.identity` — the old `node.unnest_target.identity` was always
                # None, so this clause never fired and only column-vs-column predicates
                # (query_columns == known_columns) were correctly placed here.
                if (
                    node.unnest_target.schema_column.identity in known_columns
                    or query_columns == known_columns
                ):
                    self.telemetry.optimization_predicate_pushdown += 1
                    context.optimized_plan.insert_node_after(
                        predicate.nid, predicate, context.node_id
                    )
                else:
                    remaining_predicates.append(predicate)
            context.collected_predicates = remaining_predicates

        elif node.node_type == LogicalPlanStepType.AggregateAndGroup:
            # Handle HAVING predicates (filters with aggregators) by attaching to the aggregate node
            remaining_predicates = []
            having_predicates = []
            for predicate in context.collected_predicates:
                has_agg = get_all_nodes_of_type(predicate.condition, (NodeType.AGGREGATOR,))
                if has_agg:
                    # This is a HAVING predicate — push into the aggregate
                    having_predicates.append(predicate)
                else:
                    remaining_predicates.append(predicate)

            # Attach HAVING predicates to the aggregate node
            if having_predicates:
                conditions = [p.condition for p in having_predicates]

                # Combine multiple HAVING conditions with AND
                combined = conditions[0]
                for cond in conditions[1:]:
                    from opteryx.models import Node

                    and_node = Node(node_type=NodeType.AND)
                    and_node.left = combined
                    and_node.right = cond
                    combined = and_node

                # Extract all aggregator nodes from the HAVING condition
                # and add them to the aggregate node if not already present
                aggregators_in_having = get_all_nodes_of_type(combined, (NodeType.AGGREGATOR,))
                existing_aggregates = list(node.aggregates or [])

                # Check which aggregators from HAVING are not already in the aggregates list
                for agg in aggregators_in_having:
                    # Check if this aggregator is already in the list by comparing their structure
                    is_duplicate = any(
                        format_expression(agg) == format_expression(existing_agg)
                        for existing_agg in existing_aggregates
                    )
                    if not is_duplicate:
                        # Add this aggregator to the list of aggregates
                        existing_aggregates.append(agg)

                # Add the having_condition to node properties and update the node
                node_properties = dict(node.properties)
                node_properties["having_condition"] = combined
                # Update aggregates list with any new aggregators from HAVING
                if len(existing_aggregates) > len(node.aggregates or []):
                    node_properties["aggregates"] = existing_aggregates

                context.optimized_plan.add_node(context.node_id, LogicalPlanNode(**node_properties))

                # Remove the Filter nodes from the plan
                for predicate in having_predicates:
                    context.optimized_plan.remove_node(predicate.nid, heal=True)
                    self.telemetry.optimization_predicate_pushdown += 1
            else:
                context.optimized_plan.add_node(context.node_id, LogicalPlanNode(**node.properties))

            # Non-HAVING predicates left in remaining_predicates are simply left
            # to keep flowing in collected_predicates, exactly as before this
            # optimizer touched this node type. A predicate on a GROUP BY key
            # (or anything else this aggregate's child emits) will keep being
            # validated by identity further down (Project boundaries, Scan) and
            # gets pushed as far as that allows.
            #
            # A predicate that instead needs an identity that only exists at
            # THIS aggregate's own output (e.g. a reference to an aggregate's
            # alias, bound by identity without literally embedding an
            # AGGREGATOR node in the condition -- so `has_agg` above missed it)
            # cannot be pushed below. It is deliberately NOT eagerly relocated
            # with insert_node_after here: that call mutates this node's CURRENT
            # outgoing edge mid-traversal, which is unsafe when the edge (or the
            # predicate's own expression tree) is shared/aliased with a
            # not-yet-visited ancestor (e.g. an outer GROUP BY ALL reusing the
            # SELECT list's expression node -- see the copy-memo comment in
            # opteryx/compiled/structures/node.pyx). Confirmed by a real crash
            # repro (a TRUNC() re-applied above this aggregate lost its
            # function_ref) that reproduced ONLY with the eager
            # insert_node_after and disappeared entirely once removed --
            # complete()'s plan_path-based restoration produces the identical
            # final plan shape, safely, at the end of the whole traversal.
            context.collected_predicates = remaining_predicates
            if context.last_nid:
                context.optimized_plan.add_edge(context.node_id, context.last_nid)

        elif node.node_type == LogicalPlanStepType.Join:
            join_left_rels = set(node.left_relation_names or [])
            join_right_rels = set(node.right_relation_names or [])

            def _predicate_sides(predicate):
                """(touches_left, touches_right) for a predicate's identifiers."""
                touches_left = False
                touches_right = False
                for ident in get_all_nodes_of_type(predicate, (NodeType.IDENTIFIER,)):
                    src = getattr(ident, "source", None)
                    if src in join_left_rels:
                        touches_left = True
                    if src in join_right_rels:
                        touches_right = True
                return touches_left, touches_right

            def _flatten_and(node):
                """Flatten an AND tree into a list of leaf predicates."""
                if node is None:
                    return []
                if node.node_type == NodeType.AND:
                    return _flatten_and(node.left) + _flatten_and(node.right)
                return [node]

            def _and_chain(leaves):
                """Rebuild a left-leaning AND tree from leaves; None if empty."""
                # Local import: a function-scoped `from opteryx.models import Node`
                # elsewhere in `visit` makes Node a local variable for the whole
                # method, so the module-level import isn't visible to closures.
                from opteryx.models import Node as _Node

                if not leaves:
                    return None
                result = leaves[0]
                for leaf in leaves[1:]:
                    and_node = _Node(node_type=NodeType.AND)
                    and_node.left = result
                    and_node.right = leaf
                    result = and_node
                return result

            def _is_collectable(predicate):
                """True if this predicate should be pulled out of the ON clause."""
                # Literal-on-one-side predicates: collectable as filters.
                if len(get_all_nodes_of_type(predicate.left, (NodeType.IDENTIFIER,))) == 0:
                    return True
                if len(get_all_nodes_of_type(predicate.right, (NodeType.IDENTIFIER,))) == 0:
                    return True
                # Single-side predicates in the ON clause (e.g.
                # `JOIN ... ON a.x = b.x AND a.y > a.z`) belong to that side
                # and are filters, not join keys. Pull them out so the join
                # stays a pure equi-join — DrakenInnerJoinNode only supports
                # Eq, and even when other comparators are supported the input
                # filter is cheaper.
                if join_left_rels or join_right_rels:
                    touches_left, touches_right = _predicate_sides(predicate)
                    if touches_left ^ touches_right:
                        return True
                return False

            def _inner(on_node):
                """Split the ON clause into (extracted_predicates, remaining_on)."""
                leaves = _flatten_and(on_node)
                extracted = []
                kept = []
                for leaf in leaves:
                    if leaf.node_type == NodeType.AND:
                        # nested AND that survived flattening — keep as-is
                        kept.append(leaf)
                        continue
                    if _is_collectable(leaf):
                        extracted.append(leaf)
                    else:
                        kept.append(leaf)
                return extracted, _and_chain(kept)

            if node.on:
                new_predicates, node.on = _inner(node.on)
                self.telemetry.optimization_predicate_pushdown_into_join += 1
                context.collected_predicates.extend(
                    LogicalPlanNode(
                        LogicalPlanStepType.Filter,
                        condition=node,
                        nid=random_string(),
                        relations={
                            n.source for n in get_all_nodes_of_type(node, (NodeType.IDENTIFIER,))
                        },
                        # Marks this Filter as carved out of THIS join's own ON
                        # clause (as opposed to a genuine WHERE-clause predicate
                        # collected from above). OUTER-join handling below must
                        # tell the two apart: a WHERE predicate on the build side
                        # correctly applies post-join (SQL WHERE semantics — see
                        # the `problematic`/`_dump_above` handling), but an ON
                        # conjunct on the build side is a match-candidate filter
                        # and must apply BEFORE the join, or it wrongly collapses
                        # LEFT OUTER to INNER (NULL build columns on unmatched
                        # preserved rows fail the literal comparison).
                        from_join_on=True,
                    )
                    for node in new_predicates
                )

            if context.collected_predicates:
                # push predicates which reference multiple relations here
                _emit_memo = {}

                def _dump_above(predicate):
                    """Materialise a predicate above this join — but only if the join
                    emits every column it references. Availability is ground truth: the
                    alias soup makes a leg-internal predicate (e.g. one consumed inside
                    an aggregate) look like it belongs here. Returns True if placed;
                    False means keep it collected so complete() restores it validly."""
                    if _predicate_column_ids(predicate) <= _emitted_identities(
                        context.optimized_plan, context.node_id, _emit_memo
                    ):
                        self.telemetry.optimization_predicate_pushdown += 1
                        context.optimized_plan.insert_node_after(
                            predicate.nid, predicate, context.node_id
                        )
                        return True
                    return False

                if node.type.startswith("left"):
                    # An ON-clause conjunct that references only the build
                    # (right) side is a match-candidate filter, not a WHERE
                    # predicate — it must apply BEFORE the join (a pre-filter
                    # on the right leg), never via _dump_above (post-join),
                    # which would apply `NULL op literal` to every unmatched
                    # preserved row and silently turn LEFT OUTER into INNER.
                    # Leave it uncollected here so it keeps flowing down the
                    # traversal like any ordinary single-relation predicate,
                    # landing at the right relation's scan.
                    on_clause_build_filters = [
                        p
                        for p in context.collected_predicates
                        if getattr(p, "from_join_on", False)
                        and all(
                            i.source in node.right_relation_names
                            for i in get_all_nodes_of_type(p.condition, (NodeType.IDENTIFIER,))
                        )
                    ]
                    other_predicates = [
                        p
                        for p in context.collected_predicates
                        if p not in on_clause_build_filters
                    ]

                    problematic = any(
                        i.source in node.right_relation_names
                        or i.source not in node.all_relations
                        for predicate in other_predicates
                        for i in get_all_nodes_of_type(predicate.condition, (NodeType.IDENTIFIER,))
                    )
                    # 1887 - if anything can't be pushed past this (outer) join, keep the
                    # pushable-here ones above it and let the rest fall through to be
                    # restored to their valid origins.
                    if problematic:
                        other_predicates = [
                            p for p in other_predicates if not _dump_above(p)
                        ]
                    context.collected_predicates = on_clause_build_filters + other_predicates
                elif node.type not in ("cross join", "inner"):
                    # dump all the predicates
                    # IMPROVE: push past SEMI and ANTI joins
                    context.collected_predicates = [
                        p for p in context.collected_predicates if not _dump_above(p)
                    ]
                elif node.type in ("cross join",):  # , "inner"):
                    # IMPROVE: add predicates to INNER JOIN conditions
                    # we may be able to rewrite as an inner join or non-equi join
                    remaining_predicates = []
                    non_equi_ops = {"NotEq", "Gt", "GtEq", "Lt", "LtEq"}

                    all_join_rels = set(node.left_relation_names) | set(node.right_relation_names)
                    for predicate in context.collected_predicates:
                        if (
                            predicate.relations.intersection(set(node.left_relation_names))
                            and predicate.relations.intersection(set(node.right_relation_names))
                            and predicate.relations.issubset(all_join_rels)
                        ):
                            # This predicate references both sides of the join
                            if predicate.condition.value == "Eq":
                                # Only convert when the predicate can be represented as join fields.
                                # Expressions like `s = e + INTERVAL '1' MONTH` must stay as filters.
                                try:
                                    extract_join_fields(
                                        predicate.condition,
                                        node.left_relation_names,
                                        node.right_relation_names,
                                    )
                                except UnsupportedSyntaxError:
                                    self.telemetry.optimization_predicate_pushdown += 1
                                    context.optimized_plan.insert_node_after(
                                        predicate.nid, predicate, context.node_id
                                    )
                                    continue
                                # Convert to inner join
                                node.type = "inner"
                                node.on = _add_condition(node.on, predicate.condition)
                                self.telemetry.optimization_predicate_pushdown_cross_join_to_inner_join += 1
                            else:
                                # Unsupported comparison - insert predicate above the join
                                self.telemetry.optimization_predicate_pushdown += 1
                                context.optimized_plan.insert_node_after(
                                    predicate.nid, predicate, context.node_id
                                )
                        elif predicate.relations.intersection(all_join_rels) and not predicate.relations.issubset(all_join_rels):
                            # Looks like it references something outside this join. But
                            # the alias soup (a relation known by several names — CTE
                            # name, subquery aliases) produces false positives: a
                            # leg-internal predicate (e.g. one consumed inside an
                            # aggregate) can look external. Availability is the ground
                            # truth — only materialise above the join if the join emits
                            # every column the predicate needs; otherwise keep it
                            # collected so complete() restores it to its valid origin.
                            if not _dump_above(predicate):
                                remaining_predicates.append(predicate)
                        else:
                            # Single-relation predicates can be pushed past the join
                            remaining_predicates.append(predicate)

                    if node.on:
                        node.left_columns, node.right_columns = extract_join_fields(
                            node.on, node.left_relation_names, node.right_relation_names
                        )
                        node.columns = get_all_nodes_of_type(node.on, (NodeType.IDENTIFIER,))
                    context.collected_predicates = remaining_predicates

                for predicate in context.collected_predicates:
                    remaining_predicates = []
                    for predicate in context.collected_predicates:
                        if (
                            len(predicate.relations) == 2
                            and predicate.condition.value == "Eq"
                            and set(node.right_relation_names + node.left_relation_names)
                            == set(predicate.relations)
                        ):
                            self.telemetry.optimization_predicate_pushdown_add_to_inner_join += 1
                            node.condition = _add_condition(node.condition, predicate)
                        else:
                            remaining_predicates.append(predicate)
                    context.collected_predicates = remaining_predicates

                # For INNER equi-joins, derive implied predicates: if col_A op literal
                # is collected for one join key, emit the same predicate for the partner key.
                if node.on and node.type == "inner" and context.collected_predicates:
                    equi_pairs = _get_equi_join_pairs(node.on)
                    if equi_pairs:
                        existing_keys: set = set()
                        for p in context.collected_predicates:
                            ident, op, lit = _normalize_col_op_lit(p.condition)
                            if ident is not None and getattr(ident, "schema_column", None) is not None:
                                existing_keys.add((ident.schema_column.identity, op, str(lit.value)))

                        derived = []
                        for predicate in context.collected_predicates:
                            ident, op, lit = _normalize_col_op_lit(predicate.condition)
                            if ident is None or getattr(ident, "schema_column", None) is None:
                                continue
                            col_id = ident.schema_column.identity
                            for left_col, right_col in equi_pairs:
                                lsc = getattr(left_col, "schema_column", None)
                                rsc = getattr(right_col, "schema_column", None)
                                if lsc is None or rsc is None:
                                    continue
                                if lsc.identity == col_id:
                                    target_col = right_col
                                elif rsc.identity == col_id:
                                    target_col = left_col
                                else:
                                    continue
                                tsc = getattr(target_col, "schema_column", None)
                                if tsc is None:
                                    continue
                                dedup_key = (tsc.identity, op, str(lit.value))
                                if dedup_key in existing_keys:
                                    continue
                                existing_keys.add(dedup_key)
                                derived.append(_make_implied_filter(op, target_col, lit))
                                self.telemetry.optimization_predicate_pullup_implied += 1
                        context.collected_predicates.extend(derived)

                self.telemetry.optimization_predicate_pushdown += 1
                context.optimized_plan.add_node(context.node_id, node)

            if node.on is None and node.type == ("inner"):
                raise UnsupportedSyntaxError(
                    "INNER JOIN has no valid conditions, did you mean CROSS JOIN?"
                )

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # anything we couldn't push, we need to put back
        _memo = {}
        for predicate in context.collected_predicates:
            # A predicate carrying an explicit deep-restore target was rewritten
            # (e.g. a TRUNC-alias range) into a form the ordinary top-down,
            # one-hop-at-a-time gates can't route, but which IS valid at a
            # specific node deeper in the plan. The rewriting site
            # (_inline_trunc_alias_*) computed and justified that target; place
            # the predicate directly above it. Guarded by a re-check that the
            # target still exists and still emits every column the predicate
            # needs, so a stale/invalid target can never produce a broken plan
            # -- it just falls through to the plan_path restore below.
            target = predicate.deep_restore_target
            if (
                target is not None
                and target in context.optimized_plan
                and _predicate_column_ids(predicate)
                <= _emitted_identities(context.optimized_plan, target, _memo)
            ):
                self.telemetry.optimization_predicate_pushdown_deep_restore += 1
                context.optimized_plan.insert_node_after(predicate.nid, predicate, target)
                continue

            if predicate.plan_path is not None:
                for nid in predicate.plan_path:
                    if nid in context.optimized_plan:
                        self.telemetry.optimization_predicate_pushdown_unplaced += 1
                        context.optimized_plan.insert_node_before(predicate.nid, predicate, nid)
                        break
        return context.optimized_plan

    def _handle_predicates(
        self, node: LogicalPlanNode, context: OptimizerContext
    ) -> OptimizerContext:
        # Two-pass: classify pushable predicates as selective (comparison-style) vs.
        # metadata-only (UNARY_OPERATOR), then commit. Metadata-only predicates ride
        # along on a scan only when at least one selective predicate is also being
        # pushed — otherwise a non-selective unary predicate (e.g. `col <> ''`) would
        # be the sole driver of two-pass / late-materialization, which is a net loss
        # when the predicate doesn't narrow the mask enough to pay back the overhead.
        remaining_predicates = []
        selective_to_push = []  # (predicate, condition_to_push)
        metadata_to_push = []   # (predicate, condition_to_push)
        not_pushable = []       # predicate

        # Match by column identity, not by relation name. `predicate.relations`
        # is stamped with the relation/alias name in scope at the point the
        # Filter node was originally collected — when the predicate's origin is
        # an outer query against a VIEW that the planner has since inlined down
        # to this real Scan, that name has nothing to do with `node.relation`/
        # `node.alias` and the two can never match, even for a trivially
        # pushable passthrough predicate. Column identity survives inlining
        # (renames/aliases reuse the source SchemaColumn, see
        # binder.locate_identifier), so it's the correct — and already
        # established elsewhere in this file (_dump_above) — ground truth for
        # "does this scan actually emit every column this predicate needs".
        _emit_memo = {}
        emitted_ids = _emitted_identities(context.optimized_plan, context.node_id, _emit_memo)

        for predicate in context.collected_predicates:
            predicate_ids = _predicate_column_ids(predicate)
            if not predicate_ids or not predicate_ids <= emitted_ids:
                remaining_predicates.append(predicate)
                continue

            if not node.connector:
                not_pushable.append(predicate)
                continue

            # Try to normalise CAST(col, T) op literal predicates before the
            # can_push check — CAST nodes are not in can_push's allowlist, but
            # a stripped/rescaled form may be pushable and semantically equivalent.
            # Normalisation only fires for COMPARISON_OPERATOR, so a hit is selective.
            normalized = _try_normalize_cast_predicate(predicate.condition)
            if normalized is not None:
                norm_types = set()
                if normalized.left.schema_column:
                    norm_types.add(normalized.left.schema_column.category)
                if normalized.right.schema_column:
                    norm_types.add(normalized.right.schema_column.category)
                norm_predicate = Node(node_type=predicate.node_type)
                norm_predicate.condition = normalized
                norm_predicate.relations = predicate.relations
                if node.connector.supports_predicate_pushdown and node.connector.can_push(
                    norm_predicate, norm_types
                ):
                    selective_to_push.append((predicate, normalized))
                    continue

            types = set()
            if predicate.condition.node_type == NodeType.UNARY_OPERATOR:
                if predicate.condition.centre and predicate.condition.centre.schema_column:
                    types.add(predicate.condition.centre.schema_column.category)
            else:
                if predicate.condition.left and predicate.condition.left.schema_column:
                    types.add(predicate.condition.left.schema_column.category)
                # For InList/NotInList the right side is always an ARRAY literal; its type
                # is an implementation detail of the IN operator, not a column type the
                # connector needs to handle. Including it causes can_push to spuriously
                # return False (ARRAY not in PUSHABLE_TYPES), leaving the column out of
                # pass-1 and breaking two-pass late-materialization for downstream filters.
                if predicate.condition.right and predicate.condition.right.schema_column:
                    if predicate.condition.value not in ("InList", "NotInList"):
                        types.add(predicate.condition.right.schema_column.category)
            if node.connector.supports_predicate_pushdown and node.connector.can_push(
                predicate, types
            ):
                if predicate.condition.node_type == NodeType.UNARY_OPERATOR:
                    metadata_to_push.append((predicate, predicate.condition))
                else:
                    selective_to_push.append((predicate, predicate.condition))
            else:
                not_pushable.append(predicate)

        # Commit selective predicates unconditionally.
        for _predicate, condition in selective_to_push:
            if not node.predicates:
                node.predicates = []
            node.predicates.append(condition)

        # Metadata predicates only push when a selective companion is also pushing.
        if selective_to_push:
            for _predicate, condition in metadata_to_push:
                if not node.predicates:
                    node.predicates = []
                node.predicates.append(condition)
        else:
            for predicate, _condition in metadata_to_push:
                self.telemetry.optimization_predicate_pushdown_metadata_orphaned += 1
                context.optimized_plan.insert_node_after(
                    predicate.nid, predicate, context.node_id
                )

        for predicate in not_pushable:
            self.telemetry.optimization_predicate_pushdown += 1
            context.optimized_plan.insert_node_after(predicate.nid, predicate, context.node_id)

        context.collected_predicates = remaining_predicates
        return context

    def _inline_project_alias_predicates(
        self, node: LogicalPlanNode, context: OptimizerContext
    ) -> None:
        """Inline simple project aliases referenced by a filter so the predicate can be
        pushed below the projection."""

        if node.condition is None:
            return

        alias_chain = set()
        parent_nid = context.node_id
        project_node = None

        while True:
            incoming = list(context.pre_optimized_tree.ingoing_edges(parent_nid))
            if len(incoming) != 1:
                return

            parent_nid = incoming[0][0]
            parent_node = context.pre_optimized_tree[parent_nid]

            node_alias = getattr(parent_node, "alias", None)
            if node_alias:
                alias_chain.add(node_alias)

            if parent_node.node_type == LogicalPlanStepType.Project:
                project_node = parent_node
                break
            if parent_node.node_type in (
                LogicalPlanStepType.Scan,
                LogicalPlanStepType.FunctionDataset,
            ):
                return

        if project_node is None:
            return

        # A TRUNC-alias substitution rewrites the predicate to reference the raw
        # column the alias's TRUNC()'d group key was computed from -- a column
        # the ordinary top-down, one-hop-at-a-time Project/AggregateAndGroup
        # gates in visit() can't route to (it lives below an aggregate, whose
        # one-hop emitted set doesn't include it). Instead of relying on those
        # gates, the substitution computes an explicit deep-restore target by
        # descending from the alias-defining Project (`parent_nid`) to the
        # deepest node that actually emits the rewritten column, crossing only
        # nodes a filter may soundly be pushed past (see _deep_pushdown_target).
        # complete() then places the predicate directly above that target. The
        # descent runs against pre_optimized_tree, whose Project/Aggregate/Scan
        # skeleton is stable through this strategy (only Filter nodes move), so
        # the target nid it returns is still valid at complete() time.
        _emit_memo = {}
        descent_start_nid = parent_nid

        alias_expressions = {}
        for column in project_node.columns or []:
            query_column = getattr(column, "query_column", None)
            if not query_column:
                continue

            expression = column if isinstance(column, Node) else getattr(column, "expression", None)
            if expression is None:
                continue

            alias_expressions[query_column] = (column, expression)

        if not alias_expressions:
            return

        condition = node.condition

        # PredicateCompactionStrategy (runs before this strategy) commonly fuses
        # a `col >= X AND col < Y` pair into a single BETWEEN node before this
        # code ever sees it — handle that shape directly rather than only the
        # single-comparison case, since it's the realistic shape a date-range
        # WHERE clause takes once compacted.
        if condition.node_type == NodeType.BETWEEN and self._inline_trunc_alias_between(
            node, condition, alias_expressions, alias_chain,
            context.pre_optimized_tree, descent_start_nid, _emit_memo,
        ):
            return

        if condition.node_type != NodeType.COMPARISON_OPERATOR:
            return

        if condition.value in _TRUNC_REWRITE_OPS and self._inline_trunc_alias_predicate(
            node, condition, alias_expressions, alias_chain,
            context.pre_optimized_tree, descent_start_nid, _emit_memo,
        ):
            return

        if condition.value not in {"Eq", "NotEq"}:
            return

        candidates = (
            (condition.left, condition.right),
            (condition.right, condition.left),
        )

        for alias_candidate, literal_candidate in candidates:
            if (
                alias_candidate
                and alias_candidate.node_type == NodeType.IDENTIFIER
                and alias_candidate.source_column in alias_expressions
                and literal_candidate
                and literal_candidate.node_type == NodeType.LITERAL
                and (
                    (literal_candidate.type is not None and literal_candidate.type.category == LogicalCategory.BOOLEAN)
                    or str(literal_candidate.type).upper() == "BOOLEAN"
                )
            ):
                if (
                    alias_candidate.source
                    and alias_chain
                    and alias_candidate.source not in alias_chain
                ):
                    continue

                _, expression_template = alias_expressions[alias_candidate.source_column]

                if isinstance(expression_template, Node) and get_all_nodes_of_type(
                    expression_template, (NodeType.AGGREGATOR,)
                ):
                    continue

                if getattr(expression_template, "copy", None) is not None:
                    expression = expression_template.copy()
                else:
                    expression = expression_template

                if isinstance(expression, Node):
                    expression.alias = None
                    expression.query_column = None
                    if expression.schema_column:
                        expression.schema_column.aliases = []
                elif getattr(expression, "schema_column", None):
                    expression.schema_column.aliases = []

                literal_value = literal_candidate.value
                if isinstance(literal_value, str):
                    literal_is_true = literal_value.strip().lower() in {"true", "t", "1"}
                else:
                    literal_is_true = bool(literal_value)

                negate = (not literal_is_true) if condition.value == "Eq" else literal_is_true

                if negate:
                    new_condition = Node(NodeType.NOT, centre=expression)
                    expr_name = f"NOT {format_expression(expression)}"
                    new_condition.schema_column = ExpressionColumn(
                        name=expr_name,
                        column_type=_CT_BOOLEAN,
                        expression=expr_name,
                    )
                else:
                    new_condition = expression

                node.condition = new_condition
                identifiers = get_all_nodes_of_type(new_condition, (NodeType.IDENTIFIER,))
                node.columns = identifiers
                node.relations = {
                    identifier.source
                    for identifier in identifiers
                    if getattr(identifier, "source", None)
                }

                self.telemetry.optimization_predicate_pushdown_inline_project += 1
                return

    def _inline_trunc_alias_between(
        self,
        node: LogicalPlanNode,
        condition: Node,
        alias_expressions: dict,
        alias_chain: set,
        plan: LogicalPlan,
        descent_start_nid: str,
        emit_memo: dict,
    ) -> bool:
        """BETWEEN-shaped counterpart to _inline_trunc_alias_predicate.

        PredicateCompactionStrategy's BETWEEN node always has the compared
        expression on `.left`, the lower bound on `.right`, the upper bound on
        `.centre`, and `.value == (lower_inclusive, upper_inclusive)` (see
        predicate_compaction.py's _build_range_node) — no left/right ambiguity
        to handle, unlike a plain comparison.

        Decomposes into two comparisons (lower/upper), rewrites each via
        rewrite_date_trunc_to_range, and recombines with AND. Same TRUNC-only
        allow-list and deep-restore-target validation as
        _inline_trunc_alias_predicate; see that method's docstring for the
        rationale on both.
        """
        alias_candidate = condition.left
        if not (
            alias_candidate
            and alias_candidate.node_type == NodeType.IDENTIFIER
            and alias_candidate.source_column in alias_expressions
        ):
            return False
        if alias_candidate.source and alias_chain and alias_candidate.source not in alias_chain:
            return False

        _, expression_template = alias_expressions[alias_candidate.source_column]
        expression_template = _unwrap_nested(expression_template)
        if (
            not isinstance(expression_template, Node)
            or expression_template.node_type != NodeType.FUNCTION
            or expression_template.value != "TRUNC"
        ):
            return False
        if get_all_nodes_of_type(expression_template, (NodeType.AGGREGATOR,)):
            return False

        lower_literal, upper_literal = condition.right, condition.centre
        lower_inclusive, upper_inclusive = condition.value

        def _make_side(op, literal):
            trunc_expression = expression_template.copy()
            trunc_expression.alias = None
            trunc_expression.query_column = None
            if trunc_expression.schema_column:
                trunc_expression.schema_column.aliases = []
            side_condition = Node(
                node_type=NodeType.COMPARISON_OPERATOR,
                value=op,
                left=trunc_expression,
                right=literal,
            )
            side_condition.schema_column = ExpressionColumn(
                name=format_expression(side_condition, True), column_type=_CT_BOOLEAN
            )
            return rewrite_date_trunc_to_range(side_condition, self.telemetry)

        rewritten_lower = _make_side("GtEq" if lower_inclusive else "Gt", lower_literal)
        rewritten_upper = _make_side("LtEq" if upper_inclusive else "Lt", upper_literal)

        # rewrite_date_trunc_to_range silently declines (returns its input
        # unchanged) for a literal it can't parse -- notably, parse_iso
        # misinterprets an already-folded TIMESTAMP literal (epoch
        # MICROSECONDS, Opteryx's native storage unit) as epoch SECONDS,
        # which is wildly out of range for any realistic date and returns
        # None. Committing a "rewrite" where one side silently kept its
        # original TRUNC(...) wrapper while the other got stripped produces a
        # malformed, half-rewritten predicate that crashes downstream at
        # physical-plan compile time ("FUNCTION 'TRUNC' has no function_ref").
        # Require BOTH sides to have actually lost their TRUNC wrapper before
        # committing either.
        def _still_wraps_trunc(side):
            return any(
                fn.value == "TRUNC" for fn in get_all_nodes_of_type(side, (NodeType.FUNCTION,))
            )

        if _still_wraps_trunc(rewritten_lower) or _still_wraps_trunc(rewritten_upper):
            self.telemetry.optimization_predicate_pushdown_trunc_alias_inline_declined += 1
            return False

        combined = Node(node_type=NodeType.AND, left=rewritten_lower, right=rewritten_upper)

        identifiers = get_all_nodes_of_type(combined, (NodeType.IDENTIFIER,))
        rewritten_ids = {
            sc.identity
            for ident in identifiers
            for sc in (getattr(ident, "schema_column", None),)
            if sc is not None and sc.identity is not None
        }
        group_key_identity = getattr(
            getattr(alias_candidate, "schema_column", None), "identity", None
        )
        target = (
            _deep_pushdown_target(plan, descent_start_nid, rewritten_ids, group_key_identity, emit_memo)
            if rewritten_ids
            else None
        )
        if target is None:
            self.telemetry.optimization_predicate_pushdown_trunc_alias_inline_declined += 1
            return False

        node.condition = combined
        node.columns = identifiers
        node.relations = {
            identifier.source for identifier in identifiers if getattr(identifier, "source", None)
        }
        node.deep_restore_target = target

        self.telemetry.optimization_predicate_pushdown_trunc_alias_inline += 1
        return True

    def _inline_trunc_alias_predicate(
        self,
        node: LogicalPlanNode,
        condition: Node,
        alias_expressions: dict,
        alias_chain: set,
        plan: LogicalPlan,
        descent_start_nid: str,
        emit_memo: dict,
    ) -> bool:
        """Substitute a WHERE-clause alias for `TRUNC(col, unit)` with the TRUNC
        expression itself, then immediately fold it into a range predicate on the
        raw column via the existing (calendar-correct, all-units) rewrite already
        used by PredicateRewriteStrategy.

        Only TRUNC is allow-listed here — this is a monotonic function, so a
        range comparison against its result is equivalent to a range comparison
        against the raw input (exploited by rewrite_date_trunc_to_range). Other
        computed aliases are NOT safe to substitute this way (e.g. ABS/MOD are
        not monotonic, so the pre-image of a range under them isn't a simple
        range) and are deliberately left untouched.

        PredicateRewriteStrategy (which owns rewrite_date_trunc_to_range) already
        had its one pass over the plan by the time PredicatePushdownStrategy runs
        (see strategy ordering in opteryx/planner/optimizer/__init__.py), so a
        TRUNC expression that only becomes visible via this substitution would
        otherwise never get folded — we call the rewrite directly instead of
        duplicating its logic.

        The rewritten predicate is committed only if `_deep_pushdown_target`
        finds a node deeper in the plan that actually emits the raw column it
        now references, reachable by crossing only filter-transparent nodes
        (and the one aggregate whose group key it is provably group-invariant
        under). That target is stamped on the node as `deep_restore_target` and
        honored by complete(). If no valid target exists, the rewrite is
        declined and the predicate left untouched -- always safe, since it then
        restores to its original valid position via plan_path. Requiring a
        concrete valid target (rather than a looser "the identity exists
        somewhere" check) is what prevents committing a predicate the plan
        can't actually place, which crashes at physical-plan compile time
        ("expression references column ... which the stream does not carry").

        Returns True if the predicate was rewritten (node.condition/.columns/
        .relations/.deep_restore_target were updated), False if nothing
        matched or no valid deep target was found, and the predicate is
        unchanged.
        """
        candidates = (
            (condition.left, condition.right, condition.value),
            (condition.right, condition.left, _INVERT_COMPARISON_OP.get(condition.value)),
        )

        for alias_candidate, literal_candidate, op_with_alias_on_left in candidates:
            if op_with_alias_on_left is None:
                continue
            if not (
                alias_candidate
                and alias_candidate.node_type == NodeType.IDENTIFIER
                and alias_candidate.source_column in alias_expressions
                and literal_candidate
                and literal_candidate.node_type == NodeType.LITERAL
            ):
                continue

            if (
                alias_candidate.source
                and alias_chain
                and alias_candidate.source not in alias_chain
            ):
                continue

            _, expression_template = alias_expressions[alias_candidate.source_column]
            expression_template = _unwrap_nested(expression_template)
            if (
                not isinstance(expression_template, Node)
                or expression_template.node_type != NodeType.FUNCTION
                or expression_template.value != "TRUNC"
            ):
                continue
            if get_all_nodes_of_type(expression_template, (NodeType.AGGREGATOR,)):
                continue

            trunc_expression = expression_template.copy()
            trunc_expression.alias = None
            trunc_expression.query_column = None
            if trunc_expression.schema_column:
                trunc_expression.schema_column.aliases = []

            new_condition = Node(
                node_type=NodeType.COMPARISON_OPERATOR,
                value=op_with_alias_on_left,
                left=trunc_expression,
                right=literal_candidate,
            )
            new_condition.schema_column = ExpressionColumn(
                name=format_expression(new_condition, True), column_type=_CT_BOOLEAN
            )

            rewritten = rewrite_date_trunc_to_range(new_condition, self.telemetry)

            # rewrite_date_trunc_to_range silently declines (returns its input
            # unchanged, still TRUNC-wrapped) for a literal it can't parse --
            # see _inline_trunc_alias_between's docstring/comment for the
            # concrete failure mode (an already-folded TIMESTAMP literal,
            # epoch microseconds, misread as epoch seconds). A predicate still
            # containing the TRUNC call is not a successful rewrite; committing
            # it crashes downstream at physical-plan compile time.
            if any(
                fn.value == "TRUNC"
                for fn in get_all_nodes_of_type(rewritten, (NodeType.FUNCTION,))
            ):
                self.telemetry.optimization_predicate_pushdown_trunc_alias_inline_declined += 1
                continue

            identifiers = get_all_nodes_of_type(rewritten, (NodeType.IDENTIFIER,))
            rewritten_ids = {
                sc.identity
                for ident in identifiers
                for sc in (getattr(ident, "schema_column", None),)
                if sc is not None and sc.identity is not None
            }
            group_key_identity = getattr(
                getattr(alias_candidate, "schema_column", None), "identity", None
            )
            target = (
                _deep_pushdown_target(
                    plan, descent_start_nid, rewritten_ids, group_key_identity, emit_memo
                )
                if rewritten_ids
                else None
            )
            if target is None:
                self.telemetry.optimization_predicate_pushdown_trunc_alias_inline_declined += 1
                continue

            node.condition = rewritten
            node.columns = identifiers
            node.relations = {
                identifier.source for identifier in identifiers if getattr(identifier, "source", None)
            }
            node.deep_restore_target = target

            self.telemetry.optimization_predicate_pushdown_trunc_alias_inline += 1
            return True

        return False
