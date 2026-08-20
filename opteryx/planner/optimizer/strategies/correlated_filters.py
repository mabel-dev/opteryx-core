# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Correlated Filters

Type: Cost-based (consumes propagated statistics)
Goal: Reduce Rows / IO

For an equi-join ``a.k = b.k`` the matching rows on one side are bounded by the
realized value range of the join key on the other side. We read that range from
the propagated ``node.statistics`` (post-filter / post-join-intersection — see
statistics_refresh) and push it onto the opposite leg's scan as a range
predicate, so the scan can prune row groups and pre-filter rows before the join.

This runs *after* PredicatePushdown so the original predicates are already on the
scans and their effect is reflected in the propagated key ranges. The derived
range predicates are appended directly onto the target scan's ``predicates``
list (the same channel PredicatePushdown feeds), so no second pushdown pass is
needed; scans whose connector can't take pushed predicates get a Filter node
instead. Only inner / nested-loop joins are eligible — the pushed range is a
necessary condition for a match, which would be unsound for outer joins.
"""

import datetime
import decimal
import math
import struct

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.planner import build_literal_node
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from opteryx.types.logical_type import DrakenType
from opteryx.types.logical_type import LogicalCategory
from opteryx.types.logical_type import integer_bounds
from opteryx.utils import random_string

from .optimization_strategy import (
    OptimizationStrategy,
    OptimizerContext,
    get_nodes_of_type_from_logical_plan,
)


def _phys_identity(col):
    """Column identity for a join-key identifier, for stats.columns lookup.

    Not the name: names are not unique across a plan, so a name lookup can
    silently return an unrelated relation's range."""
    if isinstance(col, bytes):
        return col
    schema_column = getattr(col, "schema_column", None)
    identity = getattr(schema_column, "identity", None) if schema_column is not None else None
    return identity if isinstance(identity, bytes) else None


def _key_value_range(stats, col):
    """Propagated value_range for *col* from a RelationStatistics, or None when
    no bound has been established (column absent / range empty)."""
    if stats is None:
        return None
    identity = _phys_identity(col)
    if identity is None:
        return None
    col_stats = stats.columns.get(identity)
    if col_stats is None:
        return None
    value_range = col_stats.value_range
    if value_range is None or (
        value_range.lower_bound is None and value_range.upper_bound is None
    ):
        return None
    return value_range


def _tightens(candidate, existing) -> bool:
    """True if *candidate* constrains a column more tightly than *existing*.

    A correlated filter only earns its keep when it removes rows the target
    scan could otherwise return. Once Scan statistics carry the manifest's real
    min/max (they did not before -- ``value_range`` was left empty for any
    column without a predicate on it), the unfiltered case makes both sides of
    a PK/FK join report the SAME full range, and pushing that back is a
    tautology: `l_partkey BETWEEN 1 AND 200000` over a column whose manifest
    bounds are already 1..200000 excludes nothing. It still costs a real
    per-row filter at runtime, and the selectivity estimator charges it the
    0.25 default per bound -- six such bounds took TPC-H lineitem's estimate
    from 6,001,215 rows to 1,465.

    Unknown/incomparable bounds return True: that is the pre-existing
    behaviour (push and let the scan sort it out), so this gate only ever
    suppresses a push it can PROVE is redundant.
    """
    if existing is None:
        return True

    def _tighter(new_bound, old_bound, keep_greater: bool) -> bool:
        if new_bound is None or old_bound is None:
            return False
        if type(new_bound) is not type(old_bound):
            return True  # incomparable -> don't claim redundancy
        return new_bound > old_bound if keep_greater else new_bound < old_bound

    return _tighter(candidate.lower_bound, existing.lower_bound, True) or _tighter(
        candidate.upper_bound, existing.upper_bound, False
    )


def _get_equi_join_pairs(on_node):
    """
    Extract (left_col, right_col) identifier pairs from a (possibly AND-nested) equi-join
    ON condition.  Returns an empty list for anything that isn't a col = col comparison.
    """
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


def _representable(bound, target_type) -> bool:
    """True if *bound* can be carried by a literal of *target_type*.

    The bound comes from the OTHER leg of the join, so its width is the other
    column's, not the target's: `p.id = s.id` puts satellites.id's 1..177 onto
    $planets.id, which is an INT8. Typing 177 as INT8 is not a widening
    question — the literal is materialised by
    `vector_int8_from_constant` and dies with a bare OverflowError.

    Dropping an unrepresentable bound is always sound: these are derived
    necessary-condition filters layered on top of the join, so a missing one
    only forgoes pruning. Both out-of-range directions are droppable — a bound
    beyond the target's width is a tautology (every INT8 is <= 177), and one
    below it makes the predicate unsatisfiable, which the join itself still
    enforces.
    """
    bounds = integer_bounds(target_type)
    if bounds is None:  # not an integer width — this check does not apply
        return True
    if not isinstance(bound, (int, float)) or isinstance(bound, bool):
        return True
    return bounds[0] <= bound <= bounds[1]


# The Python value a literal of each category must carry. `build_literal_node`
# TAGS the literal with `suggested_type` but never re-expresses the value, and
# `_materialise_constant_literal` dispatches on the VALUE's Python type for
# floats/Decimals/temporals — so a float bound tagged INT32 materialises a
# FLOAT64 constant. The compare kernel is identical-type only, so that pair
# declines and the native ExprFilter (which has no fallback) hard-fails with
# `err_op=11`. Every bound therefore gets re-expressed here, or dropped.
_CATEGORY_VALUE_TYPES = {
    LogicalCategory.BOOLEAN: bool,
    LogicalCategory.INTEGER: int,
    LogicalCategory.FLOAT: float,
    LogicalCategory.DECIMAL: decimal.Decimal,
    LogicalCategory.DATE: datetime.date,
    LogicalCategory.TIME: datetime.time,
    LogicalCategory.TIMESTAMP: datetime.datetime,
    LogicalCategory.VARCHAR: str,
    LogicalCategory.NVARCHAR: str,
    LogicalCategory.VARBINARY: bytes,
}


def _as_float(bound, target_type, keep_upper):
    """*bound* as a float that is never TIGHTER than *bound* itself.

    `float(2**53 + 1)` rounds DOWN; used as an upper bound that would exclude a
    row the join still matches. Nudge outward (toward the bound's own side of
    the range) until the float is on the safe side. FLOAT32 targets round again
    on materialisation, so the nudge is done in float32 space for those."""
    single = getattr(target_type, "physical", None) is DrakenType.FLOAT32
    try:
        result = float(bound)
    except (OverflowError, ValueError):
        return None  # magnitude has no float — drop, it is only a pruning hint
    if single:
        result = struct.unpack("<f", struct.pack("<f", result))[0]
    if not math.isfinite(result):
        return None
    # At most a couple of iterations: one ULP either side of the true value.
    for _ in range(4):
        if (result >= bound) if keep_upper else (result <= bound):
            return result
        result = math.nextafter(result, math.inf if keep_upper else -math.inf)
        if single:
            result = struct.unpack("<f", struct.pack("<f", result))[0]
    return None


def _coerce_bound(bound, target_type, keep_upper):
    """*bound* re-expressed as the Python value a *target_type* literal must
    carry, or None when it cannot be carried without narrowing the range.

    Bounds come from the OTHER leg of the join, so their type is the other
    column's — an int32 key gets float64 bounds and vice versa. Returning None
    is always sound: these are derived necessary-condition filters layered on
    top of the join, so a dropped bound only forgoes pruning (same reasoning as
    `_representable`).

    No rounding may NARROW the range beyond what the target's value domain
    already implies — a rounded bound must never exclude a row the join would
    have matched."""
    if target_type is None:
        return bound  # untyped target — build_literal_node infers from the value

    category = target_type.category
    wanted = _CATEGORY_VALUE_TYPES.get(category)
    if wanted is None:
        return None  # NULL / INTERVAL / VARIANT / ARRAY / VECTOR — no literal form

    # bool is a subclass of int; a boolean bound is only ever a boolean bound.
    if isinstance(bound, bool) or wanted is bool:
        return bound if (isinstance(bound, bool) and wanted is bool) else None

    if category is LogicalCategory.INTEGER:
        if isinstance(bound, int):
            return bound
        # Over an integer domain `k <= 4.7` and `k <= 4` select the same rows, so
        # truncating TOWARD the range is exact, not narrowing.
        if isinstance(bound, float):
            if not math.isfinite(bound):
                return None
            return math.floor(bound) if keep_upper else math.ceil(bound)
        if isinstance(bound, decimal.Decimal):
            if not bound.is_finite():
                return None
            rounding = decimal.ROUND_FLOOR if keep_upper else decimal.ROUND_CEILING
            return int(bound.to_integral_value(rounding=rounding))
        return None

    if category is LogicalCategory.FLOAT:
        if isinstance(bound, (int, float, decimal.Decimal)):
            return _as_float(bound, target_type, keep_upper)
        return None

    if category is LogicalCategory.DECIMAL:
        if isinstance(bound, decimal.Decimal):
            return bound
        if isinstance(bound, int):
            return decimal.Decimal(bound)
        # A float bound would have to be quantized to the column's declared
        # scale, and the quantize rounds in a direction this layer cannot see.
        return None

    # TIMESTAMP is a datetime.datetime; DATE is a date that is NOT a datetime
    # (build_literal_node converts a datetime to microseconds even when the
    # target is DATE32, which is days — a silent 1970 bound).
    if category is LogicalCategory.DATE:
        return bound if type(bound) is datetime.date else None
    return bound if isinstance(bound, wanted) else None


def _range_conditions(target_col, value_range):
    """Build GtEq/LtEq COMPARISON_OPERATOR condition Nodes pushing *value_range*
    (native, post-filter bounds) onto *target_col*, correctly typed."""
    target_type = getattr(getattr(target_col, "schema_column", None), "column_type", None)
    conditions = []
    for bound, operator, keep_upper in (
        (value_range.upper_bound, "LtEq", True),
        (value_range.lower_bound, "GtEq", False),
    ):
        if bound is None:
            continue
        bound = _coerce_bound(bound, target_type, keep_upper)
        if bound is None or not _representable(bound, target_type):
            continue
        conditions.append(
            Node(
                NodeType.COMPARISON_OPERATOR,
                value=operator,
                left=target_col,
                right=build_literal_node(bound, suggested_type=target_type),
            )
        )
    return conditions


def _predicate_already_present(predicates, condition):
    """True if *predicates* already contains an equivalent (op, column, literal)."""
    op = getattr(condition, "value", None)
    col = getattr(getattr(condition, "left", None), "value", None)
    lit = getattr(getattr(condition, "right", None), "value", None)
    for existing in predicates:
        if (
            getattr(existing, "value", None) == op
            and getattr(getattr(existing, "left", None), "value", None) == col
            and getattr(getattr(existing, "right", None), "value", None) == lit
        ):
            return True
    return False


class CorrelatedFiltersStrategy(OptimizationStrategy):
    # Cost-typed so the driver propagates statistics (refresh_statistics) before
    # this runs; requires predicates already pushed onto scans so those ranges
    # show up in the propagated key statistics.
    optimization_technique = "cost"
    requires = ("predicates-pushed",)

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if node.node_type == LogicalPlanStepType.Join and node.type in ("inner", "nested loop"):
            join_stats = getattr(node, "statistics", None)
            if join_stats is None:
                return context

            uuid_to_nid = {}
            for nid in list(context.optimized_plan.nodes()):
                plan_node = context.optimized_plan[nid]
                node_uuid = getattr(plan_node, "uuid", None) if plan_node is not None else None
                if node_uuid:
                    uuid_to_nid[node_uuid] = nid

            for left_key, right_key in _get_equi_join_pairs(node.on):
                left_range = _key_value_range(join_stats, left_key)
                right_range = _key_value_range(join_stats, right_key)
                # Each key's realized range constrains the *other* leg's key.
                if left_range is not None:
                    self._push_range(context, node, right_key, left_range, uuid_to_nid)
                if right_range is not None:
                    self._push_range(context, node, left_key, right_range, uuid_to_nid)

        return context

    def _push_range(self, context, join_node, target_col, value_range, uuid_to_nid):
        """Push *value_range* onto *target_col*'s scan(s): append to the scan's
        predicate list when the connector supports it, else add a Filter node."""
        target_relation = getattr(target_col, "source", None)
        if target_relation in (join_node.left_relation_names or []):
            readers = join_node.left_readers or []
        elif target_relation in (join_node.right_relation_names or []):
            readers = join_node.right_readers or []
        else:
            return

        conditions = _range_conditions(target_col, value_range)
        if not conditions:
            return

        for reader_uuid in readers:
            reader_nid = uuid_to_nid.get(reader_uuid)
            if reader_nid is None:
                continue
            scan = context.optimized_plan[reader_nid]
            if scan is None:
                continue

            # AVAILABILITY GUARD: a leg's relation names include DERIVED relations
            # (a CROSS JOIN UNNEST contributes a synthetic `$unnest-*` schema), but
            # its readers are only the base scans. Pushing a range on a derived
            # column onto a base scan attaches a predicate to a relation that does
            # not produce it — the scan's predicate resolver then dies with a
            # KeyError on the unresolvable identity. Only push onto the reader that
            # IS the target column's relation.
            scan_names = {getattr(scan, "alias", None), getattr(scan, "relation", None)}
            if target_relation not in scan_names:
                continue

            # IDENTITY GUARD: the alias check above is necessary but not
            # sufficient -- a materialised join key (e.g. an equi-join hoisted
            # off an arithmetic operand by cross_join_filter_pushdown, which
            # keeps the leg's own relation name as `.source` so
            # extract_join_fields still matches it) carries the SAME `.source`
            # as its underlying scan while its actual identity is only
            # produced by a Project sitting ABOVE that scan. Pushing a
            # predicate keyed by that identity straight onto the scan's own
            # `.predicates` asks the reader for a column it never emits --
            # the same class of failure the comment above already describes,
            # just not reachable through a relation-name mismatch. Scan
            # output is the ground truth of what a scan can filter on.
            target_identity = _phys_identity(target_col)
            scan_schema = getattr(scan, "schema", None)
            scan_identities = (
                {c.identity for c in scan_schema.columns} if scan_schema is not None else set()
            )
            if target_identity is None or target_identity not in scan_identities:
                continue

            # REDUNDANCY GUARD: compare against the SCAN's own range, not the
            # join's. _intersect_join_keys has already replaced both keys'
            # ranges on the join node with their intersection, so at that level
            # every pair looks identical and nothing would ever push.
            if not _tightens(value_range, _key_value_range(getattr(scan, "statistics", None), target_col)):
                continue

            connector = getattr(scan, "connector", None)
            if connector is not None and getattr(connector, "supports_predicate_pushdown", False):
                if not scan.predicates:
                    scan.predicates = []
                for condition in conditions:
                    if not _predicate_already_present(scan.predicates, condition):
                        scan.predicates.append(condition)
                        self.telemetry.optimization_inner_join_correlated_filter += 1
            else:
                # Fallback for non-pushdown connectors: a Filter node still
                # filters at execution, just without row-group pruning.
                for condition in conditions:
                    filter_node = LogicalPlanNode(
                        node_type=LogicalPlanStepType.Filter,
                        condition=condition,
                        columns=[target_col],
                        relations={target_relation},
                        all_relations={target_relation},
                    )
                    context.optimized_plan.insert_node_after(
                        random_string(), filter_node, reader_nid
                    )
                    self.telemetry.optimization_inner_join_correlated_filter += 1

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # This strategy mutates scan predicates / adds Filter nodes, so the
        # statistics propagated before it are now stale; flag them so the next
        # cost strategy refreshes. (Cost strategies don't get the heuristic
        # auto-invalidation from the driver.)
        plan.statistics_are_stale = True
        return plan

    def should_i_run(self, plan):
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Join,))
        return len(candidates) > 0
