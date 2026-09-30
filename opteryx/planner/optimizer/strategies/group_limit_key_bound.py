# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Key Bound for GROUP BY ... LIMIT (no ORDER BY)

Type: Heuristic
Goal: Read only the files that can hold the groups a LIMIT will return

    SELECT k, COUNT(*) FROM t GROUP BY k LIMIT 10

With no ORDER BY, ANY 10 groups are a correct answer, so the planner may choose
which. It chooses the ones a manifest can PROVE exist below a bound T, and adds
`k <= T` ahead of predicate pushdown so the ordinary manifest and row-group
pruning drop every file that cannot hold a qualifying key.

The proof. A file's min and max are values that exist in it. Collect the distinct
witness values across files; the K-th smallest (K = LIMIT + OFFSET) is T. At
least K distinct keys are <= T, so the filtered aggregate still produces >= K
groups. The filter keeps EVERY row of each surviving key, so each group's
aggregates are complete - this is a choice of groups, never a partial count.

Strings. Stored string bounds may be truncated: a stored min is a byte PREFIX p
of the true min, so the true min is < inc(p) (p with its last byte bumped) - a
real value no greater than inc(p). Only mins are used as string witnesses, and
only a prefix-free set of them (a witness that is a prefix of another could be
the same true value), so the K witnesses are distinct real values all <= the
largest inc(p). Over-capturing is fine; the rule needs "at least K", never
"exactly K".

Abandoned - quickly, and silently (no behaviour changes) - unless ALL hold:
- Limit -> Project* -> AggregateAndGroup -> Project* -> Scan, nothing else between;
  a Filter, Join, Sort, Distinct or HAVING anywhere in that chain removes rows or
  groups and voids "at least K exist".
- The aggregate has plain GROUP BY keys (no grouping sets).
- The Scan has no predicate, limit or top-N spec, and its manifest is
  authoritative, has no deletes, and bounds the key for EVERY file with values.
- The key is an INTEGER or string column (floats: NaN; temporals: unit encodings
  - not v1).
- At least K distinct provable witnesses exist, and the bound really prunes a
  file.

NULL keys are irrelevant here: no ordering means the NULL group is simply not one
of the groups chosen, and `k <= T` excludes it.
"""

from opteryx.compiled.structures.expressions import Comparison
from opteryx.compiled.structures.plan_steps import FilterStep
from opteryx.expression import NodeType
from opteryx.planner import build_literal_node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner import PlanStep
from opteryx.types.logical_type import LogicalCategory

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan

_STRING_CATEGORIES = (
    LogicalCategory.VARCHAR,
    LogicalCategory.NVARCHAR,
    LogicalCategory.VARBINARY,
)


# The value tags a manifest gives string bounds: text columns are "text", binary "bytes".
_STRING_TAGS = ("text", "bytes")


def _increment(prefix: bytes):
    """The smallest byte string greater than every string that starts with `prefix`,
    or None when there is none (all 0xFF)."""
    stripped = prefix.rstrip(b"\xff")
    if not stripped:
        return None
    return stripped[:-1] + bytes([stripped[-1] + 1])


def _integer_bound(bounds, k):
    """(T, survivor_count) for an integer key, or None."""
    if any(lo[0] != "int" or hi[0] != "int" for lo, hi in bounds):
        return None
    witnesses = sorted({v for lo, hi in bounds for v in (lo[1], hi[1])})
    if len(witnesses) < k:
        return None
    bound = witnesses[k - 1]
    return bound, sum(1 for lo, _hi in bounds if lo[1] <= bound)


def _string_bound(bounds, k):
    """(T, survivor_count) for a string key, or None."""
    if any(lo[0] not in _STRING_TAGS for lo, _hi in bounds):
        return None
    chosen = []
    for prefix in sorted({lo[1] for lo, _hi in bounds}):
        # sorted order: if any chosen witness is a prefix of this one, the LAST chosen is
        if chosen and prefix.startswith(chosen[-1]):
            continue
        chosen.append(prefix)
        if len(chosen) == k:
            break
    if len(chosen) < k:
        return None
    bound = _increment(chosen[-1])
    if bound is None:
        return None
    return bound, sum(1 for lo, _hi in bounds if lo[1] <= bound)


class GroupLimitKeyBoundStrategy(OptimizationStrategy):
    """Bound the scanned keys of `GROUP BY k LIMIT n` using manifest min/max."""

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return any(
            node.limit is not None
            for _, node in get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Limit,))
        )

    def _producer(self, plan, nid):
        ingoing = plan.ingoing_edges(nid)
        return ingoing[0][0] if len(ingoing) == 1 else None

    def _skip_projects(self, plan, nid):
        """nid, walking down through Project nodes; (nid, node) or (None, None)."""
        while nid is not None:
            node = plan[nid]
            if node is None:
                return None, None
            if node.node_type != LogicalPlanStepType.Project:
                return nid, node
            nid = self._producer(plan, nid)
        return None, None

    def visit(self, node: PlanStep, context: OptimizerContext) -> OptimizerContext:
        if node.node_type != LogicalPlanStepType.Limit:
            return context
        limit, offset = node.limit, node.offset
        if type(limit) is not int or limit <= 0 or (offset is not None and type(offset) is not int):
            return context
        k = limit + (offset or 0)

        plan = context.optimized_plan
        agg_nid, agg = self._skip_projects(plan, self._producer(plan, context.node_id))
        if agg is None or agg.node_type != LogicalPlanStepType.AggregateAndGroup:
            return context
        if agg.having_condition is not None or agg.grouping_sets is not None or not agg.groups:
            return context

        scan_nid, scan = self._skip_projects(plan, self._producer(plan, agg_nid))
        if scan is None or scan.node_type != LogicalPlanStepType.Scan:
            return context
        if scan.predicates or scan.limit is not None or scan.topn_limit is not None:
            return context
        manifest = scan.manifest
        if manifest is None or not manifest.stats_are_authoritative or manifest.has_deletes():
            return context
        if manifest.get_file_count() < 2:
            return context

        scan_identities = {c.identity for c in scan.schema.columns}
        best = None  # (survivors, key, bound)
        for key in agg.groups:
            if key.node_type != NodeType.IDENTIFIER or key.schema_column is None:
                continue
            if key.schema_column.identity not in scan_identities:
                continue
            category = key.schema_column.column_type.category
            if category == LogicalCategory.INTEGER:
                derive = _integer_bound
            elif category in _STRING_CATEGORIES:
                derive = _string_bound
            else:
                continue
            bounds = manifest.file_value_bounds(key.schema_column.name)
            if bounds is None:
                continue
            found = derive(bounds, k)
            if found is None:
                continue
            bound, survivors = found
            if survivors >= len(bounds):
                continue
            if best is None or survivors < best[0]:
                best = (survivors, key, bound)
        if best is None:
            return context

        survivors, key, bound = best
        condition = Comparison(
            value="LtEq",
            left=key,
            right=build_literal_node(
                bound, suggested_type=key.schema_column.column_type, plan_context=context.plan_context
            ),
            arena=context.plan_context.expressions,
        )
        filter_node = FilterStep(
            condition=condition,
            columns=[key],
            relations={key.source},
            all_relations={key.source},
        )
        plan.insert_node_after(filter_node, scan_nid)
        self.telemetry.increase("optimization_group_limit_key_bound")
        self.record_decision(
            "group-limit key bound",
            f"{key.schema_column.name} <= bound keeps {survivors} of "
            f"{manifest.get_file_count()} files for {k} group(s)",
        )
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
