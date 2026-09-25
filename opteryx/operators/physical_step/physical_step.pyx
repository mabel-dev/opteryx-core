# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Physical Step — a plan operator the native compiler lowers straight from its
typed logical step.

Most physical operators have no behaviour of their own: the native engine
executes them, and the plan node only tells the compiler what to build. They
used to be one descriptor class per operator, each copying a slice of its
logical step's fields out of a keyword splat. This one class replaces them. It
holds the typed logical step itself (`step`, declared on BasePlanNode) plus
what only the physical planner decides:

  kind                       which native lowering the compiler runs. It keeps
                             the retired descriptor class names ("LimitNode",
                             ...): the native engine reports it as each
                             operator's display name, and telemetry labels key
                             off it.
  join_type                  a join's physical mode ("inner", "nested_loop",
                             "left semi", ...); None for anything not a join.
  join_output_rows_estimate  the planner's output-row estimate for a join's
                             build sink (None = unknown).
  group_count_estimate       the planner's group / distinct-count estimate for
                             a GROUP BY or DISTINCT sink (None = unknown).

Everything the compiler derives from the step (key identities, window function
codes, constant replacements) is derived by the compiler, from the step.

Operators that do real work at plan or execution time — the scan readers, the
function dataset, the DDL and write sinks — keep their own classes.
"""

from opteryx.exceptions import InvalidInternalStateError
from opteryx.expression import format_expression

# BasePlanNode in scope via textual include from _operators.pyx.


# EXPLAIN name per kind. The three joins that carry their variant in the join
# type are named from it instead (see PhysicalStep.name).
cdef dict _NAMES = {
    "CteRefNode": "CTE Reference",
    "FilterNode": "Filter",
    "ProjectionNode": "Projection",
    "UngroupedAggregateNode": "Ungrouped Aggregate",
    "GroupedAggregateHashedNode": "Grouped Aggregate (Hashed)",
    "DistinctNode": "Distinction",
    "SortNode": "Sort",
    "HeapSortNode": "Heap Sort",
    "WindowNode": "Window",
    "FramedWindowNode": "Framed Window",
    "ScalarGuardNode": "SCALAR GUARD",
    "LimitNode": "LIMIT",
    "UnionNode": "Union",
    "UnnestJoinNode": "Cross Join",
    "ExitNode": "Exit",
    "AsofJoinNode": "ASOF Join",
    "BandJoinNode": "Band Join",
    "DrakenInnerJoinNode": "Inner Join Draken",
    "CrossJoinNode": "Cross Join",
    "NestedLoopJoinNode": "Nested Loop Join",
    "OuterJoinNode": None,
    "FilterJoinNode": None,
    "ExistenceJoinNode": None,
}

# The two-input joins, which draw as `JOIN (<type>)` in the mermaid diagram. A
# CROSS JOIN UNNEST is registered as a join but has one input, and draws by name.
cdef frozenset _TWO_INPUT_JOINS = frozenset({
    "AsofJoinNode", "BandJoinNode", "DrakenInnerJoinNode", "CrossJoinNode",
    "NestedLoopJoinNode", "OuterJoinNode", "FilterJoinNode", "ExistenceJoinNode",
})

cdef dict _ASOF_OPERATORS = {"Lt": "<", "LtEq": "<=", "Gt": ">", "GtEq": ">="}


cdef str _join_condition_config(PhysicalStep node, bint with_using):
    step = node.step
    join_type = node.join_type.upper()
    if step.on is not None:
        return f"{join_type} JOIN ({format_expression(step.on, True)})"
    if with_using and step.using:
        return f"{join_type} JOIN (USING {','.join(map(format_expression, step.using))})"
    return join_type


cdef object _config(PhysicalStep node):
    """The one-line EXPLAIN detail for each kind."""
    cdef str kind = node._kind
    step = node.step
    if kind == "FilterNode":
        return format_expression(step.condition)
    if kind == "ProjectionNode":
        return ", ".join(format_expression(column) for column in node.columns)
    if kind == "UngroupedAggregateNode":
        return f"AGGREGATE ({', '.join(format_expression(agg) for agg in step.aggregates or [])})"
    if kind == "GroupedAggregateHashedNode":
        return (
            f"AGGREGATE ({', '.join(format_expression(agg) for agg in step.aggregates)}) "
            f"GROUP BY ({', '.join(format_expression(group) for group in step.groups)})"
        )
    if kind == "SortNode":
        return ", ".join(
            f"{column.value} {'ASC' if ascending else 'DESC'}"
            for column, ascending in step.order_by or []
        )
    if kind == "HeapSortNode":
        order = ", ".join(
            f"{column.schema_column.name} {'ASC' if ascending else 'DESC'}"
            for column, ascending in step.order_by or []
        )
        return f"LIMIT = {-1 if step.limit is None else step.limit}, ORDER = {order}"
    if kind == "LimitNode":
        limit = float("inf") if step.limit is None else step.limit
        offset = 0 if step.offset is None else step.offset
        return f"{limit} OFFSET {offset}"
    if kind == "WindowNode":
        if step.order_by:
            return "window OVER (PARTITION BY ... ORDER BY ...)"
        return "ROW_NUMBER OVER (PARTITION BY ...)"
    if kind == "FramedWindowNode":
        return "window OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE BETWEEN ...)"
    if kind == "CteRefNode":
        return f"({step.cte_name or step.cte_key} AS {step.alias})"
    if kind == "UnnestJoinNode":
        return "CROSS JOIN "
    if kind == "AsofJoinNode":
        operator = _ASOF_OPERATORS.get(step.asof_op, step.asof_op)
        config = f"MATCH_CONDITION({step.asof_left_column} {operator} {step.asof_right_column})"
        if step.left_columns:
            config += f" USING ({', '.join(column.decode('utf8') for column in step.left_columns)})"
        return config
    if kind == "BandJoinNode":
        column = step.band_column_name
        if column is None:
            column = step.band_column.decode("utf8")
        return "%s %s lower, upper%s" % (
            column,
            "IN [" if step.band_lower_closed else "IN (",
            "]" if step.band_upper_closed else ")",
        )
    if kind == "DrakenInnerJoinNode":
        return "draken+carchar"
    if kind == "CrossJoinNode":
        return "CROSS JOIN"
    if kind == "NestedLoopJoinNode":
        return "draken"
    if kind == "OuterJoinNode" or kind == "FilterJoinNode":
        return _join_condition_config(node, True)
    if kind == "ExistenceJoinNode":
        return _join_condition_config(node, False)
    if kind == "ExitNode":
        return None
    # DistinctNode, ScalarGuardNode, UnionNode
    return ""


cdef class PhysicalStep(BasePlanNode):
    """A plan operator lowered straight from its typed logical step (see the
    module docstring)."""

    cdef readonly str _kind
    cdef readonly object join_type
    cdef readonly object join_output_rows_estimate
    cdef readonly object group_count_estimate

    def __init__(
        self,
        properties,
        str kind,
        step,
        *,
        columns=None,
        pre_update_columns=None,
        join_type=None,
        join_output_rows_estimate=None,
        group_count_estimate=None,
    ):
        if kind not in _NAMES:
            raise InvalidInternalStateError(f"'{kind}' is not a physical step kind")
        # Set before the base initialiser, which reads `kind`.
        self._kind = kind
        self.join_type = join_type
        self.join_output_rows_estimate = join_output_rows_estimate
        self.group_count_estimate = group_count_estimate
        BasePlanNode.__init__(self, properties, step, columns, pre_update_columns)

    @property
    def kind(self) -> str:
        return self._kind

    @property
    def name(self):
        name = _NAMES[self._kind]
        if name is None:
            return self.join_type.replace(" ", "_")
        return name

    @property
    def config(self):
        return _config(self)

    def to_mermaid(self, nid):
        if self._kind in _TWO_INPUT_JOINS:
            mermaid = f'NODE_{nid}["**JOIN ({self.join_type.upper()})**<br />'
        else:
            mermaid = f'NODE_{nid}["**{self.name.upper()}**<br />'
        mermaid += f"({self.execution_time / 1_000_000:,.2f}ms)"
        return mermaid + '"]'

    def __repr__(self):
        return f"<PhysicalStep {self._kind}>"
