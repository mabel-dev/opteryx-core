# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Perform some plan rewriting at the logical planning stage.

- Decompose aggregates tries to remove elementwise calculations from aggregates and
  replace them with a single calculation.
"""

from opteryx.expression import NodeType
from opteryx.models import Node


def _dedup_key(aggregate):
    """The identity of an aggregate over a plain column, for de-duplication.

    Two aggregates collapse into one computation only when they compute the same
    thing, so every modifier that changes the VALUE has to be in the key. The
    function name and the operand alone are not enough: `COUNT_DISTINCT` is
    rewritten to `COUNT` + `duplicate_treatment="Distinct"` before this runs (see
    logical_planner_builders), so `SELECT COUNT(x), COUNT_DISTINCT(x)` produced
    the same key twice and the DISTINCT one was dropped without a word — the
    projection then asked for a column nothing computed and the query died at
    compile time with "projecting a column the engine could not resolve".

    FILTER, ORDER BY and LIMIT ride in the key for the same reason. They cannot
    reach here today with distinct results — `format_expression` renders
    `COUNT(x) FILTER (WHERE ...)` identically to `COUNT(x)`, so such a pair is
    rejected upstream as an ambiguous output name — but keying on them costs
    nothing and means a dedup here can never be the thing that silently loses an
    aggregate.
    """
    from opteryx.expression import format_expression

    return (
        aggregate.value.upper(),
        aggregate.parameters[0].qualified_name,
        aggregate.duplicate_treatment,
        aggregate.null_treatment,
        None if aggregate.condition is None else format_expression(aggregate.condition),
        tuple((item[0].value, item[1]) for item in (aggregate.order or [])),
        aggregate.limit,
    )


def decompose_aggregates(aggregates, projection):
    """
    decompose aggregates into parts:
    SUM(c + 2) => SUM(c) + COUNT(c) * 2
    """
    aggregate_set = {}
    result_aggregates = []
    result_projection = projection

    for aggregate in aggregates:
        if aggregate.parameters[0].node_type != NodeType.BINARY_OPERATOR:
            if aggregate.parameters[0].node_type == NodeType.IDENTIFIER:
                key = _dedup_key(aggregate)
                if key in aggregate_set:
                    continue
                result_aggregates.append(aggregate)
                aggregate_set[key] = aggregate
            else:
                result_aggregates.append(aggregate)
            continue
        elif aggregate.value in ("MIN", "MAX"):
            identifier = aggregate.parameters[0].left
            operator = aggregate.parameters[0].value
            literal = aggregate.parameters[0].right

            if (
                identifier.node_type != NodeType.IDENTIFIER
                or literal.node_type != NodeType.LITERAL
                or operator not in ("Plus", "Minus", "Multiply", "Divide")
            ):
                result_aggregates.append(aggregate)
                continue

            if f"{aggregate.value}_{identifier.qualified_name}" not in aggregate_set:
                minmax_node = Node(
                    node_type=NodeType.AGGREGATOR, value=aggregate.value, parameters=[identifier]
                )
                result_aggregates.append(minmax_node)
                aggregate_set[f"{aggregate.value}_{identifier.qualified_name}"] = minmax_node
            else:
                minmax_node = aggregate_set[f"{aggregate.value}_{identifier.qualified_name}"]

            calculation_node = Node(
                node_type=NodeType.BINARY_OPERATOR,
                value=operator,
                left=minmax_node,
                right=literal,
                alias=aggregate.alias or aggregate.qualified_name,
            )
            result_projection = [p for p in result_projection if p != aggregate]

            result_projection.append(calculation_node)

        elif aggregate.value == "SUM":
            identifier = aggregate.parameters[0].left
            operator = aggregate.parameters[0].value
            literal = aggregate.parameters[0].right

            if (
                identifier.node_type != NodeType.IDENTIFIER
                or literal.node_type != NodeType.LITERAL
                or operator not in ("Plus", "Minus")
                or not isinstance(literal.value, int)
                or isinstance(literal.value, bool)
            ):
                result_aggregates.append(aggregate)
                continue

            if f"SUM_{identifier.qualified_name}" not in aggregate_set:
                sum_node = Node(node_type=NodeType.AGGREGATOR, value="SUM", parameters=[identifier])
                result_aggregates.append(sum_node)
                aggregate_set[f"SUM_{identifier.qualified_name}"] = sum_node
            else:
                sum_node = aggregate_set[f"SUM_{identifier.qualified_name}"]

            if f"COUNT_{identifier.qualified_name}" not in aggregate_set:
                count_node = Node(
                    node_type=NodeType.AGGREGATOR, value="COUNT", parameters=[identifier]
                )
                result_aggregates.append(count_node)
                aggregate_set[f"COUNT_{identifier.qualified_name}"] = count_node
            else:
                count_node = aggregate_set[f"COUNT_{identifier.qualified_name}"]

            scaling_node = Node(
                node_type=NodeType.BINARY_OPERATOR, value="Multiply", left=count_node, right=literal
            )
            calculation_node = Node(
                node_type=NodeType.BINARY_OPERATOR,
                value=operator,
                left=sum_node,
                right=scaling_node,
                alias=aggregate.alias or aggregate.qualified_name,
            )

            result_projection = [p for p in result_projection if p != aggregate]
            result_projection.append(calculation_node)

        else:
            result_aggregates.append(aggregate)

    return result_aggregates, result_projection
