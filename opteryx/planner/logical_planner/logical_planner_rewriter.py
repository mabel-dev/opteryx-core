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
                key = f"{aggregate.value.upper()}_{aggregate.parameters[0].qualified_name}"
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

        else:
            result_aggregates.append(aggregate)

    return result_aggregates, result_projection
