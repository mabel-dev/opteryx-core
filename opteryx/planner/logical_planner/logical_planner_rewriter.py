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
from opteryx.compiled.structures.expressions import BinaryOperator
from opteryx.compiled.structures.expressions import Aggregator


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

    ORDER BY and LIMIT ride in the key for the same reason, though they cannot reach
    here today with distinct results — keying on them costs nothing and means a
    dedup here can never be the thing that silently loses an aggregate. (A FILTER
    is lowered into the argument, `AGG(IIF(p, x, NULL))`, before this runs, so the
    argument carries it.)

    The arguments AFTER the operand are part of the key for the same reason again,
    and this one was live: an aggregate is not identified by its operand alone.
    `APPROX_PERCENTILE(x, 0.5)` and `APPROX_PERCENTILE(x, 0.95)` differ only in a
    literal the old key never read, so the second collapsed onto the first and the
    projection then asked for a column nothing computed — the natural
    `p50, p95, p99 of one column` died with "projecting a column the engine could
    not resolve", or, wrapped in a CAST, with a raw stream-layout KeyError.
    `CORR(x, y)` vs `CORR(x, z)` is the same defect on the same key.
    """
    from opteryx.expression import format_expression

    return (
        aggregate.value.upper(),
        aggregate.parameters[0].qualified_name,
        tuple(format_expression(p, True) for p in aggregate.parameters[1:]),
        aggregate.duplicate_treatment,
        aggregate.null_treatment,
        tuple(
            (
                item[0].value
                if item[0].node_type == NodeType.IDENTIFIER
                else format_expression(item[0], True),
                item[1],
            )
            for item in (aggregate.order or [])
        ),
        aggregate.limit,
    )


def _substitute_in_place(projection, aggregate, calculation_node):
    """Put the decomposed calculation in the SELECT-list slot the aggregate held.

    The projection order IS the result's column order; clients read columns by
    position. Filtering the aggregate out and appending its replacement moved every
    decomposed aggregate to the end, so `SELECT SUM(i + 1), SUM(i * 2)` came back
    as `SUM(i * 2), SUM(i + 1)` — right values, wrong headings.
    """
    return [calculation_node if p is aggregate else p for p in projection]


def decompose_aggregates(aggregates, projection):
    """
    decompose aggregates into parts:
    SUM(c + 2) => SUM(c) + COUNT(c) * 2
    """
    aggregate_set = {}
    result_aggregates = []
    result_projection = projection

    for aggregate in aggregates:
        if aggregate.parameters[0].node_type == NodeType.BINARY_OPERATOR and not any(
            p is aggregate for p in result_projection
        ):
            # Only a top-level SELECT item has a slot to take the calculation. Nested
            # (`SUM(i + 1) * 2`) there is nowhere to substitute it: the old code
            # appended a stray column and left the nested aggregate referencing a
            # column nothing computed, so the query died at compile time. Compute
            # the aggregate as written instead.
            result_aggregates.append(aggregate)
            continue
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
                minmax_node = Aggregator(
                    value=aggregate.value, parameters=[identifier], 
                arena=aggregate.arena)
                result_aggregates.append(minmax_node)
                aggregate_set[f"{aggregate.value}_{identifier.qualified_name}"] = minmax_node
            else:
                minmax_node = aggregate_set[f"{aggregate.value}_{identifier.qualified_name}"]

            calculation_node = BinaryOperator(
                value=operator,
                left=minmax_node,
                right=literal,
                alias=aggregate.alias or aggregate.qualified_name,
                arena=aggregate.arena,
            )
            result_projection = _substitute_in_place(result_projection, aggregate, calculation_node)

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
                sum_node = Aggregator(value="SUM", parameters=[identifier], arena=aggregate.arena)
                result_aggregates.append(sum_node)
                aggregate_set[f"SUM_{identifier.qualified_name}"] = sum_node
            else:
                sum_node = aggregate_set[f"SUM_{identifier.qualified_name}"]

            if f"COUNT_{identifier.qualified_name}" not in aggregate_set:
                count_node = Aggregator(
                    value="COUNT", parameters=[identifier], 
                arena=aggregate.arena)
                result_aggregates.append(count_node)
                aggregate_set[f"COUNT_{identifier.qualified_name}"] = count_node
            else:
                count_node = aggregate_set[f"COUNT_{identifier.qualified_name}"]

            scaling_node = BinaryOperator(
                value="Multiply", left=count_node, right=literal, 
            arena=aggregate.arena)
            calculation_node = BinaryOperator(
                value=operator,
                left=sum_node,
                right=scaling_node,
                alias=aggregate.alias or aggregate.qualified_name,
                arena=aggregate.arena,
            )

            result_projection = _substitute_in_place(result_projection, aggregate, calculation_node)

        else:
            result_aggregates.append(aggregate)

    return result_aggregates, result_projection
