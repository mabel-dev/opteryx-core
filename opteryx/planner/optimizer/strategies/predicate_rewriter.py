# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Predicate rewriter

Type: Heuristic
Goal: Chose more efficient predicate evaluations

We rewrite conditions to a more optimal form based on two objectives:
1) the execution of the condition is faster
2) the condition is more likely to be able to be pushed to the storage layer (where its faster)

Rewrites Implemented:

x IN (single_value)                         → x = single_value
x NOT IN (single_value)                     → x != single_value
x LIKE 'pattern'                            → x = 'pattern' (when no wildcards)
x NOT LIKE 'pattern'                        → x != 'pattern' (when no wildcards)
x LIKE '%pattern%'                          → x INSTR 'pattern' (for contains without underscores)
x NOT LIKE '%pattern%'                      → x NOT INSTR 'pattern' (for contains without underscores)
x ILIKE '%pattern%'                         → x IINSTR 'pattern' (case-insensitive version)
x NOT ILIKE '%pattern%'                     → x NOT IINSTR 'pattern' (case-insensitive version)
x LIKE '%%%pattern%%'                       → x LIKE '%pattern%' (removing adjacent wildcards)
x ANY_OP = value                            → x IN (value) (when right side is a literal)
end - start > interval                      → start + interval < end (for date comparisons)
CASE WHEN x IS NULL THEN y ELSE x END       → IFNULL(x, y)
CASE WHEN x THEN y ELSE z END               → IIF(x, y, z)
COALESCE(x, y)                              → IFNULL(x, y) (when only two parameters)
SUBSTRING(x, 1, n)                          → LEFT(x, n) (when starting at position 1)
x LIKE 'pattern1%' OR x LIKE '%pattern2'    → x REGEX '^pattern1.*|.*pattern2$' (for ORed LIKE conditions)
CONCAT(x, y, z)                             → x || y || z (CONCAT to operators)
CONCAT_WS(x, y, z)                          → y || x || z (CONCAT_WS to operators)
x = 'a' OR x = 'b' OR x = 'c'               → x IN ('a', 'b', 'c') (for ORed Equals conditions)
a = ANY(z) OR b = ANY(z) OR c = ANY(z)      → (a, b, c) @> z

#### IN THE PREDICATE ORDERING STRATEGY
a = ANY(z) AND b = ANY(z) AND c = ANY(z)    → z @>> (a, b, c)
"""

import re
from typing import Callable, Dict

from opteryx.expression import ExpressionColumn, NodeType, format_expression
from opteryx.models import Node, QueryTelemetry
from opteryx.planner import build_literal_node
from opteryx.planner.binder.operator_map import determine_type
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from opteryx.types import OrsoTypes
from opteryx.types.schema import ConstantColumn
from opteryx.utils.dates import add_single_unit, parse_iso, truncate_single
from opteryx.utils.sql import sql_like_to_regex

from .optimization_strategy import OptimizationStrategy, OptimizerContext

# fmt: off
IN_REWRITES = {"InList": "Eq", "NotInList": "NotEq"}
LIKE_REWRITES = {"Like": "Eq", "NotLike": "NotEq"}
LITERALS_TO_THE_RIGHT = {"Plus": "Minus", "Minus": "Plus"}
INSTR_REWRITES = {"Like": "InStr", "NotLike": "NotInStr", "ILike": "IInStr", "NotILike": "NotIInStr"}
# fmt: on


def rewrite_in_to_eq(predicate):
    """
    Rewrite IN conditions with a single value to equality conditions.

    If the IN condition contains only one value, it is equivalent to an equality check.
    This optimization replaces the IN condition with a faster equality check.
    """
    predicate.value = IN_REWRITES[predicate.value]
    predicate.right.value = tuple(predicate.right.value)[0]
    predicate.right.type = predicate.right.element_type or OrsoTypes.VARCHAR
    predicate.right.element_type = None
    return predicate


def reorder_interval_calc(predicate):
    """
    rewrite:
        end - start > interval => start + interval > end

    This is because comparing a Date with a Date is faster than
    comparing in Interval with an Interval.
    """
    date_start = predicate.left.right
    date_end = predicate.left.left
    interval = predicate.right

    # Check if the operation is date - date
    if predicate.left.value == "Minus":
        # Create a new binary operator node for date + interval
        new_binary_op = Node(
            node_type=NodeType.BINARY_OPERATOR,
            value="Plus",
            left=date_start,
            right=interval,
        )
        binary_op_column_name = format_expression(new_binary_op, True)
        new_binary_op.schema_column = ExpressionColumn(
            name=binary_op_column_name, type=OrsoTypes.TIMESTAMP
        )

        # Create a new comparison operator node for date > date
        predicate.node_type = NodeType.COMPARISON_OPERATOR
        predicate.right = new_binary_op
        predicate.left = date_end

        predicate_column_name = format_expression(predicate, True)
        predicate.schema_column = ExpressionColumn(
            name=predicate_column_name, type=OrsoTypes.BOOLEAN
        )

        return predicate


def rewrite_ored_like_to_regex(predicate, telemetry):
    """
    Rewrite multiple OR'ed LIKE conditions on the same column to a single regex pattern.

    Example:
    col LIKE 'pattern1%' OR col LIKE '%pattern2' OR col LIKE '%pattern3%'
    -->
    col REGEX '^pattern1.*|.*pattern2$|.*pattern3.*$'

    This optimization reduces multiple string pattern checks to a single regex evaluation.
    """
    # Collect LIKE conditions that can be combined
    like_conditions = {}

    def collect_likes(node, likes_dict):
        # Base case: LIKE/ILIKE condition
        if node.node_type == NodeType.COMPARISON_OPERATOR and node.value in {"Like", "ILike"}:
            # Only proceed if the right side is a literal
            if node.right.node_type == NodeType.LITERAL:
                # Get column identifier for grouping
                col_id = None
                if node.left.node_type == NodeType.IDENTIFIER:
                    col_id = node.left.schema_column.identity

                if col_id:
                    is_case_sensitive = node.value == "Like"
                    if col_id not in likes_dict:
                        likes_dict[col_id] = {
                            "patterns": [],
                            "nodes": [],
                        }

                    likes_dict[col_id]["patterns"].append(
                        sql_like_to_regex(
                            node.right.value, full_match=False, case_sensitive=is_case_sensitive
                        )
                    )
                    likes_dict[col_id]["nodes"].append(node)
            return

        # Recursive cases
        if node.node_type == NodeType.OR:
            collect_likes(node.left, likes_dict)
            collect_likes(node.right, likes_dict)

    collect_likes(predicate, like_conditions)

    for col_id, like_data in like_conditions.items():
        if len(like_data["patterns"]) > 1:
            telemetry.optimization_predicate_rewriter_like_to_regex += 1
            # Create a new regex pattern
            regex_pattern = "|".join(pattern for pattern in like_data["patterns"])
            new_node = like_data["nodes"][0]
            new_node.value = "RLike"
            new_node.right.value = regex_pattern
            for node in like_data["nodes"][1:]:
                node.value = False
                node.node_type = NodeType.LITERAL
                node.type = OrsoTypes.BOOLEAN

    return predicate


def rewrite_ored_any_eq_to_contains(predicate, telemetry):
    """
    Rewrite multiple OR'ed ANYOPEQ conditions on the same column to a single @> condition.

    Example:
    'a' = ANY(z) OR 'b' = ANY(z) OR 'c' = ANY(z)
    -->
    ('a', 'b', 'c') @> z

    This rewrite reduces many repeated ANY checks to a single containment operation.
    """
    anyeq_conditions = {}

    def collect_any_eq(node, grouped):
        if node.node_type == NodeType.COMPARISON_OPERATOR and node.value == "AnyOpEq":
            # Match only: literal = ANY(identifier)
            if (
                node.left.node_type == NodeType.LITERAL
                and node.right.node_type == NodeType.IDENTIFIER
            ):
                col_id = node.right.schema_column.identity
                if col_id not in grouped:
                    grouped[col_id] = {"values": [], "nodes": [], "column_node": node.right}
                grouped[col_id]["values"].append(node.left.value)
                grouped[col_id]["nodes"].append(node)
            return

        if node.node_type == NodeType.OR:
            collect_any_eq(node.left, grouped)
            collect_any_eq(node.right, grouped)

    collect_any_eq(predicate, anyeq_conditions)

    for data in anyeq_conditions.values():
        if len(data["values"]) > 1:
            telemetry.optimization_predicate_rewriter_anyeq_to_contains += 1

            # Build new comparison node: ('a', 'b', 'c') @> z
            new_node = data["nodes"][0]

            new_node.left.value = list(set(data["values"]))
            new_node.left.element_type = new_node.left.type
            new_node.left.type = OrsoTypes.ARRAY
            new_node.left.schema_column = ConstantColumn(
                name=new_node.left.name,
                type=OrsoTypes.ARRAY,
                element_type=new_node.left.element_type,
                value=new_node.left.value,
            )

            new_node.value = "AtArrow"
            new_node.node_type = NodeType.COMPARISON_OPERATOR
            new_node.right = data["column_node"]

            new_node.left, new_node.right = new_node.right, new_node.left  # Swap sides

            # Disable the remaining OR nodes
            for node in data["nodes"][1:]:
                node.node_type = NodeType.LITERAL
                node.type = OrsoTypes.BOOLEAN
                node.value = False

    return predicate


def rewrite_ored_eq_to_inlist(predicate, telemetry):
    """
    Rewrite multiple OR'ed Equals conditions on the same column to a single regex pattern.

    Example:
    name = 'Earth' OR name = 'Mars' OR name = 'Venus'
    -->
    name IN ('Earth', 'Mars', 'Venus')
    """
    # Collect Equals conditions that can be combined
    eq_conditions = {}

    def collect_eqs(node, eqs_dict):
        # Base case: LIKE/ILIKE condition
        if node.node_type == NodeType.COMPARISON_OPERATOR and node.value in {"Eq"}:
            # Only proceed if the right side is a literal
            if node.right.node_type == NodeType.LITERAL:
                # Get column identifier for grouping
                col_id = None
                if node.left.node_type == NodeType.IDENTIFIER:
                    col_id = node.left.schema_column.identity

                if col_id:
                    if col_id not in eqs_dict:
                        eqs_dict[col_id] = {
                            "values": [],
                            "nodes": [],
                        }

                    eqs_dict[col_id]["values"].append(node.right.value)
                    eqs_dict[col_id]["nodes"].append(node)
            return

        # Recursive cases
        if node.node_type == NodeType.OR:
            collect_eqs(node.left, eqs_dict)
            collect_eqs(node.right, eqs_dict)

    collect_eqs(predicate, eq_conditions)

    for col_id, eq_data in eq_conditions.items():
        if len(eq_data["values"]) > 1:
            telemetry.optimization_predicate_rewriter_eqs_to_list += 1
            # Create a new regex pattern
            new_node = eq_data["nodes"][0]
            new_node.value = "InList"
            new_node.right.value = list(set(eq_data["values"]))
            new_node.right.element_type = new_node.right.type
            new_node.right.type = OrsoTypes.ARRAY
            new_node.right.schema_column = ConstantColumn(
                name=new_node.right.name,
                type=OrsoTypes.ARRAY,
                element_type=new_node.right.element_type,
                value=new_node.right.value,
            )
            for node in eq_data["nodes"][1:]:
                node.value = False
                node.node_type = NodeType.LITERAL
                node.type = OrsoTypes.BOOLEAN

    return predicate


def rewrite_date_trunc_to_range(predicate, telemetry: QueryTelemetry):
    """
    Rewrite temporal TRUNC comparisons to range comparisons for better pushdown eligibility.

    Examples:
    TRUNC(col, 'year') = '1970-01-01'  → col >= '1970-01-01' AND col < '1971-01-01'
    TRUNC(col, 'month') <= '2021-02-01' → col < '2021-03-01'
    TRUNC(col, 'day') > '2021-01-15'    → col >= '2021-01-16'
    """

    # Extract the TRUNC function and the comparison value
    # Determine which side is the function and which is the literal
    if predicate.left.node_type == NodeType.FUNCTION and predicate.left.value == "TRUNC":
        func_node = predicate.left
        literal_node = predicate.right
        operator = predicate.value
        is_left_func = True
    elif predicate.right.node_type == NodeType.FUNCTION and predicate.right.value == "TRUNC":
        func_node = predicate.right
        literal_node = predicate.left
        operator = predicate.value
        # Flip the operator if the function is on the right
        flip_ops = {
            "Lt": "Gt",
            "Gt": "Lt",
            "LtEq": "GtEq",
            "GtEq": "LtEq",
            "Eq": "Eq",
            "NotEq": "NotEq",
        }
        operator = flip_ops.get(operator, operator)
        is_left_func = False
    else:
        return predicate

    # Ensure the function has the right structure
    if len(func_node.parameters) != 2:
        return predicate

    column_node = func_node.parameters[0]
    unit_node = func_node.parameters[1]

    # Unit must be a literal string
    if unit_node.node_type != NodeType.LITERAL or not isinstance(unit_node.value, str):
        return predicate

    unit = unit_node.value.lower()

    # Literal must be a string or timestamp
    if literal_node.node_type != NodeType.LITERAL:
        return predicate

    # Parse the literal value
    parsed_literal = parse_iso(literal_node.value)
    if parsed_literal is None:
        return predicate

    # Compute floor and next boundary
    try:
        floor_val = truncate_single(parsed_literal, unit)
        next_floor = add_single_unit(floor_val, unit, 1)
    except ValueError:
        # Unsupported unit
        return predicate

    # Determine if the literal is aligned (already at the boundary)
    is_aligned = parsed_literal == floor_val

    telemetry.optimization_predicate_rewriter_date_trunc_to_range += 1

    # Get the column's actual type to match the literal type
    column_type = OrsoTypes.VARCHAR
    if hasattr(column_node, "schema_column") and column_node.schema_column:
        column_type = column_node.schema_column.type

    # Helper function to create a literal timestamp node with VARCHAR type
    # (ISO format strings work fine for timestamp comparisons)
    def make_timestamp_literal(dt):
        lit = Node(
            node_type=NodeType.LITERAL,
            value=dt,
            type=column_type,
        )
        lit.schema_column = ExpressionColumn(name="", type=column_type)
        return lit

    # Rewrite based on operator and alignment
    if operator == "Eq":
        if not is_aligned:
            # Non-aligned equality is always false
            predicate.node_type = NodeType.LITERAL
            predicate.type = OrsoTypes.BOOLEAN
            predicate.value = False
            return predicate

        # Aligned equality: col >= floor AND col < next
        floor_literal = make_timestamp_literal(floor_val)
        next_literal = make_timestamp_literal(next_floor)

        gte_pred = Node(
            node_type=NodeType.COMPARISON_OPERATOR,
            value="GtEq",
            left=column_node,
            right=floor_literal,
            schema_column=ExpressionColumn(name="", type=OrsoTypes.BOOLEAN),
        )

        lt_pred = Node(
            node_type=NodeType.COMPARISON_OPERATOR,
            value="Lt",
            left=column_node,
            right=next_literal,
            schema_column=ExpressionColumn(name="", type=OrsoTypes.BOOLEAN),
        )

        # Create AND node
        predicate.node_type = NodeType.AND
        predicate.value = "And"
        predicate.left = gte_pred
        predicate.right = lt_pred

    elif operator == "Lt":
        # col < floor
        predicate.left = column_node
        predicate.right = make_timestamp_literal(floor_val)
        predicate.value = "Lt"

    elif operator == "LtEq":
        # col < next_floor
        predicate.left = column_node
        predicate.value = "Lt"
        predicate.right = make_timestamp_literal(next_floor)

    elif operator == "Gt":
        # col >= next_floor
        predicate.left = column_node
        predicate.value = "GtEq"
        predicate.right = make_timestamp_literal(next_floor)

    elif operator == "GtEq":
        # col >= floor (aligned) or col >= next_floor (non-aligned)
        bound = floor_val if is_aligned else next_floor
        predicate.left = column_node
        predicate.value = "GtEq"
        predicate.right = make_timestamp_literal(bound)

    elif operator == "NotEq":
        # col < floor OR col >= next_floor - create an OR node
        lt_pred = Node(
            node_type=NodeType.COMPARISON_OPERATOR,
            value="Lt",
            left=column_node,
            right=make_timestamp_literal(floor_val),
            schema_column=ExpressionColumn(name="", type=OrsoTypes.BOOLEAN),
        )

        gte_pred = Node(
            node_type=NodeType.COMPARISON_OPERATOR,
            value="GtEq",
            left=column_node,
            right=make_timestamp_literal(next_floor),
            schema_column=ExpressionColumn(name="", type=OrsoTypes.BOOLEAN),
        )

        predicate.node_type = NodeType.OR
        predicate.value = "Or"
        predicate.left = lt_pred
        predicate.right = gte_pred

    return predicate


# Define dispatcher conditions and actions
dispatcher: Dict[str, Callable] = {
    "rewrite_in_to_eq": rewrite_in_to_eq,
    "reorder_interval_calc": reorder_interval_calc,
    "rewrite_date_trunc_to_range": rewrite_date_trunc_to_range,
}


# Dispatcher conditions
def _rebind_function_node(function_node):
    """Rebind a newly-created function node to its catalog entry."""
    from opteryx.expression.functions import get_catalog

    resolved = get_catalog().resolve(function_node.value, list(function_node.parameters))
    if resolved is None:
        raise ValueError(f"Unable to resolve function '{function_node.value}'")
    function_node.function_ref = resolved


def _rewrite_predicate(predicate, telemetry: QueryTelemetry):
    if predicate.node_type == NodeType.FUNCTION:
        return _rewrite_function(predicate, telemetry)

    # Add our new rewrite for ORed LIKE conditions
    if predicate.node_type == NodeType.OR:
        rewritten = rewrite_ored_like_to_regex(predicate, telemetry)
        rewritten = rewrite_ored_eq_to_inlist(rewritten, telemetry)
        rewritten = rewrite_ored_any_eq_to_contains(rewritten, telemetry)
        if rewritten != predicate:
            return rewritten

    # if predicate.node_type in {NodeType.AND, NodeType.OR, NodeType.XOR}:
    if predicate.left:
        predicate.left = _rewrite_predicate(predicate.left, telemetry)
    if predicate.right:
        predicate.right = _rewrite_predicate(predicate.right, telemetry)
    if predicate.centre:
        predicate.centre = _rewrite_predicate(predicate.centre, telemetry)

    if predicate.node_type not in {NodeType.BINARY_OPERATOR, NodeType.COMPARISON_OPERATOR}:
        # after rewrites, some filters aren't actually predicates
        return predicate

    # Rewrite temporal TRUNC comparisons to range comparisons
    if predicate.node_type == NodeType.COMPARISON_OPERATOR:
        if (predicate.left.node_type == NodeType.FUNCTION and predicate.left.value == "TRUNC") or (
            predicate.right.node_type == NodeType.FUNCTION and predicate.right.value == "TRUNC"
        ):
            predicate = rewrite_date_trunc_to_range(predicate, telemetry)
            # After rewrite, return early if it's no longer a comparison (e.g., became a literal or AND node)
            if predicate.node_type != NodeType.COMPARISON_OPERATOR:
                return predicate

    if predicate.right.type == OrsoTypes.VARCHAR:
        if predicate.value in {"Like", "ILike", "NotLike", "NotILike"}:
            if "%%" in predicate.right.value:
                telemetry.optimization_predicate_rewriter_remove_adjacent_wildcards += 1
                predicate.right.value = re.sub(r"%+", "%", predicate.right.value)

        if predicate.value in LIKE_REWRITES:
            if "%" not in predicate.right.value and "_" not in predicate.right.value:
                telemetry.optimization_predicate_rewriter_remove_redundant_like += 1
                predicate.value = LIKE_REWRITES[predicate.value]

        if predicate.value in INSTR_REWRITES:
            if (
                "_" not in predicate.right.value
                and predicate.right.value.endswith("%")
                and predicate.right.value.startswith("%")
                and "%" not in predicate.right.value[1:-1]
            ):
                telemetry.optimization_predicate_rewriter_replace_like_with_in_string += 1
                predicate.right.value = predicate.right.value[1:-1]
                predicate.value = INSTR_REWRITES[predicate.value]

        if predicate.value in {"Like", "ILike", "NotLike", "NotILike"}:
            if (
                predicate.right.value.endswith("%")
                and "%" not in predicate.right.value[:-1]
                and "_" not in predicate.right.value
            ):
                telemetry.optimization_predicate_rewriter_replace_like_with_starts_with += 1
                pattern_bytes = predicate.right.value[:-1].encode()
                predicate.node_type = NodeType.FUNCTION
                predicate.parameters = [
                    predicate.left,
                    build_literal_node(pattern_bytes),
                    build_literal_node(predicate.value in {"ILike", "NotILike"}),
                    build_literal_node(predicate.value in {"NotLike", "NotILike"}),
                ]
                predicate.value = "_STARTS_WITH"
                predicate.left = None
                predicate.right = None
                _rebind_function_node(predicate)
            elif (
                predicate.right.value.startswith("%")
                and "%" not in predicate.right.value[1:]
                and "_" not in predicate.right.value
            ):
                telemetry.optimization_predicate_rewriter_replace_like_with_ends_with += 1
                pattern_bytes = predicate.right.value[1:].encode()
                predicate.node_type = NodeType.FUNCTION
                predicate.parameters = [
                    predicate.left,
                    build_literal_node(pattern_bytes),
                    build_literal_node(predicate.value in {"ILike", "NotILike"}),
                    build_literal_node(predicate.value in {"NotLike", "NotILike"}),
                ]
                predicate.value = "_ENDS_WITH"
                predicate.left = None
                predicate.right = None
                _rebind_function_node(predicate)

    # If the predicate was transformed to a FUNCTION node, return early
    if predicate.node_type == NodeType.FUNCTION:
        return predicate

    if predicate.right.type == OrsoTypes.BLOB:
        if predicate.value in {"Like", "ILike", "NotLike", "NotILike"}:
            if b"%%" in predicate.right.value:
                telemetry.optimization_predicate_rewriter_remove_adjacent_wildcards += 1
                predicate.right.value = re.sub(b"%+", b"%", predicate.right.value)

        if predicate.value in LIKE_REWRITES:
            if b"%" not in predicate.right.value and b"_" not in predicate.right.value:
                telemetry.optimization_predicate_rewriter_remove_redundant_like += 1
                predicate.value = LIKE_REWRITES[predicate.value]

        if predicate.value in INSTR_REWRITES:
            if (
                b"_" not in predicate.right.value
                and predicate.right.value.endswith(b"%")
                and predicate.right.value.startswith(b"%")
            ):
                telemetry.optimization_predicate_rewriter_replace_like_with_in_string += 1
                predicate.right.value = predicate.right.value[1:-1]
                predicate.value = INSTR_REWRITES[predicate.value]

        if predicate.value in {"Like", "ILike", "NotLike", "NotILike"}:
            if (
                predicate.right.value.endswith(b"%")
                and b"%" not in predicate.right.value[:-1]
                and b"_" not in predicate.right.value
            ):
                telemetry.optimization_predicate_rewriter_replace_like_with_starts_with += 1
                pattern_bytes = predicate.right.value[:-1]
                predicate.node_type = NodeType.FUNCTION
                predicate.parameters = [
                    predicate.left,
                    build_literal_node(pattern_bytes),
                    build_literal_node(predicate.value in {"ILike", "NotILike"}),
                    build_literal_node(predicate.value in {"NotLike", "NotILike"}),
                ]
                predicate.value = "_STARTS_WITH"
                predicate.left = None
                predicate.right = None
                _rebind_function_node(predicate)
            elif (
                predicate.right.value.startswith(b"%")
                and b"%" not in predicate.right.value[1:]
                and b"_" not in predicate.right.value
            ):
                telemetry.optimization_predicate_rewriter_replace_like_with_ends_with += 1
                pattern_bytes = predicate.right.value[1:]
                predicate.node_type = NodeType.FUNCTION
                predicate.parameters = [
                    predicate.left,
                    build_literal_node(pattern_bytes),
                    build_literal_node(predicate.value in {"ILike", "NotILike"}),
                    build_literal_node(predicate.value in {"NotLike", "NotILike"}),
                ]
                predicate.value = "_ENDS_WITH"
                predicate.left = None
                predicate.right = None
                _rebind_function_node(predicate)

    if predicate.value == "AnyOpEq":
        if predicate.right.node_type == NodeType.LITERAL:
            telemetry.optimization_predicate_rewriter_any_to_inlist += 1
            predicate.value = "InList"

    if predicate.value == "AnyOpNotEq":
        if predicate.right.node_type == NodeType.LITERAL:
            telemetry.optimization_predicate_rewriter_any_to_inlist += 1
            predicate.value = "NotInList"

    if predicate.value in IN_REWRITES:
        if predicate.right.node_type == NodeType.LITERAL and len(predicate.right.value) == 1:
            telemetry.optimization_predicate_rewriter_in_to_equals += 1
            return dispatcher["rewrite_in_to_eq"](predicate)

    if (
        predicate.node_type == NodeType.COMPARISON_OPERATOR
        and predicate.left.node_type == NodeType.BINARY_OPERATOR
    ):
        if (
            determine_type(predicate.left) == OrsoTypes.INTERVAL
            and determine_type(predicate.right) == OrsoTypes.INTERVAL
        ):
            telemetry.optimization_predicate_rewriter_date_ += 1
            predicate = dispatcher["reorder_interval_calc"](predicate)

    return predicate


def _rewrite_function(function, telemetry: QueryTelemetry):
    def _rebind_function_ref():
        # Rebind the function reference when the function name or parameters have been rewritten.
        # The binder runs before the optimizer, so we must update node.function_ref here.
        from opteryx.expression.functions import get_catalog

        resolved = get_catalog().resolve(function.value, list(function.parameters))
        if resolved is None:
            raise ValueError(f"Unable to resolve rewritten function '{function.value}'")
        function.function_ref = resolved
        if getattr(function, "schema_column", None) is not None and resolved.inferred_return_type:
            function.schema_column.type = resolved.inferred_return_type

    def _normalise_dfa_replacement(value):
        if isinstance(value, bytes):
            return re.sub(rb"\\\\([0-9])", rb"\\\1", value)
        if isinstance(value, str):
            return re.sub(r"\\\\([0-9])", r"\\\1", value)
        return value

    def _compile_dfa_program_blob(pattern_value, replacement_value):
        from opteryx.compiled import vector_ops as compiled_vector_ops

        if isinstance(pattern_value, str):
            pattern_value = pattern_value.encode("utf8")
        elif not isinstance(pattern_value, bytes):
            return None

        if isinstance(replacement_value, str):
            replacement_value = replacement_value.encode("utf8")
        elif not isinstance(replacement_value, bytes):
            return None

        return compiled_vector_ops.compile_dfa_program(pattern_value, replacement_value)

    def _rewrite_regexp_replace_to_dfa():
        if function.value != "REGEXP_REPLACE" or len(function.parameters) != 3:
            return None

        pattern_node = function.parameters[1]
        replacement_node = function.parameters[2]

        if (
            pattern_node.node_type != NodeType.LITERAL
            or replacement_node.node_type != NodeType.LITERAL
        ):
            return None

        pattern_value = pattern_node.value
        replacement_value = _normalise_dfa_replacement(replacement_node.value)
        compiled_program = _compile_dfa_program_blob(pattern_value, replacement_value)

        if compiled_program is None:
            return None

        telemetry.optimization_predicate_rewriter_regex_replace_to_dfa += 1
        function.value = "_DFA_REPLACE"
        function.parameters = [
            function.parameters[0],
            build_literal_node(
                compiled_program,
                root=function.parameters[1],
                suggested_type=OrsoTypes.BLOB,
            ),
        ]
        _rebind_function_ref()
        return function

    rewritten = _rewrite_regexp_replace_to_dfa()
    if rewritten is not None:
        return rewritten

    if function.value == "_CASE":
        # CASE WHEN x IS NULL THEN y ELSE x END → IFNULL(x, y)
        if len(function.parameters) == 2 and function.parameters[0].parameters[0].value == "IsNull":
            compare_column = function.parameters[0].parameters[0].centre
            target_column = function.parameters[1].parameters[1]
            value_if_null = function.parameters[1].parameters[0]

            if compare_column.schema_column.identity == target_column.schema_column.identity:
                telemetry.optimization_predicate_rewriter_case_to_ifnull += 1
                function.value = "IFNULL"
                function.parameters = [compare_column, value_if_null]
                _rebind_function_ref()
                return function
        # CASE WHEN x THEN y ELSE z END → IIF(x, y, z)
        if (
            len(function.parameters) == 2
            and len(function.parameters[0].parameters) == 2
            and function.parameters[0].parameters[1].value is True
            and len(function.parameters[1].parameters) == 2
        ):
            telemetry.optimization_predicate_rewriter_case_to_iif += 1

            compare_column = function.parameters[0].parameters[0]
            value_if_true = function.parameters[1].parameters[0]
            value_if_false = function.parameters[1].parameters[1]

            function.value = "IIF"
            function.parameters = [compare_column, value_if_true, value_if_false]
            _rebind_function_ref()
            return function
    # COALESCE(x, y) → IFNULL(x, y)
    if function.value == "COALESCE":
        if len(function.parameters) == 2:
            telemetry.optimization_predicate_rewriter_coalesce_to_ifnull += 1
            function.value = "IFNULL"
            _rebind_function_ref()
            return function
    # SUBSTRING(x, 1, n) → LEFT(x, n)
    if function.value == "SUBSTRING" and function.parameters[1].value == 1:
        telemetry.optimization_predicate_rewriter_substring_to_left += 1
        function.value = "LEFT"
        function.parameters = [function.parameters[0], function.parameters[2]]
        return function
    # CONCAT(x, y, z) → x || y || z
    if function.value == "CONCAT" and len(function.parameters) > 1:
        telemetry.optimization_predicate_rewriter_concat_to_double_pipe += 1
        left_node = function.parameters[0]
        for param in function.parameters[1:]:
            left_node = Node(
                node_type=NodeType.BINARY_OPERATOR,
                value="StringConcat",
                left=left_node,
                right=param,
                schema_column=ExpressionColumn(name="", type=OrsoTypes.VARCHAR),
            )
        left_node.alias = function.alias
        left_node.schema_column = function.schema_column
        function = left_node
    # CONCAT_WS(x, y, z) → y || x || z
    if function.value == "CONCAT_WS" and len(function.parameters) > 2:
        telemetry.optimization_predicate_rewriter_concatws_to_double_pipe += 1
        separator = function.parameters[0]
        left_node = function.parameters[1]
        for param in function.parameters[2:]:
            separator_node = Node(
                node_type=NodeType.BINARY_OPERATOR,
                value="StringConcat",
                left=left_node,
                right=separator,
                schema_column=ExpressionColumn(name="", type=OrsoTypes.VARCHAR),
            )
            left_node = Node(
                node_type=NodeType.BINARY_OPERATOR,
                value="StringConcat",
                left=separator_node,
                right=param,
                schema_column=ExpressionColumn(name="", type=OrsoTypes.VARCHAR),
            )
        left_node.alias = function.alias
        left_node.schema_column = function.schema_column
        function = left_node

    return function


class PredicateRewriteStrategy(OptimizationStrategy):
    def _rewrite_expression_list(self, expressions, telemetry):
        if not expressions:
            return expressions

        rewritten = []
        for expr in expressions:
            rewritten.append(_rewrite_predicate(expr, telemetry))
        return rewritten

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Filter:
            node.condition = _rewrite_predicate(node.condition, self.telemetry)
            context.optimized_plan[context.node_id] = node

        if node.node_type == LogicalPlanStepType.Project:
            node.columns = self._rewrite_expression_list(node.columns, self.telemetry)
            context.optimized_plan[context.node_id] = node

        if node.node_type in {LogicalPlanStepType.Aggregate, LogicalPlanStepType.AggregateAndGroup}:
            if getattr(node, "groups", None):
                node.groups = self._rewrite_expression_list(node.groups, self.telemetry)
            if getattr(node, "aggregates", None):
                node.aggregates = self._rewrite_expression_list(node.aggregates, self.telemetry)
            if getattr(node, "projection", None):
                node.projection = self._rewrite_expression_list(node.projection, self.telemetry)
            context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
