# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Join helper functions shared by the binder and join visitor.

Isolated here to break the circular import between common.py (which imports join.py)
and join.py (which needs these functions from common.py).
"""

from typing import List, Optional, Set, Tuple

from opteryx.exceptions import UnsupportedSyntaxError, compose, md_code, md_syntax
from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.expression.formatter import format_expression
from opteryx.models import LogicalColumn, Node
from opteryx.planner.expression_traits import has_volatile_function
from opteryx.types.logical_type import (
    LogicalCategory, _NUMERIC_TYPES, _TEMPORAL_TYPES, _LARGE_OBJECT_TYPES, _STRING_TYPES,
)

# Node types that can never be materialised as a column on ONE join leg, whatever
# the relation-name arithmetic says: an aggregate is not a per-row value, a
# subquery is a plan rather than an expression, and a wildcard is not one value.
_UNHOISTABLE_NODE_TYPES = (NodeType.AGGREGATOR, NodeType.SUBQUERY, NodeType.WILDCARD)


def _is_numeric_join_coercible(left_type, right_type) -> bool:
    """Return True when join-side implicit numeric coercion is safe."""
    if left_type in (LogicalCategory.BOOLEAN, LogicalCategory.NULL) or right_type in (
        LogicalCategory.BOOLEAN,
        LogicalCategory.NULL,
    ):
        return False
    return left_type in (LogicalCategory.INTEGER, LogicalCategory.FLOAT, LogicalCategory.DECIMAL) and right_type in (
        LogicalCategory.INTEGER,
        LogicalCategory.FLOAT,
        LogicalCategory.DECIMAL,
    )


def get_mismatched_condition_column_types(
    node: Node, relaxed: bool = False, allow_numeric_join_coercion: bool = False
) -> dict:
    """
    Checks that the types of the fields involved a comparison are the same on both sides.

    Parameters:
        node: Node
            The condition node representing the condition.

    Returns:
        a dictionary describing the columns
    """
    if node.node_type in (NodeType.AND, NodeType.OR, NodeType.XOR):
        left_mismatches = get_mismatched_condition_column_types(
            node.left, relaxed, allow_numeric_join_coercion
        )
        right_mismatches = get_mismatched_condition_column_types(
            node.right, relaxed, allow_numeric_join_coercion
        )
        return left_mismatches or right_mismatches

    elif node.node_type == NodeType.COMPARISON_OPERATOR:
        if node.value in (
            "Arrow",
            "LongArrow",
            "AtQuestion",
            "AtArrow",
            # IPv4 CIDR containment is INHERENTLY mixed-type: an address operand
            # (INTEGER category) against a CIDR string. Matching operand types
            # would be the bug here, not the exemption.
            "IPContainedBy",
            "IPContains",
        ) or node.value.startswith(("AllOp", "AnyOp")):
            return None  # Some ops are meant to have different types
        left_type = node.left.schema_column.category if node.left.schema_column else None
        left_display_ct = (
            node.left.schema_column.column_type if node.left.schema_column else None
        )
        if node.value in ("InList", "NotInList"):
            # The right side is an ARRAY literal; the type that must agree with
            # the left operand is the array's ELEMENT type, not LC.ARRAY itself.
            # Without this, a mistyped IN-list (`int_col IN ('Earth')`) sails
            # through here and is only caught later — or not at all — by
            # kernel strictness deep in execution (see the architect's report:
            # rewrite_in_to_eq's single-member IN retype is the only remaining
            # guard, and a looser kernel would turn this into silent wrong rows).
            right_ct = getattr(node.right, "type", None)
            right_type = right_ct.element.category if right_ct is not None and right_ct.element is not None else None
            right_display_ct = right_ct.element if right_ct is not None else None
        else:
            right_type = node.right.schema_column.category if node.right.schema_column else None
            right_display_ct = (
                node.right.schema_column.column_type if node.right.schema_column else None
            )

        if left_type and right_type and left_type != right_type:
            if (
                allow_numeric_join_coercion
                and node.left.node_type == NodeType.IDENTIFIER
                and node.right.node_type == NodeType.IDENTIFIER
                and _is_numeric_join_coercible(left_type, right_type)
            ):
                return None
            if (
                relaxed
                and (left_type in _NUMERIC_TYPES and right_type in _NUMERIC_TYPES)
                or (left_type in _TEMPORAL_TYPES and right_type in _TEMPORAL_TYPES)
                or (left_type in _NUMERIC_TYPES and right_type in _TEMPORAL_TYPES)
                or (left_type in _TEMPORAL_TYPES and right_type in _NUMERIC_TYPES)
                or (left_type in _LARGE_OBJECT_TYPES and right_type in _LARGE_OBJECT_TYPES)
                or (left_type in _STRING_TYPES and right_type in _STRING_TYPES)
                or (left_type is None or right_type is None)
            ):
                return None
            if left_type == LogicalCategory.NULL or right_type == LogicalCategory.NULL:
                return None  # None comparisons are allowed
            if (
                node.left.node_type == NodeType.COMPARISON_OPERATOR
                or node.right.node_type == NodeType.COMPARISON_OPERATOR
                or node.left.node_type == NodeType.BINARY_OPERATOR
                or node.right.node_type == NodeType.BINARY_OPERATOR
                or node.left.node_type == NodeType.EXTRACTION_OPERATOR
                or node.right.node_type == NodeType.EXTRACTION_OPERATOR
            ):
                return None  # it's compound so don't make a decision here
            # Prefer the full ColumnType string (e.g. "IPV4") over the bare
            # LogicalCategory name (e.g. "INTEGER") — IPV4 is a UINT32 refined
            # by a logical descriptor, so its category alone under-reports the
            # type and misleads the user about what column they're looking at.
            return {
                "left_column": f"{node.left.source}.{node.left.value}",
                "left_type": str(left_display_ct) if left_display_ct is not None else left_type.name,
                "left_node": node.left,
                "right_column": f"{node.right.source}.{node.right.value}",
                "right_type": str(right_display_ct)
                if right_display_ct is not None
                else right_type.name,
                "right_node": node.right,
            }

    return None  # if we reach here, it means we didn't find any inconsistencies


def extract_join_fields(
    condition_node: Node,
    left_relation_names: List[str],
    right_relation_names: List[str],
) -> Tuple[List, List, List[Node]]:
    """
    Extracts join fields from a condition node that may have multiple ANDed conditions.

    Parameters:
        condition_node: Node
            The condition node in the join clause.
        left_relation_names: List[str]
            Names of the left relations.
        right_relation_names: List[str]
            Names of the right relations.

    Returns:
        Tuple[List, List, List[Node]]
            The identities participating in the join from the left and right
            tables, followed by the Eq conjuncts that could NOT be turned into a
            key pair because an operand is an expression rather than a column.

    That third list is a RETURN VALUE and not an exception on purpose. An
    expression operand is not a permanent rejection — `JoinKeyMaterializationStrategy`
    turns one into a real key by projecting it as a column on its own leg — so the
    callers need to tell "cannot be a key here" from "cannot be a key at all", and
    they each answer it differently: the binder rejects only what nothing can hoist,
    the pushdown strategies decline the conversion and leave a Filter in place. It
    was previously signalled by raising `UnsupportedSyntaxError`, which those
    strategies caught to steer themselves — control flow through an exception, which
    the engineering contract bans, and which cannot carry the "hoistable?" answer
    anyway.
    """
    left_fields = []
    right_fields = []
    unkeyed: List[Node] = []

    if condition_node.node_type == NodeType.AND:
        left_fields_1, right_fields_1, unkeyed_1 = extract_join_fields(
            condition_node.left, left_relation_names, right_relation_names
        )
        left_fields_2, right_fields_2, unkeyed_2 = extract_join_fields(
            condition_node.right, left_relation_names, right_relation_names
        )

        left_fields.extend(left_fields_1)
        left_fields.extend(left_fields_2)

        right_fields.extend(right_fields_1)
        right_fields.extend(right_fields_2)

        unkeyed.extend(unkeyed_1)
        unkeyed.extend(unkeyed_2)

    elif condition_node.node_type == NodeType.COMPARISON_OPERATOR and condition_node.value == "Eq":
        if any(
            [
                condition_node.left.node_type not in (NodeType.IDENTIFIER, NodeType.LITERAL),
                condition_node.right.node_type not in (NodeType.IDENTIFIER, NodeType.LITERAL),
            ]
        ):
            return left_fields, right_fields, [condition_node]
        if (
            condition_node.left.source in left_relation_names
            and condition_node.right.source in right_relation_names
        ):
            left_fields.append(condition_node.left.schema_column.identity)
            right_fields.append(condition_node.right.schema_column.identity)
        elif (
            condition_node.left.source in right_relation_names
            and condition_node.right.source in left_relation_names
        ):
            right_fields.append(condition_node.left.schema_column.identity)
            left_fields.append(condition_node.right.schema_column.identity)

    return left_fields, right_fields, unkeyed


def _identifier_leg(
    identifier: Node, left_relation_names: List[str], right_relation_names: List[str]
) -> Optional[str]:
    """Which join leg a bound IDENTIFIER belongs to, or None if neither.

    `.source` is the alias the identifier was WRITTEN with and is what
    `extract_join_fields` matches on, so it is what a hoist decision has to agree
    with. `schema_column.origin` is the fallback for an identifier that resolved
    without being qualified in the SQL.
    """
    candidates: Set[str] = set()
    if identifier.source is not None:
        candidates.add(identifier.source)
    schema_column = identifier.schema_column
    origin = getattr(schema_column, "origin", None) if schema_column is not None else None
    if origin:
        candidates.update(origin)
    if not candidates:
        return None
    if candidates <= set(left_relation_names or ()):
        return "left"
    if candidates <= set(right_relation_names or ()):
        return "right"
    return None


def hoistable_operand_leg(
    expression: Node, left_relation_names: List[str], right_relation_names: List[str]
) -> Optional[str]:
    """The join leg `expression` could be materialised on as a column, or None.

    THE ONE DEFINITION of "this ON-clause operand can become a join key". The
    Binder asks it to decide whether to reject the query; JoinKeyMaterializationStrategy
    asks it to decide what to project. They must not drift: a Binder that accepts
    what the strategy will not hoist leaks a generic "unaligned key lists" refusal
    from the compiler instead of a message naming the operand.

    None covers three different "no"s the callers do not need to tell apart:
    nothing to hoist (a bare column or literal), not a deterministic per-row
    function of one leg, or it straddles both legs — which is a theta condition,
    not a key.
    """
    if expression is None:
        return None
    if expression.node_type in (NodeType.IDENTIFIER, NodeType.LITERAL):
        return None  # already a key, or contributes none — nothing to project
    if expression.schema_column is None:
        return None  # unbound: no identity to key the materialised column by
    if get_all_nodes_of_type(expression, _UNHOISTABLE_NODE_TYPES):
        return None
    if has_volatile_function(expression):
        # Projecting it changes evaluation from once-per-pair to once-per-row.
        # Same posture as constant folding and group-key reduction: never
        # relocate an expression whose value is not a function of its inputs.
        return None

    identifiers = get_all_nodes_of_type(expression, (NodeType.IDENTIFIER,))
    if not identifiers:
        return None  # a constant expression names no leg and keys nothing

    legs = {_identifier_leg(identifier, left_relation_names, right_relation_names) for identifier in identifiers}
    if len(legs) != 1:
        return None
    return legs.pop()


def plan_join_key_hoists(
    conjunct: Node, left_relation_names: List[str], right_relation_names: List[str]
) -> Optional[List[Tuple[Node, str]]]:
    """How an Eq conjunct carrying expression operand(s) could become an equi-join key.

    Returns the (expression, leg) pairs to materialise — one entry when a single
    operand is an expression, two when both are — or None when no projection makes
    this conjunct a key. Never returns an empty list: a conjunct with nothing to
    hoist is already a key and never reaches here.

    Both operands must land on OPPOSITE legs. `CAST(p.a) = CAST(p.b)` is hoistable
    twice over and still not a join key — it is a filter on one leg — and folding it
    into the key lists would pair a leg with itself.
    """
    if conjunct.node_type != NodeType.COMPARISON_OPERATOR or conjunct.value != "Eq":
        return None
    if conjunct.left is None or conjunct.right is None:
        return None

    legs: List[str] = []
    hoists: List[Tuple[Node, str]] = []
    for operand in (conjunct.left, conjunct.right):
        if operand.node_type == NodeType.IDENTIFIER:
            leg = _identifier_leg(operand, left_relation_names, right_relation_names)
            if leg is None:
                return None
            legs.append(leg)
            continue
        leg = hoistable_operand_leg(operand, left_relation_names, right_relation_names)
        if leg is None:
            return None
        legs.append(leg)
        hoists.append((operand, leg))

    if set(legs) != {"left", "right"} or not hoists:
        return None
    return hoists


def reject_unhoistable_join_operands(
    unkeyed: List[Node], left_relation_names: List[str], right_relation_names: List[str]
) -> None:
    """Raise for any Eq conjunct in `unkeyed` that no projection can turn into a key.

    Conjuncts that CAN be hoisted are left alone — JoinKeyMaterializationStrategy
    projects them onto their leg and rebuilds the key lists. This is the Binder's
    half of that split, and it is where the message lands because the Binder is the
    last phase that still sits next to the user's SQL.
    """
    for conjunct in unkeyed:
        if plan_join_key_hoists(conjunct, left_relation_names, right_relation_names) is not None:
            continue

        offenders = [
            operand
            for operand in (conjunct.left, conjunct.right)
            if operand is not None
            and operand.node_type not in (NodeType.IDENTIFIER, NodeType.LITERAL)
        ]

        # Report the reason this operand is not a key, not the generic one. The
        # four are genuinely different problems with four different fixes, and a
        # message that names the wrong one sends the user to rewrite the part of
        # their query that was fine.
        for operand in offenders:
            rendered = md_code(format_expression(operand))
            if get_all_nodes_of_type(operand, _UNHOISTABLE_NODE_TYPES):
                raise UnsupportedSyntaxError(
                    compose(
                        f"A {md_syntax('JOIN')} condition joins values row by row, and "
                        f"{rendered} has no per-row value",
                        f"Compute it in a subquery or {md_syntax('CTE')} and join on the "
                        f"resulting column",
                    )
                )
            if has_volatile_function(operand):
                raise UnsupportedSyntaxError(
                    compose(
                        f"A {md_syntax('JOIN')} condition needs a key that is the same "
                        f"every time it is read, and {rendered} is not",
                        "Materialise the value before the join if you need to join on it",
                    )
                )

        # Every operand could be computed on SOME leg, so what is wrong is where
        # they land: either an operand draws on both relations, or both operands
        # draw on the same one. Neither is an equi-join key, and neither is fixed
        # by projecting it — the condition belongs in WHERE.
        straddling = [
            md_code(format_expression(operand))
            for operand in offenders
            if hoistable_operand_leg(operand, left_relation_names, right_relation_names) is None
        ]
        if straddling:
            raise UnsupportedSyntaxError(
                compose(
                    f"A {md_syntax('JOIN')} key is computed from one side of the join, "
                    f"and {' and '.join(straddling)} draws on both",
                    f"A condition spanning both relations goes in {md_syntax('WHERE')}, "
                    f"not {md_syntax('ON')}",
                )
            )
        raise UnsupportedSyntaxError(
            compose(
                f"A {md_syntax('JOIN')} matches one relation against the other, and both "
                f"sides of "
                f"{md_code(format_expression(conjunct))} come from the same relation",
                f"A condition over a single relation goes in {md_syntax('WHERE')}, not "
                f"{md_syntax('ON')}",
            )
        )


def convert_using_to_on(
    using_fields: Set[str],
    left_relation_names: List[str],
    right_relation_names: List[str],
) -> Node:
    """
    Converts a USING field to an ON field for JOIN operations.

    Parameters:
        using_fields: Set[str]
            Set of common fields to use for joining.
        left_relation_names: List[str]
            Names of the left relations.
        right_relation_names: List[str]
            Names of the right relations.

    Returns:
        Node
            The condition node representing the ON clause.
    """
    all_conditions = []

    # Loop through all combinations of left and right relation names
    for left_relation_name in left_relation_names:
        for right_relation_name in right_relation_names:
            conditions = []
            for field in using_fields:
                condition = Node(
                    node_type=NodeType.COMPARISON_OPERATOR,
                    value="Eq",
                    do_not_create_column=True,
                )
                condition.left = LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source=left_relation_name,
                    source_column=field,
                )
                condition.right = LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source=right_relation_name,
                    source_column=field,
                )
                conditions.append(condition)

            if len(conditions) == 1:
                all_conditions.append(conditions[0])
            else:
                # Create a tree of ANDed conditions
                while len(conditions) > 1:
                    new_conditions = []
                    for i in range(0, len(conditions), 2):
                        if i + 1 < len(conditions):
                            and_node = Node(node_type=NodeType.AND, do_not_create_column=True)
                            and_node.left = conditions[i]
                            and_node.right = conditions[i + 1]
                            new_conditions.append(and_node)
                        else:
                            new_conditions.append(conditions[i])
                    conditions = new_conditions
                all_conditions.append(conditions[0])

    return conditions[0]
