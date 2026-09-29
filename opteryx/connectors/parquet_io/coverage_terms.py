# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The EXACT translation of a scan's pushed predicate into coverage terms
(docs/MANIFEST_SUM_STATISTIC_DESIGN.md §6, P3).

Row-group pruning (`predicates.extract_predicate_stats`, skene zone terms) only
ever has to prove "nothing in this row group matches", so it may DROP a
condition it cannot express or WIDEN one - either only keeps more row groups.
Coverage proves the opposite, "EVERY row in this row group matches", and a
dropped or widened condition would make that claim false. So this translation
is all-or-nothing: every conjunct becomes one exact term, or the whole predicate
is refused (None) and no row group is ever covered - pruning is unaffected.

A term is ``(column, op, ordinals)``: the column's PHYSICAL name, an op code
from `stats_coverage.hpp`, and the literal(s) in draken's ordinal space. Only
columns whose ordinal IS the value are admitted - the signed integers, UINT8/
16/32 and DATE32 (days) - and only literals of exactly that domain: an integer
literal for an integer column, a DATE literal for a DATE column. Everything
else (floats and their NaN, strings and their prefix ordinals, UINT64 and its
sign-biased ordinal, TIMESTAMP and its units, DECIMAL and its scale) is refused.
"""

import datetime
from typing import List, Optional, Tuple

# Op codes — the SAME numbering as src/cpp/engine/stats_coverage.hpp CoverageOp.
EQ, NOT_EQ, LT, LT_EQ, GT, GT_EQ, IN_LIST, IS_NULL, IS_NOT_NULL = range(9)

_COMPARISONS = {"Eq": EQ, "NotEq": NOT_EQ, "Lt": LT, "LtEq": LT_EQ, "Gt": GT, "GtEq": GT_EQ}
# The same comparison with its operands swapped: `5 < x` is `x > 5`.
_FLIPPED = {EQ: EQ, NOT_EQ: NOT_EQ, LT: GT, LT_EQ: GT_EQ, GT: LT, GT_EQ: LT_EQ}

_EPOCH = datetime.date(1970, 1, 1)

Term = Tuple[str, int, List[int]]


def _ordinal_domains():
    from opteryx.types.logical_type import DrakenType

    integers = frozenset(
        (
            DrakenType.INT8, DrakenType.INT16, DrakenType.INT32, DrakenType.INT64,
            DrakenType.UINT8, DrakenType.UINT16, DrakenType.UINT32,
        )
    )
    return integers, DrakenType.DATE32


def _column(node) -> Optional[Tuple[str, str]]:
    """(physical name, domain) of an IDENTIFIER whose ordinal is its value, or None.
    Domain is "int" or "date"."""
    from opteryx.expression import NodeType

    if node is None or node.node_type != NodeType.IDENTIFIER:
        return None
    column = node.schema_column
    if column is None or not column.name:
        return None
    column_type = column.column_type
    if column_type is None or column_type.logical is not None:
        return None
    integers, date32 = _ordinal_domains()
    if column_type.physical in integers:
        return column.name, "int"
    if column_type.physical == date32:
        return column.name, "date"
    return None


def _ordinal(domain: str, value, literal_type) -> Optional[int]:
    """The literal `value` in the column domain's ordinal space, or None when it
    is not EXACTLY a value of that domain. `literal_type` is the literal's own
    ColumnType: the binder stores native-typed literals, so a DATE literal can
    arrive as its int day count - an int is only a date when its type says so."""
    integers, date32 = _ordinal_domains()
    physical = literal_type.physical if literal_type is not None else None
    logical = literal_type.logical if literal_type is not None else None
    if domain == "int":
        # bool is an int subclass and a different type; floats never admitted -
        # `x < 2.5` is not an integer bound.
        if type(value) is not int or physical not in integers or logical is not None:
            return None
        if value < -(2**63) or value >= 2**63:
            return None
        return value
    if domain == "date":
        if type(value) is datetime.date:
            return (value - _EPOCH).days
        if type(value) is int and physical == date32 and logical is None:
            return value
        return None
    return None


def _literal_value(node):
    """(value, the literal's element ColumnType, is a literal)."""
    from opteryx.expression import NodeType

    if node is None or node.node_type != NodeType.LITERAL:
        return None, None, False
    literal_type = node.type
    # an IN list is typed as the ARRAY of its members
    if literal_type is not None and literal_type.element is not None:
        literal_type = literal_type.element
    return node.value, literal_type, True


def _term(node) -> Optional[Term]:
    from opteryx.expression import NodeType

    if node is None:
        return None
    if node.node_type == NodeType.UNARY_OPERATOR and node.value in ("IsNull", "IsNotNull"):
        column = _column(node.centre)
        if column is None:
            return None
        return column[0], IS_NULL if node.value == "IsNull" else IS_NOT_NULL, []

    if node.node_type == NodeType.BETWEEN:
        # BETWEEN is two conjuncts; this function returns one term, so it is
        # expanded by the caller. Refused here.
        return None

    if node.node_type != NodeType.COMPARISON_OPERATOR:
        return None

    if node.value == "InList":
        column = _column(node.left)
        values, literal_type, is_literal = _literal_value(node.right)
        if column is None or not is_literal or type(values) not in (list, tuple):
            return None
        ordinals = []
        for value in values:
            ordinal = _ordinal(column[1], value, literal_type)
            if ordinal is None:
                return None   # one unexpressible member makes the list unprovable
            ordinals.append(ordinal)
        if not ordinals:
            return None
        return column[0], IN_LIST, sorted(set(ordinals))

    op = _COMPARISONS.get(node.value)
    if op is None:
        return None
    column = _column(node.left)
    value, literal_type, is_literal = _literal_value(node.right)
    if column is None:
        column = _column(node.right)
        value, literal_type, is_literal = _literal_value(node.left)
        op = _FLIPPED[op]
    if column is None or not is_literal:
        return None
    ordinal = _ordinal(column[1], value, literal_type)
    if ordinal is None:
        return None
    return column[0], op, [ordinal]


def _between_terms(node) -> Optional[List[Term]]:
    from opteryx.expression import NodeType

    if node is None or node.node_type != NodeType.BETWEEN:
        return None
    column = _column(node.left)
    low, low_type, low_literal = _literal_value(node.right)
    high, high_type, high_literal = _literal_value(node.centre)
    if column is None or not low_literal or not high_literal:
        return None
    low_ordinal = _ordinal(column[1], low, low_type)
    high_ordinal = _ordinal(column[1], high, high_type)
    if low_ordinal is None or high_ordinal is None:
        return None
    lower_inclusive, upper_inclusive = node.value
    return [
        (column[0], GT_EQ if lower_inclusive else GT, [low_ordinal]),
        (column[0], LT_EQ if upper_inclusive else LT, [high_ordinal]),
    ]


def extract_coverage_terms(conditions) -> Optional[List[Term]]:
    """Every pushed conjunct as an exact coverage term, or None when ANY conjunct
    cannot be expressed exactly (then nothing can be proven covered). An empty
    predicate is an empty term list: every row group is covered."""
    terms: List[Term] = []
    for node in conditions or ():
        between = _between_terms(node)
        if between is not None:
            terms.extend(between)
            continue
        term = _term(node)
        if term is None:
            return None
        terms.append(term)
    return terms
