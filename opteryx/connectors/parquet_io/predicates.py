# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Parquet row-group predicate evaluation — Phase 1: min/max statistics pruning.

Uses the column-level min/max values recorded in the Parquet footer to decide
whether a row group can be skipped entirely *before* issuing any I/O for its
column chunks.

Design contract
---------------
- **Fail open**: if statistics are absent, the row group is kept.
- **No false negatives**: a row group is only pruned when it is *impossible*
  for any value in [col_min, col_max] to satisfy the predicate.  In the
  presence of null values this is still safe because null comparisons are
  false in SQL, so a row that would have matched is never a null row.
- Type errors or unsupported operators silently suppress pruning for that
  predicate — correctness over performance.
- Only column-op-literal comparisons (AND-combined) are handled.  OR clauses
  and non-literal expressions are passed through untouched (i.e. row group is
  not pruned for them).
"""

from __future__ import annotations

import datetime
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple

# Supported comparison operators (as used in NodeType.COMPARISON_OPERATOR nodes).
_PRUNABLE_OPS = frozenset({"Eq", "NotEq", "Gt", "GtEq", "Lt", "LtEq"})

_INVERT_OP = {
    "Gt": "Lt",
    "GtEq": "LtEq",
    "Lt": "Gt",
    "LtEq": "GtEq",
    "Eq": "Eq",
    "NotEq": "NotEq",
}


def extract_predicate_stats(conditions) -> List[Tuple[str, str, Any]]:
    """Convert pushed-down condition Nodes to ``(col_name, op, value)`` triples.

    Only simple ``identifier op literal`` comparisons are extracted.
    Unsupported predicates are silently dropped — the caller must *not*
    assume the returned list covers every condition.

    Args:
        conditions: List of ``Node`` objects from ``ParquetReadNode.predicates``.

    Returns:
        ``[(column_name, operator, literal_value), ...]`` where *column_name*
        matches the physical Parquet column name (not the identity alias).
    """
    if not conditions:
        return []
    result = []
    for node in conditions:
        between_stats = _try_extract_between(node)
        if between_stats:
            result.extend(between_stats)
            continue
        stat = _try_extract(node)
        if stat is not None:
            result.append(stat)
    return result


def _try_extract_between(node) -> List[Tuple[str, str, Any]]:
    """Decompose a BETWEEN node into two GtEq/LtEq triples for row-group pruning."""
    if node is None:
        return []

    from opteryx.expression import NodeType

    if node.node_type != NodeType.BETWEEN:
        return []
    if node.left is None or node.right is None or node.centre is None:
        return []
    if node.left.node_type != NodeType.IDENTIFIER:
        return []
    if node.right.node_type != NodeType.LITERAL or node.centre.node_type != NodeType.LITERAL:
        return []

    col_sc = getattr(node.left, "schema_column", None)
    if col_sc is None:
        return []
    col_name = getattr(col_sc, "name", None)
    if not col_name:
        return []

    lower_inclusive, upper_inclusive = node.value  # (bool, bool)
    lower_op = "GtEq" if lower_inclusive else "Gt"
    upper_op = "LtEq" if upper_inclusive else "Lt"

    lower_val = node.right.value
    upper_val = node.centre.value
    if isinstance(lower_val, datetime.date) and not isinstance(lower_val, datetime.datetime):
        lower_val = datetime.datetime.combine(lower_val, datetime.time.min)
    if isinstance(upper_val, datetime.date) and not isinstance(upper_val, datetime.datetime):
        upper_val = datetime.datetime.combine(upper_val, datetime.time.min)

    return [(col_name, lower_op, lower_val), (col_name, upper_op, upper_val)]


def _try_extract(node) -> Optional[Tuple[str, str, Any]]:
    """Return ``(col_name, op, value)`` for a simple comparison Node, or None."""
    if node is None:
        return None

    # Import here to avoid circular imports at module load time.
    from opteryx.expression import NodeType

    if node.node_type != NodeType.COMPARISON_OPERATOR:
        return None

    op = node.value
    if op not in _PRUNABLE_OPS:
        return None

    left, right = node.left, node.right
    if left is None or right is None:
        return None

    # Normalise so the identifier is always on the left.
    if left.node_type == NodeType.IDENTIFIER and right.node_type == NodeType.LITERAL:
        pass  # already normalised
    elif right.node_type == NodeType.IDENTIFIER and left.node_type == NodeType.LITERAL:
        left, right = right, left
        op = _INVERT_OP.get(op, op)
    else:
        return None  # not a col-op-literal — skip

    col_sc = getattr(left, "schema_column", None)
    if col_sc is None:
        return None
    col_name = getattr(col_sc, "name", None)
    if not col_name:
        return None

    value = right.value

    # Normalise datetime.date to datetime.datetime so comparisons with rugo's
    # microsecond-epoch timestamps don't blow up on mixed-type ordering.
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        value = datetime.datetime.combine(value, datetime.time.min)

    return (col_name, op, value)


def _can_prune_rowgroup(op: str, value: Any, col_min: Any, col_max: Any) -> bool:
    """Return True when the predicate *definitely* has no matching rows in [col_min, col_max].

    Pruning semantics (rg values in closed interval [col_min, col_max]):

    +--------+-----------------------------------------+
    | op     | prune when                              |
    +========+=========================================+
    | Gt     | col_max <= value  (nothing > value)     |
    | GtEq   | col_max <  value  (nothing >= value)    |
    | Lt     | col_min >= value  (nothing < value)     |
    | LtEq   | col_min >  value  (nothing <= value)    |
    | Eq     | value < col_min or value > col_max      |
    | NotEq  | col_min == col_max == value (all equal) |
    +--------+-----------------------------------------+
    """
    if col_min is None or col_max is None:
        return False
    try:
        if op == "Eq":
            return value < col_min or value > col_max
        if op == "NotEq":
            return col_min == col_max == value
        if op == "Gt":
            return col_max <= value
        if op == "GtEq":
            return col_max < value
        if op == "Lt":
            return col_min >= value
        if op == "LtEq":
            return col_min > value
    except TypeError:
        # Incomparable types — don't prune.
        pass
    return False


def row_group_may_satisfy(rg_meta: dict, predicates: List[Tuple[str, str, Any]]) -> bool:
    """Return True if the row group *may* contain rows that satisfy all predicates.

    Returns False (prune) when any single AND predicate can be definitively
    disproved by the column's min/max statistics.

    Args:
        rg_meta: Row group metadata dict as returned by rugo (contains
                 ``"columns"`` list, each with ``"name"``, ``"min"``, ``"max"``).
        predicates: ``[(col_name, op, value), ...]`` from
                    :func:`extract_predicate_stats`.

    Returns:
        ``False`` if the row group can be skipped, ``True`` if it must be read.
    """
    if not predicates:
        return True

    # Build col_name → stats for O(1) lookup.
    col_by_name = {col["name"]: col for col in rg_meta.get("columns", [])}

    for col_name, op, value in predicates:
        col_stats = col_by_name.get(col_name)
        if col_stats is None:
            continue  # column not present in this file — fail open
        if _can_prune_rowgroup(op, value, col_stats.get("min"), col_stats.get("max")):
            return False  # pruned

    return True
