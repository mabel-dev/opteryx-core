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
- Column-op-literal comparisons, ``BETWEEN`` and ``IN (literal, ...)`` (all
  AND-combined) are handled.  OR clauses and non-literal expressions are passed
  through untouched (i.e. row group is not pruned for them).
"""

from __future__ import annotations

import datetime
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple

# Supported comparison operators (as used in NodeType.COMPARISON_OPERATOR nodes).
_PRUNABLE_OPS = frozenset({"Eq", "NotEq", "Gt", "GtEq", "Lt", "LtEq"})

# Physical types whose stored representation is a bare temporal integer whose
# meaning depends on a domain: DATE32 stores days, TIMESTAMP64 stores unit-scaled
# ticks (s/ms/us/ns), TIME32/TIME64 store time-of-day ticks. Row-group pruning
# compares these raw integers directly, so two temporal operands are only
# order-comparable when they share both the physical type and (where applicable)
# the unit.
_TEMPORAL_PHYSICALS = None  # lazily populated from DrakenType on first use


def _temporal_domain_mismatch(col_node, literal_node) -> bool:
    """True when *col* and *literal* are both temporal but occupy different raw
    domains — DATE32 (days) vs TIMESTAMP64 (microseconds), or two TIMESTAMP64s
    with different units.

    Row-group pruning compares a column's raw decoded min/max against the
    literal's raw materialised value. Those integers are only order-comparable
    within one domain: DATE32 stores days, TIMESTAMP64[us] stores microseconds,
    etc. A cross-domain raw compare silently prunes the wrong row groups (a bare
    DATE column vs a ``::TIMESTAMP`` literal pruned every row group -> 0 rows).
    When the domains differ we decline the pushdown; the residual native filter
    (``draken_temporal_cmp``, which promotes both sides to nanoseconds) then
    produces the correct answer.
    """
    global _TEMPORAL_PHYSICALS
    if _TEMPORAL_PHYSICALS is None:
        from opteryx.types.logical_type import DrakenType

        _TEMPORAL_PHYSICALS = frozenset(
            {
                DrakenType.DATE32,
                DrakenType.TIMESTAMP64,
                DrakenType.TIME32,
                DrakenType.TIME64,
            }
        )

    col_sc = getattr(col_node, "schema_column", None)
    col_ct = getattr(col_sc, "column_type", None)
    lit_ct = getattr(literal_node, "type", None)
    if col_ct is None or lit_ct is None:
        return False

    col_phys = col_ct.physical
    lit_phys = lit_ct.physical
    if col_phys not in _TEMPORAL_PHYSICALS or lit_phys not in _TEMPORAL_PHYSICALS:
        return False  # not both temporal — not a temporal-domain question
    if col_phys != lit_phys:
        return True  # e.g. DATE32 (days) vs TIMESTAMP64 (ticks)

    # Same physical type: only the unit can still differ (TIMESTAMP64/TIME*).
    col_logical = col_ct.logical
    lit_logical = lit_ct.logical
    col_unit = col_logical.unit if col_logical is not None else None
    lit_unit = lit_logical.unit if lit_logical is not None else None
    return col_unit != lit_unit

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
        in_stat = _try_extract_in(node)
        if in_stat is not None:
            result.append(in_stat)
            continue
        func_stat = _try_extract_str_func(node)
        if func_stat is not None:
            result.append(func_stat)
            continue
        stat = _try_extract(node)
        if stat is not None:
            result.append(stat)
    return result


def _try_extract_str_func(node) -> Optional[Tuple[str, str, Any]]:
    """Return ``(col_name, "_STARTS_WITH"/"_ENDS_WITH"/"InStr", pattern_bytes)`` for
    a LIKE-rewritten string-match FUNCTION node, or None.

    These are the case-sensitive prefix/suffix/substring predicates the optimizer
    produces from ``col LIKE 'p%'`` / ``'%s'`` / ``'%x%'``. They are pure
    per-value predicates, so the dictionary decode-skip can evaluate them against
    the unique values. (Case-insensitive ``_CI_*`` / ``IInStr`` variants need case
    folding and are intentionally not extracted here.) Used only by the dict
    decode-skip; ``_rg_passes_predicates_native`` ignores these op tags (fail-open).
    """
    if node is None:
        return None

    from opteryx.expression import NodeType

    if node.node_type != NodeType.FUNCTION:
        return None
    if node.value not in ("_STARTS_WITH", "_ENDS_WITH", "InStr"):
        return None
    params = getattr(node, "parameters", None)
    if not params or len(params) != 2:
        return None
    col_node, pat_node = params[0], params[1]
    if col_node.node_type != NodeType.IDENTIFIER or pat_node.node_type != NodeType.LITERAL:
        return None
    col_sc = getattr(col_node, "schema_column", None)
    if col_sc is None:
        return None
    col_name = getattr(col_sc, "name", None)
    if not col_name:
        return None
    return (col_name, node.value, pat_node.value)


def _try_extract_in(node) -> Optional[Tuple[str, str, Any]]:
    """Return ``(col_name, "InList"/"NotInList", [values])`` for an IN node, or None.

    Only ``identifier IN (literal, literal, ...)`` is extracted: the right-hand
    side must be a single LITERAL whose value is a concrete list/tuple/set of
    scalars. Anything else (subquery IN, expression IN) is skipped.
    """
    if node is None:
        return None

    from opteryx.expression import NodeType

    if node.node_type != NodeType.COMPARISON_OPERATOR:
        return None
    op = node.value
    if op not in ("InList", "NotInList"):
        return None

    left, right = node.left, node.right
    if left is None or right is None:
        return None
    if left.node_type != NodeType.IDENTIFIER or right.node_type != NodeType.LITERAL:
        return None

    col_sc = getattr(left, "schema_column", None)
    if col_sc is None:
        return None
    col_name = getattr(col_sc, "name", None)
    if not col_name:
        return None

    # Decline a cross-domain temporal IN list (e.g. DATE column IN (::TIMESTAMP,
    # ...)) — the raw min/max compare would be domain-blind; residual filter runs.
    if _temporal_domain_mismatch(left, right):
        return None

    values = right.value
    if not isinstance(values, (list, tuple, set)):
        return None

    normalized = []
    for value in values:
        if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
            value = datetime.datetime.combine(value, datetime.time.min)
        normalized.append(value)

    return (col_name, op, normalized)


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

    # Decline the whole BETWEEN if either bound is a cross-domain temporal literal
    # (e.g. DATE column BETWEEN two ``::TIMESTAMP`` bounds) — same reasoning as
    # `_try_extract`; the residual native filter still evaluates it correctly.
    if _temporal_domain_mismatch(node.left, node.right) or _temporal_domain_mismatch(
        node.left, node.centre
    ):
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

    # A DATE column vs a ``::TIMESTAMP`` literal (or unit-mismatched temporals)
    # cannot be pruned by a raw min/max compare — decline and let the residual
    # native filter handle it (see `_temporal_domain_mismatch`).
    if _temporal_domain_mismatch(left, right):
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
    | InList | no list value in [col_min, col_max]     |
    | NotInList | col_min == col_max and it is excluded|
    +--------+-----------------------------------------+

    For ``InList``/``NotInList`` ``value`` is the list of candidate literals.
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
        if op == "InList":
            # Prune only when *no* candidate value can fall in [col_min, col_max].
            # An empty list matches nothing, so any() is False -> prune.
            return not any(col_min <= v <= col_max for v in value)
        if op == "NotInList":
            # Safe to prune only when the whole group is a single value that the
            # exclusion list removes (mirrors NotEq).
            return col_min == col_max and col_min in value
    except TypeError:
        # Incomparable types (incl. a NULL in the IN list) — don't prune.
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
