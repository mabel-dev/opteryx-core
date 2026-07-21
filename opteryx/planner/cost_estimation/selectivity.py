# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Predicate selectivity estimation.

Pure functions that take a parsed predicate Node and a RelationStatistics
and return a selectivity in [0, 1]. No plan walking, no manifest access.

Tier ordering for each predicate kind:
  1. Histogram (distogram) when available.
  2. NDV-derived bound when distinct_count is known.
  3. Textbook constant fallback.

Mirrors the selectivity logic that previously lived on Manifest. Manifest
now delegates here via a thin shim.
"""

from typing import Optional

from opteryx.expression import NodeType
from opteryx.planner.optimizer.statistics import RelationStatistics
from opteryx.third_party.maki_nage.distogram import count_up_to


_SWAPPED_OP = {
    "Lt": "Gt",
    "LtEq": "GtEq",
    "Gt": "Lt",
    "GtEq": "LtEq",
    "Eq": "Eq",
    "NotEq": "NotEq",
}


def estimate_selectivity(predicate, stats: RelationStatistics) -> float:
    """Estimate the fraction of rows in ``stats`` matching ``predicate``.

    Returns a value in [0.0, 1.0]. Never raises on missing stats; degrades
    to lower-information tiers and finally to a constant.
    """
    return _clamp01(_selectivity(predicate, stats))


# ---- top-level dispatcher ---------------------------------------------------


def _selectivity(node, stats: RelationStatistics) -> float:
    if node is None:
        return 1.0
    nt = getattr(node, "node_type", None)

    if nt == NodeType.AND:
        return _selectivity(node.left, stats) * _selectivity(node.right, stats)
    if nt == NodeType.OR:
        s1 = _selectivity(node.left, stats)
        s2 = _selectivity(node.right, stats)
        return 1.0 - (1.0 - s1) * (1.0 - s2)
    if nt == NodeType.NOT:
        return 1.0 - _selectivity(node.centre, stats)

    if nt == NodeType.UNARY_OPERATOR:
        op = node.value
        col_name = _identifier_name(node.centre)
        if col_name is None:
            return 1.0
        if op == "IsNull":
            return _selectivity_is_null(col_name, stats)
        if op == "IsNotNull":
            return 1.0 - _selectivity_is_null(col_name, stats)
        return 1.0

    if nt == NodeType.BETWEEN:
        return _selectivity_between(node, stats)

    if nt == NodeType.COMPARISON_OPERATOR:
        return _selectivity_comparison(node, stats)

    return 1.0


# ---- per-predicate-kind helpers --------------------------------------------


def _selectivity_comparison(node, stats: RelationStatistics) -> float:
    op = node.value
    left, right = node.left, node.right

    col_name = _identifier_name(left)
    literal_node = right
    if col_name is None:
        col_name = _identifier_name(right)
        literal_node = left
        op = _SWAPPED_OP.get(op, op)
    if col_name is None:
        return 1.0
    if literal_node is None or literal_node.node_type != NodeType.LITERAL:
        return 1.0

    literal_value = _literal_scalar(literal_node)

    if op == "Eq":
        return _selectivity_eq(col_name, literal_value, stats)
    if op == "NotEq":
        return 1.0 - _selectivity_eq(col_name, literal_value, stats)
    if op in ("Lt", "LtEq", "Gt", "GtEq"):
        return _selectivity_range(col_name, op, literal_value, stats)
    if op == "InList":
        return _selectivity_in(col_name, literal_value, stats)
    if op == "NotInList":
        return 1.0 - _selectivity_in(col_name, literal_value, stats)
    if op in ("Like", "ILike", "RLike"):
        return _selectivity_like(literal_value)
    if op in ("NotLike", "NotILike", "NotRLike"):
        return 1.0 - _selectivity_like(literal_value)
    if op in ("InStr", "IInStr"):
        # predicate_rewriter.INSTR_REWRITES only ever produces these from
        # "x LIKE '%pattern%'" / "x ILIKE '%pattern%'" (wildcards stripped from
        # the literal on rewrite) -- same infix-substring semantics as
        # _selectivity_like's fallback. Without this, InStr/IInStr fell
        # through to the "unknown predicate -> assume everything matches"
        # default (1.0), understating how selective these actually are for any
        # caller (e.g. a two-pass scan eligibility check) trying to estimate
        # whether a LIKE '%x%' predicate is worth pruning on.
        return _LIKE_INFIX_SELECTIVITY
    if op in ("NotInStr", "NotIInStr"):
        return 1.0 - _LIKE_INFIX_SELECTIVITY
    return 1.0


def _selectivity_eq(col_name: str, literal_value, stats: RelationStatistics) -> float:
    col = stats.columns.get(col_name)
    dgram = col.histogram if col is not None else None
    lit_f = _to_float(literal_value)

    if dgram is not None and lit_f is not None:
        total = float(dgram.count())
        if total > 0:
            bins_len = dgram.bin_count
            if bins_len > 0:
                span = dgram.max - dgram.min
                if span > 0 and bins_len > 1:
                    bin_width = span / bins_len
                    below = _count_up_to(dgram, lit_f - bin_width / 2.0)
                    above = _count_up_to(dgram, lit_f + bin_width / 2.0)
                    density = (above - below) / total
                    ndv = col.distinct_count if col is not None else None
                    if ndv and ndv > 0:
                        density = min(density, max(1.0 / ndv, density / max(ndv, 1)))
                    return _clamp01(density)
                if span == 0:
                    return 1.0 if lit_f == dgram.min else 0.0

    ndv = col.distinct_count if col is not None else None
    if ndv and ndv > 0:
        return 1.0 / ndv
    return 0.1


def _selectivity_range(
    col_name: str, op: str, literal_value, stats: RelationStatistics
) -> float:
    col = stats.columns.get(col_name)
    dgram = col.histogram if col is not None else None
    lit_f = _to_float(literal_value)

    if dgram is not None and lit_f is not None:
        total = float(dgram.count())
        if total > 0:
            below = _count_up_to(dgram, lit_f)
            fraction_below = below / total
            if op in ("Lt", "LtEq"):
                return _clamp01(fraction_below)
            return _clamp01(1.0 - fraction_below)
    return 0.25


def _selectivity_in(col_name: str, literal_value, stats: RelationStatistics) -> float:
    if not isinstance(literal_value, (list, tuple, set, frozenset)):
        return 0.1
    values = list(literal_value)
    n = len(values)
    if n == 0:
        return 0.0

    col = stats.columns.get(col_name)
    dgram = col.histogram if col is not None else None
    if dgram is not None:
        total = float(dgram.count())
        if total > 0 and dgram.bin_count > 0:
            span = dgram.max - dgram.min
            if span > 0:
                bin_width = span / dgram.bin_count
                accumulated = 0.0
                coerced_any = False
                for v in values:
                    f = _to_float(v)
                    if f is None:
                        continue
                    coerced_any = True
                    below = _count_up_to(dgram, f - bin_width / 2.0)
                    above = _count_up_to(dgram, f + bin_width / 2.0)
                    accumulated += (above - below) / total
                if coerced_any:
                    return _clamp01(accumulated)

    ndv = col.distinct_count if col is not None else None
    if ndv and ndv > 0:
        return min(1.0, n / ndv)
    return min(1.0, n * 0.1)


def _selectivity_between(node, stats: RelationStatistics) -> float:
    col_name = _identifier_name(node.left)
    if col_name is None:
        return 1.0
    right = node.right
    centre = node.centre
    if right is None or centre is None:
        return 1.0
    if right.node_type != NodeType.LITERAL or centre.node_type != NodeType.LITERAL:
        return 1.0

    a = _to_float(_literal_scalar(right))
    b = _to_float(_literal_scalar(centre))

    col = stats.columns.get(col_name)
    dgram = col.histogram if col is not None else None
    if dgram is not None and a is not None and b is not None:
        total = float(dgram.count())
        if total > 0:
            lo, hi = (a, b) if a <= b else (b, a)
            fraction = (_count_up_to(dgram, hi) - _count_up_to(dgram, lo)) / total
            return _clamp01(fraction)
    return 0.25


def _selectivity_is_null(col_name: str, stats: RelationStatistics) -> float:
    col = stats.columns.get(col_name)
    nf = col.null_fraction if col is not None else None
    if nf is None:
        return 0.05
    return _clamp01(nf)


# ---- generic helpers --------------------------------------------------------


def _clamp01(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return float(value)


def _identifier_name(node) -> Optional[str]:
    if node is None or getattr(node, "node_type", None) != NodeType.IDENTIFIER:
        return None
    name = getattr(node, "source_column", None)
    if name is None:
        name = getattr(node, "value", None)
    if isinstance(name, bytes):
        try:
            name = name.decode("utf-8")
        except UnicodeDecodeError:
            return None
    return name if isinstance(name, str) else None


def _literal_scalar(node):
    value = getattr(node, "value", None)
    if getattr(value, "item", None) is not None and not isinstance(value, (list, tuple, set, frozenset)):
        try:
            return value.item()
        except (ValueError, TypeError):
            return value
    return value


def _to_float(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _count_up_to(dgram, value: float) -> float:
    return count_up_to(dgram, value)


# Textbook constant fallbacks for LIKE-family predicates the estimator has no
# real content stats for. "Prefix" = pattern like 'foo%' (still bounds a range,
# a bit more selective); "infix" = pattern like '%foo%' or unrecognized shapes
# (no positional anchor at all, least selective). Named so InStr/IInStr (the
# rewritten form of an infix LIKE -- see predicate_rewriter.INSTR_REWRITES)
# can reuse the infix constant directly instead of re-deriving one.
_LIKE_PREFIX_SELECTIVITY = 0.25
_LIKE_INFIX_SELECTIVITY = 0.1


def _selectivity_like(literal_value) -> float:
    if isinstance(literal_value, bytes):
        try:
            literal_value = literal_value.decode("utf-8")
        except UnicodeDecodeError:
            return _LIKE_INFIX_SELECTIVITY
    if not isinstance(literal_value, str):
        return _LIKE_INFIX_SELECTIVITY
    if (
        literal_value.endswith("%")
        and "%" not in literal_value[:-1]
        and "_" not in literal_value
    ):
        return _LIKE_PREFIX_SELECTIVITY
    return _LIKE_INFIX_SELECTIVITY
