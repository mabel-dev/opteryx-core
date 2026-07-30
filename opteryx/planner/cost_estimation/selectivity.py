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

Comparisons also cover column-vs-column (`a.x = b.y`, no literal on either
side): most of these are extracted as equi-join keys before they ever reach
here, but same-relation comparisons (`a.start < a.end`) and residual
non-equi/implicit-join predicates are not. Eq/NotEq get the same NDV formula
as equi-join selectivity; range comparisons fall straight to tier 3 -- two
single-column stats say nothing about the correlation between the columns.

Mirrors the selectivity logic that previously lived on Manifest. Manifest
now delegates here via a thin shim.
"""

import math
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
        identity = _identifier_identity(node.centre)
        if identity is None:
            return 1.0
        if op == "IsNull":
            return _selectivity_is_null(identity, stats)
        if op == "IsNotNull":
            return 1.0 - _selectivity_is_null(identity, stats)
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

    left_identity = _identifier_identity(left)
    right_identity = _identifier_identity(right)

    if left_identity is not None and right_identity is not None:
        # Column-vs-column, no literal on either side. Reachable when a
        # comparison isn't extracted as an equi-join key (predicate_pushdown
        # only pulls Eq comparisons into left_columns/right_columns): a
        # same-relation comparison like `a.start_date < a.end_date`, or a
        # residual cross-relation predicate from a non-equi/implicit join.
        # Once two relations are joined, `stats.columns` is the join's merged
        # dict (see _merge_columns in statistics_refresh), so both identities
        # resolve from the same lookup regardless of which side each came from.
        return _selectivity_col_vs_col(op, left_identity, right_identity, stats)

    identity = left_identity
    literal_node = right
    if identity is None:
        identity = right_identity
        literal_node = left
        op = _SWAPPED_OP.get(op, op)
    if identity is None:
        return 1.0
    if literal_node is None or literal_node.node_type != NodeType.LITERAL:
        return 1.0

    literal_value = _literal_scalar(literal_node)

    if op == "Eq":
        return _selectivity_eq(identity, literal_value, stats)
    if op == "NotEq":
        return 1.0 - _selectivity_eq(identity, literal_value, stats)
    if op in ("Lt", "LtEq", "Gt", "GtEq"):
        return _selectivity_range(identity, op, literal_value, stats)
    if op == "InList":
        return _selectivity_in(identity, literal_value, stats)
    if op == "NotInList":
        return 1.0 - _selectivity_in(identity, literal_value, stats)
    if op in ("Like", "ILike", "RLike"):
        return _selectivity_like(literal_value)
    if op in ("NotLike", "NotILike", "NotRLike"):
        return 1.0 - _selectivity_like(literal_value)
    if op in ("InStr", "IInStr"):
        # predicate_rewriter.INSTR_REWRITES only ever produces these from
        # "x LIKE '%pattern%'" / "x ILIKE '%pattern%'" (wildcards stripped from
        # the literal on rewrite) -- same infix-substring semantics as
        # _selectivity_like's fallback. Tiered: char-class estimator when the
        # column has ANALYZE'd char-class stats and a bind-time-captured decay
        # (see _selectivity_instr); else the flat constant -- same answer as
        # before this estimator existed, so "no/partial stats" needs nothing
        # beyond this fallthrough.
        return _selectivity_instr(identity, literal_value, node, stats)
    if op in ("NotInStr", "NotIInStr"):
        return 1.0 - _selectivity_instr(identity, literal_value, node, stats)
    return 1.0


def _selectivity_eq(identity: bytes, literal_value, stats: RelationStatistics) -> float:
    col = stats.columns.get(identity)
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
    return _EQ_UNKNOWN_NDV_FALLBACK


def _selectivity_col_vs_col(
    op: str, left_identity: bytes, right_identity: bytes, stats: RelationStatistics
) -> float:
    """Selectivity for ``col op col`` -- no literal on either side.

    Eq/NotEq reuse the same NDV formula as equi-join key selectivity
    (join_cardinality._key_selectivity: ``1 / max(ndv_left, ndv_right)`` --
    a match requires landing on one of the rarer side's values). Range
    comparisons (Lt/LtEq/Gt/GtEq) between two columns have no such signal:
    per-column histograms/ranges say nothing about the CORRELATION between
    the two columns, so there is no honest way to sharpen past a constant.
    """
    if left_identity == right_identity:
        # `a.x = a.x`, `a.x <> a.x`, etc. compare a column to itself --
        # always/never true regardless of data, not an NDV question.
        if op in ("Eq", "LtEq", "GtEq"):
            return 1.0
        if op in ("NotEq", "Lt", "Gt"):
            return 0.0
        return 1.0

    if op in ("Eq", "NotEq"):
        left_col = stats.columns.get(left_identity)
        right_col = stats.columns.get(right_identity)
        left_ndv = left_col.distinct_count if left_col else None
        right_ndv = right_col.distinct_count if right_col else None
        if left_ndv and right_ndv:
            eq_selectivity = 1.0 / max(left_ndv, right_ndv)
        else:
            eq_selectivity = _EQ_UNKNOWN_NDV_FALLBACK
        return eq_selectivity if op == "Eq" else 1.0 - eq_selectivity

    if op in ("Lt", "LtEq", "Gt", "GtEq"):
        return _COLUMN_VS_COLUMN_RANGE_SELECTIVITY

    return 1.0


def _selectivity_range(
    identity: bytes, op: str, literal_value, stats: RelationStatistics
) -> float:
    col = stats.columns.get(identity)
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


def _selectivity_in(identity: bytes, literal_value, stats: RelationStatistics) -> float:
    if not isinstance(literal_value, (list, tuple, set, frozenset)):
        return 0.1
    values = list(literal_value)
    n = len(values)
    if n == 0:
        return 0.0

    col = stats.columns.get(identity)
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
    identity = _identifier_identity(node.left)
    if identity is None:
        return 1.0
    right = node.right
    centre = node.centre
    if right is None or centre is None:
        return 1.0
    if right.node_type != NodeType.LITERAL or centre.node_type != NodeType.LITERAL:
        return 1.0

    a = _to_float(_literal_scalar(right))
    b = _to_float(_literal_scalar(centre))

    col = stats.columns.get(identity)
    dgram = col.histogram if col is not None else None
    if dgram is not None and a is not None and b is not None:
        total = float(dgram.count())
        if total > 0:
            lo, hi = (a, b) if a <= b else (b, a)
            fraction = (_count_up_to(dgram, hi) - _count_up_to(dgram, lo)) / total
            return _clamp01(fraction)
    return 0.25


def _selectivity_is_null(identity: bytes, stats: RelationStatistics) -> float:
    col = stats.columns.get(identity)
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


def _identifier_identity(node) -> Optional[bytes]:
    """Identity of an IDENTIFIER node — the key ``RelationStatistics.columns`` uses.

    Names are not unique across a plan (``it1.info`` and ``mi.info`` are two
    columns both named ``info``), so a name lookup could pull another
    relation's histogram/NDV and silently mis-estimate. Returns None when no
    identity is resolvable; callers then fall back to a constant.
    """
    if node is None or getattr(node, "node_type", None) != NodeType.IDENTIFIER:
        return None
    schema_column = getattr(node, "schema_column", None)
    identity = getattr(schema_column, "identity", None) if schema_column is not None else None
    return identity if isinstance(identity, bytes) else None


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

# Equality selectivity when neither side's NDV is known -- shared by the
# literal (`col = X`) and column-vs-column (`col = col`) paths.
_EQ_UNKNOWN_NDV_FALLBACK = 0.1

# Textbook default for an inequality between two columns (`a.x < b.y`) with no
# literal on either side. Per-column histograms/ranges say nothing about the
# CORRELATION between the two columns, so there's no honest way to sharpen
# this past the standard unbounded-inequality constant (Selinger et al.).
_COLUMN_VS_COLUMN_RANGE_SELECTIVITY = 1.0 / 3.0


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


# ---- infix LIKE ('%needle%') char-class estimator ---------------------------
#
# Ported (verbatim algorithm) from scratch/like_selectivity/estimators.py's
# decayed_char_class_selectivity, the winner of an offline experiment against
# 371K real NVD VARCHAR rows + a real ClickBench query (aggregate MAE 4.45% vs
# the flat _LIKE_INFIX_SELECTIVITY baseline's 14.5%). Full derivation, failure
# modes, and rejected alternatives (entropy-based, undamped char-class, Markov
# bigram) are in that experiment; this is only the shipping subset.
#
# Model: treat "does needle occur in an average row" as a random-containment
# problem --
#   n_positions = max(avg_length - len(needle) + 1, 0)   # candidate start offsets
#   p_pos       = product of each needle byte's per-class match probability,
#                 with each position's LOG-contribution geometrically
#                 discounted by decay**i (position i) so a long needle can't
#                 collapse p_pos to ~0 on templated/repeated content the way
#                 an undamped product would -- see estimators.py's own
#                 docstring for why decay must apply to the log-contribution,
#                 not a fixed per-position weight (an earlier hand-derived
#                 variant that didn't broke selectivity's required
#                 monotonic-non-increasing-in-needle-length property).
#   selectivity = 1 - exp(-n_positions * p_pos)
#
# Known, accepted limitation: this estimator is blind to specific character
# IDENTITY within a class -- 'google', 'abcdef', and a needle that never
# occurs all get the same estimate if same length/shape. Improvement on the
# flat constant, not semantic understanding.

# The 8 byte classes, in the SAME index order opteryx.compiled.nanobind's
# vector_char_class_stats.cpp / draken_native.cpp's char_class_stats binding
# use (0=upper .. 7=control) -- Manifest.get_char_class_stats' class_proportions
# dict is keyed by these names. Vendored here (not re-derived) so the four
# things that must agree (offline classifier, native kernel, this list, the
# cardinality table below) don't drift independently — see
# tests/unit/compiled/test_char_class_stats_parity.py.
_CHAR_CLASSES = (
    "upper",
    "lower",
    "digit",
    "whitespace",
    "punct_text",
    "semantic",
    "extended",
    "control",
)

# Number of distinct byte values the native classifier assigns to each class
# (26 for upper/lower, 10 for digit, etc.) — generated from
# scratch/like_selectivity/stats.py's _BYTE_CLASS via
# `{name: int((_BYTE_CLASS == i).sum()) for i, name in enumerate(CLASSES)}`,
# not independently hand-typed (see decision in the approved plan). Used to
# convert a stored class PROPORTION back into a per-BYTE probability, assuming
# uniform distribution within the class.
_CLASS_CARDINALITY = {
    "upper": 26,
    "lower": 26,
    "digit": 10,
    "whitespace": 6,
    "punct_text": 10,
    "semantic": 22,
    "extended": 128,
    "control": 28,
}

# Byte -> _CHAR_CLASSES index, for bytes 0-255. `scratch/` is experimental,
# unpackaged code (.claude/CLAUDE.md §5) — never importable from production —
# so this is a literal copy of scratch/like_selectivity/stats.py's
# `_BYTE_CLASS` (itself the source draken's native char_class_stats kernel's
# BYTE_CLASS table was ALSO generated from), not a runtime import of it.
# tests/unit/compiled/test_char_class_stats_parity.py is the enforcement
# mechanism that keeps this, the native table, and stats.py's table in sync.
_BYTE_CLASS = (
    7, 7, 7, 7, 7, 7, 7, 7, 7, 3, 3, 3, 3, 3, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    3, 4, 4, 5, 5, 5, 5, 4, 4, 4, 5, 5, 4, 4, 4, 5,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 5, 4, 5, 5, 5, 4,
    5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 5, 5, 5, 5,
    5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 5, 5, 5, 5, 7,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
)

_LOG_PCHAR_FLOOR = math.log(1e-6)


def _classify_char(c: str) -> str:
    """Classify a single Python character into one of _CHAR_CLASSES.

    Mirrors estimators.py's _classify_char / stats.py's byte classifier
    exactly for the Latin-1 range; a genuine Unicode codepoint beyond that
    (b > 255, impossible for the native kernel's byte-level classification of
    stored UTF-8 data, but the needle literal is a Python str and could
    contain one) maps to "extended" as the closest class conceptually.
    """
    b = ord(c)
    if b > 255:
        return "extended"
    return _CHAR_CLASSES[_BYTE_CLASS[b]]


def _like_needle_str(literal_value) -> Optional[str]:
    if isinstance(literal_value, bytes):
        try:
            return literal_value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if isinstance(literal_value, str):
        return literal_value
    return None


def _containment_selectivity(p_pos: float, avg_length: float, needle_len: int) -> float:
    n_positions = max(avg_length - needle_len + 1, 0.0)
    if n_positions <= 0 or p_pos <= 0:
        return 0.0
    exponent = n_positions * p_pos
    return _clamp01(1.0 - math.exp(-exponent))


def _decayed_char_class_selectivity(
    needle: str, class_proportions: dict, avg_length: float, decay: float
) -> float:
    """Verbatim port of estimators.py's decayed_char_class_selectivity."""
    if not needle:
        return 1.0
    log_p_pos = 0.0
    for i, c in enumerate(needle):
        cls = _classify_char(c)
        prop = class_proportions.get(cls, 0.0)
        cardinality = _CLASS_CARDINALITY.get(cls, 256)
        p_char = prop / cardinality if cardinality > 0 else 0.0
        log_p_char = math.log(p_char) if p_char > 0 else _LOG_PCHAR_FLOOR
        log_p_pos += (decay**i) * log_p_char
    p_pos = math.exp(log_p_pos)
    return _containment_selectivity(p_pos, avg_length, len(needle))


def predicate_estimator_tag(predicate, stats: RelationStatistics) -> Optional[str]:
    """Which selectivity estimator would fire for `predicate` — diagnostic
    telemetry only (opteryx.planner.optimizer.statistics_refresh._predicate_note).

    Mirrors _selectivity_instr's own tier check WITHOUT re-running estimation
    (no float math, just the same presence/None checks). Returns
    "char_class_decay" | "flat_fallback" for an infix LIKE predicate
    (InStr/IInStr/NotInStr/NotIInStr — the rewritten form of "x LIKE
    '%pattern%'"), None for every other predicate kind — other predicate
    kinds have no estimator tiers worth surfacing yet.
    """
    if getattr(predicate, "node_type", None) != NodeType.COMPARISON_OPERATOR:
        return None
    op = predicate.value
    if op not in ("InStr", "IInStr", "NotInStr", "NotIInStr"):
        return None

    left, right = predicate.left, predicate.right
    identity = _identifier_identity(left)
    literal_node = right
    if identity is None:
        identity = _identifier_identity(right)
        literal_node = left
    if identity is None or literal_node is None or literal_node.node_type != NodeType.LITERAL:
        return "flat_fallback"

    col = stats.columns.get(identity)
    needle = _like_needle_str(_literal_scalar(literal_node))
    decay = predicate.like_selectivity_decay
    if (
        col is not None
        and needle is not None
        and col.class_proportions is not None
        and col.avg_length
        and col.avg_length > 0
        and decay is not None
    ):
        return "char_class_decay"
    return "flat_fallback"


def _selectivity_instr(identity: bytes, literal_value, node, stats: RelationStatistics) -> float:
    col = stats.columns.get(identity)
    needle = _like_needle_str(literal_value)
    decay = node.like_selectivity_decay  # plain attribute access — Node.__getattr__
    # returns None for any never-set attribute; bind-time capture is Part D
    # (opteryx/planner/binder/binder.py), not this module's concern.
    if (
        col is not None
        and needle is not None
        and col.class_proportions is not None
        and col.avg_length
        and col.avg_length > 0
        and decay is not None
    ):
        return _decayed_char_class_selectivity(needle, col.class_proportions, col.avg_length, decay)
    return _LIKE_INFIX_SELECTIVITY
