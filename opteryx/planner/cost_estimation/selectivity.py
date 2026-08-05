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
from opteryx.types.logical_type import DrakenType


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

    if nt == NodeType.FUNCTION:
        # predicate_rewriter.py rewrites a prefix LIKE/ILIKE ("x LIKE
        # 'foo%'") into a `_STARTS_WITH`/`_CI_STARTS_WITH` FUNCTION node, and a
        # suffix LIKE/ILIKE ("x LIKE '%foo'") into a `_ENDS_WITH`/
        # `_CI_ENDS_WITH` FUNCTION node, before this dispatcher ever sees it --
        # see the estimators below. Every other FUNCTION falls through to 1.0
        # (no selectivity model).
        if node.value == "_STARTS_WITH":
            return _selectivity_starts_with(node, stats)
        if node.value == "_CI_STARTS_WITH":
            return _selectivity_ci_starts_with(node, stats)
        if node.value == "_ENDS_WITH":
            return _selectivity_ends_with(node, stats)
        if node.value == "_CI_ENDS_WITH":
            return _selectivity_ci_ends_with(node, stats)
        return 1.0

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

    # No histogram: interpolate uniformly across the column's known min/max.
    # Coarser than a histogram (it cannot see skew) but vastly better than a
    # blind constant, because a constant charges the SAME 0.25 to a bound that
    # excludes nothing as to one that excludes almost everything.
    #
    # That mattered as soon as Scan statistics started carrying real manifest
    # bounds: CorrelatedFiltersStrategy pushes each join key's range onto the
    # opposite leg, and those pushed bounds sit just inside the target's own
    # range. JOB 10a pushed `t.id >= 2` and `t.id <= 2525745` onto a column
    # spanning 1..2528312 -- excluding 3 rows in 2.5 million -- and the flat
    # 0.25 charged each of them a 4x reduction. Four such bounds took title's
    # estimate from 632,077 rows to 2,469, and the join's own selectivity for
    # the very same constraint was applied on top: the same predicate paid for
    # twice. Interpolation returns ~1.0 for those bounds, which is the honest
    # answer and leaves the join as the single place the constraint is priced.
    #
    # Where a pushed range genuinely narrows, this stays consistent with the
    # join estimate rather than compounding it: the scan is charged the real
    # fraction, _narrow_filter_columns tightens the column's value_range, and
    # _value_range_span caps the join's tdom to that same narrowed span.
    if lit_f is not None and col is not None and col.value_range is not None:
        lower = _to_float(col.value_range.lower_bound)
        upper = _to_float(col.value_range.upper_bound)
        if lower is not None and upper is not None and upper > lower:
            fraction_below = (lit_f - lower) / (upper - lower)
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

    Mirrors _selectivity_instr's/_selectivity_starts_with's/
    _selectivity_ci_starts_with's/_selectivity_ends_with's/
    _selectivity_ci_ends_with's own tier checks WITHOUT re-running
    estimation (no float math, just the same presence/None checks). Returns
    "char_class_decay" | "flat_fallback" for an infix LIKE predicate
    (InStr/IInStr/NotInStr/NotIInStr — the rewritten form of "x LIKE
    '%pattern%'"), "ordinal_range" | "ordinal_bounds" | "flat_fallback" for a
    prefix LIKE predicate (_STARTS_WITH — "x LIKE 'foo%'": a full histogram,
    else just the relation-wide min/max span, else no signal at all),
    "char_class_prefix" | "flat_fallback" for a case-insensitive prefix LIKE
    predicate (_CI_STARTS_WITH — "x ILIKE 'foo%'"), "char_class_suffix" |
    "flat_fallback" for a suffix LIKE predicate of either case sensitivity
    (_ENDS_WITH/_CI_ENDS_WITH — "x LIKE '%foo'"/"x ILIKE '%foo'": no
    ordinal-range tier exists for a suffix at all, see the module comment
    above _selectivity_ends_with), None for every other predicate kind —
    other predicate kinds have no estimator tiers worth surfacing yet.
    """
    if getattr(predicate, "node_type", None) == NodeType.FUNCTION:
        if predicate.value == "_STARTS_WITH":
            operands = _two_operand_function_operands(predicate)
            if operands is None:
                return "flat_fallback"
            column_node, identity, _prefix = operands
            col = stats.columns.get(identity)
            if col is None or _physical_type(column_node) is None:
                return "flat_fallback"
            if col.histogram is not None:
                return "ordinal_range"
            if col.ordinal_bounds is not None:
                return "ordinal_bounds"
            return "flat_fallback"
        if predicate.value == "_CI_STARTS_WITH":
            operands = _two_operand_function_operands(predicate)
            if operands is None:
                return "flat_fallback"
            _column_node, identity, _prefix = operands
            col = stats.columns.get(identity)
            if (
                col is not None
                and col.class_proportions is not None
                and col.avg_length
                and col.avg_length > 0
            ):
                return "char_class_prefix"
            return "flat_fallback"
        if predicate.value in ("_ENDS_WITH", "_CI_ENDS_WITH"):
            operands = _two_operand_function_operands(predicate)
            if operands is None:
                return "flat_fallback"
            _column_node, identity, _suffix = operands
            col = stats.columns.get(identity)
            if (
                col is not None
                and col.class_proportions is not None
                and col.avg_length
                and col.avg_length > 0
            ):
                return "char_class_suffix"
            return "flat_fallback"
        return None

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
    if needle is not None:
        # Hard guard first: node is the full comparison (InStr/IInStr), not a
        # FUNCTION node's isolated column parameter -- resolve whichever side
        # is the bound identifier to get its physical type. `literal_value`
        # is already the decoded scalar (via _literal_scalar upstream), which
        # may have lost the original byte length for a str -- re-derive byte
        # length from `needle` is wrong for the same reason noted in
        # _selectivity_ci_starts_with (needle can be a decoded str); use the
        # RAW literal bytes when available, falling back to needle's own
        # length only if literal_value wasn't bytes to begin with.
        column_node = node.left if _identifier_identity(node.left) == identity else node.right
        physical = _physical_type(column_node)
        raw_len = len(literal_value) if isinstance(literal_value, (bytes, bytearray)) else len(needle)
        if _exceeds_max_length(raw_len, col, physical):
            return 0.0
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


# ---- prefix STARTS_WITH ('foo%') estimators ---------------------------------
#
# predicate_rewriter.py rewrites a prefix LIKE/ILIKE ("x LIKE 'foo%'") into a
# `_STARTS_WITH`/`_CI_STARTS_WITH` FUNCTION node before selectivity estimation
# ever runs, so these are reached via the NodeType.FUNCTION dispatch branch in
# `_selectivity`, not `_selectivity_comparison`.
#
# _STARTS_WITH (case-sensitive) reuses infrastructure built for a different
# purpose (scan-level file pruning): ColumnType.ordinalize() (draken) turns a
# VARCHAR value into a lexicographic-order-preserving int64 key by packing its
# first 8 bytes big-endian, then right-shifting by 1 to fit a non-negative
# int64 (zero-padded if shorter -- see draken/ops/ordinalize.h). That shift is
# a floor-by-2, so it's still strictly order-preserving (monotonic), which is
# all range comparison below needs -- but it costs the LOWEST bit of the 8th
# byte: two 8-byte-or-longer values sharing their first 7 bytes and differing
# by exactly 1 in their 8th byte collide onto the same key (adjacent values
# binned into the same ordered bucket, not a total order past 7 bytes). And
# Manifest.get_distogram already bins VARCHAR histograms in that same
# ordinal-key space (_analyze.py ordinalizes every morsel before binning,
# unconditionally for string-family columns). "Starts with prefix" is exactly
# the ordinal-key range [ordinalize(prefix), ordinalize(prefix 0xFF-padded to
# 8 bytes)] -- ordinalize zero-pads a short value automatically, so the lower
# bound is ordinalize(prefix) directly; the upper bound needs the 0xFF padding
# built explicitly. Prefixes >= 8 bytes collide with any other value sharing
# an identical first 8 bytes (and, per the low-bit note above, occasionally
# one further value one apart in its 8th byte) -- treated as a point density
# via the same bin-width technique _selectivity_eq uses for an exact-value
# lookup.
#
# _CI_STARTS_WITH (case-insensitive) can NOT reuse that range lookup: case
# variants ('Foo'/'foo'/'FOO') land in disjoint regions of ordinal-key space,
# not one contiguous range. It keeps a char-class probability model instead --
# structurally simpler than the infix estimator (_decayed_char_class_selectivity)
# since a prefix match is anchored at position 0: no n_positions search, no
# decay**i damping, just the product of each needle character's per-class
# match probability, merging upper+lower for an alphabetic character (a
# case-insensitive match accepts either case in the data).


def _physical_type(column_node):
    """The column's DrakenType (ColumnType.physical), or None.

    Resolved off the bound IDENTIFIER node itself -- predicate_rewriter
    reuses the original comparison's `left` node verbatim when building the
    `_STARTS_WITH`/`_CI_STARTS_WITH` FUNCTION node, so `schema_column` (and
    its `column_type`) is already populated from bind time.
    """
    schema_column = getattr(column_node, "schema_column", None)
    column_type = getattr(schema_column, "column_type", None) if schema_column is not None else None
    return getattr(column_type, "physical", None) if column_type is not None else None


def _exceeds_max_length(needle_len: int, col, physical) -> bool:
    """True when `needle_len` provably exceeds the column's observed maximum
    string length -- a hard, certain "impossible match" signal shared by
    every containment-style estimator below (STARTS_WITH, INSTR, ENDS_WITH,
    and their case-insensitive variants): a needle longer than anything that
    has ever appeared in the column cannot match, no probability involved.
    This is a stronger, cheaper check than any of those estimators' existing
    avg_length-based soft dampening, and should run first.

    Skipped for NVARCHAR. The external catalog's length stats are CHARACTER-
    based (Python len() on a decoded str -- opteryx_catalog's
    _compute_column_stats, verified directly against the installed package),
    while every needle length compared against col.length_bounds here is
    BYTE-based (predicate literals are bytes by the time they reach this
    module; the local ANALYZE path's native char_class_stats() kernel is
    also byte-based, self-consistently). UTF-8 byte length is always >= char
    length, so a catalog-sourced max_length can UNDER-state the true byte
    ceiling for non-ASCII content, risking a false "impossible" verdict for
    a needle that's actually still possible. VARCHAR/VARBINARY don't have
    this risk (VARBINARY has no text encoding at all to be ambiguous about;
    VARCHAR is trusted as-is -- an explicit product decision that non-ASCII
    content concentrates in NVARCHAR in practice, not a technical guarantee).
    """
    if physical == DrakenType.NVARCHAR:
        return False
    length_bounds = getattr(col, "length_bounds", None) if col is not None else None
    if length_bounds is None:
        return False
    _min_length, max_length = length_bounds
    return needle_len > max_length


def _two_operand_function_operands(node):
    """(column_node, identity, literal_bytes) for a _STARTS_WITH/_CI_STARTS_WITH/
    _ENDS_WITH/_CI_ENDS_WITH FUNCTION node, or None when any part can't be
    resolved. Generic across all four -- predicate_rewriter.py builds each as
    the same shape, `FUNCTION(col, literal)`, differing only in which end of
    the string the literal anchors to (a distinction the caller applies, not
    this helper).

    An empty literal is valid and reachable (`LIKE '%'` rewrites to
    `_STARTS_WITH(col, '')`, matching every non-null value) -- callers handle
    it correctly (empty needle / full-span ordinal range), so it is not
    rejected here.
    """
    params = getattr(node, "parameters", None)
    if not params or len(params) != 2:
        return None
    column_node, literal_node = params[0], params[1]
    identity = _identifier_identity(column_node)
    if identity is None:
        return None
    literal_bytes = getattr(literal_node, "value", None)
    if isinstance(literal_bytes, str):
        literal_bytes = literal_bytes.encode("utf-8")
    if not isinstance(literal_bytes, bytes):
        return None
    return column_node, identity, literal_bytes


def _selectivity_starts_with(node, stats: RelationStatistics) -> float:
    """Four tiers, richest signal first:

    0. Hard length guard (_exceeds_max_length): a prefix longer than the
       column's observed maximum string length cannot match ANY row --
       0.0, certain, no probability involved. Runs before anything else.
    1. Histogram (col.histogram): exact range/point density against the
       column's binned ordinal-key distribution -- see the module-level
       comment above for the range/point-density derivation.
    2. Ordinal bounds only (col.ordinal_bounds), when no histogram exists
       (e.g. per-file min/max are populated at ordinary write time, but the
       richer histogram needs an explicit ANALYZE that hasn't run): a
       relation-wide [lo, hi] with no bin-level detail. A prefix range
       entirely outside it can't match anything -> 0.0. One that overlaps
       gets a uniform-density estimate (the fraction of the observed span
       covered by the prefix's ordinal range) -- the standard textbook
       fallback when only min/max are known, but computed from a REAL
       observed range instead of a made-up constant. The >=8-byte point case
       has no span to interpolate over, so it prefers NDV (1/distinct_count,
       same fallback _selectivity_eq uses) when known.
    3. Flat constant (_LIKE_PREFIX_SELECTIVITY): no stats at all.

    The >=8-byte point-density case (tiers 1 and 2) is where ordinalize's
    precision genuinely runs out -- bytes past the first ~8 are invisible to
    the ordinal-key comparison, so a "match" there is only ever a coincidence
    of the visible prefix, not a verified full match. That branch gets an
    additional avg_length-based soft discount (mirrors
    _selectivity_ci_starts_with's), on top of the hard guard above: a needle
    close to or longer than the average string length is less likely to be a
    genuine match than the raw bucket density alone suggests. The <8-byte
    RANGE case does NOT get this discount -- it's already an exact
    computation against the real observed distribution (a too-short value
    naturally zero-pads to sort below the prefix range and is excluded on
    its own), so an extra discount there would double-count uncertainty
    that's already reflected in the real data.
    """
    operands = _two_operand_function_operands(node)
    if operands is None:
        return _LIKE_PREFIX_SELECTIVITY
    column_node, identity, prefix = operands

    col = stats.columns.get(identity)
    physical = _physical_type(column_node)
    if col is None or physical is None:
        return _LIKE_PREFIX_SELECTIVITY

    if _exceeds_max_length(len(prefix), col, physical):
        return 0.0

    # Exact-integer ordinal keys -- kept as Python ints (arbitrary precision)
    # for every comparison against col.ordinal_bounds (also exact ints, from
    # Manifest.get_ordinal_bounds). float(int) on a value this large (up to
    # ~2**62) silently rounds -- e.g. float(4051330591175588409) rounds DOWN
    # to 4051330591175588352, a different integer -- which previously made an
    # exact-match prefix compare as spuriously "less than" its own bound.
    # Only the histogram tier below, which needs Distogram's float64 API,
    # converts to float, and only for that one call.
    try:
        lo_key = physical.ordinalize(prefix)
    except ValueError:
        return _LIKE_PREFIX_SELECTIVITY
    if not isinstance(lo_key, int):
        return _LIKE_PREFIX_SELECTIVITY

    pad = 8 - min(len(prefix), 8)
    hi_key = None
    if pad > 0:
        try:
            hi_key = physical.ordinalize(prefix[:8] + b"\xff" * pad)
        except ValueError:
            return _LIKE_PREFIX_SELECTIVITY
        if not isinstance(hi_key, int):
            return _LIKE_PREFIX_SELECTIVITY

    # Only meaningful (and only computed) for the point-density case
    # (hi_key is None) -- see the docstring's discount rationale.
    point_discount = 1.0
    if hi_key is None and col.avg_length and col.avg_length > 0:
        point_discount = min(1.0, col.avg_length / len(prefix))

    dgram = col.histogram
    if dgram is not None:
        total = float(dgram.count())
        if total > 0:
            lo_f = float(lo_key)
            if hi_key is None:
                # Prefix >= 8 bytes: every string sharing this 8-byte prefix
                # collides to lo_key -- a point density, not a range.
                bins_len = dgram.bin_count
                span = dgram.max - dgram.min
                if bins_len > 1 and span > 0:
                    bin_width = span / bins_len
                    below = _count_up_to(dgram, lo_f - bin_width / 2.0)
                    above = _count_up_to(dgram, lo_f + bin_width / 2.0)
                    return _clamp01((above - below) / total * point_discount)
                # Degenerate histogram (e.g. a single bin) -- fall through to
                # the ordinal-bounds/constant tiers below rather than give up.
            else:
                below = _count_up_to(dgram, lo_f)
                above = _count_up_to(dgram, float(hi_key))
                return _clamp01((above - below) / total)

    bounds = col.ordinal_bounds
    if bounds is not None:
        bound_lo, bound_hi = bounds
        query_hi = hi_key if hi_key is not None else lo_key
        if query_hi < bound_lo or lo_key > bound_hi:
            return 0.0
        span = bound_hi - bound_lo
        if span <= 0:
            # Degenerate/constant observed range, and the disjoint check
            # above already confirmed it falls inside the query range.
            return _clamp01(1.0 * point_discount) if hi_key is None else 1.0
        if hi_key is None:
            ndv = col.distinct_count
            if ndv and ndv > 0:
                return _clamp01((1.0 / ndv) * point_discount)
            return _clamp01(_LIKE_PREFIX_SELECTIVITY * point_discount)
        overlap = min(query_hi, bound_hi) - max(lo_key, bound_lo)
        return _clamp01(overlap / span)

    return _clamp01(_LIKE_PREFIX_SELECTIVITY * point_discount) if hi_key is None else _LIKE_PREFIX_SELECTIVITY


def _ci_char_probability(c: str, class_proportions: dict) -> float:
    """Per-character match probability for the case-insensitive prefix
    estimator. For an alphabetic character, sums the upper- and
    lower-class proportions before dividing by a single case's cardinality
    (26) -- 'upper' and 'lower' have equal cardinality, so
    P(byte matches either case) = prop_upper/26 + prop_lower/26 =
    (prop_upper + prop_lower)/26, NOT the naive (prop_upper + prop_lower)
    over the SUMMED cardinality (52), which would silently halve the true
    probability. Every other class is unaffected by case and uses its own
    proportion/cardinality unchanged.
    """
    cls = _classify_char(c)
    if cls in ("upper", "lower"):
        cardinality = _CLASS_CARDINALITY["lower"]  # == _CLASS_CARDINALITY["upper"]
        prop = class_proportions.get("upper", 0.0) + class_proportions.get("lower", 0.0)
    else:
        cardinality = _CLASS_CARDINALITY.get(cls, 256)
        prop = class_proportions.get(cls, 0.0)
    return prop / cardinality if cardinality > 0 else 0.0


def _selectivity_ci_starts_with(node, stats: RelationStatistics) -> float:
    operands = _two_operand_function_operands(node)
    if operands is None:
        return _LIKE_PREFIX_SELECTIVITY
    column_node, identity, prefix = operands

    col = stats.columns.get(identity)
    needle = _like_needle_str(prefix)
    if (
        col is None
        or needle is None
        or col.class_proportions is None
        or not col.avg_length
        or col.avg_length <= 0
    ):
        return _LIKE_PREFIX_SELECTIVITY
    if not needle:
        return 1.0

    # Byte length of the literal, NOT len(needle) -- `needle` is the decoded
    # Python str from _like_needle_str, so its len() is CHARACTER count. The
    # guard (and col.length_bounds) compares in bytes; using char count here
    # would silently reintroduce the exact byte-vs-char mismatch the guard's
    # NVARCHAR skip exists to avoid, just on this side of the comparison.
    if _exceeds_max_length(len(prefix), col, _physical_type(column_node)):
        return 0.0

    log_p_pos = 0.0
    for c in needle:
        p_char = _ci_char_probability(c, col.class_proportions)
        log_p_pos += math.log(p_char) if p_char > 0 else _LOG_PCHAR_FLOOR
    p_prefix = math.exp(log_p_pos)

    discount = min(1.0, col.avg_length / len(needle))
    return _clamp01(p_prefix * discount)


# ---- suffix ENDS_WITH ('%foo') estimators -----------------------------------
#
# predicate_rewriter.py rewrites a suffix LIKE/ILIKE ("x LIKE '%foo'") into a
# `_ENDS_WITH`/`_CI_ENDS_WITH` FUNCTION node before selectivity estimation ever
# runs, reached via the same NodeType.FUNCTION dispatch branch as STARTS_WITH.
#
# Unlike a prefix, a suffix has no ordinal-key range at all: ColumnType.
# ordinalize() keys on a string's FIRST 8 bytes, so two values sharing an
# identical suffix but differing prefixes ordinalize to unrelated keys, and a
# column-wide min/max says nothing about what strings end with. Neither
# _selectivity_starts_with's ordinal-range/ordinal-bounds tiers nor
# Manifest.get_ordinal_bounds are applicable here -- there is no richer tier
# above the char-class model for EITHER case sensitivity, so both variants
# share one tier: the char-class product below, falling back to the flat
# _LIKE_PREFIX_SELECTIVITY constant when no char-class stats exist.
#
# The infix estimator (_decayed_char_class_selectivity) models a needle that
# could START at any of n_positions candidate offsets, summing/damping across
# all of them (`1 - exp(-n_positions * p_pos)` with a decay**i per-position
# discount) -- that model does not apply here. A suffix has exactly ONE valid
# anchor position (the string's last len(needle) characters), so this is a
# straight per-character product with no position search and no decay, the
# same structural shape _selectivity_ci_starts_with already uses anchored at
# position 0. The two ENDS_WITH variants differ only in which per-character
# probability function they use (plain class proportion vs case-merged), so
# they share _anchored_char_class_selectivity below rather than duplicating
# the loop.


def _plain_char_probability(c: str, class_proportions: dict) -> float:
    """Per-character match probability for the case-sensitive suffix
    estimator -- the same class_proportion/cardinality lookup
    _decayed_char_class_selectivity uses per position, factored out so
    _selectivity_ends_with doesn't duplicate it.
    """
    cls = _classify_char(c)
    cardinality = _CLASS_CARDINALITY.get(cls, 256)
    prop = class_proportions.get(cls, 0.0)
    return prop / cardinality if cardinality > 0 else 0.0


def _anchored_char_class_selectivity(
    needle: str, class_proportions: dict, avg_length: float, char_probability
) -> float:
    """Single-anchor char-class product shared by `_selectivity_ends_with`
    and `_selectivity_ci_ends_with`. ``char_probability`` is
    `_plain_char_probability` or `_ci_char_probability`, applied per
    character with no position search and no decay (see module comment
    above) plus the same avg_length/needle_len width discount
    `_selectivity_ci_starts_with` uses: a needle longer than the column's
    average string length can't fit as a suffix of an average row.
    """
    if not needle:
        return 1.0
    log_p = 0.0
    for c in needle:
        p_char = char_probability(c, class_proportions)
        log_p += math.log(p_char) if p_char > 0 else _LOG_PCHAR_FLOOR
    p_suffix = math.exp(log_p)

    discount = min(1.0, avg_length / len(needle))
    return _clamp01(p_suffix * discount)


def _selectivity_ends_with(node, stats: RelationStatistics) -> float:
    operands = _two_operand_function_operands(node)
    if operands is None:
        return _LIKE_PREFIX_SELECTIVITY
    column_node, identity, suffix = operands

    col = stats.columns.get(identity)
    needle = _like_needle_str(suffix)
    if (
        col is None
        or needle is None
        or col.class_proportions is None
        or not col.avg_length
        or col.avg_length <= 0
    ):
        return _LIKE_PREFIX_SELECTIVITY
    if not needle:
        return 1.0

    # Byte length of the literal (see _selectivity_ci_starts_with's identical
    # note): `needle` is the decoded str, `suffix` is the original bytes.
    if _exceeds_max_length(len(suffix), col, _physical_type(column_node)):
        return 0.0

    return _anchored_char_class_selectivity(
        needle, col.class_proportions, col.avg_length, _plain_char_probability
    )


def _selectivity_ci_ends_with(node, stats: RelationStatistics) -> float:
    operands = _two_operand_function_operands(node)
    if operands is None:
        return _LIKE_PREFIX_SELECTIVITY
    column_node, identity, suffix = operands

    col = stats.columns.get(identity)
    needle = _like_needle_str(suffix)
    if (
        col is None
        or needle is None
        or col.class_proportions is None
        or not col.avg_length
        or col.avg_length <= 0
    ):
        return _LIKE_PREFIX_SELECTIVITY
    if not needle:
        return 1.0

    if _exceeds_max_length(len(suffix), col, _physical_type(column_node)):
        return 0.0

    return _anchored_char_class_selectivity(
        needle, col.class_proportions, col.avg_length, _ci_char_probability
    )
