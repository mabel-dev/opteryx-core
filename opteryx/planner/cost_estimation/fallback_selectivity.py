# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Single source of truth for fallback (no-statistics) selectivity constants.

Two consumers price the same predicates: ``cost_estimation.selectivity``
(statistics attached, degrading internally to textbook constants) and
``optimizer.strategies.predicate_ordering`` (no statistics attached at all).
Until 2026-08-21 each declared its own copies with DIFFERENT values -- range
0.5 vs 0.25, LIKE 0.3 vs 0.25/0.1, InStr 0.3 vs 0.1 -- so the same predicate
was priced differently depending on whether stats were attached, and filter
order could flip on that accident. Architect ruling (2026-08-21): the
stats-informed module's values win, defined once here.

Nothing in this module may import from elsewhere in the planner -- it sits at
the bottom of the cost-estimation dependency graph.
"""

# Equality when NDV is unknown for every relevant side. Shared by the literal
# (`col = X`) and column-vs-column (`col = col`) paths in selectivity.py and by
# the equi-join key fallback in join_cardinality._key_selectivity.
EQ_UNKNOWN_NDV_FALLBACK = 0.1

# Unbounded range comparison (`col < X`) with no histogram or value range, and
# the BETWEEN fallback (a range predicate). Textbook constant (Selinger et al.).
RANGE_FALLBACK_SELECTIVITY = 0.25

# LIKE-family predicates with no content stats. "Prefix" = pattern like 'foo%'
# (still bounds a range, a bit more selective); "infix" = pattern like '%foo%'
# or unrecognized shapes (no positional anchor at all, least selective).
# InStr/IInStr (the rewritten form of an infix LIKE -- see
# predicate_rewriter.INSTR_REWRITES) reuse the infix constant directly.
LIKE_PREFIX_SELECTIVITY = 0.25
LIKE_INFIX_SELECTIVITY = 0.1

# Operator-keyed table for the no-stats ordering path
# (optimizer.strategies.predicate_ordering). Built from the scalars above so
# the two pricing paths cannot drift; negated forms are complements. The
# ordering path cannot see the LIKE pattern, so the LIKE family uses the
# prefix constant as the representative and only the InStr forms (infix by
# construction) use the infix constant.
DEFAULT_SELECTIVITY = {
    "Eq": EQ_UNKNOWN_NDV_FALLBACK,
    "NotEq": 1.0 - EQ_UNKNOWN_NDV_FALLBACK,
    "Gt": RANGE_FALLBACK_SELECTIVITY,
    "GtEq": RANGE_FALLBACK_SELECTIVITY,
    "Lt": RANGE_FALLBACK_SELECTIVITY,
    "LtEq": RANGE_FALLBACK_SELECTIVITY,
    "InStr": LIKE_INFIX_SELECTIVITY,
    "IInStr": LIKE_INFIX_SELECTIVITY,
    "NotInStr": 1.0 - LIKE_INFIX_SELECTIVITY,
    "NotIInStr": 1.0 - LIKE_INFIX_SELECTIVITY,
    "Like": LIKE_PREFIX_SELECTIVITY,
    "ILike": LIKE_PREFIX_SELECTIVITY,
    "NotLike": 1.0 - LIKE_PREFIX_SELECTIVITY,
    "NotILike": 1.0 - LIKE_PREFIX_SELECTIVITY,
    "RLike": LIKE_PREFIX_SELECTIVITY,
    "NotRLike": 1.0 - LIKE_PREFIX_SELECTIVITY,
}
