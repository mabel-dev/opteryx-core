# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Correlated Filters

Type: Cost-based (consumes propagated statistics)
Goal: Reduce Rows / IO

For a join predicate ``a.k <op> b.k + delta`` the matching rows on one side are
bounded by the realized value range of the correlated column on the other side.
We read that range from the propagated ``node.statistics`` (post-filter /
post-join-intersection — see statistics_refresh), SHIFT it by ``delta``, and push
it onto the opposite leg's scan as a range predicate, so the scan can prune row
groups and pre-filter rows before the join.

An equi-join ``a.k = b.k`` is the zero-offset special case: it transports BOTH
bounds. A band ``l.event_time > f.flow_start - INTERVAL '20' SECOND`` transports
ONE — any match needs ``l.event_time`` above ``min(f.flow_start) - 20s`` — and a
two-sided band is two such conjuncts, each contributing its own half. The
necessary-condition argument is the same in all three cases; only the offset and
the number of bounds differ. Strict comparators are transported as non-strict
(``>`` becomes ``>=`` on the same value), which weakens the derived filter and so
can only ever forgo pruning, never drop a matching row.

A second, stronger transport rides the same machinery: CONSTANT PROPAGATION. When
one operand of an equi-join resolves to a column a Project binds to a LITERAL --
the shape `WITH params AS (SELECT 'CVE-2023-49105' AS cve_id)` takes, and every
single-row parameter relation with it -- the other operand's value is not merely
bounded, it is KNOWN, and `target = <literal>` goes onto the opposite scan. That
constant is read from the PLAN (the producing Project), never from `value_range`,
which holds numbers and nothing else by ruling (see `_orderable_bound` and the
manifest inlet in statistics_refresh) and so could not carry a VARCHAR key at all.
Being an equality rather than a pair of bounds, it also reaches the connectors'
equality pushdown and the dictionary skip probes, which a range does not.

Constant propagation additionally runs on OUTER joins, into the NULL-SUPPLYING leg
only. A row of that leg which fails the ON condition contributes nothing but the
nulls it would have contributed by being absent, so removing it early is invisible;
the preserved leg is untouchable for the reason the range transport gives above.

This runs *after* PredicatePushdown so the original predicates are already on the
scans and their effect is reflected in the propagated key ranges. The derived
range predicates are appended directly onto the target scan's ``predicates``
list (the same channel PredicatePushdown feeds), so no second pushdown pass is
needed; scans whose connector can't take pushed predicates get a Filter node
instead. The RANGE transport stays restricted to inner / nested-loop joins: it
reads its bound from the propagated key statistics, and an outer join's preserved
key is not narrowed to the match there (see `_intersect_join_keys`), so there is
nothing sound for it to carry across one. Constant propagation has no such
dependency — it reads the plan — and takes the outer-join legs described above.
"""

import decimal
import math
import struct

from opteryx.expression import NodeType
from opteryx.expression.intervals import MICROSECONDS_PER_DAY
from opteryx.models import Node
from opteryx.planner import build_literal_node
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from opteryx.planner.optimizer.statistics import ColumnRange
from opteryx.types.logical_type import INTERVAL
from opteryx.types.logical_type import DrakenType
from opteryx.types.logical_type import LogicalCategory
from opteryx.types.logical_type import TimestampUnit
from opteryx.types.logical_type import integer_bounds
from opteryx.utils import random_string

from .optimization_strategy import (
    OptimizationStrategy,
    OptimizerContext,
    get_nodes_of_type_from_logical_plan,
)


def _phys_identity(col):
    """Column identity for a join-key identifier, for stats.columns lookup.

    Not the name: names are not unique across a plan, so a name lookup can
    silently return an unrelated relation's range."""
    if isinstance(col, bytes):
        return col
    schema_column = getattr(col, "schema_column", None)
    identity = getattr(schema_column, "identity", None) if schema_column is not None else None
    return identity if isinstance(identity, bytes) else None


def _key_value_range(stats, col):
    """Propagated value_range for *col* from a RelationStatistics, or None when
    no bound has been established (column absent / range empty)."""
    if stats is None:
        return None
    identity = _phys_identity(col)
    if identity is None:
        return None
    col_stats = stats.columns.get(identity)
    if col_stats is None:
        return None
    value_range = col_stats.value_range
    if value_range is None or (value_range.lower_bound is None and value_range.upper_bound is None):
        return None
    return value_range


def _tightens(candidate, existing) -> bool:
    """True if *candidate* constrains a column more tightly than *existing*.

    A correlated filter only earns its keep when it removes rows the target
    scan could otherwise return. Once Scan statistics carry the manifest's real
    min/max (they did not before -- ``value_range`` was left empty for any
    column without a predicate on it), the unfiltered case makes both sides of
    a PK/FK join report the SAME full range, and pushing that back is a
    tautology: `l_partkey BETWEEN 1 AND 200000` over a column whose manifest
    bounds are already 1..200000 excludes nothing. It still costs a real
    per-row filter at runtime, and the selectivity estimator charges it the
    0.25 default per bound -- six such bounds took TPC-H lineitem's estimate
    from 6,001,215 rows to 1,465.

    Unknown/incomparable bounds return True: that is the pre-existing
    behaviour (push and let the scan sort it out), so this gate only ever
    suppresses a push it can PROVE is redundant.
    """
    if existing is None:
        return True

    def _tighter(new_bound, old_bound, keep_greater: bool) -> bool:
        if new_bound is None or old_bound is None:
            return False
        if type(new_bound) is not type(old_bound):
            return True  # incomparable -> don't claim redundancy
        return new_bound > old_bound if keep_greater else new_bound < old_bound

    return _tighter(candidate.lower_bound, existing.lower_bound, True) or _tighter(
        candidate.upper_bound, existing.upper_bound, False
    )


# Comparators whose truth over every joined pair implies a bound on each
# operand. `NotEq` is deliberately absent: `a.k <> b.k` constrains neither side.
# The value is (bounds carried onto the LEFT operand, bounds carried onto the
# RIGHT operand), derived from `Lcol <op> Rcol + delta`:
#
#   Eq            L in [rlo+d, rhi+d]      R in [llo-d, lhi-d]
#   Lt / LtEq     L <= rhi+d               R >= llo-d
#   Gt / GtEq     L >= rlo+d               R <= lhi-d
#
# `Lt`/`Gt` collapse onto their non-strict partners on purpose — the pushed
# predicate is only ever GtEq/LtEq, and relaxing a strict bound WIDENS the
# derived filter. A wider necessary condition prunes less; it cannot drop a row.
_TRANSPORTED_BOUNDS = {
    "Eq": ("both", "both"),
    "Lt": ("upper", "lower"),
    "LtEq": ("upper", "lower"),
    "Gt": ("lower", "upper"),
    "GtEq": ("lower", "upper"),
}


def _unwrap_nested(expression):
    """Strip NESTED (parenthesis) wrappers — `(a.k) = (b.k + 1)` is `a.k = b.k + 1`."""
    while expression is not None and expression.node_type == NodeType.NESTED:
        expression = expression.centre
    return expression


def _split_offset(operand):
    """Decompose *operand* into ``(identifier, offset_terms)``, or None.

    Accepted shapes are the ones that mean "this column, displaced by a
    constant": a bare ``col``, ``col + lit``, ``col - lit`` and ``lit + col``.
    ``lit - col`` is NOT of that form (it negates the column) and is rejected,
    as is anything with a non-literal or multiplicative operand — `0.1 * b.ave`
    scales the range rather than shifting it, and no additive delta describes it.

    ``offset_terms`` is a list of ``(literal_node, sign)``. The literal's VALUE
    is deliberately left uninterpreted here: an INTERVAL carries a
    ``(months, microseconds)`` tuple that only becomes a number once the target
    column's native unit is known (see `_resolve_offset`).
    """
    operand = _unwrap_nested(operand)
    if operand is None:
        return None
    if operand.node_type == NodeType.IDENTIFIER:
        return (operand, [])
    if operand.node_type != NodeType.BINARY_OPERATOR or operand.value not in ("Plus", "Minus"):
        return None
    left = _unwrap_nested(getattr(operand, "left", None))
    right = _unwrap_nested(getattr(operand, "right", None))
    if left is None or right is None:
        return None
    if left.node_type == NodeType.IDENTIFIER and right.node_type == NodeType.LITERAL:
        return (left, [(right, 1 if operand.value == "Plus" else -1)])
    if (
        operand.value == "Plus"
        and left.node_type == NodeType.LITERAL
        and right.node_type == NodeType.IDENTIFIER
    ):
        return (right, [(left, 1)])
    return None


def _correlated_join_predicates(on_node):
    """Extract transportable correlations from a (possibly AND-nested) ON condition.

    Yields ``(left_col, right_col, offset_terms, left_bounds, right_bounds)``,
    where ``offset_terms`` is the signed literal decomposition of ``delta`` in
    ``left_col <op> right_col + delta`` and the two ``*_bounds`` say which of that
    operand's bounds the comparator lets us carry ("both" / "upper" / "lower").

    Anything that is not a comparison between two displaced identifiers yields
    nothing — the pre-existing behaviour for every non-equi shape.
    """
    if on_node is None:
        return []
    if on_node.node_type == NodeType.AND:
        return _correlated_join_predicates(on_node.left) + _correlated_join_predicates(
            on_node.right
        )
    if on_node.node_type != NodeType.COMPARISON_OPERATOR:
        return []
    sides = _TRANSPORTED_BOUNDS.get(on_node.value)
    if sides is None:
        return []
    left = _split_offset(getattr(on_node, "left", None))
    right = _split_offset(getattr(on_node, "right", None))
    if left is None or right is None:
        return []
    left_col, left_offset = left
    right_col, right_offset = right
    # `Lcol + dl <op> Rcol + dr`  ==  `Lcol <op> Rcol + (dr - dl)`.
    offset_terms = list(right_offset) + [(node, -sign) for node, sign in left_offset]
    return [(left_col, right_col, offset_terms, sides[0], sides[1])]


def _representable(bound, target_type) -> bool:
    """True if *bound* can be carried by a literal of *target_type*.

    The bound comes from the OTHER leg of the join, so its width is the other
    column's, not the target's: `p.id = s.id` puts satellites.id's 1..177 onto
    $planets.id, which is an INT8. Typing 177 as INT8 is not a widening
    question — the literal is materialised by
    `vector_int8_from_constant` and dies with a bare OverflowError.

    Dropping an unrepresentable bound is always sound: these are derived
    necessary-condition filters layered on top of the join, so a missing one
    only forgoes pruning. Both out-of-range directions are droppable — a bound
    beyond the target's width is a tautology (every INT8 is <= 177), and one
    below it makes the predicate unsatisfiable, which the join itself still
    enforces.
    """
    bounds = integer_bounds(target_type)
    if bounds is None:  # not an integer width — this check does not apply
        return True
    if not isinstance(bound, (int, float)) or isinstance(bound, bool):
        return True
    return bounds[0] <= bound <= bounds[1]


# The Python value a literal of each category must carry. `build_literal_node`
# TAGS the literal with `suggested_type` but never re-expresses the value, and
# `_materialise_constant_literal` dispatches on the VALUE's Python type for
# floats/Decimals/temporals — so a float bound tagged INT32 materialises a
# FLOAT64 constant. The compare kernel is identical-type only, so that pair
# declines and the native ExprFilter (which has no fallback) hard-fails with
# `err_op=11`. Every bound therefore gets re-expressed here, or dropped.
#
# DATE / TIME / TIMESTAMP are absent on purpose — see `_coerce_temporal_bound`.
# A temporal bound never arrives as a `datetime`: `value_range` holds the
# column's NATIVE ENCODING, an int. Listing `datetime.date` here would describe a
# value this layer cannot receive.
_CATEGORY_VALUE_TYPES = {
    LogicalCategory.BOOLEAN: bool,
    LogicalCategory.INTEGER: int,
    LogicalCategory.FLOAT: float,
    LogicalCategory.DECIMAL: decimal.Decimal,
    LogicalCategory.VARCHAR: str,
    LogicalCategory.NVARCHAR: str,
    LogicalCategory.VARBINARY: bytes,
}


# The native encoding a temporal bound is expressed in, and the physical width a
# literal carrying it must fit. For the units admitted below (and ONLY those --
# see the gate that follows), both inlets of `value_range` agree on this space:
# a manifest bound is the parquet footer's raw int (µs for TIMESTAMP64, days for
# DATE32), and a bound harvested from a predicate literal is the same int, because
# `build_literal_node` has already run `timestamp_to_int64_us` / `date_to_int64_days`
# on it. `build_literal_node` passes an int through UNTOUCHED, so handing the bound
# straight back is an identity, not a conversion — which is precisely what keeps the
# DATE32 hazard (a `datetime` silently converted to microseconds against a column
# counting DAYS, giving a 1970 bound) out of this path: no `datetime` is involved.
_TEMPORAL_NATIVE = {
    LogicalCategory.TIMESTAMP: (DrakenType.TIMESTAMP64, (-(2**63), 2**63 - 1)),
    LogicalCategory.DATE: (DrakenType.DATE32, (-(2**31), 2**31 - 1)),
}

# ⛔ A TIMESTAMP64 is admitted ONLY at MICROSECOND resolution, and that is a
# CORRECTNESS gate, not caution. `predicate_pushdown._temporal_storage_scale_us`
# looks like the general answer here — it maps DATE and every TIMESTAMP unit to a
# µs scale — but it describes what a column STORES, and the two inlets of
# `value_range` do not agree on that for a non-µs timestamp:
#
#   manifest inlet   `_scan_stats` records the parquet footer's raw int, so a
#                    TIMESTAMP[ms] column contributes MILLISECONDS.
#   predicate inlet  `_narrow_filter_columns` records a literal's value, and
#                    `build_literal_node` has already run `timestamp_to_int64_us`
#                    on it — so the SAME column contributes MICROSECONDS.
#
# For TIMESTAMP[us] and DATE the two coincide, which is what makes the identity
# pass-through in `_coerce_temporal_bound` sound. Anywhere else the bound's unit is
# not knowable from its type, and a bound read in the wrong unit is a silently
# displaced window. Do not widen this by reaching for that helper.


def _as_float(bound, target_type, keep_upper):
    """*bound* as a float that is never TIGHTER than *bound* itself.

    `float(2**53 + 1)` rounds DOWN; used as an upper bound that would exclude a
    row the join still matches. Nudge outward (toward the bound's own side of
    the range) until the float is on the safe side. FLOAT32 targets round again
    on materialisation, so the nudge is done in float32 space for those."""
    single = getattr(target_type, "physical", None) is DrakenType.FLOAT32
    try:
        result = float(bound)
    except (OverflowError, ValueError):
        return None  # magnitude has no float — drop, it is only a pruning hint
    if single:
        result = struct.unpack("<f", struct.pack("<f", result))[0]
    if not math.isfinite(result):
        return None
    # At most a couple of iterations: one ULP either side of the true value.
    for _ in range(4):
        if (result >= bound) if keep_upper else (result <= bound):
            return result
        result = math.nextafter(result, math.inf if keep_upper else -math.inf)
        if single:
            result = struct.unpack("<f", struct.pack("<f", result))[0]
    return None


def _coerce_bound(bound, target_type, keep_upper):
    """*bound* re-expressed as the Python value a *target_type* literal must
    carry, or None when it cannot be carried without narrowing the range.

    Bounds come from the OTHER leg of the join, so their type is the other
    column's — an int32 key gets float64 bounds and vice versa. Returning None
    is always sound: these are derived necessary-condition filters layered on
    top of the join, so a dropped bound only forgoes pruning (same reasoning as
    `_representable`).

    No rounding may NARROW the range beyond what the target's value domain
    already implies — a rounded bound must never exclude a row the join would
    have matched."""
    if target_type is None:
        return bound  # untyped target — build_literal_node infers from the value

    category = target_type.category

    if category in _TEMPORAL_NATIVE:
        return _coerce_temporal_bound(bound, target_type, category)

    wanted = _CATEGORY_VALUE_TYPES.get(category)
    if wanted is None:
        # NULL / INTERVAL / TIME / VARIANT / ARRAY / VECTOR — no literal form this
        # layer can build. TIME is in that list by ruling, not by omission: the
        # engine has no working TIME column type (a scan yields raw ints, CAST
        # yields raw ticks, and the operator map has no TIME rows), so a bound
        # pushed onto one would be compared in a space nothing else agrees on.
        return None

    # bool is a subclass of int; a boolean bound is only ever a boolean bound.
    if isinstance(bound, bool) or wanted is bool:
        return bound if (isinstance(bound, bool) and wanted is bool) else None

    if category is LogicalCategory.INTEGER:
        if isinstance(bound, int):
            return bound
        # Over an integer domain `k <= 4.7` and `k <= 4` select the same rows, so
        # truncating TOWARD the range is exact, not narrowing.
        if isinstance(bound, float):
            if not math.isfinite(bound):
                return None
            return math.floor(bound) if keep_upper else math.ceil(bound)
        if isinstance(bound, decimal.Decimal):
            if not bound.is_finite():
                return None
            rounding = decimal.ROUND_FLOOR if keep_upper else decimal.ROUND_CEILING
            return int(bound.to_integral_value(rounding=rounding))
        return None

    if category is LogicalCategory.FLOAT:
        if isinstance(bound, (int, float, decimal.Decimal)):
            return _as_float(bound, target_type, keep_upper)
        return None

    if category is LogicalCategory.DECIMAL:
        if isinstance(bound, decimal.Decimal):
            return bound
        if isinstance(bound, int):
            return decimal.Decimal(bound)
        # A float bound would have to be quantized to the column's declared
        # scale, and the quantize rounds in a direction this layer cannot see.
        return None

    return bound if isinstance(bound, wanted) else None


def _coerce_temporal_bound(bound, target_type, category):
    """A TIMESTAMP / DATE bound re-expressed for a literal of *target_type*.

    The bound is already in the target's native encoding (see
    `_TEMPORAL_NATIVE`), so the work here is verification, not conversion:

    * it must be an ``int`` — a float bound on a temporal column would mean an
      inlet wrote a value `value_range` does not hold, and guessing at its unit
      is exactly the mistake this whole path exists to avoid;
    * a TIMESTAMP64 must be declared in MICROSECONDS. Other units are a real
      possibility in a footer and the literal side has no unit-carrying spelling
      to match them with, so they DECLINE rather than gamble on the tick size;
    * the value must fit the physical width, or `vector_int32_from_constant`
      dies with a bare OverflowError on materialisation.

    Declining only forgoes pruning — the join still enforces the predicate.
    """
    physical, (low, high) = _TEMPORAL_NATIVE[category]
    if type(bound) is not int:
        return None
    if getattr(target_type, "physical", None) is not physical:
        return None
    if category is LogicalCategory.TIMESTAMP:
        logical = getattr(target_type, "logical", None)
        if logical is None or logical.unit is not TimestampUnit.MICROSECONDS:
            return None
    if not low <= bound <= high:
        return None
    return bound


def _resolve_offset(offset_terms, target_type):
    """The signed sum of *offset_terms* in *target_type*'s native unit, or None.

    Returns 0 for the no-offset case, which is every equi-join and every band
    written without a displacement — the overwhelmingly common path, and one
    that reaches the caller's arithmetic as an exact identity.

    Everything else must be EXACT. A shifted bound that is off by any amount in
    the tightening direction silently deletes joined rows, and nothing downstream
    can tell that from a correct answer, so this declines wherever exactness is
    not provable:

    * INTEGER / FLOAT targets take integer terms only. `b.y + 0.5` on a float
      column would make the sum a rounding question whose safe direction depends
      on the bound's side, and the float's own ULP can exceed the offset.
    * TIMESTAMP / DATE targets take INTERVAL terms only, and only the
      ``(0, microseconds)`` ones. A months component is NOT a fixed displacement —
      `interval_apply_to_temporal` applies it as calendar arithmetic with
      end-of-month day clamping, so `+ INTERVAL '1' MONTH` shifts a January bound
      by 31 days and a February one by 28. No single number describes it.
    * A DATE32 target additionally needs the µs component to be a whole number of
      DAYS. `d + INTERVAL '12' HOUR` compares in microsecond space (DATE ±
      INTERVAL yields a TIMESTAMP64, per the kernel), where a half-day offset has
      no exact day-unit spelling; it declines rather than round.
    """
    if not offset_terms:
        return 0
    category = target_type.category if target_type is not None else None
    total = 0
    for node, sign in offset_terms:
        value = node.value
        if category in _TEMPORAL_NATIVE:
            if node.type != INTERVAL or not (isinstance(value, tuple) and len(value) == 2):
                return None
            months, microseconds = value
            if months:
                return None
            if category is LogicalCategory.DATE:
                if microseconds % MICROSECONDS_PER_DAY:
                    return None
                microseconds //= MICROSECONDS_PER_DAY
            total += sign * microseconds
            continue
        if category not in (LogicalCategory.INTEGER, LogicalCategory.FLOAT):
            return None
        if type(value) is not int:
            return None
        total += sign * value
    return total


def _shifted(value_range, offset, keep):
    """*value_range* displaced by *offset*, narrowed to the bounds in *keep*.

    *keep* is "both" / "upper" / "lower": a band comparator establishes only one
    end of the range on each operand, and dropping the other half here is what
    lets `_range_conditions` — which already skips a None bound — emit the
    one-sided filter unchanged.
    """
    lower = value_range.lower_bound if keep in ("both", "lower") else None
    upper = value_range.upper_bound if keep in ("both", "upper") else None
    if offset:
        # Both bounds are ints whenever an offset survived `_resolve_offset`
        # (integer terms for a numeric target, native-unit ints for a temporal
        # one), so the displacement is exact addition, not a rounding step.
        if lower is not None:
            if type(lower) is not int:
                return None
            lower += offset
        if upper is not None:
            if type(upper) is not int:
                return None
            upper += offset
    if lower is None and upper is None:
        return None
    return ColumnRange(lower_bound=lower, upper_bound=upper)


def _column_type(col):
    """The bound ColumnType behind an identifier node, or None."""
    schema_column = getattr(col, "schema_column", None)
    return getattr(schema_column, "column_type", None) if schema_column is not None else None


def _comparable_encoding(source_type, target_type) -> bool:
    """Does a bound read off *source_type* mean the same thing to a literal of
    *target_type*?

    For numbers, yes by construction: a `value_range` bound is a plain number and
    `_coerce_bound` already re-expresses it for the target's width. For temporals
    it is a UNIT question, and getting it wrong is a silently displaced window
    rather than an error — days read as microseconds land every bound in 1970. So
    require the two ColumnTypes to be identical (frozen dataclasses, so `==` is
    structural) instead of inventing a conversion between two encodings this layer
    cannot inspect. A DATE joined against a TIMESTAMP declines and prunes nothing.
    """
    if target_type is None or target_type.category not in _TEMPORAL_NATIVE:
        return True
    return source_type == target_type


def _range_conditions(target_col, value_range):
    """Build GtEq/LtEq COMPARISON_OPERATOR condition Nodes pushing *value_range*
    (native, post-filter bounds) onto *target_col*, correctly typed."""
    target_type = _column_type(target_col)
    conditions = []
    for bound, operator, keep_upper in (
        (value_range.upper_bound, "LtEq", True),
        (value_range.lower_bound, "GtEq", False),
    ):
        if bound is None:
            continue
        bound = _coerce_bound(bound, target_type, keep_upper)
        if bound is None or not _representable(bound, target_type):
            continue
        conditions.append(
            Node(
                NodeType.COMPARISON_OPERATOR,
                value=operator,
                left=target_col,
                right=build_literal_node(bound, suggested_type=target_type),
            )
        )
    return conditions


# Descending past one of these would leave the identity ambiguous: a set operation
# has two branches that can both feed the same output column, so a literal found
# under one of them describes only half the rows reaching the join.
_SET_OPERATIONS = (
    LogicalPlanStepType.Union,
    LogicalPlanStepType.Intersect,
    LogicalPlanStepType.Except,
    LogicalPlanStepType.Difference,
)


def _constant_literal_for(plan, start_nid, identity):
    """The single LITERAL a Project below *start_nid* binds to *identity*, else None.

    An outer join BELOW this point can null-fill the column, so the honest reading
    of what is found here is "this column is that constant or NULL". Both callers
    want it as a necessary condition for an equi-join MATCH, and NULL never matches,
    so the weaker reading is the one that is used and the nulls need no accounting.

    Refuses when two Projects disagree about the identity. Constant identities are
    minted from the value, so agreement is the norm and disagreement would mean the
    identity is not the unique handle this relies on.
    """
    found = []
    seen = set()
    stack = [start_nid]
    while stack:
        nid = stack.pop()
        if nid in seen:
            continue
        seen.add(nid)
        node = plan[nid]
        if node is None:
            continue
        if node.node_type in _SET_OPERATIONS:
            continue
        if node.node_type == LogicalPlanStepType.Project:
            for column in getattr(node, "columns", None) or []:
                if (
                    column.node_type == NodeType.LITERAL
                    and _phys_identity(column) == identity
                ):
                    found.append(column)
        stack.extend(edge[0] for edge in plan.ingoing_edges(nid))
    if not found:
        return None
    first = found[0]
    for other in found[1:]:
        if type(other.value) is not type(first.value) or other.value != first.value:
            return None
    return first


def _constant_condition(target_col, literal):
    """`target_col = <literal>` as a scan predicate, or None when *literal*'s value
    has no EXACT spelling in the target's type.

    Exactness is the whole gate. A range bound may be rounded outward because a
    wider necessary condition only prunes less, but an equality that moves by any
    amount selects a different set of rows, so every category here either carries
    the value unchanged or declines. Declining costs pruning and nothing else --
    the join still enforces the predicate.

    Temporals decline: their literal is a raw int in a unit that is not recoverable
    from the type (the TIMESTAMP-unit hazard documented above `_TEMPORAL_NATIVE`),
    and an equality read in the wrong unit lands in 1970 and returns nothing.
    """
    target_type = _column_type(target_col)
    if target_type is None:
        return None
    category = target_type.category
    value = literal.value
    if getattr(value, "item", None) is not None:
        value = value.item()

    if category in (LogicalCategory.VARCHAR, LogicalCategory.NVARCHAR):
        # The engine spells a VARCHAR predicate literal as UTF-8 BYTES -- that is
        # what the parser emits for `WHERE cve_id = 'x'` -- while a Project holds
        # a folded string literal as `str`. Both reach the same value through
        # `_materialise_constant_literal`, but the manifest/dictionary pruning that
        # is the entire point of this push compares against stored bytes, so the
        # pushed predicate is minted in the canonical spelling rather than a second
        # one that only agrees at execution time.
        if type(value) is str:
            value = value.encode("utf-8")
        elif type(value) is not bytes:
            return None
    elif category is LogicalCategory.VARBINARY:
        if type(value) is not bytes:
            return None
    elif category is LogicalCategory.BOOLEAN:
        if type(value) is not bool:
            return None
    elif category is LogicalCategory.INTEGER:
        # `bool` is an `int` to isinstance; True is not an integer key value.
        if type(value) is not int or not _representable(value, target_type):
            return None
    elif category is LogicalCategory.FLOAT:
        if type(value) not in (int, float):
            return None
        as_float = _as_float(value, target_type, keep_upper=True)
        # `_as_float` nudges to stay a valid BOUND; for an equality the only
        # acceptable outcome is the value itself, unchanged by the round trip.
        if as_float is None or as_float != value:
            return None
        value = as_float
    elif category is LogicalCategory.DECIMAL:
        if not isinstance(value, decimal.Decimal):
            return None
    else:
        return None

    return Node(
        NodeType.COMPARISON_OPERATOR,
        value="Eq",
        left=target_col,
        right=build_literal_node(value, suggested_type=target_type),
    )


# Legs that may RECEIVE a constant derived from the opposite operand. A leg
# qualifies when discarding its non-matching rows early is invisible downstream:
# true for both legs of an inner join, and for the NULL-SUPPLYING leg of an outer
# join, whose unmatched rows contribute exactly the nulls their absence would.
# A PRESERVED leg never qualifies -- its unmatched rows are output rows.
# Semi/anti are absent deliberately: anti-join emits the left rows that found NO
# match, so shrinking either leg changes which rows qualify, and semi is left out
# with it rather than reasoned about in passing for a case nothing needs yet.
_CONSTANT_RECEIVING_LEGS = {
    "inner": ("left", "right"),
    "nested loop": ("left", "right"),
    "left outer": ("right",),
    "left": ("right",),
    "right outer": ("left",),
    "right": ("left",),
}


def _leg_of(join_node, target_col):
    """"left" / "right" for the leg *target_col* comes from, else None."""
    target_relation = getattr(target_col, "source", None)
    if target_relation is None:
        return None
    if target_relation in (join_node.left_relation_names or []):
        return "left"
    if target_relation in (join_node.right_relation_names or []):
        return "right"
    return None


def _predicate_already_present(predicates, condition):
    """True if *predicates* already contains an equivalent (op, column, literal)."""
    op = getattr(condition, "value", None)
    col = getattr(getattr(condition, "left", None), "value", None)
    lit = getattr(getattr(condition, "right", None), "value", None)
    for existing in predicates:
        if (
            getattr(existing, "value", None) == op
            and getattr(getattr(existing, "left", None), "value", None) == col
            and getattr(getattr(existing, "right", None), "value", None) == lit
        ):
            return True
    return False


class CorrelatedFiltersStrategy(OptimizationStrategy):
    # Cost-typed so the driver propagates statistics (refresh_statistics) before
    # this runs; requires predicates already pushed onto scans so those ranges
    # show up in the propagated key statistics.
    optimization_technique = "cost"
    requires = ("predicates-pushed",)

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if node.node_type != LogicalPlanStepType.Join:
            return context

        correlations = _correlated_join_predicates(node.on)
        if not correlations:
            return context

        ranges_eligible = (
            node.type in ("inner", "nested loop") and getattr(node, "statistics", None) is not None
        )
        constants_eligible = bool(_CONSTANT_RECEIVING_LEGS.get(node.type))
        if not ranges_eligible and not constants_eligible:
            return context

        uuid_to_nid = {}
        for nid in list(context.optimized_plan.nodes()):
            plan_node = context.optimized_plan[nid]
            node_uuid = getattr(plan_node, "uuid", None) if plan_node is not None else None
            if node_uuid:
                uuid_to_nid[node_uuid] = nid

        if ranges_eligible:
            for (
                left_col,
                right_col,
                offset_terms,
                left_bounds,
                right_bounds,
            ) in correlations:
                # The predicate reads `left_col <op> right_col + delta`. The
                # right's realized range shifted FORWARD by delta bounds the left;
                # the left's range shifted BACK by delta bounds the right.
                self._transport(
                    context, node, left_col, right_col, offset_terms, left_bounds, 1, uuid_to_nid
                )
                self._transport(
                    context, node, right_col, left_col, offset_terms, right_bounds, -1, uuid_to_nid
                )

        if constants_eligible:
            self._propagate_constants(context, node, correlations, uuid_to_nid)
        return context

    def _propagate_constants(self, context, join_node, correlations, uuid_to_nid):
        """Push `target = <literal>` for equi-join operands whose partner is a
        statically known constant."""
        receivable = _CONSTANT_RECEIVING_LEGS[join_node.type]
        join_nid = uuid_to_nid.get(getattr(join_node, "uuid", None))
        if join_nid is None:
            return

        for left_col, right_col, offset_terms, left_bounds, _ in correlations:
            # `_TRANSPORTED_BOUNDS` gives "both" to Eq alone, so this is the
            # equality test; a displaced key is a different value from the constant.
            if offset_terms or left_bounds != "both":
                continue
            for target_col, source_col in ((left_col, right_col), (right_col, left_col)):
                leg = _leg_of(join_node, target_col)
                if leg not in receivable:
                    continue
                source_identity = _phys_identity(source_col)
                if source_identity is None:
                    continue
                literal = _constant_literal_for(
                    context.optimized_plan, join_nid, source_identity
                )
                if literal is None:
                    continue
                condition = _constant_condition(target_col, literal)
                if condition is None:
                    continue
                self._push_conditions(
                    context, join_node, target_col, [condition], uuid_to_nid,
                    telemetry_reading="optimization_join_constant_propagation",
                )

    def _transport(
        self,
        context,
        join_node,
        target_col,
        source_col,
        offset_terms,
        keep,
        direction,
        uuid_to_nid,
    ):
        """Carry *source_col*'s realized range onto *target_col*, displaced by the
        predicate's offset and narrowed to the bounds *keep* names."""
        source_range = _key_value_range(getattr(join_node, "statistics", None), source_col)
        if source_range is None:
            return
        target_type = _column_type(target_col)
        if not _comparable_encoding(_column_type(source_col), target_type):
            return
        offset = _resolve_offset(offset_terms, target_type)
        if offset is None:
            return
        shifted = _shifted(source_range, direction * offset, keep)
        if shifted is None:
            return
        self._push_range(context, join_node, target_col, shifted, uuid_to_nid)

    def _push_range(self, context, join_node, target_col, value_range, uuid_to_nid):
        """Push *value_range* onto *target_col*'s scan(s): append to the scan's
        predicate list when the connector supports it, else add a Filter node."""
        conditions = _range_conditions(target_col, value_range)
        if not conditions:
            return
        self._push_conditions(
            context,
            join_node,
            target_col,
            conditions,
            uuid_to_nid,
            # REDUNDANCY GUARD: compare against the SCAN's own range, not the
            # join's. _intersect_join_keys has already replaced both keys'
            # ranges on the join node with their intersection, so at that level
            # every pair looks identical and nothing would ever push.
            skip_scan=lambda scan: not _tightens(
                value_range, _key_value_range(getattr(scan, "statistics", None), target_col)
            ),
        )

    def _push_conditions(
        self,
        context,
        join_node,
        target_col,
        conditions,
        uuid_to_nid,
        skip_scan=None,
        telemetry_reading="optimization_inner_join_correlated_filter",
    ):
        """Append *conditions* to the scan(s) producing *target_col*: onto the
        scan's predicate list when the connector supports pushdown, else as a
        Filter node above it.

        The guards below are shared by both transports because they are about the
        TARGET -- which scan really produces this column -- and not about how the
        condition was derived.
        """
        leg = _leg_of(join_node, target_col)
        if leg is None:
            return
        target_relation = getattr(target_col, "source", None)
        readers = (join_node.left_readers if leg == "left" else join_node.right_readers) or []

        for reader_uuid in readers:
            reader_nid = uuid_to_nid.get(reader_uuid)
            if reader_nid is None:
                continue
            scan = context.optimized_plan[reader_nid]
            if scan is None:
                continue

            # AVAILABILITY GUARD: a leg's relation names include DERIVED relations
            # (a CROSS JOIN UNNEST contributes a synthetic `$unnest-*` schema), but
            # its readers are only the base scans. Pushing a range on a derived
            # column onto a base scan attaches a predicate to a relation that does
            # not produce it — the scan's predicate resolver then dies with a
            # KeyError on the unresolvable identity. Only push onto the reader that
            # IS the target column's relation.
            scan_names = {getattr(scan, "alias", None), getattr(scan, "relation", None)}
            if target_relation not in scan_names:
                continue

            # IDENTITY GUARD: the alias check above is necessary but not
            # sufficient -- a materialised join key (e.g. an equi-join hoisted
            # off an arithmetic operand by cross_join_filter_pushdown, which
            # keeps the leg's own relation name as `.source` so
            # extract_join_fields still matches it) carries the SAME `.source`
            # as its underlying scan while its actual identity is only
            # produced by a Project sitting ABOVE that scan. Pushing a
            # predicate keyed by that identity straight onto the scan's own
            # `.predicates` asks the reader for a column it never emits --
            # the same class of failure the comment above already describes,
            # just not reachable through a relation-name mismatch. Scan
            # output is the ground truth of what a scan can filter on.
            target_identity = _phys_identity(target_col)
            scan_schema = getattr(scan, "schema", None)
            scan_identities = (
                {c.identity for c in scan_schema.columns} if scan_schema is not None else set()
            )
            if target_identity is None or target_identity not in scan_identities:
                continue

            if skip_scan is not None and skip_scan(scan):
                continue

            connector = getattr(scan, "connector", None)
            if connector is not None and getattr(connector, "supports_predicate_pushdown", False):
                if not scan.predicates:
                    scan.predicates = []
                for condition in conditions:
                    if not _predicate_already_present(scan.predicates, condition):
                        scan.predicates.append(condition)
                        self.telemetry.increase(telemetry_reading)
            else:
                # Fallback for non-pushdown connectors: a Filter node still
                # filters at execution, just without row-group pruning.
                for condition in conditions:
                    filter_node = LogicalPlanNode(
                        node_type=LogicalPlanStepType.Filter,
                        condition=condition,
                        columns=[target_col],
                        relations={target_relation},
                        all_relations={target_relation},
                    )
                    context.optimized_plan.insert_node_after(
                        random_string(), filter_node, reader_nid
                    )
                    self.telemetry.increase(telemetry_reading)

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # This strategy mutates scan predicates / adds Filter nodes, so the
        # statistics propagated before it are now stale; flag them so the next
        # cost strategy refreshes. (Cost strategies don't get the heuristic
        # auto-invalidation from the driver.)
        plan.statistics_are_stale = True
        return plan

    def should_i_run(self, plan):
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Join,))
        return len(candidates) > 0
