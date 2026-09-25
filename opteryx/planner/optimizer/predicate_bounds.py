# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Predicate Bound Derivation
==========================

The bounds pruners — ``Manifest.prune_files`` (file grain) and
``Manifest.ordinal_zone_map_terms`` (row-group grain) — read exactly ONE
predicate shape: ``column <op> literal``. Every other shape a user writes is
invisible to them and prunes nothing: ``IN``, ``LIKE 'abc%'``, and every
predicate that wraps the column in a transform (``price * 1.2 > 100``,
``EXTRACT(YEAR FROM ts) = 2024``, ``LEFT(name, 3) = 'abc'``).

This module derives, from those shapes, an interval on the RAW STORED COLUMN
that provably contains every row the predicate can match, and emits it in the
one shape the pruners already read. Both pruners then gain every shape here at
once, and neither learns any function semantics.

WHAT IS AND IS NOT DERIVED
--------------------------
Evidence only. Nothing here rewrites the plan, and the original predicate still
runs unchanged over every row of every surviving file. A derived interval may
therefore be WIDER than the true pre-image (costing a read that could have been
skipped) but must never be NARROWER (which would drop rows). Every rule below is
written to that asymmetry, and the "decline" branches are the load-bearing half:
a shape we cannot bound contributes nothing, which is "no information", never
"false".

Two properties make the derivation sound:

* MONOTONICITY. For a monotone non-decreasing ``f``, ``f(x) ∈ [a, b]`` confines
  ``x`` to an interval, so a bound on ``f(col)`` IS a bound on ``col``. This is
  the same reasoning ``rewrite_date_trunc_to_range`` already applies to
  ``TRUNC(ts, unit)`` — that one rewrites the plan because TRUNC's pre-image is
  EXACT; the rules here mostly are not, which is why they stay evidence.
* STRICTNESS. Every transform admitted here is NULL-in/NULL-out, so a NULL row
  can never satisfy the predicate and never needs to be inside the derived
  interval.

Deliberately NOT here:

* ``LOWER``/``UPPER``. Case folding is not order-preserving — every uppercase
  byte sorts below every lowercase one — so the sound interval for
  ``LOWER(col) = 'opteryx'`` is ``['OPTERYX', 'opteryx']``, roughly half the
  printable key space. Sound and worthless. (The satisfiable-literal test for
  those lives in ``constant_folding``; the per-file char-class identity test
  lives in ``Manifest``, which is the only place the evidence exists.)
* ``OR`` across DIFFERENT columns. A row need satisfy only one arm, so no single
  column is constrained. Same-column ORs ARE hulled, below.
* ``NotEq`` / ``NOT IN`` / ``ABS(col) > v``. Their pre-images are complements of
  intervals, which the bounds pruners cannot express.
* Division. ``/`` on two integers is not pinned down here as true or floor
  division, and the two have different pre-images. Declined rather than guessed.
"""

import datetime
import math
from typing import Any
from typing import Callable
from typing import List
from typing import Optional
from typing import Tuple

from opteryx.compiled.structures.node import Node
from opteryx.expression import NodeType
from opteryx.planner import build_literal_node
from opteryx.types.logical_type import LogicalCategory
from opteryx.compiled.structures.expressions import Comparison
from opteryx.compiled.structures.expressions import Literal

# (lower, lower_is_closed, upper, upper_is_closed); a None bound is unbounded on
# that side and its flag is meaningless. Closed/open is tracked rather than
# widened away so `col > 5` does not silently become `col >= 5` and lose the
# file whose bounds are exactly [5, 5].
Interval = Tuple[Optional[Any], bool, Optional[Any], bool]

_UNBOUNDED: Interval = (None, True, None, True)

# Comparison -> the interval it puts the LEFT operand in.
_OP_TO_INTERVAL = {
    "Eq": lambda v: (v, True, v, True),
    "Gt": lambda v: (v, False, None, True),
    "GtEq": lambda v: (v, True, None, True),
    "Lt": lambda v: (None, True, v, False),
    "LtEq": lambda v: (None, True, v, True),
}

# Operator flip for the literal-on-the-left spelling. PredicateRewriteStrategy
# normalises most of these, but this module is also reached from `prune_files`,
# which is handed predicates from sources that strategy never rewrote.
_FLIP = {"Eq": "Eq", "Gt": "Lt", "GtEq": "LtEq", "Lt": "Gt", "LtEq": "GtEq"}


def _unwrap(node):
    """Strip NESTED wrappers — `(col + 1) > 5` parses with one around the sum."""
    while node is not None and node.node_type == NodeType.NESTED:
        node = node.centre
    return node


def _scalar(value):
    """Python value of a literal, unwrapping 0-d scalar wrappers the same way
    `prune_files` does before it compares against a bound."""
    item = getattr(value, "item", None)
    return value.item() if item is not None else value


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _column_type_of(identifier, column_type_for):
    """ColumnType for an IDENTIFIER node.

    The callback (the manifest's live schema) is tried first and the node's own
    bound `schema_column` second — projection pushdown may have pruned the
    column out of that schema by the time this runs, and the node carries the
    type it was bound with regardless.
    """
    resolved = None
    if column_type_for is not None and identifier.source_column is not None:
        resolved = column_type_for(identifier.source_column)
    if resolved is not None:
        return resolved
    schema_column = getattr(identifier, "schema_column", None)
    return getattr(schema_column, "column_type", None)


# ---------------------------------------------------------------------------
# Emission — interval on a column back into `column <op> literal` nodes
# ---------------------------------------------------------------------------


def _comparison_node(identifier, operator: str, value, literal_type) -> Node:
    """A fully-bound `identifier <operator> literal` node.

    Fully bound is not optional: `prune_files` reads `left.source_column` to
    resolve the field id and `right.type` to run the temporal-domain guard, so a
    half-built node either prunes nothing or — worse — prunes against the wrong
    column's bounds.
    """
    return Comparison(
        value=operator,
        left=identifier,
        right=build_literal_node(value, suggested_type=literal_type),
    )


def _emit(identifier, interval: Interval, literal_type) -> List[Node]:
    """Canonical conjuncts for `identifier`'s value lying in `interval`."""
    lower, lower_closed, upper, upper_closed = interval

    if lower is None and upper is None:
        return []

    # A closed point interval is an equality, and equality is worth more than
    # two bounds: it is the one operator the min-k membership sketch can
    # eliminate a file on outright (`_membership_keep_masks`).
    if (
        lower is not None
        and upper is not None
        and lower_closed
        and upper_closed
        and lower == upper
    ):
        return [_comparison_node(identifier, "Eq", lower, literal_type)]

    emitted = []
    if lower is not None:
        emitted.append(
            _comparison_node(identifier, "GtEq" if lower_closed else "Gt", lower, literal_type)
        )
    if upper is not None:
        emitted.append(
            _comparison_node(identifier, "LtEq" if upper_closed else "Lt", upper, literal_type)
        )
    return emitted


def _hull(intervals: List[Interval]) -> Interval:
    """Tightest single interval containing every one of `intervals`.

    Used for the arms of a same-column OR and for the members of an IN list. A
    disjunction is only as constrained as its loosest arm, so ONE arm unbounded
    below leaves the hull unbounded below (and likewise above) — the case the
    rules above rely on when an arm is `col > 5` and another is `col < 3`.

    Callers guarantee the bounds are mutually order-comparable; a mixed-type IN
    list is refused before it reaches here rather than raising out of `min`.
    """
    if not intervals:
        return _UNBOUNDED

    lower = None
    lower_closed = True
    upper = None
    upper_closed = True

    if all(arm[0] is not None for arm in intervals):
        lower = min(arm[0] for arm in intervals)
        # Closed wins a tie: [5, ..] ∪ (5, ..] is [5, ..].
        lower_closed = any(arm[1] for arm in intervals if arm[0] == lower)
    if all(arm[2] is not None for arm in intervals):
        upper = max(arm[2] for arm in intervals)
        upper_closed = any(arm[3] for arm in intervals if arm[2] == upper)

    return (lower, lower_closed, upper, upper_closed)


# ---------------------------------------------------------------------------
# Safe arithmetic — every quotient WIDENS, never narrows
# ---------------------------------------------------------------------------
#
# A derived bound that is one ulp too tight drops rows. Integer operands use
# exact integer floor/ceil division (Python ints are arbitrary precision, so no
# rounding exists to get wrong); anything touching a float is nudged one ulp
# outward afterwards, because float division of two large values can land on
# the wrong side of the exact quotient.


# Above this magnitude an integer no longer survives a round trip through a
# float, so the float path below would introduce error instead of removing it.
_EXACT_FLOAT_LIMIT = 1 << 53


def _exactly_representable(numerator, divisor) -> bool:
    return abs(numerator) < _EXACT_FLOAT_LIMIT and abs(divisor) < _EXACT_FLOAT_LIMIT


def _floor_div(numerator, divisor):
    """Largest value <= numerator/divisor — a SAFE lower bound.

    Three arms, in decreasing order of tightness: an exact integer quotient; a
    float nudged one ulp DOWN, which is tight enough to keep `FLOOR(col*2) = 7`
    at 3.5 rather than 3; and, once the operands are too large for a float to
    represent, integer floor division — looser, but exact arithmetic rather than
    a quotient that could land on the wrong side.
    """
    if isinstance(numerator, int) and isinstance(divisor, int):
        if numerator % divisor == 0:
            return numerator // divisor
        if not _exactly_representable(numerator, divisor):
            return numerator // divisor  # floors toward -inf for either sign
    return math.nextafter(numerator / divisor, float("-inf"))


def _ceil_div(numerator, divisor):
    """Smallest value >= numerator/divisor — a SAFE upper bound. Mirrors
    `_floor_div`; `-((-n) // d)` is integer ceiling for either sign of d."""
    if isinstance(numerator, int) and isinstance(divisor, int):
        if numerator % divisor == 0:
            return numerator // divisor
        if not _exactly_representable(numerator, divisor):
            return -((-numerator) // divisor)
    return math.nextafter(numerator / divisor, float("inf"))


def _floor_int(value):
    return value if isinstance(value, int) else math.floor(value)


def _ceil_int(value):
    return value if isinstance(value, int) else math.ceil(value)


def _affine_preimage(interval: Interval, multiplier, addend) -> Optional[Interval]:
    """Interval on `x`, given `interval` holds for ``y = multiplier * x + addend``.

    Both bounds move OUTWARD (`_floor_div` / `_ceil_div`), so the result is a
    superset of the exact pre-image. A negative multiplier reverses the axis, so
    the two ends — values and closed/open flags together — swap.
    """
    if multiplier == 0:
        return None

    lower, lower_closed, upper, upper_closed = interval

    low_value = None if lower is None else lower - addend
    high_value = None if upper is None else upper - addend

    if multiplier > 0:
        new_lower = None if low_value is None else _floor_div(low_value, multiplier)
        new_upper = None if high_value is None else _ceil_div(high_value, multiplier)
        return (new_lower, lower_closed, new_upper, upper_closed)

    # y = -k*x + c is DECREASING in x: the bound on y's low end constrains x's
    # high end and vice versa.
    new_upper = None if low_value is None else _ceil_div(low_value, multiplier)
    new_lower = None if high_value is None else _floor_div(high_value, multiplier)
    return (new_lower, upper_closed, new_upper, lower_closed)


# ---------------------------------------------------------------------------
# Pre-image of a transform
# ---------------------------------------------------------------------------


def _preimage(expr, interval: Interval, column_type_for):
    """``(identifier, interval)`` — the interval `expr`'s own argument column must
    lie in for `expr` to land inside `interval`, or None when this expression is
    not one we can bound.

    Recursive, so composition works without a rule per combination:
    ``FLOOR(price * 2) = 7`` folds through the FLOOR rule and then the Multiply
    rule onto ``price``.
    """
    expr = _unwrap(expr)
    if expr is None:
        return None

    if expr.node_type == NodeType.IDENTIFIER:
        return expr, interval

    if expr.node_type == NodeType.BINARY_OPERATOR:
        folded = _preimage_arithmetic(expr, interval)
        if folded is None:
            return None
        inner, inner_interval = folded
        return _preimage(inner, inner_interval, column_type_for)

    if expr.node_type == NodeType.FUNCTION:
        folded = _preimage_function(expr, interval, column_type_for)
        if folded is None:
            return None
        inner, inner_interval = folded
        return _preimage(inner, inner_interval, column_type_for)

    return None


def _literal_operand(left, right):
    """``(expression, literal_value, literal_is_on_the_left)`` for a binary node
    with exactly one numeric-literal side, else None."""
    left = _unwrap(left)
    right = _unwrap(right)
    if left is None or right is None:
        return None
    if right.node_type == NodeType.LITERAL and left.node_type != NodeType.LITERAL:
        value = _scalar(right.value)
        return (left, value, False) if _is_number(value) else None
    if left.node_type == NodeType.LITERAL and right.node_type != NodeType.LITERAL:
        value = _scalar(left.value)
        return (right, value, True) if _is_number(value) else None
    return None


def _preimage_arithmetic(expr, interval: Interval):
    """Pre-image through `+`, `-` and `*` by a constant — all strictly monotone.

    `/` is NOT admitted: integer division and true division have different
    pre-images and which one `Divide` means is not settled here. `%` is not
    monotone at all.
    """
    operator = expr.value
    if operator not in ("Plus", "Minus", "Multiply"):
        return None

    operands = _literal_operand(expr.left, expr.right)
    if operands is None:
        return None
    inner, constant, literal_on_left = operands

    # Interval bounds must be numbers to do arithmetic on them at all — a
    # string bound reaching here means the predicate compared a sum against a
    # string, which the binder would have refused.
    for bound in (interval[0], interval[2]):
        if bound is not None and not _is_number(bound):
            return None

    if operator == "Plus":
        folded = _affine_preimage(interval, 1, constant)
    elif operator == "Minus":
        # `k - col` reverses the axis; `col - k` shifts it.
        folded = (
            _affine_preimage(interval, -1, constant)
            if literal_on_left
            else _affine_preimage(interval, 1, -constant)
        )
    else:
        folded = _affine_preimage(interval, constant, 0)

    return None if folded is None else (inner, folded)


def _literal_value(node):
    """Python value of `node` when it is a LITERAL, else `_NOT_A_LITERAL`."""
    node = _unwrap(node)
    if node is None or node.node_type != NodeType.LITERAL:
        return _NOT_A_LITERAL
    return _scalar(node.value)


class _NotALiteral:
    """Distinct from None, which is a legal literal value (SQL NULL)."""


_NOT_A_LITERAL = _NotALiteral()


def _widen(interval: Interval, margin) -> Interval:
    """`interval` grown by `margin` on each bounded side, and closed.

    The blunt instrument for a transform that moves a value by a KNOWN maximum
    amount but whose exact rounding rule this module declines to depend on
    (ROUND's half-even-vs-half-away, TRUNC's toward-zero at the sign change).
    Costs at most the files straddling the widened edge; removes a whole class
    of off-by-one wrong answers.
    """
    lower, _, upper, _ = interval
    return (
        None if lower is None else lower - margin,
        True,
        None if upper is None else upper + margin,
        True,
    )


def _floor_preimage(interval: Interval) -> Interval:
    """Pre-image of `interval` under ``y = FLOOR(x)``.

    Exact, not widened, because FLOOR has no convention to get wrong:
    ``FLOOR(x) >= a  <=>  x >= ceil(a)``      ``FLOOR(x) >  a  <=>  x >= floor(a)+1``
    ``FLOOR(x) <= b  <=>  x <  floor(b)+1``   ``FLOOR(x) <  b  <=>  x <  ceil(b)``
    """
    lower, lower_closed, upper, upper_closed = interval
    new_lower = None
    if lower is not None:
        new_lower = _ceil_int(lower) if lower_closed else _floor_int(lower) + 1
    new_upper = None
    if upper is not None:
        new_upper = _floor_int(upper) + 1 if upper_closed else _ceil_int(upper)
    # Both upper forms are EXCLUSIVE; both lower forms are inclusive.
    return (new_lower, True, new_upper, False)


def _ceiling_preimage(interval: Interval) -> Interval:
    """Pre-image of `interval` under ``y = CEILING(x)``.

    ``CEIL(x) >= a  <=>  x >  ceil(a)-1``    ``CEIL(x) >  a  <=>  x >  floor(a)``
    ``CEIL(x) <= b  <=>  x <= floor(b)``     ``CEIL(x) <  b  <=>  x <= ceil(b)-1``
    """
    lower, lower_closed, upper, upper_closed = interval
    new_lower = None
    if lower is not None:
        new_lower = _ceil_int(lower) - 1 if lower_closed else _floor_int(lower)
    new_upper = None
    if upper is not None:
        new_upper = _floor_int(upper) if upper_closed else _ceil_int(upper) - 1
    # Both lower forms are EXCLUSIVE; both upper forms are inclusive.
    return (new_lower, False, new_upper, True)


def _abs_preimage(interval: Interval) -> Optional[Interval]:
    """Pre-image of `interval` under ``y = ABS(x)``.

    Only the UPPER bound carries information: ``|x| <= b`` confines x to
    ``[-b, b]``, while ``|x| >= a`` excludes a hole in the MIDDLE of the axis —
    the complement of an interval, which the bounds pruners cannot express. So
    an interval with no upper bound (``ABS(col) > 5``) is declined outright
    rather than answered with something looser than the truth.
    """
    _, _, upper, upper_closed = interval
    if upper is None or not _is_number(upper):
        return None
    if upper < 0:
        # Unsatisfiable (no absolute value is negative). Declining leaves the
        # file in; folding it to FALSE is constant folding's job, not ours.
        return None
    return (-upper, upper_closed, upper, upper_closed)


def _sign_preimage(interval: Interval) -> Optional[Interval]:
    """Pre-image of `interval` under ``y = SIGN(x)``, whose range is {-1, 0, 1}.

    Reasoned over the three points rather than by formula: whichever of them the
    interval admits decides how far the pre-image reaches, and -1 or 1 being
    admitted makes that side unbounded.
    """
    lower, lower_closed, upper, upper_closed = interval

    def _admits(point) -> bool:
        if lower is not None:
            if point < lower or (point == lower and not lower_closed):
                return False
        if upper is not None:
            if point > upper or (point == upper and not upper_closed):
                return False
        return True

    negative, zero, positive = _admits(-1), _admits(0), _admits(1)
    if not (negative or zero or positive):
        return None  # unsatisfiable; see _abs_preimage on why we decline

    if negative:
        new_lower, new_lower_closed = None, True
    elif zero:
        new_lower, new_lower_closed = 0, True
    else:
        new_lower, new_lower_closed = 0, False

    if positive:
        new_upper, new_upper_closed = None, True
    elif zero:
        new_upper, new_upper_closed = 0, True
    else:
        new_upper, new_upper_closed = 0, False

    return (new_lower, new_lower_closed, new_upper, new_upper_closed)


# ---------------------------------------------------------------------------
# String prefixes
# ---------------------------------------------------------------------------


def _is_ascii_text(value) -> bool:
    """True when `value` is a `str` or `bytes` holding only 0x00-0x7F.

    ASCII-only on purpose. The rules below need the byte SUCCESSOR of a string,
    and incrementing the last byte of arbitrary UTF-8 can produce a sequence
    that is not valid UTF-8 at all (a continuation byte rolling past 0xBF).
    Restricting to ASCII also makes character slicing and byte slicing the same
    operation, which is what lets one rule serve VARCHAR (bytes) and NVARCHAR
    (codepoints) alike.
    """
    if isinstance(value, (bytes, bytearray)):
        return all(byte < 0x80 for byte in value)
    if isinstance(value, str):
        return all(ord(char) < 0x80 for char in value)
    return False


def _decode_ascii(value) -> Optional[str]:
    """`value` as an ASCII `str`, for rules that must SCAN it (a LIKE pattern).
    Bounds themselves are never re-typed this way — see `_text_successor`."""
    if not _is_ascii_text(value):
        return None
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("ascii")
    return value


def _text_successor(value):
    """Smallest string strictly greater than every string starting with `value`,
    IN `value`'S OWN PYTHON TYPE — `bytes` in, `bytes` out.

    Type preservation is not cosmetic. A VARCHAR literal reaches the planner as
    `bytes` (the draken string edge is bytes-only), so a rule that derived a
    `str` upper bound against a `bytes` lower bound would produce a pair that is
    not mutually order-comparable, and the whole derivation would be discarded
    as un-emittable — silently, and only in real plans, never in a test that
    built its literals by hand.

    None when there is no such string (empty, or all 0x7F), in which case the
    caller simply emits no upper bound.
    """
    if not _is_ascii_text(value):
        return None
    as_bytes = bytearray(value.encode("ascii") if isinstance(value, str) else bytes(value))
    while as_bytes and as_bytes[-1] >= 0x7F:
        as_bytes.pop()
    if not as_bytes:
        return None
    as_bytes[-1] += 1
    return as_bytes.decode("ascii") if isinstance(value, str) else bytes(as_bytes)


def _prefix_interval(prefix) -> Optional[Interval]:
    """``[prefix, successor(prefix))`` — every string starting with `prefix`."""
    if not isinstance(prefix, (str, bytes, bytearray)) or not prefix:
        return None
    successor = _text_successor(prefix)
    if successor is None:
        return (prefix, True, None, True)
    return (prefix, True, successor, False)


def _truncating_preimage(interval: Interval, width: int) -> Optional[Interval]:
    """Pre-image of `interval` under a left-truncation ``y = x[:width]``.

    Truncation is monotone non-decreasing in byte order, AND every string sorts
    at or above its own prefix. Those two facts give both ends:

    * ``x[:n] >= a``  =>  ``x >= x[:n] >= a``               (the bound passes through)
    * ``x[:n] <= b``  =>  ``x[:n] <= b[:n]``  =>  ``x < successor(b[:n])``

    The second step is why `b` is truncated first: with n=2 and b='abc', no
    2-character prefix can sit between 'ab' and 'abc', so the real constraint is
    ``x[:2] <= 'ab'`` and the bound is ``successor('ab') = 'ac'`` — using
    ``successor('abc')`` would have let 'abz' through unnecessarily.
    """
    lower, lower_closed, upper, upper_closed = interval

    if lower is not None and not isinstance(lower, (str, bytes, bytearray)):
        return None
    if upper is not None and not isinstance(upper, (str, bytes, bytearray)):
        return None

    new_upper = None
    new_upper_closed = True
    if upper is not None:
        successor = _text_successor(upper[:width])
        if successor is not None:
            new_upper, new_upper_closed = successor, False

    if lower is None and new_upper is None:
        return None
    return (lower, lower_closed, new_upper, new_upper_closed)


# ---------------------------------------------------------------------------
# Temporal
# ---------------------------------------------------------------------------

_EPOCH = datetime.datetime(1970, 1, 1)
# Indexed by LogicalType.unit.value, the same table predicate_rewriter uses.
_TICKS_PER_SECOND = (1, 10**3, 10**6, 10**9)
_TEMPORAL_CATEGORIES = (LogicalCategory.DATE, LogicalCategory.TIMESTAMP)


def _ticks_per_second(column_type) -> Optional[int]:
    """Ticks per second for a TIMESTAMP column; None for anything else."""
    if column_type is None or column_type.category != LogicalCategory.TIMESTAMP:
        return None
    unit = column_type.logical.unit.value if column_type.logical is not None else 2
    return _TICKS_PER_SECOND[unit]


def _raw_to_datetime(value, column_type) -> Optional[datetime.datetime]:
    """A raw stored temporal integer back into a `datetime`, for the rules that
    need calendar arithmetic. Only ever used to compute a bound that is then
    WIDENED, so the sub-microsecond truncation a nanosecond column takes here
    cannot tighten anything."""
    if not isinstance(value, int) or isinstance(value, bool):
        return None
    if column_type is None:
        return None
    if column_type.category == LogicalCategory.DATE:
        # Range-checked BEFORE constructing: `timedelta` raises OverflowError
        # well before year 9999, and an optimisation must never raise out of the
        # optimizer. Kept clear of both ends so the caller's own unit arithmetic
        # has room to land.
        if not -700_000 < value < 2_900_000:
            return None
        return _EPOCH + datetime.timedelta(days=value)
    ticks = _ticks_per_second(column_type)
    if ticks is None:
        return None
    microseconds = (value * 10**6) // ticks
    if not -60_000_000_000_000_000 < microseconds < 250_000_000_000_000_000:
        return None
    return _EPOCH + datetime.timedelta(microseconds=microseconds)


def _datetime_to_raw(moment: datetime.datetime, column_type):
    """A `datetime` into the raw integer the column stores, via the ONE function
    that already decides this (`predicate_rewriter._canonical_temporal_literal_value`).
    A second copy of the unit arithmetic here would be a second dialect."""
    from opteryx.planner.optimizer.strategies.predicate_rewriter import (
        _canonical_temporal_literal_value,
    )

    return _canonical_temporal_literal_value(moment, column_type)


def _year_preimage(interval: Interval, column_type) -> Optional[Interval]:
    """Pre-image of a YEAR-NUMBER interval as raw stored temporal values.

    EXTRACT(YEAR ...) is a floor onto the calendar year, so the year-number
    interval is derived by exactly the FLOOR rule and then each endpoint is
    mapped to that year's first instant. Only YEAR: MONTH, DAY, HOUR and the
    rest are cyclic, and a cyclic function's pre-image is a union of intervals,
    not one.
    """
    year_lower, _, year_upper, _ = _floor_preimage(interval)

    def _start_of_year(year):
        if not isinstance(year, int) or isinstance(year, bool):
            return None
        if year < 1 or year > 9999:
            return None
        return _datetime_to_raw(datetime.datetime(year, 1, 1), column_type)

    new_lower = None if year_lower is None else _start_of_year(year_lower)
    if year_lower is not None and new_lower is None:
        return None
    new_upper = None if year_upper is None else _start_of_year(year_upper)
    if year_upper is not None and new_upper is None:
        return None
    if new_lower is None and new_upper is None:
        return None
    # `_floor_preimage` hands back an inclusive low and an EXCLUSIVE high, and
    # the year-start mapping preserves both.
    return (new_lower, True, new_upper, False)


def _numeric_interval(interval: Interval) -> bool:
    return all(bound is None or _is_number(bound) for bound in (interval[0], interval[2]))


def _scale_argument(parameters, position: int):
    """The scale argument at `position`, defaulting to 0 when absent.

    Returns None when it is present but not a non-negative integer literal — a
    negative scale on ROUND/TRUNC moves a value by up to 10**|scale|, which the
    fixed ±1 widening below does NOT cover, and a non-literal scale is not known
    at plan time at all.
    """
    if len(parameters) <= position:
        return 0
    value = _literal_value(parameters[position])
    if isinstance(value, _NotALiteral) or not isinstance(value, int) or isinstance(value, bool):
        return None
    return value if value >= 0 else None


def _preimage_function(expr, interval: Interval, column_type_for):
    """Pre-image through one function call. See the module docstring for the
    admission rule: monotone (or boundable) with a pre-image that is ONE
    interval, and widened wherever the exact rule is not worth depending on."""
    name = expr.value
    parameters = list(expr.parameters or [])
    if not parameters:
        return None

    if name in ("FLOOR", "CEILING"):
        # Both are always built with an explicit scale (logical_planner_builders'
        # `floor`/`ceiling`), defaulting to 0. A non-zero scale changes the step
        # width, which these exact rules do not model.
        if _scale_argument(parameters, 1) != 0 or not _numeric_interval(interval):
            return None
        folded = _floor_preimage(interval) if name == "FLOOR" else _ceiling_preimage(interval)
        return parameters[0], folded

    if name in ("ROUND", "TRUNC"):
        if name == "TRUNC" and len(parameters) == 2:
            unit = _literal_value(parameters[1])
            if isinstance(unit, str):
                # The TEMPORAL overload. Not ours: `rewrite_date_trunc_to_range`
                # already turns it into an EXACT range on the raw column, in the
                # plan, before pruning ever sees it. Deriving a second, looser
                # bound here would be a second dialect for one function.
                return None
        if _scale_argument(parameters, 1) is None or not _numeric_interval(interval):
            return None
        # Widened by a whole unit rather than the exact half-step: ROUND's
        # half-way convention (half-even vs half-away-from-zero) and TRUNC's
        # behaviour at the sign change are not pinned down here, and both move a
        # value by strictly less than 1 at any non-negative scale.
        return parameters[0], _widen(interval, 1)

    if name == "ABS":
        if not _numeric_interval(interval):
            return None
        folded = _abs_preimage(interval)
        return None if folded is None else (parameters[0], folded)

    if name == "SIGN":
        if not _numeric_interval(interval):
            return None
        folded = _sign_preimage(interval)
        return None if folded is None else (parameters[0], folded)

    if name == "LEFT":
        width = _literal_value(parameters[1]) if len(parameters) > 1 else _NOT_A_LITERAL
        if not isinstance(width, int) or isinstance(width, bool) or width < 1:
            return None
        folded = _truncating_preimage(interval, width)
        return None if folded is None else (parameters[0], folded)

    if name == "SUBSTRING":
        start = _literal_value(parameters[1]) if len(parameters) > 1 else _NOT_A_LITERAL
        if start != 1:
            # Only a prefix is a prefix. SUBSTRING from any other offset is not
            # order-related to the whole string at all.
            return None
        if len(parameters) < 3:
            return parameters[0], interval  # SUBSTRING(x, 1) is x
        width = _literal_value(parameters[2])
        if not isinstance(width, int) or isinstance(width, bool) or width < 1:
            return None
        folded = _truncating_preimage(interval, width)
        return None if folded is None else (parameters[0], folded)

    if name == "EXTRACT":
        if len(parameters) != 2:
            return None
        part = _literal_value(parameters[0])
        if not isinstance(part, str) or part.lower() != "year":
            return None
        identifier = _unwrap(parameters[1])
        if identifier is None or identifier.node_type != NodeType.IDENTIFIER:
            return None
        column_type = _column_type_of(identifier, column_type_for)
        if column_type is None or column_type.category not in _TEMPORAL_CATEGORIES:
            return None
        if not _numeric_interval(interval):
            return None
        folded = _year_preimage(interval, column_type)
        return None if folded is None else (identifier, folded)

    if name == "TIME_BUCKET":
        return _time_bucket_preimage(parameters, interval, column_type_for)

    if name == "UNIXTIME":
        identifier = _unwrap(parameters[0])
        if identifier is None or identifier.node_type != NodeType.IDENTIFIER:
            return None
        ticks = _ticks_per_second(_column_type_of(identifier, column_type_for))
        if ticks is None or not _numeric_interval(interval):
            return None
        second_lower, _, second_upper, _ = _floor_preimage(interval)
        # One extra second of slack each way, so the rule does not depend on
        # whether the kernel floors or truncates toward zero at negative epochs.
        return identifier, (
            None if second_lower is None else (second_lower - 1) * ticks,
            True,
            None if second_upper is None else (second_upper + 1) * ticks,
            False,
        )

    if name == "FROM_UNIXTIME":
        identifier = _unwrap(parameters[0])
        if identifier is None or identifier.node_type != NodeType.IDENTIFIER:
            return None
        if not _numeric_interval(interval):
            return None
        # FROM_UNIXTIME returns TIMESTAMP[US] regardless of the argument, so the
        # bound arrives as raw MICROSECONDS and the column holds whole seconds.
        lower, _, upper, _ = interval
        return identifier, (
            None if lower is None else _floor_div(int(lower), 10**6) - 1,
            True,
            None if upper is None else _ceil_div(int(upper), 10**6) + 1,
            True,
        )

    return None


def _time_bucket_preimage(parameters, interval: Interval, column_type_for):
    """``TIME_BUCKET(magnitude, units, col)`` — bounded WITHOUT knowing where the
    buckets are anchored.

    Only two facts about a flooring bucket are used, and neither depends on the
    origin: the bucket label never exceeds the value it labels, and the value is
    less than one bucket width past its label.

        ``bucket(x) >= a``  =>  ``x >= bucket(x) >= a``
        ``bucket(x) <= b``  =>  ``x < b + width``

    The width is added as `magnitude + 1` calendar units rather than
    `magnitude`, because a calendar unit's length varies (February against
    January) and the bucket containing x may be longer than the one starting at
    b. One spare unit covers that without needing to know which bucket it was.
    """
    if len(parameters) != 3:
        return None
    magnitude = _literal_value(parameters[0])
    unit = _literal_value(parameters[1])
    if not _is_number(magnitude) or not 1 <= magnitude <= 100_000 or not isinstance(unit, str):
        return None
    unit = unit.lower()
    # Checked against the allowlist rather than relying on `add_single_unit` to
    # raise: an optimisation declines, it does not throw out of the optimizer.
    if unit not in ("second", "minute", "hour", "day", "week", "month", "quarter", "year"):
        return None
    identifier = _unwrap(parameters[2])
    if identifier is None or identifier.node_type != NodeType.IDENTIFIER:
        return None
    column_type = _column_type_of(identifier, column_type_for)
    if column_type is None or column_type.category not in _TEMPORAL_CATEGORIES:
        return None

    lower, lower_closed, upper, upper_closed = interval
    new_upper = None
    if upper is not None:
        moment = _raw_to_datetime(upper, column_type)
        if moment is None:
            return None
        from opteryx.utils.dates import add_single_unit

        if moment.year > 9000:
            return None  # no room to add a unit without overflowing `datetime`
        edge = add_single_unit(moment, unit, int(magnitude) + 1)
        new_upper = _datetime_to_raw(edge, column_type)

    if lower is None and new_upper is None:
        return None
    return identifier, (lower, lower_closed, new_upper, False)


# ---------------------------------------------------------------------------
# Predicate shapes -> one interval on one column
# ---------------------------------------------------------------------------


def _identity_key(identifier):
    """What makes two references "the same column" when hulling OR arms.

    Identity first (two relations can share a column NAME, and a self-join's
    `n1.name` and `n2.name` must never be hulled together); the source column
    name is the fallback for a node the binder left without a schema_column.
    """
    schema_column = getattr(identifier, "schema_column", None)
    identity = getattr(schema_column, "identity", None)
    return identity if identity is not None else identifier.source_column


def _in_list_interval(node) -> Optional[Interval]:
    """Hull of an IN list.

    A mixed-type list is refused rather than ordered: `min` over one would
    either raise or impose Python's own cross-type ordering, and neither is the
    engine's. The binder rejects mixed IN lists anyway — this is the second
    line, not the first.
    """
    values = node.right.value
    if not isinstance(values, (list, tuple, set, frozenset)):
        return None
    members = [_scalar(value) for value in values]
    if not members or any(member is None for member in members):
        return None

    if all(_is_number(member) for member in members):
        pass
    elif all(isinstance(member, str) for member in members):
        pass
    elif all(isinstance(member, (bytes, bytearray)) for member in members):
        members = [bytes(member) for member in members]
    else:
        return None

    return (min(members), True, max(members), True)


def _like_interval(node) -> Optional[Interval]:
    """Prefix range for `col LIKE 'abc%'`, and an equality for a wildcard-free
    pattern.

    A pattern whose first wildcard is at position 0 (`'%abc'`) constrains
    nothing and is declined. A pattern containing a backslash is declined
    outright rather than reasoned about: whether it escapes the next character
    decides where the prefix ENDS, and getting that wrong shortens the prefix
    (harmless) or lengthens it (drops rows).
    """
    pattern = _scalar(node.right.value)
    text = _decode_ascii(pattern)
    if text is None or "\\" in text:
        return None

    cut = len(text)
    for position, character in enumerate(text):
        if character in "%_":
            cut = position
            break
    # Sliced from the ORIGINAL, so a bytes pattern yields a bytes bound; see
    # `_text_successor` on why the type has to survive the round trip.
    prefix = pattern[:cut]
    if not prefix:
        return None

    if cut == len(text):
        # No wildcard at all: LIKE degenerates to equality.
        return (prefix, True, prefix, True)

    return _prefix_interval(prefix)


def _starts_with_interval(conjunct, column_type_for):
    """``_STARTS_WITH(col, b'abc')`` — the shape `col LIKE 'abc%'` actually has
    by the time pruning sees it.

    PredicateRewriteStrategy lowers an anchored LIKE into this FUNCTION node
    (used directly as a predicate, with no comparison wrapped around it), so the
    `Like` rule above never fires for the commonest prefix query in real plans.
    Both are kept: the rewrite only fires for a pattern ending in `%` with no
    other wildcard, and `prune_files` is also handed predicates from sources
    that strategy never rewrote.

    The siblings are deliberately NOT matched. `_CI_STARTS_WITH` is the
    case-insensitive form — the LOWER problem, where no useful interval exists —
    and `_ENDS_WITH`/`InStr` constrain no prefix at all.
    """
    if conjunct.value != "_STARTS_WITH":
        return None
    parameters = list(conjunct.parameters or [])
    if len(parameters) != 2:
        return None
    identifier = _unwrap(parameters[0])
    if identifier is None or identifier.node_type != NodeType.IDENTIFIER:
        return None
    pattern = _literal_value(parameters[1])
    if isinstance(pattern, _NotALiteral):
        return None
    interval = _prefix_interval(pattern)
    return None if interval is None else (identifier, interval)


def _comparison_interval(node) -> Optional[Tuple[Any, Interval]]:
    """`(expression, interval)` for `expr <op> literal` in either operand order."""
    operator = node.value
    left = _unwrap(node.left)
    right = _unwrap(node.right)
    if left is None or right is None:
        return None

    if right.node_type == NodeType.LITERAL and left.node_type != NodeType.LITERAL:
        expression, literal = left, right
    elif left.node_type == NodeType.LITERAL and right.node_type != NodeType.LITERAL:
        expression, literal = right, left
        operator = _FLIP.get(operator)
    else:
        return None

    builder = _OP_TO_INTERVAL.get(operator)
    if builder is None:
        return None
    value = _scalar(literal.value)
    if value is None:
        return None  # `col > NULL` is NULL, never true — not an interval
    return expression, builder(value)


def _conjunct_interval(conjunct, column_type_for, depth: int = 0):
    """`(identifier, interval)` for a conjunct that confines exactly ONE column,
    or None. `depth` bounds the OR recursion so a pathological predicate tree
    cannot make plan time quadratic."""
    conjunct = _unwrap(conjunct)
    if conjunct is None or depth > 8:
        return None

    node_type = conjunct.node_type

    if node_type == NodeType.COMPARISON_OPERATOR:
        if conjunct.value == "InList":
            left = _unwrap(conjunct.left)
            right = _unwrap(conjunct.right)
            if (
                left is None
                or left.node_type != NodeType.IDENTIFIER
                or right is None
                or right.node_type != NodeType.LITERAL
            ):
                return None
            interval = _in_list_interval(conjunct)
            return None if interval is None else (left, interval)

        if conjunct.value == "Like":
            left = _unwrap(conjunct.left)
            right = _unwrap(conjunct.right)
            if (
                left is None
                or left.node_type != NodeType.IDENTIFIER
                or right is None
                or right.node_type != NodeType.LITERAL
            ):
                return None
            interval = _like_interval(conjunct)
            return None if interval is None else (left, interval)

        comparison = _comparison_interval(conjunct)
        if comparison is None:
            return None
        expression, interval = comparison
        return _preimage(expression, interval, column_type_for)

    if node_type == NodeType.BETWEEN:
        left = _unwrap(conjunct.left)
        lower = _unwrap(conjunct.right)
        upper = _unwrap(conjunct.centre)
        if (
            left is None
            or lower is None
            or upper is None
            or lower.node_type != NodeType.LITERAL
            or upper.node_type != NodeType.LITERAL
        ):
            return None
        lower_value = _scalar(lower.value)
        upper_value = _scalar(upper.value)
        if lower_value is None or upper_value is None:
            return None
        return _preimage(left, (lower_value, True, upper_value, True), column_type_for)

    if node_type == NodeType.FUNCTION:
        return _starts_with_interval(conjunct, column_type_for)

    if node_type in (NodeType.OR, NodeType.CNF):
        # A disjunction constrains a column only when EVERY arm constrains THAT
        # column — one arm free of it (or one arm we cannot bound) means a row
        # can satisfy the predicate from anywhere in the column's domain.
        arms = (
            list(conjunct.parameters or [])
            if node_type == NodeType.CNF
            else [conjunct.left, conjunct.right]
        )
        if len(arms) < 2:
            return None
        identifier = None
        key = None
        intervals = []
        for arm in arms:
            resolved = _conjunct_interval(arm, column_type_for, depth + 1)
            if resolved is None:
                return None
            arm_identifier, arm_interval = resolved
            arm_key = _identity_key(arm_identifier)
            if arm_key is None:
                return None
            if key is None:
                identifier, key = arm_identifier, arm_key
            elif arm_key != key:
                return None
            intervals.append(arm_interval)

        # `_hull` orders the arm bounds against each other, so they have to be
        # mutually comparable; a hull of a numeric arm and a string arm is not a
        # thing, and Python would either raise or invent an answer.
        bounds = [bound for arm in intervals for bound in (arm[0], arm[2]) if bound is not None]
        if bounds and not (
            all(_is_number(bound) for bound in bounds)
            or all(isinstance(bound, str) for bound in bounds)
            or all(isinstance(bound, (bytes, bytearray)) for bound in bounds)
        ):
            return None
        return identifier, _hull(intervals)

    return None


def _is_canonical(conjunct) -> bool:
    """True when the bounds pruners already read this conjunct as it stands, so
    deriving an equivalent copy would only cost plan time."""
    if conjunct.node_type == NodeType.COMPARISON_OPERATOR:
        return (
            (conjunct.value in _OP_TO_INTERVAL or conjunct.value == "NotEq")
            and conjunct.left is not None
            and conjunct.left.node_type == NodeType.IDENTIFIER
            and conjunct.right is not None
            and conjunct.right.node_type == NodeType.LITERAL
        )
    if conjunct.node_type == NodeType.BETWEEN:
        return (
            conjunct.left is not None
            and conjunct.left.node_type == NodeType.IDENTIFIER
            and conjunct.right is not None
            and conjunct.right.node_type == NodeType.LITERAL
            and conjunct.centre is not None
            and conjunct.centre.node_type == NodeType.LITERAL
        )
    return False


def _comparable_pair(lower, upper) -> bool:
    if _is_number(lower) and _is_number(upper):
        return True
    if isinstance(lower, str) and isinstance(upper, str):
        return True
    return isinstance(lower, (bytes, bytearray)) and isinstance(upper, (bytes, bytearray))


def split_conjuncts(predicates: List) -> List:
    """The ANDed terms of `predicates`, via the ONE splitter.

    `_inner_split` knows both spellings of a conjunction (the binary AND tree and
    the n-ary DNF node) and refuses to descend an OR, which is what keeps the
    "every conjunct must hold" reasoning honest. Never write a second one.
    """
    from opteryx.planner.optimizer.strategies.split_conjunctive_predicates import _inner_split

    conjuncts: List = []
    for predicate in predicates or []:
        if predicate is not None:
            conjuncts.extend(_inner_split(predicate))
    return conjuncts


def derive_bound_conjuncts(
    predicates: List, column_type_for: Optional[Callable[[str], Any]] = None
) -> List:
    """The ANDed terms of `predicates`, PLUS a derived `column <op> literal` for
    every term that confines a column without saying so in that shape.

    A drop-in replacement for splitting the predicates directly: the original
    conjuncts come back untouched and in order, so a caller gains the new shapes
    without losing any behaviour it already had. Derived terms are additional
    evidence about the SAME conjunction — each one is implied by the term it came
    from, so ANDing it in changes nothing about which rows match.

    `column_type_for` maps a column name to its ColumnType (the manifest's live
    schema). It is optional: without it the rules fall back to each identifier's
    own bound `schema_column`, and the ones that cannot resolve a type decline.
    """
    conjuncts = split_conjuncts(predicates)
    derived: List = []

    for conjunct in conjuncts:
        if _is_canonical(conjunct):
            continue
        resolved = _conjunct_interval(conjunct, column_type_for)
        if resolved is None:
            continue
        identifier, interval = resolved
        if identifier.source_column is None:
            continue

        lower, _, upper, _ = interval
        if lower is not None and upper is not None:
            if not _comparable_pair(lower, upper):
                continue
            if lower > upper:
                # An inverted interval means the predicate is unsatisfiable. That
                # may well be true, but it is also what a derivation BUG looks
                # like, and the two are indistinguishable from here — so decline.
                # Proving a predicate false is constant folding's job; this
                # module's job is bounds, and a bounds bug drops rows silently.
                continue

        column_type = _column_type_of(identifier, column_type_for)
        literal_type = (
            column_type
            if column_type is not None and column_type.category in _TEMPORAL_CATEGORIES
            else None
        )
        derived.extend(_emit(identifier, interval, literal_type))

    return conjuncts + derived


def derive_null_terms(predicates: List) -> List[Tuple[str, bool]]:
    """`(column_name, requires_a_null_row)` for every `IS NULL` / `IS NOT NULL`
    conjunct over a bare column.

    These are not bounds and never will be — no interval describes "is absent" —
    but the manifest already counts nulls per file per column, which answers both
    of them outright:

      * `IS NULL`     — a file with zero nulls in that column holds no matching row.
      * `IS NOT NULL` — a file whose every row is null in that column holds none either.

    Both survive merge-on-read deletes, which is why they are safe to prune on:
    deleting rows can only shrink both counts, so a file with no nulls before
    deletes has none after, and one that was all-null stays all-null.
    """
    terms: List[Tuple[str, bool]] = []
    for conjunct in split_conjuncts(predicates):
        conjunct = _unwrap(conjunct)
        if conjunct is None or conjunct.node_type != NodeType.UNARY_OPERATOR:
            continue
        if conjunct.value not in ("IsNull", "IsNotNull"):
            continue
        operand = _unwrap(conjunct.centre)
        if operand is None or operand.node_type != NodeType.IDENTIFIER:
            continue
        if operand.source_column is None:
            continue
        terms.append((operand.source_column, conjunct.value == "IsNull"))
    return terms


# ---------------------------------------------------------------------------
# Case folding — bounds that hold only for a file we can PROVE is case-uniform
# ---------------------------------------------------------------------------
#
# `LOWER(col) = 'opteryx'` has no useful unconditional bound: case folding is
# not order-preserving (every uppercase byte sorts below every lowercase one),
# so the sound interval spans roughly half the printable key space.
#
# It DOES have a useful conditional one. In a file whose `col` contains no
# uppercase byte at all, LOWER is the IDENTITY, and the predicate is exactly
# `col = 'opteryx'` — a point lookup. The manifest's per-file char-class byte
# counts answer "contains no uppercase byte" outright, so the bound can be
# derived here and its precondition checked per file by the pruner.
#
# The terms below are therefore NOT ordinary conjuncts and must never be mixed
# into `derive_bound_conjuncts`' output: applying one to a file that is not
# case-uniform would drop rows.

_FOLD_FUNCTIONS = ("LOWER", "UPPER")


def derive_case_fold_conjuncts(
    predicates: List, column_type_for: Optional[Callable[[str], Any]] = None
) -> List[Tuple[str, str, List]]:
    """`(column_name, fold_name, conjuncts)` — bounds that are valid ONLY for a
    file in which `fold_name` is provably the identity on `column_name`.

    `fold_name` is "LOWER" or "UPPER"; the caller is responsible for checking the
    precondition per file (`Manifest._fold_is_identity`) and must not apply these
    to any file where it does not hold.

    Shapes:
      * ``LOWER(col) <op> lit`` / ``UPPER(col) <op> lit`` — under identity this is
        `col <op> lit`, for EVERY comparison operator, not just equality.
      * ``_CI_STARTS_WITH(col, 'abc')`` — what `col ILIKE 'abc%'` is lowered to.
        Under identity this is `col LIKE 'abc%'` with the pattern folded, so the
        ordinary prefix range applies.
      * ``col ILIKE 'abc%'`` un-lowered, for the callers PredicateRewriteStrategy
        never reached.
    """
    derived: List[Tuple[str, str, List]] = []

    for conjunct in split_conjuncts(predicates):
        conjunct = _unwrap(conjunct)
        if conjunct is None:
            continue

        identifier = None
        fold = None
        interval = None

        if conjunct.node_type == NodeType.COMPARISON_OPERATOR and conjunct.value == "ILike":
            left = _unwrap(conjunct.left)
            right = _unwrap(conjunct.right)
            if (
                left is not None
                and left.node_type == NodeType.IDENTIFIER
                and right is not None
                and right.node_type == NodeType.LITERAL
                and _is_ascii_text(_scalar(right.value))
            ):
                folded_pattern = Literal(
                    value=_scalar(right.value).lower(), type=right.type
                )
                identifier = left
                fold = "LOWER"
                interval = _like_interval(
                    Comparison(value="Like", left=left, right=folded_pattern)
                )

        elif conjunct.node_type == NodeType.COMPARISON_OPERATOR:
            comparison = _comparison_interval(conjunct)
            if comparison is not None:
                expression, candidate = comparison
                expression = _unwrap(expression)
                if (
                    expression is not None
                    and expression.node_type == NodeType.FUNCTION
                    and expression.value in _FOLD_FUNCTIONS
                    and len(expression.parameters or []) == 1
                ):
                    inner = _unwrap(expression.parameters[0])
                    if inner is not None and inner.node_type == NodeType.IDENTIFIER:
                        identifier, fold, interval = inner, expression.value, candidate

        elif conjunct.node_type == NodeType.FUNCTION and conjunct.value == "_CI_STARTS_WITH":
            parameters = list(conjunct.parameters or [])
            if len(parameters) == 2:
                inner = _unwrap(parameters[0])
                pattern = _literal_value(parameters[1])
                if (
                    inner is not None
                    and inner.node_type == NodeType.IDENTIFIER
                    and not isinstance(pattern, _NotALiteral)
                    and _is_ascii_text(pattern)
                ):
                    # The lowering does NOT fold the pattern, so fold it here —
                    # under identity the column holds no uppercase, so only the
                    # lower-cased pattern can ever match.
                    identifier, fold = inner, "LOWER"
                    interval = _prefix_interval(pattern.lower())

        if identifier is None or interval is None or identifier.source_column is None:
            continue

        lower, _, upper, _ = interval
        if lower is not None and upper is not None:
            if not _comparable_pair(lower, upper) or lower > upper:
                continue

        emitted = _emit(identifier, interval, None)
        if emitted:
            derived.append((identifier.source_column, fold, emitted))

    return derived
