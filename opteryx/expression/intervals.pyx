"""INTERVAL constants and the literal normaliser.

The Python INTERVAL operator kernels that lived here went with the Python
evaluator: interval arithmetic and comparison are native (binop_dispatch.cpp,
ops/interval_compare.h).
"""


# Conversion constants — exposed at module level for callers that historically
# imported them. Module-level Python ints; no need for cdef in the rare
# read path.
MICROSECONDS_PER_MILLISECOND = 1_000
MICROSECONDS_PER_SECOND = 1_000_000
MICROSECONDS_PER_MINUTE = 60 * MICROSECONDS_PER_SECOND
MICROSECONDS_PER_HOUR = 60 * MICROSECONDS_PER_MINUTE
MICROSECONDS_PER_DAY = 24 * MICROSECONDS_PER_HOUR
MICROSECONDS_PER_WEEK = 7 * MICROSECONDS_PER_DAY
NANOSECONDS_PER_MICROSECOND = 1_000

# The unit ladder, DESCENDING - a multi-value interval (`INTERVAL '1 3' YEAR TO MONTH`)
# walks consecutive entries from its leading unit, so the order is semantic, not
# cosmetic. Single source for the two places a unit is judged: the planner builder,
# which resolves a unit the parser ACCEPTED, and the parse-error path, which names a
# unit the parser REJECTED outright. Those are different failures with the same cause,
# and a reader must not be told two different lists of what is valid.
INTERVAL_UNITS = (
    "Year",
    "Month",
    "Week",
    "Day",
    "Hour",
    "Minute",
    "Second",
    "Millisecond",
    "Microsecond",
)


cpdef tuple normalize_interval_value(value):
    """Normalise interval literals to a canonical (months, microseconds) tuple."""
    if not (isinstance(value, tuple) and len(value) == 2):
        raise TypeError(
            f"INTERVAL literal must be a (months, microseconds) tuple, "
            f"got {type(value)!r}."
        )
    return (int(value[0]), int(value[1]))
