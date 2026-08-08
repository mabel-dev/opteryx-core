# cython: language_level=3
# distutils: language=c++

"""The buffering aggregates' memory ceilings, read from the native constants.

A deliberately tiny extension. `opteryx/variables.py` reports these budgets in
`SHOW VARIABLES`, and it sits BELOW the engine in the import graph — importing
`opteryx.operators._operators` from there is a circular import
(`_operators` -> `expression` -> `models` -> `execution_context` -> `variables`).
This module includes only `engine/agg_budgets.hpp`, which has no dependants, so
anything can import it.

The point of reading the constants rather than mirroring them in Python is that
the figure `SHOW VARIABLES` REPORTS and the figure the native sinks ENFORCE
cannot drift apart.

These budgets are enforced at EXECUTION time only. There is no plan-time
estimate in front of them: what a buffering aggregate actually retains depends
on properties no planner statistic carries, so an estimate could only be a guess
that refuses working queries. A query that cannot fit reads its input and then
fails loud on a measurement.
"""

from libc.stdint cimport int64_t

cdef extern from "engine/agg_budgets.hpp" namespace "opteryx::agg_budgets" nogil:
    const int64_t kMedianBytes
    const int64_t kArrayAggBytes
    const int64_t kCidrAggStateBytes
    const int64_t kCidrAggEmitBytes


def median_budget_bytes() -> int:
    """Bytes MEDIAN may buffer across all groups before it fails loud."""
    return <int64_t>kMedianBytes


def array_agg_budget_bytes() -> int:
    """Bytes ARRAY_AGG may buffer across all groups before it fails loud."""
    return <int64_t>kArrayAggBytes


def cidr_agg_state_budget_bytes() -> int:
    """Bytes CIDR_AGG's address sets may hold across all groups.

    Bounds the COLLECTION side. The set dedups on insert, so this grows with
    distinct addresses, not with rows.
    """
    return <int64_t>kCidrAggStateBytes


def cidr_agg_emit_budget_bytes() -> int:
    """Bytes of CIDR text CIDR_AGG may produce across all groups.

    A SEPARATE ceiling, not derivable from the state budget: the worst-case
    output is 2^31 blocks from a state sitting at exactly the collection limit,
    so fitting in memory says nothing about the answer fitting.
    """
    return <int64_t>kCidrAggEmitBytes
