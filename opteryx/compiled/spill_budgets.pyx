# cython: language_level=3
# distutils: language=c++

"""Morsel-spill thresholds, read from the native constants.

Same tiny-extension pattern (and the same layering argument) as agg_budgets:
`opteryx/variables.py` reports these in `SHOW VARIABLES` and sits below the
engine in the import graph, so it cannot reach them through
`opteryx.operators._operators` without a circular import. This module includes
only `engine/spill_budgets.hpp`, which has no dependants.

Reading the constants rather than mirroring them in Python means the figure
`SHOW VARIABLES` REPORTS and the figure the native buffers ENFORCE cannot
drift apart.

Spill triggers on MEASUREMENT, never on a plan-time estimate, and only when a
spill root is configured (`KVSTORE_LOCATION`); unconfigured, buffered
accumulation is unbounded — exactly the pre-spill engine.
"""

from libc.stdint cimport int64_t

cdef extern from "engine/spill_budgets.hpp" namespace "opteryx::spill_budgets" nogil:
    const int64_t kSpillFlushBytes
    const int64_t kSpillCeilingBytes


def spill_flush_bytes() -> int:
    """Outstanding buffered-morsel bytes at which a configured buffer flushes
    the pile to one .skene spill unit (512MB — the size of a typical
    skene-written file, so spill runs the writer at the shape it is tuned at)."""
    return <int64_t>kSpillFlushBytes


def spill_ceiling_bytes() -> int:
    """Backpressure bound on outstanding buffered-morsel bytes while a flush is
    in flight: at this line an appending worker waits for the flush and then
    flushes the pile itself, throttling the query to disk speed instead of
    growing without bound."""
    return <int64_t>kSpillCeilingBytes
