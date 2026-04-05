# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

"""Per-aggregate state classes for grouped aggregation.

These classes are declared in aggregations_state_classes.pxd and hold
per-aggregate state vectors for the finalize path.
"""

from libcpp.vector cimport vector
from libc.stdint cimport int64_t, uint8_t, int32_t


# Base class for all numeric per-aggregate states
cdef class PerAggregateNumericState:
    """Base class for numeric per-aggregate state (COUNT, SUM, MIN/MAX, AVG)"""
    def __init__(self):
        self.agg_idx = -1
        self.agg_mode = 0
        self.value_kind = 0


# COUNT state
cdef class PerAggregateCountState(PerAggregateNumericState):
    """Per-aggregate state for COUNT(*) and COUNT(col)"""
    def __init__(self):
        super().__init__()
        self.counts = vector[int64_t]()


# SUM(int64) state
cdef class PerAggregateSumInt64State(PerAggregateNumericState):
    """Per-aggregate state for SUM(int64) and similar int64 numeric aggregates"""
    def __init__(self):
        super().__init__()
        self.values = vector[int64_t]()
        self.seen = vector[int64_t]()


# SUM(float64) state
cdef class PerAggregateSumFloat64State(PerAggregateNumericState):
    """Per-aggregate state for SUM(float64) and similar float64 numeric aggregates"""
    def __init__(self):
        super().__init__()
        self.values = vector[double]()
        self.seen = vector[int64_t]()


# MIN/MAX(int64) state
cdef class PerAggregateMinMaxInt64State(PerAggregateNumericState):
    """Per-aggregate state for MIN/MAX(int64) aggregates"""
    def __init__(self):
        super().__init__()
        self.values = vector[int64_t]()
        self.seen = vector[int64_t]()


# MIN/MAX(float64) state
cdef class PerAggregateMinMaxFloat64State(PerAggregateNumericState):
    """Per-aggregate state for MIN/MAX(float64) aggregates"""
    def __init__(self):
        super().__init__()
        self.values = vector[double]()
        self.seen = vector[int64_t]()


# AVG(int64) state
cdef class PerAggregateAvgInt64State(PerAggregateNumericState):
    """Per-aggregate state for AVG(int64) aggregates"""
    def __init__(self):
        super().__init__()
        self.sums = vector[double]()
        self.counts = vector[int64_t]()


# AVG(float64) state
cdef class PerAggregateAvgFloat64State(PerAggregateNumericState):
    """Per-aggregate state for AVG(float64) aggregates"""
    def __init__(self):
        super().__init__()
        self.sums = vector[double]()
        self.counts = vector[int64_t]()


# Base class for all object per-aggregate states
cdef class PerAggregateObjectState:
    """Base class for object per-aggregate state (ANY_VALUE, COUNT(DISTINCT), string MIN/MAX)"""
    def __init__(self):
        self.agg_idx = -1
        self.agg_mode = 0
        self.value_kind = 0


# ANY_VALUE and object-based MIN/MAX state
cdef class PerAggregateAnyValueState(PerAggregateObjectState):
    """Per-aggregate state for ANY_VALUE and object-based MIN/MAX aggregates"""
    def __init__(self):
        super().__init__()
        self.object_values = []
        self.object_bytes = vector[uint8_t]()
        self.object_starts = vector[int32_t]()
        self.object_lengths = vector[int32_t]()
        self.seen = vector[int64_t]()


# COUNT(DISTINCT) state
cdef class PerAggregateCountDistinctState(PerAggregateObjectState):
    """Per-aggregate state for COUNT(DISTINCT) aggregates"""
    def __init__(self):
        super().__init__()
        self.distinct_sets = []
        self.counts = vector[int64_t]()
