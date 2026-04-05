# cython: language_level=3

from libcpp.vector cimport vector
from libc.stdint cimport int64_t


cdef class PerAggregateNumericState:
    """Base class for numeric per-aggregate state (COUNT, SUM, MIN/MAX, AVG)"""
    cdef public int agg_idx
    cdef public int agg_mode
    cdef public int value_kind


cdef class PerAggregateCountState(PerAggregateNumericState):
    """Per-aggregate state for COUNT(*) and COUNT(col)"""
    cdef public vector[int64_t] counts


cdef class PerAggregateSumInt64State(PerAggregateNumericState):
    """Per-aggregate state for SUM(int64) and similar int64 numeric aggregates"""
    cdef public vector[int64_t] values
    cdef public vector[int64_t] seen


cdef class PerAggregateSumFloat64State(PerAggregateNumericState):
    """Per-aggregate state for SUM(float64) and similar float64 numeric aggregates"""
    cdef public vector[double] values
    cdef public vector[int64_t] seen


cdef class PerAggregateMinMaxInt64State(PerAggregateNumericState):
    """Per-aggregate state for MIN/MAX(int64) aggregates"""
    cdef public vector[int64_t] values
    cdef public vector[int64_t] seen


cdef class PerAggregateMinMaxFloat64State(PerAggregateNumericState):
    """Per-aggregate state for MIN/MAX(float64) aggregates"""
    cdef public vector[double] values
    cdef public vector[int64_t] seen


cdef class PerAggregateAvgInt64State(PerAggregateNumericState):
    """Per-aggregate state for AVG(int64) aggregates"""
    cdef public vector[double] sums
    cdef public vector[int64_t] counts


cdef class PerAggregateAvgFloat64State(PerAggregateNumericState):
    """Per-aggregate state for AVG(float64) aggregates"""
    cdef public vector[double] sums
    cdef public vector[int64_t] counts
