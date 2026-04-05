# cython: language_level=3

from libcpp.vector cimport vector
from libc.stdint cimport int64_t, uint8_t, int32_t


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


cdef class PerAggregateObjectState:
    """Base class for object per-aggregate state (ANY_VALUE, COUNT(DISTINCT), string MIN/MAX)"""
    cdef public int agg_idx
    cdef public int agg_mode
    cdef public int value_kind


cdef class PerAggregateAnyValueState(PerAggregateObjectState):
    """Per-aggregate state for ANY_VALUE and object-based MIN/MAX aggregates"""
    cdef public list object_values
    cdef public vector[uint8_t] object_bytes
    cdef public vector[int32_t] object_starts
    cdef public vector[int32_t] object_lengths
    cdef public vector[int64_t] seen


cdef class PerAggregateCountDistinctState(PerAggregateObjectState):
    """Per-aggregate state for COUNT(DISTINCT) aggregates"""
    cdef public list distinct_sets
    cdef public vector[int64_t] counts
