from libc.stdint cimport int32_t, int64_t, uint64_t
from libc.stddef cimport size_t

from draken.morsels.morsel cimport Morsel
from opteryx.compiled.structures.carchar_set cimport CarcharSetWrapper


cdef int AGG_RESULT_I64
cdef int AGG_RESULT_F64
cdef int AGG_RESULT_BYTES
cdef int AGG_RESULT_OBJECT

cdef int _VTYPE_UNKNOWN
cdef int _VTYPE_INT64
cdef int _VTYPE_INTEGER
cdef int _VTYPE_FLOAT64
cdef int _VTYPE_STRING
cdef int _VTYPE_GENERIC
cdef int _VTYPE_DECIMAL


cdef class UngroupedAggregate:
    cdef bytes      column_name
    cdef bytes      alias
    cdef int        result_type
    cdef Py_ssize_t _col_idx    # -1 = unresolved; set on first apply()
    cdef int        _col_type   # _VTYPE_UNKNOWN = unresolved; set on first apply()

    cdef void    apply(self, Morsel morsel) except *
    cdef int64_t get_result_i64(self) noexcept
    cdef double  get_result_f64(self) noexcept
    cdef void    get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept
    cdef bint    is_null(self) noexcept
    cpdef object get_result(self)


cdef class CountStarAggregate(UngroupedAggregate):
    cdef int64_t _count

cdef class CountAggregate(UngroupedAggregate):
    cdef int64_t _count

cdef class SumInt64Aggregate(UngroupedAggregate):
    cdef int64_t _total
    cdef bint    _seen

cdef class SumFloat64Aggregate(UngroupedAggregate):
    cdef double _total
    cdef bint   _seen

cdef class SumDecimalAggregate(UngroupedAggregate):
    cdef object _total_dec
    cdef bint   _seen

cdef class MinInt64Aggregate(UngroupedAggregate):
    cdef int64_t _result
    cdef bint    _seen

cdef class MaxInt64Aggregate(UngroupedAggregate):
    cdef int64_t _result
    cdef bint    _seen

cdef class MinFloat64Aggregate(UngroupedAggregate):
    cdef double _result
    cdef bint   _seen

cdef class MaxFloat64Aggregate(UngroupedAggregate):
    cdef double _result
    cdef bint   _seen

cdef class MinDecimalAggregate(UngroupedAggregate):
    cdef object _result
    cdef bint   _seen

cdef class MaxDecimalAggregate(UngroupedAggregate):
    cdef object _result
    cdef bint   _seen

cdef class MinBytesAggregate(UngroupedAggregate):
    cdef bytes _result

cdef class MaxBytesAggregate(UngroupedAggregate):
    cdef bytes _result

cdef class AnyValueAggregate(UngroupedAggregate):
    cdef object _value
    cdef bint   _seen

cdef class CountDistinctAggregate(UngroupedAggregate):
    cdef CarcharSetWrapper _set  # typed — needed for nogil insert loop
    cdef uint64_t* _scratch_buf
    cdef Py_ssize_t _scratch_capacity
    cdef uint8_t* _mask_buf            # compressed-path per-distinct-value referenced flags
    cdef Py_ssize_t _mask_capacity

cdef class AvgFinalizer:
    cdef bytes  sum_alias
    cdef bytes  count_alias
    cdef bytes  output_alias


cdef class UngroupedAggregateEngine:
    # Refcount-holding list of UngroupedAggregate instances. The C array
    # below holds borrowed pointers into the same objects for Python-free
    # iteration on the hot path.
    cdef list                 _aggregates_pyrefs
    cdef void**               _agg_ptrs
    cdef Py_ssize_t           _n_aggregates
    cdef Py_ssize_t           _agg_capacity

    # AVG finalizer storage — typed, no dicts/tuples on the hot path
    cdef list                 _avg_finalizers_pyrefs
    cdef void**               _avg_ptrs  # borrowed AvgFinalizer*
    cdef Py_ssize_t           _n_avgs
    cdef Py_ssize_t           _avg_capacity

    # Set of aliases that should be hidden from finalize() output
    cdef set                  _internal_aliases

    cpdef void add_aggregate(self, UngroupedAggregate agg)
    cpdef void add_avg_finalizer(self, bytes sum_alias, bytes count_alias, object output_alias)
    cpdef void ingest(self, Morsel morsel) except *
    cpdef Morsel finalize(self)
    cdef void _grow_agg_array(self) except *
    cdef void _grow_avg_array(self) except *
    cdef object _result_for_alias(self, bytes alias)
