from libc.stdint cimport int32_t, int64_t, uint64_t
from libc.stddef cimport size_t

from opteryx.compiled.draken.morsels.morsel cimport Morsel
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

cdef class UngroupedAggregateEngine:
    cdef list _aggregates
    cdef list _avg_finalizers
    cdef set  _internal_aliases

    cpdef void add_aggregate(self, UngroupedAggregate agg)
    cpdef void add_avg_finalizer(self, bytes sum_alias, bytes count_alias, object output_alias)
    cpdef void ingest(self, Morsel morsel) except *
    cpdef Morsel finalize(self)
