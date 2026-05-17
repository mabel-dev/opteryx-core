# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t, INT64_MAX, INT64_MIN
from libc.stddef cimport size_t
from libc.float cimport DBL_MAX
from libc.stdlib cimport malloc, free
from libc.string cimport memset

from draken.vectors.vector cimport Vector, NULL_HASH, mix_hash
from draken.vectors.integer_vector cimport IntegerVector
from draken.core.buffers cimport (
    DrakenFixedBuffer, DrakenVarBuffer,
    DrakenConstantStringPayload, DrakenVector,
)
cdef extern from "_agg_kernels.hpp" namespace "opteryx::ungrouped":
    int compare_bytes(const char* a, size_t la, const char* b, size_t lb) noexcept


# ---------------------------------------------------------------------------
# Local helper functions previously imported from deleted aggregation modules
# ---------------------------------------------------------------------------

cdef inline bint _bitmap_is_valid(const uint8_t* bitmap, Py_ssize_t index) noexcept nogil:
    return ((bitmap[index >> 3] >> (index & 7)) & 1) != 0


cdef inline int64_t _read_integer_value(DrakenFixedBuffer* buf, Py_ssize_t index) noexcept nogil:
    if buf.type == 0:
        return 0
    if buf.type == 1:
        return (<int8_t*>buf.data)[index]
    if buf.type == 2:
        return (<int16_t*>buf.data)[index]
    if buf.type == 3:
        return (<int32_t*>buf.data)[index]
    return (<int64_t*>buf.data)[index]


# ---------------------------------------------------------------------------
# Result-type constants (used by all aggregate classes)
# ---------------------------------------------------------------------------
cdef int AGG_RESULT_I64    = 0
cdef int AGG_RESULT_F64    = 1
cdef int AGG_RESULT_BYTES  = 2
cdef int AGG_RESULT_OBJECT = 3


# ---------------------------------------------------------------------------
# Vector type tags — classified once per aggregate, cached as int
# ---------------------------------------------------------------------------
cdef int _VTYPE_UNKNOWN = 0
cdef int _VTYPE_INT64   = 1
cdef int _VTYPE_INTEGER = 2
cdef int _VTYPE_FLOAT64 = 3
cdef int _VTYPE_STRING   = 4
cdef int _VTYPE_GENERIC  = 5

cdef inline int _classify_vector(Vector v) noexcept:
    if isinstance(v, Int64Vector):
        return _VTYPE_INT64
    if isinstance(v, IntegerVector):
        return _VTYPE_INTEGER
    if isinstance(v, Float64Vector):
        return _VTYPE_FLOAT64
    if isinstance(v, StringVector):
        return _VTYPE_STRING
    return _VTYPE_GENERIC


# ---------------------------------------------------------------------------
# Abstract base class
# ---------------------------------------------------------------------------
cdef class UngroupedAggregate:
    """
    Base class for all ungrouped (global) aggregate accumulators.
    """
    cdef bytes      column_name
    cdef bytes      alias
    cdef int        result_type
    cdef Py_ssize_t _col_idx
    cdef int        _col_type

    def __cinit__(self):
        self.column_name = b""
        self.alias = b""
        self.result_type = AGG_RESULT_OBJECT
        self._col_idx = -1
        self._col_type = _VTYPE_UNKNOWN

    cdef void apply(self, Morsel morsel) except *:
        pass

    cdef int64_t get_result_i64(self) noexcept:
        return 0

    cdef double get_result_f64(self) noexcept:
        return 0.0

    cdef void get_result_bytes(self, const char** out_ptr, size_t* out_len) noexcept:
        out_ptr[0] = NULL
        out_len[0] = 0

    cdef bint is_null(self) noexcept:
        return True

    cpdef object get_result(self):
        return None

    def _test_apply(self, morsel):
        """Test-only driver: invoke ``apply`` from Python."""
        self.apply(<Morsel>morsel)


# ---------------------------------------------------------------------------
# Concrete aggregate implementations (textual includes — single .so)
# ---------------------------------------------------------------------------
include "ungrouped_agg_count.pyx"
include "ungrouped_agg_sum.pyx"
include "ungrouped_agg_min_max.pyx"
include "ungrouped_agg_any_value.pyx"
include "ungrouped_agg_count_distinct.pyx"
include "ungrouped_agg_median.pyx"
include "ungrouped_agg_engine.pyx"
