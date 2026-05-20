# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int8_t
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport uint64_t
from libc.stdint cimport uint8_t

from draken.core.buffers cimport DrakenFixedBuffer, DrakenVector
from draken.vectors.vector cimport Vector
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.integer64_vector cimport Integer64Vector
from draken.vectors.float64_vector cimport Float64Vector


cdef class DecimalVector(Vector):
    # Storage: int64 unscaled values in a DrakenFixedBuffer.
    # This vector always owns its buffer (no zero-copy from Arrow).
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data

    # Per-column decimal metadata
    cdef int8_t _precision
    cdef int8_t _scale

    # ------------------------------------------------------------------
    # C-level accessor protocol (Vector base interface)
    # ------------------------------------------------------------------

    cdef DrakenVector* unified(self) noexcept

    # ------------------------------------------------------------------
    # Row selection
    # ------------------------------------------------------------------

    cpdef DecimalVector take(self, int32_t[::1] indices)

    # ------------------------------------------------------------------
    # Scalar coercion and comparison helpers
    # ------------------------------------------------------------------

    cdef int64_t _coerce_scalar(self, object scalar)
    cdef bint _compare_decimal_values(self, int64_t left, int64_t right, int op) nogil
    cpdef BoolVector _compare_scalar(self, int op, int64_t rhs)
    cpdef BoolVector _compare_vector(self, DecimalVector other, int op)

    # ------------------------------------------------------------------
    # Public comparison API — scalar
    # ------------------------------------------------------------------

    cpdef BoolVector equals(self, object scalar)
    cpdef BoolVector not_equals(self, object scalar)
    cpdef BoolVector less_than(self, object scalar)
    cpdef BoolVector less_than_or_equals(self, object scalar)
    cpdef BoolVector greater_than(self, object scalar)
    cpdef BoolVector greater_than_or_equals(self, object scalar)

    # ------------------------------------------------------------------
    # Public comparison API — vector-vector
    # ------------------------------------------------------------------

    cpdef BoolVector equals_vector(self, DecimalVector other)
    cpdef BoolVector not_equals_vector(self, DecimalVector other)
    cpdef BoolVector less_than_vector(self, DecimalVector other)
    cpdef BoolVector less_than_or_equals_vector(self, DecimalVector other)
    cpdef BoolVector greater_than_vector(self, DecimalVector other)
    cpdef BoolVector greater_than_or_equals_vector(self, DecimalVector other)

    # ------------------------------------------------------------------
    # Set membership
    # ------------------------------------------------------------------

    cpdef BoolVector in_list(self, object value_set)

    # ------------------------------------------------------------------
    # Null predicate
    # ------------------------------------------------------------------

    cpdef object is_null(self)

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    cpdef object sum(self)
    cpdef object min(self)
    cpdef object max(self)

    # ------------------------------------------------------------------
    # Conversion
    # ------------------------------------------------------------------

    cpdef Float64Vector to_float64_vector(self)
    cpdef list to_pylist(self)

    # ------------------------------------------------------------------
    # Hashing and compression (overrides Vector base; declared to enable
    # cimport of the concrete implementation from other Cython modules)
    # ------------------------------------------------------------------

    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *
    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=*) except *


# Module-level factories exposed for cimport by other consumers
cdef DecimalVector from_arrow(object array)
cpdef DecimalVector from_int64_vector(Integer64Vector source, int precision, int scale)

