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

from opteryx.compiled.draken.core.buffers cimport ConstAccessor, DrakenFixedBuffer
from opteryx.compiled.draken.vectors.vector cimport Vector
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector


cdef class DecimalVector(Vector):
    # Storage: int64 unscaled values in a DrakenFixedBuffer.
    # This vector always owns its buffer (no zero-copy from Arrow).
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data

    # Per-column decimal metadata
    cdef int8_t _precision
    cdef int8_t _scale

    # ConstAccessor struct kept for the base-class protocol (always returns NULL
    # since DecimalVector has no constant-encoding path).
    cdef ConstAccessor _const_accessor

    # ------------------------------------------------------------------
    # C-level accessor protocol (Vector base interface)
    # ------------------------------------------------------------------

    cdef ConstAccessor* const_accessor(self) noexcept
    cdef void* dense_ptr(self) noexcept
    cdef uint8_t* null_bitmap_ptr(self) noexcept

    # ------------------------------------------------------------------
    # Row selection
    # ------------------------------------------------------------------

    cpdef DecimalVector take(self, int32_t[::1] indices)

    # ------------------------------------------------------------------
    # Scalar coercion and comparison helpers
    # ------------------------------------------------------------------

    cdef int64_t _coerce_scalar(self, object scalar)
    cdef bint _compare_decimal_values(self, int64_t left, int64_t right, int op) nogil
    cdef BoolVector _compare_scalar(self, int op, int64_t rhs)

    # ------------------------------------------------------------------
    # Public comparison API (scalar only)
    # ------------------------------------------------------------------

    cpdef BoolVector equals(self, object scalar)
    cpdef BoolVector not_equals(self, object scalar)
    cpdef BoolVector less_than(self, object scalar)
    cpdef BoolVector less_than_or_equals(self, object scalar)
    cpdef BoolVector greater_than(self, object scalar)
    cpdef BoolVector greater_than_or_equals(self, object scalar)

    # ------------------------------------------------------------------
    # Conversion
    # ------------------------------------------------------------------

    cpdef list to_pylist(self)

    # ------------------------------------------------------------------
    # Hashing (overrides Vector base; must be declared to enable cimport
    # of the concrete implementation from other Cython modules)
    # ------------------------------------------------------------------

    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *


# Module-level factory exposed for cimport by arrow.pyx and other consumers
cdef DecimalVector from_arrow(object array)
