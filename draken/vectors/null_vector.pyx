# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True
# cython: freethreading_compatible=True

"""
NullVector: an untyped, always-null placeholder vector.

Always constant-encoded with a null value. Carries only row count.
Used to represent "argument not supplied" in function kernels while
preserving the contract that all arguments arrive as vectors.
"""

from libc.stdint cimport int32_t, int64_t, uint32_t, uint64_t, uint8_t
from cpython.array cimport array, clone

from draken.vectors.vector cimport Vector, NULL_HASH
from draken.core.buffers cimport (
    DrakenVector,
    DRAKEN_NON_NATIVE,
    draken_zero_sel,
    draken_zero_validity,
)

cdef uint64_t _null_payload_slot = 0  # value irrelevant; validity marks every row null


cdef class NullVector(Vector):

    def __cinit__(self, Py_ssize_t length):
        self._length = length

    cdef DrakenVector* unified(self) noexcept:
        self._unified_view.data        = <void*>&_null_payload_slot
        self._unified_view.selection   = draken_zero_sel(<uint32_t>self._length)
        self._unified_view.data_length = 1
        self._unified_view.length      = <uint32_t>self._length
        self._unified_view.validity    = <uint8_t*>draken_zero_validity(<uint32_t>self._length)
        self._unified_view.type        = DRAKEN_NON_NATIVE
        return &self._unified_view

    def __len__(self):
        return self._length

    @property
    def length(self):
        return self._length

    @property
    def num_rows(self):
        return self._length

    def __getitem__(self, Py_ssize_t i):
        if i < 0 or i >= self._length:
            raise IndexError("index out of range")
        return None

    def __iter__(self):
        cdef Py_ssize_t i
        for i in range(self._length):
            yield None

    cpdef bint is_null_at(self, Py_ssize_t idx) except? False:
        return True

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        # NULL == NULL for ordering purposes
        return 0

    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        cdef Py_ssize_t i
        for i in range(self._length):
            out_buf[offset + i] = NULL_HASH

    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        cdef Py_ssize_t i
        for i in range(n):
            out[i] = NULL_HASH
        return True

    cpdef uint64_t[::1] hash(self):
        cdef array template = array('Q')
        cdef array result = clone(template, self._length, False)
        cdef uint64_t[::1] buf = result
        cdef Py_ssize_t i
        for i in range(self._length):
            buf[i] = NULL_HASH
        return buf

    cpdef object null_bitmap(self):
        return None

    cpdef NullVector take(self, int32_t[::1] indices):
        return NullVector(indices.shape[0])

    cpdef Vector materialize(self):
        return self

    def to_pylist(self):
        return [None] * self._length

    def to_arrow(self):
        import pyarrow as pa
        return pa.nulls(self._length)

    cpdef int64_t[::1] compress(self):
        cdef array template = array('q')
        cdef array result = clone(template, self._length, False)
        cdef int64_t[::1] buf = result
        cdef Py_ssize_t i
        for i in range(self._length):
            buf[i] = <int64_t>0x8000000000000000ULL
        return buf

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        cdef Py_ssize_t i
        for i in range(self._length):
            out_buf[offset + i] = <int64_t>0x8000000000000000ULL

    cpdef tuple _unified_view_fields(self):
        cdef DrakenVector* uv = self.unified()
        return (
            uv.data_length,
            uv.length,
            uv.selection != NULL,
            uv.validity != NULL,
            (<uint32_t*>uv.selection)[0] if uv.selection != NULL else -1,
            (<uint8_t*>uv.validity)[0] if uv.validity != NULL else -1,
        )

    def __repr__(self):
        return f"NullVector(length={self._length})"
