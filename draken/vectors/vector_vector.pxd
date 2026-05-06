from libc.stdint cimport int32_t, uint8_t, uint16_t, uint64_t

from draken.vectors.float32_vector cimport Float32Vector
from draken.vectors.vector cimport Vector


cdef class VectorVector(Vector):
    # Flat IEEE binary16 storage: length * dimensions uint16_t values.
    # Layout is row-major; row i occupies indices [i*dimensions, (i+1)*dimensions).
    cdef uint16_t* _data
    cdef Py_ssize_t _length
    cdef Py_ssize_t _dimensions
    cdef bint _owns_data

    # Row-level null bitmap. NULL means no nulls. Size: (length + 7) // 8 bytes.
    cdef uint8_t* _null_bitmap
    cdef bint _owns_null_bitmap

    # Arrow buffer keep-alives (zero-copy ingest).
    cdef object _arrow_parent
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf

    cdef Float32Vector _alloc_score_vector(self)

    # --- Distance kernels (the only real compute surface). ---
    # All kernels load fp16 and accumulate in fp32 internally. The query is
    # provided as fp32 because the kernel widens fp16 -> fp32 anyway; making
    # the caller materialize fp16 just to widen again would be wasteful.
    cpdef Float32Vector dot(self, float[::1] query)
    cpdef Float32Vector cosine_similarity(self, float[::1] query)
    cpdef Float32Vector l2_distance(self, float[::1] query)

    # --- Standard Vector surface. ---
    cpdef bint is_null_at(self, Py_ssize_t idx) except? False
    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0
    cpdef list to_pylist(self)

    cpdef VectorVector take(self, int32_t[::1] indices)


cdef VectorVector from_arrow(object array)
