# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""
Math kernels for the embedding code path.

Embeddings are stored as IEEE binary16 inside a draken VectorVector. These
kernels widen to fp32 internally (via draken/src/core/fp16.h, which selects
NEON FCVTL / x86 F16C / scalar fallback at compile time) so the storage stays
half-precision while compute does not lose accumulation precision.

The argsort routine mirrors numpy.argsort's stable ordering. No third-party
code is vendored here.
"""

from cpython.mem cimport PyMem_Free, PyMem_Malloc

from libc.math cimport sqrt as c_sqrt
from libc.stdint cimport int32_t, uint8_t, uint16_t
from libc.stdlib cimport free
from libc.string cimport memcpy, memset

from draken.vectors.vector_vector cimport VectorVector


cdef extern from "fp16.h" nogil:
    float draken_fp16_to_fp32(uint16_t h)
    uint16_t draken_fp32_to_fp16(float f)


cpdef VectorVector new_matrix(Py_ssize_t length, Py_ssize_t dimensions):
    """Allocate a zero-initialized VectorVector of `length` rows × `dimensions`.

    No null bitmap is allocated; all rows are present. The buffer is owned by
    the returned object and freed in __dealloc__.
    """
    if length < 0 or dimensions < 0:
        raise ValueError("length and dimensions must be non-negative")

    cdef VectorVector vv = VectorVector.__new__(VectorVector)
    vv._length = length
    vv._dimensions = dimensions
    vv._null_bitmap = NULL
    vv._owns_null_bitmap = False
    vv._arrow_parent = None
    vv._arrow_data_buf = None
    vv._arrow_null_buf = None

    cdef Py_ssize_t total = length * dimensions
    if total == 0:
        vv._data = NULL
        vv._owns_data = False
        return vv

    cdef uint16_t* buf = <uint16_t*> PyMem_Malloc(<size_t>(total * sizeof(uint16_t)))
    if buf == NULL:
        raise MemoryError()
    memset(buf, 0, <size_t>(total * sizeof(uint16_t)))
    vv._data = buf
    vv._owns_data = True
    return vv


cpdef VectorVector new_matrix_with_nulls(Py_ssize_t length, Py_ssize_t dimensions):
    """Allocate a VectorVector of `length` × `dimensions` with all rows null.

    Use `mark_present(vv, row_idx)` to mark a row as present after writing
    its data via `pack_fp32_row` / `write_row_bytes`.
    """
    cdef VectorVector vv = new_matrix(length, dimensions)
    if length == 0:
        return vv
    cdef Py_ssize_t nb_bytes = (length + 7) >> 3
    cdef uint8_t* bitmap = <uint8_t*> PyMem_Malloc(<size_t> nb_bytes)
    if bitmap == NULL:
        raise MemoryError()
    memset(bitmap, 0, <size_t> nb_bytes)
    vv._null_bitmap = bitmap
    vv._owns_null_bitmap = True
    return vv


cpdef void mark_present(VectorVector vv, Py_ssize_t row_idx) except *:
    """Mark row `row_idx` as present (1) in the null bitmap."""
    if row_idx < 0 or row_idx >= vv._length:
        raise IndexError(row_idx)
    if vv._null_bitmap == NULL:
        return
    vv._null_bitmap[row_idx >> 3] |= <uint8_t>(1 << (row_idx & 7))


cpdef void pack_fp32_row(VectorVector vv, Py_ssize_t row_idx, float[::1] src) except *:
    """Convert `src` (fp32) into row `row_idx` of `vv` (fp16). Lengths must match `vv.dimensions`."""
    if row_idx < 0 or row_idx >= vv._length:
        raise IndexError(row_idx)
    if src.shape[0] != vv._dimensions:
        raise ValueError(
            f"row width {src.shape[0]} does not match VectorVector dimensions {vv._dimensions}"
        )
    cdef uint16_t* dst = vv._data + row_idx * vv._dimensions
    cdef Py_ssize_t j
    for j in range(vv._dimensions):
        dst[j] = draken_fp32_to_fp16(src[j])


cpdef void pack_static_hash_row(
    VectorVector vv,
    Py_ssize_t row_idx,
    int32_t[::1] indices,
    float[::1] contributions,
) except *:
    """Accumulate `contributions[k]` into `scratch[indices[k]]`, L2-normalize, store fp16 in row.

    `scratch` is a stack-style fp32 buffer of length `vv.dimensions`, allocated and freed inside
    this call so the entire accumulate-normalize-pack sequence runs in C without Python steps.
    """
    if row_idx < 0 or row_idx >= vv._length:
        raise IndexError(row_idx)
    if indices.shape[0] != contributions.shape[0]:
        raise ValueError("indices and contributions must have the same length")

    cdef Py_ssize_t dim = vv._dimensions
    cdef Py_ssize_t n = indices.shape[0]
    cdef uint16_t* dst = vv._data + row_idx * dim

    if dim == 0:
        return

    cdef float* scratch = <float*> PyMem_Malloc(<size_t>(dim * sizeof(float)))
    if scratch == NULL:
        raise MemoryError()
    memset(scratch, 0, <size_t>(dim * sizeof(float)))

    cdef Py_ssize_t k, j
    cdef int32_t idx
    for k in range(n):
        idx = indices[k]
        if idx < 0 or idx >= dim:
            PyMem_Free(scratch)
            raise IndexError(idx)
        scratch[idx] += contributions[k]

    cdef float norm_sq = 0.0
    for j in range(dim):
        norm_sq += scratch[j] * scratch[j]

    cdef float norm
    if norm_sq == 0.0:
        memset(dst, 0, <size_t>(dim * sizeof(uint16_t)))
    else:
        norm = c_sqrt(norm_sq)
        for j in range(dim):
            dst[j] = draken_fp32_to_fp16(scratch[j] / norm)

    PyMem_Free(scratch)


cpdef bytes row_bytes(VectorVector vv, Py_ssize_t row_idx):
    """Return the raw fp16 bytes for row `row_idx`. Used for cache storage."""
    if row_idx < 0 or row_idx >= vv._length:
        raise IndexError(row_idx)
    cdef Py_ssize_t nbytes = vv._dimensions * sizeof(uint16_t)
    cdef const char* src = <const char*>(vv._data + row_idx * vv._dimensions)
    return src[:nbytes]


cpdef void write_row_bytes(VectorVector vv, Py_ssize_t row_idx, bytes data) except *:
    """Copy raw fp16 bytes into row `row_idx`."""
    if row_idx < 0 or row_idx >= vv._length:
        raise IndexError(row_idx)
    cdef Py_ssize_t nbytes = vv._dimensions * sizeof(uint16_t)
    if len(data) != nbytes:
        raise ValueError(
            f"row payload size {len(data)} does not match dimensions × 2 = {nbytes}"
        )
    cdef const char* src = data
    memcpy(<void*>(vv._data + row_idx * vv._dimensions), <const void*> src, <size_t> nbytes)


cpdef object row_as_fp32_array(VectorVector vv, Py_ssize_t row_idx):
    """Return an ``array.array('f')`` of length `dimensions` widened from row `row_idx`."""
    if row_idx < 0 or row_idx >= vv._length:
        raise IndexError(row_idx)

    from array import array as _array
    cdef Py_ssize_t dim = vv._dimensions
    cdef object out = _array('f', bytes(dim * sizeof(float)))
    cdef float[::1] view = out
    cdef const uint16_t* row = vv._data + row_idx * dim
    cdef Py_ssize_t j
    for j in range(dim):
        view[j] = draken_fp16_to_fp32(row[j])
    return out


cpdef double dot_fp16(VectorVector vv, Py_ssize_t a_idx, Py_ssize_t b_idx) except? 0.0:
    """Dot product of two rows of `vv`, accumulated in fp32."""
    if a_idx < 0 or a_idx >= vv._length:
        raise IndexError(a_idx)
    if b_idx < 0 or b_idx >= vv._length:
        raise IndexError(b_idx)
    cdef const uint16_t* a = vv._data + a_idx * vv._dimensions
    cdef const uint16_t* b = vv._data + b_idx * vv._dimensions
    cdef float acc = 0.0
    cdef Py_ssize_t j
    for j in range(vv._dimensions):
        acc += draken_fp16_to_fp32(a[j]) * draken_fp16_to_fp32(b[j])
    return acc


cpdef list argsort(object values, bint reverse=False):
    """Indices that would sort `values` (ascending by default).

    Operates on Python sequences of comparable scalars (BM25 scores, similarity
    scores, etc.). This is *not* a vector op and intentionally stays generic.
    """
    cdef Py_ssize_t n = len(values)
    cdef list paired = [(values[i], i) for i in range(n)]
    paired.sort(reverse=reverse)
    return [pair[1] for pair in paired]
