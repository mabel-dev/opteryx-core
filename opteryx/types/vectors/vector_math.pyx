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

Embeddings are stored as IEEE binary16 inside a draken Vector (type VECTOR_FP16).
These kernels widen to fp32 internally (via draken/src/core/fp16.h, which selects
NEON FCVTL / x86 F16C / scalar fallback at compile time) so the storage stays
half-precision while compute does not lose accumulation precision.

The uniform access pattern is data[selection[i]] for i in [0, length).
For VECTOR_FP16: each logical row occupies `dimensions` uint16_t slots, so
the row start in the flat buffer is at:
    (<uint16_t*> dv.data) + dv.selection[row_idx] * dim

Dimensions are recovered via vec._nb.logical_type_dimension (one Python call
per function, not per element — acceptable for this non-tight-loop context).

The argsort routine mirrors numpy.argsort's stable ordering. No third-party
code is vendored here.
"""

from cpython.mem cimport PyMem_Free, PyMem_Malloc

from libc.math cimport sqrt as c_sqrt
from libc.stdint cimport int32_t, uint8_t, uint16_t, uint32_t
from libc.stdlib cimport free
from libc.string cimport memcpy, memset

from draken.vectors.vector cimport Vector
from draken.core.buffers cimport DrakenVector

import draken.draken_native as _draken_native_vmath


cdef extern from "fp16.h" nogil:
    float draken_fp16_to_fp32(uint16_t h)
    uint16_t draken_fp32_to_fp16(float f)


cpdef Vector new_matrix(Py_ssize_t length, Py_ssize_t dimensions):
    """Allocate a zero-initialized VECTOR_FP16 Vector of `length` rows × `dimensions`.

    No nulls; all rows are valid. The buffer is owned by the returned Vector
    and freed when it is garbage-collected.
    """
    if length < 0 or dimensions < 0:
        raise ValueError("length and dimensions must be non-negative")
    return Vector(_draken_native_vmath.vector_fp16_zeros(length, dimensions))


cpdef Vector new_matrix_with_nulls(Py_ssize_t length, Py_ssize_t dimensions):
    """Allocate a VECTOR_FP16 Vector of `length` × `dimensions` with all rows null.

    Use `mark_present(vec, row_idx)` to mark a row valid after writing its
    data via `pack_fp32_row` / `write_row_bytes`.
    """
    if length < 0 or dimensions < 0:
        raise ValueError("length and dimensions must be non-negative")
    return Vector(_draken_native_vmath.vector_fp16_with_nulls(length, dimensions))


cpdef void mark_present(Vector vec, Py_ssize_t row_idx) except *:
    """Mark logical row `row_idx` as valid (present) in the null bitmap.

    draken convention: validity bit = 1 means valid; bit = 0 means NULL.
    `vector_fp16_with_nulls` initialises all bits to 0 (all null).
    """
    cdef DrakenVector* dv = vec.unified()
    if row_idx < 0 or row_idx >= <Py_ssize_t> dv.length:
        raise IndexError(row_idx)
    if dv.validity == NULL:
        return  # all rows already valid (no bitmap allocated)
    dv.validity[row_idx >> 3] |= <uint8_t>(1 << (row_idx & 7))


cpdef void pack_fp32_row(Vector vec, Py_ssize_t row_idx, float[::1] src) except *:
    """Convert `src` (fp32) into row `row_idx` of `vec` (stored as fp16)."""
    cdef DrakenVector* dv = vec.unified()
    cdef Py_ssize_t dim = <Py_ssize_t> vec._nb.logical_type_dimension
    if row_idx < 0 or row_idx >= <Py_ssize_t> dv.length:
        raise IndexError(row_idx)
    if src.shape[0] != dim:
        raise ValueError(
            f"row width {src.shape[0]} does not match Vector dimensions {dim}"
        )
    cdef uint16_t* dst = (<uint16_t*> dv.data) + <Py_ssize_t>(dv.selection[row_idx]) * dim
    cdef Py_ssize_t j
    for j in range(dim):
        dst[j] = draken_fp32_to_fp16(src[j])


cpdef void pack_static_hash_row(
    Vector vec,
    Py_ssize_t row_idx,
    int32_t[::1] indices,
    float[::1] contributions,
) except *:
    """Accumulate `contributions[k]` into `scratch[indices[k]]`, L2-normalise, store fp16 in row.

    `scratch` is a stack-style fp32 buffer of length `dim`, allocated and freed
    inside this call so the entire accumulate-normalise-pack sequence runs in C
    without Python steps.
    """
    cdef DrakenVector* dv = vec.unified()
    cdef Py_ssize_t dim = <Py_ssize_t> vec._nb.logical_type_dimension
    if row_idx < 0 or row_idx >= <Py_ssize_t> dv.length:
        raise IndexError(row_idx)
    if indices.shape[0] != contributions.shape[0]:
        raise ValueError("indices and contributions must have the same length")

    cdef Py_ssize_t n = indices.shape[0]
    cdef uint16_t* dst = (<uint16_t*> dv.data) + <Py_ssize_t>(dv.selection[row_idx]) * dim

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


cpdef bytes row_bytes(Vector vec, Py_ssize_t row_idx):
    """Return the raw fp16 bytes for logical row `row_idx`. Used for cache storage."""
    cdef DrakenVector* dv = vec.unified()
    cdef Py_ssize_t dim = <Py_ssize_t> vec._nb.logical_type_dimension
    if row_idx < 0 or row_idx >= <Py_ssize_t> dv.length:
        raise IndexError(row_idx)
    cdef Py_ssize_t nbytes = dim * sizeof(uint16_t)
    cdef const char* src = <const char*>(
        (<uint16_t*> dv.data) + <Py_ssize_t>(dv.selection[row_idx]) * dim
    )
    return src[:nbytes]


cpdef void write_row_bytes(Vector vec, Py_ssize_t row_idx, bytes data) except *:
    """Copy raw fp16 bytes into logical row `row_idx`."""
    cdef DrakenVector* dv = vec.unified()
    cdef Py_ssize_t dim = <Py_ssize_t> vec._nb.logical_type_dimension
    if row_idx < 0 or row_idx >= <Py_ssize_t> dv.length:
        raise IndexError(row_idx)
    cdef Py_ssize_t nbytes = dim * sizeof(uint16_t)
    if len(data) != nbytes:
        raise ValueError(
            f"row payload size {len(data)} does not match dimensions × 2 = {nbytes}"
        )
    cdef const char* src = data
    memcpy(
        <void*>((<uint16_t*> dv.data) + <Py_ssize_t>(dv.selection[row_idx]) * dim),
        <const void*> src,
        <size_t> nbytes,
    )


cpdef object row_as_fp32_array(Vector vec, Py_ssize_t row_idx):
    """Return an ``array.array('f')`` of length `dimensions` widened from row `row_idx`."""
    cdef DrakenVector* dv = vec.unified()
    cdef Py_ssize_t dim = <Py_ssize_t> vec._nb.logical_type_dimension
    if row_idx < 0 or row_idx >= <Py_ssize_t> dv.length:
        raise IndexError(row_idx)

    from array import array as _array
    cdef object out = _array('f', bytes(dim * sizeof(float)))
    cdef float[::1] view = out
    cdef const uint16_t* row = (
        (<uint16_t*> dv.data) + <Py_ssize_t>(dv.selection[row_idx]) * dim
    )
    cdef Py_ssize_t j
    for j in range(dim):
        view[j] = draken_fp16_to_fp32(row[j])
    return out


cpdef double dot_fp16(Vector vec, Py_ssize_t a_idx, Py_ssize_t b_idx) except? 0.0:
    """Dot product of two logical rows of `vec`, accumulated in fp32."""
    cdef DrakenVector* dv = vec.unified()
    cdef Py_ssize_t dim = <Py_ssize_t> vec._nb.logical_type_dimension
    if a_idx < 0 or a_idx >= <Py_ssize_t> dv.length:
        raise IndexError(a_idx)
    if b_idx < 0 or b_idx >= <Py_ssize_t> dv.length:
        raise IndexError(b_idx)
    cdef const uint16_t* a = (
        (<uint16_t*> dv.data) + <Py_ssize_t>(dv.selection[a_idx]) * dim
    )
    cdef const uint16_t* b_row = (
        (<uint16_t*> dv.data) + <Py_ssize_t>(dv.selection[b_idx]) * dim
    )
    cdef float acc = 0.0
    cdef Py_ssize_t j
    for j in range(dim):
        acc += draken_fp16_to_fp32(a[j]) * draken_fp16_to_fp32(b_row[j])
    return acc


cpdef list cosine_similarity_rows(
    Vector vec,
    Py_ssize_t start,
    Py_ssize_t count,
    float[::1] query,
) except *:
    """Cosine similarity of rows [start, start+count) of `vec` against `query` (fp32).

    Returns a Python list of float, one per row. Rows [start, start+count) are
    compared against the caller-supplied fp32 query vector. Both sides are
    widened from fp16 on the fly; no temporary allocation needed.

    Null rows in `vec` produce a score of 0.0 (null semantics: missing data is
    not similar to anything).
    """
    cdef DrakenVector* dv = vec.unified()
    cdef Py_ssize_t dim = <Py_ssize_t> vec._nb.logical_type_dimension
    if start < 0 or count < 0 or start + count > <Py_ssize_t> dv.length:
        raise IndexError(
            f"row range [{start}, {start + count}) out of bounds for length {dv.length}"
        )
    if query.shape[0] != dim:
        raise ValueError(
            f"query width {query.shape[0]} does not match vector dimensions {dim}"
        )

    # Pre-compute query L2 norm once.
    cdef float q_norm_sq = 0.0
    cdef Py_ssize_t j
    for j in range(dim):
        q_norm_sq += query[j] * query[j]
    cdef float q_norm = c_sqrt(q_norm_sq) if q_norm_sq > 0.0 else 0.0

    cdef list result = [0.0] * count
    if q_norm == 0.0:
        return result

    cdef Py_ssize_t i
    cdef float fp_val, dot, row_norm_sq, row_norm
    cdef const uint16_t* row_ptr
    for i in range(count):
        row_ptr = (<uint16_t*> dv.data) + <Py_ssize_t>(dv.selection[start + i]) * dim
        dot = 0.0
        row_norm_sq = 0.0
        for j in range(dim):
            fp_val = draken_fp16_to_fp32(row_ptr[j])
            dot += fp_val * query[j]
            row_norm_sq += fp_val * fp_val
        if row_norm_sq > 0.0:
            row_norm = c_sqrt(row_norm_sq)
            result[i] = dot / (row_norm * q_norm)
    return result


cpdef list argsort(object values, bint reverse=False):
    """Indices that would sort `values` (ascending by default).

    Operates on Python sequences of comparable scalars (BM25 scores, similarity
    scores, etc.). This is *not* a vector op and intentionally stays generic.
    """
    cdef Py_ssize_t n = len(values)
    cdef list paired = [(values[i], i) for i in range(n)]
    paired.sort(reverse=reverse)
    return [pair[1] for pair in paired]
