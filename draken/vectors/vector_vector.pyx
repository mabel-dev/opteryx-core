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
VectorVector: dense IEEE binary16 (fp16) embedding column.

This is a specialized column type for ML embedding workloads. It is *not*
a general fixed-size-list — it stores fp16 values only, with row-major
contiguous layout (length * dimensions uint16 values).

Design rationale:
- Embeddings are the only realistic FP16 use case in a query engine.
- They are L2-normalized in practice; binary16's mantissa precision matters,
  bf16's extra exponent range does not. Storage matches Parquet's FLOAT16
  logical type for memcpy-on-load.
- Per-element nulls inside an embedding row are meaningless. We support
  row-level nulls (whole embedding present or absent) and reject Arrow
  inputs with element-level nulls inside present rows.
- The compute surface is small: dot, cosine_similarity, l2_distance.
  No min/max/sum, no group-by, no hash-join keys. We don't pretend
  otherwise.
"""

from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING
from cpython.mem cimport PyMem_Free, PyMem_Malloc

from libc.math cimport sqrt as c_sqrt
from libc.stddef cimport size_t
from libc.stdint cimport int32_t, int64_t, intptr_t, uint8_t, uint16_t, uint32_t, uint64_t
from libc.stdlib cimport free, malloc
from libc.string cimport memcpy, memset

from draken.core.buffers cimport DRAKEN_ARRAY, DrakenVector, draken_identity_sel
from draken.vectors.float32_vector cimport Float32Vector
from draken.vectors.vector cimport Vector


cdef extern from "fp16.h" nogil:
    float draken_fp16_to_fp32(uint16_t h)


# --- Helpers ---------------------------------------------------------------

cdef inline bint _row_is_null(const uint8_t* bitmap, Py_ssize_t idx) noexcept nogil:
    if bitmap == NULL:
        return False
    return ((bitmap[idx >> 3] >> (idx & 7)) & 1u) == 0u


cdef inline void _validate_query(float[::1] query, Py_ssize_t dimensions) except *:
    if query.shape[0] != dimensions:
        raise ValueError(
            f"query vector length {query.shape[0]} does not match "
            f"VectorVector dimensions {dimensions}"
        )


# --- VectorVector class ----------------------------------------------------

cdef class VectorVector(Vector):

    def __cinit__(self):
        self._data = NULL
        self._length = 0
        self._dimensions = 0
        self._owns_data = False
        self._null_bitmap = NULL
        self._owns_null_bitmap = False
        self._arrow_parent = None
        self._arrow_data_buf = None
        self._arrow_null_buf = None

    def __dealloc__(self):
        if self._owns_data and self._data != NULL:
            PyMem_Free(self._data)
        self._data = NULL
        if self._owns_null_bitmap and self._null_bitmap != NULL:
            PyMem_Free(self._null_bitmap)
        self._null_bitmap = NULL

    # --- Unified view ---

    cdef DrakenVector* unified(self) noexcept:
        # VectorVector is always dense at the row level. `data` exposes the raw
        # row-major fp16 buffer; consumers needing per-row semantics downcast
        # to <VectorVector> and read ._dimensions. This mirrors the abstraction
        # crack already accepted for ArrayVector (variable-length rows behind a
        # uniform selection-indexed view).
        self._unified_view.data        = <void*>self._data
        self._unified_view.selection   = draken_identity_sel(<uint32_t>self._length)
        self._unified_view.data_length = <uint32_t>self._length
        self._unified_view.length      = <uint32_t>self._length
        self._unified_view.validity    = self._null_bitmap
        self._unified_view.type        = DRAKEN_ARRAY
        return &self._unified_view

    def _unified_fields_for_test(self):
        """Test-only: return key fields from the unified view as a tuple.

        Returns (data_is_non_null, selection_list_or_None, length, data_length,
                 validity_is_non_null) so Python tests can assert the invariants
                 without native code.
        """
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t> uv.length
        cdef Py_ssize_t i
        sel = None
        if uv.selection != NULL:
            sel = [<Py_ssize_t> uv.selection[i] for i in range(n)]
        return (
            uv.data != NULL,
            sel,
            <Py_ssize_t> uv.length,
            <Py_ssize_t> uv.data_length,
            uv.validity != NULL,
        )

    # --- Vector base overrides ---

    @property
    def length(self):
        return self._length

    def __len__(self):
        return self._length

    @property
    def dimensions(self):
        return self._dimensions

    @property
    def dtype(self):
        # Reuse DRAKEN_ARRAY so existing morsel/dispatch code treats this as a
        # list-shaped column. A dedicated DRAKEN_VECTOR_FP16 enum tag would
        # let callers branch on it explicitly; that is a follow-up requiring
        # changes to draken/src/core/buffers.h and every dispatcher.
        return DRAKEN_ARRAY

    @property
    def nbytes(self):
        cdef uint64_t total = <uint64_t>self._length * self._dimensions * sizeof(uint16_t)
        if self._null_bitmap != NULL:
            total += (<uint64_t>self._length + 7) >> 3
        return total

    cpdef object null_bitmap(self):
        if self._null_bitmap == NULL:
            return None
        cdef Py_ssize_t n = (self._length + 7) >> 3
        return PyBytes_FromStringAndSize(<char*> self._null_bitmap, n)

    cpdef bint is_null_at(self, Py_ssize_t idx) except? False:
        if idx < 0 or idx >= self._length:
            raise IndexError(idx)
        return _row_is_null(self._null_bitmap, idx)

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        """Element-wise lex compare of two embedding rows.

        Defined for ORDER BY / dedup; not a meaningful similarity ordering.
        """
        if left_idx < 0 or left_idx >= self._length:
            raise IndexError(left_idx)
        if right_idx < 0 or right_idx >= self._length:
            raise IndexError(right_idx)

        cdef const uint16_t* lrow = self._data + left_idx * self._dimensions
        cdef const uint16_t* rrow = self._data + right_idx * self._dimensions
        cdef Py_ssize_t i
        cdef float lv, rv
        for i in range(self._dimensions):
            lv = draken_fp16_to_fp32(lrow[i])
            rv = draken_fp16_to_fp32(rrow[i])
            if lv < rv:
                return -1
            if lv > rv:
                return 1
        return 0

    def __getitem__(self, Py_ssize_t i):
        if i < 0:
            i += self._length
        if i < 0 or i >= self._length:
            raise IndexError(i)
        if _row_is_null(self._null_bitmap, i):
            return None
        cdef const uint16_t* row = self._data + i * self._dimensions
        cdef list out = [None] * self._dimensions
        cdef Py_ssize_t j
        for j in range(self._dimensions):
            out[j] = draken_fp16_to_fp32(row[j])
        return out

    cpdef list to_pylist(self):
        cdef list out = [None] * self._length
        cdef Py_ssize_t i, j
        cdef const uint16_t* row
        cdef list row_list
        for i in range(self._length):
            if _row_is_null(self._null_bitmap, i):
                continue
            row = self._data + i * self._dimensions
            row_list = [None] * self._dimensions
            for j in range(self._dimensions):
                row_list[j] = draken_fp16_to_fp32(row[j])
            out[i] = row_list
        return out

    def __str__(self):
        if self._data == NULL:
            return "<VectorVector uninitialized>"
        return (
            f"<VectorVector len={self._length} dimensions={self._dimensions} "
            f"preview={self.to_pylist()[:3]}>"
        )

    # --- Take ---

    cpdef VectorVector take(self, int32_t[::1] indices):
        cdef Py_ssize_t n_out = indices.shape[0]
        cdef Py_ssize_t row_bytes = self._dimensions * sizeof(uint16_t)
        cdef uint16_t* new_data = <uint16_t*> PyMem_Malloc(<size_t>(n_out * row_bytes))
        if new_data == NULL and n_out * row_bytes > 0:
            raise MemoryError()

        cdef Py_ssize_t nb_bytes = (n_out + 7) >> 3
        cdef uint8_t* new_nulls = NULL
        if self._null_bitmap != NULL:
            new_nulls = <uint8_t*> PyMem_Malloc(<size_t> nb_bytes)
            if new_nulls == NULL:
                PyMem_Free(new_data)
                raise MemoryError()
            memset(new_nulls, 0, nb_bytes)

        cdef Py_ssize_t i
        cdef int32_t src_idx
        for i in range(n_out):
            src_idx = indices[i]
            if src_idx < 0 or src_idx >= self._length:
                PyMem_Free(new_data)
                if new_nulls != NULL:
                    PyMem_Free(new_nulls)
                raise IndexError(src_idx)
            memcpy(
                <void*>(<intptr_t> new_data + i * row_bytes),
                <const void*>(<intptr_t> self._data + src_idx * row_bytes),
                <size_t> row_bytes,
            )
            if new_nulls != NULL:
                if not _row_is_null(self._null_bitmap, src_idx):
                    new_nulls[i >> 3] |= <uint8_t>(1 << (i & 7))

        cdef VectorVector out = VectorVector.__new__(VectorVector)
        out._data = new_data
        out._length = n_out
        out._dimensions = self._dimensions
        out._owns_data = True
        out._null_bitmap = new_nulls
        out._owns_null_bitmap = new_nulls != NULL
        return out

    # --- Distance kernels ---
    #
    # All three load fp16, widen to fp32, accumulate in fp32. Compilers
    # auto-vectorize these on AArch64 (NEON FCVTL2 + FMA) and x86-with-F16C
    # (VCVTPH2PS + FMA). Explicit SIMD intrinsics are a follow-up.
    #
    # Returns a Float32Vector of length self._length. Null input rows
    # propagate as nulls (output null bitmap mirrors input row null bitmap).

    cdef Float32Vector _alloc_score_vector(self):
        """Allocate a Float32Vector(length) and copy our row null bitmap.

        The output null bitmap is a byte-for-byte copy of the input row
        bitmap; one input row produces exactly one output scalar.
        """
        cdef Float32Vector out = Float32Vector(<size_t> self._length)
        cdef Py_ssize_t nb_bytes
        cdef uint8_t* new_nulls
        if self._null_bitmap != NULL:
            nb_bytes = (self._length + 7) >> 3
            new_nulls = <uint8_t*> malloc(<size_t> nb_bytes)
            if new_nulls == NULL:
                raise MemoryError()
            memcpy(new_nulls, self._null_bitmap, <size_t> nb_bytes)
            out.ptr.null_bitmap = new_nulls
        else:
            out.ptr.null_bitmap = NULL
        return out

    cpdef Float32Vector dot(self, float[::1] query):
        _validate_query(query, self._dimensions)
        cdef Float32Vector out = self._alloc_score_vector()
        cdef float* dst = <float*> out.ptr.data
        cdef Py_ssize_t i, j
        cdef const uint16_t* row
        cdef float acc
        for i in range(self._length):
            if _row_is_null(self._null_bitmap, i):
                dst[i] = 0.0
                continue
            row = self._data + i * self._dimensions
            acc = 0.0
            for j in range(self._dimensions):
                acc += draken_fp16_to_fp32(row[j]) * query[j]
            dst[i] = acc
        return out

    cpdef Float32Vector cosine_similarity(self, float[::1] query):
        _validate_query(query, self._dimensions)
        cdef Py_ssize_t i, j
        cdef float qnorm_sq = 0.0
        for j in range(self._dimensions):
            qnorm_sq += query[j] * query[j]
        cdef float qnorm = c_sqrt(qnorm_sq)

        cdef Float32Vector out = self._alloc_score_vector()
        cdef float* dst = <float*> out.ptr.data
        cdef const uint16_t* row
        cdef float dot_p, rnorm_sq, rv
        for i in range(self._length):
            if _row_is_null(self._null_bitmap, i):
                dst[i] = 0.0
                continue
            row = self._data + i * self._dimensions
            dot_p = 0.0
            rnorm_sq = 0.0
            for j in range(self._dimensions):
                rv = draken_fp16_to_fp32(row[j])
                dot_p += rv * query[j]
                rnorm_sq += rv * rv
            if rnorm_sq == 0.0 or qnorm == 0.0:
                dst[i] = 0.0
            else:
                dst[i] = dot_p / (c_sqrt(rnorm_sq) * qnorm)
        return out

    cpdef Float32Vector l2_distance(self, float[::1] query):
        _validate_query(query, self._dimensions)
        cdef Float32Vector out = self._alloc_score_vector()
        cdef float* dst = <float*> out.ptr.data
        cdef Py_ssize_t i, j
        cdef const uint16_t* row
        cdef float diff, acc
        for i in range(self._length):
            if _row_is_null(self._null_bitmap, i):
                dst[i] = 0.0
                continue
            row = self._data + i * self._dimensions
            acc = 0.0
            for j in range(self._dimensions):
                diff = draken_fp16_to_fp32(row[j]) - query[j]
                acc += diff * diff
            dst[i] = c_sqrt(acc)
        return out

    # --- Arrow round-trip ---

    def to_arrow(self):
        import pyarrow as pa

        if self._data == NULL or self._length == 0:
            return pa.array([], type=pa.list_(pa.float16(), self._dimensions))

        cdef Py_ssize_t n_values = self._length * self._dimensions
        cdef Py_ssize_t data_bytes = n_values * sizeof(uint16_t)
        cdef Py_ssize_t nb_bytes = (self._length + 7) >> 3

        list_null = None
        if self._null_bitmap != NULL:
            list_null = pa.foreign_buffer(
                <intptr_t> self._null_bitmap, nb_bytes, base=self,
            )

        data_buf = pa.foreign_buffer(
            <intptr_t> self._data, data_bytes, base=self,
        )
        child = pa.Array.from_buffers(
            pa.float16(), n_values, [None, data_buf],
        )
        return pa.Array.from_buffers(
            pa.list_(pa.float16(), self._dimensions),
            self._length,
            [list_null],
            children=[child],
        )


