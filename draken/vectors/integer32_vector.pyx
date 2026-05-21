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

from cpython.bytes cimport PyBytes_AS_STRING, PyBytes_FromStringAndSize
from libc.stddef cimport size_t
from libc.stdint cimport int32_t, int64_t, uint8_t, uint32_t, uint64_t, intptr_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset, memcpy

from draken.core.buffers cimport DrakenFixedBuffer, DrakenType
from draken.core.buffers cimport DRAKEN_INT32
from draken.core.buffers cimport DrakenVector
from draken.core.buffers cimport draken_vector_from_dense, draken_vector_from_constant
from draken.core.fixed_vector cimport (
    alloc_fixed_buffer, buf_dtype, buf_itemsize, buf_length, free_fixed_buffer,
)
from draken.vectors.vector cimport MIX_HASH_CONSTANT, NULL_HASH, Vector, mix_hash, simd_mix_hash, simd_popcount
from draken.vectors.bool_vector cimport BoolVector

cdef extern from "simd_bitops.h" nogil:
    void simd_and_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n)

cdef extern from "draken/vectors/_integer_compare.hpp" namespace "draken::integer_cmp" nogil:
    bint dispatch_compare_once(int op, int64_t a, int64_t b)
    void dispatch_scalar_nonnull_i32(int op, const int32_t* data, int64_t value, uint8_t* dst, size_t n)
    void dispatch_scalar_branchless_i32(int op, const int32_t* data, int64_t value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_scalar_branching_i32(int op, const int32_t* data, int64_t value, const uint8_t* src_null, uint8_t* dst, size_t n)
    void dispatch_vector_nonnull_i32_i32(int op, const int32_t* a, const int32_t* b, uint8_t* dst, size_t n)
    void dispatch_vector_one_null_branchless_i32_i32(int op, const int32_t* a, const int32_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_one_null_branching_i32_i32(int op, const int32_t* a, const int32_t* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branchless_i32_i32(int op, const int32_t* a, const int32_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)
    void dispatch_vector_both_null_branching_i32_i32(int op, const int32_t* a, const int32_t* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)

DEF INTEGER_HASH_CHUNK = 1024
cdef uint8_t _CONST_NULL_BYTE = 0


cdef inline bint _bitmap_is_valid(uint8_t* bitmap, Py_ssize_t idx) noexcept nogil:
    return (bitmap[idx >> 3] >> (idx & 7)) & 1


cdef class Integer32Vector(Vector):
    """Fixed-width signed 32-bit integer vector (INT32, itemsize=4)."""

    @classmethod
    def from_constant(cls, value, length, is_null=False):
        cdef int64_t ivalue = 0
        if length < 0:
            raise ValueError("length must be non-negative")
        if value is None and not is_null:
            raise ValueError("value cannot be None unless is_null=True")
        if not is_null and value is not None:
            ivalue = <int64_t>int(value)
        cdef Integer32Vector vec = Integer32Vector(<size_t>length)
        if not (is_null or value is None):
            (<int32_t*>vec.ptr.data)[0] = <int32_t>ivalue
        vec.ptr.length = <size_t>length
        vec._unified_view = draken_vector_from_constant(
            vec.ptr.data, <uint32_t>length, DRAKEN_INT32,
            &_CONST_NULL_BYTE if is_null else NULL)
        return vec

    def __cinit__(self, size_t length=0, bint wrap=False):
        if wrap:
            self.ptr = NULL
            self.owns_data = False
            self._unified_view = draken_vector_from_dense(NULL, 0, DRAKEN_INT32, NULL)
        else:
            self.ptr = alloc_fixed_buffer(DRAKEN_INT32, length, 4)
            self.owns_data = True
            self._unified_view = draken_vector_from_dense(
                self.ptr.data, <uint32_t>length, DRAKEN_INT32, NULL)

    def __dealloc__(self):
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    cdef DrakenVector* unified(self) noexcept:
        return &self._unified_view

    @property
    def length(self):
        return self.ptr.length

    def __len__(self):
        return self.ptr.length

    @property
    def itemsize(self):
        return 4

    @property
    def dtype(self):
        return DRAKEN_INT32

    @property
    def null_count(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        if uv.validity == NULL:
            return 0
        return n - <Py_ssize_t>simd_popcount(uv.validity, (<size_t>n + 7) >> 3)

    def __getitem__(self, Py_ssize_t i):
        cdef DrakenVector* uv = self.unified()
        if i < 0 or i >= <Py_ssize_t>uv.length:
            raise IndexError("Index out of bounds")
        if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
            return None
        return <int32_t>(<int32_t*>uv.data)[uv.selection[i]]

    def to_arrow(self):
        import pyarrow as pa
        cdef DrakenVector* uv = self.unified()
        if uv.data_length == 1:
            val = <int32_t>(<int32_t*>uv.data)[0]
            if uv.validity != NULL:
                return pa.nulls(self.ptr.length, type=pa.int32())
            return pa.array([val] * self.ptr.length, type=pa.int32())
        cdef size_t nbytes = self.ptr.length * 4
        cdef intptr_t addr = <intptr_t>self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)
        buffers = [None, data_buf]
        if self.ptr.null_bitmap != NULL:
            buffers[0] = pa.foreign_buffer(
                <intptr_t>self.ptr.null_bitmap,
                (self.ptr.length + 7) // 8,
                base=self,
            )
        return pa.Array.from_buffers(pa.int32(), self.ptr.length, buffers)

    cpdef list to_pylist(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = <Py_ssize_t>uv.length
        cdef int32_t* data = <int32_t*>uv.data
        cdef list out = []
        for i in range(n):
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                out.append(None)
            else:
                out.append(data[<Py_ssize_t>uv.selection[i]])
        return out

    cpdef int64_t min(self):
        cdef DrakenVector* uv = self.unified()
        cdef int32_t* data = <int32_t*>uv.data
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t i, start
        cdef int64_t m
        cdef bint seen = False

        if n == 0:
            raise ValueError("Cannot compute min of empty column")

        if uv.validity == NULL:
            m = <int64_t>data[uv.selection[0]]
            with nogil:
                for i in range(1, n):
                    if <int64_t>data[uv.selection[i]] < m:
                        m = <int64_t>data[uv.selection[i]]
            return m
        else:
            for i in range(n):
                if (uv.validity[i >> 3] >> (i & 7)) & 1:
                    m = <int64_t>data[uv.selection[i]]
                    seen = True
                    start = i + 1
                    break
            if not seen:
                raise ValueError("Cannot compute min of all-null column")
            with nogil:
                for i in range(start, n):
                    if (uv.validity[i >> 3] >> (i & 7)) & 1:
                        if <int64_t>data[uv.selection[i]] < m:
                            m = <int64_t>data[uv.selection[i]]
            return m

    cpdef int64_t max(self):
        cdef DrakenVector* uv = self.unified()
        cdef int32_t* data = <int32_t*>uv.data
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t i, start
        cdef int64_t m
        cdef bint seen = False

        if n == 0:
            raise ValueError("Cannot compute max of empty column")

        if uv.validity == NULL:
            m = <int64_t>data[uv.selection[0]]
            with nogil:
                for i in range(1, n):
                    if <int64_t>data[uv.selection[i]] > m:
                        m = <int64_t>data[uv.selection[i]]
            return m
        else:
            for i in range(n):
                if (uv.validity[i >> 3] >> (i & 7)) & 1:
                    m = <int64_t>data[uv.selection[i]]
                    seen = True
                    start = i + 1
                    break
            if not seen:
                raise ValueError("Cannot compute max of all-null column")
            with nogil:
                for i in range(start, n):
                    if (uv.validity[i >> 3] >> (i & 7)) & 1:
                        if <int64_t>data[uv.selection[i]] > m:
                            m = <int64_t>data[uv.selection[i]]
            return m

    cpdef int64_t sum(self):
        cdef DrakenVector* uv = self.unified()
        cdef int32_t* data = <int32_t*>uv.data
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t i
        cdef int64_t total = 0

        if uv.validity == NULL:
            with nogil:
                for i in range(n):
                    total += <int64_t>data[uv.selection[i]]
        else:
            with nogil:
                for i in range(n):
                    if (uv.validity[i >> 3] >> (i & 7)) & 1:
                        total += <int64_t>data[uv.selection[i]]
        return total

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        cdef DrakenVector* uv = self.unified()
        cdef uint8_t* null_bitmap = uv.validity
        cdef bint left_is_null = null_bitmap != NULL and not ((null_bitmap[left_idx >> 3] >> (left_idx & 7)) & 1)
        cdef bint right_is_null = null_bitmap != NULL and not ((null_bitmap[right_idx >> 3] >> (right_idx & 7)) & 1)
        if left_is_null or right_is_null:
            return 0
        cdef int32_t* data = <int32_t*>uv.data
        cdef int64_t left_val = <int64_t>data[uv.selection[left_idx]]
        cdef int64_t right_val = <int64_t>data[uv.selection[right_idx]]
        if left_val < right_val: return -1
        elif left_val > right_val: return 1
        else: return 0

    cpdef bint is_null_at(self, Py_ssize_t idx) except? False:
        cdef DrakenVector* uv = self.unified()
        if uv.validity == NULL:
            return False
        return ((uv.validity[idx >> 3] >> (idx & 7)) & 1) == 0

    cpdef Integer32Vector take(self, int32_t[::1] indices):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef Integer32Vector out = Integer32Vector(<size_t>n)
        cdef int32_t* data = <int32_t*>uv.data
        cdef int32_t* dst = <int32_t*>out.ptr.data
        cdef uint8_t* src_null = uv.validity
        cdef uint8_t* out_null = NULL
        cdef int32_t src_idx
        cdef Py_ssize_t out_nbytes

        if src_null != NULL and n > 0:
            out_nbytes = (n + 7) >> 3
            out_null = <uint8_t*>malloc(<size_t>out_nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, <size_t>out_nbytes)

        for i in range(n):
            src_idx = indices[i]
            if src_null != NULL and not ((src_null[src_idx >> 3] >> (src_idx & 7)) & 1):
                dst[i] = 0
            else:
                dst[i] = data[<Py_ssize_t>uv.selection[<Py_ssize_t>src_idx]]
                if out_null != NULL:
                    out_null[i >> 3] |= <uint8_t>(1 << (i & 7))

        out.ptr.null_bitmap = out_null
        out._unified_view = draken_vector_from_dense(
            out.ptr.data, <uint32_t>n, DRAKEN_INT32, out.ptr.null_bitmap)
        return out

    cdef BoolVector _make_all_null_bool(self, Py_ssize_t n):
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef uint8_t* null_bm
        memset(dst, 0, nbytes)
        if nbytes != 0:
            null_bm = <uint8_t*>malloc(nbytes)
            if null_bm == NULL:
                raise MemoryError()
            memset(null_bm, 0, nbytes)
            out.ptr.null_bitmap = null_bm
        else:
            out.ptr.null_bitmap = NULL
        return out

    cdef BoolVector _compare_scalar(self, int64_t value, int op):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*>out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask
        cdef int32_t* data = <int32_t*>uv.data
        cdef Py_ssize_t i

        if nbytes > 0:
            memset(dst, 0, nbytes)

        if uv.validity != NULL and nbytes != 0:
            out_null = <uint8_t*>malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, uv.validity, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        for i in range(n):
            if uv.validity == NULL or ((uv.validity[i >> 3] >> (i & 7)) & 1):
                if dispatch_compare_once(op, <int64_t>data[uv.selection[i]], value):
                    dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        return out

    cdef BoolVector _compare_vector(self, Integer32Vector other, int op):
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVector* ouv = other.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef int32_t* data1 = <int32_t*>uv.data
        cdef int32_t* data2 = <int32_t*>ouv.data
        cdef Py_ssize_t i
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef bint null1, null2

        if n != <Py_ssize_t>ouv.length:
            raise ValueError("Vectors must have the same length")

        out = BoolVector(<size_t>n)
        dst = <uint8_t*>out.ptr.data
        if nbytes > 0:
            memset(dst, 0, nbytes)

        if (uv.validity != NULL or ouv.validity != NULL) and nbytes != 0:
            out_null = <uint8_t*>malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        for i in range(n):
            null1 = uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1)
            null2 = ouv.validity != NULL and not ((ouv.validity[i >> 3] >> (i & 7)) & 1)
            if null1 or null2:
                continue
            if dispatch_compare_once(op, <int64_t>data1[uv.selection[i]], <int64_t>data2[ouv.selection[i]]):
                dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            if out_null != NULL:
                out_null[i >> 3] |= <uint8_t>(1 << (i & 7))
        return out

    cpdef BoolVector equals(self, int64_t value):
        return self._compare_scalar(value, 0)

    cpdef BoolVector equals_vector(self, Integer32Vector other):
        return self._compare_vector(other, 0)

    cpdef BoolVector not_equals(self, int64_t value):
        return self._compare_scalar(value, 1)

    cpdef BoolVector not_equals_vector(self, Integer32Vector other):
        return self._compare_vector(other, 1)

    cpdef BoolVector greater_than(self, int64_t value):
        return self._compare_scalar(value, 2)

    cpdef BoolVector greater_than_vector(self, Integer32Vector other):
        return self._compare_vector(other, 2)

    cpdef BoolVector greater_than_or_equals(self, int64_t value):
        return self._compare_scalar(value, 3)

    cpdef BoolVector greater_than_or_equals_vector(self, Integer32Vector other):
        return self._compare_vector(other, 3)

    cpdef BoolVector less_than(self, int64_t value):
        return self._compare_scalar(value, 4)

    cpdef BoolVector less_than_vector(self, Integer32Vector other):
        return self._compare_vector(other, 4)

    cpdef BoolVector less_than_or_equals(self, int64_t value):
        return self._compare_scalar(value, 5)

    cpdef BoolVector less_than_or_equals_vector(self, Integer32Vector other):
        return self._compare_vector(other, 5)

    cpdef BoolVector between(self, int64_t lower, int64_t upper,
                              bint lower_inclusive=True, bint upper_inclusive=True):
        cdef BoolVector lo, hi
        if lower_inclusive:
            lo = self._compare_scalar(lower, 3)
        else:
            lo = self._compare_scalar(lower, 2)
        if upper_inclusive:
            hi = self._compare_scalar(upper, 5)
        else:
            hi = self._compare_scalar(upper, 4)
        return lo.and_vector(hi)

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0,
    ) except *:
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        cdef uint8_t* null_bitmap = uv.validity
        cdef bint has_nulls = null_bitmap != NULL
        cdef Py_ssize_t i, block, j
        cdef uint64_t is_valid
        cdef uint64_t* dst
        cdef int32_t* data = <int32_t*>uv.data
        cdef uint64_t[INTEGER_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*>scratch

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Integer32Vector.hash_into: output buffer too small")

        dst = &out_buf[0] + offset
        i = 0
        while i < n:
            block = n - i
            if block > INTEGER_HASH_CHUNK:
                block = INTEGER_HASH_CHUNK
            if has_nulls:
                for j in range(block):
                    is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                    scratch[j] = (<uint64_t>(<int64_t>data[<Py_ssize_t>uv.selection[i + j]]) * is_valid) | (NULL_HASH * (1 - is_valid))
            else:
                for j in range(block):
                    scratch[j] = <uint64_t>(<int64_t>data[<Py_ssize_t>uv.selection[i + j]])
            simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
            i += block

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k
        k = min(<Py_ssize_t>self.ptr.length, 10)
        for i in range(k):
            vals.append(self[i])
        return f"<Integer32Vector len={self.ptr.length} values={vals}>"


cdef Integer32Vector integer32_from_arrow(object array):
    """Zero-copy wrap of a PyArrow int32/uint32 array as an Integer32Vector."""
    import pyarrow as pa

    cdef Integer32Vector vec = Integer32Vector(0, True)
    cdef intptr_t base_ptr
    cdef Py_ssize_t arr_offset
    cdef intptr_t nb_addr
    cdef Py_ssize_t nb_size
    cdef object new_bitmap_bytes
    cdef uint8_t* dst_bitmap
    cdef uint8_t* src_bitmap
    cdef size_t arr_len
    cdef Py_ssize_t j

    vec.ptr = <DrakenFixedBuffer*>malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    bufs = array.buffers()
    vec._arrow_null_buf = bufs[0]
    vec._arrow_data_buf = bufs[1]

    base_ptr = <intptr_t>bufs[1].address
    arr_offset = array.offset

    vec.ptr.type = DRAKEN_INT32
    vec.ptr.itemsize = 4
    vec.ptr.length = <size_t>len(array)
    vec.ptr.data = <void*>(base_ptr + arr_offset * 4)

    if bufs[0] is not None:
        nb_addr = bufs[0].address
        if arr_offset % 8 == 0:
            vec.ptr.null_bitmap = (<uint8_t*>nb_addr) + (arr_offset >> 3)
        else:
            nb_size = (len(array) + 7) // 8
            new_bitmap_bytes = PyBytes_FromStringAndSize(NULL, nb_size)
            dst_bitmap = <uint8_t*>PyBytes_AS_STRING(new_bitmap_bytes)
            memset(dst_bitmap, 0, nb_size)
            src_bitmap = <uint8_t*>nb_addr
            for j in range(len(array)):
                if (src_bitmap[(arr_offset + j) >> 3] >> ((arr_offset + j) & 7)) & 1:
                    dst_bitmap[j >> 3] |= (1 << (j & 7))
            vec.ptr.null_bitmap = dst_bitmap
            vec._arrow_null_buf = new_bitmap_bytes
    else:
        vec.ptr.null_bitmap = NULL

    arr_len = <size_t>len(array)
    vec._unified_view = draken_vector_from_dense(
        vec.ptr.data, <uint32_t>arr_len, DRAKEN_INT32, vec.ptr.null_bitmap)
    return vec
