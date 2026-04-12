# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
DecimalVector: Cython implementation of an int64-backed decimal column vector for Draken.

This module provides:
- The DecimalVector class for efficient decimal storage using unscaled int64 values
- Integration with DrakenFixedBuffer and related C helpers for memory management
- Arrow interoperability (copies data on import; always owns the buffer)
- Fast comparison and null handling for decimal columns

Storage: unscaled int64 values. For example, 3.7 at scale=1 is stored as 37.

Precision is capped at 18 (the maximum that fits in int64). Raise
NotImplementedError for precision > 18; callers should downcast to float64 instead.

Scale and precision are per-column metadata stored as int8_t fields.
No dictionary or constant encoding is supported — dense storage only.
"""

from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.string cimport memset, memcpy

from libc.stddef cimport size_t
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport intptr_t
from libc.stdint cimport uint64_t
from libc.stdint cimport uint8_t
from libc.stdlib cimport free, malloc

from opteryx.compiled.draken.core.buffers cimport ConstAccessor, DrakenFixedBuffer
from opteryx.compiled.draken.core.buffers cimport DRAKEN_INT64
from opteryx.compiled.draken.core.buffers cimport DRAKEN_ENCODING_DENSE
from opteryx.compiled.draken.core.fixed_vector cimport alloc_fixed_buffer
from opteryx.compiled.draken.core.fixed_vector cimport buf_dtype
from opteryx.compiled.draken.core.fixed_vector cimport buf_itemsize
from opteryx.compiled.draken.core.fixed_vector cimport buf_length
from opteryx.compiled.draken.core.fixed_vector cimport free_fixed_buffer
from opteryx.compiled.draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector


# ---------------------------------------------------------------------------
# Comparison operator constants (compile-time DEF, no runtime overhead)
# ---------------------------------------------------------------------------

DEF EQ  = 0
DEF NEQ = 1
DEF LT  = 2
DEF LTE = 3
DEF GT  = 4
DEF GTE = 5

DEF DECIMAL_HASH_CHUNK = 1024


cdef inline bint _bitmap_is_valid(uint8_t* bitmap, Py_ssize_t idx) noexcept nogil:
    """Return 1 if the bit at position idx is set (value is valid / non-null)."""
    return (bitmap[idx >> 3] >> (idx & 7)) & 1


cdef class DecimalVector(Vector):
    """
    Fixed-width decimal column backed by int64 unscaled values.

    Scale and precision are stored per-column as int8_t metadata.
    Precision must be <= 18 to fit within int64 range.
    No dictionary or constant encoding is supported — dense storage only.
    The vector always owns its buffer; from_arrow copies all data.
    """

    def __cinit__(self, size_t length=0):
        """Allocate a new owned int64 buffer of `length` elements.

        Unlike Int64Vector/Date32Vector there is no wrap=True path; from_arrow
        always copies data into a fresh allocation, so a placeholder constructor
        is not required.
        """
        self.ptr = alloc_fixed_buffer(DRAKEN_INT64, length, 8)
        self.owns_data = True
        self._const_accessor.length = 0
        self._const_accessor.value_type = DRAKEN_INT64
        self._const_accessor.value_ptr = NULL
        self._const_accessor.is_null = 0
        self._precision = 18
        self._scale = 0

    def __dealloc__(self):
        # free_fixed_buffer(ptr, True) frees: data buffer, null_bitmap (if set),
        # and the DrakenFixedBuffer header.  The null_bitmap is malloc'd in
        # from_arrow (or take) and owned by this vector.
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    # ------------------------------------------------------------------
    # C-level accessor protocol (required by Vector base)
    # ------------------------------------------------------------------

    cdef ConstAccessor* const_accessor(self) noexcept:
        # DecimalVector has no constant encoding path.
        return NULL

    cdef void* dense_ptr(self) noexcept:
        if self.ptr == NULL:
            return NULL
        return self.ptr.data

    cdef uint8_t* null_bitmap_ptr(self) noexcept:
        if self.ptr == NULL:
            return NULL
        return self.ptr.null_bitmap

    # ------------------------------------------------------------------
    # Metadata properties
    # ------------------------------------------------------------------

    @property
    def length(self):
        return buf_length(self.ptr)

    def __len__(self):
        return buf_length(self.ptr)

    @property
    def itemsize(self):
        return buf_itemsize(self.ptr)

    @property
    def dtype(self):
        return buf_dtype(self.ptr)

    @property
    def null_count(self):
        """Return the number of null values in the vector."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef Py_ssize_t count = 0
        cdef uint8_t byte, bit
        if ptr.null_bitmap == NULL:
            return 0
        for i in range(n):
            byte = ptr.null_bitmap[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                count += 1
        return count

    # ------------------------------------------------------------------
    # Element access
    # ------------------------------------------------------------------

    def __getitem__(self, Py_ssize_t i):
        """Return the value at index i as a Python Decimal, or None if null."""
        import decimal
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        if i < 0 or <size_t>i >= ptr.length:
            raise IndexError("Index out of bounds")
        if ptr.null_bitmap != NULL:
            if not _bitmap_is_valid(ptr.null_bitmap, i):
                return None
        cdef object factor = decimal.Decimal(10) ** (-self._scale)
        return decimal.Decimal(data[i]) * factor

    # ------------------------------------------------------------------
    # Arrow interoperability
    # ------------------------------------------------------------------

    def to_arrow(self):
        """Convert to a PyArrow decimal128 array.

        Builds a decimal128 buffer by sign-extending each int64 unscaled value
        to 128 bits.  Arrow stores decimal128 as 16-byte little-endian integers;
        positive values have zero-padded high bytes, negative values have
        0xFF-filled high bytes (two's complement sign extension).
        """
        import pyarrow as pa
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = <Py_ssize_t>ptr.length
        cdef int64_t* src = <int64_t*> ptr.data
        cdef Py_ssize_t i
        cdef int64_t val
        cdef Py_ssize_t nb_size = (n + 7) >> 3
        cdef object null_buf = None

        # Build decimal128 buffer (16 bytes per value, zero-initialised by bytearray)
        dec_buf = bytearray(n * 16)
        cdef uint8_t[::1] dec_view = dec_buf

        for i in range(n):
            val = src[i]
            memcpy(&dec_view[i * 16], &val, 8)          # low 8 bytes: unscaled int64
            if val < 0:
                memset(&dec_view[i * 16 + 8], 0xFF, 8)  # sign-extend high bytes

        if ptr.null_bitmap != NULL and nb_size > 0:
            null_buf = pa.py_buffer(bytes((<uint8_t*>ptr.null_bitmap)[:nb_size]))

        data_buf = pa.py_buffer(bytes(dec_buf))
        return pa.Array.from_buffers(
            pa.decimal128(self._precision, self._scale),
            n,
            [null_buf, data_buf],
        )

    cpdef list to_pylist(self):
        """Return the vector as a Python list of Decimal values (None for nulls)."""
        import decimal
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef list out = []
        cdef uint8_t byte, bit
        cdef object factor = decimal.Decimal(10) ** (-self._scale)

        if ptr.null_bitmap == NULL:
            for i in range(n):
                out.append(decimal.Decimal(data[i]) * factor)
        else:
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if bit:
                    out.append(decimal.Decimal(data[i]) * factor)
                else:
                    out.append(None)
        return out

    # ------------------------------------------------------------------
    # Row selection
    # ------------------------------------------------------------------

    cpdef DecimalVector take(self, int32_t[::1] indices):
        """Return a new DecimalVector containing only the rows named by `indices`."""
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef DecimalVector out = DecimalVector(<size_t>n)
        out._precision = self._precision
        out._scale = self._scale

        cdef int64_t* src = <int64_t*> self.ptr.data
        cdef int64_t* dst = <int64_t*> out.ptr.data
        cdef uint8_t* src_null = self.ptr.null_bitmap
        cdef uint8_t* out_null = NULL
        cdef int32_t src_idx
        cdef Py_ssize_t out_nbytes
        cdef uint8_t byte

        if src_null == NULL:
            for i in range(n):
                dst[i] = src[indices[i]]
            out.ptr.null_bitmap = NULL
        else:
            out_nbytes = (n + 7) >> 3
            out_null = <uint8_t*> malloc(out_nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, out_nbytes)

            for i in range(n):
                src_idx = indices[i]
                byte = src_null[src_idx >> 3]
                if byte & (1 << (src_idx & 7)):
                    dst[i] = src[src_idx]
                    out_null[i >> 3] |= (1 << (i & 7))
                else:
                    dst[i] = 0

            out.ptr.null_bitmap = out_null

        return out

    # ------------------------------------------------------------------
    # Scalar coercion (inline: called from every comparison method)
    # ------------------------------------------------------------------

    cdef inline int64_t _coerce_scalar(self, object scalar):
        """Convert a Python scalar to the int64 unscaled representation at self._scale."""
        import decimal
        if isinstance(scalar, decimal.Decimal):
            return <int64_t>int(scalar * (decimal.Decimal(10) ** self._scale))
        if isinstance(scalar, int):
            return <int64_t>(int(scalar) * (10 ** int(self._scale)))
        if isinstance(scalar, float):
            return <int64_t>int(round(scalar * (10 ** int(self._scale))))
        raise TypeError(f"Cannot compare DecimalVector with {type(scalar)!r}")

    # ------------------------------------------------------------------
    # Comparison — scalar only (no vector-to-vector for now)
    # ------------------------------------------------------------------

    cdef inline bint _compare_decimal_values(self, int64_t left, int64_t right, int op) nogil:
        if op == EQ:
            return left == right
        if op == NEQ:
            return left != right
        if op == LT:
            return left < right
        if op == LTE:
            return left <= right
        if op == GT:
            return left > right
        return left >= right    # GTE

    cdef BoolVector _compare_scalar(self, int op, int64_t rhs):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef uint8_t* src_null = ptr.null_bitmap
        cdef Py_ssize_t i, n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask

        memset(dst, 0, nbytes)
        if src_null != NULL and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, src_null, nbytes)
            # Mask out padding bits in the last byte so consumers see a clean bitmap
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        for i in range(n):
            if src_null == NULL or ((src_null[i >> 3] >> (i & 7)) & 1):
                if self._compare_decimal_values(data[i], rhs, op):
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef BoolVector equals(self, object scalar):
        return self._compare_scalar(EQ, self._coerce_scalar(scalar))

    cpdef BoolVector not_equals(self, object scalar):
        return self._compare_scalar(NEQ, self._coerce_scalar(scalar))

    cpdef BoolVector less_than(self, object scalar):
        return self._compare_scalar(LT, self._coerce_scalar(scalar))

    cpdef BoolVector less_than_or_equals(self, object scalar):
        return self._compare_scalar(LTE, self._coerce_scalar(scalar))

    cpdef BoolVector greater_than(self, object scalar):
        return self._compare_scalar(GT, self._coerce_scalar(scalar))

    cpdef BoolVector greater_than_or_equals(self, object scalar):
        return self._compare_scalar(GTE, self._coerce_scalar(scalar))

    # ------------------------------------------------------------------
    # Hashing — identical to Int64Vector since storage IS int64
    # ------------------------------------------------------------------

    cdef inline void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0
    ) except *:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef uint64_t* dst_base
        cdef Py_ssize_t i, j, block
        cdef uint64_t is_valid

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("DecimalVector.hash_into: output buffer too small")
        dst_base = &out_buf[0]

        cdef int64_t* data = <int64_t*> ptr.data
        cdef uint64_t* dst = dst_base + offset
        cdef uint64_t* as_uint64 = <uint64_t*> data
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL
        cdef uint64_t[DECIMAL_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch

        if has_nulls:
            i = 0
            while i < n:
                block = n - i
                if block > DECIMAL_HASH_CHUNK:
                    block = DECIMAL_HASH_CHUNK
                for j in range(block):
                    is_valid = (null_bitmap[(i + j) >> 3] >> ((i + j) & 7)) & 1
                    scratch[j] = (as_uint64[i + j] * is_valid) | (NULL_HASH * (1 - is_valid))
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block
        else:
            simd_mix_hash(dst, as_uint64, <size_t>n)

    # ------------------------------------------------------------------
    # Compression — unscaled int64 values pass through directly
    # ------------------------------------------------------------------

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Write unscaled int64 values into out_buf.  Null rows emit INT64_MIN."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("DecimalVector.compress_into: output buffer too small")

        cdef int64_t* dst = &out_buf[offset]
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL
        cdef int64_t* src = <int64_t*> ptr.data

        if not has_nulls:
            # Fast path: bulk memcpy
            memcpy(<void*>dst, <const void*>src, <size_t>(n * sizeof(int64_t)))
            return

        for i in range(n):
            if (null_bitmap[i >> 3] >> (i & 7)) & 1:
                dst[i] = src[i]
            else:
                dst[i] = <int64_t> -9223372036854775808

    # ------------------------------------------------------------------
    # Debug representation
    # ------------------------------------------------------------------

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k = min(<Py_ssize_t>buf_length(self.ptr), 10)
        cdef int64_t* data = <int64_t*> self.ptr.data
        for i in range(k):
            vals.append(data[i])
        return (
            f"<DecimalVector len={buf_length(self.ptr)} "
            f"precision={self._precision} scale={self._scale} "
            f"values={vals}>"
        )


# ---------------------------------------------------------------------------
# Module-level factory: from_arrow
# ---------------------------------------------------------------------------

cdef DecimalVector from_arrow(object array):
    """
    Build a DecimalVector from a PyArrow decimal128 array.

    Always copies all data — no zero-copy.  The returned vector fully owns its
    buffer; it is freed by free_fixed_buffer in __dealloc__.

    Precision must be <= 18; raises NotImplementedError otherwise since the
    full unscaled value would overflow int64.

    Null bitmap handling mirrors int64_vector.from_arrow:
    - If offset % 8 == 0: byte-aligned slice copy
    - Otherwise:          bit-by-bit copy with offset shift
    The resulting bitmap is malloc'd and assigned to vec.ptr.null_bitmap so that
    free_fixed_buffer(ptr, True) will free it correctly.
    """
    import pyarrow as pa

    cdef object pa_type = array.type
    if not pa.types.is_decimal(pa_type):
        raise TypeError(
            f"DecimalVector.from_arrow expects a decimal Arrow array; got {pa_type!r}"
        )

    cdef int precision = pa_type.precision
    cdef int scale = pa_type.scale

    if precision > 18:
        raise NotImplementedError(
            f"DecimalVector supports precision up to 18 (int64-backed); "
            f"got precision={precision}. Reduce precision or use float64."
        )

    cdef Py_ssize_t n = len(array)
    # DecimalVector.__cinit__ always allocates; owns_data is True by default.
    cdef DecimalVector vec = DecimalVector(<size_t>n)

    cdef object bufs = array.buffers()
    cdef intptr_t dec_addr = <intptr_t> bufs[1].address
    cdef uint8_t* dec_data = <uint8_t*> dec_addr
    cdef int64_t* dst = <int64_t*> vec.ptr.data
    cdef Py_ssize_t offset = array.offset
    cdef Py_ssize_t i

    # decimal128 stores 16 bytes per value in little-endian int128.
    # For precision <= 18 the unscaled integer fits in int64, so the low
    # 8 bytes are sufficient (both x86 and ARM targets are little-endian).
    for i in range(n):
        memcpy(dst + i, dec_data + (offset + i) * 16, 8)

    # Null bitmap: allocate a fresh malloc'd copy owned by the vector.
    # free_fixed_buffer(ptr, True) will free ptr.null_bitmap unconditionally,
    # so this pointer must be malloc'd (not a slice of a Python buffer).
    cdef Py_ssize_t nb_size = (n + 7) >> 3
    cdef uint8_t* src_bitmap
    cdef uint8_t* new_bitmap
    cdef intptr_t nb_addr

    if bufs[0] is not None and n > 0:
        nb_addr = bufs[0].address
        src_bitmap = <uint8_t*> nb_addr

        new_bitmap = <uint8_t*> malloc(nb_size)
        if new_bitmap == NULL:
            raise MemoryError()

        if offset % 8 == 0:
            # Byte-aligned: single memcpy
            memcpy(new_bitmap, src_bitmap + (offset >> 3), nb_size)
        else:
            # Unaligned offset: copy bit by bit with shift
            memset(new_bitmap, 0, nb_size)
            for i in range(n):
                if (src_bitmap[(offset + i) >> 3] >> ((offset + i) & 7)) & 1:
                    new_bitmap[i >> 3] |= <uint8_t>(1 << (i & 7))

        vec.ptr.null_bitmap = new_bitmap
    else:
        vec.ptr.null_bitmap = NULL

    vec._precision = <int8_t>precision
    vec._scale = <int8_t>scale

    return vec
