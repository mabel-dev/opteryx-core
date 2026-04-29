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
- Fast comparison, null handling, hashing, and aggregation for decimal columns

Storage: unscaled int64 values. For example, 3.7 at scale=1 is stored as 37.

Two storage modes:
  Dense:    _has_const=False — one int64 per row in a DrakenFixedBuffer
  Constant: _has_const=True  — no dense buffer; all rows share a single _const_value

Precision is capped at 18 (the maximum that fits in int64). Raise NotImplementedError
for precision > 18; callers should downcast to float64 instead.

Scale and precision are per-column metadata stored as int8_t fields.
No dictionary encoding is supported.
"""

from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.string cimport memset, memcpy
from libc.stddef cimport size_t
from libc.stdint cimport int8_t, int32_t, int64_t, intptr_t, uint8_t, uint64_t
from libc.stdlib cimport malloc, free

from draken.core.buffers cimport ConstAccessor, DrakenFixedBuffer
from draken.core.buffers cimport DRAKEN_INT64
from draken.core.buffers cimport DRAKEN_ENCODING_DENSE
from draken.core.buffers cimport DRAKEN_ENCODING_CONSTANT
from draken.core.fixed_vector cimport alloc_fixed_buffer
from draken.core.fixed_vector cimport buf_dtype
from draken.core.fixed_vector cimport buf_itemsize
from draken.core.fixed_vector cimport buf_length
from draken.core.fixed_vector cimport free_fixed_buffer
from draken.vectors.vector cimport Vector, NULL_HASH, simd_mix_hash, simd_popcount
from draken.vectors.bool_vector cimport BoolVector


# ---------------------------------------------------------------------------
# Comparison operator constants
# ---------------------------------------------------------------------------

DEF EQ  = 0
DEF NEQ = 1
DEF LT  = 2
DEF LTE = 3
DEF GT  = 4
DEF GTE = 5

cdef const int64_t INT64_MIN_VALUE = <int64_t>0x8000000000000000

DEF DECIMAL_HASH_CHUNK = 1024


cdef inline bint _bitmap_is_valid(uint8_t* bitmap, Py_ssize_t idx) noexcept nogil:
    """Return 1 if the bit at position idx is set (value is valid / non-null)."""
    return (bitmap[idx >> 3] >> (idx & 7)) & 1


# ---------------------------------------------------------------------------
# DecimalVector class
# ---------------------------------------------------------------------------

cdef class DecimalVector(Vector):
    """
    Fixed-width decimal column backed by int64 unscaled values.

    Scale and precision are stored per-column as int8_t metadata.
    Precision must be <= 18 to fit within int64 range.

    Two storage modes:
      Dense:    _has_const=False — one int64 per row in a DrakenFixedBuffer.
      Constant: _has_const=True  — no dense buffer; all rows share _const_value.

    The vector always owns its buffer in dense mode; from_arrow copies all data.
    """

    def __cinit__(self, size_t length=0):
        self.ptr = alloc_fixed_buffer(DRAKEN_INT64, length, 8)
        self.owns_data = True
        self._const_accessor.length = 0
        self._const_accessor.value_type = DRAKEN_INT64
        self._const_accessor.value_ptr = NULL
        self._const_accessor.is_null = 0
        self._precision = 18
        self._scale = 0
        self._has_const = False
        self._const_is_null = False
        self._const_value = 0

    def __dealloc__(self):
        # free_fixed_buffer(ptr, True) frees: data buffer, null_bitmap (if set),
        # and the DrakenFixedBuffer header.  Constant-encoded vectors still
        # have a stub allocation (ptr is never NULL after __cinit__).
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    # ------------------------------------------------------------------
    # Constant-encoding factory
    # ------------------------------------------------------------------

    @classmethod
    def from_constant(cls, value, length, is_null=False):
        """Create a constant-encoded DecimalVector with no dense allocation.

        All rows appear to hold the same value (or NULL).  Scale and precision
        are inferred from the Python Decimal value when provided.

        Args:
            value:    Python Decimal, int, or float (or None when is_null=True)
            length:   Number of logical rows
            is_null:  When True, all rows are NULL

        Returns:
            DecimalVector in constant-encoding mode
        """
        import decimal as _decimal
        cdef DecimalVector vec
        cdef int scale
        cdef int64_t unscaled
        cdef object d
        cdef object sign, digits, exp_obj

        if length < 0:
            raise ValueError("length must be non-negative")

        vec = DecimalVector(0)          # stub alloc; ptr.length remains 0
        vec.ptr.length = <size_t>length
        vec.ptr.null_bitmap = NULL
        vec._has_const = True
        vec._const_is_null = bool(is_null or value is None)
        vec._encoding = DRAKEN_ENCODING_CONSTANT

        if vec._const_is_null:
            vec._const_value = 0
            return vec

        if isinstance(value, _decimal.Decimal):
            sign, digits, exp_obj = value.as_tuple()
            scale = max(0, -int(exp_obj))
            unscaled = <int64_t>int(value * (_decimal.Decimal(10) ** scale))
            vec._scale = <int8_t>min(scale, 18)
            vec._precision = <int8_t>min(18, max(1, len(str(abs(int(unscaled))))))
            vec._const_value = unscaled
        elif isinstance(value, int):
            vec._scale = 0
            vec._const_value = <int64_t>value
        elif isinstance(value, float):
            d = _decimal.Decimal(str(value))
            sign, digits, exp_obj = d.as_tuple()
            scale = max(0, -int(exp_obj))
            unscaled = <int64_t>int(d * (_decimal.Decimal(10) ** scale))
            vec._scale = <int8_t>min(scale, 18)
            vec._const_value = unscaled
        else:
            raise TypeError(
                f"DecimalVector.from_constant: unsupported value type {type(value)!r}"
            )

        return vec

    # ------------------------------------------------------------------
    # C-level accessor protocol (required by Vector base)
    # ------------------------------------------------------------------

    cdef ConstAccessor* const_accessor(self) noexcept:
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
    def ordered(self):
        """DecimalVector has no dictionary ordering."""
        return False

    @property
    def code_width(self):
        """DecimalVector has no dictionary encoding."""
        return None

    @property
    def dictionary_size(self):
        """DecimalVector has no dictionary encoding."""
        return 0

    @property
    def dictionary_value_type(self):
        """DecimalVector has no dictionary encoding."""
        return None

    @property
    def null_count(self):
        """Return the number of null values in the vector."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        if self._has_const:
            return n if self._const_is_null else 0
        if ptr.null_bitmap == NULL:
            return 0
        return n - <Py_ssize_t>simd_popcount(ptr.null_bitmap, (<size_t>n + 7) >> 3)

    # ------------------------------------------------------------------
    # Element access
    # ------------------------------------------------------------------

    def __getitem__(self, Py_ssize_t i):
        """Return the value at index i as a Python Decimal, or None if null."""
        import decimal
        cdef DrakenFixedBuffer* ptr
        cdef int64_t* data
        cdef object factor

        ptr = self.ptr
        if i < 0 or <size_t>i >= ptr.length:
            raise IndexError("Index out of bounds")

        factor = decimal.Decimal(10) ** (-self._scale)

        if self._has_const:
            if self._const_is_null:
                return None
            return decimal.Decimal(self._const_value) * factor

        data = <int64_t*> ptr.data
        if ptr.null_bitmap != NULL:
            if not _bitmap_is_valid(ptr.null_bitmap, i):
                return None
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
        import decimal as _decimal
        import pyarrow as pa
        cdef DrakenFixedBuffer* ptr
        cdef Py_ssize_t n, i, nb_size
        cdef int64_t* src
        cdef int64_t val
        cdef uint8_t[::1] dec_view
        cdef object null_buf, dec_buf, data_buf, arrow_type, factor, pyval

        ptr = self.ptr
        n = <Py_ssize_t>ptr.length
        nb_size = (n + 7) >> 3
        null_buf = None

        if self._has_const:
            arrow_type = pa.decimal128(
                self._precision if self._precision > 0 else 18, self._scale
            )
            if self._const_is_null:
                return pa.array([None] * n, type=arrow_type)
            factor = _decimal.Decimal(10) ** (-self._scale)
            pyval = _decimal.Decimal(self._const_value) * factor
            return pa.array([pyval] * n, type=arrow_type)

        src = <int64_t*> ptr.data

        # Build decimal128 buffer (16 bytes per value, zero-initialised by bytearray).
        dec_buf = bytearray(n * 16)
        dec_view = dec_buf

        for i in range(n):
            val = src[i]
            memcpy(&dec_view[i * 16], &val, 8)           # low 8 bytes: unscaled int64
            if val < 0:
                memset(&dec_view[i * 16 + 8], 0xFF, 8)   # sign-extend high bytes

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
        cdef DrakenFixedBuffer* ptr
        cdef int64_t* data
        cdef Py_ssize_t i, n
        cdef list out
        cdef uint8_t byte, bit
        cdef object factor, val_py

        ptr = self.ptr
        n = ptr.length
        factor = decimal.Decimal(10) ** (-self._scale)

        if self._has_const:
            if self._const_is_null:
                return [None] * n
            val_py = decimal.Decimal(self._const_value) * factor
            return [val_py] * n

        data = <int64_t*> ptr.data
        out = []

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
        cdef Py_ssize_t i, n, out_nbytes
        cdef DecimalVector out
        cdef int64_t* src
        cdef int64_t* dst
        cdef uint8_t* src_null
        cdef uint8_t* out_null
        cdef int32_t src_idx
        cdef uint8_t byte

        n = indices.shape[0]
        out = DecimalVector(<size_t>n)
        out._precision = self._precision
        out._scale = self._scale
        dst = <int64_t*> out.ptr.data
        out_null = NULL
        out_nbytes = (n + 7) >> 3

        # Constant-encoding: materialise the selected rows into dense storage.
        if self._has_const:
            if self._const_is_null:
                if out_nbytes > 0:
                    out_null = <uint8_t*> malloc(out_nbytes)
                    if out_null == NULL:
                        raise MemoryError()
                    memset(out_null, 0, out_nbytes)
                    out.ptr.null_bitmap = out_null
                else:
                    out.ptr.null_bitmap = NULL
                if n > 0:
                    memset(dst, 0, <size_t>(n * sizeof(int64_t)))
            else:
                for i in range(n):
                    dst[i] = self._const_value
                out.ptr.null_bitmap = NULL
            return out

        src = <int64_t*> self.ptr.data
        src_null = self.ptr.null_bitmap

        if src_null == NULL:
            for i in range(n):
                dst[i] = src[indices[i]]
            out.ptr.null_bitmap = NULL
        else:
            if out_nbytes > 0:
                out_null = <uint8_t*> malloc(out_nbytes)
                if out_null == NULL:
                    raise MemoryError()
                memset(out_null, 0, out_nbytes)

            for i in range(n):
                src_idx = indices[i]
                byte = src_null[src_idx >> 3]
                if byte & (1 << (src_idx & 7)):
                    dst[i] = src[src_idx]
                    if out_null != NULL:
                        out_null[i >> 3] |= (1 << (i & 7))
                else:
                    dst[i] = 0

            out.ptr.null_bitmap = out_null

        return out

    # ------------------------------------------------------------------
    # Scalar coercion
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
        raise TypeError(f"Cannot coerce {type(scalar)!r} for DecimalVector comparison")

    # ------------------------------------------------------------------
    # Comparison kernel (nogil, used by both scalar and vector paths)
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

    # ------------------------------------------------------------------
    # Scalar comparison
    # ------------------------------------------------------------------

    cdef BoolVector _compare_scalar(self, int op, int64_t rhs):
        cdef DrakenFixedBuffer* ptr
        cdef int64_t* data
        cdef uint8_t* src_null
        cdef Py_ssize_t i, n, nbytes
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null
        cdef uint8_t mask
        cdef bint matched

        ptr = self.ptr
        n = ptr.length
        nbytes = (n + 7) >> 3
        out = BoolVector(<size_t>n)
        dst = <uint8_t*> out.ptr.data
        out_null = NULL

        if nbytes > 0:
            memset(dst, 0, nbytes)

        # Constant-encoding fast path
        if self._has_const:
            if self._const_is_null:
                if nbytes != 0:
                    out_null = <uint8_t*> malloc(nbytes)
                    if out_null == NULL:
                        raise MemoryError()
                    memset(out_null, 0, nbytes)
                    out.ptr.null_bitmap = out_null
                else:
                    out.ptr.null_bitmap = NULL
                return out
            matched = self._compare_decimal_values(self._const_value, rhs, op)
            if matched and nbytes > 0:
                memset(dst, 0xFF, nbytes)
                if (n & 7) != 0:
                    mask = <uint8_t>((1 << (n & 7)) - 1)
                    dst[nbytes - 1] &= mask
            out.ptr.null_bitmap = NULL
            return out

        # Dense path
        data = <int64_t*> ptr.data
        src_null = ptr.null_bitmap

        if src_null != NULL and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, src_null, nbytes)
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

    # ------------------------------------------------------------------
    # Vector-vector comparison
    # ------------------------------------------------------------------

    cdef BoolVector _compare_vector(self, DecimalVector other, int op):
        """Element-wise comparison between two DecimalVectors of the same scale."""
        cdef DrakenFixedBuffer* ptr1
        cdef DrakenFixedBuffer* ptr2
        cdef int64_t* data1
        cdef int64_t* data2
        cdef uint8_t* null1
        cdef uint8_t* null2
        cdef Py_ssize_t n, nbytes, i
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null
        cdef bint valid1, valid2, any_nullable
        cdef int64_t lval, rval

        ptr1 = self.ptr
        ptr2 = other.ptr
        n = ptr1.length
        nbytes = (n + 7) >> 3
        out_null = NULL

        if n != ptr2.length:
            raise ValueError(
                f"DecimalVector._compare_vector: length mismatch {n} vs {ptr2.length}"
            )
        if self._scale != other._scale:
            raise ValueError(
                f"DecimalVector._compare_vector: scale mismatch "
                f"{self._scale} vs {other._scale}"
            )

        out = BoolVector(<size_t>n)
        dst = <uint8_t*> out.ptr.data

        if nbytes > 0:
            memset(dst, 0, nbytes)

        null1 = ptr1.null_bitmap
        null2 = ptr2.null_bitmap
        data1 = <int64_t*> ptr1.data
        data2 = <int64_t*> ptr2.data

        any_nullable = (
            (null1 != NULL or null2 != NULL or self._has_const or other._has_const)
            and nbytes != 0
        )
        if any_nullable:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        for i in range(n):
            # Resolve left value
            if self._has_const:
                if self._const_is_null:
                    continue
                lval = self._const_value
                valid1 = True
            else:
                valid1 = (null1 == NULL) or (((null1[i >> 3] >> (i & 7)) & 1) != 0)
                lval = data1[i]

            # Resolve right value
            if other._has_const:
                if other._const_is_null:
                    continue
                rval = other._const_value
                valid2 = True
            else:
                valid2 = (null2 == NULL) or (((null2[i >> 3] >> (i & 7)) & 1) != 0)
                rval = data2[i]

            if valid1 and valid2:
                if out_null != NULL:
                    out_null[i >> 3] |= (1 << (i & 7))
                if self._compare_decimal_values(lval, rval, op):
                    dst[i >> 3] |= (1 << (i & 7))

        return out

    # ------------------------------------------------------------------
    # Public scalar comparison API
    # ------------------------------------------------------------------

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
    # Public vector-vector comparison API
    # ------------------------------------------------------------------

    cpdef BoolVector equals_vector(self, DecimalVector other):
        return self._compare_vector(other, EQ)

    cpdef BoolVector not_equals_vector(self, DecimalVector other):
        return self._compare_vector(other, NEQ)

    cpdef BoolVector less_than_vector(self, DecimalVector other):
        return self._compare_vector(other, LT)

    cpdef BoolVector less_than_or_equals_vector(self, DecimalVector other):
        return self._compare_vector(other, LTE)

    cpdef BoolVector greater_than_vector(self, DecimalVector other):
        return self._compare_vector(other, GT)

    cpdef BoolVector greater_than_or_equals_vector(self, DecimalVector other):
        return self._compare_vector(other, GTE)

    # ------------------------------------------------------------------
    # Set membership
    # ------------------------------------------------------------------

    cpdef BoolVector in_list(self, object value_set):
        """Return mask: 1 if element is in value_set, else 0.  Propagates NULLs."""
        cdef DrakenFixedBuffer* ptr
        cdef int64_t* data
        cdef uint8_t* src_null
        cdef Py_ssize_t i, n, nbytes
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null
        cdef uint8_t mask
        cdef object coerced, item

        ptr = self.ptr
        n = ptr.length
        nbytes = (n + 7) >> 3
        out_null = NULL

        if not isinstance(value_set, (set, frozenset)):
            value_set = set(value_set)

        # Coerce each element of the set to the unscaled int64 at this vector's scale
        coerced = set()
        for item in value_set:
            coerced.add(self._coerce_scalar(item))

        out = BoolVector(<size_t>n)
        dst = <uint8_t*> out.ptr.data

        if nbytes > 0:
            memset(dst, 0, nbytes)

        # Constant-encoding fast path
        if self._has_const:
            if self._const_is_null:
                if nbytes != 0:
                    out_null = <uint8_t*> malloc(nbytes)
                    if out_null == NULL:
                        raise MemoryError()
                    memset(out_null, 0, nbytes)
                    out.ptr.null_bitmap = out_null
                else:
                    out.ptr.null_bitmap = NULL
                return out
            if self._const_value in coerced and nbytes > 0:
                memset(dst, 0xFF, nbytes)
                if (n & 7) != 0:
                    mask = <uint8_t>((1 << (n & 7)) - 1)
                    dst[nbytes - 1] &= mask
            out.ptr.null_bitmap = NULL
            return out

        # Dense path
        data = <int64_t*> ptr.data
        src_null = ptr.null_bitmap

        if src_null != NULL and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, src_null, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        for i in range(n):
            if src_null == NULL or ((src_null[i >> 3] >> (i & 7)) & 1):
                if data[i] in coerced:
                    dst[i >> 3] |= (1 << (i & 7))
        return out

    # ------------------------------------------------------------------
    # Null predicate
    # ------------------------------------------------------------------

    cpdef object is_null(self):
        """Return int8_t memoryview: 1 if the row is null, 0 otherwise.

        Returns an empty list for zero-length vectors (Cython does not support
        zero-length typed memoryviews).
        """
        cdef DrakenFixedBuffer* ptr
        cdef Py_ssize_t i, n
        cdef int8_t* buf
        cdef uint8_t byte, bit

        ptr = self.ptr
        n = ptr.length

        if n == 0:
            return []

        buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()

        if self._has_const:
            for i in range(n):
                buf[i] = 1 if self._const_is_null else 0
            return <int8_t[:n]> buf

        if ptr.null_bitmap == NULL:
            for i in range(n):
                buf[i] = 0
        else:
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                buf[i] = 0 if bit else 1
        return <int8_t[:n]> buf

    # ------------------------------------------------------------------
    # Aggregation  (return Python Decimal so scale is preserved)
    # ------------------------------------------------------------------

    cpdef object sum(self):
        """Return sum as a Python Decimal, skipping nulls.  Returns Decimal(0) for all-null."""
        import decimal as _decimal
        cdef DrakenFixedBuffer* ptr
        cdef int64_t* data
        cdef uint8_t* nb
        cdef Py_ssize_t i, n
        cdef int64_t total
        cdef object factor

        ptr = self.ptr
        n = ptr.length
        total = 0
        factor = _decimal.Decimal(10) ** (-self._scale)

        if self._has_const:
            if self._const_is_null or n == 0:
                return _decimal.Decimal(0)
            return _decimal.Decimal(self._const_value * <int64_t>n) * factor

        data = <int64_t*> ptr.data
        nb = ptr.null_bitmap

        for i in range(n):
            if nb == NULL or ((nb[i >> 3] >> (i & 7)) & 1):
                total += data[i]
        return _decimal.Decimal(total) * factor

    cpdef object min(self):
        """Return minimum as a Python Decimal, excluding nulls."""
        import decimal as _decimal
        cdef DrakenFixedBuffer* ptr
        cdef int64_t* data
        cdef uint8_t* nb
        cdef Py_ssize_t i, n
        cdef int64_t m
        cdef bint found
        cdef object factor

        ptr = self.ptr
        n = ptr.length
        found = False
        factor = _decimal.Decimal(10) ** (-self._scale)

        if n == 0:
            raise ValueError("Cannot compute min of empty DecimalVector")

        if self._has_const:
            if self._const_is_null:
                raise ValueError("Cannot compute min of all-null DecimalVector")
            return _decimal.Decimal(self._const_value) * factor

        data = <int64_t*> ptr.data
        nb = ptr.null_bitmap

        for i in range(n):
            if nb == NULL or ((nb[i >> 3] >> (i & 7)) & 1):
                m = data[i]
                found = True
                break

        if not found:
            raise ValueError("Cannot compute min of all-null DecimalVector")

        for i in range(i + 1, n):
            if nb == NULL or ((nb[i >> 3] >> (i & 7)) & 1):
                if data[i] < m:
                    m = data[i]
        return _decimal.Decimal(m) * factor

    cpdef object max(self):
        """Return maximum as a Python Decimal, excluding nulls."""
        import decimal as _decimal
        cdef DrakenFixedBuffer* ptr
        cdef int64_t* data
        cdef uint8_t* nb
        cdef Py_ssize_t i, n
        cdef int64_t m
        cdef bint found
        cdef object factor

        ptr = self.ptr
        n = ptr.length
        found = False
        factor = _decimal.Decimal(10) ** (-self._scale)

        if n == 0:
            raise ValueError("Cannot compute max of empty DecimalVector")

        if self._has_const:
            if self._const_is_null:
                raise ValueError("Cannot compute max of all-null DecimalVector")
            return _decimal.Decimal(self._const_value) * factor

        data = <int64_t*> ptr.data
        nb = ptr.null_bitmap

        for i in range(n):
            if nb == NULL or ((nb[i >> 3] >> (i & 7)) & 1):
                m = data[i]
                found = True
                break

        if not found:
            raise ValueError("Cannot compute max of all-null DecimalVector")

        for i in range(i + 1, n):
            if nb == NULL or ((nb[i >> 3] >> (i & 7)) & 1):
                if data[i] > m:
                    m = data[i]
        return _decimal.Decimal(m) * factor

    # ------------------------------------------------------------------
    # Hashing — identical logic to Int64Vector since storage IS int64
    # ------------------------------------------------------------------

    cdef inline void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0
    ) except *:
        cdef DrakenFixedBuffer* ptr
        cdef int64_t* data
        cdef uint64_t* dst
        cdef uint64_t* as_uint64
        cdef uint8_t* null_bitmap
        cdef Py_ssize_t n, i, j, block
        cdef uint64_t is_valid, const_raw
        cdef bint has_nulls
        cdef uint64_t[DECIMAL_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr

        ptr = self.ptr
        n = ptr.length
        scratch_ptr = <uint64_t*> scratch

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("DecimalVector.hash_into: output buffer too small")

        dst = (&out_buf[0]) + offset

        # Constant-encoding: all rows share the same hash
        if self._has_const:
            if self._const_is_null:
                const_raw = NULL_HASH
            else:
                const_raw = <uint64_t> self._const_value
                simd_mix_hash(scratch_ptr, &const_raw, 1)
                const_raw = scratch[0]
            for i in range(n):
                dst[i] = const_raw
            return

        # Dense path
        data = <int64_t*> ptr.data
        as_uint64 = <uint64_t*> data
        null_bitmap = ptr.null_bitmap
        has_nulls = null_bitmap != NULL

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
        cdef DrakenFixedBuffer* ptr
        cdef int64_t* src
        cdef int64_t* dst
        cdef uint8_t* null_bitmap
        cdef Py_ssize_t n, i
        cdef bint has_nulls
        cdef int64_t fill

        ptr = self.ptr
        n = ptr.length

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("DecimalVector.compress_into: output buffer too small")

        dst = &out_buf[offset]

        # Constant-encoding path
        if self._has_const:
            fill = INT64_MIN_VALUE if self._const_is_null else self._const_value
            for i in range(n):
                dst[i] = fill
            return

        # Dense path
        null_bitmap = ptr.null_bitmap
        has_nulls = null_bitmap != NULL
        src = <int64_t*> ptr.data

        if not has_nulls:
            # Fast path: bulk memcpy
            memcpy(<void*>dst, <const void*>src, <size_t>(n * sizeof(int64_t)))
            return

        for i in range(n):
            if (null_bitmap[i >> 3] >> (i & 7)) & 1:
                dst[i] = src[i]
            else:
                dst[i] = INT64_MIN_VALUE

    # ------------------------------------------------------------------
    # Debug representation
    # ------------------------------------------------------------------

    def __str__(self):
        cdef Py_ssize_t n, i, k
        cdef int64_t* data
        cdef list vals

        n = <Py_ssize_t>buf_length(self.ptr)

        if self._has_const:
            tag = "NULL" if self._const_is_null else str(self._const_value)
            return (
                f"<DecimalVector[const] len={n} "
                f"precision={self._precision} scale={self._scale} "
                f"value={tag}>"
            )

        vals = []
        k = min(n, 10)
        data = <int64_t*> self.ptr.data
        for i in range(k):
            vals.append(data[i])
        return (
            f"<DecimalVector len={n} "
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

    Null bitmap handling:
    - If offset % 8 == 0: byte-aligned slice copy
    - Otherwise:          bit-by-bit copy with offset shift
    The resulting bitmap is malloc'd and assigned to vec.ptr.null_bitmap so that
    free_fixed_buffer(ptr, True) will free it correctly.
    """
    import pyarrow as pa
    cdef object pa_type
    cdef int precision, scale
    cdef Py_ssize_t n, i, nb_size, offset
    cdef DecimalVector vec
    cdef object bufs
    cdef intptr_t dec_addr, nb_addr
    cdef uint8_t* dec_data
    cdef uint8_t* src_bitmap
    cdef uint8_t* new_bitmap
    cdef int64_t* dst

    pa_type = array.type
    if not pa.types.is_decimal(pa_type):
        raise TypeError(
            f"DecimalVector.from_arrow expects a decimal Arrow array; got {pa_type!r}"
        )

    precision = pa_type.precision
    scale = pa_type.scale

    if precision > 18:
        raise NotImplementedError(
            f"DecimalVector supports precision up to 18 (int64-backed); "
            f"got precision={precision}. Reduce precision or use float64."
        )

    n = len(array)
    vec = DecimalVector(<size_t>n)

    bufs = array.buffers()
    dec_addr = <intptr_t> bufs[1].address
    dec_data = <uint8_t*> dec_addr
    dst = <int64_t*> vec.ptr.data
    offset = array.offset

    # decimal128 stores 16 bytes per value in little-endian int128.
    # For precision <= 18 the unscaled integer fits in int64, so the low
    # 8 bytes are sufficient (both x86 and ARM targets are little-endian).
    for i in range(n):
        memcpy(dst + i, dec_data + (offset + i) * 16, 8)

    # Null bitmap: allocate a fresh malloc'd copy owned by the vector.
    nb_size = (n + 7) >> 3

    if bufs[0] is not None and n > 0:
        nb_addr = bufs[0].address
        src_bitmap = <uint8_t*> nb_addr
        new_bitmap = <uint8_t*> malloc(nb_size)
        if new_bitmap == NULL:
            raise MemoryError()

        if offset % 8 == 0:
            memcpy(new_bitmap, src_bitmap + (offset >> 3), nb_size)
        else:
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
