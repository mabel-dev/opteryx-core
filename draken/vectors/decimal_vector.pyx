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
DecimalVector: Cython implementation of an int64-backed decimal column vector for Draken.

This module provides:
- The DecimalVector class for efficient decimal storage using unscaled int64 values
- Integration with DrakenFixedBuffer and related C helpers for memory management
- Arrow interoperability (copies data on import; always owns the buffer)
- Fast comparison, null handling, hashing, and aggregation for decimal columns

Storage: unscaled int64 values. For example, 3.7 at scale=1 is stored as 37.

Two storage modes:
  Dense:    data_length == n — one int64 per row in a DrakenFixedBuffer
  Constant: data_length == 1 — all rows share a single value at ptr.data[0]

Precision is capped at 18 (the maximum that fits in int64). Raise NotImplementedError
for precision > 18; callers should downcast to float64 instead.

Scale and precision are per-column metadata stored as int8_t fields.
No dictionary encoding is supported.
"""

from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.string cimport memset, memcpy
from libc.stddef cimport size_t
from libc.stdint cimport int8_t, int32_t, int64_t, intptr_t, uint8_t, uint32_t, uint64_t
from libc.stdlib cimport malloc, free

from draken.core.buffers cimport DrakenFixedBuffer, DrakenVector
from draken.core.buffers cimport DRAKEN_INT64
from draken.core.buffers cimport DRAKEN_NON_NATIVE
from draken.core.buffers cimport draken_vector_from_dense, draken_vector_from_constant
from draken.core.fixed_vector cimport alloc_fixed_buffer
from draken.core.fixed_vector cimport buf_dtype
from draken.core.fixed_vector cimport buf_itemsize
from draken.core.fixed_vector cimport buf_length
from draken.core.fixed_vector cimport free_fixed_buffer
from draken.vectors.vector cimport Vector, NULL_HASH, simd_mix_hash, simd_popcount
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.integer64_vector cimport Integer64Vector, _materialize_dict_int64
from draken.vectors.float64_vector cimport Float64Vector


cdef uint8_t _CONST_NULL_BYTE = 0

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
      Dense:    data_length == n — one int64 per row in a DrakenFixedBuffer.
      Constant: data_length == 1 — all rows share a single value at ptr.data[0].

    The vector always owns its buffer in dense mode; from_arrow copies all data.
    """

    def __cinit__(self, size_t length=0):
        self.ptr = alloc_fixed_buffer(DRAKEN_INT64, length, 8)
        self.owns_data = True
        self._precision = 18
        self._scale = 0
        self._unified_view = draken_vector_from_dense(
            self.ptr.data, <uint32_t>length, DRAKEN_INT64, NULL)

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

        cdef bint _is_null = bool(is_null or value is None)
        cdef int64_t const_val = 0

        if not _is_null:
            if isinstance(value, _decimal.Decimal):
                sign, digits, exp_obj = value.as_tuple()
                scale = max(0, -int(exp_obj))
                unscaled = <int64_t>int(value * (_decimal.Decimal(10) ** scale))
            elif isinstance(value, int):
                scale = 0
                unscaled = <int64_t>value
            elif isinstance(value, float):
                d = _decimal.Decimal(str(value))
                sign, digits, exp_obj = d.as_tuple()
                scale = max(0, -int(exp_obj))
                unscaled = <int64_t>int(d * (_decimal.Decimal(10) ** scale))
            else:
                raise TypeError(
                    f"DecimalVector.from_constant: unsupported value type {type(value)!r}"
                )
            const_val = unscaled

        vec = DecimalVector(1)
        (<int64_t*>vec.ptr.data)[0] = const_val
        vec.ptr.length = <size_t>length
        vec.ptr.null_bitmap = NULL

        if not _is_null and isinstance(value, (_decimal.Decimal, float)):
            vec._scale = <int8_t>min(scale, 18)
            if isinstance(value, _decimal.Decimal):
                vec._precision = <int8_t>min(18, max(1, len(str(abs(int(const_val))))))

        vec._unified_view = draken_vector_from_constant(
            vec.ptr.data, <uint32_t>length, DRAKEN_INT64,
            &_CONST_NULL_BYTE if _is_null else NULL)
        return vec

    # ------------------------------------------------------------------
    # C-level accessor protocol (required by Vector base)
    # ------------------------------------------------------------------

    cdef DrakenVector* unified(self) noexcept:
        return &self._unified_view

    # ------------------------------------------------------------------
    # Metadata properties
    # ------------------------------------------------------------------

    @property
    def length(self):
        return self.ptr.length

    def __len__(self):
        return self.ptr.length

    @property
    def itemsize(self):
        return 8

    @property
    def dtype(self):
        return DRAKEN_NON_NATIVE

    @property
    def ordered(self):
        """DecimalVector has no dictionary ordering."""
        return False

    # Producer-layer introspection only — not for dispatch.
    @property
    def code_width(self):
        """DecimalVector has no dictionary encoding."""
        return None

    # Producer-layer introspection only — not for dispatch.
    @property
    def dictionary_size(self):
        """DecimalVector has no dictionary encoding."""
        return 0

    # Producer-layer introspection only — not for dispatch.
    @property
    def dictionary_value_type(self):
        """DecimalVector has no dictionary encoding."""
        return None

    @property
    def null_count(self):
        """Return the number of null values in the vector."""
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = <Py_ssize_t>uv.length
        if uv.validity == NULL:
            return 0
        return n - <Py_ssize_t>simd_popcount(uv.validity, (<size_t>n + 7) >> 3)

    # ------------------------------------------------------------------
    # Element access
    # ------------------------------------------------------------------

    def __getitem__(self, Py_ssize_t i):
        """Return the value at index i as a Python Decimal, or None if null."""
        import decimal
        cdef DrakenVector* uv = self.unified()

        if i < 0 or <size_t>i >= uv.length:
            raise IndexError("Index out of bounds")

        if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
            return None

        cdef object factor = decimal.Decimal(10) ** (-self._scale)
        return decimal.Decimal((<int64_t*>uv.data)[uv.selection[i]]) * factor

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
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n, i, nb_size
        cdef int64_t* src
        cdef int64_t val
        cdef uint8_t[::1] dec_view
        cdef object null_buf, dec_buf, data_buf, arrow_type, factor, pyval

        n = <Py_ssize_t>uv.length
        nb_size = (n + 7) >> 3
        null_buf = None

        if uv.data_length == 1:
            arrow_type = pa.decimal128(
                self._precision if self._precision > 0 else 18, self._scale
            )
            if uv.validity != NULL:
                return pa.array([None] * n, type=arrow_type)
            factor = _decimal.Decimal(10) ** (-self._scale)
            pyval = _decimal.Decimal((<int64_t*>uv.data)[0]) * factor
            return pa.array([pyval] * n, type=arrow_type)

        src = <int64_t*>uv.data

        # Build decimal128 buffer (16 bytes per value, zero-initialised by bytearray).
        dec_buf = bytearray(n * 16)
        dec_view = dec_buf

        for i in range(n):
            val = src[i]
            memcpy(&dec_view[i * 16], &val, 8)           # low 8 bytes: unscaled int64
            if val < 0:
                memset(&dec_view[i * 16 + 8], 0xFF, 8)   # sign-extend high bytes

        if uv.validity != NULL and nb_size > 0:
            null_buf = pa.py_buffer(bytes((<uint8_t*>uv.validity)[:nb_size]))

        data_buf = pa.py_buffer(bytes(dec_buf))
        return pa.Array.from_buffers(
            pa.decimal128(self._precision, self._scale),
            n,
            [null_buf, data_buf],
        )

    cpdef Float64Vector to_float64_vector(self):
        """Convert to a Float64Vector by dividing unscaled int64 values by 10^scale.

        Returns a Float64Vector with the actual decimal values as double-precision
        floating point.  Used for arithmetic operations where DECIMAL semantics
        can be approximated by float64 without unacceptable precision loss.
        """
        cdef DrakenVector* uv = self.unified()
        cdef int64_t* src
        cdef Py_ssize_t i, n
        cdef Float64Vector out
        cdef double* dst
        cdef uint8_t* out_null
        cdef size_t nb_bytes
        cdef double factor

        factor = 10.0 ** (-self._scale)
        n = <Py_ssize_t>uv.length

        out = Float64Vector(<size_t>n)
        src = <int64_t*>uv.data
        dst = <double*>(<void*>out.ptr.data)

        for i in range(n):
            dst[i] = <double>src[<Py_ssize_t>uv.selection[i]] * factor

        if uv.validity != NULL and n > 0:
            nb_bytes = (<size_t>n + 7) >> 3
            out_null = <uint8_t*>malloc(nb_bytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, uv.validity, nb_bytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        return out

    cpdef list to_pylist(self):
        """Return the vector as a Python list of Decimal values (None for nulls)."""
        import decimal
        cdef DrakenVector* uv = self.unified()
        cdef int64_t* data
        cdef Py_ssize_t i, n
        cdef list out
        cdef uint8_t byte, bit
        cdef object factor, val_py

        n = <Py_ssize_t>uv.length
        factor = decimal.Decimal(10) ** (-self._scale)

        data = <int64_t*>uv.data
        out = []

        for i in range(n):
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                out.append(None)
            else:
                out.append(decimal.Decimal(data[<Py_ssize_t>uv.selection[i]]) * factor)
        return out

    # ------------------------------------------------------------------
    # Row selection
    # ------------------------------------------------------------------

    cpdef DecimalVector take(self, int32_t[::1] indices):
        """Return a new DecimalVector containing only the rows named by `indices`."""
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n, out_nbytes
        cdef DecimalVector out
        cdef int64_t* data = <int64_t*>uv.data
        cdef int64_t* dst
        cdef uint8_t* src_null = uv.validity
        cdef uint8_t* out_null = NULL
        cdef int32_t src_idx

        n = indices.shape[0]
        out = DecimalVector(<size_t>n)
        out._precision = self._precision
        out._scale = self._scale
        dst = <int64_t*>out.ptr.data
        out_nbytes = (n + 7) >> 3

        if src_null != NULL and n > 0:
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
            out.ptr.data, <uint32_t>n, DRAKEN_INT64, out.ptr.null_bitmap)
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

    cpdef BoolVector _compare_scalar(self, int op, int64_t rhs):
        cdef DrakenVector* uv = self.unified()
        cdef int64_t* data = <int64_t*>uv.data
        cdef Py_ssize_t i, n, nbytes
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef uint8_t mask

        n = <Py_ssize_t>uv.length
        nbytes = (n + 7) >> 3
        out = BoolVector(<size_t>n)
        dst = <uint8_t*>out.ptr.data

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
                if self._compare_decimal_values(data[uv.selection[i]], rhs, op):
                    dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        return out

    # ------------------------------------------------------------------
    # Vector-vector comparison
    # ------------------------------------------------------------------

    cpdef BoolVector _compare_vector(self, DecimalVector other, int op):
        """Element-wise comparison between two DecimalVectors of the same scale."""
        cdef DrakenVector* uv = self.unified()
        cdef DrakenVector* uv2 = other.unified()
        cdef int64_t* data1 = <int64_t*>uv.data
        cdef int64_t* data2 = <int64_t*>uv2.data
        cdef Py_ssize_t n, nbytes, i
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null = NULL
        cdef bint null1, null2

        n = <Py_ssize_t>uv.length
        nbytes = (n + 7) >> 3

        if n != <Py_ssize_t>uv2.length:
            raise ValueError(
                f"DecimalVector._compare_vector: length mismatch {n} vs {uv2.length}"
            )
        if self._scale != other._scale:
            raise ValueError(
                f"DecimalVector._compare_vector: scale mismatch "
                f"{self._scale} vs {other._scale}"
            )

        out = BoolVector(<size_t>n)
        dst = <uint8_t*>out.ptr.data

        if nbytes > 0:
            memset(dst, 0, nbytes)

        if (uv.validity != NULL or uv2.validity != NULL) and nbytes != 0:
            out_null = <uint8_t*>malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        for i in range(n):
            null1 = uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1)
            null2 = uv2.validity != NULL and not ((uv2.validity[i >> 3] >> (i & 7)) & 1)
            if null1 or null2:
                continue
            if self._compare_decimal_values(data1[uv.selection[i]], data2[uv2.selection[i]], op):
                dst[i >> 3] |= <uint8_t>(1 << (i & 7))
            if out_null != NULL:
                out_null[i >> 3] |= <uint8_t>(1 << (i & 7))

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
        cdef DrakenVector* uv = self.unified()
        cdef int64_t* data
        cdef Py_ssize_t i, n, nbytes
        cdef BoolVector out
        cdef uint8_t* dst
        cdef uint8_t* out_null
        cdef uint8_t mask
        cdef object coerced, item

        n = <Py_ssize_t>uv.length
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

        data = <int64_t*>uv.data

        if uv.validity != NULL and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
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
                if data[<Py_ssize_t>uv.selection[i]] in coerced:
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
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t i, n
        cdef int8_t* buf
        cdef uint8_t byte, bit

        n = <Py_ssize_t>uv.length

        if n == 0:
            return []

        buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()

        if uv.validity == NULL:
            for i in range(n):
                buf[i] = 0
        else:
            for i in range(n):
                buf[i] = 0 if ((uv.validity[i >> 3] >> (i & 7)) & 1) else 1
        return <int8_t[:n]> buf

    # ------------------------------------------------------------------
    # Aggregation  (return Python Decimal so scale is preserved)
    # ------------------------------------------------------------------

    cpdef object sum(self):
        """Return sum as a Python Decimal, skipping nulls.  Returns Decimal(0) for all-null."""
        import decimal as _decimal
        cdef DrakenVector* uv = self.unified()
        cdef int64_t* data = <int64_t*>uv.data
        cdef Py_ssize_t i, n
        cdef int64_t total = 0
        cdef object factor

        n = <Py_ssize_t>uv.length
        factor = _decimal.Decimal(10) ** (-self._scale)

        for i in range(n):
            if uv.validity == NULL or ((uv.validity[i >> 3] >> (i & 7)) & 1):
                total += data[uv.selection[i]]
        return _decimal.Decimal(total) * factor

    cpdef object min(self):
        """Return minimum as a Python Decimal, excluding nulls."""
        import decimal as _decimal
        cdef DrakenVector* uv = self.unified()
        cdef int64_t* data = <int64_t*>uv.data
        cdef Py_ssize_t i, n, start
        cdef int64_t m
        cdef bint found = False
        cdef object factor

        n = <Py_ssize_t>uv.length
        factor = _decimal.Decimal(10) ** (-self._scale)

        if n == 0:
            raise ValueError("Cannot compute min of empty DecimalVector")

        for i in range(n):
            if uv.validity == NULL or ((uv.validity[i >> 3] >> (i & 7)) & 1):
                m = data[uv.selection[i]]
                found = True
                start = i + 1
                break

        if not found:
            raise ValueError("Cannot compute min of all-null DecimalVector")

        for i in range(start, n):
            if uv.validity == NULL or ((uv.validity[i >> 3] >> (i & 7)) & 1):
                if data[uv.selection[i]] < m:
                    m = data[uv.selection[i]]
        return _decimal.Decimal(m) * factor

    cpdef object max(self):
        """Return maximum as a Python Decimal, excluding nulls."""
        import decimal as _decimal
        cdef DrakenVector* uv = self.unified()
        cdef int64_t* data = <int64_t*>uv.data
        cdef Py_ssize_t i, n, start
        cdef int64_t m
        cdef bint found = False
        cdef object factor

        n = <Py_ssize_t>uv.length
        factor = _decimal.Decimal(10) ** (-self._scale)

        if n == 0:
            raise ValueError("Cannot compute max of empty DecimalVector")

        for i in range(n):
            if uv.validity == NULL or ((uv.validity[i >> 3] >> (i & 7)) & 1):
                m = data[uv.selection[i]]
                found = True
                start = i + 1
                break

        if not found:
            raise ValueError("Cannot compute max of all-null DecimalVector")

        for i in range(start, n):
            if uv.validity == NULL or ((uv.validity[i >> 3] >> (i & 7)) & 1):
                if data[uv.selection[i]] > m:
                    m = data[uv.selection[i]]
        return _decimal.Decimal(m) * factor

    # ------------------------------------------------------------------
    # Hashing — identical logic to Integer64Vector since storage IS int64
    # ------------------------------------------------------------------

    cdef inline void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0
    ) except *:
        cdef DrakenVector* uv = self.unified()
        cdef uint64_t* dst
        cdef uint64_t* as_uint64 = <uint64_t*>uv.data
        cdef Py_ssize_t n, i, j, block
        cdef uint64_t is_valid
        cdef uint64_t[DECIMAL_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr

        n = <Py_ssize_t>uv.length
        scratch_ptr = <uint64_t*>scratch

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("DecimalVector.hash_into: output buffer too small")

        dst = (&out_buf[0]) + offset

        if uv.validity != NULL:
            i = 0
            while i < n:
                block = n - i
                if block > DECIMAL_HASH_CHUNK:
                    block = DECIMAL_HASH_CHUNK
                for j in range(block):
                    is_valid = (uv.validity[(i + j) >> 3] >> ((i + j) & 7)) & 1
                    scratch[j] = (as_uint64[<Py_ssize_t>uv.selection[i + j]] * is_valid) | (NULL_HASH * (1 - is_valid))
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block
        else:
            i = 0
            while i < n:
                block = n - i
                if block > DECIMAL_HASH_CHUNK:
                    block = DECIMAL_HASH_CHUNK
                for j in range(block):
                    scratch[j] = as_uint64[<Py_ssize_t>uv.selection[i + j]]
                simd_mix_hash(dst + i, scratch_ptr, <size_t>block)
                i += block

    # ------------------------------------------------------------------
    # Compression — unscaled int64 values pass through directly
    # ------------------------------------------------------------------

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Write unscaled int64 values into out_buf.  Null rows emit INT64_MIN."""
        cdef DrakenVector* uv = self.unified()
        cdef int64_t* data = <int64_t*>uv.data
        cdef int64_t* dst
        cdef Py_ssize_t n, i

        n = <Py_ssize_t>uv.length

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("DecimalVector.compress_into: output buffer too small")

        dst = &out_buf[offset]

        for i in range(n):
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                dst[i] = INT64_MIN_VALUE
            else:
                dst[i] = data[uv.selection[i]]

    # ------------------------------------------------------------------
    # Debug representation
    # ------------------------------------------------------------------

    def __str__(self):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n, i, k
        cdef int64_t* data
        cdef list vals

        n = <Py_ssize_t>uv.length

        vals = []
        k = min(n, 10)
        data = <int64_t*>uv.data
        for i in range(k):
            if uv.validity != NULL and not ((uv.validity[i >> 3] >> (i & 7)) & 1):
                vals.append(None)
            else:
                vals.append(data[<Py_ssize_t>uv.selection[i]])
        return (
            f"<DecimalVector len={n} "
            f"precision={self._precision} scale={self._scale} "
            f"values={vals}>"
        )


# ---------------------------------------------------------------------------
# Module-level factory: from_int64_vector
# ---------------------------------------------------------------------------

cpdef DecimalVector from_int64_vector(Integer64Vector source, int precision, int scale):
    """
    Build a DecimalVector from an Integer64Vector containing unscaled integer values.

    This is the native Draken conversion path for parquet DECIMAL columns, which
    are stored as INT64 physical type with a decimal logical type annotation.

    The resulting vector owns its buffer (copied, never borrowed).
    Scale and precision are set from the caller-provided values.

    Args:
        source:    Integer64Vector with unscaled integer values (dense or const)
        precision: Decimal precision (max 18 for int64 backing)
        scale:     Decimal scale (number of digits after decimal point)

    Returns:
        DecimalVector with the same data and null bitmap as source.
    """
    cdef Py_ssize_t n
    cdef DecimalVector out
    cdef int64_t* src_data
    cdef int64_t* dst_data
    cdef uint8_t* src_null
    cdef uint8_t* out_null
    cdef size_t nb_bytes
    cdef DrakenVector* src_uv
    cdef int64_t const_val_i64

    if precision > 18:
        raise NotImplementedError(
            f"DecimalVector supports precision up to 18; got precision={precision}."
        )

    src_uv = source.unified()
    n = <Py_ssize_t>src_uv.length

    # Allocate output and copy data via uniform selection[i] access
    out = DecimalVector(<size_t>n)
    out._precision = <int8_t>precision
    out._scale = <int8_t>scale

    if n > 0:
        src_data = <int64_t*>src_uv.data
        dst_data = <int64_t*>out.ptr.data
        for i in range(n):
            if src_uv.validity != NULL and not ((src_uv.validity[i >> 3] >> (i & 7)) & 1):
                dst_data[i] = 0
            else:
                dst_data[i] = src_data[<Py_ssize_t>src_uv.selection[i]]

    # Copy null bitmap
    if src_uv.validity != NULL and n > 0:
        nb_bytes = (<size_t>n + 7) >> 3
        out_null = <uint8_t*>malloc(nb_bytes)
        if out_null == NULL:
            raise MemoryError()
        memcpy(out_null, src_uv.validity, nb_bytes)
        out.ptr.null_bitmap = out_null
    else:
        out.ptr.null_bitmap = NULL

    out._unified_view = draken_vector_from_dense(
        out.ptr.data, <uint32_t>n, DRAKEN_INT64, out.ptr.null_bitmap)
    return out
