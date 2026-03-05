# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from cpython.buffer cimport PyBUF_READ
from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.memoryview cimport PyMemoryView_FromMemory
from libc.stddef cimport size_t
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport intptr_t
from libc.stdint cimport uint64_t
from libc.stdint cimport uint8_t
from libc.stdlib cimport free
from libc.stdlib cimport malloc
from libc.string cimport memcpy
from libc.string cimport memset

from opteryx.compiled.structures.relation_statistics cimport to_int
from opteryx.draken.core.buffers cimport DRAKEN_BOOL
from opteryx.draken.core.buffers cimport DRAKEN_CONSTANT
from opteryx.draken.core.buffers cimport DRAKEN_FLOAT64
from opteryx.draken.core.buffers cimport DRAKEN_INT64
from opteryx.draken.core.buffers cimport DRAKEN_STRING
from opteryx.draken.core.buffers cimport DrakenConstantBuffer
from opteryx.draken.core.buffers cimport DrakenConstantStringPayload
from opteryx.draken.core.buffers cimport DrakenType
from opteryx.draken.vectors.bool_vector cimport BoolVector
from opteryx.draken.vectors.vector cimport NULL_HASH
from opteryx.draken.vectors.vector cimport Vector
from opteryx.draken.vectors.vector cimport mix_hash


cdef extern from *:
    """
    #define XXH_INLINE_ALL
    #include "xxhash.h"
    """
    uint64_t XXH3_64bits(const void* input, size_t length) nogil


cdef const uint64_t TRUE_HASH = <uint64_t>0x4f112caa54efa882
cdef const uint64_t FALSE_HASH = <uint64_t>0xc2fd8b2343f83ce7


cdef inline bint _supported_value_type(int value_type) noexcept nogil:
    return (
        value_type == DRAKEN_INT64
        or value_type == DRAKEN_FLOAT64
        or value_type == DRAKEN_BOOL
        or value_type == DRAKEN_STRING
    )


cdef inline void _set_true_bit(uint8_t* bits, Py_ssize_t i) noexcept nogil:
    bits[i >> 3] |= <uint8_t>(1 << (i & 7))


cdef inline bint _is_valid(uint8_t* bits, Py_ssize_t i) noexcept nogil:
    if bits == NULL:
        return True
    return ((bits[i >> 3] >> (i & 7)) & 1) != 0


cdef inline void _set_all_bits(uint8_t* bits, Py_ssize_t n) noexcept nogil:
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef uint8_t mask
    if nbytes <= 0:
        return
    memset(bits, 0xFF, nbytes)
    if (n & 7) != 0:
        mask = <uint8_t>((1 << (n & 7)) - 1)
        bits[nbytes - 1] &= mask


cdef inline object _coerce_literal_bytes(object literal):
    if literal is None:
        return None
    if hasattr(literal, "as_py"):
        try:
            literal = literal.as_py()
        except Exception:
            return None
    if isinstance(literal, (bytes, bytearray, memoryview)):
        try:
            return bytes(literal)
        except Exception:
            return None
    if isinstance(literal, str):
        try:
            return literal.encode("utf8")
        except Exception:
            return None
    return None


cdef inline object _default_value_for_type(int value_type):
    if value_type == DRAKEN_INT64:
        return 0
    if value_type == DRAKEN_FLOAT64:
        return 0.0
    if value_type == DRAKEN_BOOL:
        return False
    if value_type == DRAKEN_STRING:
        return b""
    return None


cdef int _value_type_from_dtype(object dtype):
    cdef int dtype_int
    if dtype is None:
        return -1

    if isinstance(dtype, int):
        dtype_int = <int>dtype
        if _supported_value_type(dtype_int):
            return dtype_int
        return -1

    try:
        import pyarrow as pa

        if pa.types.is_boolean(dtype):
            return DRAKEN_BOOL
        if pa.types.is_integer(dtype):
            return DRAKEN_INT64
        if pa.types.is_floating(dtype):
            return DRAKEN_FLOAT64
        if pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_binary(dtype):
            return DRAKEN_STRING
    except Exception:
        return -1

    return -1


cdef int _infer_value_type(object value):
    if value is None:
        return -1
    if hasattr(value, "as_py"):
        try:
            value = value.as_py()
        except Exception:
            return -1
    if isinstance(value, bool):
        return DRAKEN_BOOL
    if isinstance(value, int):
        return DRAKEN_INT64
    if isinstance(value, float):
        return DRAKEN_FLOAT64
    if isinstance(value, (bytes, str)):
        return DRAKEN_STRING
    return -1


cdef object _coerce_value_for_type(object value, int value_type, bint* ok):
    cdef object coerced
    ok[0] = True
    if hasattr(value, "as_py"):
        try:
            value = value.as_py()
        except Exception:
            ok[0] = False
            return None

    try:
        if value_type == DRAKEN_INT64:
            return int(value)
        if value_type == DRAKEN_FLOAT64:
            return float(value)
        if value_type == DRAKEN_BOOL:
            return bool(value)
        if value_type == DRAKEN_STRING:
            coerced = _coerce_literal_bytes(value)
            if coerced is None:
                ok[0] = False
            return coerced
    except Exception:
        ok[0] = False
        return None

    ok[0] = False
    return None


cdef class ConstantVector(Vector):
    def __cinit__(
        self,
        size_t length=0,
        int value_type=DRAKEN_INT64,
        object value=0,
        object null_bitmap=None,
        bint wrap=False,
    ):
        if wrap:
            self.ptr = NULL
            self.owns_data = False
            return

        self.ptr = <DrakenConstantBuffer*>malloc(sizeof(DrakenConstantBuffer))
        if self.ptr == NULL:
            raise MemoryError()

        self.ptr.type = DRAKEN_CONSTANT
        self.ptr.value_type = <DrakenType>value_type
        self.ptr.value = NULL
        self.ptr.length = length
        self.ptr.null_bitmap = NULL
        self.owns_data = True

        if not _supported_value_type(value_type):
            raise TypeError("ConstantVector only supports int64, float64, bool, and string values")

        self._set_scalar(value)
        self._set_null_bitmap(null_bitmap)

    def __dealloc__(self):
        cdef DrakenConstantStringPayload* payload
        if self.ptr == NULL:
            return
        if self.owns_data:
            if self.ptr.null_bitmap != NULL:
                free(self.ptr.null_bitmap)
                self.ptr.null_bitmap = NULL
            if self.ptr.value != NULL:
                if self.ptr.value_type == DRAKEN_STRING:
                    payload = <DrakenConstantStringPayload*>self.ptr.value
                    if payload.data != NULL:
                        free(payload.data)
                    free(payload)
                else:
                    free(self.ptr.value)
                self.ptr.value = NULL
        free(self.ptr)
        self.ptr = NULL

    cdef void _set_scalar(self, object value) except *:
        cdef int64_t* i64_ptr
        cdef double* f64_ptr
        cdef uint8_t* b_ptr
        cdef DrakenConstantStringPayload* s_ptr
        cdef bytes value_bytes
        cdef char* src
        cdef Py_ssize_t src_len
        cdef object coerced
        cdef bint ok = True

        if self.ptr.value != NULL:
            if self.ptr.value_type == DRAKEN_STRING:
                s_ptr = <DrakenConstantStringPayload*>self.ptr.value
                if s_ptr.data != NULL:
                    free(s_ptr.data)
                free(s_ptr)
            else:
                free(self.ptr.value)
            self.ptr.value = NULL

        coerced = _coerce_value_for_type(value, self.ptr.value_type, &ok) if value is not None else _default_value_for_type(self.ptr.value_type)
        if not ok:
            raise TypeError("Unable to coerce constant value to requested type")

        if self.ptr.value_type == DRAKEN_INT64:
            i64_ptr = <int64_t*>malloc(sizeof(int64_t))
            if i64_ptr == NULL:
                raise MemoryError()
            i64_ptr[0] = <int64_t>coerced
            self.ptr.value = <void*>i64_ptr
            return

        if self.ptr.value_type == DRAKEN_FLOAT64:
            f64_ptr = <double*>malloc(sizeof(double))
            if f64_ptr == NULL:
                raise MemoryError()
            f64_ptr[0] = <double>coerced
            self.ptr.value = <void*>f64_ptr
            return

        if self.ptr.value_type == DRAKEN_BOOL:
            b_ptr = <uint8_t*>malloc(sizeof(uint8_t))
            if b_ptr == NULL:
                raise MemoryError()
            b_ptr[0] = 1 if coerced else 0
            self.ptr.value = <void*>b_ptr
            return

        # DRAKEN_STRING
        value_bytes = bytes(coerced) if coerced is not None else b""
        s_ptr = <DrakenConstantStringPayload*>malloc(sizeof(DrakenConstantStringPayload))
        if s_ptr == NULL:
            raise MemoryError()
        s_ptr.length = <int32_t>len(value_bytes)
        s_ptr.data = NULL
        if s_ptr.length > 0:
            if PyBytes_AsStringAndSize(value_bytes, &src, &src_len) != 0:
                free(s_ptr)
                raise ValueError("invalid constant string value")
            s_ptr.data = <uint8_t*>malloc(<size_t>src_len)
            if s_ptr.data == NULL:
                free(s_ptr)
                raise MemoryError()
            memcpy(s_ptr.data, <const void*>src, <size_t>src_len)
        self.ptr.value = <void*>s_ptr

    cdef void _set_null_bitmap(self, object null_bitmap) except *:
        cdef bytes payload
        cdef char* src
        cdef Py_ssize_t src_len
        cdef Py_ssize_t expected_len
        cdef uint8_t* dst
        cdef uint8_t mask

        if self.ptr.null_bitmap != NULL:
            free(self.ptr.null_bitmap)
            self.ptr.null_bitmap = NULL

        if null_bitmap is None:
            return

        payload = bytes(null_bitmap)
        if PyBytes_AsStringAndSize(payload, &src, &src_len) != 0:
            raise ValueError("invalid null bitmap payload")

        expected_len = (self.ptr.length + 7) >> 3
        if src_len != expected_len:
            raise ValueError(
                f"ConstantVector null bitmap length mismatch: expected {expected_len}, got {src_len}"
            )
        if expected_len == 0:
            return

        dst = <uint8_t*>malloc(<size_t>expected_len)
        if dst == NULL:
            raise MemoryError()
        memcpy(dst, <const void*>src, <size_t>expected_len)
        if (self.ptr.length & 7) != 0:
            mask = <uint8_t>((1 << (self.ptr.length & 7)) - 1)
            dst[expected_len - 1] &= mask
        self.ptr.null_bitmap = dst

    @property
    def length(self):
        return self.ptr.length

    def __len__(self):
        return self.ptr.length

    @property
    def dtype(self):
        return DRAKEN_CONSTANT

    @property
    def value_type(self):
        return self.ptr.value_type

    @property
    def null_count(self):
        cdef Py_ssize_t i
        cdef Py_ssize_t n = self.ptr.length
        cdef Py_ssize_t count = 0
        cdef uint8_t byte
        if self.ptr.null_bitmap == NULL:
            return 0
        for i in range(n):
            byte = self.ptr.null_bitmap[i >> 3]
            if ((byte >> (i & 7)) & 1) == 0:
                count += 1
        return count

    cpdef object null_bitmap(self):
        cdef Py_ssize_t nb_size
        if self.ptr.null_bitmap == NULL:
            return None
        nb_size = (self.ptr.length + 7) // 8
        if nb_size == 0:
            return None
        return PyMemoryView_FromMemory(<char*>self.ptr.null_bitmap, nb_size, PyBUF_READ)

    cpdef object scalar_value(self):
        cdef DrakenConstantStringPayload* s_ptr
        if self.ptr.value == NULL:
            return None
        if self.ptr.value_type == DRAKEN_INT64:
            return (<int64_t*>self.ptr.value)[0]
        if self.ptr.value_type == DRAKEN_FLOAT64:
            return (<double*>self.ptr.value)[0]
        if self.ptr.value_type == DRAKEN_BOOL:
            return (<uint8_t*>self.ptr.value)[0] != 0
        if self.ptr.value_type == DRAKEN_STRING:
            s_ptr = <DrakenConstantStringPayload*>self.ptr.value
            if s_ptr.length <= 0 or s_ptr.data == NULL:
                return b""
            return PyBytes_FromStringAndSize(<const char*>s_ptr.data, s_ptr.length)
        raise TypeError("Unsupported constant value type")

    def __getitem__(self, Py_ssize_t i):
        if i < 0 or i >= <Py_ssize_t>self.ptr.length:
            raise IndexError("Index out of range")
        if not _is_valid(self.ptr.null_bitmap, i):
            return None
        return self.scalar_value()

    cpdef list to_pylist(self):
        cdef Py_ssize_t i
        cdef Py_ssize_t n = self.ptr.length
        cdef object scalar
        cdef list out
        if n == 0:
            return []
        scalar = self.scalar_value()
        if self.ptr.null_bitmap == NULL:
            return [scalar] * n

        out = [None] * n
        for i in range(n):
            if _is_valid(self.ptr.null_bitmap, i):
                out[i] = scalar
        return out

    def to_arrow(self):
        import pyarrow as pa

        cdef Py_ssize_t n = self.ptr.length
        cdef object arrow_type
        cdef object values
        cdef object values_buffers
        cdef object null_buf

        if self.ptr.value_type == DRAKEN_INT64:
            arrow_type = pa.int64()
        elif self.ptr.value_type == DRAKEN_FLOAT64:
            arrow_type = pa.float64()
        elif self.ptr.value_type == DRAKEN_BOOL:
            arrow_type = pa.bool_()
        elif self.ptr.value_type == DRAKEN_STRING:
            arrow_type = pa.binary()
        else:
            raise TypeError("Unsupported constant value type")

        if n == 0:
            return pa.array([], type=arrow_type)

        values = pa.repeat(pa.scalar(self.scalar_value(), type=arrow_type), n)
        if self.ptr.null_bitmap == NULL:
            return values

        null_buf = pa.foreign_buffer(<intptr_t>self.ptr.null_bitmap, (n + 7) // 8, base=self)
        values_buffers = values.buffers()
        if self.ptr.value_type == DRAKEN_STRING:
            return pa.Array.from_buffers(
                arrow_type,
                n,
                [
                    null_buf,
                    values_buffers[1],
                    values_buffers[2],
                ],
            )
        return pa.Array.from_buffers(
            arrow_type,
            n,
            [
                null_buf,
                values_buffers[1],
            ],
        )

    cpdef ConstantVector take(self, int32_t[::1] indices):
        cdef Py_ssize_t i
        cdef Py_ssize_t n = indices.shape[0]
        cdef ConstantVector out = ConstantVector(
            <size_t>n,
            self.ptr.value_type,
            self.scalar_value(),
        )
        cdef int32_t src_idx
        cdef Py_ssize_t nbytes
        cdef uint8_t* out_null

        if n == 0:
            return out

        if self.ptr.null_bitmap == NULL:
            for i in range(n):
                src_idx = indices[i]
                if src_idx < 0 or src_idx >= <int32_t>self.ptr.length:
                    raise IndexError("Index out of range")
            return out

        nbytes = (n + 7) >> 3
        out_null = <uint8_t*>malloc(<size_t>nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)

        for i in range(n):
            src_idx = indices[i]
            if src_idx < 0 or src_idx >= <int32_t>self.ptr.length:
                free(out_null)
                raise IndexError("Index out of range")
            if _is_valid(self.ptr.null_bitmap, src_idx):
                _set_true_bit(out_null, i)

        out.ptr.null_bitmap = out_null
        return out

    cpdef BoolVector equals(self, object literal):
        return self._compare_scalar(literal, 0)

    cpdef BoolVector not_equals(self, object literal):
        return self._compare_scalar(literal, 1)

    cpdef BoolVector less_than(self, object literal):
        return self._compare_scalar(literal, 2)

    cpdef BoolVector greater_than(self, object literal):
        return self._compare_scalar(literal, 3)

    cpdef BoolVector less_than_or_equals(self, object literal):
        return self._compare_scalar(literal, 4)

    cpdef BoolVector greater_than_or_equals(self, object literal):
        return self._compare_scalar(literal, 5)

    cdef bint _compare_values(self, object left, object right, int op) except *:
        if op == 0:
            return left == right
        if op == 1:
            return left != right
        if op == 2:
            return left < right
        if op == 3:
            return left > right
        if op == 4:
            return left <= right
        return left >= right

    cdef BoolVector _compare_scalar(self, object literal, int op):
        cdef Py_ssize_t n = self.ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* out_bits = <uint8_t*>out.ptr.data
        cdef bint parse_ok = True
        cdef object coerced
        cdef object scalar
        cdef bint match
        cdef Py_ssize_t i
        cdef bint ok = True

        if nbytes > 0:
            memset(out_bits, 0, nbytes)
        out.ptr.null_bitmap = NULL

        if self.ptr.value_type == DRAKEN_STRING and op in (2, 3, 4, 5):
            raise TypeError("Constant string range comparisons are not supported")

        if literal is None:
            parse_ok = False
        else:
            coerced = _coerce_value_for_type(literal, self.ptr.value_type, &ok)
            parse_ok = ok

        if not parse_ok:
            if op == 1:
                if self.ptr.null_bitmap == NULL:
                    _set_all_bits(out_bits, n)
                else:
                    for i in range(n):
                        if _is_valid(self.ptr.null_bitmap, i):
                            _set_true_bit(out_bits, i)
            return out

        scalar = self.scalar_value()
        match = self._compare_values(scalar, coerced, op)
        if not match:
            return out

        if self.ptr.null_bitmap == NULL:
            _set_all_bits(out_bits, n)
            return out

        for i in range(n):
            if _is_valid(self.ptr.null_bitmap, i):
                _set_true_bit(out_bits, i)
        return out

    cpdef BoolVector in_list(self, object literals):
        cdef Py_ssize_t n = self.ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* out_bits = <uint8_t*>out.ptr.data
        cdef object literal
        cdef bint include_null = False
        cdef bint matched = False
        cdef object scalar = self.scalar_value()
        cdef object coerced
        cdef bint ok = True
        cdef Py_ssize_t i

        if nbytes > 0:
            memset(out_bits, 0, nbytes)
        out.ptr.null_bitmap = NULL

        if literals is None:
            return out

        for literal in literals:
            if literal is None:
                include_null = True
                continue
            coerced = _coerce_value_for_type(literal, self.ptr.value_type, &ok)
            if not ok:
                ok = True
                continue
            if scalar == coerced:
                matched = True

        if self.ptr.null_bitmap == NULL:
            if matched:
                _set_all_bits(out_bits, n)
            return out

        for i in range(n):
            if _is_valid(self.ptr.null_bitmap, i):
                if matched:
                    _set_true_bit(out_bits, i)
            elif include_null:
                _set_true_bit(out_bits, i)
        return out

    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        cdef Py_ssize_t n = self.ptr.length
        cdef Py_ssize_t i
        cdef uint64_t value_hash
        cdef DrakenConstantStringPayload* s_ptr

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("ConstantVector.hash_into: output buffer too small")

        if self.ptr.value_type == DRAKEN_INT64:
            value_hash = <uint64_t>(<int64_t*>self.ptr.value)[0]
        elif self.ptr.value_type == DRAKEN_FLOAT64:
            value_hash = (<uint64_t*>self.ptr.value)[0]
        elif self.ptr.value_type == DRAKEN_BOOL:
            value_hash = TRUE_HASH if (<uint8_t*>self.ptr.value)[0] != 0 else FALSE_HASH
        elif self.ptr.value_type == DRAKEN_STRING:
            s_ptr = <DrakenConstantStringPayload*>self.ptr.value
            value_hash = XXH3_64bits(<const void*>s_ptr.data, <size_t>s_ptr.length)
        else:
            raise TypeError("Unsupported constant value type for hashing")

        if self.ptr.null_bitmap == NULL:
            for i in range(n):
                out_buf[offset + i] = mix_hash(out_buf[offset + i], value_hash)
            return

        for i in range(n):
            if _is_valid(self.ptr.null_bitmap, i):
                out_buf[offset + i] = mix_hash(out_buf[offset + i], value_hash)
            else:
                out_buf[offset + i] = mix_hash(out_buf[offset + i], NULL_HASH)

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        cdef Py_ssize_t n = self.ptr.length
        cdef Py_ssize_t i
        cdef int64_t scalar = <int64_t>to_int(self.scalar_value())
        cdef int64_t null_value = <int64_t>to_int(None)

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("ConstantVector.compress_into: output buffer too small")

        if self.ptr.null_bitmap == NULL:
            for i in range(n):
                out_buf[offset + i] = scalar
            return

        for i in range(n):
            if _is_valid(self.ptr.null_bitmap, i):
                out_buf[offset + i] = scalar
            else:
                out_buf[offset + i] = null_value


cdef object _try_build_constant_from_sequence(object seq, object dtype):
    cdef Py_ssize_t n
    cdef Py_ssize_t i
    cdef object value
    cdef object base_value = None
    cdef bint has_value = False
    cdef int value_type
    cdef object coerced
    cdef bint ok = True
    cdef int null_count = 0
    cdef bytearray validity
    cdef uint8_t* valid_bits
    cdef bytes validity_bytes
    cdef Py_ssize_t nbytes

    if seq is None:
        return None
    n = len(seq)
    if n == 0:
        return None

    nbytes = (n + 7) >> 3
    validity = bytearray(nbytes)
    for i in range(nbytes):
        validity[i] = 0xFF

    for i in range(n):
        value = seq[i]
        if hasattr(value, "as_py"):
            try:
                value = value.as_py()
            except Exception:
                return None
        if value is None:
            null_count += 1
            validity[i >> 3] &= <uint8_t>(~(1 << (i & 7)))
            continue
        if not has_value:
            base_value = value
            has_value = True

    if not has_value:
        return from_scalar(None, <size_t>n, dtype=dtype)

    value_type = _value_type_from_dtype(dtype)
    if value_type < 0:
        value_type = _infer_value_type(base_value)
    if value_type < 0 or not _supported_value_type(value_type):
        return None

    coerced = _coerce_value_for_type(base_value, value_type, &ok)
    if not ok:
        return None

    for i in range(n):
        value = seq[i]
        if value is None:
            continue
        value = _coerce_value_for_type(value, value_type, &ok)
        if not ok:
            return None
        if value != coerced:
            return None

    if null_count == 0:
        return ConstantVector(<size_t>n, value_type, coerced)

    validity_bytes = bytes(validity)
    return ConstantVector(<size_t>n, value_type, coerced, validity_bytes)


cdef object from_sequence(object data, object dtype=None):
    cdef object seq

    if isinstance(data, (bytes, bytearray, memoryview, str, dict)):
        return None

    if isinstance(data, (list, tuple)):
        return _try_build_constant_from_sequence(data, dtype)

    if hasattr(data, "__iter__") and hasattr(data, "__len__"):
        try:
            seq = list(data)
        except Exception:
            return None
        return _try_build_constant_from_sequence(seq, dtype)

    return None


cpdef object from_scalar(object value, size_t length, object dtype=None):
    cdef int value_type
    cdef object coerced
    cdef bint ok = True
    cdef bytes null_bitmap = None

    value_type = _value_type_from_dtype(dtype)
    if value_type < 0:
        value_type = _infer_value_type(value)
    if value is None and value_type < 0:
        return None
    if value_type < 0 or not _supported_value_type(value_type):
        return None

    if value is None:
        coerced = _default_value_for_type(value_type)
        if length > 0:
            null_bitmap = bytes((length + 7) >> 3)
        return ConstantVector(length, value_type, coerced, null_bitmap)

    coerced = _coerce_value_for_type(value, value_type, &ok)
    if not ok:
        return None
    return ConstantVector(length, value_type, coerced)
