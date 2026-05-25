# cython: language_level=3
# Cython shim for draken.vectors.string_vector — E.24 vtable bridge.

from cpython.object cimport PyObject
from libc.stdint cimport int32_t, uint8_t, uint32_t
from libc.stddef cimport size_t

from draken.core.buffers cimport (
    DrakenVector, DrakenStringArena, DrakenVarBuffer,
)
from draken.vectors.vector cimport Vector

cdef extern from "core/draken_bridge.h":
    const DrakenVector* draken_vector_unwrap(PyObject* obj)


# StringElement must match the struct in string_vector.pxd exactly.
cdef struct StringElement:
    char* ptr
    Py_ssize_t length
    int is_null


cdef class StringVector(Vector):
    @classmethod
    def from_constant(cls, value, num_rows, is_null=False):
        from draken.draken_native import vector_from_string_sequence
        if is_null:
            return cls(vector_from_string_sequence([None] * num_rows))
        str_val = value.decode("utf-8") if isinstance(value, bytes) else str(value)
        return cls(vector_from_string_sequence([str_val] * num_rows))

    cdef _StringVectorCIterator c_iter(self):
        raise NotImplementedError("StringVector.c_iter not implemented in E.24 shim")


cdef class StringVectorBuilder:
    @staticmethod
    def with_estimate(Py_ssize_t capacity, Py_ssize_t avg_len):
        cdef StringVectorBuilder b = StringVectorBuilder.__new__(StringVectorBuilder)
        b._strs = []
        return b

    def append(self, bytes value):
        self._strs.append(value)

    def append_null(self):
        self._strs.append(None)

    cdef void append_bytes(self, const char* data, Py_ssize_t length):
        if data == NULL:
            self._strs.append(b"")
        else:
            self._strs.append(bytes(data[:length]))

    def finish(self):
        from draken.draken_native import vector_from_string_sequence
        decoded = [s.decode('utf-8', errors='replace') for s in self._strs]
        return StringVector(vector_from_string_sequence(decoded))


cdef class _StringVectorCIterator:
    cdef bint next(self, StringElement* elem) noexcept:
        return 0


cdef DrakenStringArena* _varbuffer_to_string_arena(
    const uint8_t* src_data,
    const int32_t* src_offsets,
    const uint8_t* src_nulls,
    Py_ssize_t row_count,
) except? NULL:
    raise NotImplementedError("_varbuffer_to_string_arena not implemented in E.24 shim")


cdef StringVector from_dict_buffers(
    const int32_t[::1] codes,
    const int32_t[::1] dict_offsets,
    const int32_t[::1] dict_lengths,
    const uint8_t[::1] arena_bytes,
    object row_validity=None,
):
    from draken.draken_native import vector_from_string_sequence
    cdef Py_ssize_t n = len(codes)
    cdef Py_ssize_t dict_n = len(dict_lengths)
    cdef Py_ssize_t i, off, ln, code

    # Build dictionary entry strings
    cdef list dict_strs = []
    for i in range(dict_n):
        off = dict_offsets[i]
        ln = dict_lengths[i]
        dict_strs.append(bytes(arena_bytes[off:off + ln]).decode('utf-8', errors='replace'))

    # Expand codes to row values
    cdef list rows = []
    for i in range(n):
        code = codes[i]
        if code < 0 or code >= dict_n:
            rows.append(None)
        else:
            rows.append(dict_strs[code])

    # Apply row validity bitmap
    if row_validity is not None:
        for i in range(n):
            if not ((row_validity[i >> 3] >> (i & 7)) & 1):
                rows[i] = None

    return StringVector(vector_from_string_sequence(rows))


cdef StringVector from_packed_dict(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const int32_t* dict_offsets,
    const uint8_t* dict_data,
    Py_ssize_t dict_size,
    const uint8_t* row_null_bitmap=NULL,
    bint ordered=False,
    const uint8_t* dict_entry_null_bitmap=NULL,
):
    raise NotImplementedError("StringVector.from_packed_dict not implemented in E.24 shim")


cdef StringVector make_string_dict_only(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t row_count,
    const uint32_t* dict_offsets,
    const uint8_t* dict_data,
    Py_ssize_t dict_size,
    Py_ssize_t arena_size,
    const uint8_t* valid_bits,
):
    from draken.draken_native import vector_from_string_sequence
    cdef list dict_strs = []
    cdef Py_ssize_t i, off, end_off, ln, code
    cdef uint8_t byte_val

    for i in range(dict_size):
        off = dict_offsets[i]
        end_off = dict_offsets[i + 1]
        ln = end_off - off
        dict_strs.append(bytes(dict_data[off:end_off]).decode('utf-8', errors='replace'))

    cdef list rows = []
    for i in range(row_count):
        if code_width == 1:
            code = codes[i]
        elif code_width == 2:
            code = codes[2 * i] | (codes[2 * i + 1] << 8)
        else:  # code_width == 4
            code = (codes[4 * i] | (codes[4 * i + 1] << 8) |
                    (codes[4 * i + 2] << 16) | (codes[4 * i + 3] << 24))
        rows.append(dict_strs[code] if 0 <= code < dict_size else None)

    if valid_bits != NULL:
        for i in range(row_count):
            if not ((valid_bits[i >> 3] >> (i & 7)) & 1):
                rows[i] = None

    return StringVector(vector_from_string_sequence(rows))
