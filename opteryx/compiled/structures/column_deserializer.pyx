# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True

"""
Deserialize IPC blobs from MemoryPool into Draken vectors.

Reads the binary format produced by ipc_serialize.hpp using typed pointer
arithmetic — no struct module, no json, no Python objects in the hot path.

All vectors are constructed with owned memory (alloc + memcpy) so there are
no lifetime dependencies on the MemoryPool read buffer.
"""

from libc.stdint cimport uint8_t, int32_t, int64_t, uint32_t, uint64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy

from opteryx.compiled.structures.memory_pool cimport MemoryPool

from draken.vectors.int64_vector cimport Int64Vector
from draken.vectors.int64_vector cimport from_packed_dict as int64_from_packed_dict
from draken.vectors.int64_vector cimport make_int64_dict_only
from draken.vectors.float64_vector cimport Float64Vector
from draken.vectors.float64_vector cimport from_packed_dict as float64_from_packed_dict
from draken.vectors.float64_vector cimport make_float64_dict_only
from draken.vectors.float32_vector cimport Float32Vector
from draken.vectors.float32_vector cimport from_packed_dict as float32_from_packed_dict
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.string_vector cimport StringVector, from_packed_dict, from_dict_buffers, make_string_dict_only

# Type tags — must match ipc_serialize.hpp
DEF TAG_INT64       = 1
DEF TAG_INT32       = 2
DEF TAG_FLOAT32     = 3
DEF TAG_FLOAT64     = 4
DEF TAG_BOOL        = 5
DEF TAG_STR_DICT    = 6
DEF TAG_STR_PLAIN   = 7
DEF TAG_INT64_DICT  = 8
DEF TAG_FLOAT32_DICT = 9
DEF TAG_FLOAT64_DICT = 10


cdef inline const uint8_t* _read_u32(const uint8_t* p, uint32_t* out) noexcept nogil:
    out[0] = ((<uint32_t>p[0])       |
              (<uint32_t>p[1] <<  8)  |
              (<uint32_t>p[2] << 16)  |
              (<uint32_t>p[3] << 24))
    return p + 4


cdef inline uint8_t* _copy_null_bitmap(const uint8_t* src, uint32_t nbytes) except NULL:
    """Allocate and memcpy the null bitmap. Returns NULL on zero length."""
    if nbytes == 0:
        return NULL
    cdef uint8_t* dst = <uint8_t*>malloc(nbytes)
    if dst == NULL:
        raise MemoryError()
    memcpy(dst, src, nbytes)
    return dst


cdef object _build_int64(const uint8_t* p, uint32_t num_rows,
                          const uint8_t* null_bitmap, uint32_t null_bitmap_len):
    cdef uint32_t data_len
    p = _read_u32(p, &data_len)
    cdef uint32_t n = data_len >> 3  # / sizeof(int64_t)

    cdef Int64Vector vec = Int64Vector(<size_t>n)
    if data_len > 0:
        memcpy(vec.ptr.data, p, data_len)
    vec.ptr.null_bitmap = _copy_null_bitmap(null_bitmap, null_bitmap_len)
    return vec


cdef object _build_int32(const uint8_t* p, uint32_t num_rows,
                          const uint8_t* null_bitmap, uint32_t null_bitmap_len):
    """Widen int32 → int64 at IPC decode time."""
    cdef uint32_t data_len
    p = _read_u32(p, &data_len)
    cdef uint32_t n = data_len >> 2  # / sizeof(int32_t)

    cdef Int64Vector vec = Int64Vector(<size_t>n)
    cdef int64_t* dst = <int64_t*>vec.ptr.data
    cdef const int32_t* src = <const int32_t*>p
    cdef uint32_t i
    for i in range(n):
        dst[i] = <int64_t>src[i]
    vec.ptr.null_bitmap = _copy_null_bitmap(null_bitmap, null_bitmap_len)
    return vec


cdef object _build_float32(const uint8_t* p, uint32_t num_rows,
                            const uint8_t* null_bitmap, uint32_t null_bitmap_len):
    cdef uint32_t data_len
    p = _read_u32(p, &data_len)
    cdef uint32_t n = data_len >> 2  # / sizeof(float)

    cdef Float32Vector vec = Float32Vector(<size_t>n)
    if data_len > 0:
        memcpy(vec.ptr.data, p, data_len)
    vec.ptr.null_bitmap = _copy_null_bitmap(null_bitmap, null_bitmap_len)
    return vec


cdef object _build_float64(const uint8_t* p, uint32_t num_rows,
                            const uint8_t* null_bitmap, uint32_t null_bitmap_len):
    cdef uint32_t data_len
    p = _read_u32(p, &data_len)
    cdef uint32_t n = data_len >> 3  # / sizeof(double)

    cdef Float64Vector vec = Float64Vector(<size_t>n)
    if data_len > 0:
        memcpy(vec.ptr.data, p, data_len)
    vec.ptr.null_bitmap = _copy_null_bitmap(null_bitmap, null_bitmap_len)
    return vec


cdef object _build_bool(const uint8_t* p, uint32_t num_rows,
                         const uint8_t* null_bitmap, uint32_t null_bitmap_len):
    cdef uint32_t data_len
    p = _read_u32(p, &data_len)

    cdef BoolVector vec = BoolVector(<size_t>data_len)
    if data_len > 0:
        memcpy(vec.ptr.data, p, data_len)
    vec.ptr.null_bitmap = _copy_null_bitmap(null_bitmap, null_bitmap_len)
    return vec


cdef object _build_numeric_dict_int64(const uint8_t* p, uint32_t num_rows,
                                       const uint8_t* null_bitmap, uint32_t null_bitmap_len):
    cdef uint32_t dict_size
    p = _read_u32(p, &dict_size)
    cdef uint8_t code_width = p[0]
    p += 1
    cdef uint32_t codes_len
    p = _read_u32(p, &codes_len)
    cdef const uint8_t* codes_ptr = p
    p += codes_len
    cdef uint32_t values_len
    p = _read_u32(p, &values_len)
    cdef const int64_t* dict_ptr = <const int64_t*>p
    return make_int64_dict_only(
        codes_ptr, code_width, <Py_ssize_t>num_rows,
        dict_ptr, <Py_ssize_t>dict_size,
        null_bitmap if null_bitmap_len > 0 else NULL,
    )


cdef object _build_numeric_dict_float32(const uint8_t* p, uint32_t num_rows,
                                         const uint8_t* null_bitmap, uint32_t null_bitmap_len):
    cdef uint32_t dict_size
    p = _read_u32(p, &dict_size)
    cdef uint8_t code_width = p[0]
    p += 1
    cdef uint32_t codes_len
    p = _read_u32(p, &codes_len)
    cdef const uint8_t* codes_ptr = p
    p += codes_len
    cdef uint32_t values_len
    p = _read_u32(p, &values_len)
    cdef const float* dict_ptr = <const float*>p
    return float32_from_packed_dict(
        codes_ptr, code_width, <Py_ssize_t>num_rows,
        dict_ptr, <Py_ssize_t>dict_size,
        null_bitmap if null_bitmap_len > 0 else NULL,
    )


cdef object _build_numeric_dict_float64(const uint8_t* p, uint32_t num_rows,
                                         const uint8_t* null_bitmap, uint32_t null_bitmap_len):
    cdef uint32_t dict_size
    p = _read_u32(p, &dict_size)
    cdef uint8_t code_width = p[0]
    p += 1
    cdef uint32_t codes_len
    p = _read_u32(p, &codes_len)
    cdef const uint8_t* codes_ptr = p
    p += codes_len
    cdef uint32_t values_len
    p = _read_u32(p, &values_len)
    cdef const double* dict_ptr = <const double*>p
    return make_float64_dict_only(
        codes_ptr, code_width, <Py_ssize_t>num_rows,
        dict_ptr, <Py_ssize_t>dict_size,
        null_bitmap if null_bitmap_len > 0 else NULL,
    )


cdef object _build_string_dict(const uint8_t* p, uint32_t num_rows,
                                const uint8_t* null_bitmap, uint32_t null_bitmap_len):
    cdef uint32_t dict_size
    p = _read_u32(p, &dict_size)

    cdef uint8_t code_width = p[0]
    p += 1

    cdef uint32_t codes_len
    p = _read_u32(p, &codes_len)
    cdef const uint8_t* codes_ptr = p
    p += codes_len

    cdef uint32_t offsets_count
    p = _read_u32(p, &offsets_count)
    cdef const int32_t* offsets_ptr = <const int32_t*>p
    p += offsets_count * 4

    cdef int32_t arena_len = offsets_ptr[dict_size]  # sentinel value

    return make_string_dict_only(
        codes_ptr,
        code_width,
        <Py_ssize_t>num_rows,
        <const uint32_t*>offsets_ptr,
        p,                    # arena_ptr
        <Py_ssize_t>dict_size,
        <Py_ssize_t>arena_len,
        null_bitmap if null_bitmap_len > 0 else NULL,
    )


cdef object _build_string_plain(const uint8_t* p, uint32_t num_rows,
                                 const uint8_t* null_bitmap, uint32_t null_bitmap_len):
    """Build StringVector from plain length-prefixed string list via from_dict_buffers."""
    cdef uint32_t n
    p = _read_u32(p, &n)

    if n == 0:
        return StringVector(0, 0)

    # Two-pass: first compute arena size, then build
    cdef const uint8_t* scan = p
    cdef uint32_t slen, total_arena = 0, i
    for i in range(n):
        scan = _read_u32(scan, &slen)
        total_arena += slen
        scan += slen

    cdef int32_t* offsets = <int32_t*>malloc((n + 1) * sizeof(int32_t))
    cdef int32_t* lengths = <int32_t*>malloc(n * sizeof(int32_t))
    cdef uint8_t* arena   = <uint8_t*>malloc(total_arena + 1)  # +1 so ptr is never NULL
    cdef int32_t* codes   = <int32_t*>malloc(n * sizeof(int32_t))
    if offsets == NULL or lengths == NULL or arena == NULL or codes == NULL:
        free(offsets); free(lengths); free(arena); free(codes)
        raise MemoryError()

    cdef uint32_t arena_pos = 0
    for i in range(n):
        p = _read_u32(p, &slen)
        offsets[i] = <int32_t>arena_pos
        lengths[i] = <int32_t>slen
        if slen > 0:
            memcpy(arena + arena_pos, p, slen)
        p += slen
        arena_pos += slen
        codes[i] = <int32_t>i
    offsets[n] = <int32_t>arena_pos

    cdef int32_t[::1] codes_v   = <int32_t[:n]>codes
    cdef int32_t[::1] offsets_v = <int32_t[:n]>offsets
    cdef int32_t[::1] lengths_v = <int32_t[:n]>lengths
    cdef uint32_t arena_view_len = arena_pos if arena_pos > 0 else 1
    cdef uint8_t[::1] arena_v   = <uint8_t[:arena_view_len]>arena

    cdef StringVector vec
    try:
        if null_bitmap_len > 0:
            validity_v = <uint8_t[:null_bitmap_len]>null_bitmap
            vec = from_dict_buffers(codes_v, offsets_v, lengths_v, arena_v, validity_v)
        else:
            vec = from_dict_buffers(codes_v, offsets_v, lengths_v, arena_v)
    finally:
        free(offsets)
        free(lengths)
        free(arena)
        free(codes)
    return vec


cpdef object deserialize_column(int64_t ref_id, MemoryPool pool):
    """Deserialize one IPC blob from MemoryPool into a Draken vector."""
    cdef bytes raw = pool.read(ref_id, False, False)
    if not raw:
        raise ValueError(f"Failed to read ref_id {ref_id} from MemoryPool")

    cdef const uint8_t* p = <const uint8_t*><char*>raw

    cdef uint8_t tag = p[0]
    p += 1

    cdef uint32_t num_rows
    p = _read_u32(p, &num_rows)

    cdef uint32_t null_bitmap_len
    p = _read_u32(p, &null_bitmap_len)
    cdef const uint8_t* null_bitmap = p
    p += null_bitmap_len

    if tag == TAG_INT64:
        return _build_int64(p, num_rows, null_bitmap, null_bitmap_len)
    elif tag == TAG_INT32:
        return _build_int32(p, num_rows, null_bitmap, null_bitmap_len)
    elif tag == TAG_FLOAT32:
        return _build_float32(p, num_rows, null_bitmap, null_bitmap_len)
    elif tag == TAG_FLOAT64:
        return _build_float64(p, num_rows, null_bitmap, null_bitmap_len)
    elif tag == TAG_BOOL:
        return _build_bool(p, num_rows, null_bitmap, null_bitmap_len)
    elif tag == TAG_STR_DICT:
        return _build_string_dict(p, num_rows, null_bitmap, null_bitmap_len)
    elif tag == TAG_STR_PLAIN:
        return _build_string_plain(p, num_rows, null_bitmap, null_bitmap_len)
    elif tag == TAG_INT64_DICT:
        return _build_numeric_dict_int64(p, num_rows, null_bitmap, null_bitmap_len)
    elif tag == TAG_FLOAT32_DICT:
        return _build_numeric_dict_float32(p, num_rows, null_bitmap, null_bitmap_len)
    elif tag == TAG_FLOAT64_DICT:
        return _build_numeric_dict_float64(p, num_rows, null_bitmap, null_bitmap_len)
    else:
        raise ValueError(f"Unknown IPC type tag: {tag}")


cpdef dict deserialize_row_group(dict ref_ids, MemoryPool pool):
    """Deserialize all columns for a row group from MemoryPool into Draken vectors."""
    cdef dict row_group = {}
    cdef int64_t ref_id
    for col_name, ref_id in ref_ids.items():
        row_group[col_name] = deserialize_column(ref_id, pool)
        pool.release(ref_id)
    return row_group
