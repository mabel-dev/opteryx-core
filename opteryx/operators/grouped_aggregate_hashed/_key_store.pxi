# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

# KeyStore — per-group key storage and reconstruction.
#
# store_new_rows()        — hot path; no Python/isinstance in inner loop.
#                           Single-column and multi-column paths append keys
#                           directly into final-form Draken buffers.
#
# reconstruct_vectors()   — finalize path; wraps owned Draken buffers directly.
#                           No legacy codec storage or decode path remains.

from libc.string cimport memset, memcpy
from libc.stdint cimport int32_t, int64_t, uint8_t, uint16_t, uint32_t
from libc.stdlib cimport realloc, free

from libcpp.vector cimport vector

from draken.core.buffers cimport DrakenFixedBuffer, DrakenVarBuffer, DrakenType, DrakenVector
from draken.core.buffers cimport DRAKEN_INT64
from draken.core.buffers cimport DRAKEN_STRING
from draken.core.fixed_vector cimport alloc_fixed_buffer, free_fixed_buffer
from draken.core.var_vector cimport alloc_var_buffer, free_var_buffer
from draken.vectors.vector cimport Vector
from draken.vectors.int64_vector cimport Int64Vector, _materialize_dict_int64, _refresh_unified_int64
from draken.vectors.float64_vector cimport Float64Vector, _materialize_dict_float64
from draken.vectors.string_vector cimport StringVector, _materialize_dict_string, _refresh_unified_string
from draken.core.buffers cimport DrakenConstantStringPayload
from draken.vectors.bool_vector cimport BoolVector


# ---------------------------------------------------------------------------
# Key kind constants
# ---------------------------------------------------------------------------
cdef int KEY_MULTI_FIXED_INT = 1
cdef int KEY_MULTI_FIXED_DATE32 = 2
cdef int KEY_MULTI_FIXED_TIME32 = 3
cdef int KEY_MULTI_FIXED_TIME64 = 4
cdef int KEY_MULTI_FIXED_TIMESTAMP64 = 5
cdef int KEY_MULTI_ENCODED_STRING = 6

# ---------------------------------------------------------------------------
# Dispatch codes for multi-column store_new_rows (replaces isinstance in loop)
# ---------------------------------------------------------------------------
cdef int _DISPATCH_INT64        = 0
cdef int _DISPATCH_BOOL         = 1
cdef int _DISPATCH_FLOAT64      = 2
cdef int _DISPATCH_STRING       = 3
cdef int _DISPATCH_DICT_STRING  = 4
cdef int _DISPATCH_DICT_INT64   = 5
cdef int _DISPATCH_DICT_FLOAT64 = 6
cdef int _DISPATCH_CONST_STRING = 7




# ---------------------------------------------------------------------------
# Packed-code reader (re-declared here for .pxi locality; identical to the
# one in int64_vector.pyx / string_vector.pyx, which are not exported via .pxd)
# ---------------------------------------------------------------------------
cdef inline uint32_t _ks_read_packed_code(const uint8_t* codes, uint8_t code_width, Py_ssize_t row_idx) noexcept nogil:
    if code_width == 1:
        return (<const uint8_t*>codes)[row_idx]
    if code_width == 2:
        return (<const uint16_t*>codes)[row_idx]
    return (<const uint32_t*>codes)[row_idx]


# ---------------------------------------------------------------------------
# Null-bitmap helper (re-declared here for .pxi locality)
# ---------------------------------------------------------------------------
cdef inline bint _ks_bitmap_is_valid(uint8_t* bitmap, Py_ssize_t index) noexcept nogil:
    if bitmap == NULL:
        return True
    return ((bitmap[index >> 3] >> (index & 7)) & 1) != 0


# ---------------------------------------------------------------------------
# Buffer wrapping helpers
# ---------------------------------------------------------------------------

cdef StringVector _wrap_string_buffer(DrakenVarBuffer* buf) except *:
    cdef StringVector vec = StringVector(0, 0, True)
    vec.ptr = buf
    vec.owns_data = True
    vec._dict_values = NULL
    vec._dict_codes = NULL
    vec._dict_code_width = 0
    vec._dict_ordered = 0
    vec._dict_accessor.codes = NULL
    vec._dict_accessor.code_width = 0
    vec._dict_accessor.row_nulls = NULL
    vec._dict_accessor.length = 0
    vec._dict_accessor.dict_values = NULL
    vec._dict_accessor.value_type = DRAKEN_STRING
    vec._const_accessor.length = 0
    vec._const_accessor.value_type = DRAKEN_STRING
    vec._const_accessor.value_ptr = NULL
    vec._const_accessor.is_null = 0
    vec._const_value = NULL
    vec._has_const = False
    vec._const_is_null = False
    _refresh_unified_string(vec)
    return vec


cdef inline Py_ssize_t _ks_bitmap_nbytes(Py_ssize_t length) noexcept nogil:
    return (length + 7) >> 3


cdef inline uint8_t* _ks_alloc_all_valid_bitmap(Py_ssize_t length) except NULL:
    cdef Py_ssize_t nbytes = _ks_bitmap_nbytes(length)
    cdef uint8_t* bitmap
    if nbytes == 0:
        return NULL
    bitmap = <uint8_t*>malloc(nbytes)
    if bitmap == NULL:
        raise MemoryError()
    memset(bitmap, 0xFF, nbytes)
    return bitmap


cdef inline void _ks_bitmap_clear(uint8_t* bitmap, Py_ssize_t index) noexcept nogil:
    bitmap[index >> 3] &= ~(1 << (index & 7))


cdef inline void _ks_bitmap_set(uint8_t* bitmap, Py_ssize_t index) noexcept nogil:
    bitmap[index >> 3] |= (1 << (index & 7))


cdef inline Py_ssize_t _ks_growth_target(Py_ssize_t current_size, Py_ssize_t required_size, Py_ssize_t minimum_size) noexcept nogil:
    cdef Py_ssize_t target = current_size * 2 if current_size > 0 else minimum_size
    if target < required_size:
        target = required_size
    return target


cdef inline void _ks_ensure_string_capacity(
    DrakenVarBuffer* buf,
    Py_ssize_t current_rows,
    Py_ssize_t current_bytes,
    Py_ssize_t required_rows,
    Py_ssize_t required_bytes,
    Py_ssize_t* bytes_capacity_ref = NULL,
) except *:
    cdef int32_t* new_offsets
    cdef uint8_t* new_data
    cdef Py_ssize_t new_rows_cap
    cdef Py_ssize_t new_bytes_cap

    if required_rows > current_rows:
        new_rows_cap = _ks_growth_target(current_rows, required_rows, 8)
        new_offsets = <int32_t*>realloc(buf.offsets, (new_rows_cap + 1) * sizeof(int32_t))
        if new_offsets == NULL:
            raise MemoryError()
        buf.offsets = new_offsets
        buf.length = <size_t>new_rows_cap

    if required_bytes > current_bytes:
        new_bytes_cap = _ks_growth_target(current_bytes, required_bytes, 64)
        new_data = <uint8_t*>realloc(buf.data, new_bytes_cap)
        if new_data == NULL:
            raise MemoryError()
        buf.data = new_data
        if bytes_capacity_ref != NULL:
            bytes_capacity_ref[0] = new_bytes_cap
    elif bytes_capacity_ref != NULL and bytes_capacity_ref[0] < current_bytes:
        bytes_capacity_ref[0] = current_bytes


cdef inline void _ks_ensure_fixed_capacity(
    DrakenFixedBuffer* buf,
    Py_ssize_t current_rows,
    Py_ssize_t required_rows,
) except *:
    cdef void* new_data
    cdef Py_ssize_t new_rows_cap
    cdef Py_ssize_t old_bytes
    cdef Py_ssize_t new_bytes

    if required_rows <= current_rows:
        return

    new_rows_cap = _ks_growth_target(current_rows, required_rows, 8)

    old_bytes = current_rows * <Py_ssize_t>buf.itemsize
    new_bytes = new_rows_cap * <Py_ssize_t>buf.itemsize

    new_data = realloc(buf.data, new_bytes)
    if new_data == NULL:
        raise MemoryError()
    buf.data = new_data
    if new_bytes > old_bytes:
        memset(<uint8_t*>buf.data + old_bytes, 0, new_bytes - old_bytes)
    buf.length = <size_t>new_rows_cap


cdef inline void _ks_ensure_bitmap_capacity(
    uint8_t** bitmap_ref,
    Py_ssize_t current_rows,
    Py_ssize_t required_rows,
) except *:
    cdef Py_ssize_t current_bytes = _ks_bitmap_nbytes(current_rows)
    cdef Py_ssize_t required_bytes = _ks_bitmap_nbytes(required_rows)
    cdef Py_ssize_t new_rows_cap
    cdef Py_ssize_t new_bytes_cap
    cdef uint8_t* grown_bitmap

    # Cold first-allocation: we may have skipped allocating a bitmap while
    # every prior row was valid.  As soon as we need one — even at a row
    # count that fits in the same byte — allocate with prior rows marked
    # valid.  Without this the caller would dereference a NULL bitmap.
    if bitmap_ref[0] == NULL:
        new_rows_cap = _ks_growth_target(current_rows, required_rows, 8)
        bitmap_ref[0] = _ks_alloc_all_valid_bitmap(new_rows_cap)
        return

    if required_bytes <= current_bytes:
        return

    new_rows_cap = _ks_growth_target(current_rows, required_rows, 8)
    new_bytes_cap = _ks_bitmap_nbytes(new_rows_cap)

    grown_bitmap = <uint8_t*>realloc(bitmap_ref[0], new_bytes_cap)
    if grown_bitmap == NULL:
        raise MemoryError()
    memset(grown_bitmap + current_bytes, 0xFF, new_bytes_cap - current_bytes)
    bitmap_ref[0] = grown_bitmap


cdef inline void _ks_reserve_single_string_direct(
    DrakenVarBuffer* buf,
    uint8_t** null_bitmap_ref,
    Py_ssize_t current_rows,
    Py_ssize_t current_bytes,
    Py_ssize_t additional_rows,
    Py_ssize_t additional_bytes,
    bint needs_null_bitmap,
) except *:
    cdef Py_ssize_t required_rows = current_rows + additional_rows
    cdef Py_ssize_t required_bytes = current_bytes + additional_bytes

    _ks_ensure_string_capacity(
        buf,
        current_rows,
        current_bytes,
        required_rows,
        required_bytes,
        NULL,
    )
    if needs_null_bitmap:
        _ks_ensure_bitmap_capacity(null_bitmap_ref, current_rows, required_rows)


cdef inline void _ks_reserve_fixed_direct(
    DrakenFixedBuffer* buf,
    uint8_t** null_bitmap_ref,
    Py_ssize_t current_rows,
    Py_ssize_t additional_rows,
    bint needs_null_bitmap,
) except *:
    cdef Py_ssize_t required_rows = current_rows + additional_rows

    _ks_ensure_fixed_capacity(buf, current_rows, required_rows)
    if needs_null_bitmap:
        _ks_ensure_bitmap_capacity(null_bitmap_ref, current_rows, required_rows)


cdef inline void _ks_append_single_string_direct(
    DrakenVarBuffer* buf,
    uint8_t** null_bitmap_ref,
    Py_ssize_t* row_count_ref,
    Py_ssize_t* bytes_used_ref,
    const char* str_ptr,
    Py_ssize_t str_len,
    int64_t valid_flag,
) except *:
    cdef Py_ssize_t row_idx = row_count_ref[0]
    cdef Py_ssize_t next_bytes = bytes_used_ref[0] + (str_len if valid_flag else 0)

    _ks_ensure_string_capacity(
        buf,
        row_count_ref[0],
        bytes_used_ref[0],
        row_idx + 1,
        next_bytes,
        NULL,
    )

    if row_idx == 0:
        buf.offsets[0] = 0

    if valid_flag:
        if str_len > 0:
            memcpy(buf.data + bytes_used_ref[0], str_ptr, str_len)
        bytes_used_ref[0] = next_bytes
        if null_bitmap_ref[0] != NULL:
            _ks_bitmap_set(null_bitmap_ref[0], row_idx)
    else:
        _ks_ensure_bitmap_capacity(null_bitmap_ref, row_idx, row_idx + 1)
        _ks_bitmap_clear(null_bitmap_ref[0], row_idx)

    buf.offsets[row_idx + 1] = <int32_t>bytes_used_ref[0]
    row_count_ref[0] = row_idx + 1
    buf.null_bitmap = null_bitmap_ref[0]


cdef inline void _ks_append_fixed_direct(
    DrakenFixedBuffer* buf,
    uint8_t** null_bitmap_ref,
    Py_ssize_t* row_count_ref,
    int64_t value,
    int64_t valid_flag,
) except *:
    cdef Py_ssize_t row_idx = row_count_ref[0]
    cdef int64_t* data

    _ks_ensure_fixed_capacity(buf, row_count_ref[0], row_idx + 1)
    data = <int64_t*>buf.data
    data[row_idx] = value

    if not valid_flag:
        _ks_ensure_bitmap_capacity(null_bitmap_ref, row_idx, row_idx + 1)
        _ks_bitmap_clear(null_bitmap_ref[0], row_idx)
    elif null_bitmap_ref[0] != NULL:
        _ks_bitmap_set(null_bitmap_ref[0], row_idx)

    row_count_ref[0] = row_idx + 1
    buf.null_bitmap = null_bitmap_ref[0]


cdef inline void _ks_store_single_fixed_bulk_int64(
    DrakenFixedBuffer* buf,
    uint8_t** null_bitmap_ref,
    Py_ssize_t* row_count_ref,
    const int64_t* row_indices,
    Py_ssize_t n_new,
    uint8_t* src_nulls,
    int64_t* src_data,
    bint has_const,
    int64_t const_value,
) except *:
    cdef Py_ssize_t start_row = row_count_ref[0]
    cdef Py_ssize_t ri
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t out_row
    cdef bint needs_null_bitmap = False
    cdef int64_t* dst

    for ri in range(n_new):
        row_idx = row_indices[ri]
        if not _ks_bitmap_is_valid(src_nulls, row_idx):
            needs_null_bitmap = True
            break

    _ks_reserve_fixed_direct(
        buf,
        null_bitmap_ref,
        start_row,
        n_new,
        needs_null_bitmap,
    )

    dst = <int64_t*>buf.data
    for ri in range(n_new):
        row_idx = row_indices[ri]
        out_row = start_row + ri
        if _ks_bitmap_is_valid(src_nulls, row_idx):
            if has_const:
                dst[out_row] = const_value
            else:
                dst[out_row] = src_data[row_idx]
            if null_bitmap_ref[0] != NULL:
                _ks_bitmap_set(null_bitmap_ref[0], out_row)
        else:
            dst[out_row] = 0
            if null_bitmap_ref[0] != NULL:
                _ks_bitmap_clear(null_bitmap_ref[0], out_row)

    row_count_ref[0] = start_row + n_new
    buf.null_bitmap = null_bitmap_ref[0]


cdef inline void _ks_store_single_fixed_bulk_bool(
    DrakenFixedBuffer* buf,
    uint8_t** null_bitmap_ref,
    Py_ssize_t* row_count_ref,
    const int64_t* row_indices,
    Py_ssize_t n_new,
    uint8_t* src_nulls,
    uint8_t* src_data,
    bint has_const,
    uint8_t const_value,
) except *:
    cdef Py_ssize_t start_row = row_count_ref[0]
    cdef Py_ssize_t ri
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t out_row
    cdef bint needs_null_bitmap = False
    cdef int64_t* dst

    for ri in range(n_new):
        row_idx = row_indices[ri]
        if not _ks_bitmap_is_valid(src_nulls, row_idx):
            needs_null_bitmap = True
            break

    _ks_reserve_fixed_direct(
        buf,
        null_bitmap_ref,
        start_row,
        n_new,
        needs_null_bitmap,
    )

    dst = <int64_t*>buf.data
    for ri in range(n_new):
        row_idx = row_indices[ri]
        out_row = start_row + ri
        if _ks_bitmap_is_valid(src_nulls, row_idx):
            if has_const:
                dst[out_row] = <int64_t>const_value
            else:
                dst[out_row] = <int64_t>((src_data[row_idx >> 3] >> (row_idx & 7)) & 1)
            if null_bitmap_ref[0] != NULL:
                _ks_bitmap_set(null_bitmap_ref[0], out_row)
        else:
            dst[out_row] = 0
            if null_bitmap_ref[0] != NULL:
                _ks_bitmap_clear(null_bitmap_ref[0], out_row)

    row_count_ref[0] = start_row + n_new
    buf.null_bitmap = null_bitmap_ref[0]


cdef inline void _ks_store_multi_fixed_bulk(
    DrakenFixedBuffer* buf,
    uint8_t** null_bitmap_ref,
    Py_ssize_t* row_count_ref,
    const int64_t* row_indices,
    Py_ssize_t n_new,
    int src_disp,
    uint8_t* src_nulls,
    size_t src_dense_ptr,
    bint src_has_const,
    int64_t src_const_value,
) except *:
    cdef Py_ssize_t start_row = row_count_ref[0]
    cdef Py_ssize_t ri
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t out_row
    cdef int64_t* dst
    cdef int64_t* src_i64 = <int64_t*>src_dense_ptr
    cdef uint8_t* src_bool = <uint8_t*>src_dense_ptr

    _ks_ensure_fixed_capacity(buf, start_row, start_row + n_new)

    dst = <int64_t*>buf.data
    for ri in range(n_new):
        row_idx = row_indices[ri]
        out_row = start_row + ri

        if _ks_bitmap_is_valid(src_nulls, row_idx):
            if src_has_const:
                dst[out_row] = src_const_value
            elif src_disp == _DISPATCH_BOOL:
                dst[out_row] = <int64_t>((src_bool[row_idx >> 3] >> (row_idx & 7)) & 1)
            else:
                dst[out_row] = src_i64[row_idx]
            if null_bitmap_ref[0] != NULL:
                _ks_bitmap_set(null_bitmap_ref[0], out_row)
        else:
            dst[out_row] = 0
            if null_bitmap_ref[0] == NULL:
                _ks_ensure_bitmap_capacity(null_bitmap_ref, start_row, start_row + n_new)
            _ks_bitmap_clear(null_bitmap_ref[0], out_row)

    row_count_ref[0] = start_row + n_new
    buf.null_bitmap = null_bitmap_ref[0]


cdef inline void _ks_store_multi_string_bulk(
    DrakenVarBuffer* buf,
    uint8_t** null_bitmap_ref,
    Py_ssize_t* row_count_ref,
    Py_ssize_t* bytes_used_ref,
    const int64_t* row_indices,
    Py_ssize_t n_new,
    uint8_t* src_nulls,
    DrakenVarBuffer* src_vbuf,
) except *:
    cdef Py_ssize_t start_row = row_count_ref[0]
    cdef Py_ssize_t bytes_used = bytes_used_ref[0]
    cdef Py_ssize_t ri
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t out_row
    cdef Py_ssize_t str_len
    cdef Py_ssize_t required_rows = start_row + n_new
    cdef Py_ssize_t current_row_capacity = <Py_ssize_t>buf.length
    cdef Py_ssize_t current_byte_capacity = 0

    if bytes_used > 0:
        current_byte_capacity = bytes_used

    _ks_ensure_string_capacity(
        buf,
        current_row_capacity,
        current_byte_capacity,
        required_rows,
        bytes_used + max(n_new * 16, 64),
        &current_byte_capacity,
    )
    current_row_capacity = <Py_ssize_t>buf.length

    if start_row == 0:
        buf.offsets[0] = 0

    for ri in range(n_new):
        row_idx = row_indices[ri]
        out_row = start_row + ri

        if _ks_bitmap_is_valid(src_nulls, row_idx) and src_vbuf != NULL:
            str_len = src_vbuf.offsets[row_idx + 1] - src_vbuf.offsets[row_idx]
            if str_len > 0:
                if bytes_used + str_len > current_byte_capacity:
                    _ks_ensure_string_capacity(
                        buf,
                        current_row_capacity,
                        current_byte_capacity,
                        required_rows,
                        bytes_used + str_len,
                        &current_byte_capacity,
                    )
                    current_row_capacity = <Py_ssize_t>buf.length
                memcpy(buf.data + bytes_used, src_vbuf.data + src_vbuf.offsets[row_idx], str_len)
            bytes_used += str_len
            if null_bitmap_ref[0] != NULL:
                _ks_bitmap_set(null_bitmap_ref[0], out_row)
        else:
            if null_bitmap_ref[0] == NULL:
                _ks_ensure_bitmap_capacity(null_bitmap_ref, start_row, required_rows)
            _ks_bitmap_clear(null_bitmap_ref[0], out_row)

        buf.offsets[out_row + 1] = <int32_t>bytes_used

    row_count_ref[0] = required_rows
    bytes_used_ref[0] = bytes_used
    buf.null_bitmap = null_bitmap_ref[0]


cdef inline void _ks_store_multi_const_string_bulk(
    DrakenVarBuffer* buf,
    uint8_t** null_bitmap_ref,
    Py_ssize_t* row_count_ref,
    Py_ssize_t* bytes_used_ref,
    Py_ssize_t n_new,
    bint is_null,
    const uint8_t* const_data,
    int32_t const_len,
) except *:
    """Store n_new copies of a constant string (or null) into the key buffer."""
    cdef Py_ssize_t start_row = row_count_ref[0]
    cdef Py_ssize_t bytes_used = bytes_used_ref[0]
    cdef Py_ssize_t ri
    cdef Py_ssize_t out_row
    cdef Py_ssize_t required_rows = start_row + n_new
    cdef Py_ssize_t current_row_capacity = <Py_ssize_t>buf.length
    cdef Py_ssize_t current_byte_capacity = bytes_used if bytes_used > 0 else 0
    cdef Py_ssize_t total_bytes = bytes_used + (0 if is_null else <Py_ssize_t>const_len * n_new)

    _ks_ensure_string_capacity(
        buf,
        current_row_capacity,
        current_byte_capacity,
        required_rows,
        total_bytes + 64,
        &current_byte_capacity,
    )

    if start_row == 0:
        buf.offsets[0] = 0

    if is_null:
        _ks_ensure_bitmap_capacity(null_bitmap_ref, start_row, required_rows)
        for ri in range(n_new):
            out_row = start_row + ri
            _ks_bitmap_clear(null_bitmap_ref[0], out_row)
            buf.offsets[out_row + 1] = <int32_t>bytes_used
    else:
        for ri in range(n_new):
            out_row = start_row + ri
            if const_len > 0 and const_data != NULL:
                memcpy(buf.data + bytes_used, const_data, <size_t>const_len)
                bytes_used += const_len
            buf.offsets[out_row + 1] = <int32_t>bytes_used
            if null_bitmap_ref[0] != NULL:
                _ks_bitmap_set(null_bitmap_ref[0], out_row)

    row_count_ref[0] = required_rows
    bytes_used_ref[0] = bytes_used
    buf.null_bitmap = null_bitmap_ref[0]


# ---------------------------------------------------------------------------
# Dict-encoded bulk helpers — read from the K-entry dict buffer via code index
# instead of expanding to an N-row dense buffer first.
# ---------------------------------------------------------------------------

cdef inline void _ks_store_fixed_bulk_dict(
    DrakenFixedBuffer* buf,
    uint8_t** null_bitmap_ref,
    Py_ssize_t* row_count_ref,
    const int64_t* row_indices,
    Py_ssize_t n_new,
    uint8_t* row_nulls,
    const int64_t* dict_data,
    const uint8_t* codes,
    uint8_t code_width,
) except *:
    """Store int64/float64 dict-encoded keys without materializing the full column.

    dict_data must point to the raw int64_t values of the dictionary buffer
    (cast from DrakenFixedBuffer.data).  For float64, caller passes the float
    bits as int64 — matching the dense path that does <int64_t*>fv.dense_ptr().
    """
    cdef Py_ssize_t start_row = row_count_ref[0]
    cdef Py_ssize_t ri
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t out_row
    cdef uint32_t code
    cdef int64_t* dst
    cdef bint needs_null_bitmap = False

    for ri in range(n_new):
        row_idx = row_indices[ri]
        if row_nulls != NULL and not ((row_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
            needs_null_bitmap = True
            break

    _ks_reserve_fixed_direct(buf, null_bitmap_ref, start_row, n_new, needs_null_bitmap)

    dst = <int64_t*>buf.data
    for ri in range(n_new):
        row_idx = row_indices[ri]
        out_row = start_row + ri
        if row_nulls != NULL and not ((row_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
            dst[out_row] = 0
            if null_bitmap_ref[0] != NULL:
                _ks_bitmap_clear(null_bitmap_ref[0], out_row)
        else:
            code = _ks_read_packed_code(codes, code_width, row_idx)
            dst[out_row] = dict_data[code]
            if null_bitmap_ref[0] != NULL:
                _ks_bitmap_set(null_bitmap_ref[0], out_row)

    row_count_ref[0] = start_row + n_new
    buf.null_bitmap = null_bitmap_ref[0]


cdef inline void _ks_store_string_bulk_dict(
    DrakenVarBuffer* buf,
    uint8_t** null_bitmap_ref,
    Py_ssize_t* row_count_ref,
    Py_ssize_t* bytes_used_ref,
    const int64_t* row_indices,
    Py_ssize_t n_new,
    uint8_t* row_nulls,
    DrakenVarBuffer* dict_vbuf,
    const uint8_t* codes,
    uint8_t code_width,
) except *:
    """Store string dict-encoded keys without materializing the full column."""
    cdef Py_ssize_t start_row = row_count_ref[0]
    cdef Py_ssize_t bytes_used = bytes_used_ref[0]
    cdef Py_ssize_t ri
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t out_row
    cdef Py_ssize_t str_len
    cdef Py_ssize_t required_rows = start_row + n_new
    cdef Py_ssize_t current_row_capacity = <Py_ssize_t>buf.length
    cdef Py_ssize_t current_byte_capacity = bytes_used if bytes_used > 0 else 0
    cdef uint32_t code

    _ks_ensure_string_capacity(
        buf,
        current_row_capacity,
        current_byte_capacity,
        required_rows,
        bytes_used + max(n_new * 16, 64),
        &current_byte_capacity,
    )
    current_row_capacity = <Py_ssize_t>buf.length

    if start_row == 0:
        buf.offsets[0] = 0

    for ri in range(n_new):
        row_idx = row_indices[ri]
        out_row = start_row + ri

        if row_nulls != NULL and not ((row_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
            if null_bitmap_ref[0] == NULL:
                _ks_ensure_bitmap_capacity(null_bitmap_ref, start_row, required_rows)
            _ks_bitmap_clear(null_bitmap_ref[0], out_row)
        else:
            code = _ks_read_packed_code(codes, code_width, row_idx)
            str_len = dict_vbuf.offsets[code + 1] - dict_vbuf.offsets[code]
            if str_len > 0:
                if bytes_used + str_len > current_byte_capacity:
                    _ks_ensure_string_capacity(
                        buf,
                        current_row_capacity,
                        current_byte_capacity,
                        required_rows,
                        bytes_used + str_len,
                        &current_byte_capacity,
                    )
                    current_row_capacity = <Py_ssize_t>buf.length
                memcpy(buf.data + bytes_used, dict_vbuf.data + dict_vbuf.offsets[code], str_len)
            bytes_used += str_len
            if null_bitmap_ref[0] != NULL:
                _ks_bitmap_set(null_bitmap_ref[0], out_row)

        buf.offsets[out_row + 1] = <int32_t>bytes_used

    row_count_ref[0] = required_rows
    bytes_used_ref[0] = bytes_used
    buf.null_bitmap = null_bitmap_ref[0]


# ---------------------------------------------------------------------------
# KeyStore
# ---------------------------------------------------------------------------

cdef class KeyStore:
    """
    Stores the group-key values for new groups in final-form Draken buffers.
    """

    cdef list _group_columns          # list[bytes|str] — read at init only
    cdef vector[int64_t] _key_kinds   # KEY_MULTI_FIXED_* or KEY_MULTI_ENCODED_STRING per column
    cdef Py_ssize_t _n_cols

    cdef DrakenFixedBuffer* _single_fixed_buf
    cdef uint8_t* _single_fixed_nulls
    cdef Py_ssize_t _single_fixed_rows
    cdef bint _single_fixed_direct

    cdef DrakenVarBuffer* _single_string_buf
    cdef uint8_t* _single_string_nulls
    cdef Py_ssize_t _single_string_rows
    cdef Py_ssize_t _single_string_bytes
    cdef bint _single_string_direct

    cdef vector[DrakenFixedBuffer*] _multi_fixed_bufs
    cdef vector[uint8_t*] _multi_fixed_nulls
    cdef vector[Py_ssize_t] _multi_fixed_rows
    cdef vector[DrakenVarBuffer*] _multi_string_bufs
    cdef vector[uint8_t*] _multi_string_nulls
    cdef vector[Py_ssize_t] _multi_string_rows
    cdef vector[Py_ssize_t] _multi_string_bytes
    cdef vector[int] _multi_storage_kind
    cdef vector[int] _multi_storage_slot
    cdef bint _multi_direct

    def __cinit__(self, list group_columns, list key_kinds):
        self._group_columns = group_columns
        self._n_cols = len(group_columns)
        self._single_fixed_buf = NULL
        self._single_fixed_nulls = NULL
        self._single_fixed_rows = 0
        self._single_fixed_direct = False
        self._single_string_buf = NULL
        self._single_string_nulls = NULL
        self._single_string_rows = 0
        self._single_string_bytes = 0
        self._single_string_direct = False
        self._multi_direct = False

        cdef Py_ssize_t i
        cdef int fixed_slot = 0
        cdef int string_slot = 0

        for i in range(len(key_kinds)):
            self._key_kinds.push_back(<int64_t>key_kinds[i])

        if self._n_cols == 1 and len(key_kinds) == 1:
            if key_kinds[0] == KEY_MULTI_ENCODED_STRING:
                self._single_string_buf = alloc_var_buffer(DRAKEN_STRING, 0, 0)
                self._single_string_buf.offsets[0] = 0
                self._single_string_direct = True
            else:
                self._single_fixed_buf = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
                self._single_fixed_direct = True
        elif self._n_cols > 1:
            self._multi_direct = True
            self._multi_storage_kind.resize(self._n_cols)
            self._multi_storage_slot.resize(self._n_cols)

            for i in range(self._n_cols):
                if key_kinds[i] == KEY_MULTI_ENCODED_STRING:
                    self._multi_storage_kind[i] = _DISPATCH_STRING
                    self._multi_storage_slot[i] = string_slot
                    self._multi_string_bufs.push_back(alloc_var_buffer(DRAKEN_STRING, 0, 0))
                    self._multi_string_bufs[string_slot].offsets[0] = 0
                    self._multi_string_nulls.push_back(NULL)
                    self._multi_string_rows.push_back(0)
                    self._multi_string_bytes.push_back(0)
                    string_slot += 1
                else:
                    self._multi_storage_kind[i] = _DISPATCH_INT64
                    self._multi_storage_slot[i] = fixed_slot
                    self._multi_fixed_bufs.push_back(alloc_fixed_buffer(DRAKEN_INT64, 0, 8))
                    self._multi_fixed_nulls.push_back(NULL)
                    self._multi_fixed_rows.push_back(0)
                    fixed_slot += 1

    def __dealloc__(self):
        cdef Py_ssize_t i

        if self._single_fixed_buf != NULL:
            self._single_fixed_buf.null_bitmap = self._single_fixed_nulls
            free_fixed_buffer(self._single_fixed_buf, True)
            self._single_fixed_buf = NULL
            self._single_fixed_nulls = NULL
            self._single_fixed_rows = 0

        if self._single_string_buf != NULL:
            self._single_string_buf.null_bitmap = self._single_string_nulls
            free_var_buffer(self._single_string_buf, True)
            self._single_string_buf = NULL
            self._single_string_nulls = NULL
            self._single_string_rows = 0
            self._single_string_bytes = 0

        for i in range(self._multi_fixed_bufs.size()):
            if self._multi_fixed_bufs[i] != NULL:
                self._multi_fixed_bufs[i].null_bitmap = self._multi_fixed_nulls[i]
                free_fixed_buffer(self._multi_fixed_bufs[i], True)
                self._multi_fixed_bufs[i] = NULL

        for i in range(self._multi_string_bufs.size()):
            if self._multi_string_bufs[i] != NULL:
                self._multi_string_bufs[i].null_bitmap = self._multi_string_nulls[i]
                free_var_buffer(self._multi_string_bufs[i], True)
                self._multi_string_bufs[i] = NULL

    # ------------------------------------------------------------------
    # store_new_rows — hot path, called once per morsel
    # ------------------------------------------------------------------

    cdef void store_new_rows(
        self,
        object morsel,            # Morsel
        const int64_t* row_indices,
        Py_ssize_t n_new,
    ) except *:
        """
        Encode group keys for new groups and append to the byte store.

        Single-column path: specialised per vector type; no isinstance in loop.
        Multi-column path: dispatch codes + raw C pointers pre-computed once per
        morsel; the inner per-row loop contains only integer comparisons.
        """
        if n_new == 0:
            return

        cdef Py_ssize_t col_idx, ri, row_idx
        cdef int64_t key_kind
        cdef list vecs
        cdef Vector vec
        cdef Int64Vector iv
        cdef Float64Vector fv
        cdef StringVector sv
        cdef BoolVector bv
        cdef int64_t* i64_data
        cdef uint8_t* bool_data
        cdef uint8_t* raw_bool_data
        cdef uint8_t* nulls
        cdef DrakenVarBuffer* vbuf
        cdef int64_t int_val
        cdef int64_t valid_flag
        cdef const char* str_ptr
        cdef Py_ssize_t str_len
        cdef int64_t const_i64
        cdef uint8_t const_bool
        cdef uint8_t* bool_nulls
        cdef const char* const_str_ptr
        cdef int32_t const_str_len

        # Single-column dict paths (avoids N-row materialization)
        cdef DrakenVarBuffer* sv_dv
        cdef const uint8_t* sv_dc
        cdef uint8_t sv_dcw
        cdef uint8_t* sv_rnulls
        cdef uint32_t sv_code
        cdef int64_t* iv_dict_data
        cdef const uint8_t* iv_dc
        cdef uint8_t iv_dcw
        cdef uint8_t* iv_rnulls
        cdef int64_t* fv_dict_data
        cdef const uint8_t* fv_dc
        cdef uint8_t fv_dcw
        cdef uint8_t* fv_rnulls

        # Multi-column pre-computed dispatch
        cdef vector[int]    col_dispatch
        cdef vector[size_t] col_null_ptrs
        cdef vector[size_t] col_dense_ptrs
        cdef vector[size_t] col_varbuf_ptrs
        cdef vector[bint]   col_has_const
        cdef vector[int64_t] col_const_vals
        cdef vector[size_t] col_dict_code_ptrs
        cdef vector[uint8_t] col_dict_code_widths
        cdef int disp
        cdef int storage_slot
        cdef DrakenFixedBuffer* fixed_buf
        cdef DrakenVarBuffer* string_buf
        cdef DrakenConstantStringPayload* cpl
        cdef DrakenConstantStringPayload* _csp
        cdef DrakenVector* uv
        cdef Py_ssize_t additional_bytes
        cdef bint needs_null_bitmap

        if self._n_cols == 1:
            # ----------------------------------------------------------------
            # Single-column fast paths — statically dispatched
            # ----------------------------------------------------------------
            key_kind = self._key_kinds[0]
            vec = morsel.column(self._group_columns[0])
            nulls = vec.null_bitmap_ptr()

            if key_kind == KEY_MULTI_ENCODED_STRING:
                sv = <StringVector>vec
                uv = sv.unified()
                if uv.selection != NULL:
                    # Dict path: read directly from dict values — no N-row materialization.
                    sv_dv     = <DrakenVarBuffer*>uv.data
                    sv_dc     = <const uint8_t*>uv.selection
                    sv_dcw    = uv.sel_width
                    sv_rnulls = uv.validity
                    if self._single_string_direct:
                        _ks_store_string_bulk_dict(
                            self._single_string_buf,
                            &self._single_string_nulls,
                            &self._single_string_rows,
                            &self._single_string_bytes,
                            row_indices,
                            n_new,
                            sv_rnulls,
                            sv_dv,
                            sv_dc,
                            sv_dcw,
                        )
                    else:
                        raise RuntimeError("single string codec path removed")
                elif uv.data_length == 1:
                    # Constant-encoded: same value for every row, no per-row pointer.
                    if self._single_string_direct:
                        if uv.validity != NULL:
                            const_str_ptr = NULL
                            const_str_len = 0
                        else:
                            _csp = <DrakenConstantStringPayload*>uv.data
                            const_str_ptr = <const char*>_csp.data
                            const_str_len = _csp.length
                        for ri in range(n_new):
                            _ks_append_single_string_direct(
                                self._single_string_buf,
                                &self._single_string_nulls,
                                &self._single_string_rows,
                                &self._single_string_bytes,
                                const_str_ptr if uv.validity == NULL else NULL,
                                const_str_len if uv.validity == NULL else 0,
                                0 if uv.validity != NULL else 1,
                            )
                    else:
                        raise RuntimeError("single string codec path removed")
                else:
                    vbuf  = sv.ptr
                    nulls = sv.null_bitmap_ptr()
                    if self._single_string_direct:
                        for ri in range(n_new):
                            row_idx = row_indices[ri]
                            valid_flag = 1 if _ks_bitmap_is_valid(nulls, row_idx) else 0
                            if valid_flag and vbuf != NULL:
                                str_ptr = <const char*>(vbuf.data + vbuf.offsets[row_idx])
                                str_len  = vbuf.offsets[row_idx + 1] - vbuf.offsets[row_idx]
                            else:
                                str_ptr = NULL
                                str_len  = 0
                            _ks_append_single_string_direct(
                                self._single_string_buf,
                                &self._single_string_nulls,
                                &self._single_string_rows,
                                &self._single_string_bytes,
                                str_ptr,
                                str_len,
                                valid_flag,
                            )
                    else:
                        raise RuntimeError("single string codec path removed")

            elif isinstance(vec, Int64Vector):
                iv = <Int64Vector>vec
                uv = iv.unified()
                if uv.selection != NULL:
                    # Dict path: read directly from dict values — no N-row materialization.
                    iv_dict_data = <int64_t*>uv.data
                    iv_dc        = <const uint8_t*>uv.selection
                    iv_dcw       = uv.sel_width
                    iv_rnulls    = uv.validity
                    if self._single_fixed_direct:
                        _ks_store_fixed_bulk_dict(
                            self._single_fixed_buf,
                            &self._single_fixed_nulls,
                            &self._single_fixed_rows,
                            row_indices,
                            n_new,
                            iv_rnulls,
                            iv_dict_data,
                            iv_dc,
                            iv_dcw,
                        )
                    else:
                        raise RuntimeError("single fixed key codec path removed")
                elif uv.data_length == 1:
                    const_i64 = (<int64_t*>uv.data)[0]
                    if self._single_fixed_direct:
                        _ks_store_single_fixed_bulk_int64(
                            self._single_fixed_buf,
                            &self._single_fixed_nulls,
                            &self._single_fixed_rows,
                            row_indices,
                            n_new,
                            nulls,
                            NULL,
                            True,
                            const_i64,
                        )
                    else:
                        raise RuntimeError("single fixed key codec path removed")
                else:
                    if self._single_fixed_direct:
                        i64_data = <int64_t*>iv.dense_ptr()
                        _ks_store_single_fixed_bulk_int64(
                            self._single_fixed_buf,
                            &self._single_fixed_nulls,
                            &self._single_fixed_rows,
                            row_indices,
                            n_new,
                            nulls,
                            i64_data,
                            False,
                            0,
                        )
                    else:
                        raise RuntimeError("single fixed key codec path removed")

            elif isinstance(vec, BoolVector):
                bv = <BoolVector>vec
                # BoolVector.null_bitmap_ptr() returns NULL (base impl); use ptr.null_bitmap
                bool_nulls = bv.ptr.null_bitmap
                if self._single_fixed_direct:
                    if bv.unified().data_length == 1:
                        const_bool = (<uint8_t*>bv.unified().data)[0]
                        _ks_store_single_fixed_bulk_bool(
                            self._single_fixed_buf,
                            &self._single_fixed_nulls,
                            &self._single_fixed_rows,
                            row_indices,
                            n_new,
                            bool_nulls,
                            NULL,
                            True,
                            const_bool,
                        )
                    else:
                        bool_data = <uint8_t*>bv.ptr.data
                        _ks_store_single_fixed_bulk_bool(
                            self._single_fixed_buf,
                            &self._single_fixed_nulls,
                            &self._single_fixed_rows,
                            row_indices,
                            n_new,
                            bool_nulls,
                            bool_data,
                            False,
                            0,
                        )
                else:
                    raise RuntimeError("single fixed key codec path removed")

            else:
                # Float64Vector and other fixed-width types — store as raw int64 bits
                fv = <Float64Vector>vec
                uv = fv.unified()
                if uv.selection != NULL:
                    # Dict path: float bits stored as int64 in the dict buffer.
                    fv_dict_data = <int64_t*>uv.data
                    fv_dc        = <const uint8_t*>uv.selection
                    fv_dcw       = uv.sel_width
                    fv_rnulls    = uv.validity
                    if self._single_fixed_direct:
                        _ks_store_fixed_bulk_dict(
                            self._single_fixed_buf,
                            &self._single_fixed_nulls,
                            &self._single_fixed_rows,
                            row_indices,
                            n_new,
                            fv_rnulls,
                            fv_dict_data,
                            fv_dc,
                            fv_dcw,
                        )
                    else:
                        raise RuntimeError("single fixed key codec path removed")
                elif uv.data_length == 1:
                    # Reinterpret double bits as int64 — fixed buffer slot is int64_t.
                    const_i64 = (<int64_t*>uv.data)[0]
                    if self._single_fixed_direct:
                        _ks_store_single_fixed_bulk_int64(
                            self._single_fixed_buf,
                            &self._single_fixed_nulls,
                            &self._single_fixed_rows,
                            row_indices,
                            n_new,
                            nulls,
                            NULL,
                            True,
                            const_i64,
                        )
                    else:
                        raise RuntimeError("single fixed key codec path removed")
                else:
                    i64_data = <int64_t*>fv.dense_ptr()
                    if self._single_fixed_direct:
                        _ks_store_single_fixed_bulk_int64(
                            self._single_fixed_buf,
                            &self._single_fixed_nulls,
                            &self._single_fixed_rows,
                            row_indices,
                            n_new,
                            nulls,
                            i64_data,
                            False,
                            0,
                        )
                    else:
                        raise RuntimeError("single fixed key codec path removed")

        else:
            # ----------------------------------------------------------------
            # Multi-column path
            # ----------------------------------------------------------------
            # Pre-fetch all column vectors once (Python call, outside inner loop).
            vecs = [morsel.column(self._group_columns[col_idx]) for col_idx in range(self._n_cols)]

            # Pre-compute per-column dispatch codes + raw C pointers.
            # This runs once per morsel and eliminates isinstance() from the
            # inner row loop, replacing it with cheap integer comparisons.
            col_dispatch.resize(self._n_cols)
            col_null_ptrs.resize(self._n_cols, 0)
            col_dense_ptrs.resize(self._n_cols, 0)
            col_varbuf_ptrs.resize(self._n_cols, 0)
            col_has_const.resize(self._n_cols, False)
            col_const_vals.resize(self._n_cols, 0)
            col_dict_code_ptrs.resize(self._n_cols, 0)
            col_dict_code_widths.resize(self._n_cols, 0)

            for col_idx in range(self._n_cols):
                key_kind = self._key_kinds[col_idx]
                vec = vecs[col_idx]

                if key_kind == KEY_MULTI_ENCODED_STRING:
                    sv = <StringVector>vec
                    uv = sv.unified()
                    if uv.selection != NULL:
                        # Dict path: capture dict pointers; no N-row materialization.
                        col_dispatch[col_idx]        = _DISPATCH_DICT_STRING
                        col_null_ptrs[col_idx]       = <size_t>uv.validity
                        col_varbuf_ptrs[col_idx]     = <size_t>uv.data
                        col_dict_code_ptrs[col_idx]  = <size_t>uv.selection
                        col_dict_code_widths[col_idx] = uv.sel_width
                    elif uv.data_length == 1:
                        # Constant-encoded: same string value for every row.
                        col_dispatch[col_idx]    = _DISPATCH_CONST_STRING
                        col_has_const[col_idx]   = uv.validity == NULL
                        col_varbuf_ptrs[col_idx] = <size_t>uv.data
                    else:
                        col_dispatch[col_idx]    = _DISPATCH_STRING
                        col_null_ptrs[col_idx]   = <size_t>sv.null_bitmap_ptr()
                        col_varbuf_ptrs[col_idx] = <size_t>sv.ptr

                elif isinstance(vec, Int64Vector):
                    iv = <Int64Vector>vec
                    uv = iv.unified()
                    if uv.selection != NULL:
                        # Dict path: capture dict pointers; no N-row materialization.
                        col_dispatch[col_idx]        = _DISPATCH_DICT_INT64
                        col_null_ptrs[col_idx]       = <size_t>uv.validity
                        col_dense_ptrs[col_idx]      = <size_t>uv.data
                        col_dict_code_ptrs[col_idx]  = <size_t>uv.selection
                        col_dict_code_widths[col_idx] = uv.sel_width
                    else:
                        col_dispatch[col_idx]  = _DISPATCH_INT64
                        col_null_ptrs[col_idx] = <size_t>iv.null_bitmap_ptr()
                        if uv.data_length == 1:
                            col_has_const[col_idx]  = True
                            col_const_vals[col_idx] = (<int64_t*>uv.data)[0]
                        else:
                            col_dense_ptrs[col_idx] = <size_t>iv.dense_ptr()

                elif isinstance(vec, BoolVector):
                    col_dispatch[col_idx] = _DISPATCH_BOOL
                    bv = <BoolVector>vec
                    # BoolVector: null bitmap lives at ptr.null_bitmap, not null_bitmap_ptr()
                    col_null_ptrs[col_idx] = <size_t>bv.ptr.null_bitmap
                    if bv.unified().data_length == 1:
                        col_has_const[col_idx] = True
                        col_const_vals[col_idx] = <int64_t>(<uint8_t*>bv.unified().data)[0]
                    else:
                        col_dense_ptrs[col_idx] = <size_t>bv.ptr.data

                else:
                    # Float64Vector and other fixed-width types
                    fv = <Float64Vector>vec
                    uv = fv.unified()
                    if uv.selection != NULL:
                        # Dict path: float bits stored as int64 — matches dense cast.
                        col_dispatch[col_idx]        = _DISPATCH_DICT_FLOAT64
                        col_null_ptrs[col_idx]       = <size_t>uv.validity
                        col_dense_ptrs[col_idx]      = <size_t>uv.data
                        col_dict_code_ptrs[col_idx]  = <size_t>uv.selection
                        col_dict_code_widths[col_idx] = uv.sel_width
                    else:
                        col_dispatch[col_idx]    = _DISPATCH_FLOAT64
                        col_null_ptrs[col_idx]   = <size_t>fv.null_bitmap_ptr()
                        if uv.data_length == 1:
                            # Reinterpret double bits as int64 — fixed buffer slot is int64_t.
                            col_has_const[col_idx]  = True
                            col_const_vals[col_idx] = (<int64_t*>uv.data)[0]
                        else:
                            col_dense_ptrs[col_idx]  = <size_t>fv.dense_ptr()

            if self._multi_direct:
                for col_idx in range(self._n_cols):
                    disp = col_dispatch[col_idx]
                    storage_slot = self._multi_storage_slot[col_idx]

                    if disp == _DISPATCH_STRING:
                        _ks_store_multi_string_bulk(
                            self._multi_string_bufs[storage_slot],
                            &self._multi_string_nulls[storage_slot],
                            &self._multi_string_rows[storage_slot],
                            &self._multi_string_bytes[storage_slot],
                            row_indices,
                            n_new,
                            <uint8_t*>col_null_ptrs[col_idx],
                            <DrakenVarBuffer*>col_varbuf_ptrs[col_idx],
                        )
                    elif disp == _DISPATCH_DICT_STRING:
                        _ks_store_string_bulk_dict(
                            self._multi_string_bufs[storage_slot],
                            &self._multi_string_nulls[storage_slot],
                            &self._multi_string_rows[storage_slot],
                            &self._multi_string_bytes[storage_slot],
                            row_indices,
                            n_new,
                            <uint8_t*>col_null_ptrs[col_idx],
                            <DrakenVarBuffer*>col_varbuf_ptrs[col_idx],
                            <const uint8_t*>col_dict_code_ptrs[col_idx],
                            col_dict_code_widths[col_idx],
                        )
                    elif disp == _DISPATCH_CONST_STRING:
                        cpl = <DrakenConstantStringPayload*>col_varbuf_ptrs[col_idx]
                        _ks_store_multi_const_string_bulk(
                            self._multi_string_bufs[storage_slot],
                            &self._multi_string_nulls[storage_slot],
                            &self._multi_string_rows[storage_slot],
                            &self._multi_string_bytes[storage_slot],
                            n_new,
                            not col_has_const[col_idx],
                            cpl.data if cpl != NULL else NULL,
                            cpl.length if cpl != NULL else 0,
                        )
                    elif disp == _DISPATCH_DICT_INT64 or disp == _DISPATCH_DICT_FLOAT64:
                        _ks_store_fixed_bulk_dict(
                            self._multi_fixed_bufs[storage_slot],
                            &self._multi_fixed_nulls[storage_slot],
                            &self._multi_fixed_rows[storage_slot],
                            row_indices,
                            n_new,
                            <uint8_t*>col_null_ptrs[col_idx],
                            <const int64_t*>col_dense_ptrs[col_idx],
                            <const uint8_t*>col_dict_code_ptrs[col_idx],
                            col_dict_code_widths[col_idx],
                        )
                    else:
                        _ks_store_multi_fixed_bulk(
                            self._multi_fixed_bufs[storage_slot],
                            &self._multi_fixed_nulls[storage_slot],
                            &self._multi_fixed_rows[storage_slot],
                            row_indices,
                            n_new,
                            disp,
                            <uint8_t*>col_null_ptrs[col_idx],
                            col_dense_ptrs[col_idx],
                            col_has_const[col_idx],
                            col_const_vals[col_idx],
                        )
            else:
                raise RuntimeError("legacy key codec path removed")

    # ------------------------------------------------------------------
    # reconstruct_vectors — finalize path, called once
    # ------------------------------------------------------------------

    cdef void reconstruct_vectors(
        self,
        int64_t num_groups,
        list out_names,
        list out_vecs,
    ) except *:
        """
        Decode stored group keys directly into Draken Vectors.

        Fixed columns  → Int64Vector with owned malloc'd buffer + optional null
                         bitmap; zero Python int objects allocated.
        String columns → StringVector built via StringVectorBuilder.append_bytes();
                         zero Python str objects; no pyarrow conversion.

        Single-column paths delegate to module-level helpers (_recon_single_fixed /
        _recon_single_string) which are specialised and tightly bounded.

        Multi-column path pre-allocates one vector per column, then fills all of
        them in a single decode loop.
        """
        cdef Py_ssize_t col_idx
        cdef int64_t key_kind
        cdef object col_name
        cdef Int64Vector _fixed_iv
        cdef DrakenFixedBuffer* _fixed_buf
        cdef DrakenVarBuffer* _string_buf

        # ---- Single-column fast paths ----
        if self._n_cols == 1:
            key_kind = self._key_kinds[0]
            col_name = self._group_columns[0]
            out_names.append(col_name.decode("utf-8") if isinstance(col_name, bytes) else col_name)

            if key_kind == KEY_MULTI_ENCODED_STRING:
                if self._single_string_direct:
                    self._single_string_buf.length = <size_t>self._single_string_rows
                    self._single_string_buf.null_bitmap = self._single_string_nulls
                    out_vecs.append(_wrap_string_buffer(self._single_string_buf))
                    self._single_string_buf = alloc_var_buffer(DRAKEN_STRING, 0, 0)
                    self._single_string_buf.offsets[0] = 0
                    self._single_string_nulls = NULL
                    self._single_string_rows = 0
                    self._single_string_bytes = 0
                else:
                    raise RuntimeError("single string codec path removed")
            else:
                if self._single_fixed_direct:
                    _fixed_buf = self._single_fixed_buf
                    _fixed_buf.length = <size_t>self._single_fixed_rows
                    _fixed_buf.null_bitmap = self._single_fixed_nulls

                    _fixed_iv = Int64Vector(0, True)
                    _fixed_iv.ptr = _fixed_buf
                    _fixed_iv.owns_data = True
                    _fixed_iv._dict_values = NULL
                    _fixed_iv._dict_codes = NULL
                    _fixed_iv._dict_code_width = 0
                    _fixed_iv._dict_ordered = 0
                    _fixed_iv._dict_accessor.codes = NULL
                    _fixed_iv._dict_accessor.code_width = 0
                    _fixed_iv._dict_accessor.row_nulls = NULL
                    _fixed_iv._dict_accessor.length = 0
                    _fixed_iv._dict_accessor.dict_values = NULL
                    _fixed_iv._dict_accessor.value_type = DRAKEN_INT64
                    _fixed_iv._const_accessor.length = 0
                    _fixed_iv._const_accessor.value_type = DRAKEN_INT64
                    _fixed_iv._const_accessor.value_ptr = NULL
                    _fixed_iv._const_accessor.is_null = 0
                    _fixed_iv._const_value = 0
                    _fixed_iv._has_const = False
                    _fixed_iv._const_is_null = False
                    _refresh_unified_int64(_fixed_iv)
                    out_vecs.append(_fixed_iv)

                    self._single_fixed_buf = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
                    self._single_fixed_nulls = NULL
                    self._single_fixed_rows = 0
                else:
                    raise RuntimeError("single fixed codec path removed")
            return

        # ---- Multi-column path ----
        if self._multi_direct:
            for col_idx in range(self._n_cols):
                col_name = self._group_columns[col_idx]
                out_names.append(col_name.decode("utf-8") if isinstance(col_name, bytes) else col_name)

                if self._multi_storage_kind[col_idx] == _DISPATCH_STRING:
                    storage_slot = self._multi_storage_slot[col_idx]
                    _string_buf = self._multi_string_bufs[storage_slot]
                    _string_buf.length = <size_t>self._multi_string_rows[storage_slot]
                    _string_buf.null_bitmap = self._multi_string_nulls[storage_slot]
                    out_vecs.append(_wrap_string_buffer(_string_buf))

                    self._multi_string_bufs[storage_slot] = alloc_var_buffer(DRAKEN_STRING, 0, 0)
                    self._multi_string_bufs[storage_slot].offsets[0] = 0
                    self._multi_string_nulls[storage_slot] = NULL
                    self._multi_string_rows[storage_slot] = 0
                    self._multi_string_bytes[storage_slot] = 0
                else:
                    storage_slot = self._multi_storage_slot[col_idx]
                    _fixed_buf = self._multi_fixed_bufs[storage_slot]
                    _fixed_buf.length = <size_t>self._multi_fixed_rows[storage_slot]
                    _fixed_buf.null_bitmap = self._multi_fixed_nulls[storage_slot]

                    _fixed_iv = Int64Vector(0, True)
                    _fixed_iv.ptr = _fixed_buf
                    _fixed_iv.owns_data = True
                    _fixed_iv._dict_values = NULL
                    _fixed_iv._dict_codes = NULL
                    _fixed_iv._dict_code_width = 0
                    _fixed_iv._dict_ordered = 0
                    _fixed_iv._dict_accessor.codes = NULL
                    _fixed_iv._dict_accessor.code_width = 0
                    _fixed_iv._dict_accessor.row_nulls = NULL
                    _fixed_iv._dict_accessor.length = 0
                    _fixed_iv._dict_accessor.dict_values = NULL
                    _fixed_iv._dict_accessor.value_type = DRAKEN_INT64
                    _fixed_iv._const_accessor.length = 0
                    _fixed_iv._const_accessor.value_type = DRAKEN_INT64
                    _fixed_iv._const_accessor.value_ptr = NULL
                    _fixed_iv._const_accessor.is_null = 0
                    _fixed_iv._const_value = 0
                    _fixed_iv._has_const = False
                    _fixed_iv._const_is_null = False
                    _refresh_unified_int64(_fixed_iv)
                    out_vecs.append(_fixed_iv)

                    self._multi_fixed_bufs[storage_slot] = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
                    self._multi_fixed_nulls[storage_slot] = NULL
                    self._multi_fixed_rows[storage_slot] = 0
            return

        raise RuntimeError("legacy key codec reconstruct path removed")
