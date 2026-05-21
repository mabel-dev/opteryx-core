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
from libc.stdint cimport int32_t, int64_t, uint8_t, uint32_t
from libc.stdlib cimport realloc, free

from libcpp.vector cimport vector

from draken.core.buffers cimport DrakenFixedBuffer, DrakenVarBuffer, DrakenType, DrakenVector
from draken.core.buffers cimport DrakenStringArena, DrakenStringSlot
from draken.core.buffers cimport str_length, str_data
from draken.core.buffers cimport STR_INLINE_MAX
from draken.core.buffers cimport str_init_null, str_init_inline, str_init_extern
from draken.core.buffers cimport DRAKEN_INT64
from draken.core.buffers cimport DRAKEN_STRING
from draken.core.buffers cimport draken_vector_from_dense
from draken.core.fixed_vector cimport alloc_fixed_buffer, free_fixed_buffer
from draken.core.var_vector cimport alloc_var_buffer, free_var_buffer
from draken.core.string_arena cimport alloc_string_arena
from draken.vectors.vector cimport Vector
from draken.vectors.integer64_vector cimport Integer64Vector
from draken.vectors.float64_vector cimport Float64Vector
from draken.vectors.string_vector cimport StringVector
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
    cdef DrakenStringArena* arena
    cdef Py_ssize_t i, off, slen, total_extern_bytes = 0
    cdef Py_ssize_t row_count = <Py_ssize_t>buf.length
    cdef const uint8_t* src_data = <const uint8_t*>buf.data
    cdef const int32_t* src_offsets = buf.offsets
    cdef uint8_t* src_nulls = buf.null_bitmap
    cdef DrakenStringSlot* slots
    cdef uint8_t* arena_bytes
    cdef size_t arena_used = 0
    cdef bint row_is_null

    for i in range(row_count):
        slen = src_offsets[i + 1] - src_offsets[i]
        if slen > STR_INLINE_MAX:
            total_extern_bytes += slen

    arena = alloc_string_arena(DRAKEN_STRING, <size_t>row_count, <size_t>total_extern_bytes)
    if arena == NULL:
        raise MemoryError()

    slots = arena.slots
    arena_bytes = arena.arena

    for i in range(row_count):
        off = src_offsets[i]
        slen = src_offsets[i + 1] - off
        row_is_null = (src_nulls != NULL and ((src_nulls[i >> 3] >> (i & 7)) & 1) == 0)
        if row_is_null:
            str_init_null(&slots[i])
        elif slen <= STR_INLINE_MAX:
            str_init_inline(&slots[i], src_data + off, <uint32_t>slen)
        else:
            memcpy(arena_bytes + arena_used, src_data + off, <size_t>slen)
            str_init_extern(&slots[i], src_data + off, <uint32_t>slen, <uint64_t>arena_used)
            arena_used += <size_t>slen

    arena.arena_used = arena_used
    arena.length = <size_t>row_count

    vec.ptr = buf
    vec.owns_data = True
    vec._unified_view = draken_vector_from_dense(
        <void*>arena, <uint32_t>row_count, DRAKEN_STRING, src_nulls)
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


cdef inline void _ks_store_single_fixed_bulk_bool(
    DrakenFixedBuffer* buf,
    uint8_t** null_bitmap_ref,
    Py_ssize_t* row_count_ref,
    const int64_t* row_indices,
    Py_ssize_t n_new,
    uint8_t* src_nulls,
    const uint8_t* src_data,
    const uint32_t* src_sel,
) except *:
    cdef Py_ssize_t start_row = row_count_ref[0]
    cdef Py_ssize_t ri
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t out_row
    cdef uint32_t code
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
            code = src_sel[row_idx]
            dst[out_row] = <int64_t>((src_data[code >> 3] >> (code & 7)) & 1)
            if null_bitmap_ref[0] != NULL:
                _ks_bitmap_set(null_bitmap_ref[0], out_row)
        else:
            dst[out_row] = 0
            if null_bitmap_ref[0] != NULL:
                _ks_bitmap_clear(null_bitmap_ref[0], out_row)

    row_count_ref[0] = start_row + n_new
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
    const uint32_t* codes,
) except *:
    """Store int64/float64 keys via unified selection: data[selection[i]].

    dict_data points to the raw int64_t values of the DrakenVector.data buffer.
    For float64, caller passes the float bits reinterpreted as int64.
    codes is uv.selection — never NULL; works for dense, constant, and dict layouts.
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
            code = codes[row_idx]
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
    DrakenStringArena* dict_garena,
    const uint32_t* codes,
) except *:
    """Store string dict-encoded keys without materializing the full column.
    codes is always uint32_t* (selection array in unified format).
    """
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
    cdef DrakenStringSlot* gs_slot
    cdef const uint8_t* gs_sdata

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
            code = codes[row_idx]
            gs_slot = &dict_garena.slots[code]
            str_len = <Py_ssize_t>str_length(gs_slot)
            if str_len > 0:
                gs_sdata = str_data(gs_slot, dict_garena.arena)
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
                memcpy(buf.data + bytes_used, gs_sdata, str_len)
            bytes_used += str_len
            if null_bitmap_ref[0] != NULL:
                _ks_bitmap_set(null_bitmap_ref[0], out_row)

        buf.offsets[out_row + 1] = <int32_t>bytes_used

    row_count_ref[0] = required_rows
    bytes_used_ref[0] = bytes_used
    buf.null_bitmap = null_bitmap_ref[0]


cdef inline void _ks_store_multi_bool_bulk(
    DrakenFixedBuffer* buf,
    uint8_t** null_bitmap_ref,
    Py_ssize_t* row_count_ref,
    const int64_t* row_indices,
    Py_ssize_t n_new,
    uint8_t* src_nulls,
    const uint8_t* src_data,
    const uint32_t* src_sel,
) except *:
    cdef Py_ssize_t start_row = row_count_ref[0]
    cdef Py_ssize_t ri, row_idx, out_row
    cdef uint32_t code
    cdef int64_t* dst

    _ks_ensure_fixed_capacity(buf, start_row, start_row + n_new)
    dst = <int64_t*>buf.data

    for ri in range(n_new):
        row_idx = row_indices[ri]
        out_row = start_row + ri
        if src_nulls != NULL and not ((src_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
            dst[out_row] = 0
            if null_bitmap_ref[0] == NULL:
                _ks_ensure_bitmap_capacity(null_bitmap_ref, start_row, start_row + n_new)
            _ks_bitmap_clear(null_bitmap_ref[0], out_row)
        else:
            code = src_sel[row_idx]
            dst[out_row] = <int64_t>((src_data[code >> 3] >> (code & 7)) & 1)
            if null_bitmap_ref[0] != NULL:
                _ks_bitmap_set(null_bitmap_ref[0], out_row)

    row_count_ref[0] = start_row + n_new
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

        cdef Py_ssize_t col_idx
        cdef int64_t key_kind
        cdef list vecs
        cdef Vector vec
        cdef Integer64Vector iv
        cdef Float64Vector fv
        cdef StringVector sv
        cdef BoolVector bv

        # Multi-column pre-computed dispatch
        cdef vector[int]    col_dispatch
        cdef vector[size_t] col_null_ptrs
        cdef vector[size_t] col_dense_ptrs
        cdef vector[size_t] col_arena_ptrs
        cdef vector[size_t] col_dict_code_ptrs
        cdef int disp
        cdef int storage_slot
        cdef DrakenVector* uv

        if self._n_cols == 1:
            # ----------------------------------------------------------------
            # Single-column fast paths — statically dispatched
            # ----------------------------------------------------------------
            key_kind = self._key_kinds[0]
            vec = morsel.column(self._group_columns[0])

            if key_kind == KEY_MULTI_ENCODED_STRING:
                sv = <StringVector>vec
                uv = sv.unified()
                if self._single_string_direct:
                    _ks_store_string_bulk_dict(
                        self._single_string_buf,
                        &self._single_string_nulls,
                        &self._single_string_rows,
                        &self._single_string_bytes,
                        row_indices,
                        n_new,
                        uv.validity,
                        <DrakenStringArena*>uv.data,
                        uv.selection,
                    )
                else:
                    raise RuntimeError("single string codec path removed")

            elif isinstance(vec, Integer64Vector):
                iv = <Integer64Vector>vec
                uv = iv.unified()
                if self._single_fixed_direct:
                    _ks_store_fixed_bulk_dict(
                        self._single_fixed_buf,
                        &self._single_fixed_nulls,
                        &self._single_fixed_rows,
                        row_indices,
                        n_new,
                        uv.validity,
                        <const int64_t*>uv.data,
                        uv.selection,
                    )
                else:
                    raise RuntimeError("single fixed key codec path removed")

            elif isinstance(vec, BoolVector):
                bv = <BoolVector>vec
                uv = bv.unified()
                if self._single_fixed_direct:
                    _ks_store_single_fixed_bulk_bool(
                        self._single_fixed_buf,
                        &self._single_fixed_nulls,
                        &self._single_fixed_rows,
                        row_indices,
                        n_new,
                        uv.validity,
                        <const uint8_t*>uv.data,
                        uv.selection,
                    )
                else:
                    raise RuntimeError("single fixed key codec path removed")

            else:
                # Float64Vector and other fixed-width types — store as raw int64 bits
                fv = <Float64Vector>vec
                uv = fv.unified()
                if self._single_fixed_direct:
                    _ks_store_fixed_bulk_dict(
                        self._single_fixed_buf,
                        &self._single_fixed_nulls,
                        &self._single_fixed_rows,
                        row_indices,
                        n_new,
                        uv.validity,
                        <const int64_t*>uv.data,
                        uv.selection,
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
            col_arena_ptrs.resize(self._n_cols, 0)
            col_dict_code_ptrs.resize(self._n_cols, 0)

            for col_idx in range(self._n_cols):
                key_kind = self._key_kinds[col_idx]
                vec = vecs[col_idx]

                if key_kind == KEY_MULTI_ENCODED_STRING:
                    sv = <StringVector>vec
                    uv = sv.unified()
                    col_dispatch[col_idx]       = _DISPATCH_STRING
                    col_null_ptrs[col_idx]      = <size_t>uv.validity
                    col_arena_ptrs[col_idx]     = <size_t>uv.data
                    col_dict_code_ptrs[col_idx] = <size_t>uv.selection

                elif isinstance(vec, Integer64Vector):
                    iv = <Integer64Vector>vec
                    uv = iv.unified()
                    col_dispatch[col_idx]       = _DISPATCH_INT64
                    col_null_ptrs[col_idx]      = <size_t>uv.validity
                    col_dense_ptrs[col_idx]     = <size_t>uv.data
                    col_dict_code_ptrs[col_idx] = <size_t>uv.selection

                elif isinstance(vec, BoolVector):
                    bv = <BoolVector>vec
                    uv = bv.unified()
                    col_dispatch[col_idx]       = _DISPATCH_BOOL
                    col_null_ptrs[col_idx]      = <size_t>uv.validity
                    col_dense_ptrs[col_idx]     = <size_t>uv.data
                    col_dict_code_ptrs[col_idx] = <size_t>uv.selection

                else:
                    # Float64Vector and other fixed-width types
                    fv = <Float64Vector>vec
                    uv = fv.unified()
                    col_dispatch[col_idx]       = _DISPATCH_FLOAT64
                    col_null_ptrs[col_idx]      = <size_t>uv.validity
                    col_dense_ptrs[col_idx]     = <size_t>uv.data
                    col_dict_code_ptrs[col_idx] = <size_t>uv.selection

            if self._multi_direct:
                for col_idx in range(self._n_cols):
                    disp = col_dispatch[col_idx]
                    storage_slot = self._multi_storage_slot[col_idx]

                    if disp == _DISPATCH_STRING:
                        _ks_store_string_bulk_dict(
                            self._multi_string_bufs[storage_slot],
                            &self._multi_string_nulls[storage_slot],
                            &self._multi_string_rows[storage_slot],
                            &self._multi_string_bytes[storage_slot],
                            row_indices,
                            n_new,
                            <uint8_t*>col_null_ptrs[col_idx],
                            <DrakenStringArena*>col_arena_ptrs[col_idx],
                            <const uint32_t*>col_dict_code_ptrs[col_idx],
                        )
                    elif disp == _DISPATCH_INT64 or disp == _DISPATCH_FLOAT64:
                        _ks_store_fixed_bulk_dict(
                            self._multi_fixed_bufs[storage_slot],
                            &self._multi_fixed_nulls[storage_slot],
                            &self._multi_fixed_rows[storage_slot],
                            row_indices,
                            n_new,
                            <uint8_t*>col_null_ptrs[col_idx],
                            <const int64_t*>col_dense_ptrs[col_idx],
                            <const uint32_t*>col_dict_code_ptrs[col_idx],
                        )
                    elif disp == _DISPATCH_BOOL:
                        _ks_store_multi_bool_bulk(
                            self._multi_fixed_bufs[storage_slot],
                            &self._multi_fixed_nulls[storage_slot],
                            &self._multi_fixed_rows[storage_slot],
                            row_indices,
                            n_new,
                            <uint8_t*>col_null_ptrs[col_idx],
                            <const uint8_t*>col_dense_ptrs[col_idx],
                            <const uint32_t*>col_dict_code_ptrs[col_idx],
                        )
                    else:
                        raise RuntimeError("unknown dispatch code in multi-column key store")
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

        Fixed columns  → Integer64Vector with owned malloc'd buffer + optional null
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
        cdef Integer64Vector _fixed_iv
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

                    _fixed_iv = Integer64Vector(0, True)
                    _fixed_iv.ptr = _fixed_buf
                    _fixed_iv.owns_data = True
                    _fixed_iv._unified_view = draken_vector_from_dense(_fixed_buf.data, <uint32_t>_fixed_buf.length, DRAKEN_INT64, _fixed_buf.null_bitmap)
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

                    _fixed_iv = Integer64Vector(0, True)
                    _fixed_iv.ptr = _fixed_buf
                    _fixed_iv.owns_data = True
                    _fixed_iv._unified_view = draken_vector_from_dense(_fixed_buf.data, <uint32_t>_fixed_buf.length, DRAKEN_INT64, _fixed_buf.null_bitmap)
                    out_vecs.append(_fixed_iv)

                    self._multi_fixed_bufs[storage_slot] = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
                    self._multi_fixed_nulls[storage_slot] = NULL
                    self._multi_fixed_rows[storage_slot] = 0
            return

        raise RuntimeError("legacy key codec reconstruct path removed")
