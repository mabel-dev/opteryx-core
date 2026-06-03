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
from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint32_t
from libc.stdlib cimport realloc, free
from libc.stddef cimport size_t

from libcpp.vector cimport vector

from cpython.object cimport PyObject

from draken.core.buffers cimport DrakenFixedBuffer, DrakenType, DrakenVector
from draken.core.buffers cimport DrakenStringArena, DrakenStringSlot
from draken.core.buffers cimport str_length, str_data, str_is_inline, str_clone_with_offset
from draken.core.buffers cimport STR_INLINE_MAX
from draken.core.buffers cimport str_init_null, str_init_inline, str_init_extern
from draken.core.buffers cimport DRAKEN_INT64, DRAKEN_VARCHAR, DRAKEN_NVARCHAR, DRAKEN_VARBINARY, DRAKEN_TIMESTAMP64, DRAKEN_FLOAT64
from draken.core.buffers cimport DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32, DRAKEN_FLOAT32, DRAKEN_DATE32, DRAKEN_TIME32


# Byte width of a fixed-width DrakenType's payload. Group keys are stored in an
# int64 buffer; narrow values must be read at their true width and widened, NOT
# read at int64 stride (that over-reads past the source buffer — heap overflow).
cdef inline int _ks_fixed_itemsize(DrakenType t) noexcept nogil:
    if t == DRAKEN_INT8:
        return 1
    if t == DRAKEN_INT16:
        return 2
    if t == DRAKEN_INT32 or t == DRAKEN_FLOAT32 or t == DRAKEN_DATE32 or t == DRAKEN_TIME32:
        return 4
    return 8   # INT64, FLOAT64, TIME64, TIMESTAMP64, DECIMAL, etc.
from draken.core.fixed_vector cimport alloc_fixed_buffer, free_fixed_buffer
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.vector cimport Vector
from draken.vectors.vector cimport from_decoded as _ks_vector_from_decoded

cdef extern from "core/alloc.h" nogil:
    void* draken_malloc(size_t n) nogil
    void  draken_free(void* p) nogil

# C-level Py_DECREF to balance the NEW reference draken_vector_own_string returns.
cdef extern from *:
    """static inline void _ks_decref(PyObject* op) { Py_XDECREF(op); }"""
    void _ks_decref(PyObject* op)

cdef extern from "core/draken_bridge.h":
    # Wrap hand-built German-string buffers (slots + arena + validity, all
    # draken_malloc'd) into a new string-family Vector. Ownership of all three
    # transfers on call; slots' precomputed hash32 is trusted (no rehash).
    # Returns a NEW reference to a Python Vector, or NULL on failure.
    PyObject* draken_vector_own_string(
        DrakenStringSlot* slots, uint8_t* arena, size_t arena_len,
        uint8_t* validity, uint32_t length, DrakenType vec_type)

    # Wrap a draken_malloc'd int64 buffer as a DRAKEN_TIMESTAMP64 Vector with the
    # given unit ("s"/"ms"/"us"/"ns"). Ownership of data + validity transfers on
    # call. Returns a NEW reference to a Python Vector, or NULL on failure.
    PyObject* draken_vector_own_timestamp(
        void* data, uint8_t* validity, uint32_t length, const char* unit_str)


# ---------------------------------------------------------------------------
# Key kind constants
# ---------------------------------------------------------------------------
cdef int KEY_MULTI_FIXED_INT = 1
cdef int KEY_MULTI_FIXED_DATE32 = 2
cdef int KEY_MULTI_FIXED_TIME32 = 3
cdef int KEY_MULTI_FIXED_TIME64 = 4
cdef int KEY_MULTI_FIXED_TIMESTAMP64 = 5
cdef int KEY_MULTI_ENCODED_STRING = 6
cdef int KEY_MULTI_FIXED_FLOAT64 = 7

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

cdef Vector _ks_consume_fixed8_buffer(DrakenFixedBuffer* buf, DrakenType out_type) except *:
    """Copy a libc-malloc'd 8-byte-slot DrakenFixedBuffer into a fresh
    draken_malloc'd Vector tagged `out_type` and free the source. Used in the
    finalize path (reconstruct_vectors) where the collector hands the buffer
    off and immediately swaps in a fresh one.

    Storage is always 8-byte slots; `out_type` only reinterprets the bits at
    the Vector surface. INT64 keys emerge as INT64; FLOAT64 keys are stored as
    raw double bits in the same int64 buffer and re-tagged FLOAT64 here so they
    surface as doubles, not giant integers.

    Cross-allocator copy is required: collector state uses libc malloc
    (alloc_fixed_buffer); Vectors require draken_malloc (mimalloc).
    """
    cdef Py_ssize_t length = <Py_ssize_t>buf.length
    cdef size_t nbytes
    cdef int64_t* out_data
    cdef uint8_t* validity = NULL
    cdef Py_ssize_t bitmap_bytes

    if length <= 0:
        free_fixed_buffer(buf, True)
        return _ks_vector_from_decoded(NULL, NULL, 0, out_type)

    nbytes = <size_t>length * sizeof(int64_t)
    out_data = <int64_t*>draken_malloc(nbytes)
    if out_data == NULL:
        free_fixed_buffer(buf, True)
        raise MemoryError()
    memcpy(out_data, buf.data, nbytes)

    if buf.null_bitmap != NULL:
        bitmap_bytes = (length + 7) >> 3
        validity = <uint8_t*>draken_malloc(<size_t>bitmap_bytes)
        if validity == NULL:
            draken_free(out_data)
            free_fixed_buffer(buf, True)
            raise MemoryError()
        memcpy(validity, buf.null_bitmap, bitmap_bytes)

    free_fixed_buffer(buf, True)
    return _ks_vector_from_decoded(<void*>out_data, validity, <uint32_t>length, out_type)


cdef Vector _ks_own_timestamp_buffer(void* data, uint8_t* validity,
                                     uint32_t length, bytes unit) except *:
    """Wrap a draken_malloc'd int64 buffer (raw timestamp values, in `unit`)
    as a DRAKEN_TIMESTAMP64 Vector. Ownership of data + validity transfers to
    the Vector (own_timestamp frees them on failure)."""
    cdef PyObject* raw = draken_vector_own_timestamp(data, validity, length, <const char*>unit)
    if raw == NULL:
        raise MemoryError("draken_vector_own_timestamp failed")
    cdef Vector result = <Vector>(<object>raw)   # Cython incref → 2
    _ks_decref(raw)                              # balance the NEW ref → 1
    return result


cdef Vector _ks_consume_timestamp_buffer(DrakenFixedBuffer* buf, bytes unit) except *:
    """Like _ks_consume_fixed8_buffer, but emits a DRAKEN_TIMESTAMP64 Vector
    tagged with `unit`. Copies the libc-malloc'd buffer into draken_malloc'd
    storage (cross-allocator), frees the source, and transfers ownership."""
    cdef Py_ssize_t length = <Py_ssize_t>buf.length
    cdef size_t nbytes
    cdef int64_t* out_data
    cdef uint8_t* validity = NULL
    cdef Py_ssize_t bitmap_bytes

    if length <= 0:
        free_fixed_buffer(buf, True)
        return _ks_own_timestamp_buffer(NULL, NULL, 0, unit)

    nbytes = <size_t>length * sizeof(int64_t)
    out_data = <int64_t*>draken_malloc(nbytes)
    if out_data == NULL:
        free_fixed_buffer(buf, True)
        raise MemoryError()
    memcpy(out_data, buf.data, nbytes)

    if buf.null_bitmap != NULL:
        bitmap_bytes = (length + 7) >> 3
        validity = <uint8_t*>draken_malloc(<size_t>bitmap_bytes)
        if validity == NULL:
            draken_free(out_data)
            free_fixed_buffer(buf, True)
            raise MemoryError()
        memcpy(validity, buf.null_bitmap, bitmap_bytes)

    free_fixed_buffer(buf, True)
    return _ks_own_timestamp_buffer(<void*>out_data, validity, <uint32_t>length, unit)


# ---------------------------------------------------------------------------
# GsAccum — German-string accumulator for group keys.
#
# Group keys are accumulated DIRECTLY in the engine's native string format
# (DrakenStringSlot[] + arena), not as an intermediate Arrow-varlen buffer.
# Each new group copies its source slot verbatim — preserving the precomputed
# prefix and hash32 — and, for long (>12B) strings, copies the payload bytes
# once into the accumulator arena. Inline (<=12B) strings need no arena copy.
#
# At finalize the buffers transfer straight into a Vector via
# draken_vector_own_string with NO second copy and NO hash recomputation.
# (draken_malloc == malloc, so realloc-grown buffers transfer cleanly.)
# ---------------------------------------------------------------------------
cdef struct GsAccum:
    DrakenStringSlot* slots
    Py_ssize_t        slots_cap
    Py_ssize_t        rows
    uint8_t*          arena
    Py_ssize_t        arena_cap
    Py_ssize_t        arena_used
    uint8_t*          nulls        # validity bitmap; NULL = all-valid so far
    DrakenType        type


cdef inline void gs_accum_init(GsAccum* a, DrakenType t) noexcept nogil:
    a.slots = NULL
    a.slots_cap = 0
    a.rows = 0
    a.arena = NULL
    a.arena_cap = 0
    a.arena_used = 0
    a.nulls = NULL
    a.type = t


cdef inline void gs_accum_free(GsAccum* a) noexcept nogil:
    if a.slots != NULL:
        draken_free(a.slots)
        a.slots = NULL
    if a.arena != NULL:
        draken_free(a.arena)
        a.arena = NULL
    if a.nulls != NULL:
        draken_free(a.nulls)
        a.nulls = NULL
    a.slots_cap = 0
    a.rows = 0
    a.arena_cap = 0
    a.arena_used = 0


cdef inline void _gs_reserve_slots(GsAccum* a, Py_ssize_t need) except *:
    cdef Py_ssize_t newcap
    cdef void* p
    if need <= a.slots_cap:
        return
    newcap = a.slots_cap * 2 if a.slots_cap > 0 else 16
    if newcap < need:
        newcap = need
    p = realloc(a.slots, <size_t>newcap * sizeof(DrakenStringSlot))
    if p == NULL:
        raise MemoryError()
    a.slots = <DrakenStringSlot*>p
    a.slots_cap = newcap


cdef inline void _gs_reserve_arena(GsAccum* a, Py_ssize_t need) except *:
    cdef Py_ssize_t newcap
    cdef void* p
    if need <= a.arena_cap:
        return
    newcap = a.arena_cap * 2 if a.arena_cap > 0 else 64
    if newcap < need:
        newcap = need
    p = realloc(a.arena, <size_t>newcap)
    if p == NULL:
        raise MemoryError()
    a.arena = <uint8_t*>p
    a.arena_cap = newcap


# Append one group key, sourced from src_slot (in the source arena src_arena).
# valid=0 → null key (zeroed slot, validity bit cleared). Preserves the source
# slot's prefix/hash32 verbatim; copies arena bytes only for long strings.
cdef inline void gs_accum_append(GsAccum* a, const DrakenStringSlot* src_slot,
                                 const uint8_t* src_arena, bint valid) except *:
    cdef Py_ssize_t row = a.rows
    cdef DrakenStringSlot* dst
    cdef Py_ssize_t slen
    _gs_reserve_slots(a, row + 1)
    dst = &a.slots[row]
    if not valid:
        str_init_null(dst)
        _ks_ensure_bitmap_capacity(&a.nulls, row, row + 1)
        _ks_bitmap_clear(a.nulls, row)
    elif str_is_inline(src_slot):
        str_clone_with_offset(dst, src_slot, 0)   # inline bytes self-contained
        if a.nulls != NULL:
            _ks_bitmap_set(a.nulls, row)
    else:
        slen = <Py_ssize_t>str_length(src_slot)
        _gs_reserve_arena(a, a.arena_used + slen)
        memcpy(a.arena + a.arena_used, str_data(src_slot, src_arena), <size_t>slen)
        str_clone_with_offset(dst, src_slot, <uint32_t>a.arena_used)  # rebased, hash kept
        a.arena_used += slen
        if a.nulls != NULL:
            _ks_bitmap_set(a.nulls, row)
    a.rows = row + 1


# Finalize: transfer the accumulated buffers into a Vector (no copy, no rehash)
# and reset the accumulator to empty for reuse.
cdef inline Vector gs_accum_to_vector(GsAccum* a) except *:
    cdef PyObject* raw = draken_vector_own_string(
        a.slots, a.arena, <size_t>a.arena_used, a.nulls, <uint32_t>a.rows, a.type)
    if raw == NULL:
        raise MemoryError("draken_vector_own_string failed")
    cdef Vector result = <Vector>(<object>raw)   # Cython incref → 2
    _ks_decref(raw)                               # balance the NEW ref → 1
    # Ownership transferred to the Vector; reset accumulator (do not free).
    a.slots = NULL; a.slots_cap = 0; a.rows = 0
    a.arena = NULL; a.arena_cap = 0; a.arena_used = 0
    a.nulls = NULL
    return result


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
    const void* dict_data,
    const uint32_t* codes,
    int src_itemsize,
) except *:
    """Store fixed-width keys via unified selection: data[selection[i]].

    dict_data points at the raw DrakenVector.data buffer; `src_itemsize` is the
    source element width in bytes (1/2/4/8). Narrow values are read at their true
    width and widened (sign-extended) into the int64 key buffer — reading at
    int64 stride would over-read past the source buffer (heap overflow) and
    corrupt the key value. For 8-byte sources (INT64/FLOAT64/TIMESTAMP64/…) the
    int64 read is the existing raw-bits behaviour.
    codes is uv.selection — never NULL; works for dense, constant, and dict layouts.

    Single-pass: bitmap is lazily allocated on the first null encountered.
    _ks_alloc_all_valid_bitmap initialises all bits to 1, so rows stored before
    the first null are automatically marked valid without a back-fill pass.
    """
    cdef Py_ssize_t start_row = row_count_ref[0]
    cdef Py_ssize_t ri
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t out_row
    cdef uint32_t code
    cdef int64_t* dst
    cdef const int64_t* d8 = <const int64_t*>dict_data
    cdef const int32_t* d4 = <const int32_t*>dict_data
    cdef const int16_t* d2 = <const int16_t*>dict_data
    cdef const int8_t*  d1 = <const int8_t*>dict_data

    _ks_ensure_fixed_capacity(buf, start_row, start_row + n_new)

    dst = <int64_t*>buf.data
    for ri in range(n_new):
        row_idx = row_indices[ri]
        out_row = start_row + ri
        if row_nulls != NULL and not ((row_nulls[row_idx >> 3] >> (row_idx & 7)) & 1):
            dst[out_row] = 0
            if null_bitmap_ref[0] == NULL:
                # First null seen — lazily allocate bitmap with all prior rows
                # marked valid (alloc_all_valid_bitmap sets all bits to 1).
                _ks_ensure_bitmap_capacity(null_bitmap_ref, start_row, start_row + n_new)
            _ks_bitmap_clear(null_bitmap_ref[0], out_row)
        else:
            code = codes[row_idx]
            # Width-dispatch (loop-invariant branch — predicted, effectively free).
            if src_itemsize == 8:
                dst[out_row] = d8[code]
            elif src_itemsize == 4:
                dst[out_row] = <int64_t>d4[code]
            elif src_itemsize == 2:
                dst[out_row] = <int64_t>d2[code]
            else:
                dst[out_row] = <int64_t>d1[code]
            if null_bitmap_ref[0] != NULL:
                _ks_bitmap_set(null_bitmap_ref[0], out_row)

    row_count_ref[0] = start_row + n_new
    buf.null_bitmap = null_bitmap_ref[0]


cdef inline void _ks_gs_store_bulk_dict(
    GsAccum* a,
    const int64_t* row_indices,
    Py_ssize_t n_new,
    uint8_t* row_nulls,
    DrakenStringArena* dict_garena,
    const uint32_t* codes,
) except *:
    """Append n_new new group keys directly as German-string slots.

    For each new row: take its dict code → source slot, and append it to the
    accumulator (slot copied verbatim, hash32 preserved; long-string bytes
    copied once into the accumulator arena). codes is the unified selection.
    """
    cdef Py_ssize_t ri
    cdef Py_ssize_t row_idx
    cdef bint valid
    cdef const DrakenStringSlot* src
    for ri in range(n_new):
        row_idx = row_indices[ri]
        valid = (row_nulls == NULL) or (((row_nulls[row_idx >> 3] >> (row_idx & 7)) & 1) != 0)
        if valid:
            src = &dict_garena.slots[codes[row_idx]]
            gs_accum_append(a, src, dict_garena.arena, True)
        else:
            gs_accum_append(a, NULL, NULL, False)


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
    cdef list _ts_units               # per-column timestamp unit (bytes) or None; set post-init

    cdef DrakenFixedBuffer* _single_fixed_buf
    cdef uint8_t* _single_fixed_nulls
    cdef Py_ssize_t _single_fixed_rows
    cdef bint _single_fixed_direct

    cdef GsAccum _single_gs                   # single string-key accumulator
    cdef bint _single_string_direct

    cdef vector[DrakenFixedBuffer*] _multi_fixed_bufs
    cdef vector[uint8_t*] _multi_fixed_nulls
    cdef vector[Py_ssize_t] _multi_fixed_rows
    cdef vector[GsAccum] _multi_gs            # one accumulator per string key column
    cdef vector[int] _multi_storage_kind
    cdef vector[int] _multi_storage_slot
    cdef bint _multi_direct

    def __cinit__(self, list group_columns, list key_kinds):
        self._group_columns = group_columns
        self._n_cols = len(group_columns)
        self._ts_units = [None] * self._n_cols
        self._single_fixed_buf = NULL
        self._single_fixed_nulls = NULL
        self._single_fixed_rows = 0
        self._single_fixed_direct = False
        gs_accum_init(&self._single_gs, DRAKEN_VARCHAR)
        self._single_string_direct = False
        self._multi_direct = False

        cdef Py_ssize_t i
        cdef int fixed_slot = 0
        cdef int string_slot = 0
        cdef GsAccum _ga

        for i in range(len(key_kinds)):
            self._key_kinds.push_back(<int64_t>key_kinds[i])

        if self._n_cols == 1 and len(key_kinds) == 1:
            if key_kinds[0] == KEY_MULTI_ENCODED_STRING:
                gs_accum_init(&self._single_gs, DRAKEN_VARCHAR)
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
                    gs_accum_init(&_ga, DRAKEN_VARCHAR)
                    self._multi_gs.push_back(_ga)
                    string_slot += 1
                else:
                    self._multi_storage_kind[i] = _DISPATCH_INT64
                    self._multi_storage_slot[i] = fixed_slot
                    self._multi_fixed_bufs.push_back(alloc_fixed_buffer(DRAKEN_INT64, 0, 8))
                    self._multi_fixed_nulls.push_back(NULL)
                    self._multi_fixed_rows.push_back(0)
                    fixed_slot += 1

    def set_string_col_type(self, Py_ssize_t col_idx, DrakenType draken_type):
        """Set the DrakenType tag (VARCHAR/NVARCHAR/VARBINARY) on the string key
        accumulator for column col_idx.

        Called from the factory once the actual column type is known from the
        first morsel. Storage is identical across the three types; the tag only
        drives the output Vector's op semantics.
        """
        cdef int storage_slot
        if self._n_cols == 1:
            self._single_gs.type = draken_type
        else:
            storage_slot = self._multi_storage_slot[col_idx]
            if storage_slot < <int>self._multi_gs.size():
                self._multi_gs[storage_slot].type = draken_type

    def set_timestamp_col_unit(self, Py_ssize_t col_idx, object unit):
        """Record the timestamp unit ("s"/"ms"/"us"/"ns") for a TIMESTAMP64 key
        column. Storage is identical to int64; the unit is reapplied at
        reconstruct so the group key emerges as TIMESTAMP, not raw epoch int."""
        if isinstance(unit, str):
            unit = unit.encode("utf-8")
        self._ts_units[col_idx] = unit

    def __dealloc__(self):
        cdef Py_ssize_t i

        if self._single_fixed_buf != NULL:
            self._single_fixed_buf.null_bitmap = self._single_fixed_nulls
            free_fixed_buffer(self._single_fixed_buf, True)
            self._single_fixed_buf = NULL
            self._single_fixed_nulls = NULL
            self._single_fixed_rows = 0

        gs_accum_free(&self._single_gs)

        for i in range(self._multi_fixed_bufs.size()):
            if self._multi_fixed_bufs[i] != NULL:
                self._multi_fixed_bufs[i].null_bitmap = self._multi_fixed_nulls[i]
                free_fixed_buffer(self._multi_fixed_bufs[i], True)
                self._multi_fixed_bufs[i] = NULL

        for i in range(self._multi_gs.size()):
            gs_accum_free(&self._multi_gs[i])

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
        cdef Vector iv
        cdef Vector fv
        cdef BoolVector bv

        # Multi-column pre-computed dispatch
        cdef vector[int]    col_dispatch
        cdef vector[int]    col_itemsizes
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

            uv = (<Vector>vec).unified()

            if key_kind == KEY_MULTI_ENCODED_STRING:
                if self._single_string_direct:
                    _ks_gs_store_bulk_dict(
                        &self._single_gs,
                        row_indices,
                        n_new,
                        uv.validity,
                        <DrakenStringArena*>uv.data,
                        uv.selection,
                    )
                else:
                    raise RuntimeError("single string codec path removed")

            elif uv.type == DRAKEN_INT64 or uv.type == DRAKEN_INT32 or uv.type == DRAKEN_INT16 or uv.type == DRAKEN_INT8:
                if self._single_fixed_direct:
                    _ks_store_fixed_bulk_dict(
                        self._single_fixed_buf,
                        &self._single_fixed_nulls,
                        &self._single_fixed_rows,
                        row_indices,
                        n_new,
                        uv.validity,
                        <const void*>uv.data,
                        uv.selection,
                        _ks_fixed_itemsize(uv.type),
                    )
                else:
                    raise RuntimeError("single fixed key codec path removed")

            elif uv.type == DRAKEN_BOOL:
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
                # Float64 and other fixed-width types — store raw bits as int64,
                # read at the source's true width (4-byte FLOAT32/DATE32/TIME32
                # must not be read at int64 stride).
                if self._single_fixed_direct:
                    _ks_store_fixed_bulk_dict(
                        self._single_fixed_buf,
                        &self._single_fixed_nulls,
                        &self._single_fixed_rows,
                        row_indices,
                        n_new,
                        uv.validity,
                        <const void*>uv.data,
                        uv.selection,
                        _ks_fixed_itemsize(uv.type),
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
            col_itemsizes.resize(self._n_cols, 8)
            col_null_ptrs.resize(self._n_cols, 0)
            col_dense_ptrs.resize(self._n_cols, 0)
            col_arena_ptrs.resize(self._n_cols, 0)
            col_dict_code_ptrs.resize(self._n_cols, 0)

            for col_idx in range(self._n_cols):
                key_kind = self._key_kinds[col_idx]
                vec = vecs[col_idx]
                uv = (<Vector>vec).unified()

                if key_kind == KEY_MULTI_ENCODED_STRING:
                    col_dispatch[col_idx]       = _DISPATCH_STRING
                    col_null_ptrs[col_idx]      = <size_t>uv.validity
                    col_arena_ptrs[col_idx]     = <size_t>uv.data
                    col_dict_code_ptrs[col_idx] = <size_t>uv.selection

                elif uv.type == DRAKEN_INT64 or uv.type == DRAKEN_INT32 or uv.type == DRAKEN_INT16 or uv.type == DRAKEN_INT8:
                    col_dispatch[col_idx]       = _DISPATCH_INT64
                    col_itemsizes[col_idx]      = _ks_fixed_itemsize(uv.type)
                    col_null_ptrs[col_idx]      = <size_t>uv.validity
                    col_dense_ptrs[col_idx]     = <size_t>uv.data
                    col_dict_code_ptrs[col_idx] = <size_t>uv.selection

                elif uv.type == DRAKEN_BOOL:
                    bv = <BoolVector>vec
                    uv = bv.unified()
                    col_dispatch[col_idx]       = _DISPATCH_BOOL
                    col_null_ptrs[col_idx]      = <size_t>uv.validity
                    col_dense_ptrs[col_idx]     = <size_t>uv.data
                    col_dict_code_ptrs[col_idx] = <size_t>uv.selection

                else:
                    # Float64 and other fixed-width types — stored as raw bits,
                    # read at the source's true width.
                    col_dispatch[col_idx]       = _DISPATCH_FLOAT64
                    col_itemsizes[col_idx]      = _ks_fixed_itemsize(uv.type)
                    col_null_ptrs[col_idx]      = <size_t>uv.validity
                    col_dense_ptrs[col_idx]     = <size_t>uv.data
                    col_dict_code_ptrs[col_idx] = <size_t>uv.selection

            if self._multi_direct:
                for col_idx in range(self._n_cols):
                    disp = col_dispatch[col_idx]
                    storage_slot = self._multi_storage_slot[col_idx]

                    if disp == _DISPATCH_STRING:
                        _ks_gs_store_bulk_dict(
                            &self._multi_gs[storage_slot],
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
                            <const void*>col_dense_ptrs[col_idx],
                            <const uint32_t*>col_dict_code_ptrs[col_idx],
                            col_itemsizes[col_idx],
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
        cdef DrakenFixedBuffer* _fixed_buf
        cdef uint32_t fixed_length
        cdef Py_ssize_t nbytes, vbytes
        cdef void* new_data
        cdef uint8_t* new_validity
        cdef Vector fixed_iv

        # ---- Single-column fast paths ----
        if self._n_cols == 1:
            key_kind = self._key_kinds[0]
            col_name = self._group_columns[0]
            out_names.append(col_name.decode("utf-8") if isinstance(col_name, bytes) else col_name)

            if key_kind == KEY_MULTI_ENCODED_STRING:
                if self._single_string_direct:
                    # Transfer the accumulated slots+arena straight into a Vector
                    # (no copy, no rehash); the accumulator resets to empty.
                    out_vecs.append(gs_accum_to_vector(&self._single_gs))
                else:
                    raise RuntimeError("single string codec path removed")
            else:
                if self._single_fixed_direct:
                    _fixed_buf = self._single_fixed_buf
                    fixed_length = <uint32_t>self._single_fixed_rows
                    nbytes = <Py_ssize_t>fixed_length * 8
                    new_data = draken_malloc(<size_t>nbytes) if nbytes > 0 else NULL
                    if nbytes > 0 and new_data == NULL:
                        raise MemoryError()
                    if nbytes > 0:
                        memcpy(new_data, _fixed_buf.data, <size_t>nbytes)
                    new_validity = NULL
                    if self._single_fixed_nulls != NULL:
                        vbytes = _ks_bitmap_nbytes(<Py_ssize_t>fixed_length)
                        if vbytes > 0:
                            new_validity = <uint8_t*>draken_malloc(<size_t>vbytes)
                            if new_validity == NULL:
                                draken_free(new_data)
                                raise MemoryError()
                            memcpy(new_validity, self._single_fixed_nulls, <size_t>vbytes)
                    _fixed_buf.null_bitmap = self._single_fixed_nulls
                    free_fixed_buffer(_fixed_buf, True)
                    self._single_fixed_buf = NULL
                    if key_kind == KEY_MULTI_FIXED_TIMESTAMP64 and self._ts_units[0] is not None:
                        fixed_iv = _ks_own_timestamp_buffer(
                            new_data, new_validity, fixed_length, self._ts_units[0])
                    elif key_kind == KEY_MULTI_FIXED_FLOAT64:
                        fixed_iv = _from_decoded(new_data, new_validity, fixed_length, DRAKEN_FLOAT64)
                    else:
                        fixed_iv = _from_decoded(new_data, new_validity, fixed_length, DRAKEN_INT64)
                    out_vecs.append(fixed_iv)

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
                    out_vecs.append(gs_accum_to_vector(&self._multi_gs[storage_slot]))
                else:
                    storage_slot = self._multi_storage_slot[col_idx]
                    _fixed_buf = self._multi_fixed_bufs[storage_slot]
                    _fixed_buf.length = <size_t>self._multi_fixed_rows[storage_slot]
                    _fixed_buf.null_bitmap = self._multi_fixed_nulls[storage_slot]

                    # _ks_consume_fixed8_buffer copies + frees _fixed_buf and
                    # returns ownership transferred to a fresh draken_malloc'd
                    # Vector. TIMESTAMP64 keys reapply their captured unit so the
                    # column emerges as TIMESTAMP, not raw epoch int64. FLOAT64
                    # keys are re-tagged FLOAT64 so the raw double bits surface as
                    # doubles, not giant integers.
                    if self._key_kinds[col_idx] == KEY_MULTI_FIXED_TIMESTAMP64 and self._ts_units[col_idx] is not None:
                        out_vecs.append(_ks_consume_timestamp_buffer(_fixed_buf, self._ts_units[col_idx]))
                    elif self._key_kinds[col_idx] == KEY_MULTI_FIXED_FLOAT64:
                        out_vecs.append(_ks_consume_fixed8_buffer(_fixed_buf, DRAKEN_FLOAT64))
                    else:
                        out_vecs.append(_ks_consume_fixed8_buffer(_fixed_buf, DRAKEN_INT64))

                    self._multi_fixed_bufs[storage_slot] = alloc_fixed_buffer(DRAKEN_INT64, 0, 8)
                    self._multi_fixed_nulls[storage_slot] = NULL
                    self._multi_fixed_rows[storage_slot] = 0
            return

        raise RuntimeError("legacy key codec reconstruct path removed")
