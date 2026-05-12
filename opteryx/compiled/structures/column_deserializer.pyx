# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True

"""
Deserialize IPC blobs from MemoryPool into Draken vectors.

Reads the binary format produced by ipc_serialize.hpp using typed pointer
arithmetic — no struct module, no json, no Python objects in the hot path.

Fixed-width tags (1..5) are dispatched into the C++ deserialiser in
``src/cpp/ipc_deserialize.cpp`` so the malloc + memcpy of the destination
buffer happens with the GIL released. Dict/string tags (6..10) stay on the
existing Cython path because their Vector internals (codes, packed-dict
DrakenVarBuffer, string arena) need bespoke ownership-transfer factories
to move off the GIL safely; that's deliberately scoped out of this change.

All vectors end up with owned memory (Cython or C++ allocated, freed by the
Vector's dealloc) so there are no lifetime dependencies on the MemoryPool
read buffer after this function returns.
"""

from libc.stdint cimport uint8_t, int32_t, int64_t, uint32_t, uint64_t
from libc.stddef cimport size_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy
from libcpp.vector cimport vector

from opteryx.compiled.structures.memory_pool cimport MemoryPool, ReadResult, CppMemoryPool

from draken.vectors.int64_vector cimport Int64Vector
from draken.vectors.int64_vector cimport from_decoded as int64_from_decoded
from draken.vectors.int64_vector cimport from_packed_dict as int64_from_packed_dict
from draken.vectors.int64_vector cimport make_int64_dict_only
from draken.vectors.float64_vector cimport Float64Vector
from draken.vectors.float64_vector cimport from_decoded as float64_from_decoded
from draken.vectors.float64_vector cimport from_packed_dict as float64_from_packed_dict
from draken.vectors.float64_vector cimport make_float64_dict_only
from draken.vectors.float32_vector cimport Float32Vector
from draken.vectors.float32_vector cimport from_decoded as float32_from_decoded
from draken.vectors.float32_vector cimport from_packed_dict as float32_from_packed_dict
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.bool_vector cimport from_decoded as bool_from_decoded
from draken.vectors.string_vector cimport StringVector, from_packed_dict, from_dict_buffers, make_string_dict_only


cdef extern from "ipc_deserialize.hpp" namespace "opteryx":
    cdef enum IpcKind:
        IpcKind_Int64   "opteryx::IpcKind::Int64"
        IpcKind_Float32 "opteryx::IpcKind::Float32"
        IpcKind_Float64 "opteryx::IpcKind::Float64"
        IpcKind_Bool    "opteryx::IpcKind::Bool"

    cdef struct DecodedFixedColumn:
        IpcKind  kind
        uint32_t num_rows
        void*    data
        uint8_t* null_bitmap
        int      status
        uint8_t  tag

    void deserialize_fixed_column(const uint8_t* data, int64_t length,
                                  DecodedFixedColumn& out) nogil

    void deserialize_row_group_fixed(CppMemoryPool& pool,
                                     const int64_t* ref_ids,
                                     size_t n_cols,
                                     DecodedFixedColumn* out) nogil

# Status codes mirror DeserializeStatus in ipc_deserialize.hpp. Anything
# non-zero except kStatusNotHandled is a hard error; kStatusNotHandled means
# the C++ side identified a dict/string tag and we fall back to Cython.
DEF STATUS_OK           = 0
DEF STATUS_TRUNCATED    = 1
DEF STATUS_OOM          = 2
DEF STATUS_UNKNOWN_TAG  = 3
DEF STATUS_NOT_HANDLED  = 4

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
    """Allocate and memcpy the null bitmap. Caller must guarantee nbytes > 0
    (because Cython's `except NULL` reserves NULL as the exception sentinel,
    so this function may not legitimately return NULL on success)."""
    cdef uint8_t* dst = <uint8_t*>malloc(nbytes)
    if dst == NULL:
        raise MemoryError()
    memcpy(dst, src, nbytes)
    return dst


# Fixed-width column builders (_build_int64, _build_int32, _build_float32,
# _build_float64, _build_bool) used to live here. They have been replaced by
# the C++ implementation in src/cpp/ipc_deserialize.cpp, which performs the
# same malloc + memcpy with the GIL released. The dispatch happens in
# deserialize_column below.


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

    # On ARM, direct pointer cast to int32_t* from unaligned IPC bytes causes SIGBUS.
    # Copy to an aligned buffer before any int32 reads.
    cdef uint32_t* offsets_buf = <uint32_t*>malloc(offsets_count * sizeof(uint32_t))
    if offsets_buf == NULL:
        raise MemoryError()
    memcpy(offsets_buf, p, offsets_count * sizeof(uint32_t))
    p += offsets_count * 4

    cdef int32_t arena_len = <int32_t>offsets_buf[dict_size]  # sentinel value

    try:
        return make_string_dict_only(
            codes_ptr,
            code_width,
            <Py_ssize_t>num_rows,
            offsets_buf,
            p,                    # arena_ptr
            <Py_ssize_t>dict_size,
            <Py_ssize_t>arena_len,
            null_bitmap if null_bitmap_len > 0 else NULL,
        )
    finally:
        free(offsets_buf)


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


cdef inline object _wrap_decoded_fixed(DecodedFixedColumn& dc):
    """Transfer ownership of the malloc'd buffers in `dc` into a Draken Vector.

    Called with the GIL held, after the nogil C++ deserialiser has populated
    `dc`. On any failure between buffer transfer and Vector construction the
    caller's responsibility is to free dc.data/dc.null_bitmap — but
    `from_decoded` only raises on the small `malloc(sizeof(DrakenFixedBuffer))`
    inside, and in that case the Vector has not yet taken ownership, so we
    must release the buffers ourselves.
    """
    cdef object vec
    try:
        if dc.kind == IpcKind_Int64:
            vec = int64_from_decoded(dc.data, dc.null_bitmap, <size_t>dc.num_rows)
        elif dc.kind == IpcKind_Float64:
            vec = float64_from_decoded(dc.data, dc.null_bitmap, <size_t>dc.num_rows)
        elif dc.kind == IpcKind_Float32:
            vec = float32_from_decoded(dc.data, dc.null_bitmap, <size_t>dc.num_rows)
        elif dc.kind == IpcKind_Bool:
            vec = bool_from_decoded(dc.data, dc.null_bitmap, <size_t>dc.num_rows)
        else:
            # Unreachable under normal flow — the C++ side only sets these four kinds.
            free(dc.data)
            free(dc.null_bitmap)
            raise ValueError(f"Unexpected IpcKind from C++ deserialiser: {<int>dc.kind}")
    except:
        # from_decoded raised before taking ownership; release the malloc'd buffers.
        free(dc.data)
        free(dc.null_bitmap)
        raise
    return vec


cpdef object deserialize_column(int64_t ref_id, MemoryPool pool):
    """Deserialize one IPC blob from MemoryPool into a Draken vector.

    Uses the Cython-native pool surface: reads the raw pointer under a latch
    (preventing concurrent compaction from moving the segment), parses
    directly from pool memory with no intermediate ``bytes`` copy, then
    unlatches in a finally block.

    Fixed-width tags (int64, int32→int64, float32, float64, bool) are
    dispatched into the C++ deserialiser, which performs the destination
    malloc + memcpy with the GIL released and returns owned buffers that
    `_wrap_decoded_fixed` slots into a Draken Vector.

    Dict/string tags (6..10) still parse in this Cython function — porting
    them requires ownership-transfer factories for codes/dict_values/arena,
    which is the natural follow-on to this change.
    """
    cdef ReadResult r
    cdef const uint8_t* p
    cdef uint8_t tag
    cdef uint32_t num_rows
    cdef uint32_t null_bitmap_len
    cdef const uint8_t* null_bitmap
    cdef object result
    cdef DecodedFixedColumn dc

    with nogil:
        r = pool.read(ref_id, True)  # latch=True pins the segment

    if r.length == 0:
        with nogil:
            pool.unlatch(ref_id)
        raise ValueError(f"Failed to read ref_id {ref_id} from MemoryPool")

    try:
        p = <const uint8_t*>r.ptr

        # Peek the tag (one byte) so we know whether to dispatch to C++ or
        # take the Cython dict/string path. Reading one byte from latched pool
        # memory is essentially free.
        tag = p[0]

        if (tag == TAG_INT64 or tag == TAG_INT32 or tag == TAG_FLOAT32
                or tag == TAG_FLOAT64 or tag == TAG_BOOL):
            # Fixed-width: full IPC parse + malloc + memcpy happens in C++
            # with the GIL released. The destination buffers come back already
            # owned-by-malloc; we transfer them into a Vector under the GIL.
            with nogil:
                deserialize_fixed_column(<const uint8_t*>r.ptr, r.length, dc)

            if dc.status != STATUS_OK:
                # All non-OK statuses on a fixed-width tag are hard errors —
                # the kStatusNotHandled path is unreachable here because we
                # only call C++ for tags in the fixed-width range.
                free(dc.data)
                free(dc.null_bitmap)
                if dc.status == STATUS_OOM:
                    raise MemoryError()
                raise ValueError(
                    f"C++ IPC deserialise failed: tag={tag} status={dc.status}"
                )
            result = _wrap_decoded_fixed(dc)
        else:
            # Dict / string tags — parse in Cython as before. Advance past the
            # IPC header to the type-specific body.
            p += 1
            p = _read_u32(p, &num_rows)
            p = _read_u32(p, &null_bitmap_len)
            null_bitmap = p
            p += null_bitmap_len

            if tag == TAG_STR_DICT:
                result = _build_string_dict(p, num_rows, null_bitmap, null_bitmap_len)
            elif tag == TAG_STR_PLAIN:
                result = _build_string_plain(p, num_rows, null_bitmap, null_bitmap_len)
            elif tag == TAG_INT64_DICT:
                result = _build_numeric_dict_int64(p, num_rows, null_bitmap, null_bitmap_len)
            elif tag == TAG_FLOAT32_DICT:
                result = _build_numeric_dict_float32(p, num_rows, null_bitmap, null_bitmap_len)
            elif tag == TAG_FLOAT64_DICT:
                result = _build_numeric_dict_float64(p, num_rows, null_bitmap, null_bitmap_len)
            else:
                raise ValueError(f"Unknown IPC type tag: {tag}")
    finally:
        with nogil:
            pool.unlatch(ref_id)

    return result


cpdef dict deserialize_row_group(dict ref_ids, MemoryPool pool):
    """Deserialize all columns for a row group from MemoryPool into Draken vectors.

    Fixed-width columns (tags 1..5) are deserialised in a single batched C++
    call that performs pool.read/parse/malloc/memcpy/unlatch for every column
    in one nogil window — collapsing per-column GIL transitions from O(n) to
    O(1) per row group.

    Dict/string columns (tags 6..10) come back as ``kStatusNotHandled`` from
    the batched driver (which unlatches them) and are then routed through the
    existing single-column ``deserialize_column`` path, which re-latches under
    the pool's internal mutex and parses in Cython.
    """
    cdef Py_ssize_t n = len(ref_ids)
    cdef dict row_group = {}
    if n == 0:
        return row_group

    cdef list names = list(ref_ids.keys())
    cdef vector[int64_t] refs
    cdef DecodedFixedColumn* outs
    cdef Py_ssize_t i
    cdef str col_name
    cdef int64_t ref_id
    cdef int status
    cdef object vec

    refs.reserve(n)
    for r in ref_ids.values():
        refs.push_back(<int64_t>r)

    outs = <DecodedFixedColumn*>malloc(<size_t>n * sizeof(DecodedFixedColumn))
    if outs == NULL:
        raise MemoryError()

    try:
        with nogil:
            deserialize_row_group_fixed(
                pool._pool[0], refs.data(), <size_t>n, outs
            )

        for i in range(n):
            col_name = names[i]
            ref_id = refs[i]
            status = outs[i].status

            if status == STATUS_OK:
                vec = _wrap_decoded_fixed(outs[i])
            elif status == STATUS_NOT_HANDLED:
                # Dict / string tag — fall back to the Cython per-column path
                # which re-latches the (still-pinned-by-its-own-ref) segment.
                vec = deserialize_column(ref_id, pool)
            elif status == STATUS_OOM:
                raise MemoryError()
            else:
                raise ValueError(
                    f"C++ batched IPC deserialise failed: "
                    f"ref={ref_id} status={status} tag={outs[i].tag}"
                )

            row_group[col_name] = vec
            with nogil:
                pool.release(ref_id)
    finally:
        free(outs)

    return row_group
