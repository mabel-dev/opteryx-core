# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""
Deserialize IPC blobs from MemoryPool into Draken vectors.

Reads the binary format produced by ipc_serialize.hpp using typed pointer
arithmetic — no struct module, no json, no Python objects in the hot path.

Fixed-width tags (1..5) are dispatched into the C++ deserialiser in
``src/cpp/ipc_deserialize.cpp`` so the malloc + memcpy of the destination
buffer happens with the GIL released. Dict/string tags (6..10) stay on the
existing Cython path because their construction requires building slot arrays
and arenas; that work stays in Cython.

All vectors end up with owned memory (draken_malloc-allocated, freed by the
Vector's dealloc via mimalloc) so there are no lifetime dependencies on the
MemoryPool read buffer after this function returns.

Note: draken_vector_own_array does not support per-element child nullability.
Child null bitmaps in the IPC stream are read and discarded; child elements
are treated as fully valid.
"""

from libc.stdint cimport uint8_t, uint16_t, int32_t, int64_t, uint32_t, uint64_t
from libc.stddef cimport size_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, memset
from libcpp.vector cimport vector
from cpython.object cimport PyObject

from opteryx.compiled.structures.memory_pool cimport MemoryPool, ReadResult, CppMemoryPool

from draken.core.buffers cimport DrakenVector, DrakenType
from draken.core.buffers cimport DRAKEN_INT32, DRAKEN_INT64, DRAKEN_FLOAT32, DRAKEN_FLOAT64, DRAKEN_BOOL, DRAKEN_VARCHAR, DRAKEN_DECIMAL128
from draken.core.buffers cimport DRAKEN_UINT8, DRAKEN_UINT16, DRAKEN_UINT32, DRAKEN_UINT64
from draken.core.buffers cimport DRAKEN_INT8, DRAKEN_INT16
from draken.vectors.vector cimport Vector, from_decoded as _vector_from_decoded
from draken.vectors.vector cimport dict_int64_from_decoded as _dict_i64_from_decoded
from draken.vectors.vector cimport dict_from_decoded as _dict_from_decoded

cdef extern from "core/alloc.h" nogil:
    void* draken_malloc(size_t n) nogil
    void  draken_free(void* p) nogil

cdef extern from "core/string_slot.h" nogil:
    ctypedef struct DrakenStringSlot:
        pass   # opaque 16-byte slot
    void draken_build_string_slot(DrakenStringSlot* slot, const uint8_t* bytes,
                                  uint32_t length, uint32_t arena_offset) nogil
    void str_init_null(DrakenStringSlot* slot) nogil

# Inline Py_DECREF helper — mirrors _vec_shim_decref in _vector_shim.pyx.
cdef extern from *:
    """static inline void _cd_decref(PyObject* op) { Py_DECREF(op); }"""
    void _cd_decref(PyObject* op)

cdef extern from "core/draken_bridge.h":
    const DrakenVector* draken_vector_unwrap(PyObject* obj)
    int draken_vector_mark_dict_sorted(PyObject* obj)
    PyObject* draken_vector_own_string(
        DrakenStringSlot* slots, uint8_t* arena, size_t arena_len,
        uint8_t* validity, uint32_t length, DrakenType vec_type,
        const uint64_t* keyhash)
    PyObject* draken_vector_own_string_dict(
        DrakenStringSlot* slots, uint8_t* arena, size_t arena_len,
        uint32_t* codes, uint32_t data_length,
        uint8_t* validity, uint32_t length, DrakenType vec_type,
        const uint64_t* keyhash)
    PyObject* draken_vector_own_array(
        int32_t* parent_offsets, DrakenStringSlot* child_slots,
        uint8_t* child_arena, size_t child_arena_len,
        uint32_t child_length, DrakenType child_type,
        uint8_t* child_validity,
        uint8_t* parent_validity, uint32_t length)
    PyObject* draken_vector_own_array_numeric(
        int32_t* parent_offsets, void* child_data,
        uint8_t* child_validity, uint32_t child_length,
        DrakenType child_type,
        uint8_t* parent_validity, uint32_t length)
    PyObject* draken_vector_own_array_child(
        int32_t* parent_offsets, PyObject* child_obj,
        uint8_t* parent_validity, uint32_t length)


cdef extern from "ipc_deserialize.hpp" namespace "opteryx":
    cdef enum IpcKind:
        IpcKind_Int64   "opteryx::IpcKind::Int64"
        IpcKind_Float32 "opteryx::IpcKind::Float32"
        IpcKind_Float64 "opteryx::IpcKind::Float64"
        IpcKind_Bool    "opteryx::IpcKind::Bool"
        IpcKind_Int128  "opteryx::IpcKind::Int128"

    cdef struct DecodedFixedColumn:
        IpcKind  kind
        uint32_t num_rows
        void*    data
        uint32_t data_len   # compact payload bytes (K * element_size, K <= num_rows)
        uint8_t* null_bitmap
        int      status
        uint8_t  tag
        uint8_t  decimal_precision   # DECIMAL128 only
        uint8_t  decimal_scale       # DECIMAL128 only

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
DEF TAG_ARRAY       = 11
DEF TAG_INT128      = 12
# E33 — unsigned, plain (exact declared width, never widened) and dict.
DEF TAG_UINT8       = 13
DEF TAG_UINT16      = 14
DEF TAG_UINT32      = 15
DEF TAG_UINT64      = 16
DEF TAG_UINT8_DICT  = 17
DEF TAG_UINT16_DICT = 18
DEF TAG_UINT32_DICT = 19
DEF TAG_UINT64_DICT = 20
# E33 — signed narrow, plain (exact declared width, never widened) and dict.
# TAG_INT32_EXACT is distinct from TAG_INT32 (2), which is the legacy widening
# tag still used for int32-backed DECIMAL.
DEF TAG_INT8        = 21
DEF TAG_INT16       = 22
DEF TAG_INT32_EXACT = 23
DEF TAG_INT8_DICT   = 24
DEF TAG_INT16_DICT  = 25
DEF TAG_INT32_DICT  = 26

# ARRAY child element type tags — must match CHILD_* in ipc_serialize.hpp.
DEF CHILD_INT64   = 1
DEF CHILD_INT32   = 2
DEF CHILD_FLOAT32 = 3
DEF CHILD_FLOAT64 = 4
DEF CHILD_BOOL    = 5
DEF CHILD_STRING  = 6
DEF CHILD_UINT64  = 7   # 64-bit unsigned leaf
DEF CHILD_ARRAY   = 8   # nested list child (recursive block)
# Narrow integer leaves — parquet carries these as physical int32 plus an
# INTEGER(bitWidth, isSigned) annotation, so they need tags of their own or the
# declared width is lost on the wire. Appended; 1..8 never renumber.
DEF CHILD_INT8    = 9
DEF CHILD_INT16   = 10
DEF CHILD_UINT8   = 11
DEF CHILD_UINT16  = 12
DEF CHILD_UINT32  = 13

# sizeof(DrakenStringSlot) == 16 always (16-byte fixed-width slot, documented in string_slot.h).
DEF SLOT_BYTES = 16


cdef inline const uint8_t* _read_u32(const uint8_t* p, uint32_t* out) noexcept nogil:
    out[0] = ((<uint32_t>p[0])       |
              (<uint32_t>p[1] <<  8)  |
              (<uint32_t>p[2] << 16)  |
              (<uint32_t>p[3] << 24))
    return p + 4


# ─── Helpers ─────────────────────────────────────────────────────────────────

cdef inline uint8_t* _copy_validity(const uint8_t* src, uint32_t nbytes) except NULL:
    """draken_malloc + memcpy a null-bitmap. nbytes must be > 0."""
    cdef uint8_t* dst = <uint8_t*>draken_malloc(nbytes)
    if dst == NULL:
        raise MemoryError()
    memcpy(dst, src, nbytes)
    return dst


cdef inline Vector _wrap_raw_pyobj(PyObject* raw):
    """Transfer a NEW PyObject* reference into a Vector shim."""
    cdef Vector result = Vector.__new__(Vector)
    result._nb = <object>raw   # Cython incref → refcount = 2
    _cd_decref(raw)            # balance the NEW ref   → refcount = 1
    result._dv = draken_vector_unwrap(raw)
    return result


# ─── Fixed-column wrapper ─────────────────────────────────────────────────────

cdef inline Vector _wrap_decoded_bool(DecodedFixedColumn& dc):
    """Wrap a TAG_BOOL fixed column into a bit-packed DRAKEN_BOOL Vector.

    The IPC payload carries K present (non-null) values bit-packed 1 bit/value
    LSB-first (rugo's serialize_bool). DRAKEN_BOOL wants a POSITIONAL bit-packed
    buffer of (num_rows + 7) // 8 bytes with row i at bit i.

      Non-nullable: K == num_rows, so the compact payload is already positional —
                    wrap dc.data directly.
      Nullable:     K < num_rows; scatter the K present bits to their row
                    positions (null rows stay 0; the validity bitmap masks them).
    """
    cdef uint32_t num_rows = dc.num_rows
    cdef uint32_t pos_bytes = (num_rows + 7) >> 3
    cdef void*    pos_data
    cdef uint8_t* src
    cdef uint8_t* dst
    cdef uint32_t row_i
    cdef uint32_t compact_i

    if dc.null_bitmap == NULL or num_rows == 0:
        # Non-nullable (or empty): compact payload == positional payload.
        return _vector_from_decoded(dc.data, dc.null_bitmap, num_rows, DRAKEN_BOOL)

    pos_data = draken_malloc(<size_t>(pos_bytes if pos_bytes > 0 else 1))
    if pos_data == NULL:
        draken_free(dc.data)
        draken_free(dc.null_bitmap)
        raise MemoryError()
    memset(<uint8_t*>pos_data, 0, <size_t>(pos_bytes if pos_bytes > 0 else 1))

    src = <uint8_t*>dc.data   # may be NULL when every row is null (K == 0)
    dst = <uint8_t*>pos_data
    compact_i = 0
    if src != NULL:
        for row_i in range(num_rows):
            if (dc.null_bitmap[row_i >> 3] >> (row_i & 7)) & 1:
                if (src[compact_i >> 3] >> (compact_i & 7)) & 1:
                    dst[row_i >> 3] |= <uint8_t>(1 << (row_i & 7))
                compact_i += 1
    draken_free(dc.data)
    return _vector_from_decoded(pos_data, dc.null_bitmap, num_rows, DRAKEN_BOOL)


cdef inline Vector _wrap_decoded_fixed(DecodedFixedColumn& dc):
    """Transfer ownership of draken_malloc'd buffers in dc into a new Vector.

    Parquet omits null rows from the value stream, so dc.data may be a COMPACT
    buffer of K present values (K <= num_rows). The Draken model requires a
    POSITIONAL buffer of num_rows slots (row i at slot i), with null slots holding
    zero. When dc.null_bitmap is set and data_len < num_rows * element_size we
    scatter the compact values to their row positions here, at the Draken boundary.
    Bool columns are bit-packed (byte count doesn't map 1:1 to rows) and need a
    bit-granular scatter, so they are handled separately by _wrap_decoded_bool.
    """
    cdef DrakenType dtype
    cdef uint32_t elem_size
    cdef bint is_decimal128 = False
    cdef Vector v128s
    cdef Vector vout
    if dc.kind == IpcKind_Int64:
        dtype = DRAKEN_INT64
        elem_size = 8
    elif dc.kind == IpcKind_Float64:
        dtype = DRAKEN_FLOAT64
        elem_size = 8
    elif dc.kind == IpcKind_Float32:
        dtype = DRAKEN_FLOAT32
        elem_size = 4
    elif dc.kind == IpcKind_Bool:
        # Bit-packed; the byte count doesn't map 1:1 to rows, so the generic
        # byte-granular scatter below cannot serve it. Handle it bit-wise.
        return _wrap_decoded_bool(dc)
    elif dc.kind == IpcKind_Int128:
        # DECIMAL128: 16-byte __int128 per slot. Scatter (compact→positional) handled
        # below. After building the vector we attach the (precision, scale) descriptor
        # so it emerges as a properly-typed DRAKEN_DECIMAL128 with a LogicalType.
        dtype = DRAKEN_DECIMAL128
        elem_size = 16
        is_decimal128 = True
    else:
        draken_free(dc.data)
        draken_free(dc.null_bitmap)
        raise ValueError(f"Unexpected IpcKind from C++ deserialiser: {<int>dc.kind}")

    # Scatter compact → positional when the buffer is shorter than num_rows * elem_size.
    # This happens for nullable plain-encoded columns (Parquet stores K < N values).
    cdef uint32_t full_bytes = dc.num_rows * elem_size
    cdef void*    pos_data
    cdef uint8_t* src
    cdef uint8_t* dst
    cdef uint32_t row_i
    cdef uint32_t compact_i
    cdef uint8_t  bit

    if (elem_size > 0
            and dc.null_bitmap != NULL
            and dc.data_len < full_bytes
            and dc.num_rows > 0):
        # Allocate a full num_rows * elem_size positional buffer, zero-filled.
        pos_data = draken_malloc(<size_t>full_bytes)
        if pos_data == NULL:
            draken_free(dc.data)
            draken_free(dc.null_bitmap)
            raise MemoryError()
        memset(<uint8_t*>pos_data, 0, <size_t>full_bytes)
        # Scatter: walk every row, copy the next compact value when the row is valid.
        src = <uint8_t*>dc.data
        dst = <uint8_t*>pos_data
        compact_i = 0
        for row_i in range(dc.num_rows):
            bit = (dc.null_bitmap[row_i >> 3] >> (row_i & 7)) & 1
            if bit:
                memcpy(dst + row_i * elem_size,
                       src + compact_i * elem_size,
                       elem_size)
                compact_i += 1
        draken_free(dc.data)
        v128s = _vector_from_decoded(pos_data, dc.null_bitmap, dc.num_rows, dtype)
        if is_decimal128 and dc.num_rows > 0:
            v128s._nb.set_decimal_descriptor(dc.decimal_precision, dc.decimal_scale)
        return v128s

    vout = _vector_from_decoded(dc.data, dc.null_bitmap, dc.num_rows, dtype)
    if is_decimal128 and dc.num_rows > 0:
        vout._nb.set_decimal_descriptor(dc.decimal_precision, dc.decimal_scale)
    return vout


# ─── Numeric dict builders ────────────────────────────────────────────────────

cdef object _build_numeric_dict_int64(const uint8_t* p, uint32_t num_rows,
                                       const uint8_t* null_bitmap, uint32_t null_bitmap_len):
    """Deserialize TAG_INT64_DICT preserving dict encoding via dict_int64_from_decoded."""
    cdef uint32_t dict_size
    p = _read_u32(p, &dict_size)
    cdef uint8_t code_width = p[0]
    p += 1
    cdef uint8_t is_sorted = p[0]   # sorted-dictionary hint
    p += 1
    cdef uint32_t codes_len
    p = _read_u32(p, &codes_len)
    cdef const uint8_t* codes_ptr = p
    p += codes_len
    cdef uint32_t values_len
    p = _read_u32(p, &values_len)
    # Keep dict_src as uint8_t* — the buffer is byte-stream data with no
    # guaranteed 8-byte alignment.  Casting to int64_t* triggers UBSAN
    # "misaligned load"; memcpy handles alignment correctly regardless.
    cdef const uint8_t* dict_src = p

    # draken_malloc dict values — transferred to _dict_i64_from_decoded
    cdef void* dict_vals = draken_malloc(<size_t>dict_size * sizeof(int64_t))
    if dict_vals == NULL:
        raise MemoryError()
    memcpy(dict_vals, dict_src, <size_t>dict_size * sizeof(int64_t))

    # draken_malloc uint32_t codes (expand from variable-width packed) — transferred
    cdef uint32_t* codes_buf = <uint32_t*>draken_malloc(<size_t>num_rows * sizeof(uint32_t))
    if codes_buf == NULL:
        draken_free(dict_vals)
        raise MemoryError()

    # Codes are packed bytes with no alignment guarantee — use memcpy to read
    # each code to avoid UBSAN "misaligned load" on the uint16/uint32 casts.
    cdef uint32_t i
    cdef uint16_t tmp16
    cdef uint32_t tmp32
    if code_width == 1:
        for i in range(num_rows):
            codes_buf[i] = <uint32_t>codes_ptr[i]
    elif code_width == 2:
        for i in range(num_rows):
            memcpy(&tmp16, codes_ptr + i * 2, 2)
            codes_buf[i] = <uint32_t>tmp16
    else:  # code_width == 4
        for i in range(num_rows):
            memcpy(&tmp32, codes_ptr + i * 4, 4)
            codes_buf[i] = tmp32

    # draken_malloc validity — transferred to _dict_i64_from_decoded
    cdef uint8_t* validity_buf = NULL
    if null_bitmap_len > 0:
        validity_buf = <uint8_t*>draken_malloc(null_bitmap_len)
        if validity_buf == NULL:
            draken_free(codes_buf)
            draken_free(dict_vals)
            raise MemoryError()
        memcpy(validity_buf, null_bitmap, null_bitmap_len)

    # All three buffers are now draken_malloc'd; ownership transferred on call.
    cdef Vector _v = _dict_i64_from_decoded(dict_vals, dict_size, codes_buf, num_rows, validity_buf)
    if is_sorted:
        draken_vector_mark_dict_sorted(<PyObject*>_v._nb)
    return _v


cdef object _build_numeric_dict_float32(const uint8_t* p, uint32_t num_rows,
                                         const uint8_t* null_bitmap, uint32_t null_bitmap_len):
    """Deserialize TAG_FLOAT32_DICT — expand to dense (no float dict bridge exists)."""
    cdef uint32_t dict_size
    p = _read_u32(p, &dict_size)
    cdef uint8_t code_width = p[0]
    p += 1
    p += 1   # is_sorted byte: float dicts densify, hint unused (skip to stay in sync)
    cdef uint32_t codes_len
    p = _read_u32(p, &codes_len)
    cdef const uint8_t* codes_ptr = p
    p += codes_len
    cdef uint32_t values_len
    p = _read_u32(p, &values_len)
    # Keep dict_src as uint8_t* — byte-stream with no guaranteed 4-byte alignment.
    # Use memcpy to extract each float value to avoid UBSAN misaligned load.
    cdef const uint8_t* dict_src = p

    cdef void* expanded = draken_malloc(<size_t>num_rows * sizeof(float))
    if expanded == NULL:
        raise MemoryError()
    cdef float* dst = <float*>expanded
    cdef uint32_t i
    cdef uint16_t tmp16
    cdef uint32_t tmp32
    cdef float ftmp
    if code_width == 1:
        for i in range(num_rows):
            memcpy(&ftmp, dict_src + <size_t>codes_ptr[i] * 4, 4)
            dst[i] = ftmp
    elif code_width == 2:
        for i in range(num_rows):
            memcpy(&tmp16, codes_ptr + i * 2, 2)
            memcpy(&ftmp, dict_src + <size_t>tmp16 * 4, 4)
            dst[i] = ftmp
    else:
        for i in range(num_rows):
            memcpy(&tmp32, codes_ptr + i * 4, 4)
            memcpy(&ftmp, dict_src + <size_t>tmp32 * 4, 4)
            dst[i] = ftmp

    cdef uint8_t* validity_buf = NULL
    if null_bitmap_len > 0:
        validity_buf = <uint8_t*>draken_malloc(null_bitmap_len)
        if validity_buf == NULL:
            draken_free(expanded)
            raise MemoryError()
        memcpy(validity_buf, null_bitmap, null_bitmap_len)

    return _vector_from_decoded(expanded, validity_buf, num_rows, DRAKEN_FLOAT32)


cdef object _build_numeric_dict_float64(const uint8_t* p, uint32_t num_rows,
                                         const uint8_t* null_bitmap, uint32_t null_bitmap_len):
    """Deserialize TAG_FLOAT64_DICT — expand to dense (no float dict bridge exists)."""
    cdef uint32_t dict_size
    p = _read_u32(p, &dict_size)
    cdef uint8_t code_width = p[0]
    p += 1
    p += 1   # is_sorted byte: float dicts densify, hint unused (skip to stay in sync)
    cdef uint32_t codes_len
    p = _read_u32(p, &codes_len)
    cdef const uint8_t* codes_ptr = p
    p += codes_len
    cdef uint32_t values_len
    p = _read_u32(p, &values_len)
    # Keep dict_src as uint8_t* — byte-stream with no guaranteed 8-byte alignment.
    # Use memcpy to extract each double value to avoid UBSAN misaligned load.
    cdef const uint8_t* dict_src = p

    cdef void* expanded = draken_malloc(<size_t>num_rows * sizeof(double))
    if expanded == NULL:
        raise MemoryError()
    cdef double* dst = <double*>expanded
    cdef uint32_t i
    cdef uint16_t tmp16
    cdef uint32_t tmp32
    cdef double dtmp
    if code_width == 1:
        for i in range(num_rows):
            memcpy(&dtmp, dict_src + <size_t>codes_ptr[i] * 8, 8)
            dst[i] = dtmp
    elif code_width == 2:
        for i in range(num_rows):
            memcpy(&tmp16, codes_ptr + i * 2, 2)
            memcpy(&dtmp, dict_src + <size_t>tmp16 * 8, 8)
            dst[i] = dtmp
    else:
        for i in range(num_rows):
            memcpy(&tmp32, codes_ptr + i * 4, 4)
            memcpy(&dtmp, dict_src + <size_t>tmp32 * 8, 8)
            dst[i] = dtmp

    cdef uint8_t* validity_buf = NULL
    if null_bitmap_len > 0:
        validity_buf = <uint8_t*>draken_malloc(null_bitmap_len)
        if validity_buf == NULL:
            draken_free(expanded)
            raise MemoryError()
        memcpy(validity_buf, null_bitmap, null_bitmap_len)

    return _vector_from_decoded(expanded, validity_buf, num_rows, DRAKEN_FLOAT64)


# ─── E33: exact-width integer builders ───────────────────────────────────────
# Unsigned plain (13-16) / dict (17-20) and signed narrow plain (21-23) / dict
# (24-26) tags are not routed through the C++ fast path (ipc_deserialize.hpp only
# knows tags 1-5); they're new enough tags that adding them there is deferred,
# mirroring how dict/string tags 6-10 already stay on this Cython path.
# elem_bytes (1/2/4/8) always matches the declared width exactly — these never
# widen, unlike the int32->int64 convention the legacy builders above use.
#
# Both builders are parameterized by (elem_bytes, dtype) and are signedness-
# agnostic: the bytes on the wire are already the value at the declared width in
# whichever domain the column is, so only the DrakenType tag differs.

cdef object _build_numeric_plain_narrow(const uint8_t* p, uint32_t num_rows,
                                      const uint8_t* null_bitmap, uint32_t null_bitmap_len,
                                      int elem_bytes, DrakenType dtype):
    """Deserialize an exact-width plain column (uint8/16/32/64, int8/16/32),
    scattering compact->positional
    exactly like _wrap_decoded_fixed does for the C++ fast-path tags."""
    cdef uint32_t data_len
    p = _read_u32(p, &data_len)
    cdef uint32_t full_bytes = num_rows * <uint32_t>elem_bytes
    cdef void* pos_data = draken_malloc(<size_t>(full_bytes if full_bytes > 0 else 1))
    if pos_data == NULL:
        raise MemoryError()

    cdef uint8_t* dst = <uint8_t*>pos_data
    cdef uint32_t row_i
    cdef uint32_t compact_i
    cdef uint8_t bit
    if null_bitmap_len > 0 and data_len < full_bytes and num_rows > 0:
        memset(dst, 0, <size_t>full_bytes)
        compact_i = 0
        for row_i in range(num_rows):
            bit = (null_bitmap[row_i >> 3] >> (row_i & 7)) & 1
            if bit:
                memcpy(dst + row_i * elem_bytes, p + <size_t>compact_i * elem_bytes, elem_bytes)
                compact_i += 1
    elif full_bytes > 0:
        memcpy(dst, p, <size_t>full_bytes)

    cdef uint8_t* validity_buf = NULL
    if null_bitmap_len > 0:
        validity_buf = <uint8_t*>draken_malloc(null_bitmap_len)
        if validity_buf == NULL:
            draken_free(pos_data)
            raise MemoryError()
        memcpy(validity_buf, null_bitmap, null_bitmap_len)

    return _vector_from_decoded(pos_data, validity_buf, num_rows, dtype)


cdef object _build_numeric_dict_narrow(const uint8_t* p, uint32_t num_rows,
                                     const uint8_t* null_bitmap, uint32_t null_bitmap_len,
                                     int elem_bytes, DrakenType dtype):
    """Deserialize an exact-width dict column (uint8/16/32/64, int8/16/32),
    preserving dict encoding via
    dict_from_decoded (generic analogue of _build_numeric_dict_int64)."""
    cdef uint32_t dict_size
    p = _read_u32(p, &dict_size)
    cdef uint8_t code_width = p[0]
    p += 1
    cdef uint8_t is_sorted = p[0]
    p += 1
    cdef uint32_t codes_len
    p = _read_u32(p, &codes_len)
    cdef const uint8_t* codes_ptr = p
    p += codes_len
    cdef uint32_t values_len
    p = _read_u32(p, &values_len)
    cdef const uint8_t* dict_src = p

    cdef void* dict_vals = draken_malloc(<size_t>dict_size * <size_t>elem_bytes)
    if dict_vals == NULL:
        raise MemoryError()
    memcpy(dict_vals, dict_src, <size_t>dict_size * <size_t>elem_bytes)

    cdef uint32_t* codes_buf = <uint32_t*>draken_malloc(<size_t>num_rows * sizeof(uint32_t))
    if codes_buf == NULL:
        draken_free(dict_vals)
        raise MemoryError()

    cdef uint32_t i
    cdef uint16_t tmp16
    cdef uint32_t tmp32
    if code_width == 1:
        for i in range(num_rows):
            codes_buf[i] = <uint32_t>codes_ptr[i]
    elif code_width == 2:
        for i in range(num_rows):
            memcpy(&tmp16, codes_ptr + i * 2, 2)
            codes_buf[i] = <uint32_t>tmp16
    else:
        for i in range(num_rows):
            memcpy(&tmp32, codes_ptr + i * 4, 4)
            codes_buf[i] = tmp32

    cdef uint8_t* validity_buf = NULL
    if null_bitmap_len > 0:
        validity_buf = <uint8_t*>draken_malloc(null_bitmap_len)
        if validity_buf == NULL:
            draken_free(codes_buf)
            draken_free(dict_vals)
            raise MemoryError()
        memcpy(validity_buf, null_bitmap, null_bitmap_len)

    cdef Vector _v = _dict_from_decoded(dict_vals, dict_size, codes_buf, num_rows, validity_buf, dtype)
    if is_sorted:
        draken_vector_mark_dict_sorted(<PyObject*>_v._nb)
    return _v


# ─── String builders ──────────────────────────────────────────────────────────

cdef object _build_string_dict(const uint8_t* p, uint32_t num_rows,
                                const uint8_t* null_bitmap, uint32_t null_bitmap_len,
                                DrakenType want_type=DRAKEN_VARCHAR):
    """Deserialize TAG_STR_DICT into a Vector tagged `want_type` (VARCHAR/NVARCHAR/
    VARBINARY — the schema's declared physical type; all three share this exact
    byte layout) using DrakenStringSlot."""
    cdef uint32_t dict_size
    p = _read_u32(p, &dict_size)

    cdef uint8_t code_width = p[0]
    p += 1
    cdef uint8_t is_sorted = p[0]   # sorted-dictionary hint
    p += 1

    cdef uint32_t codes_len
    p = _read_u32(p, &codes_len)
    cdef const uint8_t* codes_ptr = p
    p += codes_len

    cdef uint32_t offsets_count
    p = _read_u32(p, &offsets_count)

    # Copy offsets to aligned temp buffer (ARM SIGBUS if unaligned access).
    # This is a temporary buffer — free with stdlib free after use.
    cdef uint32_t* offsets_buf = <uint32_t*>malloc(offsets_count * sizeof(uint32_t))
    if offsets_buf == NULL:
        raise MemoryError()
    memcpy(offsets_buf, p, offsets_count * sizeof(uint32_t))
    p += offsets_count * 4

    cdef uint32_t arena_len = offsets_buf[dict_size]   # sentinel value = total arena bytes
    cdef const uint8_t* arena_src = p                  # points into IPC buffer (not owned)

    # ---- Build the compact value array (K unique slots) + codes selection ----
    # K = dict_size unique values; rows reference them through the codes selection,
    # exactly the encoding the IPC stream carries. Materializing one slot per row
    # would discard that compactness — downstream access is the uniform
    # value_array[selection[i]] path either way (data_length < length is just an
    # observable property, not a separate vector type).
    cdef uint32_t k, row, s_off, s_off_end, slen
    cdef uint16_t tmp16
    cdef uint32_t tmp32
    cdef DrakenStringSlot* slot_ptr
    cdef uint8_t* slots_buf
    cdef uint8_t* arena_buf
    cdef uint8_t* validity_buf = NULL
    cdef uint32_t* codes_buf
    cdef size_t arena_alloc
    cdef PyObject* raw

    # Edge: no unique values (empty or all-null column). Keep this corner on the
    # proven dense path — num_rows null slots, identity selection.
    if dict_size == 0:
        slots_buf = <uint8_t*>draken_malloc(<size_t>(num_rows if num_rows > 0 else 1) * SLOT_BYTES)
        if slots_buf == NULL:
            free(offsets_buf)
            raise MemoryError()
        for row in range(num_rows):
            str_init_null(<DrakenStringSlot*>(slots_buf + <size_t>row * SLOT_BYTES))
        free(offsets_buf)
        if null_bitmap_len > 0:
            validity_buf = <uint8_t*>draken_malloc(null_bitmap_len)
            if validity_buf == NULL:
                draken_free(slots_buf)
                raise MemoryError()
            memcpy(validity_buf, null_bitmap, null_bitmap_len)
        raw = draken_vector_own_string(
            <DrakenStringSlot*>slots_buf, NULL, 0,
            validity_buf, num_rows, want_type, NULL)  # E37: deserialize path = future work
        if raw == NULL:
            raise MemoryError("draken_vector_own_string failed")
        return _wrap_raw_pyobj(raw)

    # Value array: one slot per unique value (draken_malloc'd; transferred on call).
    slots_buf = <uint8_t*>draken_malloc(<size_t>dict_size * SLOT_BYTES)
    if slots_buf == NULL:
        free(offsets_buf)
        raise MemoryError()
    for k in range(dict_size):
        slot_ptr = <DrakenStringSlot*>(slots_buf + <size_t>k * SLOT_BYTES)
        s_off     = offsets_buf[k]
        s_off_end = offsets_buf[k + 1]
        slen      = s_off_end - s_off
        draken_build_string_slot(slot_ptr, arena_src + s_off, slen, s_off)

    # Arena copy backing the long-form unique slots (draken_malloc'd; transferred).
    arena_alloc = <size_t>arena_len if arena_len > 0 else 1
    arena_buf = <uint8_t*>draken_malloc(arena_alloc)
    if arena_buf == NULL:
        draken_free(slots_buf)
        free(offsets_buf)
        raise MemoryError()
    if arena_len > 0:
        memcpy(arena_buf, arena_src, arena_len)

    # Codes selection: one uint32 per row (draken_malloc'd; transferred on call).
    # Null rows take code 0 — the validity bitmap masks them.
    codes_buf = <uint32_t*>draken_malloc(<size_t>(num_rows if num_rows > 0 else 1) * sizeof(uint32_t))
    if codes_buf == NULL:
        draken_free(arena_buf)
        draken_free(slots_buf)
        free(offsets_buf)
        raise MemoryError()
    for row in range(num_rows):
        if null_bitmap_len > 0 and not ((null_bitmap[row >> 3] >> (row & 7)) & 1):
            codes_buf[row] = 0
        elif code_width == 1:
            codes_buf[row] = <uint32_t>codes_ptr[row]
        elif code_width == 2:
            memcpy(&tmp16, codes_ptr + row * 2, 2)
            codes_buf[row] = <uint32_t>tmp16
        else:
            memcpy(&tmp32, codes_ptr + row * 4, 4)
            codes_buf[row] = tmp32

    free(offsets_buf)

    # Validity (draken_malloc'd; transferred on call).
    if null_bitmap_len > 0:
        validity_buf = <uint8_t*>draken_malloc(null_bitmap_len)
        if validity_buf == NULL:
            draken_free(codes_buf)
            draken_free(arena_buf)
            draken_free(slots_buf)
            raise MemoryError()
        memcpy(validity_buf, null_bitmap, null_bitmap_len)

    # All four draken_malloc'd buffers are transferred on call (even on failure).
    raw = draken_vector_own_string_dict(
        <DrakenStringSlot*>slots_buf,
        arena_buf, <size_t>arena_len,
        codes_buf, dict_size,
        validity_buf, num_rows, want_type, NULL)  # E37: deserialize path = future work
    if raw == NULL:
        raise MemoryError("draken_vector_own_string_dict failed")
    if is_sorted:
        draken_vector_mark_dict_sorted(raw)
    return _wrap_raw_pyobj(raw)


cdef object _build_string_plain(const uint8_t* p, uint32_t num_rows,
                                 const uint8_t* null_bitmap, uint32_t null_bitmap_len,
                                 DrakenType want_type=DRAKEN_VARCHAR):
    """Deserialize TAG_STR_PLAIN (one length-prefixed string per row) into a Vector
    tagged `want_type` (VARCHAR/NVARCHAR/VARBINARY — same byte layout, the schema's
    declared physical type)."""
    cdef uint32_t n
    cdef PyObject* raw
    p = _read_u32(p, &n)

    # The `n` records in the stream are COMPACT (present-only) — Parquet omits null
    # rows from the value stream, so a null row contributes no record. The Draken
    # vector is POSITIONAL: one slot per logical row (row i at slot i), null rows
    # holding an init-null slot masked by the validity bitmap. So the output length
    # is num_rows, NOT n; we scatter the n present records to their row positions
    # using the null bitmap — exactly like _wrap_decoded_fixed does for numerics.
    # (Treating n as the length dropped every null row: all-null → 0 rows, and a
    # partially-null plain column silently lost its null rows.)
    if num_rows == 0:
        # Empty column — no slots needed.
        raw = draken_vector_own_string(NULL, NULL, 0, NULL, 0, want_type, NULL)
        if raw == NULL:
            raise MemoryError("draken_vector_own_string failed (empty)")
        return _wrap_raw_pyobj(raw)

    # First pass: compute total arena size. Only long strings (len > 12) live in
    # the arena — inline strings (len <= STR_INLINE_MAX=12) are stored in the slot
    # and never reference the arena. Sizing from all lengths over-allocates and
    # forces every later dict-preserving take/slice to re-copy the dead bytes.
    cdef const uint8_t* scan = p
    cdef uint32_t slen, i, total_arena = 0
    for i in range(n):
        scan = _read_u32(scan, &slen)
        if slen > 12:
            total_arena += slen
        scan += slen

    # Allocate draken_malloc'd buffers — all transferred to draken_vector_own_string.
    cdef uint8_t* slots_buf = <uint8_t*>draken_malloc(<size_t>num_rows * SLOT_BYTES)
    if slots_buf == NULL:
        raise MemoryError()

    cdef size_t arena_alloc = <size_t>total_arena if total_arena > 0 else 1
    cdef uint8_t* arena_buf = <uint8_t*>draken_malloc(arena_alloc)
    if arena_buf == NULL:
        draken_free(slots_buf)
        raise MemoryError()

    cdef uint8_t* validity_buf = NULL
    if null_bitmap_len > 0:
        validity_buf = <uint8_t*>draken_malloc(null_bitmap_len)
        if validity_buf == NULL:
            draken_free(arena_buf)
            draken_free(slots_buf)
            raise MemoryError()
        memcpy(validity_buf, null_bitmap, null_bitmap_len)

    # Second pass: scatter present records into positional slots. Null rows get an
    # init-null slot and consume NO record from the (compact) stream; present rows
    # consume the next record. With no null bitmap the column is non-nullable, so
    # every row is present and n == num_rows (a straight positional copy).
    cdef uint32_t arena_pos = 0
    cdef DrakenStringSlot* slot_ptr
    for i in range(num_rows):
        slot_ptr = <DrakenStringSlot*>(slots_buf + <size_t>i * SLOT_BYTES)
        if null_bitmap_len > 0 and not ((null_bitmap[i >> 3] >> (i & 7)) & 1):
            # Null row: init null slot; the stream carries no bytes for it.
            str_init_null(slot_ptr)
            continue
        p = _read_u32(p, &slen)
        if slen > 12:
            # Long string → arena. draken_build_string_slot computes the hash from
            # p and records arena_pos as the offset; the bytes must live in the arena.
            memcpy(arena_buf + arena_pos, p, slen)
            draken_build_string_slot(slot_ptr, p, slen, arena_pos)
            p += slen
            arena_pos += slen
        else:
            # Inline string (len <= STR_INLINE_MAX): bytes stored in the slot; the
            # arena is not touched and arena_pos is not advanced (offset ignored).
            draken_build_string_slot(slot_ptr, p, slen, arena_pos)
            p += slen

    # All three draken_malloc'd buffers transferred on call (even on failure).
    raw = draken_vector_own_string(
        <DrakenStringSlot*>slots_buf,
        arena_buf, <size_t>arena_pos,
        validity_buf, num_rows, want_type, NULL)  # E37: deserialize path = future work
    if raw == NULL:
        raise MemoryError("draken_vector_own_string failed")
    return _wrap_raw_pyobj(raw)


# ─── Array builder ────────────────────────────────────────────────────────────

cdef object _build_array_vector(const uint8_t* p, uint32_t num_rows,
                                const uint8_t* list_null_bmap, uint32_t list_null_bmap_len):
    """Deserialize TAG_ARRAY (11): reads the child_type_tag byte written right
    after the common IPC header (see ipc_serialize.hpp) and dispatches to the
    string or numeric child builder.
    """
    cdef uint8_t child_type_tag = p[0]
    p += 1
    if child_type_tag == CHILD_STRING:
        return _build_array_vector_string(p, num_rows, list_null_bmap, list_null_bmap_len)
    if child_type_tag == CHILD_ARRAY:
        return _build_array_vector_nested(p, num_rows, list_null_bmap, list_null_bmap_len)
    return _build_array_vector_numeric(p, num_rows, list_null_bmap, list_null_bmap_len, child_type_tag)


cdef object _build_array_vector_nested(const uint8_t* p, uint32_t num_rows,
                                       const uint8_t* list_null_bmap, uint32_t list_null_bmap_len):
    """Deserialize a CHILD_ARRAY TAG_ARRAY (11): the child of this list level is
    itself a list. Builds the child Vector recursively, then wraps it under this
    level's offsets + validity — producing a native list<list<...>> Vector with
    no Python nested list.

    Wire layout after child_type_tag (see ipc_serialize.hpp for the writer side):
      uint32_t  child_count
      int32_t[(num_rows+1)]  offsets
      uint32_t  inner_null_bmap_len
      uint8_t[inner_null_bmap_len]  inner_null_bmap   (validity of the child level)
      <child block, starting at its own child_type_tag; num_rows == child_count>
    """
    cdef uint32_t child_count
    p = _read_u32(p, &child_count)

    # parent_offsets — draken_malloc'd; transferred to draken_vector_own_array_child.
    cdef uint32_t offsets_bytes = (num_rows + 1) * sizeof(int32_t)
    cdef int32_t* parent_offsets = <int32_t*>draken_malloc(offsets_bytes)
    if parent_offsets == NULL:
        raise MemoryError()
    memcpy(parent_offsets, p, offsets_bytes)
    p += offsets_bytes

    # Inner (child-level) validity bitmap, then the child block itself.
    cdef uint32_t inner_null_bmap_len
    p = _read_u32(p, &inner_null_bmap_len)
    cdef const uint8_t* inner_null_bmap = p
    p += inner_null_bmap_len

    # Recurse: build the child list Vector (num_rows == child_count).
    cdef Vector child_vec
    try:
        child_vec = _build_array_vector(p, child_count, inner_null_bmap, inner_null_bmap_len)
    except BaseException:
        draken_free(parent_offsets)
        raise

    # parent_validity — draken_malloc'd; transferred to draken_vector_own_array_child.
    cdef uint8_t* parent_validity = NULL
    if list_null_bmap_len > 0:
        parent_validity = <uint8_t*>draken_malloc(list_null_bmap_len)
        if parent_validity == NULL:
            draken_free(parent_offsets)
            raise MemoryError()
        memcpy(parent_validity, list_null_bmap, list_null_bmap_len)

    # draken_vector_own_array_child MOVES the child VectorOwner out of the child
    # Vector's nanobind handle (leaving an empty husk) and takes ownership of
    # parent_offsets/parent_validity.
    cdef PyObject* raw = draken_vector_own_array_child(
        parent_offsets, <PyObject*>child_vec._nb, parent_validity, num_rows)
    if raw == NULL:
        raise MemoryError("draken_vector_own_array_child failed")
    return _wrap_raw_pyobj(raw)


cdef object _build_array_vector_string(const uint8_t* p, uint32_t num_rows,
                                const uint8_t* list_null_bmap, uint32_t list_null_bmap_len):
    """Deserialize a CHILD_STRING TAG_ARRAY (11) into a DRAKEN_ARRAY[VARCHAR] Vector.

    Wire layout after child_type_tag:
      uint32_t  child_count
      int32_t[(num_rows+1)]  offsets
      uint32_t  child_null_bmap_len
      uint8_t[child_null_bmap_len]  child_null_bmap  (per-child-element validity)
      for i in 0..child_count-1:
        uint32_t len
        uint8_t[len] bytes

    The child_null_bmap (Arrow convention, bit set = valid) is passed through to
    draken so null string elements read back as None, not ''.
    """
    cdef uint32_t child_count
    p = _read_u32(p, &child_count)

    # parent_offsets — draken_malloc'd; transferred to draken_vector_own_array.
    cdef uint32_t offsets_bytes = (num_rows + 1) * sizeof(int32_t)
    cdef int32_t* parent_offsets = <int32_t*>draken_malloc(offsets_bytes)
    if parent_offsets == NULL:
        raise MemoryError()
    memcpy(parent_offsets, p, offsets_bytes)
    p += offsets_bytes

    # Child null bitmap: capture (advance p); copied into an owned buffer below.
    cdef uint32_t child_null_bmap_len
    p = _read_u32(p, &child_null_bmap_len)
    cdef const uint8_t* child_null_bmap_src = p
    p += child_null_bmap_len

    # First pass: compute total child arena size.
    cdef const uint8_t* scan = p
    cdef uint32_t slen, i, total_child_arena = 0
    for i in range(child_count):
        scan = _read_u32(scan, &slen)
        total_child_arena += slen
        scan += slen

    # child_slots_buf — draken_malloc'd; transferred to draken_vector_own_array.
    cdef uint8_t* child_slots_buf = NULL
    if child_count > 0:
        child_slots_buf = <uint8_t*>draken_malloc(<size_t>child_count * SLOT_BYTES)
        if child_slots_buf == NULL:
            draken_free(parent_offsets)
            raise MemoryError()

    # child_arena — draken_malloc'd; transferred to draken_vector_own_array.
    cdef uint8_t* child_arena = NULL
    cdef size_t child_arena_len = <size_t>total_child_arena
    if total_child_arena > 0:
        child_arena = <uint8_t*>draken_malloc(child_arena_len)
        if child_arena == NULL:
            if child_slots_buf != NULL:
                draken_free(child_slots_buf)
            draken_free(parent_offsets)
            raise MemoryError()

    # child_validity — draken_malloc'd; transferred to draken_vector_own_array.
    cdef uint8_t* child_validity = NULL
    if child_null_bmap_len > 0:
        child_validity = <uint8_t*>draken_malloc(child_null_bmap_len)
        if child_validity == NULL:
            if child_arena != NULL:
                draken_free(child_arena)
            if child_slots_buf != NULL:
                draken_free(child_slots_buf)
            draken_free(parent_offsets)
            raise MemoryError()
        memcpy(child_validity, child_null_bmap_src, child_null_bmap_len)

    # parent_validity — draken_malloc'd; transferred to draken_vector_own_array.
    cdef uint8_t* parent_validity = NULL
    if list_null_bmap_len > 0:
        parent_validity = <uint8_t*>draken_malloc(list_null_bmap_len)
        if parent_validity == NULL:
            if child_validity != NULL:
                draken_free(child_validity)
            if child_arena != NULL:
                draken_free(child_arena)
            if child_slots_buf != NULL:
                draken_free(child_slots_buf)
            draken_free(parent_offsets)
            raise MemoryError()
        memcpy(parent_validity, list_null_bmap, list_null_bmap_len)

    # Second pass: fill child arena and build child slots.
    cdef uint32_t arena_pos = 0
    cdef DrakenStringSlot* slot_ptr
    for i in range(child_count):
        slot_ptr = <DrakenStringSlot*>(child_slots_buf + <size_t>i * SLOT_BYTES)
        p = _read_u32(p, &slen)
        if slen > 0 and child_arena != NULL:
            memcpy(child_arena + arena_pos, p, slen)
        # draken_build_string_slot reads from p (IPC buffer) for hash/inline bytes.
        # For inline strings (slen <= 12): stored inline, arena_pos harmless.
        # For long strings (slen > 12): hash from p, arena_offset = arena_pos.
        draken_build_string_slot(slot_ptr, p, slen, arena_pos)
        p += slen
        arena_pos += slen

    # All non-NULL buffers are draken_malloc'd and ownership is transferred
    # to draken_vector_own_array unconditionally on call entry (even on failure).
    cdef PyObject* raw = draken_vector_own_array(
        parent_offsets,
        <DrakenStringSlot*>child_slots_buf,
        child_arena, child_arena_len,
        child_count, DRAKEN_VARCHAR,
        child_validity,
        parent_validity, num_rows)
    if raw == NULL:
        raise MemoryError("draken_vector_own_array failed")
    return _wrap_raw_pyobj(raw)


cdef object _build_array_vector_numeric(const uint8_t* p, uint32_t num_rows,
                                        const uint8_t* list_null_bmap, uint32_t list_null_bmap_len,
                                        uint8_t child_type_tag):
    """Deserialize a fixed-width-child TAG_ARRAY (11) into a DRAKEN_ARRAY[T] Vector,
    T in {INT32, INT64, FLOAT32, FLOAT64, BOOL}.

    Wire layout after child_type_tag (see ipc_serialize.hpp for the writer side):
      uint32_t  child_count
      int32_t[(num_rows+1)]  offsets
      uint32_t  child_null_bmap_len
      uint8_t[child_null_bmap_len]  child_null_bmap
      CHILD_BOOL: uint8_t[(child_count+7)/8]  bit-packed values, LSB-first
      otherwise:  uint8_t[child_count * elem_size]  packed native-endian values
    """
    cdef uint32_t child_count
    p = _read_u32(p, &child_count)

    # parent_offsets — draken_malloc'd; transferred to draken_vector_own_array_numeric.
    cdef uint32_t offsets_bytes = (num_rows + 1) * sizeof(int32_t)
    cdef int32_t* parent_offsets = <int32_t*>draken_malloc(offsets_bytes)
    if parent_offsets == NULL:
        raise MemoryError()
    memcpy(parent_offsets, p, offsets_bytes)
    p += offsets_bytes

    cdef uint32_t child_null_bmap_len
    p = _read_u32(p, &child_null_bmap_len)
    cdef const uint8_t* child_null_bmap_src = p
    p += child_null_bmap_len

    cdef DrakenType child_type
    cdef uint32_t elem_size
    if child_type_tag == CHILD_INT64:
        child_type = DRAKEN_INT64
        elem_size = 8
    elif child_type_tag == CHILD_INT32:
        child_type = DRAKEN_INT32
        elem_size = 4
    elif child_type_tag == CHILD_UINT64:
        child_type = DRAKEN_UINT64
        elem_size = 8
    elif child_type_tag == CHILD_FLOAT32:
        child_type = DRAKEN_FLOAT32
        elem_size = 4
    elif child_type_tag == CHILD_FLOAT64:
        child_type = DRAKEN_FLOAT64
        elem_size = 8
    elif child_type_tag == CHILD_INT8:
        child_type = DRAKEN_INT8
        elem_size = 1
    elif child_type_tag == CHILD_INT16:
        child_type = DRAKEN_INT16
        elem_size = 2
    elif child_type_tag == CHILD_UINT8:
        child_type = DRAKEN_UINT8
        elem_size = 1
    elif child_type_tag == CHILD_UINT16:
        child_type = DRAKEN_UINT16
        elem_size = 2
    elif child_type_tag == CHILD_UINT32:
        child_type = DRAKEN_UINT32
        elem_size = 4
    else:
        child_type = DRAKEN_BOOL
        elem_size = 0

    cdef uint32_t data_bytes = (child_count + 7) // 8 if child_type_tag == CHILD_BOOL \
        else child_count * elem_size

    # child_data — draken_malloc'd; transferred to draken_vector_own_array_numeric.
    cdef uint8_t* child_data = NULL
    if data_bytes > 0:
        child_data = <uint8_t*>draken_malloc(data_bytes)
        if child_data == NULL:
            draken_free(parent_offsets)
            raise MemoryError()
        memcpy(child_data, p, data_bytes)
    p += data_bytes

    # child_validity — draken_malloc'd; transferred to draken_vector_own_array_numeric.
    cdef uint8_t* child_validity = NULL
    if child_null_bmap_len > 0:
        child_validity = <uint8_t*>draken_malloc(child_null_bmap_len)
        if child_validity == NULL:
            if child_data != NULL:
                draken_free(child_data)
            draken_free(parent_offsets)
            raise MemoryError()
        memcpy(child_validity, child_null_bmap_src, child_null_bmap_len)

    # parent_validity — draken_malloc'd; transferred to draken_vector_own_array_numeric.
    cdef uint8_t* parent_validity = NULL
    if list_null_bmap_len > 0:
        parent_validity = <uint8_t*>draken_malloc(list_null_bmap_len)
        if parent_validity == NULL:
            if child_validity != NULL:
                draken_free(child_validity)
            if child_data != NULL:
                draken_free(child_data)
            draken_free(parent_offsets)
            raise MemoryError()
        memcpy(parent_validity, list_null_bmap, list_null_bmap_len)

    # All non-NULL buffers are draken_malloc'd and ownership is transferred
    # to draken_vector_own_array_numeric unconditionally on call entry (even on failure).
    cdef PyObject* raw = draken_vector_own_array_numeric(
        parent_offsets, <void*>child_data, child_validity, child_count,
        child_type, parent_validity, num_rows)
    if raw == NULL:
        raise MemoryError("draken_vector_own_array_numeric failed")
    return _wrap_raw_pyobj(raw)


# ─── Public deserialization entry points ──────────────────────────────────────

cpdef object deserialize_column(int64_t ref_id, MemoryPool pool, DrakenType want_string_type=DRAKEN_VARCHAR):
    """Deserialize one IPC blob from MemoryPool into a Draken vector.

    `want_string_type` is the schema's declared physical type (VARCHAR/NVARCHAR/
    VARBINARY) for TAG_STR_DICT/TAG_STR_PLAIN columns — all three share the exact
    same DrakenStringSlot/arena byte layout, so this only changes the type tag
    the resulting Vector carries, never how the bytes are parsed. Ignored for
    non-string tags.

    Uses the Cython-native pool surface: reads the raw pointer under a latch
    (preventing concurrent compaction from moving the segment), parses
    directly from pool memory with no intermediate ``bytes`` copy, then
    unlatches in a finally block.

    Fixed-width tags (int64, int32→int64, float32, float64, bool) are
    dispatched into the C++ deserialiser, which performs the destination
    malloc + memcpy with the GIL released and returns owned buffers that
    `_wrap_decoded_fixed` slots into a Draken Vector.

    Dict/string tags (6..10) still parse in this Cython function.
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
            # owned-by-draken_malloc; we transfer them into a Vector under the GIL.
            with nogil:
                deserialize_fixed_column(<const uint8_t*>r.ptr, r.length, dc)

            if dc.status != STATUS_OK:
                # All non-OK statuses on a fixed-width tag are hard errors —
                # the kStatusNotHandled path is unreachable here because we
                # only call C++ for tags in the fixed-width range.
                draken_free(dc.data)
                draken_free(dc.null_bitmap)
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
                result = _build_string_dict(p, num_rows, null_bitmap, null_bitmap_len, want_string_type)
            elif tag == TAG_STR_PLAIN:
                result = _build_string_plain(p, num_rows, null_bitmap, null_bitmap_len, want_string_type)
            elif tag == TAG_INT64_DICT:
                result = _build_numeric_dict_int64(p, num_rows, null_bitmap, null_bitmap_len)
            elif tag == TAG_FLOAT32_DICT:
                result = _build_numeric_dict_float32(p, num_rows, null_bitmap, null_bitmap_len)
            elif tag == TAG_FLOAT64_DICT:
                result = _build_numeric_dict_float64(p, num_rows, null_bitmap, null_bitmap_len)
            elif tag == TAG_ARRAY:
                result = _build_array_vector(p, num_rows, null_bitmap, null_bitmap_len)
            elif tag == TAG_UINT8:
                result = _build_numeric_plain_narrow(p, num_rows, null_bitmap, null_bitmap_len, 1, DRAKEN_UINT8)
            elif tag == TAG_UINT16:
                result = _build_numeric_plain_narrow(p, num_rows, null_bitmap, null_bitmap_len, 2, DRAKEN_UINT16)
            elif tag == TAG_UINT32:
                result = _build_numeric_plain_narrow(p, num_rows, null_bitmap, null_bitmap_len, 4, DRAKEN_UINT32)
            elif tag == TAG_UINT64:
                result = _build_numeric_plain_narrow(p, num_rows, null_bitmap, null_bitmap_len, 8, DRAKEN_UINT64)
            elif tag == TAG_UINT8_DICT:
                result = _build_numeric_dict_narrow(p, num_rows, null_bitmap, null_bitmap_len, 1, DRAKEN_UINT8)
            elif tag == TAG_UINT16_DICT:
                result = _build_numeric_dict_narrow(p, num_rows, null_bitmap, null_bitmap_len, 2, DRAKEN_UINT16)
            elif tag == TAG_UINT32_DICT:
                result = _build_numeric_dict_narrow(p, num_rows, null_bitmap, null_bitmap_len, 4, DRAKEN_UINT32)
            elif tag == TAG_UINT64_DICT:
                result = _build_numeric_dict_narrow(p, num_rows, null_bitmap, null_bitmap_len, 8, DRAKEN_UINT64)
            elif tag == TAG_INT8:
                result = _build_numeric_plain_narrow(p, num_rows, null_bitmap, null_bitmap_len, 1, DRAKEN_INT8)
            elif tag == TAG_INT16:
                result = _build_numeric_plain_narrow(p, num_rows, null_bitmap, null_bitmap_len, 2, DRAKEN_INT16)
            elif tag == TAG_INT32_EXACT:
                result = _build_numeric_plain_narrow(p, num_rows, null_bitmap, null_bitmap_len, 4, DRAKEN_INT32)
            elif tag == TAG_INT8_DICT:
                result = _build_numeric_dict_narrow(p, num_rows, null_bitmap, null_bitmap_len, 1, DRAKEN_INT8)
            elif tag == TAG_INT16_DICT:
                result = _build_numeric_dict_narrow(p, num_rows, null_bitmap, null_bitmap_len, 2, DRAKEN_INT16)
            elif tag == TAG_INT32_DICT:
                result = _build_numeric_dict_narrow(p, num_rows, null_bitmap, null_bitmap_len, 4, DRAKEN_INT32)
            else:
                raise ValueError(f"Unknown IPC type tag: {tag}")
    finally:
        with nogil:
            pool.unlatch(ref_id)

    return result


cpdef dict deserialize_row_group(dict ref_ids, MemoryPool pool, dict string_types=None):
    """Deserialize all columns for a row group from MemoryPool into Draken vectors.

    `string_types` (optional): column name (bytes) -> declared DrakenType, for
    columns whose schema type is NVARCHAR/VARBINARY rather than the VARCHAR
    default (all three share the same byte layout — see `deserialize_column`).

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
    cdef bytes col_name
    cdef int64_t ref_id
    cdef int status
    cdef object vec

    refs.reserve(n)
    for r in ref_ids.values():
        refs.push_back(<int64_t>r)

    outs = <DecodedFixedColumn*>malloc(<size_t>n * sizeof(DecodedFixedColumn))
    if outs == NULL:
        raise MemoryError()

    i = 0
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
                vec = deserialize_column(
                    ref_id, pool,
                    string_types.get(col_name, DRAKEN_VARCHAR) if string_types is not None else DRAKEN_VARCHAR,
                )
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
    except BaseException:
        # deserialize_row_group_fixed already read/unlatched every column in
        # the batch before returning (see ipc_deserialize.cpp), so a raise at
        # column i leaves refs[i:] committed-but-unreleased in the pool for
        # the rest of the query. Release them here or they sit pinned until
        # the whole MemoryPool is torn down at query end. Best-effort: a
        # ref_id that was never validly committed (e.g. the bad-ref-id status
        # path) will itself raise on release — swallow that so it can't mask
        # the original exception we're propagating.
        for j in range(i, n):
            try:
                with nogil:
                    pool.release(<int64_t>refs[j])
            except Exception:
                pass
        raise
    finally:
        free(outs)

    return row_group
