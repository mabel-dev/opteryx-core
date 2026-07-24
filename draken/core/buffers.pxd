# Hand-written declarations mirroring draken/core/buffers.h — the frozen ABI
# surface for the ~95 compiled `cimport draken.core.buffers` sites (07).
#
# nanobind emits no cimportable .pxd, so the C++-defined struct is bound here by
# `cdef extern from "core/buffers.h"`. This file MUST stay byte-for-byte
# consistent with the header: a stale .pxd is silent ABI drift. The header's
# static_asserts pin the C side; this pxd is the Cython side of the same
# contract and is verified by eye against the header (consumers are not rebuilt
# in this milestone).

from libc.stdint cimport int32_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t
from libc.stdlib cimport free, malloc

cdef extern from "core/buffers.h":

    ctypedef enum DrakenType:
        DRAKEN_INT8
        DRAKEN_INT16
        DRAKEN_INT32
        DRAKEN_INT64
        DRAKEN_DECIMAL
        DRAKEN_FLOAT32
        DRAKEN_FLOAT64
        DRAKEN_DATE32
        DRAKEN_TIMESTAMP64
        DRAKEN_TIME32
        DRAKEN_TIME64
        DRAKEN_INTERVAL
        DRAKEN_BOOL
        DRAKEN_VARCHAR
        DRAKEN_NVARCHAR
        DRAKEN_VARBINARY
        DRAKEN_VARIANT
        DRAKEN_ARRAY
        DRAKEN_NULL
        DRAKEN_VECTOR_FP16
        DRAKEN_DECIMAL128
        DRAKEN_UINT8
        DRAKEN_UINT16
        DRAKEN_UINT32
        DRAKEN_UINT64

    # Category-A layout hint bits (informational; 0 = "don't know").
    unsigned int DRAKEN_SEL_IDENTITY
    unsigned int DRAKEN_SEL_PERMUTATION
    unsigned int DRAKEN_DICT_KEYS_SORTED
    unsigned int DRAKEN_DICT_CODES_DENSE

    # Fixed-width column
    ctypedef struct DrakenFixedBuffer:
        void* data                 # int64_t*, double*, etc.
        uint8_t* null_bitmap       # optional, 1 bit per row
        size_t length
        size_t itemsize
        DrakenType type

    # Variable-width column (string/binary)
    ctypedef struct DrakenVarBuffer:
        uint8_t* data              # UTF-8 bytes
        uint32_t* offsets          # [N+1] entries — unsigned, up to 4 GB
        uint8_t* null_bitmap       # optional
        size_t length
        DrakenType type

    ctypedef struct DrakenConstantStringPayload:
        uint8_t* data
        int32_t length

# German string (a.k.a. Umbra string). 16-byte slot:
#   inline (len <= 12):  uint32_t length + 12 inline bytes
#   extern (len  > 12):  uint32_t length + 4-byte prefix + uint64_t arena_offset
# Defined in draken/core/string_slot.h. Treated as opaque on the Cython side;
# production code accesses fields exclusively through the C inline helpers
# (str_equals, str_compare, str_data, str_prefix4, gs_lp_word) which inline
# cleanly via cdef extern.
cdef extern from "core/string_slot.h":
    int STR_INLINE_MAX

    ctypedef struct DrakenStringSlot:
        pass  # opaque; sizeof(DrakenStringSlot) still resolves at C compile time

    uint32_t str_length(const DrakenStringSlot* s) noexcept nogil
    int      str_is_inline(const DrakenStringSlot* s) noexcept nogil
    uint32_t str_prefix4(const DrakenStringSlot* s) noexcept nogil
    const uint8_t* str_data(const DrakenStringSlot* s, const uint8_t* arena_base) noexcept nogil
    int      str_equals(const DrakenStringSlot* a, const uint8_t* arena_a,
                       const DrakenStringSlot* b, const uint8_t* arena_b) noexcept nogil
    int      str_compare(const DrakenStringSlot* a, const uint8_t* arena_a,
                        const DrakenStringSlot* b, const uint8_t* arena_b) noexcept nogil
    # Builder-side initializers. Each zeroes the slot before writing so that
    # short strings with length < 4 produce deterministic lp_word bytes.
    void     str_init_null(DrakenStringSlot* s) nogil
    void     str_init_inline(DrakenStringSlot* s, const uint8_t* src, uint32_t length) nogil
    void     str_init_extern(DrakenStringSlot* s, const uint8_t* src,
                            uint32_t length, uint32_t hash32, uint32_t arena_offset) nogil
    void     str_clone_with_offset(DrakenStringSlot* dst, const DrakenStringSlot* src,
                                   uint32_t new_arena_offset) nogil

cdef extern from "core/buffers.h":

    # German-string storage. Used as the `data` payload of a string
    # DrakenVector under the unified format.
    ctypedef struct DrakenStringArena:
        DrakenStringSlot* slots
        uint8_t*      arena
        size_t        length
        size_t        arena_used
        size_t        arena_cap
        uint8_t*      null_bitmap
        uint8_t       owns_buffers
        DrakenType    type

    # Array column (list<T>)
    ctypedef struct DrakenArrayBuffer:
        int32_t* offsets           # [length + 1] entries
        void* values               # pointer to another column (DrakenFixedColumn*, DrakenVarColumn*, etc.)
        uint8_t* null_bitmap       # optional, 1 bit per row
        size_t length              # number of array entries (rows)
        DrakenType value_type      # type of the child values

    # Unified vector view — one shape, one access pattern.
    # Access is always: data[selection[i]] for i in [0, length).
    # `selection` is never NULL; it points at the global identity (former dense),
    # global zero (former constant), or owned uint32 codes (former dict).
    # `flags` carries Category-A layout hints (0 = "don't know").
    # See buffers.h for full semantics and vector_alloc.h for constructors.
    ctypedef struct DrakenVector:
        void*           data
        const uint32_t* selection
        uint32_t        data_length
        uint32_t        length
        uint8_t*        validity
        DrakenType      type
        uint8_t         flags

    # Approximate in-memory footprint (bytes) of one vector's owned payload —
    # data buffer (dedup-aware), string arena, and validity bitmap. `selection`
    # is excluded (shared global for dense/constant). See buffers.h for the full
    # contract and the DRAKEN_ARRAY under-count limitation. nogil: pure field
    # reads, no allocation.
    size_t draken_vector_nbytes(const DrakenVector* v) noexcept nogil

cdef extern from "core/vector_alloc.h":
    const uint32_t* draken_identity_sel(uint32_t length) nogil
    const uint32_t* draken_zero_sel(uint32_t length) nogil
    const uint8_t* draken_zero_validity(uint32_t length) nogil

    DrakenVector draken_vector_from_dense(
        void* data, uint32_t length, DrakenType type, uint8_t* validity) nogil

    DrakenVector draken_vector_from_constant(
        void* data, uint32_t length, DrakenType type, uint8_t* validity) nogil

    DrakenVector draken_vector_from_dict(
        void* data, uint32_t data_length,
        const uint32_t* codes, uint32_t length,
        DrakenType type, uint8_t* validity) nogil


# Variable-width buffer allocator (07: var_vector is "real but internalize, and
# rename" — its one external caller is re-homed in a later milestone). Kept on
# the frozen ABI surface here so the listed consumers continue to bind it via
# `cimport draken.core.buffers`. Inline so it links into each extension.
cdef inline DrakenVarBuffer* alloc_var_buffer(DrakenType dtype, size_t length, size_t bytes_cap):
    cdef DrakenVarBuffer* buf = <DrakenVarBuffer*> malloc(sizeof(DrakenVarBuffer))
    if buf == NULL:
        raise MemoryError()

    # allocate offsets: length + 1
    buf.offsets = <uint32_t*> malloc((length + 1) * sizeof(uint32_t))
    if buf.offsets == NULL:
        free(buf)
        raise MemoryError()

    # allocate data buffer
    if bytes_cap > 0:
        buf.data = <uint8_t*> malloc(bytes_cap)
        if buf.data == NULL:
            free(buf.offsets)
            free(buf)
            raise MemoryError()
    else:
        buf.data = NULL

    buf.null_bitmap = NULL
    buf.length = length
    buf.type = dtype
    return buf
