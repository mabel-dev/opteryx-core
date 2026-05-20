from libc.stdint cimport int32_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t

cdef extern from "core/buffers.h":

    ctypedef enum DrakenType:
        DRAKEN_INT8
        DRAKEN_INT16
        DRAKEN_INT32
        DRAKEN_INT64
        DRAKEN_FLOAT32
        DRAKEN_FLOAT64
        DRAKEN_DATE32
        DRAKEN_TIMESTAMP64
        DRAKEN_TIME32
        DRAKEN_TIME64
        DRAKEN_INTERVAL
        DRAKEN_BOOL
        DRAKEN_STRING
        DRAKEN_DICTIONARY
        DRAKEN_CONSTANT
        DRAKEN_ARRAY

        DRAKEN_NON_NATIVE

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
        int32_t* offsets           # [N+1] entries
        uint8_t* null_bitmap       # optional
        size_t length
        DrakenType type

    ctypedef struct DrakenDictionaryBuffer:
        uint8_t* codes
        uint8_t code_width
        uint8_t* null_bitmap
        size_t length
        uint8_t ordered
        DrakenVarBuffer* dictionary_values
        DrakenType type

    ctypedef struct DrakenConstantStringPayload:
        uint8_t* data
        int32_t length

# German string (a.k.a. Umbra string). 16-byte slot:
#   inline (len <= 12):  uint32_t length + 12 inline bytes
#   extern (len  > 12):  uint32_t length + 4-byte prefix + uint64_t arena_offset
# Defined in draken/src/core/string_slot.h. Treated as opaque on the Cython
# side; production code accesses fields exclusively through the C inline
# helpers (str_equals, str_compare, str_data, str_prefix4, gs_lp_word) which
# inline cleanly via cdef extern.
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
                            uint32_t length, uint64_t arena_offset) nogil

cdef extern from "core/buffers.h":

    # German-string storage. Used as the `data` payload of a string
    # DrakenVector under the unified format (Track B).
    ctypedef struct DrakenStringArena:
        DrakenStringSlot* slots
        uint8_t*      arena
        size_t        length
        size_t        arena_used
        size_t        arena_cap
        uint8_t*      null_bitmap
        uint8_t       owns_buffers
        DrakenType    type

    ctypedef struct DrakenConstantBuffer:
        DrakenType type
        DrakenType value_type
        void* value
        size_t length
        uint8_t* null_bitmap

    # Array column (list<T>)
    ctypedef struct DrakenArrayBuffer:
        int32_t* offsets           # [length + 1] entries
        void* values               # pointer to another column (DrakenFixedColumn*, DrakenVarColumn*, etc.)
        uint8_t* null_bitmap       # optional, 1 bit per row
        size_t length              # number of array entries (rows)
        DrakenType value_type      # type of the child values

    ctypedef struct DrakenMorsel:
        const char** column_names
        DrakenType* column_types
        void** columns
        size_t num_columns
        size_t num_rows

    # Unified vector view — one shape, one access pattern.
    # Access is always: data[selection[i]] for i in [0, length).
    # `selection` is never NULL; it points at the global identity (former dense),
    # global zero (former constant), or owned uint32 codes (former dict).
    # See buffers.h for full semantics and vector_alloc.h for constructors.
    ctypedef struct DrakenVector:
        void*           data
        const uint32_t* selection
        uint32_t        data_length
        uint32_t        length
        uint8_t*        validity
        DrakenType      type

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

# Lightweight view struct returned by DictionaryVector.dict_accessor().
# All fields are shortcuts into the underlying DrakenDictionaryBuffer so
# callers never need raw ptr arithmetic.  Phase 1 — no C struct changes yet;
# dict_values remains DrakenVarBuffer* for both string and numeric backing.
cdef struct DictAccessor:
    uint8_t*         codes       # raw code array (code_width bytes per code index)
    uint8_t          code_width  # 1, 2, or 4 bytes per code
    uint8_t*         row_nulls   # row-level null bitmap (NULL means all rows valid)
    size_t           length      # number of rows
    DrakenVarBuffer* dict_values # backing dictionary buffer
    DrakenType       value_type  # element type of the dictionary entries

cdef struct ConstAccessor:
    size_t      length
    DrakenType  value_type
    void*       value_ptr
    uint8_t     is_null
