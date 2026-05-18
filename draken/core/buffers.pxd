from libc.stdint cimport int32_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint32_t

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

    # Unified vector view — see buffers.h for encoding semantics.
    # Invariant: selection == NULL  XOR  sel_width == 0.
    ctypedef struct DrakenVector:
        void*      data
        size_t     data_length
        void*      selection
        uint8_t    sel_width
        size_t     length
        uint8_t*   validity
        size_t     itemsize
        DrakenType type

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
