#pragma once
#include <stdint.h>
#include <stddef.h>
#include "german_string.h"

typedef enum {
    // Integer types: 1–19
    DRAKEN_INT8           = 1,
    DRAKEN_INT16          = 2,
    DRAKEN_INT32          = 3,
    DRAKEN_INT64          = 4,

    // Floating-point types: 20–29
    DRAKEN_FLOAT32        = 20,
    DRAKEN_FLOAT64        = 21,

    // Temporal types: 30–49
    DRAKEN_DATE32         = 30,
    DRAKEN_TIMESTAMP64    = 40,
    DRAKEN_TIME32         = 41,
    DRAKEN_TIME64         = 42,
    DRAKEN_INTERVAL       = 43,

    // Boolean: 50
    DRAKEN_BOOL           = 50,

    // String-like: 60–79
    DRAKEN_STRING         = 60,
    DRAKEN_DICTIONARY     = 61,
    DRAKEN_CONSTANT       = 62,

    // Complex types: 80–99
    DRAKEN_ARRAY          = 80,

    // Catch-all
    DRAKEN_NON_NATIVE     = 100,  // Unoptimized or fallback-wrapped Arrow types
} DrakenType;

typedef struct {
    void* data;               // int64_t*, double*, etc.
    uint8_t* null_bitmap;     // optional, 1 bit per row
    size_t length;
    size_t itemsize;
    DrakenType type;
} DrakenFixedBuffer;

typedef struct {
    uint8_t* data;            // UTF-8 bytes
    int32_t* offsets;         // [N+1] entries
    uint8_t* null_bitmap;     // optional
    size_t length;
    DrakenType type;
} DrakenVarBuffer;

typedef struct {
    uint8_t* codes;               // code stream, width selected by code_width
    uint8_t code_width;           // bytes per code: 1, 2, or 4
    uint8_t* null_bitmap;         // optional, 1 bit per row
    size_t length;                // number of rows
    uint8_t ordered;              // dictionary order flag from parquet/arrow metadata
    DrakenVarBuffer* dictionary_values;  // dictionary payload buffer (type in DrakenVarBuffer.type)
    DrakenType type;              // DRAKEN_DICTIONARY
} DrakenDictionaryBuffer;

typedef struct {
    uint8_t* data;
    int32_t length;
} DrakenConstantStringPayload;

// German-string storage. Replaces DrakenVarBuffer for string values: an array
// of 16-byte GermanString slots (length + inline-12 OR length + prefix + arena
// offset) plus a byte arena for long-form payloads (> 12 bytes). Used as the
// `data` payload of a string DrakenVector under the unified format.
//
// Lifetime: slots and arena are both owned by this struct when owns_buffers
// is non-zero. arena_used tracks bytes consumed during construction; arena_cap
// is the allocation size. Slots whose length <= 12 do not reference the arena.
typedef struct {
    GermanString* slots;       // [length] slot array
    uint8_t*      arena;       // long-form byte arena (may be NULL when all rows inline)
    size_t        length;      // number of slots
    size_t        arena_used;  // bytes consumed in arena
    size_t        arena_cap;   // arena allocation size
    uint8_t*      null_bitmap; // optional, 1 bit per row
    uint8_t       owns_buffers;// free slots/arena/null_bitmap on free?
    DrakenType    type;        // DRAKEN_STRING (or DRAKEN_NON_NATIVE for binary)
} DrakenGermanArena;

typedef struct {
    DrakenType type;          // DRAKEN_CONSTANT
    DrakenType value_type;    // scalar value type
    void* value;              // owned scalar payload
    size_t length;            // logical row count
    uint8_t* null_bitmap;     // optional row validity bitmap
} DrakenConstantBuffer;

typedef struct {
    int32_t* offsets;         // [length + 1] entries
    void* values;             // pointer to another column's data (DrakenFixedColumn*, DrakenVarColumn*, etc.)
    uint8_t* null_bitmap;     // optional, 1 bit per row
    size_t length;            // number of array entries (rows)
    DrakenType value_type;    // type of the child values
} DrakenArrayBuffer;

typedef struct {
    void* run_values;         // Fixed-size types: flat value array; strings: byte arena
    int32_t* run_lengths;     // [num_runs] repetition counts, sum = length
    size_t num_runs;          // Number of run pairs
    uint8_t* null_bitmap;     // optional: row-level nulls (applies to logically expanded rows)
    size_t length;            // Total logical row count (sum of run_lengths)
    DrakenType type;          // Data type (DRAKEN_INT64, DRAKEN_STRING, etc.)
    // String support (NULL for non-string types):
    int32_t* run_str_lens;    // Byte length per run value [num_runs]
    uint32_t* run_str_offsets;// Byte offset in run_values arena [num_runs]
} DrakenRLEBuffer;

typedef struct {
    const char** column_names;       // length == num_columns
    DrakenType* column_types;        // length == num_columns
    void** columns;                  // (DrakenFixedColumn* or DrakenVarColumn*)[num_columns]
    size_t num_columns;
    size_t num_rows;
} DrakenMorsel;

// Unified vector view — one shape, one access pattern.
//
// Access is always: data[selection[i]]  for i in [0, length).
//
// `selection` is never NULL. For dense vectors it points at the lazy-grown
// global identity permutation; for constant vectors at the lazy-grown global
// zero vector; for dict-encoded vectors at owned uint32 codes. The choice of
// pointer is owned by the C constructors in vector_alloc.h. No operator,
// kernel, or wrapper may specialize on encoding shape — there is no
// "dense fast-path", no `data_length == 1` shortcut, no NULL check.
//
// Memory-layout hints (informational only — never used in hot loops):
//   former-dense    => selection points at draken_identity_sel,  data_length == length
//   former-constant => selection points at draken_zero_sel,      data_length == 1
//   former-dict     => selection is owned codes,                 data_length <  length
typedef struct {
    void*             data;        // typed payload (cast at Cython typed-wrapper level)
    const uint32_t*   selection;   // always valid; indices into data
    uint32_t          data_length; // number of unique values in data
    uint32_t          length;      // logical row count
    uint8_t*          validity;    // 1-bit-per-logical-row null mask; NULL = all valid
    DrakenType        type;
} DrakenVector;
