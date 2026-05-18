#pragma once
#include <stdint.h>
#include <stddef.h>

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

// Unified vector view (Phase 1 — not yet used by any kernel).
//
// Encoding semantics via (data, selection, sel_width):
//   DENSE:      selection == NULL, sel_width == 0, data_length == length
//   DICTIONARY: selection = codes, sel_width in {1,2,4}, data_length = dict_size
//   CONSTANT:   selection == NULL, sel_width == 0, data_length == 1
//   RLE:        never reaches execution; expanded at scan boundary
//
// Invariant: selection == NULL  XOR  sel_width == 0.
typedef struct {
    void*      data;        // unique values: all rows (dense), dict entries (dict), 1 element (const)
    size_t     data_length; // elements in data, NOT logical row count
    void*      selection;   // gather indices; NULL = sequential identity (dense/const)
    uint8_t    sel_width;   // 0 when selection==NULL; 1, 2, or 4 bytes per code otherwise
    size_t     length;      // logical row count
    uint8_t*   validity;    // 1-bit-per-logical-row null mask; NULL = all valid
    size_t     itemsize;    // bytes per data element (0 for var-width)
    DrakenType type;
} DrakenVector;
