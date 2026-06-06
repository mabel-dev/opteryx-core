#pragma once

#include <cstdint>
#include <string>
#include <vector>

// fast_parsers lives in _jsonl/core/ and is included by path — _csv does not
// link against _jsonl. These are header-only bounded parsers with no JSONL-specific
// dependencies.
#include "../../_jsonl/core/fast_parsers.hpp"

#include "csv_parse_context.hpp"
#include "csv_row_map.hpp"

// DrakenType and DrakenStringSlot are needed for the ParsedColumn carrier.
#include "buffers.h"
#include "string_slot.h"

// PyObject forward declaration so this header can declare wrap_csv_column()
// without forcing Python.h include order onto every consumer.
#ifndef Py_PYTHON_H
struct _object;
typedef struct _object PyObject;
#endif

namespace rugo::_csv {

// ---------------------------------------------------------------------------
// Inferred column type (speculative; typed build validates every value).
// ---------------------------------------------------------------------------
enum class CsvColumnType : uint8_t {
    Int64   = 0,
    Float64 = 1,
    Bool    = 2,
    String  = 3,
    Null    = 4,   // all values in sample were empty
};

// ---------------------------------------------------------------------------
// ParsedCsvColumn — column data in draken_malloc buffers, ready to be wrapped
// into a DrakenVector. Contains no Python objects: safe to produce off the GIL
// in parallel, then wrapped serially under the GIL via wrap_csv_column().
//
// Buffer ownership semantics mirror the JSONL ParsedColumn: once wrap_csv_column()
// consumes this struct, it owns the buffers. Do not free them independently.
// ---------------------------------------------------------------------------
struct ParsedCsvColumn {
    DrakenType        type      = DRAKEN_VARCHAR;
    uint32_t          length    = 0;     // logical row count
    uint8_t*          validity  = nullptr; // draken_malloc'd or NULL (all valid)
    bool              is_string = false;
    void*             data      = nullptr; // typed buffer (draken_vector_own_raw)
    DrakenStringSlot* slots     = nullptr; // string slots (draken_vector_own_string)
    uint8_t*          arena     = nullptr;
    size_t            arena_len = 0;
};

// ---------------------------------------------------------------------------
// Unescape a quoted field value in-place into `out`.
//
// `src` points to the first byte after the opening '"'; `len` is CsvFieldSpan::length.
// Handles both \" and "" escape sequences. Any other \x sequence absorbs the backslash
// (liberal mode). `out` must have capacity >= `len`.
//
// Returns the number of bytes written to `out` (always <= `len`).
// ---------------------------------------------------------------------------
uint32_t unescape_csv_field(
    const uint8_t* src,
    uint16_t       len,
    uint8_t*       out) noexcept;

// ---------------------------------------------------------------------------
// Parse one column from its CsvFieldSpan array.
//
// `buffer` is the full file buffer (spans hold offsets into it).
// `spans` is row_map.column_spans[i] for the target column.
// `survivor_bitmap` is the output of evaluate_predicates(); pass an empty vector
// to include all rows.
//
// Type inference: speculates Int64 → widens to Float64 → falls back to String.
// Bool tries "true"/"false" (case-insensitive) → falls back to String.
// Empty field (span.length == 0 && !span.was_quoted) → null regardless of type.
// Non-empty span with was_quoted == true and length == 0 → empty string, not null
// (an explicitly quoted empty field is "" which is a present, zero-length string).
//
// ⚠ FIELD LENGTH CAP: values are read from spans whose length is at most 65,535
// bytes. See CsvFieldSpan documentation. Callers that need unescape must supply
// a scratch buffer of at least UINT16_MAX bytes; this function allocates one
// internally if needed.
//
// Pure C++; no Python. Safe to call with the GIL released.
// ---------------------------------------------------------------------------
ParsedCsvColumn parse_csv_column(
    const uint8_t*                    buffer,
    const std::vector<CsvFieldSpan>&  spans,
    const std::vector<uint8_t>&       survivor_bitmap);

// ---------------------------------------------------------------------------
// Parse all projected columns in parallel (one thread-pool task per column).
//
// `column_indices` lists the request_cols indices (into row_map) to build.
// Returns one ParsedCsvColumn per entry in the same order.
// max_threads == 0 uses hardware_concurrency, clamped to column count.
//
// Pure C++; no Python. Release the GIL before calling.
// ---------------------------------------------------------------------------
std::vector<ParsedCsvColumn> parse_all_csv_columns(
    const uint8_t*              buffer,
    const CsvRowMap&            row_map,
    const std::vector<size_t>&  column_indices,
    const std::vector<uint8_t>& survivor_bitmap,
    size_t                      max_threads = 0);

// ---------------------------------------------------------------------------
// Wrap a ParsedCsvColumn into an owned DrakenVector.
//
// Creates a Python object — call under the GIL. Transfers buffer ownership
// into the vector via draken_vector_own_raw / draken_vector_own_string.
// Returns a NEW reference, or NULL with a Python exception set on failure.
// ---------------------------------------------------------------------------
PyObject* wrap_csv_column(ParsedCsvColumn& pc);

}  // namespace rugo::_csv
