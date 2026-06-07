#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "../../_jsonl/core/fast_parsers.hpp"
#include "csv_parse_context.hpp"

#include "buffers.h"
#include "string_slot.h"

#ifndef Py_PYTHON_H
struct _object;
typedef struct _object PyObject;
#endif

namespace rugo::_csv {

// ---------------------------------------------------------------------------
// ParsedCsvColumn — draken_malloc-owned column buffers ready for wrapping.
// Buffer ownership transfers to DrakenVector via wrap_csv_column().
// ---------------------------------------------------------------------------
struct ParsedCsvColumn {
    DrakenType        type      = DRAKEN_VARCHAR;
    uint32_t          length    = 0;
    uint8_t*          validity  = nullptr;
    bool              is_string = false;
    void*             data      = nullptr;
    DrakenStringSlot* slots     = nullptr;
    uint8_t*          arena     = nullptr;
    size_t            arena_len = 0;
};

// ---------------------------------------------------------------------------
// StreamResult — output of build_columns_streaming.
// ---------------------------------------------------------------------------
struct StreamResult {
    std::vector<ParsedCsvColumn> columns;
    uint32_t                     num_rows;   // survivors after predicate filtering
};

// ---------------------------------------------------------------------------
// Unescape a quoted field value in-place into `out`.
// Handles both \" and "" escape sequences.
// Returns the number of bytes written (always <= len).
// ---------------------------------------------------------------------------
uint32_t unescape_csv_field(
    const uint8_t* src,
    uint16_t       len,
    uint8_t*       out) noexcept;

// ---------------------------------------------------------------------------
// build_columns_streaming — two-pass read (split-find + streaming build).
//
// Pass 1: find_safe_splits_parallel → safe newline positions for threading.
//         Also sniffs column types from first 128 non-null values per column.
// Pass 2: each thread scans its byte range once — the SIMD structural scan
//         drives a field FSM that evaluates predicates inline and writes
//         projected column values directly to typed output buffers.
//         No intermediate span store.
//
// Parameters:
//   buffer          — full file buffer
//   length          — buffer length
//   header_offset   — byte offset where data rows start (from parse_csv_header)
//   column_names    — column names in file order (from parse_csv_header)
//   num_cols        — column count from header
//   request_ordinals — sorted column ordinals to scan (projected ∪ predicate)
//   proj_indices    — indices into request_ordinals for projected output columns
//   ctx             — parse context (delimiter, predicates, etc.)
//   max_threads     — 0 = hardware_concurrency
//
// Release the GIL before calling.
// ---------------------------------------------------------------------------
StreamResult build_columns_streaming(
    const uint8_t*               buffer,
    size_t                       length,
    size_t                       header_offset,
    const std::vector<std::string>& column_names,
    uint32_t                     num_cols,
    const std::vector<uint32_t>& request_ordinals,
    const std::vector<size_t>&   proj_indices,
    const CsvParseContext&       ctx,
    size_t                       max_threads);

// ---------------------------------------------------------------------------
// wrap_csv_column — GIL required. Transfers buffer ownership into DrakenVector.
// ---------------------------------------------------------------------------
PyObject* wrap_csv_column(ParsedCsvColumn& pc);

}  // namespace rugo::_csv
