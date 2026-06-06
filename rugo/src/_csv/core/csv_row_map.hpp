#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include "csv_parse_context.hpp"
#include "csv_scan.hpp"

namespace rugo::_csv {

// ---------------------------------------------------------------------------
// CsvFieldSpan — position of one field value inside the file buffer.
//
// `start` is the byte offset of the first byte of the raw field value (i.e.
// the byte AFTER the opening '"' for quoted fields, or the first byte of the
// value for unquoted fields).
//
// ⚠ FIELD LENGTH CAP: 65,535 BYTES (uint16_t)
// Fields longer than 65,535 bytes are truncated to 65,535 bytes. This is an
// explicit design trade-off: uint16_t halves the span-table memory footprint.
// Analytical CSV fields are almost universally short. Callers that require full
// fidelity on arbitrary-length text MUST NOT use this reader — use Parquet or
// JSONL instead.
//
// A null field (empty unquoted value or missing column) has length == 0 and
// was_quoted == false. Callers distinguish null from empty-string by consulting
// the null bitmap emitted by the column builder.
// ---------------------------------------------------------------------------
struct CsvFieldSpan {
    uint32_t start;       // byte offset into file buffer
    uint16_t length;      // raw byte count; clamped to UINT16_MAX — see cap note above
    bool     was_quoted;  // field was opened with '"'
    bool     has_escape;  // contains \" or "" — unescape needed before parse/return
};

static_assert(sizeof(CsvFieldSpan) == 8, "CsvFieldSpan must be 8 bytes");

// Sentinel for a null / missing field.
inline CsvFieldSpan null_span() noexcept {
    return CsvFieldSpan{0, 0, false, false};
}

// ---------------------------------------------------------------------------
// CsvRowMap — result of the span-extraction phase.
//
// Contains one span array per *requested* column (projected ∪ predicate).
// Columns that were not requested are not stored — their bytes are skipped
// during the FSM walk. For a 100-column file with `SELECT a, b WHERE c > 5`,
// three span arrays are stored, not one hundred.
// ---------------------------------------------------------------------------
struct CsvRowMap {
    uint32_t                               num_rows;       // rows scanned (incl. empty tail)
    uint32_t                               num_cols;       // columns as counted in header/row-0
    std::vector<std::string>               column_names;   // header names, or col_0…col_{N-1}

    // Parallel arrays: request_cols[i] is the file ordinal of the column whose
    // spans are stored in column_spans[i].
    std::vector<uint32_t>                  request_cols;
    std::vector<std::vector<CsvFieldSpan>> column_spans;   // [i][row] → span

    // Returns the column_spans index for a given file ordinal, or -1 if not requested.
    int spans_index_for_ordinal(uint32_t ordinal) const noexcept {
        for (size_t i = 0; i < request_cols.size(); ++i) {
            if (request_cols[i] == ordinal) return static_cast<int>(i);
        }
        return -1;
    }
};

// ---------------------------------------------------------------------------
// Header parse.
//
// Scans from `data` to the first unquoted '\n', populating `ctx`'s column map
// from the resulting field names. Returns the byte offset of the first data row
// (i.e. one past the header '\n').
//
// If ctx.has_header == false, synthesises column names col_0 … col_{N-1} by
// counting delimiters in the first row, then returns 0 so that row is treated
// as data by subsequent phases.
//
// Populates `column_names_out` (in file order) and `num_cols_out`.
// Aborts with a descriptive message if the header row is malformed or empty.
// ---------------------------------------------------------------------------
size_t parse_csv_header(
    const uint8_t*          data,
    size_t                  length,
    const CsvParseContext&  ctx,
    std::vector<std::string>& column_names_out,
    uint32_t&               num_cols_out);

// ---------------------------------------------------------------------------
// Span extraction — single range.
//
// Runs the five-state quote FSM (FIELD_START / UNQUOTED / QUOTED /
// ESCAPE_IN_QUOTED / DOUBLE_QUOTE_PENDING) over [data, data+length).
//
// For each row, records one CsvFieldSpan per column ordinal listed in
// `request_ordinals`. Columns not in the set are skipped: the FSM counts
// delimiters to track the current ordinal but does not record spans for
// unrequested columns. This is the columnar read advantage: O(requested_cols)
// work per row, not O(all_cols).
//
// `num_cols` must be the column count determined from the header (used to
// detect ragged rows). `data_offset` is added to every start position so
// that callers working on a sub-range get absolute buffer offsets in the
// returned spans.
//
// Returns a CsvRowMap whose column_spans[i] has one entry per row (null_span()
// for missing or empty fields). The row count includes any trailing empty row
// that may appear at the end of a CRLF file.
// ---------------------------------------------------------------------------
CsvRowMap extract_spans(
    const uint8_t*              data,
    size_t                      length,
    uint32_t                    data_offset,
    const CsvParseContext&      ctx,
    uint32_t                    num_cols,
    const std::vector<uint32_t>& request_ordinals);

// ---------------------------------------------------------------------------
// Span extraction — multithreaded.
//
// Splits the buffer into newline-aligned ranges (using find_safe_splits()),
// runs extract_spans() on each range in parallel via BS::thread_pool, then
// merges the per-range CsvRowMaps in order by flat concatenation of the
// per-column span vectors. max_threads == 0 uses hardware_concurrency.
//
// Thread count is clamped to min(max_threads, safe_split_count, hardware_concurrency).
// Falls back to single-threaded extract_spans() when fewer than two safe splits
// are found (e.g. heavily quoted files with few safe newlines).
// ---------------------------------------------------------------------------
CsvRowMap extract_spans_threaded(
    const uint8_t*              data,
    size_t                      length,
    size_t                      header_offset,   // byte offset where data rows start
    const CsvParseContext&      ctx,
    uint32_t                    num_cols,
    const std::vector<uint32_t>& request_ordinals,
    size_t                      max_threads = 0);

// ---------------------------------------------------------------------------
// Predicate evaluation.
//
// For each predicate in ctx.predicates, parses the raw bytes of the predicate
// column's spans and evaluates the comparison (using the same int64 → float64
// → varchar ladder as the column builder). Builds a per-row survivor bitmap:
// bit[r] = 1 if row r passes ALL predicates, 0 otherwise.
//
// Returns a byte vector of ceil(num_rows / 8) bytes, packed LSB-first.
// If ctx.predicates is empty, returns an empty vector (all rows implicitly
// survive — callers check `bitmap.empty()` to skip filtering).
//
// `row_map` must have column_spans populated for every predicate column.
// If a predicate column is missing from row_map (caller failed to request it),
// every row fails that predicate.
// ---------------------------------------------------------------------------
std::vector<uint8_t> evaluate_predicates(
    const uint8_t*        buffer,
    const CsvRowMap&      row_map,
    const CsvParseContext& ctx);

}  // namespace rugo::_csv
