#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace rugo::_csv {

// Op codes shared with JSONL predicate encoding: 0=EQ 1=NE 2=LT 3=LE 4=GT 5=GE
struct CsvPredicate {
    std::string column;   // column name (matched against header)
    uint8_t     op;       // comparison operator
    std::string value;    // raw comparison value as a byte string
};

// Per-reader configuration. Construct once; treat as immutable after construction.
// The structural LUT is built in the constructor from `delimiter` — do not change
// `delimiter` after construction without calling rebuild_lut().
struct CsvParseContext {
    // ---------------------------------------------------------------------------
    // Format options
    // ---------------------------------------------------------------------------

    // Field separator byte. Comma by default; set to '\t' for TSV.
    // Must not be '"' or '\\' — those are reserved structural bytes.
    uint8_t delimiter = ',';

    // If true, the first row is a header; column names are taken from it.
    // If false, the first row is data and columns are named col_0, col_1, …
    bool has_header = true;

    // ---------------------------------------------------------------------------
    // Type inference
    // ---------------------------------------------------------------------------

    // Number of non-null values per projected column sniff_csv_column_types()
    // samples before settling on a type (INT64 -> FLOAT64 -> VARCHAR widening).
    // A value past this sample window that doesn't fit the sniffed type is a
    // type mismatch -- see `ignore_errors` below for how that is handled.
    uint32_t sniff_sample_size = 128;

    // A post-sniff value that doesn't parse as the column's sniffed type
    // (e.g. sniffed INT64, but a later row has "abc"): false (default) fails
    // the whole read loud, naming the column and the offending value; true
    // treats that single value as NULL instead. Never silently coerced to 0 --
    // that was a pre-existing bug (see rugo/src/csv/core/csv_column_builder.cpp
    // commit_row), not a supported behavior.
    bool ignore_errors = false;

    // ---------------------------------------------------------------------------
    // Pushdown
    // ---------------------------------------------------------------------------

    // Columns to extract. Empty = all columns. Names must match the header row
    // (or col_0 … col_{N-1} when has_header=false) byte-for-byte.
    std::vector<std::string> projected_columns;

    // Predicates applied during span extraction. Rows that fail all predicates
    // for a given column are excluded from the typed column build.
    std::vector<CsvPredicate> predicates;

    // ---------------------------------------------------------------------------
    // Threading
    // ---------------------------------------------------------------------------

    // Maximum threads for span extraction and column build. 0 = hardware_concurrency.
    size_t max_threads = 0;

    // ---------------------------------------------------------------------------
    // Structural LUT (built from delimiter; do not hand-populate)
    // ---------------------------------------------------------------------------

    // 256-entry LUT: nonzero → structural byte; (value - 1) == CsvMarkerType.
    // Rebuilt by rebuild_lut() whenever delimiter changes.
    uint8_t lut[256];

    CsvParseContext() { rebuild_lut(); }

    // Call after changing `delimiter` to keep the LUT consistent.
    void rebuild_lut() {
        std::memset(lut, 0, 256);
        lut[static_cast<uint8_t>('\n')]      = 1;  // NEWLINE
        lut[static_cast<uint8_t>('\r')]      = 2;  // CR
        lut[static_cast<uint8_t>(delimiter)] = 3;  // DELIMITER
        lut[static_cast<uint8_t>('"')]       = 4;  // QUOTE
        lut[static_cast<uint8_t>('\\')]      = 5;  // BACKSLASH
    }
};

// LUT index → marker identity. Values match CsvParseContext::lut[] (value - 1).
enum class CsvMarkerType : uint8_t {
    NEWLINE   = 0,
    CR        = 1,
    DELIMITER = 2,
    QUOTE     = 3,
    BACKSLASH = 4,
};

struct CsvMarkerPosition {
    uint32_t       position;
    CsvMarkerType  type;
};

}  // namespace rugo::_csv
