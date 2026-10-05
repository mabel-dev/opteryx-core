#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>

#include "markers.hpp"
#include "value_parser.hpp"
#include "field_span.hpp"
#include "interpreter.hpp"   // SpanBases
#include "parse_context.hpp"
#include "buffers.h"       // DrakenType
#include "string_slot.h"   // DrakenStringSlot
#include "declared_type.hpp"   // DeclaredType — explicit_schema's type vocabulary

namespace rugo::_jsonl {

// Row-range parallel executor (defined in column_builder.cpp). Only ever passed by pointer
// here, so the thread-pool header stays out of this one. nullptr == run serially.
class RowExec;

enum class ColumnType : uint8_t {
    Int64   = 0,
    Float64 = 1,
    Bool    = 2,
    String  = 3,
    Null    = 4,
    Array   = 5,  // first sampled value was a JSON array; see parse_context's parse_arrays
    Variant = 6,  // first sampled value was a JSON object; see parse_context's parse_objects
};

// String column result: raw bytes extracted from JSON (no parsing)
struct StringColumnResult {
    ColumnType inferred_type = ColumnType::String;  // type hint from first non-null value
    size_t num_rows = 0;
    std::vector<uint8_t>  data;       // concatenated string values
    std::vector<uint32_t> offsets;    // start position of each row in `bases`
    std::vector<uint32_t> lengths;    // length of each row's value
    std::vector<uint8_t>  null_bitmap; // null marker: bit=1 (valid), bit=0 (null)
    bool data_owned = false;          // offsets index into `data` (copy/unescape), not the buffer
    bool any_value_seen = false;      // true iff at least one row resolved a non-null value
                                       // (false => the column is absent/null on every row)
    bool any_key_seen = false;        // true iff at least one row CARRIED the key at all
                                       // (a present key with a null value counts; an absent
                                       // key does not) — distinguishes "sparse" from "not
                                       // in this data", which any_value_seen cannot
    // Per-row value SHAPE (markers.hpp ValueType), filled only when extract_column is
    // asked for it (RecordValueTypes). A string value's slice is its content between
    // the quotes, so a string "[1]" and an array [1] are byte-identical as slices; this is
    // the only way a consumer can tell them apart. Rows with no value hold Unknown.
    // Empty when recording was not requested (or, under IfArrayHinted, the hint was not
    // Array) — a consumer that needs it must check the size, never assume.
    std::vector<uint8_t>  value_types;

    // What `offsets` index, row by row: `data` when data_owned, else the column's source
    // bytes (ColumnMap::bases — one base per chunk of an input read in chunks).
    SpanBases bases;

    uint8_t*  data_ptr()   { return data.empty() ? nullptr : data.data(); }
    uint32_t* offset_ptr() { return offsets.empty() ? nullptr : offsets.data(); }
    uint32_t* length_ptr() { return lengths.empty() ? nullptr : lengths.data(); }
    uint8_t*  bitmap_ptr() { return null_bitmap.empty() ? nullptr : null_bitmap.data(); }
};

// Whether extract_column fills StringColumnResult::value_types.
enum class RecordValueTypes : uint8_t {
    Never,          // default: no consumer reads the shape, so no row pays the byte write
    Always,         // declared ARRAY<T>/VARIANT: the strict per-row check needs every shape
    IfArrayHinted,  // speculative path: fill only when the sample hint resolves to Array,
                    // so parse_array_column can refuse a string row that merely looks like
                    // an array; every other column pays nothing
};

// Extract one column from its ColumnMap spans (one per row; span_absent = not carried).
//   copy_bytes = true  : offsets index into result.data, which holds a copy of each
//                        slice (needed when slices must outlive `buffer`, e.g. the
//                        multi-chunk merge concatenates several buffers).
//   copy_bytes = false : no copy — offsets index the column's source bytes (`source`,
//                        per chunk). Saves a full copy of the column's bytes.
// may_have_escapes: when true AND the column is a string, values are JSON-unescaped into
// result.data (forcing copy mode; result.data_owned is set). Gate it on a cheap buffer-wide
// '\' check so escape-free data keeps the zero-copy fast path. result.bases says what the
// offsets index either way. Throws std::length_error when a copied column's bytes exceed
// 4 GiB (its offsets are uint32_t).
StringColumnResult extract_column(
    const SpanBases&                          source,
    const std::vector<FieldSpan>&             col,
    bool                                       copy_bytes = true,
    bool                                       may_have_escapes = false,
    // Only the first `sample_size` rows are consulted for the type hint
    // (ParseContext.infer_sample_size), taken from the first non-null value in that
    // window. parse_typed_column always validates the WHOLE column against whatever
    // hint (if any) is chosen and falls back to VARCHAR on a mismatch, so no value is
    // ever misparsed — but if the sample window is entirely null, no hint forms at all
    // and the column is typed VARCHAR even where a larger sample would have picked a
    // narrower type.
    size_t                                     sample_size = SIZE_MAX,
    // Splits the row walk across workers. nullptr (the default) runs it serially in the
    // calling thread — required when the caller is itself already one task per column.
    const RowExec*                             rows = nullptr,
    // Fill result.value_types (one ValueType per row) — see RecordValueTypes. Off by
    // default: only a declared ARRAY/VARIANT column or an Array-hinted speculative column
    // needs the shape, and every other column would pay a byte write per row for nothing.
    RecordValueTypes                           record_value_types = RecordValueTypes::Never
);

// A column parsed into owned draken_malloc buffers, ready to be wrapped into a Draken
// Vector. Holds NO Python objects, so it can be produced off the GIL (in parallel) and
// wrapped serially under the GIL via wrap_column() (jsonl/_jsonl_column_wrap.hpp —
// the Python edge, kept out of this pure-C++ core). Buffer ownership transfers to the
// Vector on wrap; this is a plain carrier with no destructor.
struct ParsedColumn {
    DrakenType        type     = DRAKEN_VARCHAR;
    uint32_t          length   = 0;
    uint8_t*          validity = nullptr;          // draken_malloc'd or NULL (all valid)
    bool              is_string = false;
    bool              all_null = false;             // every row absent/null (schema reporting)
    // Declared (explicit_schema) columns only: the key never appeared in ANY record. A
    // caller pinning one chunk's schema onto another needs this to tell a column that is
    // merely sparse here from one this data does not have at all — an all-null typed
    // column is the RIGHT answer for both, so the vector alone cannot say which.
    bool              key_absent = false;
    void*             data     = nullptr;          // typed buffer (own_raw)
    DrakenStringSlot* slots    = nullptr;          // string slots (own_string)
    uint8_t*          arena    = nullptr;
    size_t            arena_len = 0;
    // Dict-shaped string column (ParseContext::intern_nested_text): `slots` holds
    // `data_length` UNIQUE values and `codes` (draken_malloc'd, `length` entries) is the
    // per-row selection. A NULL row is a validity bit; its code is 0 and never read.
    // nullptr = dense (slots has `length` entries).
    uint32_t*         codes    = nullptr;
    uint32_t          data_length = 0;

    // ARRAY-only fields (type == DRAKEN_ARRAY): child element buffers, one parent-offset
    // pair per row. Child is EITHER a string-family vector (child_slots/child_arena, when
    // child_type is VARCHAR/NVARCHAR/VARBINARY) OR a fixed-width numeric/bool vector
    // (child_data, when child_type is INT64/FLOAT64/BOOL) — never both.
    int32_t*          array_parent_offsets = nullptr;  // draken_malloc'd int32_t[length+1]
    DrakenType         array_child_type = DRAKEN_VARCHAR;
    uint32_t           array_child_length = 0;
    uint8_t*           array_child_validity = nullptr;
    DrakenStringSlot*  array_child_slots = nullptr;
    uint8_t*           array_child_arena = nullptr;
    size_t             array_child_arena_len = 0;
    void*              array_child_data = nullptr;

    // Set when a column's first-sampled value was a JSON array, parse_arrays was
    // requested, but some row was out of v1 scope — a non-array value (including a
    // string whose text merely looks like an array), malformed array text, nested
    // containers, or a heterogeneous mix of scalar element types — so the column fell
    // back to raw JSON text (DRAKEN_VARCHAR) instead, same as parse_arrays=False. The caller
    // (Cython edge, under the GIL) surfaces this as a Python warning; C++ itself never
    // warns because parse_all_columns runs off the GIL.
    bool              array_fallback = false;

    // Logical-type descriptor for a column parsed from an explicit_schema
    // declaration. Ordinals from draken/logical_type.h, zero (LogicalKind::NONE)
    // for everything else. IPV4 is the reason this exists: it shares UINT32's
    // physical tag, so a vector carrying the right 32 bits and no descriptor is
    // an ordinary unsigned column and every consumer that needs IPv4 either
    // renders it as an integer or refuses outright. Carried through to
    // wrap_column, which attaches it via draken_vector_own_raw_logical.
    uint8_t           logical_kind   = 0;
    uint8_t           unit           = 2;   // TimestampUnit: microseconds
    int16_t           offset_minutes = 0;
    uint8_t           precision      = 0;
    uint8_t           scale          = 0;
};

// Parse every named column from the column-major document map (map.cols[i] holds
// column_names[i] — the same list interpret_jsonl_threaded was given), in parallel. Pure
// C++, no Python — safe to call with the GIL released. Returns one ParsedColumn per name.
//
// context.explicit_schema: a column named here skips speculative type inference entirely
// and is parsed STRICTLY as the declared type. The vocabulary is the platform's canonical
// type names — see rugo/src/declared_type.hpp for the full list and rugo/src/declared_parse.hpp
// for the per-value contract. A value that doesn't fit throws std::invalid_argument (caller
// must catch/translate — unlike the default speculative path, a declared-schema mismatch is a
// real data/schema error, not something to silently fall back past).
// context.infer_sample_size bounds the speculative-path type hint window for undeclared
// columns (see extract_column).
std::vector<ParsedColumn> parse_all_columns(
    const uint8_t*                             buffer,
    const ColumnMap&                           map,
    const std::vector<std::string>&            column_names,
    size_t                                     max_threads,
    bool                                       may_have_escapes,
    const ParseContext&                        context
);

}  // namespace rugo::_jsonl
