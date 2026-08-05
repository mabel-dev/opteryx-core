#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>

#include "markers.hpp"
#include "value_parser.hpp"
#include "field_span.hpp"
#include "parse_context.hpp"
#include "buffers.h"       // DrakenType
#include "string_slot.h"   // DrakenStringSlot

// PyObject forward declaration — lets this header declare a Vector-producing
// function without forcing Python.h include-order onto every consumer.
// When Python.h is already included (the .cpp and the Cython TU both do),
// Py_PYTHON_H is defined and the real declaration is used instead.
#ifndef Py_PYTHON_H
struct _object;
typedef struct _object PyObject;
#endif

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
    std::vector<uint32_t> offsets;    // start position of each row in data
    std::vector<uint32_t> lengths;    // length of each row's value
    std::vector<uint8_t>  null_bitmap; // null marker: bit=1 (valid), bit=0 (null)
    bool data_owned = false;          // offsets index into `data` (copy/unescape), not the buffer
    bool any_value_seen = false;      // true iff at least one row resolved a non-null value
                                       // (false => the column is absent/null on every row)

    uint8_t*  data_ptr()   { return data.empty() ? nullptr : data.data(); }
    uint32_t* offset_ptr() { return offsets.empty() ? nullptr : offsets.data(); }
    uint32_t* length_ptr() { return lengths.empty() ? nullptr : lengths.data(); }
    uint8_t*  bitmap_ptr() { return null_bitmap.empty() ? nullptr : null_bitmap.data(); }
};

// Extract one column (ordinal prediction for fast key lookup).
//   copy_bytes = true  : offsets index into result.data, which holds a copy of each
//                        slice (needed when slices must outlive `buffer`, e.g. the
//                        multi-chunk merge concatenates several buffers).
//   copy_bytes = false : no copy — offsets index into the original `buffer`, and the
//                        builder must be given that same buffer as its `base`. Saves a
//                        full copy of the column's bytes (the single-chunk fast path).
// may_have_escapes: when true AND the column is a string, values are JSON-unescaped into
// result.data (forcing copy mode; result.data_owned is set). Gate it on a cheap buffer-wide
// '\' check so escape-free data keeps the zero-copy fast path. Check result.data_owned to
// pick the builder's base (result.data_ptr() vs the original buffer).
StringColumnResult extract_column(
    const uint8_t*                            buffer,
    const RecordSet& records,
    const std::string&                         column_name,
    OrdinalPredictor&                         predictor,
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
    const RowExec*                             rows = nullptr
);

// Build an owned Draken VARCHAR Vector from an extracted column. Slice bytes are read
// from `base + offsets[i]`: pass scr.data_ptr() in copy mode, or the original buffer
// in no-copy mode. Allocates draken_malloc slot + arena + validity, populates
// German-string slots in one pass, transfers ownership via draken_vector_own_string.
// Python edge lives here in C++; the .pyx stays typed-only. Returns a NEW reference.
PyObject* build_varchar_vector(const uint8_t* base, StringColumnResult& scr);

// Build an owned typed Draken Vector from an extracted column, using the column's
// inferred type as a cheap speculative hint and validating by parse. Number columns
// try int64, then widen to float64, then fall back to VARCHAR; bool tries true/false
// then falls back to VARCHAR; string/null go straight to VARCHAR. The prediction is
// never load-bearing — a parse miss falls back to an always-correct path. Slice bytes
// are read from `base + offsets[i]` (see build_varchar_vector). Returns a NEW reference.
PyObject* build_typed_vector(const uint8_t* base, StringColumnResult& scr);

// A column parsed into owned draken_malloc buffers, ready to be wrapped into a Draken
// Vector. Holds NO Python objects, so it can be produced off the GIL (in parallel) and
// wrapped serially under the GIL via wrap_column(). Buffer ownership transfers to the
// Vector on wrap; this is a plain carrier with no destructor.
struct ParsedColumn {
    DrakenType        type     = DRAKEN_VARCHAR;
    uint32_t          length   = 0;
    uint8_t*          validity = nullptr;          // draken_malloc'd or NULL (all valid)
    bool              is_string = false;
    bool              all_null = false;             // every row absent/null (schema reporting)
    void*             data     = nullptr;          // typed buffer (own_raw)
    DrakenStringSlot* slots    = nullptr;          // string slots (own_string)
    uint8_t*          arena    = nullptr;
    size_t            arena_len = 0;

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
    // requested, but some row's array contained nested containers or a heterogeneous
    // mix of scalar element types (out of v1 scope) — the column fell back to raw
    // JSON text (DRAKEN_VARCHAR) instead, same as parse_arrays=False. The caller
    // (Cython edge, under the GIL) surfaces this as a Python warning; C++ itself never
    // warns because parse_all_columns runs off the GIL.
    bool              array_fallback = false;
};

// Parse every named column from the document map in parallel (one task per column,
// thread pool capped at max_threads / hardware_concurrency / column count). Pure C++,
// no Python — safe to call with the GIL released. Returns one ParsedColumn per name.
//
// context.explicit_schema: a column named here skips speculative type inference entirely
// and is parsed STRICTLY as the declared type ("int64" | "double" | "boolean" | "string");
// a value that doesn't fit throws std::invalid_argument (caller must catch/translate —
// unlike the default speculative path, a declared-schema mismatch is a real data/schema
// error, not something to silently fall back past). context.infer_sample_size bounds the
// speculative-path type hint window for undeclared columns (see extract_column).
std::vector<ParsedColumn> parse_all_columns(
    const uint8_t*                             buffer,
    const RecordSet& records,
    const std::vector<std::string>&            column_names,
    size_t                                     max_threads,
    bool                                       may_have_escapes,
    const ParseContext&                        context
);

// Wrap a ParsedColumn into an owned Draken Vector. Creates a Python object — call under
// the GIL. Returns a NEW reference, or NULL with an exception set on failure.
PyObject* wrap_column(ParsedColumn& pc);

}  // namespace rugo::_jsonl
