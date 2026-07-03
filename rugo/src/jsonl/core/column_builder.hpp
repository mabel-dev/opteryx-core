#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>

#include "markers.hpp"
#include "value_parser.hpp"
#include "field_span.hpp"
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

enum class ColumnType : uint8_t {
    Int64   = 0,
    Float64 = 1,
    Bool    = 2,
    String  = 3,
    Null    = 4,
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
    bool                                       may_have_escapes = false
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
    void*             data     = nullptr;          // typed buffer (own_raw)
    DrakenStringSlot* slots    = nullptr;          // string slots (own_string)
    uint8_t*          arena    = nullptr;
    size_t            arena_len = 0;
};

// Parse every named column from the document map in parallel (one task per column,
// thread pool capped at max_threads / hardware_concurrency / column count). Pure C++,
// no Python — safe to call with the GIL released. Returns one ParsedColumn per name.
std::vector<ParsedColumn> parse_all_columns(
    const uint8_t*                             buffer,
    const RecordSet& records,
    const std::vector<std::string>&            column_names,
    size_t                                     max_threads,
    bool                                       may_have_escapes = false
);

// Wrap a ParsedColumn into an owned Draken Vector. Creates a Python object — call under
// the GIL. Returns a NEW reference, or NULL with an exception set on failure.
PyObject* wrap_column(ParsedColumn& pc);

}  // namespace rugo::_jsonl
