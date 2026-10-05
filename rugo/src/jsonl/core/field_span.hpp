#ifndef _JSONL_FIELD_SPAN_HPP_
#define _JSONL_FIELD_SPAN_HPP_

#include <vector>
#include <string>
#include <cstdint>
#include <cstddef>
#include "markers.hpp"
#include "parse_context.hpp"
#include "interpreter.hpp"   // ColumnMap

namespace rugo::_jsonl {

// Result of interpreting a buffer
struct InterpreterResult {
    // Column-major document map: for each requested column, one span per surviving row
    // (records that are well-formed lines and pass every predicate), in input order.
    ColumnMap all_records;

    // Bytes consumed from the buffer
    size_t bytes_consumed = 0;

    // Number of records that passed predicates (== all_records.rows)
    size_t num_records_passed = 0;
};

// Stateless interpreter: maps [range_start, buffer_length) of `buffer_data` into the
// columns named by `columns` (build_columns), with the context's predicates pushed down
// (interpret_jsonl_threaded passes one newline-aligned range at a time).
//
// `columns` is the output — discover_column_names' result, plus any declared column it
// lacks — in output order; ColumnMap::cols[i] is columns[i]. A predicate on a column not in
// `columns` is evaluated but not output.
InterpreterResult interpret_jsonl(
    const uint8_t* buffer_data,
    size_t buffer_length,
    const ParseContext& context,
    const std::vector<std::string>& columns,
    size_t range_start = 0,
    const std::vector<LineSpan>* lines = nullptr  // prefilter survivors — see build_columns
);


// Multithreaded interpret: splits the buffer into newline-aligned ranges, maps each on a
// thread pool, and appends the ranges' columns in order. max_threads == 0 uses
// hardware_concurrency. No intermediate copies of the input — all ranges share one buffer.
//
// use_prefilter: run the raw Volnitsky prefilter (jsonl_reader.hpp) INSIDE each range
// task. The gate decides once, on the buffer's head; then every task cuts its own range
// into segments (prefilter_plan_segments, re-judged per 4MB window) and scans the
// surviving lines of the filtered ones in place, the unfiltered ones by the normal path —
// the prefilter runs on every thread, and nothing is copied.
InterpreterResult interpret_jsonl_threaded(
    const uint8_t* buffer_data,
    size_t buffer_length,
    const ParseContext& context,
    const std::vector<std::string>& columns,
    size_t max_threads = 0,
    bool use_prefilter = false
);

}  // namespace rugo::_jsonl

#endif  // _JSONL_FIELD_SPAN_HPP_
