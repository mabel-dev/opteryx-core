#ifndef _JSONL_FIELD_SPAN_HPP_
#define _JSONL_FIELD_SPAN_HPP_

#include <vector>
#include <cstdint>
#include <cstddef>
#include "markers.hpp"
#include "parse_context.hpp"
#include "interpreter.hpp"   // RecordSet

namespace rugo::_jsonl {

// Ordinal predictor: rolling history of key positions
class OrdinalPredictor {
public:
    static constexpr size_t HISTORY_SIZE = 8;

    struct KeyHistory {
        uint16_t ordinals[HISTORY_SIZE];  // Last 8 ordinals (-1 if not found)
        uint8_t position = 0;             // Circular buffer index
        bool disabled = false;            // Disable for high-entropy keys
    };

    // Predict next ordinal for a key
    std::vector<uint16_t> get_candidates(const std::string& key) const;

    // Update history with observed ordinal
    void update_history(const std::string& key, uint16_t ordinal);

    // Mark a key as disabled (high-entropy, no prediction)
    void disable_key(const std::string& key);

private:
    std::map<std::string, KeyHistory> histories;
};

// Result of interpreting a buffer
struct InterpreterResult {
    // Flat-arena document map: all surviving records' fields, contiguous, with per-record
    // offsets. Field order within a record is file order (the projected subset when a
    // projection is applied).
    RecordSet all_records;

    // Bytes consumed from the buffer
    size_t bytes_consumed = 0;

    // Inferred schema (if ParseContext.infer_schema = true)
    std::map<std::string, std::string> inferred_schema;

    // Number of records that passed predicates
    size_t num_records_passed = 0;
};

// Stateless interpreter: process buffer with projection, predicates, schema.
// Maps [range_start, buffer_length) of `buffer_data` (build_map) and produces FieldSpans
// for complete records (interpret_jsonl_threaded passes one newline-aligned range at a
// time).
InterpreterResult interpret_jsonl(
    const uint8_t* buffer_data,
    size_t buffer_length,
    const ParseContext& context,
    OrdinalPredictor& predictor,  // Updated in-place
    size_t range_start = 0,
    const std::vector<LineSpan>* lines = nullptr  // prefilter survivors — see build_map
);


// Multithreaded scan + interpret: splits the buffer into newline-aligned ranges,
// processes each on a thread pool, and merges the records in order. max_threads == 0
// uses hardware_concurrency. No intermediate copies — all ranges share one buffer.
//
// use_prefilter: run the raw Volnitsky prefilter (jsonl_reader.hpp) INSIDE each range
// task. The gate decides once, on the buffer's head; then every task finds the surviving
// lines of its own range (prefilter_lines) and scans only those, in place — the prefilter
// runs on every thread, and nothing is copied.
InterpreterResult interpret_jsonl_threaded(
    const uint8_t* buffer_data,
    size_t buffer_length,
    const ParseContext& context,
    OrdinalPredictor& predictor,
    size_t max_threads = 0,
    bool use_prefilter = false
);

}  // namespace rugo::_jsonl

#endif  // _JSONL_FIELD_SPAN_HPP_
