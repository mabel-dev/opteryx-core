#ifndef _JSONL_FIELD_SPAN_HPP_
#define _JSONL_FIELD_SPAN_HPP_

#include <vector>
#include <cstdint>
#include "markers.hpp"
#include "parse_context.hpp"

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
    // FieldSpans for all complete records: [record_idx][field_idx]
    // field_idx is in the order of projected_columns in ParseContext
    std::vector<std::vector<FieldSpan>> all_records;

    // Bytes consumed from the buffer
    size_t bytes_consumed = 0;

    // Inferred schema (if ParseContext.infer_schema = true)
    std::map<std::string, std::string> inferred_schema;

    // Number of records that passed predicates
    size_t num_records_passed = 0;
};

// Stateless interpreter: process buffer with projection, predicates, schema
// Takes marker array from SIMD scan, produces FieldSpans for complete records
InterpreterResult interpret_jsonl(
    const uint8_t* buffer_data,
    size_t buffer_length,
    const std::vector<MarkerPosition>& markers,
    const ParseContext& context,
    OrdinalPredictor& predictor  // Updated in-place
);

}  // namespace rugo::_jsonl

#endif  // _JSONL_FIELD_SPAN_HPP_
