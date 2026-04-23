#include "field_span.hpp"
#include "interpreter.hpp"
#include "structural_scan.hpp"
#include "value_parser.hpp"
#include <algorithm>
#include <map>
#include <cctype>

namespace rugo::_jsonl {

// OrdinalPredictor implementation
std::vector<uint16_t> OrdinalPredictor::get_candidates(const std::string& key) const {
    auto it = histories.find(key);
    if (it == histories.end()) {
        return {};  // No history yet, no prediction
    }

    const auto& history = it->second;
    if (history.disabled) {
        return {};  // Prediction disabled for this key
    }

    // Count occurrences of each ordinal in the history
    std::map<uint16_t, uint8_t> ordinal_counts;
    std::map<uint16_t, uint8_t> ordinal_recency;  // Position in circular buffer (higher = more recent)

    for (size_t i = 0; i < HISTORY_SIZE; ++i) {
        uint16_t ord = history.ordinals[i];
        if (ord != 0xFFFF) {  // 0xFFFF means not found
            ordinal_counts[ord]++;
            ordinal_recency[ord] = (history.position >= i) ?
                                    (history.position - i) :
                                    (history.position + HISTORY_SIZE - i);
        }
    }

    // Build candidate list based on heuristics
    std::vector<uint16_t> candidates;

    // First: ordinals appearing 5+ times (very stable)
    for (const auto& [ord, count] : ordinal_counts) {
        if (count >= 5) {
            candidates.push_back(ord);
        }
    }

    // Second: ordinals appearing 3-4 times (reasonably stable), sorted by recency
    std::vector<uint16_t> secondary;
    for (const auto& [ord, count] : ordinal_counts) {
        if (count >= 3 && count < 5) {
            secondary.push_back(ord);
        }
    }
    std::sort(secondary.begin(), secondary.end(),
              [&ordinal_recency](uint16_t a, uint16_t b) {
                  return ordinal_recency[a] > ordinal_recency[b];
              });
    candidates.insert(candidates.end(), secondary.begin(), secondary.end());

    // If we have no candidates (entropy), return empty and let caller brute force
    if (candidates.empty()) {
        return {};
    }

    return candidates;
}

void OrdinalPredictor::update_history(const std::string& key, uint16_t ordinal) {
    auto& history = histories[key];

    // Add ordinal to circular buffer
    history.ordinals[history.position] = ordinal;
    history.position = (history.position + 1) % HISTORY_SIZE;

    // Track brute-force fallbacks (ordinal = 0xFFFF means not found)
    // TODO: Phase 4 - count consecutive not-found to disable prediction
}

void OrdinalPredictor::disable_key(const std::string& key) {
    if (auto it = histories.find(key); it != histories.end()) {
        it->second.disabled = true;
    }
}

// Interpreter implementation
InterpreterResult interpret_jsonl(
    const uint8_t* buffer_data,
    size_t buffer_length,
    const std::vector<MarkerPosition>& markers,
    const ParseContext& context,
    OrdinalPredictor& predictor) {

    InterpreterResult result;

    if (buffer_length == 0) {
        result.bytes_consumed = 0;
        return result;
    }

    // Build index: for quick lookup of marker positions
    std::map<std::string, uint32_t> marker_index;  // Unused for now, but structure is ready

    // Find record boundaries (NEWLINE markers)
    std::vector<std::pair<uint32_t, uint32_t>> record_bounds;  // (start, end)
    uint32_t record_start = 0;

    for (const auto& marker : markers) {
        if (marker.marker_type == static_cast<uint8_t>(MarkerType::NEWLINE)) {
            record_bounds.push_back({record_start, marker.position});
            record_start = marker.position + 1;
        }
    }

    // Add final record if it doesn't end with newline
    if (record_start < buffer_length) {
        // Check if this record is complete (has closing brace)
        bool has_closing_brace = false;
        for (uint32_t i = record_start; i < buffer_length; ++i) {
            if (buffer_data[i] == '}') {
                has_closing_brace = true;
                break;
            }
        }

        if (has_closing_brace) {
            record_bounds.push_back({record_start, static_cast<uint32_t>(buffer_length)});
        } else {
            // Incomplete record at end: will be consumed next time
            // bytes_consumed = record_start means this partial record is not consumed
            result.bytes_consumed = record_start;
            return result;
        }
    }

    // Parse each record
    RecordInterpreter rec_interp;
    result.all_records.reserve(record_bounds.size());

    for (const auto& [rec_start, rec_end] : record_bounds) {
        auto all_fields = rec_interp.parse_record(buffer_data, rec_start, rec_end, markers, marker_index);

        if (all_fields.empty()) {
            continue;  // Skip empty records
        }

        // Apply predicates: filter records that don't match
        bool passes_predicates = true;
        for (const auto& pred : context.predicates) {
            // Find the field with this column name
            bool found = false;
            for (const auto& field : all_fields) {
                // Extract key name
                uint32_t key_len = field.key_end - field.key_start + 1;
                std::string field_name(
                    reinterpret_cast<const char*>(buffer_data + field.key_start),
                    key_len
                );

                if (field_name == pred.column) {
                    // Evaluate predicate on this field
                    if (!evaluate_predicate(buffer_data, field, pred)) {
                        passes_predicates = false;
                        break;
                    }
                    found = true;
                    break;
                }
            }

            // If predicate column not found in record, treat as NULL (fails predicate)
            if (!found) {
                passes_predicates = false;
                break;
            }

            if (!passes_predicates) {
                break;
            }
        }

        if (!passes_predicates) {
            continue;  // Skip records that don't match predicates
        }

        // Apply projection: extract only requested columns
        std::vector<FieldSpan> projected_fields;

        if (context.projected_columns.empty()) {
            // No projection: include all fields
            projected_fields = all_fields;
        } else {
            // Projection: include only requested columns in order
            for (const auto& col_name : context.projected_columns) {
                for (const auto& field : all_fields) {
                    uint32_t key_len = field.key_end - field.key_start + 1;
                    std::string field_name(
                        reinterpret_cast<const char*>(buffer_data + field.key_start),
                        key_len
                    );

                    if (field_name == col_name) {
                        projected_fields.push_back(field);
                        break;
                    }
                }

                // If column not in record, add placeholder with NULL value
                // TODO: Phase 6 - handle missing columns (emit NULL)
            }
        }

        if (!projected_fields.empty()) {
            result.all_records.push_back(projected_fields);
            result.num_records_passed++;
        }
    }

    // All complete records have been consumed
    if (!record_bounds.empty()) {
        result.bytes_consumed = record_bounds.back().second;
        if (record_bounds.back().second < buffer_length &&
            buffer_data[record_bounds.back().second] == '\n') {
            result.bytes_consumed++;
        }
    } else {
        result.bytes_consumed = 0;
    }

    return result;
}

}  // namespace rugo::_jsonl
