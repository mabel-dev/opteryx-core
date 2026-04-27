#include "field_span.hpp"
#include "interpreter.hpp"
#include "structural_scan.hpp"
#include "value_parser.hpp"
#include <algorithm>
#include <map>
#include <unordered_map>
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


    // Fast hash-based field lookup to avoid string extraction
    // Pre-compute hash of each predicate/projection column name once
    auto hash_span = [](const uint8_t* data, size_t len) -> uint64_t {
        uint64_t h = 0xcbf29ce484222325ULL;
        for (size_t i = 0; i < len; ++i) {
            h ^= data[i];
            h *= 0x100000001b3ULL;
        }
        return h;
    };

    // Build lookup maps: hash -> (ordinal, predicate_index)
    std::unordered_map<uint64_t, int> pred_hash_to_idx;
    std::unordered_map<uint64_t, int> proj_hash_to_idx;

    for (size_t i = 0; i < context.predicates.size(); ++i) {
        auto col_data = reinterpret_cast<const uint8_t*>(context.predicates[i].column.data());
        auto col_len = context.predicates[i].column.size();
        uint64_t h = hash_span(col_data, col_len);
        if (pred_hash_to_idx.find(h) == pred_hash_to_idx.end()) {
            pred_hash_to_idx[h] = i;
        }
    }

    for (size_t i = 0; i < context.projected_columns.size(); ++i) {
        auto col_data = reinterpret_cast<const uint8_t*>(context.projected_columns[i].data());
        auto col_len = context.projected_columns[i].size();
        uint64_t h = hash_span(col_data, col_len);
        if (proj_hash_to_idx.find(h) == proj_hash_to_idx.end()) {
            proj_hash_to_idx[h] = i;
        }
    }

    // Build document map from markers (linear pass)
    auto all_records = build_map(buffer_data, buffer_length, markers);
    result.all_records.reserve(all_records.size());

    for (const auto& all_fields : all_records) {

        if (all_fields.empty()) {
            continue;  // Skip empty records
        }

        // Build field lookup using hashes: O(n_fields) scan with O(1) hash lookups
        std::unordered_map<int, const FieldSpan*> fields_by_pred_idx;
        std::unordered_map<int, const FieldSpan*> fields_by_proj_idx;

        for (const auto& field : all_fields) {
            uint64_t field_hash = hash_span(buffer_data + field.key_start, field.key_width);

            // O(1) lookup in predicate hash map
            auto pred_it = pred_hash_to_idx.find(field_hash);
            if (pred_it != pred_hash_to_idx.end()) {
                fields_by_pred_idx[pred_it->second] = &field;
            }

            // O(1) lookup in projection hash map
            auto proj_it = proj_hash_to_idx.find(field_hash);
            if (proj_it != proj_hash_to_idx.end()) {
                fields_by_proj_idx[proj_it->second] = &field;
            }
        }

        // Apply predicates: filter records that don't match
        bool passes_predicates = true;
        for (size_t i = 0; i < context.predicates.size(); ++i) {
            auto it = fields_by_pred_idx.find(i);
            if (it == fields_by_pred_idx.end()) {
                passes_predicates = false;
                break;
            }

            if (!evaluate_predicate(buffer_data, *it->second, context.predicates[i])) {
                passes_predicates = false;
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
            for (size_t i = 0; i < context.projected_columns.size(); ++i) {
                auto it = fields_by_proj_idx.find(i);
                if (it != fields_by_proj_idx.end()) {
                    projected_fields.push_back(*it->second);
                }
                // If column not in record, skip (will be filled as NULL in Phase 6)
            }
        }

        if (!projected_fields.empty()) {
            result.all_records.push_back(projected_fields);
            result.num_records_passed++;
        }
    }

    // Bytes consumed: find the position after the last newline marker
    result.bytes_consumed = 0;
    for (const auto& marker : markers) {
        if (marker.marker_type == static_cast<uint8_t>(MarkerType::NEWLINE)) {
            result.bytes_consumed = marker.position + 1;
        }
    }
    // If no newlines, we consumed up to the end of the last complete record
    // (build_map only returns complete records)
    if (result.bytes_consumed == 0 && !all_records.empty()) {
        result.bytes_consumed = buffer_length;
    }

    return result;
}

}  // namespace rugo::_jsonl
