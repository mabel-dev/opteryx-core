#ifndef _JSONL_INTERPRETER_HPP_
#define _JSONL_INTERPRETER_HPP_

#include <vector>
#include <map>
#include <string>
#include <cstdint>
#include <optional>

#include "markers.hpp"
#include "parse_context.hpp"

namespace rugo::_jsonl {

// A column to emit from each record, matched by exact bytes (length + first-byte fast
// reject, then memcmp — no hashing). The wanted set is projected ∪ predicate columns.
// pred_idx is the index into MapProjection::predicates when this column carries an
// inline-evaluated predicate, else -1.
struct WantedColumn {
    const char* name;
    uint32_t    len;
    uint8_t     first;     // name[0], for fast reject
    int         pred_idx;  // predicate index, or -1
};

// Projection + predicate pushdown for build_map. nullptr => emit every field (data-blind
// full map, the right shape for a full read). When set, build is MINIMAL EXTENT: only
// fields whose key matches a wanted column are materialized, and once all `num_wanted` are
// found the rest of the record is skipped to the newline (no stepping, no materialization
// of the tail). Predicate columns are evaluated INLINE the moment their value is emitted —
// a failing row is dropped and skipped right there, so failing rows never materialize
// their later columns. Ordinals count every field, so emitted spans carry true positions.
struct MapProjection {
    const std::vector<WantedColumn>* columns;
    size_t                           num_wanted;
    const std::vector<Predicate>*    predicates;
};

// A view over one record's fields inside a RecordSet's flat span arena. Cheap to copy
// (pointer + length); supports range-for and indexing so consumers read it like the old
// per-record vector.
struct RecordView {
    const FieldSpan* ptr = nullptr;
    uint32_t         n   = 0;
    const FieldSpan* begin() const { return ptr; }
    const FieldSpan* end()   const { return ptr + n; }
    size_t           size()  const { return n; }
    bool             empty() const { return n == 0; }
    const FieldSpan& operator[](size_t i) const { return ptr[i]; }
};

// Flat-arena document map: every field of every record lives in one contiguous `spans`
// buffer, with per-record ranges [offsets[r], offsets[r+1]). Replaces the old
// std::vector<std::vector<FieldSpan>> (one malloc per record) — the build allocates two
// growing buffers instead of N+1, which dominates map-build cost on narrow rows.
struct RecordSet {
    std::vector<FieldSpan> spans;     // all fields of all records, contiguous
    std::vector<uint32_t>  offsets;   // size = num_records + 1; starts {0}

    size_t     num_records() const { return offsets.empty() ? 0 : offsets.size() - 1; }
    size_t     size()        const { return num_records(); }
    RecordView operator[](size_t r) const {
        return RecordView{ spans.data() + offsets[r], offsets[r + 1] - offsets[r] };
    }
    // Append another set's records (offsets rebased onto this set's span arena).
    void append(const RecordSet& other) {
        const uint32_t base = static_cast<uint32_t>(spans.size());
        spans.insert(spans.end(), other.spans.begin(), other.spans.end());
        if (offsets.empty()) offsets.push_back(0);
        for (size_t i = 1; i < other.offsets.size(); ++i)
            offsets.push_back(base + other.offsets[i]);
    }
};

// Build the document map from structural markers (linear single pass).
//
// Value shape is coarse (string / array / object / scalar) and read only from the
// structural delimiter — no value parsing. Container values ([…], {…}) are bounded with a
// string-and-escape-aware byte scan so interior commas/brackets do not truncate them.
// Key identity is never hashed; with a projection it is matched by exact bytes only for
// the wanted set, materialising only those fields and stopping each record once they are
// found (minimal extent). Predicate filtering and final column ordering are the consumer's
// job (finalize_records / extract_column).
RecordSet build_map(
    const uint8_t* buffer,
    size_t buffer_length,
    const std::vector<MarkerPosition>& markers,
    const MapProjection* proj = nullptr
);

// Collect the keys of a RecordSet's first record as strings (for column-name discovery at
// the Cython edge, so RecordSet's internals stay opaque to Cython).
std::vector<std::string> first_record_keys(const RecordSet& rs, const uint8_t* buffer);

// SPIKE: data-blind document-map build driven by a structural bitmap (set bit => structural
// byte) instead of a marker vector. Iterates set bits (ctz + blsr); identical FSM and output
// to the no-projection build_map. Measures whether the compact bitmap index beats the
// ~8-bytes-per-marker vector on the memory-bound scan+build path.
RecordSet build_map_bitmap(
    const uint8_t* buffer,
    size_t buffer_length,
    const std::vector<uint64_t>& bitmap
);

// Helper for interpreting a single JSON record (deprecated, use build_map)
class RecordInterpreter {
public:
    // Parse a single record given marker positions and byte range
    // Returns FieldSpans for all key-value pairs found in the record
    std::vector<FieldSpan> parse_record(
        const uint8_t* buffer,
        uint32_t record_start,
        uint32_t record_end,
        const std::vector<MarkerPosition>& markers,
        const std::map<std::string, uint32_t>& marker_index  // [position] -> index in markers
    );

private:
    // Find the closing quote for an opening quote, accounting for escapes
    uint32_t find_closing_quote(
        uint32_t open_quote_pos,
        const std::vector<MarkerPosition>& markers,
        const std::map<std::string, uint32_t>& marker_index,
        uint32_t record_end
    );

    // Classify value type by examining buffer at value_start
    ValueType classify_value_type(
        const uint8_t* buffer,
        uint32_t value_start
    );

    // Skip whitespace forward in buffer
    uint32_t skip_whitespace(const uint8_t* buffer, uint32_t pos, uint32_t limit);

    // Extract key string (unquoted)
    std::string extract_key(const uint8_t* buffer, uint32_t key_start, uint32_t key_end);
};

}  // namespace rugo::_jsonl

#endif  // _JSONL_INTERPRETER_HPP_
