#ifndef _JSONL_READER_HPP_
#define _JSONL_READER_HPP_

#include <vector>
#include <cstdint>
#include <cstddef>

namespace rugo::_jsonl {

// SPIKE: Sparser-style raw prefilter for string-equality. Walks records (newline-split)
// and keeps only lines that contain `needle` (a value-anchored, formatting-invariant byte
// pattern, e.g. the JSON-encoded quoted value `"abc-123"`), using Volnitsky substring
// search. SOUND for string equality: a matching record always contains those bytes, so we
// never drop a real match; false positives (the bytes appear elsewhere) are verified away
// downstream. `candidates` is the concatenation of surviving lines, ready to re-read.
struct PrefilterResult {
    std::vector<uint8_t> candidates;     // surviving lines, newline-terminated
    size_t total_records   = 0;
    size_t matched_records = 0;
};
PrefilterResult volnitsky_prefilter(
    const uint8_t* buffer, size_t length,
    const uint8_t* needle, size_t needle_len);

}  // namespace rugo::_jsonl

#endif  // _JSONL_READER_HPP_
