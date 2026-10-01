#ifndef _JSONL_READER_HPP_
#define _JSONL_READER_HPP_

#include <vector>
#include <string>
#include <cstdint>
#include <cstddef>

#include "parse_context.hpp"

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

// Gated Volnitsky raw prefilter for a single selective string-equality predicate.
// Returns true when prefiltering applies, with `out` holding the surviving candidate
// lines (EMPTY when no record can match — the buffer then yields 0 rows); false when it
// does not apply, and the caller keeps reading the original buffer. Only ever touches
// BOUNDED samples (first ~4KB, first ~1MB) to decide, never the whole buffer, so it stays
// cheap over a multi-hundred-GB mapping. SOUND: the needle is the quoted value, which a
// matching record always contains regardless of whitespace; the predicate is re-applied
// downstream so false positives are verified away. Self-disabling on non-string, short,
// or non-selective cases.
bool maybe_prefilter(const uint8_t* buffer, size_t length, const ParseContext& context,
                     std::vector<uint8_t>& out);

// Python's repr() of the text `bytes` decodes to (UTF-8, errors='replace'): the quote
// choice and the escapes repr() makes, so a message built here reads exactly as the
// f"{...!r}" it replaced.
std::string py_str_repr(const uint8_t* bytes, size_t length);

// The fail_on_error message for the first malformed record, at `offset` in `buffer`:
// its 1-based line number and up to 200 bytes of the line. Error path only (not hot) —
// it counts newlines over every byte before `offset`.
std::string malformed_error_message(const uint8_t* buffer, size_t length, uint32_t offset);

}  // namespace rugo::_jsonl

#endif  // _JSONL_READER_HPP_
