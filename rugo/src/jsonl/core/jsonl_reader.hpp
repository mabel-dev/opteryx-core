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
//
// `keep_unicode_escapes`: ALSO keep every line containing a `\u` escape. For a predicate
// that compares DECODED text (a nested `->>` column), a record may spell the value with
// `\uXXXX` escapes, so its bytes need not contain the needle — those lines cannot be ruled
// out by a byte search and must survive to be parsed. Line order is preserved.
struct PrefilterResult {
    std::vector<uint8_t> candidates;     // surviving lines, newline-terminated
    size_t total_records   = 0;
    size_t matched_records = 0;
};
PrefilterResult volnitsky_prefilter(
    const uint8_t* buffer, size_t length,
    const uint8_t* needle, size_t needle_len,
    bool keep_unicode_escapes = false);

// The surviving lines of [from, to) under the same rule, as spans into `buffer` — no copy.
// `from` must be a line start (0, or one past a newline). This is what the parser's range
// tasks run (interpret_jsonl_threaded): each task finds its own range's lines and scans
// only those, in place.
std::vector<LineSpan> prefilter_lines(
    const uint8_t* buffer, size_t from, size_t to,
    const uint8_t* needle, size_t needle_len,
    bool keep_unicode_escapes = false);

// One prefilter needle: the bytes to search for, and whether `\u` lines must also be kept
// (decoded-text comparison — see volnitsky_prefilter).
struct PrefilterNeedle {
    std::string needle;
    bool keep_unicode_escapes = false;
};

// Gated Volnitsky raw prefilter over the pushed string-equality predicates. The pushed
// predicates are AND-ed, so a record that fails any one of them is dropped regardless —
// every eligible predicate is a sound filter on its own, and the MOST SELECTIVE one on the
// sample is used. Eligible:
//   * top-level column, string literal, value stored quoted in the first record: the
//     needle is the quoted value (compared as raw bytes downstream, so its bytes are in
//     every matching record);
//   * nested `->>` column, string literal of >= 6 bytes drawn only from [A-Za-z0-9._:-]:
//     `->>` compares decoded text, and a value of those bytes appears verbatim except when
//     spelled with `\u` escapes, so those lines are kept too. The needle is the QUOTED
//     value, unless the literal could be a JSON number / boolean token — then a matching
//     field may hold it unquoted, and the needle is the bare value.
// Returns true when prefiltering applies, with `out` holding the surviving candidate
// lines (EMPTY when no record can match — the buffer then yields 0 rows); false when it
// does not apply, and the caller keeps reading the original buffer. Only ever touches
// BOUNDED samples (first ~4KB, first ~1MB per candidate) to decide, never the whole
// buffer, so it stays cheap over a multi-hundred-GB mapping. The predicates are
// re-applied downstream so false positives are verified away. Self-disabling when no
// predicate is eligible or the best one keeps more than 30% of the sampled records.
//
// choose_prefilter_needle is the gate alone: it decides from the bounded samples and
// returns the needle, for callers that find the lines themselves (prefilter_lines).
// maybe_prefilter is the gate plus a copy of the surviving lines into `out`, for callers
// that parse a buffer of their own (the engine's per-chunk decode workers).
bool choose_prefilter_needle(const uint8_t* buffer, size_t length, const ParseContext& context,
                             PrefilterNeedle& out);
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
