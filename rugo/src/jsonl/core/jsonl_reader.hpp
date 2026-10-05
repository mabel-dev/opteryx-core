#ifndef _JSONL_READER_HPP_
#define _JSONL_READER_HPP_

#include <vector>
#include <string>
#include <cstdint>
#include <cstddef>

#include "parse_context.hpp"

namespace rugo::_jsonl {

// Sparser-style raw prefilter: before any structural parsing, drop the lines that cannot
// satisfy the pushed string-equality / IN predicates, by searching their raw bytes for
// value-anchored, formatting-invariant needles (e.g. the JSON-encoded quoted value
// `"abc-123"`). SOUND: a matching record always contains a needle of every eligible
// predicate, so a real match is never dropped; false positives (the bytes appear
// elsewhere) are verified away downstream, where every predicate is re-applied.

// One prefilter CLAUSE: a record can satisfy its predicate only if its bytes contain at
// least ONE of `needles` (an IN list contributes one needle per member; `=` one needle), or
// — when `keep_unicode_escapes` — it holds a `\u` escape: a predicate that compares
// DECODED text (a nested `->>` column) may meet the value spelled with `\uXXXX` escapes,
// so its bytes need not contain the needle, and such a line must survive to be parsed.
struct PrefilterClause {
    std::vector<std::string> needles;
    bool keep_unicode_escapes = false;
};

// The prefilter: AND of clauses (the pushed predicates are AND-ed, so each eligible one is
// a necessary condition on its own). clauses[0] DRIVES the scan — a single multi-pattern
// Volnitsky pass over the range finds its lines; every other clause is CONFIRMED on each
// surviving line only (a short in-line search), so a non-selective clause costs nothing
// on the lines the driver already dropped.
struct PrefilterPlan {
    std::vector<PrefilterClause> clauses;
    // Per driver needle: two byte positions to sieve on (the rarest bytes on the gate's
    // sample), for the SIMD driver. Empty = Volnitsky driver.
    std::vector<std::pair<uint32_t, uint32_t>> sieve;
};

// [from, to) cut into line-aligned segments, in order. The gate judged only the buffer's
// head, so every 4MB window is re-judged on its own first 256KB: a FILTERED segment holds
// the plan's surviving lines (spans into `buffer`, no copy); an unfiltered one is a
// non-selective region the caller parses whole, by its normal path. `from` must be a line
// start.
struct PrefilterSegment {
    size_t from, to;
    bool filtered;
    std::vector<LineSpan> lines;   // filtered only
};
std::vector<PrefilterSegment> prefilter_plan_segments(
    const uint8_t* buffer, size_t from, size_t to, const PrefilterPlan& plan);

// Gated Volnitsky raw prefilter over the pushed string-equality / IN predicates. Eligible
// (each becomes one clause):
//   * `=` on a string literal, or IN whose every member is a string literal — one needle per
//     literal, all under the same column rule:
//   * top-level column whose value is stored quoted in the first record: the needle is the
//     quoted value (compared as raw bytes downstream, so its bytes are in every matching
//     record);
//   * nested `->>` column, literal drawn only from [A-Za-z0-9._:-]: `->>` compares decoded
//     text, and a value of those bytes appears verbatim except when spelled with `\u`
//     escapes, so those lines are kept too. The needle is the QUOTED value, unless the
//     literal could be a JSON number / boolean token — then a matching field may hold it
//     unquoted, and the needle is the bare value.
//   A needle shorter than kMinNeedle bytes makes its clause ineligible (Volnitsky needs a
//   bigram; the selectivity sample decides whether a short one pays off).
// Returns true when prefiltering applies, with `out` holding the plan: the clause hitting
// the fewest sampled records drives, every other eligible clause confirms. False when no
// predicate is eligible or the plan keeps more than 30% of the sampled records — the caller
// keeps reading the original buffer. Only ever touches BOUNDED samples (first ~4KB, first
// ~1MB) to decide, never the whole buffer. The predicates are re-applied downstream so
// false positives are verified away.
//
// choose_prefilter_plan is the gate alone, for callers that find the lines themselves
// (prefilter_plan_segments). maybe_prefilter is the gate plus a copy of the surviving lines
// into `out`, for callers that parse a buffer of their own (the engine's per-chunk decode
// workers).
bool choose_prefilter_plan(const uint8_t* buffer, size_t length, const ParseContext& context,
                           PrefilterPlan& out);
bool maybe_prefilter(const uint8_t* buffer, size_t length, const ParseContext& context,
                     std::vector<uint8_t>& out);

// Python's repr() of the text `bytes` decodes to (UTF-8, errors='replace'): the quote
// choice and the escapes repr() makes, so a message built here reads exactly as the
// f"{...!r}" it replaced.
std::string py_str_repr(const uint8_t* bytes, size_t length);

// The fail_on_error message for the first malformed record, at `offset` in `buffer`:
// its 1-based line number and up to 200 bytes of the line. Error path only (not hot) —
// it counts newlines over every byte before `offset`.
std::string malformed_error_message(const uint8_t* buffer, size_t length, size_t offset);

}  // namespace rugo::_jsonl

#endif  // _JSONL_READER_HPP_
