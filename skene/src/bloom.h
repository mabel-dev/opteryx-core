#pragma once
// Internal: Split-Block Bloom Filter (FORMAT.md §9.1).
//
// Same structure as the Parquet SBBF and rugo's implementation of it: a sequence
// of 32-byte blocks, each 8 little-endian uint32 words, one bit set per word,
// block chosen by the canonical `((hash >> 32) * num_blocks) >> 32`, block count
// always a power of two. XXH64 seed 0 over the value's native bytes.
//
// DESTINATION: this belongs in draken, next to rugo's copy, so the file engine
// and skene share one implementation. It lives here for now because skene is
// speculative and relocating live rugo code into draken would be production
// churn for work that may be dropped. If skene is kept, the two collapse into
// one — and they are deliberately written to the same specification so that
// collapse is a deletion rather than a reconciliation.

#include <cstdint>
#include <vector>

#include "skene/status.h"

#include "core/buffers.h"

namespace skene {

// The rate skene targets by default, and the reason it is not Parquet's 1%.
//
// Parquet pays a separate round trip to fetch a bloom filter, so accuracy is
// worth buying — a false positive there costs a wasted request. Ours arrives in
// the same fetch as the footer and sits BEHIND 8k zone-map pruning, so a false
// positive costs one chunk read we may well have done anyway. Halving the bytes
// to double the miss rate is the right side of that trade for us and the wrong
// side for them.
inline constexpr double kDefaultFalsePositiveRate = 0.05;

// Built over the `data` array — on a value-ordered column that is the
// deduplicated dictionary, so the filter costs NDV insertions rather than
// row-count insertions and is exactly as accurate. `data_length` is then the
// EXACT distinct count, so the sizing gets a true count rather than an estimate.
//
// Returns false when the column's type has no defined byte representation to
// hash (ARRAY, NULL, FP16); that is a normal outcome, not an error.
bool bloom_build(const DrakenVector& vector, double false_positive_rate,
                 std::vector<uint8_t>* out);

// Probes a serialized filter. A false result is PROOF the value is absent; a
// true result means "cannot rule it out" at the filter's configured rate, so a
// caller must still read and check.
//
// Fails loud on a body that is not a valid filter rather than answering from
// whatever bytes are there — a corrupt filter that says "absent" would silently
// drop rows from a result.
Status bloom_probe(const uint8_t* stored, uint64_t stored_bytes,
                   const void* value_bytes, uint32_t value_length,
                   bool* out_may_contain);

// Hashes one value of a vector the way bloom_build does, so a caller can probe
// with a value it holds rather than reconstructing the encoding.
bool bloom_hash_value(const void* value_bytes, uint32_t value_length,
                      uint64_t* out_hash);

}  // namespace skene
