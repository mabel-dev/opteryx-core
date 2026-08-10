#pragma once

#include <cstdint>
#include <cstring>
#include <cmath>
#include <system_error>

#include "fast_float/fast_float.h"
#include "ops/float_ops.h"   // fp_canon — ingestion canonicalisation of -0.0 / NaN

namespace rugo::_jsonl {

// Fast integer parser via fast_float::from_chars — overflow-safe, same contract
// as the float parser below. Returns false if the input is not a valid integer
// or if it overflows int64_t.
inline bool fast_parse_int64(const uint8_t* data, uint32_t start, uint32_t end, int64_t& out) noexcept {
    if (start > end) return false;
    const char* first = reinterpret_cast<const char*>(data + start);
    const char* last  = reinterpret_cast<const char*>(data + end + 1);
    auto answer = fast_float::from_chars(first, last, out);
    return answer.ec == std::errc() && answer.ptr == last;
}

// Fast float parser. Bounded by [start, end] via fast_float::from_chars — must NOT
// use strtod here: the slice buffer is concatenated values with no separators, so
// strtod would over-read past `end` into the next value. The whole slice must be
// consumed (answer.ptr == last) so trailing garbage (e.g. "3.5x") is rejected.
inline bool fast_parse_float64(const uint8_t* data, uint32_t start, uint32_t end, double& out) noexcept {
    if (start > end) return false;

    const char* first = reinterpret_cast<const char*>(data + start);
    const char* last  = reinterpret_cast<const char*>(data + end + 1);

    double value;
    auto answer = fast_float::from_chars(first, last, value);
    if (answer.ec != std::errc() || answer.ptr != last) {
        return false;
    }

    // Ingestion canonicalisation: -0.0 -> +0.0, any NaN bit-pattern -> one
    // canonical quiet NaN (draken/ops/float_ops.h, architect-locked 2026-05-22).
    // Every caller of this parser is an ingestion point — JSONL and CSV value
    // decode, CSV type sniffing, and the CSV pushed-predicate literal — so one
    // canon here is the whole text-format story.
    //
    // Unlike the Parquet reader, there is no fidelity argument for keeping the
    // sign here: `-0.0` in a CSV cell is a LITERAL, and the SQL literal path
    // already canonicalises it (`CAST(-0.0 AS FLOAT64)` is `0.0`). Leaving it
    // signed made the same text mean different things depending on whether it
    // arrived through a file or through the query, and split `GROUP BY f` into
    // two groups for a value `f = 0.0` matched twice.
    out = draken::ops::fp_canon(value);
    return true;
}

}  // namespace rugo::_jsonl
