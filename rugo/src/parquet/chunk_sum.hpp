#pragma once
// rugo/src/parquet/chunk_sum.hpp — the `rugo.sum` column-chunk statistic.
//
// Parquet's Statistics struct has no sum. The spec's extension slot for a
// per-chunk fact is ColumnMetaData.key_value_metadata (parquet.thrift field 8),
// one list per column chunk — exactly the granularity of a row group's sum. rugo
// writes one entry there for every integer column chunk:
//
//   key   "rugo.sum"
//   value the EXACT sum of the chunk's non-null values, as signed decimal text
//
// "Integer column" = a scalar INT32/INT64 chunk with no logical annotation other
// than an integer width/sign one: INT8/16/32/64 and UINT8/16/32/64. Temporal,
// DECIMAL, float, boolean, string and LIST chunks get no sum. Unsigned values
// zero-extend (a UINT64 above INT64_MAX is its value). The rule is draken's
// exact-sum rule (draken/ops/exact_sum.h), restated for rugo's physical slots.
//
// Text, not bytes: any reader can decode it without knowing rugo, and parsing a
// few dozen characters is nothing next to the footer read. A chunk without the
// key means NOT TRACKED, never zero. Readers trust the key only in a file whose
// `created_by` identifies rugo — the same rule as sorting_columns.

#include <cstdint>
#include <string>
#include <string_view>

namespace rugo_parquet {

inline constexpr std::string_view kChunkSumKey = "rugo.sum";

inline std::string format_chunk_sum(__int128 value) {
    if (value == 0) return "0";
    const bool negative = value < 0;
    // Magnitude as unsigned: -(INT128_MIN) does not fit a signed __int128.
    unsigned __int128 magnitude = negative
        ? static_cast<unsigned __int128>(0) - static_cast<unsigned __int128>(value)
        : static_cast<unsigned __int128>(value);
    char buffer[48];
    int at = static_cast<int>(sizeof(buffer));
    while (magnitude != 0) {
        buffer[--at] = static_cast<char>('0' + static_cast<int>(magnitude % 10u));
        magnitude /= 10u;
    }
    if (negative) buffer[--at] = '-';
    return std::string(buffer + at, sizeof(buffer) - static_cast<size_t>(at));
}

// False (out untouched) for anything that is not exactly an optional '-'
// followed by 1..39 decimal digits whose value fits __int128.
inline bool parse_chunk_sum(std::string_view text, __int128* out) {
    if (text.empty()) return false;
    size_t at = 0;
    const bool negative = text[0] == '-';
    if (negative) at = 1;
    if (at == text.size() || text.size() - at > 39) return false;
    // Accumulate the magnitude unsigned; the bound is INT128_MAX, or its
    // magnitude plus one for a negative value.
    const unsigned __int128 limit =
        (static_cast<unsigned __int128>(1) << 127) - (negative ? 0u : 1u);
    unsigned __int128 magnitude = 0;
    for (; at < text.size(); ++at) {
        const char c = text[at];
        if (c < '0' || c > '9') return false;
        const unsigned digit = static_cast<unsigned>(c - '0');
        if (magnitude > (limit - digit) / 10u) return false;
        magnitude = magnitude * 10u + digit;
    }
    *out = negative ? static_cast<__int128>(static_cast<unsigned __int128>(0) - magnitude)
                    : static_cast<__int128>(magnitude);
    return true;
}

}  // namespace rugo_parquet
