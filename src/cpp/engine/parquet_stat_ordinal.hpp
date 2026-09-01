#pragma once
// src/cpp/engine/parquet_stat_ordinal.hpp — convert a parquet column-chunk
// statistic (raw bytes + the footer's physical/logical type strings) into
// DRAKEN'S ORDINAL SPACE, so a parquet row group's zone map can be tested
// against a RuntimeKeyBound with no second comparison dialect.
//
// See docs/RUNTIME_MINMAX_FILTER_DESIGN.md §3.3, option (b). Option (a) —
// comparing in stat-byte space via CompareStatBytes — was rejected because it
// requires encoding the build-side ordinal BACK into parquet stat bytes, and
// because CompareStatBytes falls through to a lexicographic compare for
// fixed_len_byte_array, which is wrong for negative big-endian two's
// complement DECIMAL. Converting forwards into the ordinal space skene and
// Manifest._ordinalize_literal already share keeps one dialect.
//
// WHAT THIS DELIBERATELY DOES NOT HANDLE (v1):
//   int96, float, double, byte_array, fixed_len_byte_array (so: DECIMAL,
//   VARCHAR, temporal-as-bytes).
// Those return false, which the caller must treat exactly as `valid == 0` is
// treated in runtime_bound.hpp: CONTRIBUTES NO TERM, prunes nothing, costs a
// read rather than an answer. Widening this set is a measurement question, not
// a correctness one — but every added type must be ordinalized through
// draken/ops/ordinalize.h and never through a bespoke comparison here.
//
// Signedness comes from StatsLogicalIsUnsigned (rugo/src/parquet/metadata.hpp),
// the SAME predicate AggregateColumnStats' CompareStatBytes uses. An unsigned
// column stores its magnitude in a signed int32/int64 slot, so any value at or
// above the signed midpoint has a negative bit pattern; reading those as signed
// inverts the range and prunes row groups that genuinely match.

#include <cstdint>
#include <cstring>
#include <string>

#include "ops/ordinalize.h"  // draken's ordinal space — the ONE dialect
#include "metadata.hpp"      // StatsLogicalIsUnsigned

namespace opteryx::engine {

// Decode one statistic into draken's ordinal space.
//
// `bytes` is ColumnStats::min or ::max verbatim (little-endian, as parquet
// writes int32/int64 statistics). Returns false — leaving *out untouched — when
// the type is outside the supported set or the buffer is too short for the
// physical width. A short buffer is a malformed footer, not a value: refusing is
// the only safe answer, since guessing a bound prunes real rows.
inline bool stat_bytes_to_ordinal(const std::string& physical_type,
                                  const std::string& logical_type,
                                  const std::string& bytes,
                                  int64_t* out) noexcept {
    const bool is_unsigned = StatsLogicalIsUnsigned(logical_type);
    if (physical_type == "int32") {
        if (bytes.size() < 4) return false;
        if (is_unsigned) {
            uint32_t u = 0;
            std::memcpy(&u, bytes.data(), 4);
            // uint32 fits int64 with its natural order intact — widen, do NOT
            // route through ordinalize_scalar_u64's sign-bit bias, which is
            // only correct for the full uint64 range.
            *out = draken::ops::ordinalize_scalar_widen<int64_t>(
                static_cast<int64_t>(u));
            return true;
        }
        int32_t v = 0;
        std::memcpy(&v, bytes.data(), 4);
        *out = draken::ops::ordinalize_scalar_widen<int32_t>(v);
        return true;
    }
    if (physical_type == "int64") {
        if (bytes.size() < 8) return false;
        if (is_unsigned) {
            uint64_t u = 0;
            std::memcpy(&u, bytes.data(), 8);
            *out = draken::ops::ordinalize_scalar_u64(u);
            return true;
        }
        int64_t v = 0;
        std::memcpy(&v, bytes.data(), 8);
        *out = draken::ops::ordinalize_scalar_widen<int64_t>(v);
        return true;
    }
    return false;
}

}  // namespace opteryx::engine
