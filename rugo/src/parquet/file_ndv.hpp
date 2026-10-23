#pragma once
// rugo/src/parquet/file_ndv.hpp — the `rugo.ndv.<column>` file-level statistic.
//
// A file's distinct count per column cannot be recovered from the per-chunk
// Statistics.distinct_count once a column is spread unsorted over many row
// groups (the reader can only bracket it between the max and the sum). So the
// writer counts it over the whole file, from the XXH64 value hashes it already
// computes for every chunk, and states it once in FileMetaData
// key_value_metadata (parquet.thrift field 5):
//
//   key   "rugo.ndv." + the column's TOP-LEVEL name
//   value "exact:<n>"    n distinct non-null values, counted exactly
//         "kmv1024:<n>"  n estimated by a K=1024 KMV sketch (~3% relative
//                        standard error), at least 1024
//
// Text, not bytes: any reader can decode it without knowing rugo. A column
// without the key means NOT TRACKED, never zero (an all-null column, a bool or
// LIST column, or a file rugo patched). Readers trust the key only in a file
// whose `created_by` identifies rugo — the same rule as `rugo.sum`.
#include <cstdint>
#include <string>
#include <string_view>

namespace rugo_parquet {

inline constexpr std::string_view kFileNdvKeyPrefix = "rugo.ndv.";
inline constexpr std::string_view kFileNdvExactTag = "exact:";
inline constexpr std::string_view kFileNdvKmvTag = "kmv1024:";

inline std::string format_file_ndv(bool exact, uint64_t count) {
    return std::string(exact ? kFileNdvExactTag : kFileNdvKmvTag) + std::to_string(count);
}

// False (outputs untouched) for anything that is not exactly one of the two
// tags followed by 1..18 decimal digits (the count must fit int64).
inline bool parse_file_ndv(std::string_view text, bool* exact, int64_t* count) {
    bool is_exact;
    std::string_view digits;
    if (text.substr(0, kFileNdvExactTag.size()) == kFileNdvExactTag) {
        is_exact = true;
        digits = text.substr(kFileNdvExactTag.size());
    } else if (text.substr(0, kFileNdvKmvTag.size()) == kFileNdvKmvTag) {
        is_exact = false;
        digits = text.substr(kFileNdvKmvTag.size());
    } else {
        return false;
    }
    if (digits.empty() || digits.size() > 18) return false;
    int64_t value = 0;
    for (const char c : digits) {
        if (c < '0' || c > '9') return false;
        value = value * 10 + (c - '0');
    }
    *exact = is_exact;
    *count = value;
    return true;
}

}  // namespace rugo_parquet
