// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// draken/ops/column_profile.h — per-column profile reductions: ONE implementation
// shared by the Vector bindings (draken_native.cpp: char_class_stats,
// ordinal_min_max, histogram_bucket) and native per-file statistics
// accumulators (opteryx's planner file_stats.hpp), so a statistic computed in
// either place is the same number.
//
// The ordinal reductions read ordinalize()'s INT64 output, where a null row is
// the sentinel ORDINAL_NULL baked into the data (no validity bitmap): they skip
// it explicitly.

#pragma once

#include <cstdint>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "ops/ordinalize.h"

namespace draken {
namespace ops {

// Byte classes for the LIKE '%needle%' selectivity estimator: 0=upper 1=lower
// 2=digit 3=whitespace 4=punct_text 5=semantic 6=extended 7=control. A byte-for-
// byte port of opteryx-core's scratch/like_selectivity/stats.py `_BYTE_CLASS` -
// NOT re-derived by hand. Every byte 0-255 has exactly one class.
inline constexpr uint8_t kByteClass[256] = {
    7, 7, 7, 7, 7, 7, 7, 7, 7, 3, 3, 3, 3, 3, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    3, 4, 4, 5, 5, 5, 5, 4, 4, 4, 5, 5, 4, 4, 4, 5,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 5, 4, 5, 5, 5, 4,
    5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 5, 5, 5, 5,
    5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 5, 5, 5, 5, 7,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
};

// Byte-class counts, total bytes and the length range of a string vector's
// valid rows. Accumulates into `s` (start from a value-initialised one), so a
// caller can fold several vectors (row groups) into one profile.
struct CharClassStats {
    uint64_t counts[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    uint64_t total_bytes = 0;
    uint32_t min_len = 0xFFFFFFFFu;
    uint32_t max_len = 0;
    bool any = false;
};

inline bool is_string_type(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

// `v` must be a string vector (is_string_type).
inline void char_class_stats(const DrakenVector& v, CharClassStats& s) {
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v.data);
    for (uint32_t i = 0; i < v.length; ++i) {
        if (v.validity != nullptr && !((v.validity[i >> 3] >> (i & 7u)) & 1u)) continue;
        const DrakenStringSlot* slot = &sa->slots[v.selection[i]];
        const uint8_t* p = str_data(slot, sa->arena);
        const uint32_t len = str_length(slot);
        for (uint32_t j = 0; j < len; ++j) s.counts[kByteClass[p[j]]] += 1;
        s.total_bytes += len;
        if (len < s.min_len) s.min_len = len;
        if (len > s.max_len) s.max_len = len;
        s.any = true;
    }
}

// The min and max ordinal key of an ordinalize() output (`data[selection[i]]`,
// i < n), skipping ORDINAL_NULL. False when every row is null / there are none.
inline bool ordinal_min_max(const int64_t* data, const uint32_t* selection, uint32_t n,
                            int64_t& vmin, int64_t& vmax) {
    bool any = false;
    int64_t lo = INT64_MAX, hi = INT64_MIN;
    for (uint32_t i = 0; i < n; ++i) {
        const int64_t val = data[selection[i]];
        if (val == ORDINAL_NULL) continue;
        any = true;
        if (val < lo) lo = val;
        if (val > hi) hi = val;
    }
    if (any) {
        vmin = lo;
        vmax = hi;
    }
    return any;
}

// Equi-width histogram of an ordinalize() output over [vmin, vmax] into
// `counts[0..n_bins)` (added to, not reset), skipping ORDINAL_NULL. vmin == vmax
// puts every value in bin 0. The bin is int(frac * (n_bins - 1)) clamped:
// floating-point rounding at the vmax boundary can otherwise land one past the end.
inline void histogram_bucket(const int64_t* data, const uint32_t* selection, uint32_t n,
                             int64_t vmin, int64_t vmax, int64_t n_bins, int64_t* counts) {
    const int64_t span = vmax - vmin;
    for (uint32_t i = 0; i < n; ++i) {
        const int64_t val = data[selection[i]];
        if (val == ORDINAL_NULL) continue;
        int64_t bin;
        if (span <= 0) {
            bin = 0;
        } else {
            const double frac = static_cast<double>(val - vmin) / static_cast<double>(span);
            bin = static_cast<int64_t>(frac * static_cast<double>(n_bins - 1));
            if (bin < 0) bin = 0;
            if (bin >= n_bins) bin = n_bins - 1;
        }
        counts[bin] += 1;
    }
}

}  // namespace ops
}  // namespace draken
