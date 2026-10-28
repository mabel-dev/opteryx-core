#pragma once
// src/cpp/engine/topn_keep.hpp — the top-n cut a late-materialization scan makes before
// its second pass (parquet: native_latmat_scan_source.hpp, skene:
// native_skene_latmat_scan_source.hpp, ORC: native_orc_scan_source.hpp,
// docs/ORC_TOPN_LATE_MATERIALISATION_DESIGN.md).
//
// It does NOT write an ordering. It builds draken's own normalized sort keys
// (`build_sort_keys`) over the candidate rows and uses draken's own comparator
// (`SortKeyCmp`) — the SAME definition the downstream TopNSink sorts with — and keeps
// every row that is not strictly worse than the n-th best:
//
//     nth_element(idx, idx + n - 1, cmp);  b = idx[n-1];
//     keep[r] = !cmp(b, r);          // r is not strictly worse than the boundary
//
// So it is correct by construction for every key type, for ties (a tied row compares
// neither before nor after `b` and is kept) and for NULLs and NaN, whose placement the
// comparator already encodes. Dropping only strictly-worse rows is what makes the
// TopNSink's answer over the kept rows identical to the un-pushed plan's.

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <vector>

#include "native_sort.hpp"   // build_sort_keys / SortKeySpec / SortKeyColumn / SortKeyCmp
#include "operator.hpp"      // ErrCtx, MorselPtr

namespace opteryx::engine {

// `keep[r]` over the rows of `ms` concatenated in order (the order build_sort_keys
// walks them in): 1 for every row not strictly worse than the n-th best under `spec`.
// Every row is kept when there are no more than `n`. False with `err` set when the
// sort keys cannot be built, or when `n` is zero (a LIMIT 0 never reaches a scan's
// top-n cut, and keeping everything would be a wrong answer's shape).
inline bool topn_keep_mask(const std::vector<MorselPtr>& ms, const std::vector<SortKeySpec>& spec, size_t n,
                           std::vector<uint8_t>& keep, ErrCtx& err) {
    size_t total = 0;
    for (const MorselPtr& m : ms) total += m->num_rows();
    keep.assign(total, 1u);
    if (n == 0) {
        err.code = 1;
        err.msg = "topn_keep_mask: a non-positive top-n limit";
        return false;
    }
    if (n >= total) return true;
    std::vector<SortKeyColumn> keys;
    if (!build_sort_keys(ms, spec, keys, err)) return false;
    SortKeyCmp cmp{keys};
    std::vector<uint32_t> idx(total);
    std::iota(idx.begin(), idx.end(), 0u);
    std::nth_element(idx.begin(), idx.begin() + static_cast<ptrdiff_t>(n - 1), idx.end(), cmp);
    const uint32_t boundary = idx[n - 1];
    for (size_t r = 0; r < total; ++r) keep[r] = cmp(boundary, static_cast<uint32_t>(r)) ? 0u : 1u;
    return true;
}

}  // namespace opteryx::engine
