#pragma once
// page_search.hpp — substring search over raw PLAIN byte_array bytes.
//
// docs/PARQUET_PAGE_SEARCH_DESIGN.md §2. A PLAIN byte_array value region (a
// data page's value stream, or a PLAIN dictionary page) is one buffer:
// [len32][bytes][len32][bytes]... A `LIKE '%x%'` does not need the values as
// strings to be answered — it needs the bytes. One SIMD search runs over the
// whole region, ignoring value boundaries; each occurrence is then mapped back
// to its value by a length-prefix cursor that only ever moves forward, and only
// as far as the last occurrence. A region with no occurrence costs one SIMD
// pass and no walk at all.
//
// Correctness: a value contains the needle iff some occurrence lies wholly
// inside that value's payload. An occurrence that straddles a value boundary or
// overlaps a length prefix is rejected and the search resumes one byte later
// (occurrences may overlap), so no candidate occurrence is ever skipped
// unexamined. After an accepted occurrence the search resumes at the end of that
// value: the value is decided, and nothing before its end can start a match in
// a later value.

#include "simd_find.h"   // simd_find_cs (draken/simd)

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

namespace rugo { namespace page_search {

// Set hit[v] = 1 for every value v (v < n_values) of the PLAIN byte_array region
// [region, region + region_len) that contains `pat`. `hit` must hold n_values
// bytes; entries already 1 stay 1 (several needles OR into one array). Returns
// the number of values this call newly marked.
//
// Values past n_values are not walked: bytes after the n_values-th value are not
// values (the decoder's value loop stops at present_count too), so an occurrence
// there is ignored. A length prefix that runs past the region is a corrupt page
// and throws — stopping early would silently drop matching rows.
inline uint32_t mark_values_containing(const uint8_t* region, size_t region_len,
                                       uint32_t n_values,
                                       const uint8_t* pat, size_t pat_len,
                                       uint8_t* hit) {
    if (pat_len == 0)
        throw std::logic_error("page search: empty needle (the caller must not arm it)");
    uint32_t marked = 0;
    size_t from = 0;
    // Cursor: value `v` has payload [vstart, vend). v == n_values means "not loaded yet"
    // for the first value only; `loaded` distinguishes it.
    uint32_t v = 0;
    size_t vstart = 0, vend = 0;
    bool loaded = false;
    auto load_at = [&](size_t prefix_at) {
        if (prefix_at + 4 > region_len)
            throw std::runtime_error("PLAIN byte_array page: value " + std::to_string(v) +
                                     " length prefix runs past the page");
        int32_t len;
        std::memcpy(&len, region + prefix_at, 4);
        if (len < 0 || prefix_at + 4 + static_cast<size_t>(len) > region_len)
            throw std::runtime_error("PLAIN byte_array page: value " + std::to_string(v) +
                                     " length runs past the page");
        vstart = prefix_at + 4;
        vend = vstart + static_cast<size_t>(len);
    };
    while (from < region_len) {
        size_t p = simd_find_cs(region + from, region_len - from, pat, pat_len);
        if (p == SIZE_MAX) break;
        p += from;
        if (!loaded) {
            if (n_values == 0) break;
            load_at(0);
            loaded = true;
        }
        // Advance to the value whose payload ends after p (prefix bytes belong to
        // the value that follows them).
        while (vend <= p) {
            if (v + 1 >= n_values) return marked;   // p lies past the last value
            ++v;
            load_at(vend);
        }
        if (p >= vstart && p + pat_len <= vend) {
            if (!hit[v]) { hit[v] = 1; ++marked; }
            from = vend;
        } else {
            from = p + 1;
        }
    }
    return marked;
}

}}  // namespace rugo::page_search
