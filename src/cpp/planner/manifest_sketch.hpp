// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/manifest_sketch.hpp — reductions over a manifest's
// whole-column sketch vectors, with no Python in them.
//
// A sketch vector is a draken `array<array<T>>` (one outer row per data file,
// one middle row per data-file column, leaf = that column's values: min-k
// hashes, histogram bins or the 8 char-class byte counts). The reductions here
// are THE implementation: the nanobind bindings
// (opteryx/compiled/nanobind/vector_sketch_reduce.cpp) and the native manifest
// (native_manifest.hpp) both call them.
//
// Nested traversal contract: an ARRAY Vector's `data` IS the int32 offsets
// buffer; the child lives out-of-band and is reached through the bridge's
// unwrap helpers, never by casting `data`. Row i's child range is
// offsets[selection[i]] .. offsets[selection[i]+1]; leaf value k is
// leaf.data[leaf.selection[k]] with validity indexed by the raw child index.

#pragma once

#include <cstdint>
#include <vector>

#include "core/buffers.h"
#include "core/kmv_sketch.h"

namespace opteryx::planner {

// THE manifest sketch — draken/core/kmv_sketch.h. The family is part of the
// TYPE, so a cross-family union cannot compile (architect ruling 2026-08-21).
using ManifestSketch = draken::KmvSketch<32u, draken::KmvHashFamily::kDrakenVectorHash>;
using SkeneSketch = draken::KmvSketch<32u, draken::KmvHashFamily::kXxh3ValueBytes>;

inline bool sketch_bit_valid(const uint8_t* validity, uint32_t idx) {
    // validity == NULL means "all valid" (unified-format convention).
    return validity == nullptr || ((validity[idx >> 3] >> (idx & 7u)) & 1u);
}

// Read-only view over a two-level array<array<T>> Vector (outer=files,
// middle=columns, leaf=values). Centralizes the offset/selection composition —
// the one part of these kernels that segfaults if an index expression is wrong —
// so it is written and reviewed once, not re-derived per kernel. Leaf typing and
// per-value null handling stay with each kernel (they differ: hashes skip nulls,
// histogram bins read null as 0), so the view stops at the leaf index range.
struct NestedArrayView {
    const DrakenVector* outer = nullptr;
    const DrakenVector* mid = nullptr;
    const DrakenVector* leaf = nullptr;

    NestedArrayView() = default;
    NestedArrayView(const DrakenVector* o, const DrakenVector* m, const DrakenVector* l)
        : outer(o), mid(m), leaf(l) {}

    bool present() const { return outer != nullptr; }
    uint32_t n_files() const { return outer->length; }
    bool leaf_valid(int32_t g) const { return sketch_bit_valid(leaf->validity, static_cast<uint32_t>(g)); }

    // Resolve file `i`'s `field` slice to a leaf index range [g0, g1); calls
    // fn(g0, g1) once, or not at all when the file has no slice for that column
    // (null outer/middle row or column out of range).
    template <typename Fn>
    void with_field_slice(uint32_t i, int64_t field, Fn&& fn) const {
        if (i >= outer->length) return;
        if (!sketch_bit_valid(outer->validity, i)) return;
        const int32_t* poff = static_cast<const int32_t*>(outer->data);
        const int32_t* moff = static_cast<const int32_t*>(mid->data);
        const uint32_t pi = outer->selection[i];
        const int64_t mrow = static_cast<int64_t>(poff[pi]) + field;
        if (mrow >= poff[pi + 1u]) return;
        if (!sketch_bit_valid(mid->validity, static_cast<uint32_t>(mrow))) return;
        const uint32_t mj = mid->selection[static_cast<uint32_t>(mrow)];
        fn(moff[mj], moff[mj + 1u]);
    }

    uint64_t u64(int32_t g) const {
        return static_cast<const uint64_t*>(leaf->data)[leaf->selection[static_cast<uint32_t>(g)]];
    }
    int64_t i64(int32_t g) const {
        return static_cast<const int64_t*>(leaf->data)[leaf->selection[static_cast<uint32_t>(g)]];
    }
};

// The KMV union of `field`'s min-hash sketches over the outer rows `rows`.
// Merged into `out`; the caller reads size/exactness/estimate from it.
inline void kmv_union(const NestedArrayView& v, int64_t field, const std::vector<uint32_t>& rows,
                      ManifestSketch& out) {
    if (field < 0) return;
    for (uint32_t i : rows) {
        v.with_field_slice(i, field, [&](int32_t g0, int32_t g1) {
            for (int32_t g = g0; g < g1; ++g) {
                if (!v.leaf_valid(g)) continue;
                out.add(v.u64(g));
            }
        });
    }
}

// Sum `field`'s 8-class byte counts over the outer rows `rows`. False when no
// row has a well-formed (8-wide) slice - "no stats", distinct from all-zero.
inline bool char_class_totals(const NestedArrayView& v, int64_t field, const std::vector<uint32_t>& rows,
                              int64_t (&totals)[8]) {
    for (int k = 0; k < 8; ++k) totals[k] = 0;
    if (field < 0) return false;
    bool any = false;
    for (uint32_t i : rows) {
        v.with_field_slice(i, field, [&](int32_t g0, int32_t g1) {
            if (g1 - g0 != 8) return;   // absent/malformed slice for this file -- skip
            any = true;
            for (int32_t g = g0; g < g1; ++g) {
                totals[g - g0] += v.leaf_valid(g) ? v.i64(g) : 0;
            }
        });
    }
    return any;
}

// `field`'s histogram bins for outer row `i`, appended to `counts` (a null bin
// reads as 0 - the neutral count). Appends nothing when the row has no slice.
inline void histogram_slice(const NestedArrayView& v, int64_t field, uint32_t i, std::vector<int64_t>& counts) {
    v.with_field_slice(i, field, [&](int32_t g0, int32_t g1) {
        for (int32_t g = g0; g < g1; ++g) {
            counts.push_back(v.leaf_valid(g) ? v.i64(g) : 0);
        }
    });
}

// (distinct count, exact) from a merged sketch's K smallest hashes, ROUNDED
// (+0.5) - the stored-sketch estimator estimate_from_min_k has always been,
// as distinct from KmvSketch::estimate()'s truncating one that kmv_ndv uses.
template <typename Sketch>
inline double rounded_estimate(const Sketch& sketch, bool& exact) {
    exact = sketch.is_exact();
    if (exact) return static_cast<double>(sketch.size());
    const double v = static_cast<double>(sketch.kth_smallest()) / draken::kKmvHashSpace;
    if (v <= 0.0) return static_cast<double>(Sketch::kK);
    return static_cast<double>(Sketch::kK - 1u) / v + 0.5;
}

}  // namespace opteryx::planner
