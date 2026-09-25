// opteryx/compiled/nanobind/vector_sketch_reduce.cpp
//
// Consumer-edge reductions over manifest KMV sketch vectors. These read a draken
// `array<array<uint64>>` Vector (one outer row per data file, one middle row per
// data-file column, leaf = that column's <=K min-hashes) through the draken ABI
// and compute cardinality/statistics WITHOUT boxing the hashes into Python. The
// KMV math is opteryx's, not draken's — draken only supplies the nested-array
// access primitives (unwrap / array_child_unwrap / array_grandchild_unwrap).
//
// Replaces the Python merge loop in opteryx/models/manifest.py:estimate_cardinality
// (and the K-minhash half of opteryx/utils/kmv.py) on the read/merge side.
//
// Nested traversal contract (see draken_native.cpp:4293 and the two-level skeleton
// in the E0 readback path): an ARRAY Vector's `data` IS the int32 offsets buffer;
// the child lives out-of-band on the owner tree and is reached via the bridge
// unwrap helpers, never by casting `data`. Row i's child range is
// offsets[selection[i]] .. offsets[selection[i]+1]; leaf value k is
// leaf.data[leaf.selection[k]] with validity indexed by the raw child index.

#include <Python.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/vector.h>

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "core/buffers.h"
#include "core/draken_bridge.h"
#include "core/kmv_sketch.h"

namespace nb = nanobind;

// THE sketch — draken/core/kmv_sketch.h, shared with skene's value-ordering
// decline and rugo's dictionary-encoding decision. The family tag is a
// correctness discriminant: these hashes are draken's Vector.hash(), which does
// NOT interchange with skene's XXH3-over-value-bytes sketches (they disagree
// about nulls and about decimal identity, so a cross-family union is a number
// with no meaning — architect ruling 2026-08-21). Making the family part of the
// TYPE means such a merge cannot compile.
using ManifestSketch =
    draken::KmvSketch<32u, draken::KmvHashFamily::kDrakenVectorHash>;

// KMV sketch width — must match the writer (opteryx_catalog manifest.MIN_K_HASHES)
// and the Python reader (manifest.estimate_cardinality K=32).
static const uint32_t KMV_K = static_cast<uint32_t>(ManifestSketch::kK);

static inline bool bit_valid(const uint8_t* validity, uint32_t idx) {
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
    const DrakenVector* outer;
    const DrakenVector* mid;
    const DrakenVector* leaf;
    const int32_t*  poff;   // outer offsets
    const uint32_t* psel;   // outer selection
    const int32_t*  moff;   // middle offsets
    const uint32_t* msel;   // middle selection
    const uint32_t* lsel;   // leaf selection

    uint32_t n_files() const { return outer->length; }
    bool leaf_valid(int32_t g) const { return bit_valid(leaf->validity, static_cast<uint32_t>(g)); }
};

static NestedArrayView make_nested_view(nb::object column) {
    NestedArrayView v;
    v.outer = draken_vector_unwrap(column.ptr());
    if (!v.outer) throw nb::python_error();
    v.mid = draken_array_child_unwrap(column.ptr());
    if (!v.mid) throw nb::python_error();
    v.leaf = draken_array_grandchild_unwrap(column.ptr());
    if (!v.leaf) throw nb::python_error();
    v.poff = static_cast<const int32_t*>(v.outer->data);
    v.psel = v.outer->selection;
    v.moff = static_cast<const int32_t*>(v.mid->data);
    v.msel = v.mid->selection;
    v.lsel = v.leaf->selection;
    return v;
}

// Resolve file `i`'s field_id slice to a leaf index range [g0, g1); calls
// fn(g0, g1) once, or not at all when the file has no slice for that column
// (null outer/middle row or column out of range). The caller iterates the range
// with the leaf typing/null policy it needs.
template <typename Fn>
static inline void with_field_slice(const NestedArrayView& v, uint32_t i,
                                    int64_t field_id, Fn&& fn) {
    if (i >= v.outer->length) return;
    if (!bit_valid(v.outer->validity, i)) return;
    const uint32_t pi = v.psel[i];
    const int64_t  mrow = static_cast<int64_t>(v.poff[pi]) + field_id;
    if (mrow >= v.poff[pi + 1u]) return;
    if (!bit_valid(v.mid->validity, static_cast<uint32_t>(mrow))) return;
    const uint32_t mj = v.msel[static_cast<uint32_t>(mrow)];
    fn(v.moff[mj], v.moff[mj + 1u]);
}

// kmv_ndv(column, field_id) -> Optional[int]
//
// column: array<array<uint64>> Vector for one manifest's `min_k_hashes`.
// field_id: positional data-file column index (schema order), matching the Python
//           `file_entry.min_k_hashes[field_id]` access.
//
// Merges every file's min-hash sketch for that column into the global K smallest
// distinct hashes (KMV union), then returns:
//   * the exact distinct count when fewer than K hashes were seen (the sketch is
//     the complete distinct set), or
//   * the KMV cardinality estimate (K-1) * 2^64 / kth-smallest otherwise.
// Returns None when no hashes exist for the column (mirrors estimate_cardinality
// returning None). This is the native equivalent of the Python merge loop; the
// estimate branch uses the same float division as Python and may differ by at most
// one from the pure-Python result on the estimate path (it is a cardinality
// *estimate*; the exact-count branch is bit-identical).
// `rows`, when given, restricts the merge to those outer row indices (the
// manifest's surviving files after pruning, in original vector-row order). This
// keeps the native reduction aligned with the same file set the Python path
// reduces over — the vector is built once over the full file set, but
// prune_files shrinks the logical file list, so the caller passes the live rows.
// None → every row.
static nb::object kmv_ndv(nb::object column, int64_t field_id,
                          std::optional<std::vector<uint32_t>> rows = std::nullopt) {
    const NestedArrayView v = make_nested_view(column);
    if (v.leaf->type != DRAKEN_UINT64)
        throw nb::type_error("kmv_ndv: leaf type must be UINT64");
    if (field_id < 0)
        return nb::none();

    const uint64_t* ldata = static_cast<const uint64_t*>(v.leaf->data);

    ManifestSketch kmin;       // the K smallest distinct hashes seen so far

    auto merge_row = [&](uint32_t i) {
        with_field_slice(v, i, field_id, [&](int32_t g0, int32_t g1) {
            for (int32_t g = g0; g < g1; ++g) {
                if (!v.leaf_valid(g)) continue;
                kmin.add(ldata[v.lsel[static_cast<uint32_t>(g)]]);
            }
        });
    };

    if (rows.has_value()) {
        for (uint32_t i : *rows) merge_row(i);
    } else {
        for (uint32_t i = 0, n = v.n_files(); i < n; ++i) merge_row(i);
    }

    if (kmin.size() == 0u)
        return nb::none();
    if (kmin.is_exact())
        return nb::int_(static_cast<int64_t>(kmin.size()));    // exact distinct count

    // KMV estimate (K-1)/v, v = kth-smallest normalised into [0,1). The result
    // can exceed 2^64 for tiny kth, so build the Python int from a double
    // (truncates toward zero, matching Python int(float)). This replaces an
    // open-coded (K-1)*2^64/kth here: the two expressions were verified to
    // truncate identically over 2,000,000 random kth values, so the merged
    // estimates this returns are unchanged.
    return nb::steal<nb::object>(PyLong_FromDouble(kmin.estimate()));
}

// sketch_keep_mask(column, field_id, probe_hashes) -> bytes[n_files]
//
// Conservative exact-set file elimination for `col = v` / `col IN (...)`. Given
// the pre-hashed probe values (hashed by the caller with the SAME function the
// sketch was built with), returns a per-file keep mask: byte 1 = keep, 0 =
// eliminate.
//
// A file is eliminated ONLY when its sketch for `field_id` is UNSATURATED — fewer
// than K entries, i.e. the complete distinct set — AND none of the probe hashes
// appear in it. Every other case keeps the file: a saturated sketch (>= K, a
// truncated bottom-K sample that can't rule a value out), an empty/missing sketch,
// a null row, or a column with no sketch. This guarantees a file is dropped only
// when it provably contains none of the probe values, so a wrong/mismatched probe
// hash can only ever DISABLE elimination, never drop a file that matches.
static nb::object sketch_keep_mask(nb::object column, int64_t field_id,
                                   std::vector<uint64_t> probe_hashes) {
    const NestedArrayView v = make_nested_view(column);
    if (v.leaf->type != DRAKEN_UINT64)
        throw nb::type_error("sketch_keep_mask: leaf type must be UINT64");

    std::string mask(v.n_files(), static_cast<char>(1));   // default: keep all

    // Nothing to eliminate on → keep everything.
    if (field_id < 0 || probe_hashes.empty())
        return nb::bytes(mask.data(), mask.size());

    const std::unordered_set<uint64_t> probes(probe_hashes.begin(), probe_hashes.end());
    const uint64_t* ldata = static_cast<const uint64_t*>(v.leaf->data);

    for (uint32_t i = 0, n = v.n_files(); i < n; ++i) {
        with_field_slice(v, i, field_id, [&](int32_t g0, int32_t g1) {
            const uint32_t count = static_cast<uint32_t>(g1 - g0);
            // Empty (ambiguous) or saturated (truncated sample) → cannot rule out → keep.
            if (count == 0 || count >= KMV_K) return;
            // Unsaturated complete set: eliminate iff no probe hash is present.
            for (int32_t g = g0; g < g1; ++g) {
                if (!v.leaf_valid(g)) continue;
                if (probes.count(ldata[v.lsel[static_cast<uint32_t>(g)]])) return;  // present → keep
            }
            mask[i] = static_cast<char>(0);                            // provably absent → drop
        });
    }
    return nb::bytes(mask.data(), mask.size());
}

// histogram_field_slices(column, field_id) -> (counts_bytes, offsets_bytes)
//
// Gathers one column's per-file histogram-count slices out of a manifest's
// array<array<int64>> histogram_counts Vector into two flat native buffers:
//   counts_bytes  — all files' field_id bin counts concatenated (int64, native endian)
//   offsets_bytes — int32[n_files+1]; file i's counts are counts[off[i]:off[i+1]]
// The caller slices these zero-copy (memoryview) into load_counts_i64 + merge —
// no nested-list boxing, no (center,count) tuples. A file with no histogram for
// field_id (null row, column absent, empty slice) gets an empty range
// (off[i] == off[i+1]). Null leaf bins are read as 0 — the manifest writer emits
// dense int counts, so a null bin is not expected, but 0 is the neutral count.
static nb::object histogram_field_slices(nb::object column, int64_t field_id) {
    const NestedArrayView v = make_nested_view(column);
    if (v.leaf->type != DRAKEN_INT64)
        throw nb::type_error("histogram_field_slices: leaf type must be INT64");

    const int64_t* ldata = static_cast<const int64_t*>(v.leaf->data);

    std::vector<int64_t> counts;
    std::vector<int32_t> offsets;
    offsets.reserve(v.n_files() + 1);
    offsets.push_back(0);

    for (uint32_t i = 0, n = v.n_files(); i < n; ++i) {
        with_field_slice(v, i, field_id, [&](int32_t g0, int32_t g1) {
            for (int32_t g = g0; g < g1; ++g) {
                counts.push_back(v.leaf_valid(g) ? ldata[v.lsel[static_cast<uint32_t>(g)]] : 0);
            }
        });
        offsets.push_back(static_cast<int32_t>(counts.size()));
    }

    nb::bytes counts_b(reinterpret_cast<const char*>(counts.data()),
                       counts.size() * sizeof(int64_t));
    nb::bytes offsets_b(reinterpret_cast<const char*>(offsets.data()),
                        offsets.size() * sizeof(int32_t));
    return nb::make_tuple(counts_b, offsets_b);
}

// char_class_field_totals(column, field_id, rows=None) -> Optional[list[8]]
//
// column: array<array<int64>> Vector for one manifest's `char_class_counts`
// (8-class byte histogram per file per column — see draken's
// Vector.char_class_stats()). field_id: positional data-file column index.
//
// Sums that column's 8-class leaf across every file (or, when `rows` is
// given, only those outer row indices — the manifest's surviving files
// after pruning, same `rows` convention as kmv_ndv). Unlike
// histogram_field_slices this returns one relation-wide total, not per-file
// slices: the char-class selectivity estimator (opteryx/planner/
// cost_estimation/selectivity.py) only needs aggregate class proportions
// for the whole column, never a per-file breakdown. Returns None when the
// column has no char-class data anywhere (every file's slice for field_id
// is absent/malformed) — distinguishes "no stats" from "stats are all zero".
static nb::object char_class_field_totals(nb::object column, int64_t field_id,
                                          std::optional<std::vector<uint32_t>> rows = std::nullopt) {
    const NestedArrayView v = make_nested_view(column);
    if (v.leaf->type != DRAKEN_INT64)
        throw nb::type_error("char_class_field_totals: leaf type must be INT64");
    if (field_id < 0)
        return nb::none();

    const int64_t* ldata = static_cast<const int64_t*>(v.leaf->data);
    int64_t totals[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    bool any = false;

    auto sum_row = [&](uint32_t i) {
        with_field_slice(v, i, field_id, [&](int32_t g0, int32_t g1) {
            if (g1 - g0 != 8) return;   // absent/malformed slice for this file -- skip
            any = true;
            for (int32_t g = g0; g < g1; ++g) {
                totals[g - g0] += v.leaf_valid(g) ? ldata[v.lsel[static_cast<uint32_t>(g)]] : 0;
            }
        });
    };

    if (rows.has_value()) {
        for (uint32_t i : *rows) sum_row(i);
    } else {
        for (uint32_t i = 0, n = v.n_files(); i < n; ++i) sum_row(i);
    }

    if (!any) return nb::none();
    nb::list out;
    for (int k = 0; k < 8; ++k) out.append(nb::int_(totals[k]));
    return out;
}

// ── Write side: the sketch opteryx/utils/kmv.py used to implement in Python ──
//
// ANALYZE feeds this one morsel of Vector.hash() output at a time; `min_k`
// returns the sorted K smallest distinct hashes, which is the per-file sketch
// the manifest stores and estimate_cardinality merges across files. It is the
// SAME ManifestSketch the read side above merges with, so write and read cannot
// drift — which they could when one was a Python `set` trimmed with
// heapq.nsmallest and the other a C++ std::set.
class ColumnSketch {
  public:
    void update(const std::vector<uint64_t>& hashes) {
        // Pure C++ over the batch — the caller's list is already converted.
        nb::gil_scoped_release _gil;
        for (const uint64_t hash : hashes) sketch_.add(hash);
    }

    std::vector<uint64_t> min_k() const { return sketch_.min_k(ManifestSketch::kK); }

  private:
    ManifestSketch sketch_;
};

// Union of KMV sketches: the K smallest DISTINCT hashes across all of them.
//
// This is the whole reason a sketch is stored rather than a scalar, and the
// union is EXACT: if a hash is among the K smallest of the combined set and it
// came from sketch A, it is necessarily among the K smallest of A, so no input
// can hide a hash the answer needs.
//
// ⛔ Every input must come from the SAME hash family. ANALYZE's sketches and a
// skene v3 file's are draken's Vector.hash() (family 2); a skene v2 file's are
// XXH3 over value bytes (family 1). Merging across families produces a number
// with no meaning — see skene FORMAT.md §8 (SketchRecordHeader.hash_family) and
// the header block in draken/core/kmv_sketch.h.
static std::vector<uint64_t> merge_min_k(
        const std::vector<std::vector<uint64_t>>& sketches) {
    ManifestSketch merged;
    {
        nb::gil_scoped_release _gil;
        for (const auto& sketch : sketches)
            for (const uint64_t hash : sketch) merged.add(hash);
    }
    return merged.min_k(ManifestSketch::kK);
}

// (distinct_count, is_exact) from a merged sketch.
//
// Fewer than K hashes means the sketch never filled, so it holds EVERY distinct
// value and its length is the exact answer — the regime that matters most,
// because it covers every low-cardinality column. At or above K it is the
// standard KMV estimator (K-1)/v, relative standard error ~1/sqrt(K-2) (~18.9%
// at K=32).
//
// The +0.5 is this function's own and is NOT the shared estimator's: kmv_ndv
// above truncates (it matches Python's int(float) on the read path) while this
// one rounds, which is what the Python it replaces did. Rounding here is
// load-bearing for nothing but bit-for-bit continuity of numbers the planner
// already consumes, so it is preserved verbatim rather than harmonised.
static nb::object estimate_from_min_k(const std::vector<uint64_t>& min_k) {
    if (min_k.size() < ManifestSketch::kK)
        return nb::make_tuple(nb::int_(static_cast<int64_t>(min_k.size())), true);
    const double v =
        static_cast<double>(min_k[ManifestSketch::kK - 1u]) / draken::kKmvHashSpace;
    if (v <= 0.0)
        // Needs the K-th smallest hash to be 0 — report K rather than infinity,
        // matching the shared estimator's own guard.
        return nb::make_tuple(nb::int_(static_cast<int64_t>(ManifestSketch::kK)), false);
    const double est = static_cast<double>(ManifestSketch::kK - 1u) / v;
    return nb::make_tuple(nb::steal<nb::object>(PyLong_FromDouble(est + 0.5)), false);
}

void register_vector_sketch_reduce(nb::module_ &m) {
    nb::class_<ColumnSketch>(m, "ColumnSketch")
        .def(nb::init<>())
        .def("update", &ColumnSketch::update, nb::arg("hashes"),
            "Fold one batch of native per-row hashes (Vector.hash()) into the sketch.")
        .def("min_k", &ColumnSketch::min_k,
            "The sorted K smallest distinct hashes — the per-file sketch the manifest stores.");

    m.def("merge_min_k", &merge_min_k, nb::arg("sketches"),
        "Union of KMV sketches: the K=32 smallest DISTINCT hashes across all of them. "
        "Every input must come from the SAME hash function (draken's Vector.hash()).");

    m.def("estimate_from_min_k", &estimate_from_min_k, nb::arg("min_k"),
        "(distinct_count, is_exact) from a merged K=32 sketch. Exact when the sketch "
        "never filled, else the KMV (K-1)/v estimate.");


    m.def("kmv_ndv", &kmv_ndv,
        nb::arg("column"), nb::arg("field_id"), nb::arg("rows") = nb::none(),
        "Estimate distinct count for one column from a manifest's array<array<uint64>> "
        "min_k_hashes Vector (KMV union across files). Returns exact count when the "
        "merged sketch is unsaturated, else the KMV estimate; None if empty. `rows` "
        "restricts the merge to those outer row indices (surviving files post-prune).");

    m.def("sketch_keep_mask", &sketch_keep_mask,
        nb::arg("column"), nb::arg("field_id"), nb::arg("probe_hashes"),
        "Conservative exact-set file elimination for = / IN. Returns bytes[n_files] "
        "(1=keep, 0=eliminate); a file is dropped only when its unsaturated (<K) "
        "sketch for field_id provably contains none of the probe hashes.");

    m.def("histogram_field_slices", &histogram_field_slices,
        nb::arg("column"), nb::arg("field_id"),
        "Gather one column's per-file histogram bin counts from an array<array<int64>> "
        "Vector into flat (counts_bytes int64, offsets_bytes int32[n_files+1]) for "
        "zero-copy load_counts_i64 + merge. Empty range for files lacking the column.");

    m.def("char_class_field_totals", &char_class_field_totals,
        nb::arg("column"), nb::arg("field_id"), nb::arg("rows") = nb::none(),
        "Sum one column's 8-class byte counts across every file (or, when `rows` is "
        "given, only those outer row indices) from a manifest's array<array<int64>> "
        "char_class_counts Vector. Returns list[8] of int, or None if the column has "
        "no char-class data anywhere.");
}
