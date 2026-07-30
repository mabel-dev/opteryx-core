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
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

#include "core/buffers.h"
#include "core/draken_bridge.h"

namespace nb = nanobind;

// KMV sketch width — must match the writer (opteryx_catalog manifest.MIN_K_HASHES)
// and the Python reader (manifest.estimate_cardinality K=32).
static const uint32_t KMV_K = 32;

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

    std::set<uint64_t> kmin;   // the K smallest distinct hashes seen so far
    uint64_t worst = 0;        // == *kmin.rbegin() once kmin is full (size == K)

    auto merge_row = [&](uint32_t i) {
        with_field_slice(v, i, field_id, [&](int32_t g0, int32_t g1) {
            for (int32_t g = g0; g < g1; ++g) {
                if (!v.leaf_valid(g)) continue;
                const uint64_t hv = ldata[v.lsel[static_cast<uint32_t>(g)]];
                if (kmin.size() < KMV_K) {
                    kmin.insert(hv);
                    if (kmin.size() == KMV_K) worst = *kmin.rbegin();
                } else if (hv < worst) {
                    if (kmin.insert(hv).second) {
                        kmin.erase(std::prev(kmin.end()));     // drop the largest
                        worst = *kmin.rbegin();
                    }
                }
            }
        });
    };

    if (rows.has_value()) {
        for (uint32_t i : *rows) merge_row(i);
    } else {
        for (uint32_t i = 0, n = v.n_files(); i < n; ++i) merge_row(i);
    }

    if (kmin.empty())
        return nb::none();
    if (kmin.size() < KMV_K)
        return nb::int_(static_cast<int64_t>(kmin.size()));    // exact distinct count

    // KMV estimate: (K-1) * 2^64 / kth-smallest. Numerator needs 128 bits; the
    // result can exceed 2^64 for tiny kth, so build the Python int from a double
    // (truncates toward zero, matching Python int(float)).
    const uint64_t kth = *kmin.rbegin();                       // K-th smallest (index K-1)
    const double num = static_cast<double>(
        (static_cast<__int128>(KMV_K - 1)) << 64);
    const double est = num / static_cast<double>(kth);
    return nb::steal<nb::object>(PyLong_FromDouble(est));
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

void register_vector_sketch_reduce(nb::module_ &m) {
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
